package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/loadbalancer/lb/pkg/logger"
	"github.com/loadbalancer/lb/pkg/types"
	"gopkg.in/yaml.v3"
)

type Manager struct {
	mu      sync.RWMutex
	config  *types.Config
	path    string
	version int64
}

func NewManager(path string) *Manager {
	return &Manager{
		path: path,
	}
}

func (m *Manager) Load() (*types.Config, error) {
	cfg, err := loadWithIncludes(m.path)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.config = cfg
	m.version++
	cfgVer := m.version
	m.mu.Unlock()
	logger.RecordAudit("system", "config_load", fmt.Sprintf("version=%d path=%s", cfgVer, m.path))
	return cfg, nil
}

func (m *Manager) Get() *types.Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

func (m *Manager) Version() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.version
}

func (m *Manager) Reload() (*types.Config, error) {
	cfg, err := m.Load()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func loadWithIncludes(path string) (*types.Config, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve config path: %w", err)
	}
	baseDir := filepath.Dir(absPath)

	data, err := ioutil.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", absPath, err)
	}

	var cfg types.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", absPath, err)
	}

	visited := map[string]bool{absPath: true}
	for _, inc := range cfg.Includes {
		incPath := inc
		if !filepath.IsAbs(incPath) {
			incPath = filepath.Join(baseDir, incPath)
		}
		incPath = filepath.Clean(incPath)
		if visited[incPath] {
			continue
		}
		visited[incPath] = true
		if _, err := os.Stat(incPath); os.IsNotExist(err) {
			logger.Log.Warn().Str("include", incPath).Msg("include file not found, skipping")
			continue
		}
		incData, err := ioutil.ReadFile(incPath)
		if err != nil {
			return nil, fmt.Errorf("read include %s: %w", incPath, err)
		}
		var incCfg types.Config
		if err := yaml.Unmarshal(incData, &incCfg); err != nil {
			return nil, fmt.Errorf("parse include %s: %w", incPath, err)
		}
		cfg.Backends = append(cfg.Backends, incCfg.Backends...)
		cfg.Rules = append(cfg.Rules, incCfg.Rules...)
	}

	applyDefaults(&cfg)
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyDefaults(cfg *types.Config) {
	if cfg.Port == 0 {
		if envPort := os.Getenv("LB_PORT"); envPort != "" {
			fmt.Sscanf(envPort, "%d", &cfg.Port)
		}
		if cfg.Port == 0 {
			cfg.Port = 80
		}
	}
	if cfg.AdminPort == 0 {
		cfg.AdminPort = 9090
	}
	if cfg.HealthCheck.IntervalSec == 0 {
		cfg.HealthCheck.IntervalSec = 10
	}
	if cfg.HealthCheck.TimeoutSec == 0 {
		cfg.HealthCheck.TimeoutSec = 5
	}
	if cfg.HealthCheck.UnhealthyThreshold == 0 {
		cfg.HealthCheck.UnhealthyThreshold = 3
	}
	if cfg.HealthCheck.HealthyThreshold == 0 {
		cfg.HealthCheck.HealthyThreshold = 2
	}
	if cfg.HealthCheck.Type == "" {
		cfg.HealthCheck.Type = types.HealthCheckTCP
	}
	if cfg.HealthCheck.Type == types.HealthCheckHTTP && cfg.HealthCheck.HTTPPath == "" {
		cfg.HealthCheck.HTTPPath = "/health"
	}
	if cfg.HealthCheck.Type == types.HealthCheckHTTP && cfg.HealthCheck.HTTPExpectedStatus == 0 {
		cfg.HealthCheck.HTTPExpectedStatus = 200
	}
	if cfg.CircuitBreaker.FailureThreshold == 0 {
		cfg.CircuitBreaker.FailureThreshold = 5
	}
	if cfg.CircuitBreaker.CooldownSec == 0 {
		cfg.CircuitBreaker.CooldownSec = 30
	}
	for _, b := range cfg.Backends {
		if b.Weight <= 0 {
			b.Weight = 1
		}
		if b.MaxConns <= 0 {
			b.MaxConns = 1000
		}
		b.Healthy = true
		b.CBState = types.CircuitClosed
	}
	if cfg.RateLimit.Bucket == 0 {
		cfg.RateLimit.Bucket = 100
	}
	if cfg.RateLimit.Rate <= 0 {
		cfg.RateLimit.Rate = 100
	}
	if cfg.AccessLog.MaxHours == 0 {
		cfg.AccessLog.MaxHours = 24 * 7
	}
}

func validateConfig(cfg *types.Config) error {
	seenBackend := map[string]bool{}
	for _, b := range cfg.Backends {
		if b.ID == "" {
			return fmt.Errorf("backend missing id")
		}
		if b.Address == "" {
			return fmt.Errorf("backend %s missing address", b.ID)
		}
		if seenBackend[b.ID] {
			return fmt.Errorf("duplicate backend id: %s", b.ID)
		}
		seenBackend[b.ID] = true
	}
	seenRule := map[string]bool{}
	for _, r := range cfg.Rules {
		if r.ID == "" {
			return fmt.Errorf("rule missing id")
		}
		if seenRule[r.ID] {
			return fmt.Errorf("duplicate rule id: %s", r.ID)
		}
		seenRule[r.ID] = true
		if r.BackendGroup == "" {
			return fmt.Errorf("rule %s missing backend_group", r.ID)
		}
		if r.Strategy == "" {
			r.Strategy = types.StrategyRoundRobin
		}
	}
	return nil
}
