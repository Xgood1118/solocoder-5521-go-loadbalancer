package healthcheck

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/loadbalancer/lb/pkg/logger"
	"github.com/loadbalancer/lb/pkg/types"
)

type Checker struct {
	mu       sync.RWMutex
	cfg      types.HealthCheckConfig
	backends map[string]*types.Backend
	stopChs  map[string]chan struct{}
	running  bool
}

func NewChecker(cfg types.HealthCheckConfig) *Checker {
	return &Checker{
		cfg:      cfg,
		backends: make(map[string]*types.Backend),
		stopChs:  make(map[string]chan struct{}),
	}
}

func (c *Checker) AddBackend(b *types.Backend) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.backends[b.ID]; exists {
		return
	}
	c.backends[b.ID] = b
	stopCh := make(chan struct{})
	c.stopChs[b.ID] = stopCh
	go c.runCheck(b, stopCh)
	logger.Log.Info().Str("backend", b.ID).Str("address", b.Address).Msg("health check started")
}

func (c *Checker) RemoveBackend(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.stopChs[id]; ok {
		close(ch)
		delete(c.stopChs, id)
	}
	delete(c.backends, id)
}

func (c *Checker) UpdateConfig(cfg types.HealthCheckConfig) {
	c.mu.Lock()
	c.cfg = cfg
	c.mu.Unlock()
}

func (c *Checker) StopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, ch := range c.stopChs {
		close(ch)
		delete(c.stopChs, id)
		delete(c.backends, id)
		_ = id
	}
}

func (c *Checker) runCheck(b *types.Backend, stopCh chan struct{}) {
	c.mu.RLock()
	interval := time.Duration(c.cfg.IntervalSec) * time.Second
	c.mu.RUnlock()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			logger.Log.Info().Str("backend", b.ID).Msg("health check stopped")
			return
		case <-ticker.C:
			c.doCheck(b)
		}
	}
}

func (c *Checker) doCheck(b *types.Backend) {
	c.mu.RLock()
	cfg := c.cfg
	timeout := time.Duration(cfg.TimeoutSec) * time.Second
	c.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var ok bool
	var errMsg string
	switch cfg.Type {
	case types.HealthCheckHTTP:
		ok, errMsg = c.checkHTTP(ctx, b, cfg)
	case types.HealthCheckTCP:
		ok, errMsg = c.checkTCP(ctx, b, cfg)
	default:
		ok, errMsg = c.checkTCP(ctx, b, cfg)
	}

	now := time.Now()
	b.Mu.Lock()
	b.LastHeartbeat = now
	if ok {
		b.UnhealthyCount = 0
		b.HealthyCount++
		if !b.Healthy && int(b.HealthyCount) >= cfg.HealthyThreshold {
			b.Healthy = true
			b.HealthyCount = 0
			logger.Log.Warn().Str("backend", b.ID).Str("address", b.Address).Msg("backend recovered to healthy")
		} else if b.Healthy {
			b.HealthyCount = 0
		}
	} else {
		b.HealthyCount = 0
		b.UnhealthyCount++
		if b.Healthy && int(b.UnhealthyCount) >= cfg.UnhealthyThreshold {
			b.Healthy = false
			b.UnhealthyCount = 0
			logger.Log.Warn().Str("backend", b.ID).Str("address", b.Address).Str("reason", errMsg).Msg("backend marked unhealthy")
		}
	}
	b.Mu.Unlock()
}

func (c *Checker) checkHTTP(ctx context.Context, b *types.Backend, cfg types.HealthCheckConfig) (bool, string) {
	scheme := "http"
	url := fmt.Sprintf("%s://%s%s", scheme, b.Address, cfg.HTTPPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Sprintf("build request: %v", err)
	}
	client := &http.Client{Timeout: time.Duration(cfg.TimeoutSec) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Sprintf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != cfg.HTTPExpectedStatus {
		return false, fmt.Sprintf("unexpected status %d (expected %d)", resp.StatusCode, cfg.HTTPExpectedStatus)
	}
	return true, ""
}

func (c *Checker) checkTCP(ctx context.Context, b *types.Backend, cfg types.HealthCheckConfig) (bool, string) {
	dialer := &net.Dialer{Timeout: time.Duration(cfg.TimeoutSec) * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", b.Address)
	if err != nil {
		return false, fmt.Sprintf("dial: %v", err)
	}
	conn.Close()
	return true, ""
}
