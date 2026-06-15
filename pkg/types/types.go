package types

import (
	"sync"
	"sync/atomic"
	"time"
)

type LoadBalancerStrategy string

const (
	StrategyRoundRobin     LoadBalancerStrategy = "round_robin"
	StrategyWeightedRR     LoadBalancerStrategy = "weighted_round_robin"
	StrategyLeastConn      LoadBalancerStrategy = "least_connections"
	StrategyIPHash         LoadBalancerStrategy = "ip_hash"
)

type HealthCheckType string

const (
	HealthCheckHTTP HealthCheckType = "http"
	HealthCheckTCP  HealthCheckType = "tcp"
)

type CircuitBreakerState string

const (
	CircuitClosed   CircuitBreakerState = "closed"
	CircuitOpen     CircuitBreakerState = "open"
	CircuitHalfOpen CircuitBreakerState = "half_open"
)

type Backend struct {
	ID             string              `yaml:"id" json:"id"`
	Group          string              `yaml:"group" json:"group"`
	Address        string              `yaml:"address" json:"address"`
	Weight         int                 `yaml:"weight" json:"weight"`
	MaxConns       int                 `yaml:"max_conns" json:"max_conns"`
	Healthy        bool                `json:"healthy"`
	LastHeartbeat  time.Time           `json:"last_heartbeat"`
	ActiveConns    int64               `json:"active_conns"`
	UnhealthyCount int32               `json:"-"`
	HealthyCount   int32               `json:"-"`
	CBState        CircuitBreakerState `json:"circuit_breaker_state"`
	CBFailCount    int32               `json:"-"`
	CBLastStateAt  time.Time           `json:"-"`
	Mu             sync.RWMutex        `json:"-"`
}

type HealthCheckConfig struct {
	Type                  HealthCheckType `yaml:"type" json:"type"`
	IntervalSec           int             `yaml:"interval_sec" json:"interval_sec"`
	TimeoutSec            int             `yaml:"timeout_sec" json:"timeout_sec"`
	UnhealthyThreshold    int             `yaml:"unhealthy_threshold" json:"unhealthy_threshold"`
	HealthyThreshold      int             `yaml:"healthy_threshold" json:"healthy_threshold"`
	HTTPPath              string          `yaml:"http_path,omitempty" json:"http_path,omitempty"`
	HTTPExpectedStatus    int             `yaml:"http_expected_status,omitempty" json:"http_expected_status,omitempty"`
}

type CircuitBreakerConfig struct {
	FailureThreshold int   `yaml:"failure_threshold" json:"failure_threshold"`
	CooldownSec      int   `yaml:"cooldown_sec" json:"cooldown_sec"`
	HalfOpenRequests int   `yaml:"half_open_requests,omitempty" json:"half_open_requests,omitempty"`
}

type RateLimitConfig struct {
	Enabled   bool    `yaml:"enabled" json:"enabled"`
	Rate      float64 `yaml:"rate_per_sec" json:"rate_per_sec"`
	Bucket    int     `yaml:"bucket" json:"bucket"`
}

type AccessLogConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	FilePath string `yaml:"file_path" json:"file_path"`
	MaxHours int    `yaml:"max_hours" json:"max_hours"`
}

type Rule struct {
	ID          string               `yaml:"id" json:"id"`
	Domain      string               `yaml:"domain,omitempty" json:"domain,omitempty"`
	PathPrefix  string               `yaml:"path_prefix,omitempty" json:"path_prefix,omitempty"`
	BackendGroup string              `yaml:"backend_group" json:"backend_group"`
	Strategy    LoadBalancerStrategy `yaml:"strategy" json:"strategy"`
	Priority    int                  `yaml:"priority" json:"priority"`
	StripPrefix bool                 `yaml:"strip_prefix,omitempty" json:"strip_prefix,omitempty"`
}

type Config struct {
	Port           int                   `yaml:"port" json:"port"`
	AdminPort      int                   `yaml:"admin_port" json:"admin_port"`
	Includes       []string              `yaml:"includes,omitempty" json:"includes,omitempty"`
	HealthCheck    HealthCheckConfig     `yaml:"health_check" json:"health_check"`
	CircuitBreaker CircuitBreakerConfig  `yaml:"circuit_breaker" json:"circuit_breaker"`
	RateLimit      RateLimitConfig       `yaml:"rate_limit" json:"rate_limit"`
	AccessLog      AccessLogConfig       `yaml:"access_log" json:"access_log"`
	Backends       []*Backend            `yaml:"backends" json:"backends"`
	Rules          []*Rule               `yaml:"rules" json:"rules"`
}

type AuditLogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	User      string    `json:"user"`
	Action    string    `json:"action"`
	Detail    string    `json:"detail"`
}

type LatencySample struct {
	Timestamp time.Time
	LatencyMs float64
	IsErr     bool
}

func (b *Backend) IncConns() {
	atomic.AddInt64(&b.ActiveConns, 1)
}

func (b *Backend) DecConns() {
	atomic.AddInt64(&b.ActiveConns, -1)
}

func (b *Backend) GetConns() int64 {
	return atomic.LoadInt64(&b.ActiveConns)
}
