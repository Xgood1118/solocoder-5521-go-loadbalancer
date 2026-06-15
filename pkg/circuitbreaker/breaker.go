package circuitbreaker

import (
	"sync"
	"time"

	"github.com/loadbalancer/lb/pkg/logger"
	"github.com/loadbalancer/lb/pkg/types"
)

type Manager struct {
	mu       sync.RWMutex
	cfg      types.CircuitBreakerConfig
	breakers map[string]*breakerState
}

type breakerState struct {
	failCount int32
	halfCount int32
}

func NewManager(cfg types.CircuitBreakerConfig) *Manager {
	return &Manager{
		cfg:      cfg,
		breakers: make(map[string]*breakerState),
	}
}

func (m *Manager) UpdateConfig(cfg types.CircuitBreakerConfig) {
	m.mu.Lock()
	m.cfg = cfg
	m.mu.Unlock()
}

func (m *Manager) getOrCreate(id string) *breakerState {
	bs, ok := m.breakers[id]
	if !ok {
		bs = &breakerState{}
		m.breakers[id] = bs
	}
	return bs
}

func (m *Manager) Allow(b *types.Backend) bool {
	m.mu.RLock()
	cfg := m.cfg
	m.mu.RUnlock()

	b.Mu.Lock()
	defer b.Mu.Unlock()

	switch b.CBState {
	case types.CircuitClosed:
		return true
	case types.CircuitOpen:
		cooldown := time.Duration(cfg.CooldownSec) * time.Second
		halfPoint := cooldown / 2
		if time.Since(b.CBLastStateAt) >= halfPoint {
			m.mu.Lock()
			bs := m.getOrCreate(b.ID)
			bs.halfCount = 1
			m.mu.Unlock()
			b.CBState = types.CircuitHalfOpen
			b.CBLastStateAt = time.Now()
			logger.Log.Warn().Str("backend", b.ID).Msg("circuit breaker transitioned to half_open")
			return true
		}
		return false
	case types.CircuitHalfOpen:
		m.mu.Lock()
		bs := m.getOrCreate(b.ID)
		limit := int32(1)
		if cfg.HalfOpenRequests > 0 {
			limit = int32(cfg.HalfOpenRequests)
		}
		allowed := false
		if bs.halfCount < limit {
			bs.halfCount++
			allowed = true
		}
		m.mu.Unlock()
		return allowed
	default:
		return true
	}
}

func (m *Manager) RecordSuccess(b *types.Backend) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	m.mu.Lock()
	bs := m.getOrCreate(b.ID)
	m.mu.Unlock()

	switch b.CBState {
	case types.CircuitClosed:
		bs.failCount = 0
	case types.CircuitHalfOpen:
		b.CBState = types.CircuitClosed
		b.CBLastStateAt = time.Now()
		bs.failCount = 0
		bs.halfCount = 0
		logger.Log.Warn().Str("backend", b.ID).Msg("circuit breaker transitioned to closed (success)")
	case types.CircuitOpen:
		bs.failCount = 0
	}
}

func (m *Manager) RecordFailure(b *types.Backend) {
	m.mu.RLock()
	cfg := m.cfg
	m.mu.RUnlock()

	b.Mu.Lock()
	defer b.Mu.Unlock()

	m.mu.Lock()
	bs := m.getOrCreate(b.ID)
	bs.failCount++
	m.mu.Unlock()

	switch b.CBState {
	case types.CircuitClosed:
		if bs.failCount >= int32(cfg.FailureThreshold) {
			b.CBState = types.CircuitOpen
			b.CBLastStateAt = time.Now()
			logger.Log.Warn().Str("backend", b.ID).Int32("failures", bs.failCount).Msg("circuit breaker transitioned to open")
		}
	case types.CircuitHalfOpen:
		b.CBState = types.CircuitOpen
		b.CBLastStateAt = time.Now()
		bs.halfCount = 0
		logger.Log.Warn().Str("backend", b.ID).Msg("circuit breaker transitioned to open (half_open failed)")
	}
}
