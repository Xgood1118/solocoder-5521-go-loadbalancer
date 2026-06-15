package ratelimit

import (
	"sync"
	"time"

	"github.com/loadbalancer/lb/pkg/types"
)

type tokenBucket struct {
	tokens   float64
	lastFill time.Time
	mu       sync.Mutex
}

type Limiter struct {
	mu       sync.RWMutex
	cfg      types.RateLimitConfig
	buckets  map[string]*tokenBucket
	lastPurge time.Time
}

func NewLimiter(cfg types.RateLimitConfig) *Limiter {
	return &Limiter{
		cfg:      cfg,
		buckets:  make(map[string]*tokenBucket),
		lastPurge: time.Now(),
	}
}

func (l *Limiter) UpdateConfig(cfg types.RateLimitConfig) {
	l.mu.Lock()
	l.cfg = cfg
	l.mu.Unlock()
}

func (l *Limiter) refill(tb *tokenBucket, cfg types.RateLimitConfig) {
	now := time.Now()
	elapsed := now.Sub(tb.lastFill).Seconds()
	tb.tokens += elapsed * cfg.Rate
	if tb.tokens > float64(cfg.Bucket) {
		tb.tokens = float64(cfg.Bucket)
	}
	tb.lastFill = now
}

func (l *Limiter) Allow(key string) (allowed bool, limit, remaining int, resetSec int) {
	l.mu.RLock()
	cfg := l.cfg
	l.mu.RUnlock()

	limit = cfg.Bucket
	resetSec = 0
	remaining = 0

	if !cfg.Enabled {
		return true, limit, limit, 0
	}

	l.mu.Lock()
	now := time.Now()
	if now.Sub(l.lastPurge) > time.Hour {
		for k, v := range l.buckets {
			v.mu.Lock()
			idle := now.Sub(v.lastFill)
			v.mu.Unlock()
			if idle > time.Hour {
				delete(l.buckets, k)
			}
		}
		l.lastPurge = now
	}
	tb, ok := l.buckets[key]
	if !ok {
		tb = &tokenBucket{tokens: float64(cfg.Bucket), lastFill: now}
		l.buckets[key] = tb
	}
	l.mu.Unlock()

	tb.mu.Lock()
	defer tb.mu.Unlock()

	l.refill(tb, cfg)

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		allowed = true
		remaining = int(tb.tokens)
		if remaining < 0 {
			remaining = 0
		}
	} else {
		allowed = false
		remaining = 0
		deficit := 1.0 - tb.tokens
		resetSec = int(deficit/cfg.Rate) + 1
		if resetSec < 1 {
			resetSec = 1
		}
	}
	return
}
