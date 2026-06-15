package circuitbreaker

import (
	"sync"
	"testing"
	"time"

	"github.com/loadbalancer/lb/pkg/types"
)

func TestCircuitBreakerFlow(t *testing.T) {
	cfg := types.CircuitBreakerConfig{
		FailureThreshold: 3,
		CooldownSec:      1,
	}
	m := NewManager(cfg)

	b := &types.Backend{
		ID:          "test",
		CBState:     types.CircuitClosed,
		CBLastStateAt: time.Now().Add(-time.Hour),
	}

	for i := 0; i < 5; i++ {
		if !m.Allow(b) {
			t.Fatalf("expected allow on iteration %d", i)
		}
	}

	for i := 0; i < 3; i++ {
		m.RecordFailure(b)
	}

	if m.Allow(b) {
		t.Error("expected circuit open after 3 failures")
	}
	if b.CBState != types.CircuitOpen {
		t.Errorf("expected state open, got %s", b.CBState)
	}

	m.RecordSuccess(b)
	if b.CBState == types.CircuitClosed {
		t.Error("circuit should stay open even with success when in open state")
	}

	b.CBLastStateAt = time.Now().Add(-2 * time.Second)
	if !m.Allow(b) {
		t.Error("expected half-open transition after cooldown")
	}
	if b.CBState != types.CircuitHalfOpen {
		t.Errorf("expected half_open, got %s", b.CBState)
	}

	if m.Allow(b) {
		t.Error("expected only one request in half-open (default limit 1)")
	}

	m.RecordSuccess(b)
	if b.CBState != types.CircuitClosed {
		t.Errorf("expected closed after half-open success, got %s", b.CBState)
	}
}

func TestCircuitBreakerConcurrent(t *testing.T) {
	cfg := types.CircuitBreakerConfig{
		FailureThreshold: 10,
		CooldownSec:      5,
	}
	m := NewManager(cfg)

	b := &types.Backend{
		ID:            "test-conc",
		CBState:       types.CircuitClosed,
		CBLastStateAt: time.Now().Add(-time.Hour),
	}

	var wg sync.WaitGroup
	workers := 20
	perWorker := 100

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				allowed := m.Allow(b)
				if allowed {
					if i%3 == 0 {
						m.RecordFailure(b)
					} else {
						m.RecordSuccess(b)
					}
				}
			}
		}()
	}
	wg.Wait()

	t.Logf("final state: %s", b.CBState)
}
