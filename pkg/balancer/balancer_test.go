package balancer

import (
	"net/http"
	"sync"
	"testing"

	"github.com/loadbalancer/lb/pkg/types"
)

func TestWeightedRoundRobinDistribution(t *testing.T) {
	backends := []*types.Backend{
		{ID: "a", Group: "g", Address: "127.0.0.1:8001", Weight: 3, MaxConns: 100, Healthy: true, CBState: types.CircuitClosed},
		{ID: "b", Group: "g", Address: "127.0.0.1:8002", Weight: 2, MaxConns: 100, Healthy: true, CBState: types.CircuitClosed},
		{ID: "c", Group: "g", Address: "127.0.0.1:8003", Weight: 1, MaxConns: 100, Healthy: true, CBState: types.CircuitClosed},
	}
	wb := NewWeightedRoundRobin(backends)

	counts := map[string]int{}
	total := 6000
	for i := 0; i < total; i++ {
		req, _ := http.NewRequest("GET", "/", nil)
		be, err := wb.Next(req)
		if err != nil {
			t.Fatal(err)
		}
		counts[be.ID]++
	}

	ratioA := float64(counts["a"]) / float64(total)
	ratioB := float64(counts["b"]) / float64(total)
	ratioC := float64(counts["c"]) / float64(total)

	expectedA := 3.0 / 6.0
	expectedB := 2.0 / 6.0
	expectedC := 1.0 / 6.0

	t.Logf("distribution: a=%.3f (exp %.3f), b=%.3f (exp %.3f), c=%.3f (exp %.3f)",
		ratioA, expectedA, ratioB, expectedB, ratioC, expectedC)

	tolerance := 0.05
	if diff := abs(ratioA - expectedA); diff > tolerance {
		t.Errorf("backend a ratio off: got %.3f, expected ~%.3f", ratioA, expectedA)
	}
	if diff := abs(ratioB - expectedB); diff > tolerance {
		t.Errorf("backend b ratio off: got %.3f, expected ~%.3f", ratioB, expectedB)
	}
	if diff := abs(ratioC - expectedC); diff > tolerance {
		t.Errorf("backend c ratio off: got %.3f, expected ~%.3f", ratioC, expectedC)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func TestWeightedRoundRobinConcurrent(t *testing.T) {
	backends := []*types.Backend{
		{ID: "a", Group: "g", Address: "127.0.0.1:8001", Weight: 5, MaxConns: 1000, Healthy: true, CBState: types.CircuitClosed},
		{ID: "b", Group: "g", Address: "127.0.0.1:8002", Weight: 3, MaxConns: 1000, Healthy: true, CBState: types.CircuitClosed},
		{ID: "c", Group: "g", Address: "127.0.0.1:8003", Weight: 2, MaxConns: 1000, Healthy: true, CBState: types.CircuitClosed},
	}
	wb := NewWeightedRoundRobin(backends)

	var wg sync.WaitGroup
	workers := 50
	perWorker := 200
	total := workers * perWorker

	counts := make([]map[string]int, workers)
	for i := 0; i < workers; i++ {
		counts[i] = make(map[string]int)
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				req, _ := http.NewRequest("GET", "/", nil)
				req.RemoteAddr = "10.0.0.1:12345"
				be, err := wb.Next(req)
				if err == nil {
					counts[workerID][be.ID]++
				}
			}
		}(w)
	}
	wg.Wait()

	agg := map[string]int{}
	for _, m := range counts {
		for k, v := range m {
			agg[k] += v
		}
	}
	t.Logf("total requests: %d, distribution: %+v", total, agg)
	gotTotal := 0
	for _, v := range agg {
		gotTotal += v
	}
	if gotTotal != total {
		t.Errorf("expected %d total, got %d", total, gotTotal)
	}
}

func TestIPHashConsistency(t *testing.T) {
	backends := []*types.Backend{
		{ID: "a", Group: "g", Address: "127.0.0.1:8001", Weight: 1, MaxConns: 100, Healthy: true, CBState: types.CircuitClosed},
		{ID: "b", Group: "g", Address: "127.0.0.1:8002", Weight: 1, MaxConns: 100, Healthy: true, CBState: types.CircuitClosed},
		{ID: "c", Group: "g", Address: "127.0.0.1:8003", Weight: 1, MaxConns: 100, Healthy: true, CBState: types.CircuitClosed},
	}
	ih := NewIPHash(backends)

	req1, _ := http.NewRequest("GET", "/", nil)
	req1.RemoteAddr = "192.168.1.100:54321"
	be1, err := ih.Next(req1)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		req, _ := http.NewRequest("GET", "/", nil)
		req.RemoteAddr = "192.168.1.100:54321"
		be, err := ih.Next(req)
		if err != nil {
			t.Fatal(err)
		}
		if be.ID != be1.ID {
			t.Errorf("ip hash inconsistent: expected %s, got %s", be1.ID, be.ID)
		}
	}
}

func TestLeastConnections(t *testing.T) {
	backends := []*types.Backend{
		{ID: "a", Group: "g", Address: "127.0.0.1:8001", Weight: 1, MaxConns: 100, Healthy: true, CBState: types.CircuitClosed},
		{ID: "b", Group: "g", Address: "127.0.0.1:8002", Weight: 1, MaxConns: 100, Healthy: true, CBState: types.CircuitClosed},
		{ID: "c", Group: "g", Address: "127.0.0.1:8003", Weight: 1, MaxConns: 100, Healthy: true, CBState: types.CircuitClosed},
	}
	lc := NewLeastConnections(backends)

	backends[0].IncConns()
	backends[0].IncConns()
	backends[0].IncConns()
	backends[1].IncConns()

	req, _ := http.NewRequest("GET", "/", nil)
	be, err := lc.Next(req)
	if err != nil {
		t.Fatal(err)
	}
	if be.ID != "c" {
		t.Errorf("expected least connections backend c, got %s", be.ID)
	}
}
