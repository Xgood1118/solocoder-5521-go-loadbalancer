package stats

import (
	"math"
	"testing"
	"time"
)

func TestCollectorWindow(t *testing.T) {
	c := NewCollector(10000, time.Minute)

	for i := 0; i < 100; i++ {
		c.Record(float64(i), i%5 == 0)
	}

	time.Sleep(10 * time.Millisecond)

	snap := c.Snapshot(5 * time.Minute)
	if snap.TotalRequests != 100 {
		t.Errorf("expected 100 total requests, got %d", snap.TotalRequests)
	}
	if snap.WindowRequests != 100 {
		t.Errorf("expected 100 window requests, got %d", snap.WindowRequests)
	}
	expectedErrs := 20
	if int(snap.WindowErrors) != expectedErrs {
		t.Errorf("expected %d window errors, got %d", expectedErrs, snap.WindowErrors)
	}
	if snap.AvgLatencyMs <= 0 {
		t.Errorf("expected positive avg latency")
	}
	if snap.P99LatencyMs <= 0 {
		t.Errorf("expected positive p99 latency")
	}
	t.Logf("avg=%.2fms p99=%.2fms window=%s reqs=%d errs=%d",
		snap.AvgLatencyMs, snap.P99LatencyMs, snap.WindowDuration,
		snap.WindowRequests, snap.WindowErrors)
}

func TestCollectorSmallWindow(t *testing.T) {
	c := NewCollector(10000, time.Minute)

	for i := 0; i < 50; i++ {
		c.Record(10.0, false)
	}
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 50; i++ {
		c.Record(20.0, false)
	}

	snap := c.Snapshot(30 * time.Millisecond)
	if snap.WindowRequests > 100 {
		t.Errorf("window requests should be <= total, got %d", snap.WindowRequests)
	}
	t.Logf("short window: %d requests (total %d)", snap.WindowRequests, snap.TotalRequests)
}

func TestCollectorP99(t *testing.T) {
	c := NewCollector(10000, time.Minute)

	for i := 0; i < 100; i++ {
		lat := float64(i + 1)
		c.Record(lat, false)
	}
	snap := c.Snapshot(time.Minute)
	expectedP99 := 99.0
	if math.Abs(snap.P99LatencyMs-expectedP99) > 0.01 {
		t.Errorf("expected p99 ~%.2f, got %.2f", expectedP99, snap.P99LatencyMs)
	}
	t.Logf("p99=%.2fms avg=%.2fms", snap.P99LatencyMs, snap.AvgLatencyMs)
}
