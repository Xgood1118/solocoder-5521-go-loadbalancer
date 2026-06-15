package stats

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loadbalancer/lb/pkg/types"
)

type RingBuffer struct {
	data     []types.LatencySample
	capacity int
	size     int
	head     int
	mu       sync.RWMutex
}

func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		data:     make([]types.LatencySample, capacity),
		capacity: capacity,
	}
}

func (rb *RingBuffer) Push(s types.LatencySample) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.data[rb.head] = s
	rb.head = (rb.head + 1) % rb.capacity
	if rb.size < rb.capacity {
		rb.size++
	}
}

func (rb *RingBuffer) Snapshot(window time.Duration) []types.LatencySample {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	cutoff := time.Now().Add(-window)
	result := make([]types.LatencySample, 0, rb.size)
	start := 0
	if rb.size == rb.capacity {
		start = rb.head
	}
	for i := 0; i < rb.size; i++ {
		idx := (start + i) % rb.capacity
		if rb.data[idx].Timestamp.After(cutoff) {
			result = append(result, rb.data[idx])
		}
	}
	return result
}

type Collector struct {
	ring       *RingBuffer
	reqTotal   int64
	errTotal   int64
	reqWindow  int64
	errWindow  int64
	windowStart time.Time
	windowMu   sync.Mutex
}

func NewCollector(windowSize int, windowDur time.Duration) *Collector {
	return &Collector{
		ring:        NewRingBuffer(windowSize),
		windowStart: time.Now(),
	}
}

func (c *Collector) Record(latencyMs float64, isErr bool) {
	atomic.AddInt64(&c.reqTotal, 1)
	c.windowMu.Lock()
	if time.Since(c.windowStart) > time.Minute {
		c.reqWindow = 0
		c.errWindow = 0
		c.windowStart = time.Now()
	}
	c.reqWindow++
	if isErr {
		atomic.AddInt64(&c.errTotal, 1)
		c.errWindow++
	}
	c.windowMu.Unlock()
	c.ring.Push(types.LatencySample{Timestamp: time.Now(), LatencyMs: latencyMs})
}

type Snapshot struct {
	TotalRequests   int64
	TotalErrors     int64
	WindowRequests  int64
	WindowErrors    int64
	AvgLatencyMs    float64
	P99LatencyMs    float64
	WindowDuration  string
}

func (c *Collector) Snapshot(window time.Duration) *Snapshot {
	samples := c.ring.Snapshot(window)
	s := &Snapshot{}
	s.TotalRequests = atomic.LoadInt64(&c.reqTotal)
	s.TotalErrors = atomic.LoadInt64(&c.errTotal)
	c.windowMu.Lock()
	s.WindowRequests = c.reqWindow
	s.WindowErrors = c.errWindow
	s.WindowDuration = time.Since(c.windowStart).Round(time.Second).String()
	c.windowMu.Unlock()
	if len(samples) == 0 {
		return s
	}
	total := 0.0
	latencies := make([]float64, len(samples))
	for i, sm := range samples {
		total += sm.LatencyMs
		latencies[i] = sm.LatencyMs
	}
	s.AvgLatencyMs = total / float64(len(samples))
	sort.Float64s(latencies)
	p99Idx := int(math.Ceil(0.99*float64(len(latencies)))) - 1
	if p99Idx < 0 {
		p99Idx = 0
	}
	if p99Idx >= len(latencies) {
		p99Idx = len(latencies) - 1
	}
	s.P99LatencyMs = latencies[p99Idx]
	return s
}

func (c *Collector) ResetCounters() {
	atomic.StoreInt64(&c.reqTotal, 0)
	atomic.StoreInt64(&c.errTotal, 0)
	c.windowMu.Lock()
	c.reqWindow = 0
	c.errWindow = 0
	c.windowStart = time.Now()
	c.windowMu.Unlock()
}
