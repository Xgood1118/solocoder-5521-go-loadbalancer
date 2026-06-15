package balancer

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/loadbalancer/lb/pkg/types"
)

var ErrNoAvailableBackend = errors.New("no available backend")

type LoadBalancer interface {
	Next(r *http.Request) (*types.Backend, error)
	Strategy() types.LoadBalancerStrategy
	SetBackends(backends []*types.Backend)
	GetBackends() []*types.Backend
}

type baseBalancer struct {
	mu       sync.RWMutex
	backends []*types.Backend
}

func (b *baseBalancer) SetBackends(backends []*types.Backend) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.backends = backends
}

func (b *baseBalancer) GetBackends() []*types.Backend {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.backends
}

func (b *baseBalancer) availableLocked() []*types.Backend {
	available := make([]*types.Backend, 0, len(b.backends))
	for _, be := range b.backends {
		be.Mu.RLock()
		ok := be.Healthy && be.CBState != types.CircuitOpen
		conns := be.ActiveConns
		max := int64(be.MaxConns)
		be.Mu.RUnlock()
		if ok && conns < max {
			available = append(available, be)
		}
	}
	return available
}

type RoundRobin struct {
	baseBalancer
	counter uint64
}

func NewRoundRobin(backends []*types.Backend) *RoundRobin {
	return &RoundRobin{baseBalancer: baseBalancer{backends: backends}}
}

func (r *RoundRobin) Strategy() types.LoadBalancerStrategy {
	return types.StrategyRoundRobin
}

func (r *RoundRobin) Next(req *http.Request) (*types.Backend, error) {
	r.mu.RLock()
	available := r.availableLocked()
	r.mu.RUnlock()
	if len(available) == 0 {
		return nil, ErrNoAvailableBackend
	}
	n := atomic.AddUint64(&r.counter, 1) - 1
	return available[int(n%uint64(len(available)))], nil
}

type WeightedRoundRobin struct {
	baseBalancer
	muEx       sync.Mutex
	curWeights []int
}

func NewWeightedRoundRobin(backends []*types.Backend) *WeightedRoundRobin {
	w := &WeightedRoundRobin{
		baseBalancer: baseBalancer{backends: backends},
		curWeights:   make([]int, len(backends)),
	}
	return w
}

func (w *WeightedRoundRobin) Strategy() types.LoadBalancerStrategy {
	return types.StrategyWeightedRR
}

func (w *WeightedRoundRobin) SetBackends(backends []*types.Backend) {
	w.baseBalancer.SetBackends(backends)
	w.muEx.Lock()
	w.curWeights = make([]int, len(backends))
	w.muEx.Unlock()
}

func (w *WeightedRoundRobin) Next(req *http.Request) (*types.Backend, error) {
	w.mu.RLock()
	allBackends := w.backends
	w.mu.RUnlock()

	type entry struct {
		idx     int
		backend *types.Backend
		weight  int
	}
	var candidates []entry
	totalWeight := 0
	for i, be := range allBackends {
		be.Mu.RLock()
		healthy := be.Healthy
		cbOpen := be.CBState == types.CircuitOpen
		conns := be.ActiveConns
		max := int64(be.MaxConns)
		wt := be.Weight
		be.Mu.RUnlock()
		if healthy && !cbOpen && conns < max {
			candidates = append(candidates, entry{idx: i, backend: be, weight: wt})
			totalWeight += wt
		}
	}

	if len(candidates) == 0 {
		return nil, ErrNoAvailableBackend
	}
	if totalWeight <= 0 {
		return candidates[0].backend, nil
	}

	w.muEx.Lock()
	defer w.muEx.Unlock()

	if len(w.curWeights) != len(allBackends) {
		w.curWeights = make([]int, len(allBackends))
	}

	bestIdx := -1
	bestWeight := -1
	for _, c := range candidates {
		w.curWeights[c.idx] += c.weight
		if w.curWeights[c.idx] > bestWeight {
			bestWeight = w.curWeights[c.idx]
			bestIdx = c.idx
		}
	}
	if bestIdx >= 0 {
		w.curWeights[bestIdx] -= totalWeight
	}

	for _, c := range candidates {
		if c.idx == bestIdx {
			return c.backend, nil
		}
	}
	return candidates[0].backend, nil
}

type LeastConnections struct {
	baseBalancer
}

func NewLeastConnections(backends []*types.Backend) *LeastConnections {
	return &LeastConnections{baseBalancer: baseBalancer{backends: backends}}
}

func (l *LeastConnections) Strategy() types.LoadBalancerStrategy {
	return types.StrategyLeastConn
}

func (l *LeastConnections) Next(req *http.Request) (*types.Backend, error) {
	l.mu.RLock()
	available := l.availableLocked()
	l.mu.RUnlock()
	if len(available) == 0 {
		return nil, ErrNoAvailableBackend
	}
	var best *types.Backend
	var bestConns int64 = math.MaxInt64
	for _, be := range available {
		c := be.GetConns()
		if c < bestConns {
			bestConns = c
			best = be
		}
	}
	if best == nil {
		return available[0], nil
	}
	return best, nil
}

type IPHash struct {
	baseBalancer
}

func NewIPHash(backends []*types.Backend) *IPHash {
	return &IPHash{baseBalancer: baseBalancer{backends: backends}}
}

func (i *IPHash) Strategy() types.LoadBalancerStrategy {
	return types.StrategyIPHash
}

func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (i *IPHash) Next(req *http.Request) (*types.Backend, error) {
	i.mu.RLock()
	available := i.availableLocked()
	i.mu.RUnlock()
	if len(available) == 0 {
		return nil, ErrNoAvailableBackend
	}
	ip := extractClientIP(req)
	h := fnv.New32a()
	h.Write([]byte(ip))
	sum := h.Sum32()
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, sum)
	h2 := fnv.New32a()
	h2.Write(buf)
	hash := h2.Sum32()
	idx := int(hash % uint32(len(available)))
	return available[idx], nil
}

func New(strategy types.LoadBalancerStrategy, backends []*types.Backend) LoadBalancer {
	switch strategy {
	case types.StrategyWeightedRR:
		return NewWeightedRoundRobin(backends)
	case types.StrategyLeastConn:
		return NewLeastConnections(backends)
	case types.StrategyIPHash:
		return NewIPHash(backends)
	default:
		return NewRoundRobin(backends)
	}
}
