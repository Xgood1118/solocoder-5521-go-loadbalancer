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
	muEx sync.Mutex
	cw   []int
	idx  int
}

func NewWeightedRoundRobin(backends []*types.Backend) *WeightedRoundRobin {
	w := &WeightedRoundRobin{baseBalancer: baseBalancer{backends: backends}}
	w.rebuildWeights()
	return w
}

func (w *WeightedRoundRobin) Strategy() types.LoadBalancerStrategy {
	return types.StrategyWeightedRR
}

func (w *WeightedRoundRobin) SetBackends(backends []*types.Backend) {
	w.baseBalancer.SetBackends(backends)
	w.muEx.Lock()
	w.rebuildWeights()
	w.muEx.Unlock()
}

func (w *WeightedRoundRobin) rebuildWeights() {
	w.cw = make([]int, len(w.backends))
	acc := 0
	for i, b := range w.backends {
		acc += b.Weight
		w.cw[i] = acc
	}
	w.idx = 0
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func gcdSlice(arr []int) int {
	result := arr[0]
	for i := 1; i < len(arr); i++ {
		result = gcd(result, arr[i])
		if result == 1 {
			return 1
		}
	}
	return result
}

func (w *WeightedRoundRobin) Next(req *http.Request) (*types.Backend, error) {
	w.muEx.Lock()
	defer w.muEx.Unlock()
	w.mu.RLock()
	available := w.availableLocked()
	w.mu.RUnlock()
	if len(available) == 0 {
		return nil, ErrNoAvailableBackend
	}
	weights := make([]int, len(available))
	for i, b := range available {
		weights[i] = b.Weight
	}
	g := gcdSlice(weights)
	total := 0
	for _, x := range weights {
		total += x
	}
	if total == 0 {
		return available[0], nil
	}
	for {
		w.idx = (w.idx + 1) % len(available)
		if w.idx == 0 {
			w.cw = make([]int, len(available))
			acc := 0
			for i, wt := range weights {
				acc += wt
				w.cw[i] = acc
			}
		}
		step := (w.idx * g) % total
		for i, cw := range w.cw {
			if step < cw {
				if i == w.idx {
					return available[i], nil
				}
				break
			}
		}
		return available[w.idx], nil
	}
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
