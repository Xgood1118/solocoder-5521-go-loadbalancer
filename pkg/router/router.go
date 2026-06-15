package router

import (
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/loadbalancer/lb/pkg/balancer"
	"github.com/loadbalancer/lb/pkg/types"
)

type Router struct {
	mu            sync.RWMutex
	rules         []*types.Rule
	balancers     map[string]balancer.LoadBalancer
	backendsByGrp map[string][]*types.Backend
	defaultStrategy types.LoadBalancerStrategy
}

func New(defaultStrategy types.LoadBalancerStrategy) *Router {
	return &Router{
		balancers:     make(map[string]balancer.LoadBalancer),
		backendsByGrp: make(map[string][]*types.Backend),
		defaultStrategy: defaultStrategy,
	}
}

func (r *Router) Rebuild(cfg *types.Config) {
	backendsByGrp := make(map[string][]*types.Backend)
	for _, b := range cfg.Backends {
		backendsByGrp[b.Group] = append(backendsByGrp[b.Group], b)
	}

	rules := make([]*types.Rule, len(cfg.Rules))
	copy(rules, cfg.Rules)
	sort.SliceStable(rules, func(i, j int) bool {
		return rules[i].Priority > rules[j].Priority
	})

	balancers := make(map[string]balancer.LoadBalancer)
	for _, rule := range rules {
		strategy := rule.Strategy
		if strategy == "" {
			strategy = r.defaultStrategy
		}
		bs := backendsByGrp[rule.BackendGroup]
		balancers[rule.ID] = balancer.New(strategy, bs)
	}

	r.mu.Lock()
	r.rules = rules
	r.balancers = balancers
	r.backendsByGrp = backendsByGrp
	r.mu.Unlock()
}

func (r *Router) SetStrategyForRule(ruleID string, strategy types.LoadBalancerStrategy) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rule := range r.rules {
		if rule.ID == ruleID {
			rule.Strategy = strategy
			bs := r.backendsByGrp[rule.BackendGroup]
			r.balancers[ruleID] = balancer.New(strategy, bs)
			return true
		}
	}
	return false
}

func (r *Router) UpsertBackend(b *types.Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	grp := b.Group
	existing := r.backendsByGrp[grp]
	found := false
	for i, eb := range existing {
		if eb.ID == b.ID {
			existing[i] = b
			found = true
			break
		}
	}
	if !found {
		existing = append(existing, b)
		r.backendsByGrp[grp] = existing
	}
	for _, rule := range r.rules {
		if rule.BackendGroup == grp {
			if lb, ok := r.balancers[rule.ID]; ok {
				lb.SetBackends(existing)
			}
		}
	}
}

func (r *Router) DeleteBackend(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	var grp string
	var found bool
	for g, list := range r.backendsByGrp {
		for i, b := range list {
			if b.ID == id {
				grp = g
				r.backendsByGrp[g] = append(list[:i], list[i+1:]...)
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		return false
	}
	for _, rule := range r.rules {
		if rule.BackendGroup == grp {
			if lb, ok := r.balancers[rule.ID]; ok {
				lb.SetBackends(r.backendsByGrp[grp])
			}
		}
	}
	return true
}

func (r *Router) UpsertRule(rule *types.Rule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	found := false
	for i, er := range r.rules {
		if er.ID == rule.ID {
			r.rules[i] = rule
			found = true
			break
		}
	}
	if !found {
		r.rules = append(r.rules, rule)
	}
	sort.SliceStable(r.rules, func(i, j int) bool {
		return r.rules[i].Priority > r.rules[j].Priority
	})
	bs := r.backendsByGrp[rule.BackendGroup]
	strategy := rule.Strategy
	if strategy == "" {
		strategy = r.defaultStrategy
	}
	r.balancers[rule.ID] = balancer.New(strategy, bs)
}

func (r *Router) DeleteRule(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, er := range r.rules {
		if er.ID == id {
			r.rules = append(r.rules[:i], r.rules[i+1:]...)
			delete(r.balancers, id)
			return true
		}
	}
	return false
}

type MatchResult struct {
	Rule     *types.Rule
	Balancer balancer.LoadBalancer
}

func (r *Router) Match(req *http.Request) (*MatchResult, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	host := req.Host
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}
	path := req.URL.Path
	for _, rule := range r.rules {
		domainOK := rule.Domain == "" || strings.EqualFold(rule.Domain, host)
		pathOK := rule.PathPrefix == "" || strings.HasPrefix(path, rule.PathPrefix)
		if domainOK && pathOK {
			lb, ok := r.balancers[rule.ID]
			if !ok {
				continue
			}
			return &MatchResult{Rule: rule, Balancer: lb}, true
		}
	}
	return nil, false
}

func (r *Router) Rules() []*types.Rule {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*types.Rule, len(r.rules))
	copy(out, r.rules)
	return out
}

func (r *Router) BackendsByGroup(group string) []*types.Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*types.Backend, len(r.backendsByGrp[group]))
	copy(out, r.backendsByGrp[group])
	return out
}

func (r *Router) AllBackends() []*types.Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*types.Backend
	for _, list := range r.backendsByGrp {
		out = append(out, list...)
	}
	return out
}
