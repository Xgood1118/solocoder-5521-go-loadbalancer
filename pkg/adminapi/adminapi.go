package adminapi

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	cb "github.com/loadbalancer/lb/pkg/circuitbreaker"
	hc "github.com/loadbalancer/lb/pkg/healthcheck"
	"github.com/loadbalancer/lb/pkg/logger"
	"github.com/loadbalancer/lb/pkg/ratelimit"
	"github.com/loadbalancer/lb/pkg/router"
	"github.com/loadbalancer/lb/pkg/stats"
	"github.com/loadbalancer/lb/pkg/types"
)

type Server struct {
	router         *router.Router
	healthChecker  *hc.Checker
	cbManager      *cb.Manager
	rateLimiter    *ratelimit.Limiter
	statsCollector *stats.Collector
}

func NewServer(
	r *router.Router,
	hcc *hc.Checker,
	cbm *cb.Manager,
	rl *ratelimit.Limiter,
	sc *stats.Collector,
) *Server {
	return &Server{
		router:         r,
		healthChecker:  hcc,
		cbManager:      cbm,
		rateLimiter:    rl,
		statsCollector: sc,
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

type errorResp struct {
	Error string `json:"error"`
}

func (s *Server) listBackends(w http.ResponseWriter, r *http.Request) {
	bs := s.router.AllBackends()
	if q := r.URL.Query().Get("group"); q != "" {
		bs = s.router.BackendsByGroup(q)
	}
	sort.Slice(bs, func(i, j int) bool { return bs[i].ID < bs[j].ID })
	writeJSON(w, http.StatusOK, bs)
}

func (s *Server) addBackend(w http.ResponseWriter, r *http.Request) {
	var b types.Backend
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResp{err.Error()})
		return
	}
	if b.ID == "" || b.Address == "" || b.Group == "" {
		writeJSON(w, http.StatusBadRequest, errorResp{"id, address, group required"})
		return
	}
	if b.Weight <= 0 {
		b.Weight = 1
	}
	if b.MaxConns <= 0 {
		b.MaxConns = 1000
	}
	b.Healthy = true
	b.CBState = types.CircuitClosed
	s.router.UpsertBackend(&b)
	s.healthChecker.AddBackend(&b)
	logger.RecordAudit("admin-api", "add_backend", b.ID+"/"+b.Address)
	writeJSON(w, http.StatusCreated, b)
}

func (s *Server) deleteBackend(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errorResp{"id required"})
		return
	}
	s.healthChecker.RemoveBackend(id)
	if ok := s.router.DeleteBackend(id); !ok {
		writeJSON(w, http.StatusNotFound, errorResp{"backend not found"})
		return
	}
	logger.RecordAudit("admin-api", "delete_backend", id)
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "id": id})
}

func (s *Server) listRules(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.router.Rules())
}

func (s *Server) addRule(w http.ResponseWriter, r *http.Request) {
	var rule types.Rule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResp{err.Error()})
		return
	}
	if rule.ID == "" || rule.BackendGroup == "" {
		writeJSON(w, http.StatusBadRequest, errorResp{"id, backend_group required"})
		return
	}
	if rule.Strategy == "" {
		rule.Strategy = types.StrategyRoundRobin
	}
	s.router.UpsertRule(&rule)
	logger.RecordAudit("admin-api", "add_rule", rule.ID)
	writeJSON(w, http.StatusCreated, rule)
}

func (s *Server) deleteRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errorResp{"id required"})
		return
	}
	if ok := s.router.DeleteRule(id); !ok {
		writeJSON(w, http.StatusNotFound, errorResp{"rule not found"})
		return
	}
	logger.RecordAudit("admin-api", "delete_rule", id)
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "id": id})
}

type setStrategyReq struct {
	Strategy types.LoadBalancerStrategy `json:"strategy"`
}

func (s *Server) setStrategy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errorResp{"rule id required"})
		return
	}
	var req setStrategyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResp{err.Error()})
		return
	}
	switch req.Strategy {
	case types.StrategyRoundRobin, types.StrategyWeightedRR, types.StrategyLeastConn, types.StrategyIPHash:
	default:
		writeJSON(w, http.StatusBadRequest, errorResp{"invalid strategy"})
		return
	}
	if ok := s.router.SetStrategyForRule(id, req.Strategy); !ok {
		writeJSON(w, http.StatusNotFound, errorResp{"rule not found"})
		return
	}
	logger.RecordAudit("admin-api", "set_strategy", id+"/"+string(req.Strategy))
	writeJSON(w, http.StatusOK, map[string]interface{}{"rule_id": id, "strategy": req.Strategy})
}

func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	window := 5 * time.Minute
	if ws := r.URL.Query().Get("window"); ws != "" {
		if d, err := time.ParseDuration(ws); err == nil {
			window = d
		}
	}
	snap := s.statsCollector.Snapshot(window)
	var activeConnsTotal int64
	backends := s.router.AllBackends()
	for _, b := range backends {
		activeConnsTotal += b.GetConns()
	}
	resp := map[string]interface{}{
		"window":            window.String(),
		"total_requests":    snap.TotalRequests,
		"total_errors":      snap.TotalErrors,
		"window_requests":   snap.WindowRequests,
		"window_errors":     snap.WindowErrors,
		"avg_latency_ms":    snap.AvgLatencyMs,
		"p99_latency_ms":    snap.P99LatencyMs,
		"active_connections": activeConnsTotal,
		"backend_count":     len(backends),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /backends", s.listBackends)
	mux.HandleFunc("POST /backends", s.addBackend)
	mux.HandleFunc("DELETE /backends/{id}", s.deleteBackend)
	mux.HandleFunc("GET /rules", s.listRules)
	mux.HandleFunc("POST /rules", s.addRule)
	mux.HandleFunc("DELETE /rules/{id}", s.deleteRule)
	mux.HandleFunc("POST /rules/{id}/strategy", s.setStrategy)
	mux.HandleFunc("GET /stats", s.getStats)
	return mux
}
