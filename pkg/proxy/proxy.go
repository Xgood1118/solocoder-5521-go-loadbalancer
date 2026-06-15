package proxy

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/loadbalancer/lb/pkg/accesslog"
	cb "github.com/loadbalancer/lb/pkg/circuitbreaker"
	"github.com/loadbalancer/lb/pkg/logger"
	prom "github.com/loadbalancer/lb/pkg/prommetrics"
	"github.com/loadbalancer/lb/pkg/ratelimit"
	"github.com/loadbalancer/lb/pkg/router"
	"github.com/loadbalancer/lb/pkg/stats"
	"github.com/loadbalancer/lb/pkg/types"
)

type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
	bytesSent  int64
}

func (w *responseWriterInterceptor) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriterInterceptor) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytesSent += int64(n)
	return n, err
}

type Server struct {
	router          *router.Router
	cbManager       *cb.Manager
	rateLimiter     *ratelimit.Limiter
	statsCollector  *stats.Collector
	accessLogWriter *accesslog.Writer
	reverseProxies  map[string]*httputil.ReverseProxy
}

func NewServer(
	r *router.Router,
	cbm *cb.Manager,
	rl *ratelimit.Limiter,
	sc *stats.Collector,
	aw *accesslog.Writer,
) *Server {
	return &Server{
		router:          r,
		cbManager:       cbm,
		rateLimiter:     rl,
		statsCollector:  sc,
		accessLogWriter: aw,
		reverseProxies:  make(map[string]*httputil.ReverseProxy),
	}
}

func (s *Server) getOrCreateProxy(address string) *httputil.ReverseProxy {
	if p, ok := s.reverseProxies[address]; ok {
		return p
	}
	targetURL := &url.URL{
		Scheme: "http",
		Host:   address,
	}
	rp := httputil.NewSingleHostReverseProxy(targetURL)
	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		logger.Log.Error().Err(err).Str("backend", address).Msg("reverse proxy error")
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
	}
	s.reverseProxies[address] = rp
	return rp
}

func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	host := r.RemoteAddr
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}
	return host
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	clientIP := extractClientIP(r)

	allowed, limit, remaining, resetSec := s.rateLimiter.Allow(clientIP)
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(limit))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	if resetSec > 0 {
		w.Header().Set("X-RateLimit-Reset", strconv.Itoa(resetSec))
	}
	if !allowed {
		prom.RateLimitedTotal.Inc()
		w.Header().Set("Retry-After", strconv.Itoa(resetSec))
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		latencyMs := float64(time.Since(start).Nanoseconds()) / 1e6
		s.statsCollector.Record(latencyMs, true)
		entry := s.accessLogWriter.BuildEntry(r, http.StatusTooManyRequests, 0, latencyMs, "", "")
		s.accessLogWriter.Log(entry)
		return
	}

	match, ok := s.router.Match(r)
	if !ok {
		http.Error(w, "Not Found (no matching rule)", http.StatusNotFound)
		latencyMs := float64(time.Since(start).Nanoseconds()) / 1e6
		s.statsCollector.Record(latencyMs, true)
		entry := s.accessLogWriter.BuildEntry(r, http.StatusNotFound, 0, latencyMs, "", "")
		s.accessLogWriter.Log(entry)
		return
	}

	rule := match.Rule
	lb := match.Balancer

	be, err := lb.Next(r)
	if err != nil {
		http.Error(w, "Service Unavailable (no backend)", http.StatusServiceUnavailable)
		latencyMs := float64(time.Since(start).Nanoseconds()) / 1e6
		s.statsCollector.Record(latencyMs, true)
		prom.HTTPErrorsTotal.WithLabelValues(rule.ID, rule.BackendGroup, "no_backend").Inc()
		entry := s.accessLogWriter.BuildEntry(r, http.StatusServiceUnavailable, 0, latencyMs, "", "")
		s.accessLogWriter.Log(entry)
		return
	}

	if !s.cbManager.Allow(be) {
		prom.HTTPErrorsTotal.WithLabelValues(rule.ID, rule.BackendGroup, "circuit_open").Inc()
		http.Error(w, "Service Unavailable (circuit breaker open)", http.StatusServiceUnavailable)
		latencyMs := float64(time.Since(start).Nanoseconds()) / 1e6
		s.statsCollector.Record(latencyMs, true)
		entry := s.accessLogWriter.BuildEntry(r, http.StatusServiceUnavailable, 0, latencyMs, be.ID, be.Address)
		s.accessLogWriter.Log(entry)
		return
	}

	be.IncConns()
	defer be.DecConns()

	prom.ActiveConnections.WithLabelValues(be.ID, be.Group).Set(float64(be.GetConns()))

	origPath := r.URL.Path
	if rule.StripPrefix && rule.PathPrefix != "" {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, rule.PathPrefix)
		if !strings.HasPrefix(r.URL.Path, "/") {
			r.URL.Path = "/" + r.URL.Path
		}
	}

	iw := &responseWriterInterceptor{ResponseWriter: w, statusCode: http.StatusOK}
	rp := s.getOrCreateProxy(be.Address)

	proxyErr := false
	rp.ServeHTTP(iw, r)
	latencyMs := float64(time.Since(start).Nanoseconds()) / 1e6

	if rule.StripPrefix && rule.PathPrefix != "" {
		r.URL.Path = origPath
	}

	isErr := iw.statusCode >= 500 || proxyErr
	s.statsCollector.Record(latencyMs, isErr)
	prom.HTTPRequestsTotal.WithLabelValues(rule.ID, rule.BackendGroup, fmt.Sprintf("%d", iw.statusCode)).Inc()
	prom.HTTPLatencySeconds.WithLabelValues(rule.ID, rule.BackendGroup).Observe(latencyMs / 1000.0)

	be.Mu.RLock()
	healthy := be.Healthy
	cbState := be.CBState
	be.Mu.RUnlock()

	healthVal := 0.0
	if healthy {
		healthVal = 1.0
	}
	prom.BackendHealth.WithLabelValues(be.ID, be.Group).Set(healthVal)

	var cbVal float64
	switch cbState {
	case types.CircuitClosed:
		cbVal = 0
	case types.CircuitHalfOpen:
		cbVal = 1
	case types.CircuitOpen:
		cbVal = 2
	}
	prom.CircuitBreakerState.WithLabelValues(be.ID, be.Group).Set(cbVal)

	if isErr {
		s.cbManager.RecordFailure(be)
		prom.HTTPErrorsTotal.WithLabelValues(rule.ID, rule.BackendGroup, fmt.Sprintf("status_%d", iw.statusCode)).Inc()
	} else {
		s.cbManager.RecordSuccess(be)
	}

	entry := s.accessLogWriter.BuildEntry(r, iw.statusCode, iw.bytesSent, latencyMs, be.ID, be.Address)
	s.accessLogWriter.Log(entry)
}
