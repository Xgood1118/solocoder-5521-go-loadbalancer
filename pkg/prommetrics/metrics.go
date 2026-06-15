package prommetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lb_http_requests_total",
			Help: "Total number of HTTP requests processed by the load balancer.",
		},
		[]string{"rule_id", "backend_group", "status_code"},
	)
	HTTPErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lb_http_errors_total",
			Help: "Total number of HTTP request errors.",
		},
		[]string{"rule_id", "backend_group", "error_type"},
	)
	HTTPLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lb_http_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"rule_id", "backend_group"},
	)
	ActiveConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lb_backend_active_connections",
			Help: "Current active connections to each backend.",
		},
		[]string{"backend_id", "backend_group"},
	)
	BackendHealth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lb_backend_health",
			Help: "Backend health status (1 = healthy, 0 = unhealthy).",
		},
		[]string{"backend_id", "backend_group"},
	)
	CircuitBreakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lb_circuit_breaker_state",
			Help: "Circuit breaker state (0=closed, 1=half_open, 2=open).",
		},
		[]string{"backend_id", "backend_group"},
	)
	RateLimitedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "lb_rate_limited_requests_total",
			Help: "Total number of rate-limited requests (HTTP 429).",
		},
	)
)
