package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/loadbalancer/lb/pkg/accesslog"
	"github.com/loadbalancer/lb/pkg/adminapi"
	cb "github.com/loadbalancer/lb/pkg/circuitbreaker"
	"github.com/loadbalancer/lb/pkg/config"
	hc "github.com/loadbalancer/lb/pkg/healthcheck"
	"github.com/loadbalancer/lb/pkg/logger"
	prom "github.com/loadbalancer/lb/pkg/prommetrics"
	"github.com/loadbalancer/lb/pkg/proxy"
	"github.com/loadbalancer/lb/pkg/ratelimit"
	"github.com/loadbalancer/lb/pkg/router"
	"github.com/loadbalancer/lb/pkg/stats"
	"github.com/loadbalancer/lb/pkg/types"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logLevel := os.Getenv("LB_LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	logger.Init(logLevel)

	configPath := os.Getenv("LB_CONFIG")
	if configPath == "" {
		configPath = "config.yaml"
	}

	cfgMgr := config.NewManager(configPath)
	cfg, err := cfgMgr.Load()
	if err != nil {
		logger.Log.Fatal().Err(err).Msg("failed to load config")
	}

	rt := router.New(types.StrategyRoundRobin)
	rt.Rebuild(cfg)

	healthChecker := hc.NewChecker(cfg.HealthCheck)
	for _, b := range cfg.Backends {
		healthChecker.AddBackend(b)
	}

	cbManager := cb.NewManager(cfg.CircuitBreaker)
	rateLimiter := ratelimit.NewLimiter(cfg.RateLimit)
	statsCollector := stats.NewCollector(100000, time.Minute)
	accessLogWriter := accesslog.NewWriter(cfg.AccessLog)

	proxyServer := proxy.NewServer(rt, cbManager, rateLimiter, statsCollector, accessLogWriter)
	adminServer := adminapi.NewServer(rt, healthChecker, cbManager, rateLimiter, statsCollector)

	_ = prom.HTTPRequestsTotal

	port := cfg.Port
	adminPort := cfg.AdminPort
	if envPort := os.Getenv("LB_PORT"); envPort != "" {
		fmt.Sscanf(envPort, "%d", &port)
	}

	mainMux := http.NewServeMux()
	mainMux.HandleFunc("/metrics", promhttp.Handler().ServeHTTP)
	mainMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mainMux.HandleFunc("/", proxyServer.ServeHTTP)

	mainHTTP := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mainMux,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/metrics", promhttp.Handler().ServeHTTP)
	adminMux.Handle("/", adminServer.Handler())

	adminHTTP := &http.Server{
		Addr:              fmt.Sprintf(":%d", adminPort),
		Handler:           adminMux,
		ReadHeaderTimeout: 30 * time.Second,
	}

	go func() {
		logger.Log.Info().Int("port", port).Msg("load balancer starting")
		if err := mainHTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal().Err(err).Msg("main server error")
		}
	}()

	go func() {
		logger.Log.Info().Int("port", adminPort).Msg("admin api starting")
		if err := adminHTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal().Err(err).Msg("admin server error")
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP:
			logger.Log.Info().Msg("SIGHUP received, reloading config")
			newCfg, err := cfgMgr.Reload()
			if err != nil {
				logger.Log.Error().Err(err).Msg("reload failed")
				continue
			}
			rt.Rebuild(newCfg)
			healthChecker.UpdateConfig(newCfg.HealthCheck)
			cbManager.UpdateConfig(newCfg.CircuitBreaker)
			rateLimiter.UpdateConfig(newCfg.RateLimit)
			accessLogWriter.UpdateConfig(newCfg.AccessLog)

			oldIDs := map[string]bool{}
			for _, b := range cfg.Backends {
				oldIDs[b.ID] = true
			}
			newIDs := map[string]bool{}
			for _, b := range newCfg.Backends {
				newIDs[b.ID] = true
				if !oldIDs[b.ID] {
					healthChecker.AddBackend(b)
				}
			}
			for id := range oldIDs {
				if !newIDs[id] {
					healthChecker.RemoveBackend(id)
				}
			}
			cfg = newCfg
			logger.Log.Info().Int64("version", cfgMgr.Version()).Msg("config reloaded successfully")

		case syscall.SIGINT, syscall.SIGTERM:
			logger.Log.Info().Msg("shutdown signal received")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			mainHTTP.Shutdown(shutdownCtx)
			adminHTTP.Shutdown(shutdownCtx)
			healthChecker.StopAll()
			accessLogWriter.Stop()
			logger.Log.Info().Msg("shutdown complete")
			return
		}
	}
}
