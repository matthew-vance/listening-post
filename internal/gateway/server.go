package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

type readiness struct {
	ready atomic.Bool
}

func newServer(logger *slog.Logger, stations stationLookup, store heartbeatSaver, pub eventPublisher) http.Handler {
	mux := http.NewServeMux()
	auth, timeout := bearerAuth(logger, stations), withTimeout(5*time.Second)
	mux.Handle("POST /v1/events", timeout(auth(handleEventsPost(logger, pub))))
	mux.Handle("POST /v1/stations/heartbeat", timeout(auth(handleHeartbeatPost(logger, store))))
	return mux
}

func newAdminServer(r *readiness, logger *slog.Logger, checks map[string]func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", handleHealthz())
	mux.Handle("GET /readyz", handleReadyz(r, checks))
	return mux
}
