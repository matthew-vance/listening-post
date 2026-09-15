package gateway

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// Run starts the gateway and blocks until ctx is cancelled.
//
// Environment:
//
//	PORT           public API listener (default 8080)
//	ADMIN_PORT     health/readiness listener, internal only (default 9091)
//	DATABASE_URL   Postgres connection URL (required; schema applied by `just migrate`)
//	KAFKA_BROKERS  comma-separated bootstrap brokers (required)
//	KAFKA_RAW      topic events are published to (default events.raw)
func Run(ctx context.Context, getenv func(string) string, logger *slog.Logger) error {
	dbURL := getenv("DATABASE_URL")
	if dbURL == "" {
		return errors.New("DATABASE_URL is not set")
	}
	pool, err := openDB(ctx, dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	logger.Info("connected to database")

	brokers := getenv("KAFKA_BROKERS")
	if brokers == "" {
		return errors.New("KAFKA_BROKERS is not set")
	}
	kafka, err := openKafka(ctx, strings.Split(brokers, ","))
	if err != nil {
		return err
	}
	defer kafka.Close() // after the servers drain: flushes anything still in flight
	logger.Info("connected to kafka")

	pub := &kafkaPublisher{client: kafka, topic: cmp.Or(getenv("KAFKA_RAW"), "events.raw")}
	checks := map[string]func(context.Context) error{"database": pool.Ping, "kafka": kafka.Ping}

	r := &readiness{}
	// ReadHeaderTimeout bounds how long a client can dribble headers before it costs us a goroutine.
	public := &http.Server{Addr: ":" + cmp.Or(getenv("PORT"), "8080"), Handler: newServer(logger, &stationStore{pool: pool}, &heartbeatStore{pool: pool}, pub), ReadHeaderTimeout: 10 * time.Second}
	admin := &http.Server{Addr: ":" + cmp.Or(getenv("ADMIN_PORT"), "9091"), Handler: newAdminServer(r, checks), ReadHeaderTimeout: 10 * time.Second}

	// Bind synchronously so a taken port fails run() outright and ready is only set once both listeners exist.
	errc := make(chan error, 2)
	for name, srv := range map[string]*http.Server{"public": public, "admin": admin} {
		ln, err := net.Listen("tcp", srv.Addr)
		if err != nil {
			return fmt.Errorf("%s: listen: %w", name, err)
		}
		logger.Info("listening", "server", name, "addr", srv.Addr)
		go func() {
			if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("%s: serve: %w", name, err)
			}
		}()
	}
	// Dependencies pinged, listeners bound: ready means what it says. Ongoing health is the checks in /readyz.
	r.ready.Store(true)

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errc: // a dead listener must not leave us running and reporting ready
	}
	r.ready.Store(false)
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Drain public first so /readyz reports 503 to probes for the whole drain window.
	return errors.Join(serveErr, public.Shutdown(shutdownCtx), admin.Shutdown(shutdownCtx))
}
