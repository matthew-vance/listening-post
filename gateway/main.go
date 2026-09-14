package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Getenv, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run starts the gateway and blocks until ctx is cancelled or SIGINT/SIGTERM.
//
// Environment:
//
//	PORT           public API listener (default 8080)
//	ADMIN_PORT     health/readiness listener, internal only (default 9091)
//	DATABASE_URL   Postgres connection URL (required; schema applied by `just migrate`)
//	KAFKA_BROKERS  comma-separated bootstrap brokers (required)
//	KAFKA_TOPIC    topic events are published to (default events.raw)
func run(ctx context.Context, getenv func(string) string, stderr io.Writer) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(stderr, nil))

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

	pub := &kafkaPublisher{client: kafka, topic: cmp.Or(getenv("KAFKA_TOPIC"), "events.raw")}
	checks := map[string]func(context.Context) error{"database": pool.Ping, "kafka": kafka.Ping}

	r := &readiness{}
	public := &http.Server{Addr: ":" + cmp.Or(getenv("PORT"), "8080"), Handler: newServer(logger, &stationStore{pool: pool}, &heartbeatStore{pool: pool}, pub)}
	admin := &http.Server{Addr: ":" + cmp.Or(getenv("ADMIN_PORT"), "9091"), Handler: newAdminServer(r, checks)}

	errc := make(chan error, 2)
	for name, srv := range map[string]*http.Server{"public": public, "admin": admin} {
		go func() {
			logger.Info("listening", "server", name, "addr", srv.Addr)
			if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("%s: listen and serve: %w", name, err)
			}
		}()
	}
	// ponytail: nothing to wait on yet; flip this after downstream deps connect.
	r.ready.Store(true)

	var listenErr error
	select {
	case <-ctx.Done():
	case listenErr = <-errc: // a dead listener must not leave us running and reporting ready
	}
	r.shuttingDown.Store(true)
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Drain public first so /readyz reports 503 to probes for the whole drain window.
	return errors.Join(listenErr, public.Shutdown(shutdownCtx), admin.Shutdown(shutdownCtx))
}
