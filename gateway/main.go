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
//	STATIONS_FILE  JSON map of station name → sha256(token) hex (default stations.json)
func run(ctx context.Context, getenv func(string) string, stderr io.Writer) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(stderr, nil))

	reg, err := loadStations(cmp.Or(getenv("STATIONS_FILE"), "stations.json"))
	if err != nil {
		return err
	}
	logger.Info("loaded stations", "count", len(reg))

	r := &readiness{}
	public := &http.Server{Addr: ":" + cmp.Or(getenv("PORT"), "8080"), Handler: newServer(logger, reg)}
	admin := &http.Server{Addr: ":" + cmp.Or(getenv("ADMIN_PORT"), "9091"), Handler: newAdminServer(r)}

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
