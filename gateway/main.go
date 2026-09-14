package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
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
//	PORT        public API listener (default 8080)
//	ADMIN_PORT  health/readiness listener, internal only (default 9091)
func run(ctx context.Context, getenv func(string) string, stderr io.Writer) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger := log.New(stderr, "", log.LstdFlags)

	// ponytail: two env vars; add a config struct when there's a third.
	envOr := func(key, def string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return def
	}

	r := &readiness{}
	public := &http.Server{Addr: ":" + envOr("PORT", "8080"), Handler: NewServer(logger)}
	admin := &http.Server{Addr: ":" + envOr("ADMIN_PORT", "9091"), Handler: NewAdminServer(r)}

	for name, srv := range map[string]*http.Server{"public": public, "admin": admin} {
		go func() {
			logger.Printf("%s listening on %s", name, srv.Addr)
			if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				logger.Printf("%s: error listening and serving: %s", name, err)
			}
		}()
	}
	// ponytail: nothing to wait on yet; flip this after downstream deps connect.
	r.ready.Store(true)

	<-ctx.Done()
	r.shuttingDown.Store(true)
	logger.Print("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Drain public first so /readyz reports 503 to probes for the whole drain window.
	return errors.Join(public.Shutdown(shutdownCtx), admin.Shutdown(shutdownCtx))
}
