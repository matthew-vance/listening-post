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

func run(ctx context.Context, getenv func(string) string, stderr io.Writer) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger := log.New(stderr, "", log.LstdFlags)

	// ponytail: PORT is the only config; add a config struct when a second value shows up.
	port := getenv("PORT")
	if port == "" {
		port = "8080"
	}

	r := &readiness{}
	srv := &http.Server{Addr: ":" + port, Handler: NewServer(r)}

	go func() {
		logger.Printf("listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("error listening and serving: %s", err)
		}
	}()
	// ponytail: nothing to wait on yet; flip this after downstream deps connect.
	r.ready.Store(true)

	<-ctx.Done()
	r.shuttingDown.Store(true)
	logger.Print("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown http server: %w", err)
	}
	return nil
}
