package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/matthew-vance/listening-post/internal/archiver"
	"github.com/matthew-vance/listening-post/internal/gateway"
	"github.com/matthew-vance/listening-post/internal/processor"
)

type service func(ctx context.Context, getenv func(string) string, logger *slog.Logger) error

// services run together in one process, connected through Kafka rather than each other. Each Run documents its
// own environment; the names are disjoint so they can share one.
var services = map[string]service{
	"gateway":   gateway.Run,
	"processor": processor.Run,
	"archiver":  archiver.Run,
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(ctx, os.Getenv, logger, services); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run starts every service and blocks until all have returned. The first to return, with an error or on ctx
// cancellation, stops the rest: one dead loop must not leave the process up and half-working.
func run(ctx context.Context, getenv func(string) string, logger *slog.Logger, services map[string]service) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, len(services))
	for name, svc := range services {
		go func() {
			err := svc(ctx, getenv, logger.With("service", name))
			cancel()
			if err != nil {
				err = fmt.Errorf("%s: %w", name, err)
			}
			errc <- err
		}()
	}
	var errs []error
	for range services {
		errs = append(errs, <-errc)
	}
	logger.Info("shut down")
	return errors.Join(errs...)
}
