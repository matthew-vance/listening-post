package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/matthew-vance/listening-post/internal/archiver"
	"github.com/matthew-vance/listening-post/internal/gateway"
	"github.com/matthew-vance/listening-post/internal/history"
)

type service func(ctx context.Context, logger *slog.Logger) error

// services run together in one process, connected through Kafka rather than each other. Each Run documents its
// own environment; the names are disjoint so they can share one. The processor is a Flink job (flink/), not here.
var services = map[string]service{
	"gateway":  svc(gateway.LoadConfig, gateway.Run),
	"archiver": svc(archiver.LoadConfig, archiver.Run),
	"history":  svc(history.LoadConfig, history.Run),
}

func svc[C any](load func(func(string) string) (C, error), run func(context.Context, C, *slog.Logger) error) service {
	return func(ctx context.Context, logger *slog.Logger) error {
		cfg, err := load(os.Getenv)
		if err != nil {
			return err
		}
		return run(ctx, cfg, logger)
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(ctx, logger, services); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run starts every service and blocks until all have returned. The first to return, with an error or on ctx
// cancellation, stops the rest: one dead loop must not leave the process up and half-working.
func run(ctx context.Context, logger *slog.Logger, services map[string]service) error {
	g, ctx := errgroup.WithContext(ctx)
	for name, svc := range services {
		g.Go(func() error {
			if err := svc(ctx, logger.With("service", name)); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	logger.Info("shut down")
	return nil
}
