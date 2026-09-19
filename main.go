package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/matthew-vance/listening-post/internal/archiver"
	"github.com/matthew-vance/listening-post/internal/gateway"
	"github.com/matthew-vance/listening-post/internal/history"
	"github.com/matthew-vance/listening-post/internal/pause"
)

type service func(ctx context.Context, logger *slog.Logger) error

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
	archiverGate := pause.New()
	if err := run(ctx, logger, selectedServices(archiverGate)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// selectedServices returns the services this process runs: all of them, unless SERVICES names a subset (a
// comma-separated list). The archiver's pause gate is shared with the gateway so its admin server can pause the
// archiver in-process — that's how the backfill stops the archiver without a separate container.
func selectedServices(archiverGate *pause.Gate) map[string]service {
	all := map[string]service{
		"gateway": svc(gateway.LoadConfig, func(ctx context.Context, cfg gateway.Config, logger *slog.Logger) error {
			return gateway.Run(ctx, cfg, logger, archiverGate)
		}),
		"archiver": svc(archiver.LoadConfig, func(ctx context.Context, cfg archiver.Config, logger *slog.Logger) error {
			return archiver.Run(ctx, cfg, logger, archiverGate)
		}),
		"history": svc(history.LoadConfig, history.Run),
	}
	v := os.Getenv("SERVICES")
	if v == "" {
		return all
	}
	out := make(map[string]service, len(all))
	for _, name := range strings.Split(v, ",") {
		if svc, ok := all[name]; ok {
			out[name] = svc
		}
	}
	return out
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
