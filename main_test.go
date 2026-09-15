package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
)

func TestRunStopsAllWhenOneFails(t *testing.T) {
	boom := errors.New("boom")
	var stopped bool
	err := run(t.Context(), slog.New(slog.DiscardHandler), map[string]service{
		"bad": func(context.Context, *slog.Logger) error { return boom },
		"good": func(ctx context.Context, _ *slog.Logger) error {
			<-ctx.Done() // blocks forever unless the failure cancels it
			stopped = true
			return nil
		},
	})
	if !errors.Is(err, boom) || err.Error() != "bad: boom" {
		t.Fatalf("err = %v, want bad: boom", err)
	}
	if !stopped {
		t.Fatal("good service was not stopped")
	}
}
