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
	err := run(t.Context(), func(string) string { return "" }, slog.New(slog.DiscardHandler), map[string]service{
		"bad": func(context.Context, func(string) string, *slog.Logger) error { return boom },
		"good": func(ctx context.Context, _ func(string) string, _ *slog.Logger) error {
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
