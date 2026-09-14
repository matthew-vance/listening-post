package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type processor struct {
	client *kgo.Client // consumes in and produces out; one client keeps the code and the config small
	out    string
	logger *slog.Logger
}

// run decodes every fetched batch and produces it before committing, so a crash re-decodes rather than drops.
// Lines that don't parse are logged and skipped: the raw archive keeps them, and events.decoded is derived.
func (p *processor) run(ctx context.Context) error {
	for {
		fetches := p.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("poll: %w", err)
		}

		var out []*kgo.Record
		skipped := 0
		fetches.EachRecord(func(in *kgo.Record) {
			rec, err := decodeRecord(p.out, in)
			if err != nil {
				skipped++
				p.logger.Warn("skipping record", "partition", in.Partition, "offset", in.Offset, "err", err)
				return
			}
			out = append(out, rec)
		})
		if len(out) > 0 {
			if err := p.client.ProduceSync(ctx, out...).FirstErr(); err != nil {
				if ctx.Err() != nil {
					return nil // shutting down; uncommitted records are re-decoded next start
				}
				return fmt.Errorf("produce to %s: %w", p.out, err)
			}
		}
		// Fresh context: ctx may have been cancelled while producing, and the batch is acked, so commit it.
		commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := p.client.CommitUncommittedOffsets(commitCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
		p.logger.Info("decoded", "in", len(out)+skipped, "out", len(out), "skipped", skipped)
	}
}
