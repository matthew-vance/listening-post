package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// batchLoop polls, hands each batch to handle, and produces what it returns before committing, so a crash
// re-handles rather than drops. Each poll is bounded by pollTimeout, evaluated per iteration, so handlers with
// periodic work (expiry sweeps) still run on time when nothing arrives. Commit uses a fresh context: ctx may have
// been cancelled mid-produce, and the batch is acked.
func batchLoop(ctx context.Context, client *kgo.Client, pollTimeout func() time.Duration, handle func(kgo.Fetches) []*kgo.Record) error {
	for {
		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout())
		fetches := client.PollFetches(pollCtx)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("poll: %w", err)
		}

		if out := handle(fetches); len(out) > 0 {
			if err := client.ProduceSync(ctx, out...).FirstErr(); err != nil {
				if ctx.Err() != nil {
					return nil // shutting down; uncommitted records are re-handled next start
				}
				return fmt.Errorf("produce to %s: %w", out[0].Topic, err)
			}
		}
		commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := client.CommitUncommittedOffsets(commitCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
		client.AllowRebalance()
	}
}

// decodeLoop decodes every raw record onto out. Lines that don't parse are logged and skipped: the raw archive
// keeps them, and events.decoded is derived.
func decodeLoop(ctx context.Context, client *kgo.Client, out string, logger *slog.Logger) error {
	return batchLoop(ctx, client, func() time.Duration { return time.Minute }, func(fetches kgo.Fetches) []*kgo.Record {
		var recs []*kgo.Record
		skipped := 0
		fetches.EachRecord(func(in *kgo.Record) {
			rec, err := decodeRecord(out, in)
			if err != nil {
				skipped++
				logger.Warn("skipping record", "partition", in.Partition, "offset", in.Offset, "err", err)
				return
			}
			recs = append(recs, rec)
		})
		if len(recs)+skipped > 0 {
			logger.Info("decoded", "in", len(recs)+skipped, "out", len(recs), "skipped", skipped)
		}
		return recs
	})
}
