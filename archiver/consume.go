package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type archiver struct {
	client       *kgo.Client
	store        blobStore
	logger       *slog.Logger
	flushRecords int
	flushAfter   time.Duration
}

// run consumes until ctx is cancelled, flushing a batch to Parquet every flushRecords or flushAfter, whichever
// comes first, and committing offsets only after the batch's files are all in place. A final flush runs on exit.
// ponytail: single goroutine, one instance; add per-partition workers when ~33 rec/s becomes thousands.
func (a *archiver) run(ctx context.Context) error {
	var pending []row
	lastFlush := time.Now()

	flush := func() error {
		if len(pending) == 0 {
			lastFlush = time.Now()
			return nil
		}
		files, err := groupAndWrite(ctx, a.logger, a.store, pending)
		if err != nil {
			return err
		}
		// Commit with a fresh context: on shutdown ctx is already cancelled and the files are written; losing the
		// commit here would only mean re-archiving (idempotent), but there's no reason to.
		commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.client.CommitUncommittedOffsets(commitCtx); err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
		a.logger.Info("flushed", "records", len(pending), "files", len(files))
		pending = pending[:0]
		lastFlush = time.Now()
		return nil
	}

	for {
		pollCtx, cancel := context.WithDeadline(ctx, lastFlush.Add(a.flushAfter))
		fetches := a.client.PollRecords(pollCtx, a.flushRecords-len(pending))
		cancel()
		if ctx.Err() != nil {
			return flush()
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("poll: %w", err)
		}
		fetches.EachRecord(func(rec *kgo.Record) { pending = append(pending, decode(rec)) })

		if len(pending) >= a.flushRecords || time.Since(lastFlush) >= a.flushAfter {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}
