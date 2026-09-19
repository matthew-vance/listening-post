package archiver

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
	dir          string
	logger       *slog.Logger
	flushRecords int
	flushAfter   time.Duration
}

// run consumes until ctx is cancelled, flushing a batch to Parquet every flushRecords or flushAfter, whichever
// comes first, and committing offsets only after the batch's files are all in place. A final flush runs on exit.
// Everything a poll returned goes into pending before any exit path can commit: a poll is "uncommitted" the
// moment it returns, so committing without its rows on disk would lose them from the archive for good.
// ponytail: single goroutine, one instance; add per-partition workers when ~33 rec/s becomes thousands.
func (a *archiver) run(ctx context.Context) error {
	pending := make([]row, 0, a.flushRecords)
	lastFlush := time.Now()
	for {
		pollCtx, cancel := context.WithDeadline(ctx, lastFlush.Add(a.flushAfter))
		fetches := a.client.PollRecords(pollCtx, a.flushRecords-len(pending))
		cancel()
		fetches.EachRecord(func(rec *kgo.Record) {
			r, err := decode(rec)
			if err != nil {
				a.logger.Error("archiving undecodable record", "partition", rec.Partition, "offset", rec.Offset, "err", err)
			}
			pending = append(pending, r)
		})
		if ctx.Err() != nil {
			return a.flush(pending)
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("poll: %w", err)
		}

		if len(pending) >= a.flushRecords || time.Since(lastFlush) >= a.flushAfter {
			if err := a.flush(pending); err != nil {
				return err
			}
			pending, lastFlush = pending[:0], time.Now()
		}
	}
}

// flush writes rows to Parquet, then commits their offsets. The commit uses a fresh context: on shutdown ctx is
// already cancelled and the files are written; losing the commit would only mean re-archiving (idempotent), but
// there's no reason to.
func (a *archiver) flush(rows []row) error {
	if len(rows) == 0 {
		return nil
	}
	files, err := groupAndWrite(a.dir, rows)
	if err != nil {
		return err
	}
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.client.CommitUncommittedOffsets(commitCtx); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	a.logger.Info("flushed", "records", len(rows), "files", len(files))
	return nil
}
