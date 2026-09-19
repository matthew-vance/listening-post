package history

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Run persists aircraft Traces to Timescale until ctx is cancelled.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()
	logger.Info("connected to database")

	client, err := wire.OpenKafka(ctx, cfg.KafkaBrokers,
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.KafkaTopic),
		kgo.DisableAutoCommit(), // offsets advance only once rows are committed
	)
	if err != nil {
		return err
	}
	defer client.Close()
	logger.Info("persisting", "topic", cfg.KafkaTopic, "group", cfg.Group, "flush_records", cfg.FlushRecords, "flush_seconds", cfg.FlushSeconds)

	w := &writer{client: client, pool: pool, logger: logger, Config: cfg}
	err = w.run(ctx)
	logger.Info("shut down")
	return err
}

type writer struct {
	client *kgo.Client
	pool   *pgxpool.Pool
	logger *slog.Logger
	Config
}

// run consumes until ctx is cancelled, inserting a batch every flushRecords or flushAfter, whichever comes
// first, and committing offsets only after the batch's rows are all in the table. As with the archiver, a poll
// is uncommitted the moment it returns, so committing without its rows in the table would lose them for good.
func (w *writer) run(ctx context.Context) error {
	flushAfter := time.Duration(w.FlushSeconds) * time.Second
	pending := make([]traceRow, 0, w.FlushRecords)
	lastFlush := time.Now()
	for {
		pollCtx, cancel := context.WithDeadline(ctx, lastFlush.Add(flushAfter))
		fetches := w.client.PollRecords(pollCtx, w.FlushRecords-len(pending))
		cancel()
		fetches.EachRecord(func(rec *kgo.Record) {
			r, err := decodeTrace(rec.Value)
			if err != nil {
				w.logger.Error("skipping undecodable trace", "partition", rec.Partition, "offset", rec.Offset, "err", err)
				return
			}
			pending = append(pending, r)
		})
		if ctx.Err() != nil {
			return w.flush(pending)
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("poll: %w", err)
		}

		if len(pending) >= w.FlushRecords || time.Since(lastFlush) >= flushAfter {
			if err := w.flush(pending); err != nil {
				return err
			}
			pending, lastFlush = pending[:0], time.Now()
		}
	}
}

// flush inserts rows, then commits their offsets. A crash between the two re-reads the batch, and the idempotency
// key makes the re-insert a no-op. The commit uses a fresh context: on shutdown ctx is already cancelled and the
// rows are inserted; losing the commit would only mean re-reading (idempotent).
func (w *writer) flush(rows []traceRow) error {
	if len(rows) == 0 {
		return nil
	}
	if err := w.insert(rows); err != nil {
		return err
	}
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.client.CommitUncommittedOffsets(commitCtx); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	w.logger.Info("flushed", "rows", len(rows))
	return nil
}
