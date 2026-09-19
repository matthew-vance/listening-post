// Package consume runs the poll → decode → batch → flush → commit loop the Go services share when draining a
// Kafka topic into durable storage (the archiver and the history writer). The offset discipline lives in the
// caller's flush: offsets are committed there, only after a batch is durable, so Batch itself never commits.
package consume

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Batch consumes a topic into batches of decoded records until ctx is cancelled. It polls, decodes each record
// via decode, accumulates until flushRecords records or flushSeconds, then calls flush to store the batch and
// commit its offsets. A non-nil error from decode is fatal: it propagates, naming the record, with the offset
// left uncommitted so the record is re-read on restart rather than dropped. Everything a poll returns is decoded
// before any exit path can commit — a poll is uncommitted the moment it returns.
func Batch[T any](ctx context.Context, client *kgo.Client, flushRecords, flushSeconds int, decode func(*kgo.Record) (T, error), flush func([]T) error) error {
	flushAfter := time.Duration(flushSeconds) * time.Second
	pending := make([]T, 0, flushRecords)
	lastFlush := time.Now()
	for {
		pollCtx, cancel := context.WithDeadline(ctx, lastFlush.Add(flushAfter))
		fetches := client.PollRecords(pollCtx, flushRecords-len(pending))
		cancel()
		if err := collect(fetches, decode, &pending); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return flush(pending)
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("poll: %w", err)
		}
		if len(pending) >= flushRecords || time.Since(lastFlush) >= flushAfter {
			if err := flush(pending); err != nil {
				return err
			}
			pending, lastFlush = pending[:0], time.Now()
		}
	}
}

// collect decodes every record in a poll, stopping at the first decode error so no record past it is acked.
func collect[T any](fetches kgo.Fetches, decode func(*kgo.Record) (T, error), pending *[]T) error {
	var err error
	fetches.EachRecord(func(rec *kgo.Record) {
		if err != nil {
			return
		}
		r, e := decode(rec)
		if e != nil {
			err = fmt.Errorf("decode %s/%d: %w", rec.Topic, rec.Offset, e)
			return
		}
		*pending = append(*pending, r)
	})
	return err
}
