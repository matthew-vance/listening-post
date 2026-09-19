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

// ErrPaused is returned by Batch when its pause function reports true: the batch has been flushed and offsets
// committed, and the caller should stop consuming until it is resumed.
var ErrPaused = errors.New("paused")

// Batch consumes a topic into batches of decoded records until ctx is cancelled. It polls, decodes each record
// via decode, accumulates until flushRecords records or flushSeconds, then calls flush to store the batch and
// commit its offsets. A non-nil error from decode is fatal: it propagates, naming the record, with the offset
// left uncommitted so the record is re-read on restart rather than dropped. Everything a poll returns is decoded
// before any exit path can commit — a poll is uncommitted the moment it returns.
//
// pause, if non-nil, is checked before each poll: when it reports true, any pending rows are flushed and Batch
// returns ErrPaused, so the caller can release the consumer and wait to be resumed.
func Batch[T any](ctx context.Context, client *kgo.Client, flushRecords, flushSeconds int, decode func(*kgo.Record) (T, error), flush func([]T) error, pause func() bool) error {
	flushAfter := time.Duration(flushSeconds) * time.Second
	pending := make([]T, 0, flushRecords)
	lastFlush := time.Now()
	for {
		if pause != nil && pause() {
			if err := flush(pending); err != nil {
				return err
			}
			return ErrPaused
		}
		deadline := lastFlush.Add(flushAfter)
		if pause != nil {
			// Tick the poll once a second so a pause is noticed promptly instead of only when the flush
			// deadline (or the next record) arrives.
			if d := time.Now().Add(time.Second); d.Before(deadline) {
				deadline = d
			}
		}
		pollCtx, cancel := context.WithDeadline(ctx, deadline)
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
