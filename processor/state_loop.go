package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type stateLoop struct {
	client *kgo.Client // consumes events.decoded, produces aircraft.state
	topic  string      // aircraft.state
	state  *state
	logger *slog.Logger
	now    func() time.Time
}

// warm rebuilds the in-memory picture from the compacted topic so a restart is invisible to consumers:
// the first snapshot after restart still carries everything learned before it.
// ponytail: reads the whole topic; partition-aware warm-up when there are several processor instances.
func (l *stateLoop) warm(ctx context.Context, brokers []string) error {
	admin := kadm.NewClient(l.client)
	ends, err := admin.ListEndOffsets(ctx, l.topic)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	remaining := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			remaining[o.Partition] = o.Offset
		}
	})
	if len(remaining) == 0 {
		return nil
	}

	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(l.topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return fmt.Errorf("warm-up client: %w", err)
	}
	defer reader.Close()

	loaded := 0
	for len(remaining) > 0 {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("warm-up poll: %w", err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset+1 >= remaining[r.Partition] {
				delete(remaining, r.Partition)
			}
			icao := string(r.Key)
			if r.Value == nil { // tombstone
				delete(l.state.aircraft, icao)
				return
			}
			var snap snapshot
			if err := json.Unmarshal(r.Value, &snap); err != nil {
				l.logger.Warn("warm-up: bad snapshot", "key", icao, "err", err)
				return
			}
			a := newAircraft(icao)
			a.snap = snap
			// Every field is at least as new as last_seen's message; exact per-field times aren't persisted.
			for _, f := range []string{"callsign", "altitude", "ground_speed", "track", "lat", "lon", "vertical_rate", "squawk", "alert", "emergency", "spi", "on_ground", "position_ts"} {
				a.fieldTS[f] = snap.LastSeen
			}
			l.state.aircraft[icao] = a
			loaded++
		})
	}
	l.logger.Info("warmed state", "aircraft", len(l.state.aircraft), "records", loaded)
	return nil
}

// run folds decoded messages into state and publishes a snapshot per change plus tombstones for expired aircraft.
func (l *stateLoop) run(ctx context.Context) error {
	const sweepEvery = 10 * time.Second
	lastSweep := l.now()
	for {
		// Bounded poll so expiry sweeps happen even when the sky is quiet and nothing arrives.
		pollCtx, cancel := context.WithDeadline(ctx, lastSweep.Add(sweepEvery))
		fetches := l.client.PollFetches(pollCtx)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("poll: %w", err)
		}

		var out []*kgo.Record
		fetches.EachRecord(func(in *kgo.Record) {
			var m decodedRecord
			if err := json.Unmarshal(in.Value, &m); err != nil {
				l.logger.Warn("skipping decoded record", "partition", in.Partition, "offset", in.Offset, "err", err)
				return
			}
			if m.ICAO == "" {
				return // nothing to key state on
			}
			if snap, changed := l.state.apply(m); changed {
				value, _ := json.Marshal(snap)
				out = append(out, &kgo.Record{Topic: l.topic, Key: []byte(m.ICAO), Value: value})
			}
		})
		if now := l.now(); now.Sub(lastSweep) >= sweepEvery {
			lastSweep = now
			for _, icao := range l.state.expire(now) {
				out = append(out, &kgo.Record{Topic: l.topic, Key: []byte(icao), Value: nil})
				l.logger.Info("expired", "icao", icao)
			}
		}

		if len(out) > 0 {
			if err := l.client.ProduceSync(ctx, out...).FirstErr(); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("produce to %s: %w", l.topic, err)
			}
		}
		commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := l.client.CommitUncommittedOffsets(commitCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
		if len(out) > 0 {
			l.logger.Info("state", "snapshots", len(out), "tracked", len(l.state.aircraft))
		}
	}
}
