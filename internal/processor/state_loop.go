package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type stateLoop struct {
	client *kgo.Client // consumes events.decoded, produces aircraft.state
	seeds  []string
	in     string // events.decoded
	topic  string // aircraft.state
	state  *state
	logger *slog.Logger
	now    func() time.Time

	// mu guards state: rebalance callbacks run concurrently with run whenever the last poll returned nothing,
	// since the BlockRebalanceOnPoll gate only holds after a non-empty poll.
	mu sync.Mutex
}

// onAssigned rebuilds the in-memory picture for newly owned partitions from the compacted topic, so a restart or
// rebalance is invisible to consumers: the first snapshot afterwards still carries everything learned before.
func (l *stateLoop) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.warm(ctx, assigned[l.in]); err != nil {
		l.logger.Error("warm-up", "err", err) // degraded, not fatal: the next message on each field fills it back in
	}
}

// onRevoked forgets aircraft on partitions that now belong to another instance, so this one never expires them.
func (l *stateLoop) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state.drop(revoked[l.in])
}

// warm reads the given partitions of aircraft.state to their end. Snapshots are produced to the same partition
// number their decoded messages arrived on, so partition N of the state topic is exactly what an owner of
// partition N of events.decoded needs.
func (l *stateLoop) warm(ctx context.Context, partitions []int32) error {
	if len(partitions) == 0 {
		return nil
	}
	admin := kadm.NewClient(l.client)
	ends, err := admin.ListEndOffsets(ctx, l.topic)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	remaining := map[int32]int64{}
	offsets := map[int32]kgo.Offset{}
	for _, p := range partitions {
		if o, ok := ends.Lookup(l.topic, p); ok && o.Offset > 0 {
			remaining[p] = o.Offset
			offsets[p] = kgo.NewOffset().AtStart()
		}
	}
	if len(remaining) == 0 {
		return nil
	}

	reader, err := kgo.NewClient(kgo.SeedBrokers(l.seeds...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{l.topic: offsets}))
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
			var snap wire.Snapshot
			if err := json.Unmarshal(r.Value, &snap); err != nil {
				l.logger.Warn("warm-up: bad snapshot", "key", icao, "err", err)
				return
			}
			l.state.restore(snap, r.Partition)
			loaded++
		})
	}
	l.logger.Info("warmed state", "partitions", partitions, "aircraft", len(l.state.aircraft), "records", loaded)
	return nil
}

func (l *stateLoop) record(snap wire.Snapshot, partition int32) *kgo.Record {
	value, _ := json.Marshal(snap)
	return &kgo.Record{Topic: l.topic, Partition: partition, Key: []byte(snap.ICAO), Value: value}
}

// run folds decoded messages into state and publishes a snapshot per change plus tombstones for expired aircraft.
func (l *stateLoop) run(ctx context.Context) error {
	const sweepEvery = 10 * time.Second
	lastSweep := l.now()
	// Anchor the poll deadline to the last sweep rather than the poll start, otherwise a trickle of traffic
	// resets the timer each poll and sweeps slip to ~2x sweepEvery.
	nextSweep := func() time.Duration { return sweepEvery - l.now().Sub(lastSweep) }
	return batchLoop(ctx, l.client, nextSweep, func(fetches kgo.Fetches) []*kgo.Record {
		l.mu.Lock()
		defer l.mu.Unlock()
		var out []*kgo.Record
		fetches.EachRecord(func(in *kgo.Record) {
			var m wire.Decoded
			if err := json.Unmarshal(in.Value, &m); err != nil {
				l.logger.Warn("skipping decoded record", "partition", in.Partition, "offset", in.Offset, "err", err)
				return
			}
			if m.ICAO == "" {
				return // nothing to key state on
			}
			if snap, changed := l.state.apply(m, in.Partition); changed {
				out = append(out, l.record(snap, in.Partition))
			}
		})
		if now := l.now(); now.Sub(lastSweep) >= sweepEvery {
			lastSweep = now
			for _, a := range l.state.expire(now) {
				out = append(out, &kgo.Record{Topic: l.topic, Partition: a.partition, Key: []byte(a.snap.ICAO), Value: nil})
				l.logger.Info("expired", "icao", a.snap.ICAO)
			}
			for _, a := range l.state.unpublished() {
				out = append(out, l.record(a.snap, a.partition))
			}
		}
		if len(out) > 0 {
			l.logger.Info("state", "snapshots", len(out), "tracked", len(l.state.aircraft))
		}
		return out
	})
}
