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
				l.state.forget(icao)
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
	l.logger.Info("warmed state", "partitions", partitions, "aircraft", l.state.size(), "records", loaded)
	return nil
}

// run folds decoded messages into state and publishes what fold returns: snapshots on change, and on each sweep
// tombstones for expired aircraft and republished snapshots for ones heard but unchanged.
func (l *stateLoop) run(ctx context.Context) error {
	return batchLoop(ctx, l.client, func() time.Duration { return l.state.untilSweep(l.now()) }, func(fetches kgo.Fetches) []*kgo.Record {
		l.mu.Lock()
		defer l.mu.Unlock()
		var batch []decodedIn
		fetches.EachRecord(func(in *kgo.Record) {
			var m wire.Decoded
			if err := json.Unmarshal(in.Value, &m); err != nil {
				l.logger.Warn("skipping decoded record", "partition", in.Partition, "offset", in.Offset, "err", err)
				return
			}
			if m.ICAO == "" {
				return // nothing to key state on
			}
			batch = append(batch, decodedIn{Decoded: m, partition: in.Partition})
		})
		var out []*kgo.Record
		for _, o := range l.state.fold(batch, l.now()) {
			var value []byte
			if o.snap == nil {
				l.logger.Info("expired", "icao", o.icao)
			} else {
				value, _ = json.Marshal(o.snap)
			}
			out = append(out, &kgo.Record{Topic: l.topic, Partition: o.partition, Key: []byte(o.icao), Value: value})
		}
		if len(out) > 0 {
			l.logger.Info("state", "snapshots", len(out), "tracked", l.state.size())
		}
		return out
	})
}
