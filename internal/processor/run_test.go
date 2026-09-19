package processor

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/matthew-vance/listening-post/internal/kafkatest"
	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// testConfig makes fresh topics with the given partition count and a Config wired to them.
func testConfig(t *testing.T, partitions int32, expireSeconds int) Config {
	t.Helper()
	in := kafkatest.TopicN(t, partitions)
	return Config{
		KafkaBrokers:        kafkatest.Brokers,
		KafkaRaw:            in,
		KafkaDecoded:        kafkatest.TopicN(t, partitions),
		ProcessorGroup:      "g_" + in,
		KafkaState:          kafkatest.Compacted(t, partitions),
		ProcessorStateGroup: "gs_" + in,
		ExpireSeconds:       expireSeconds,
	}
}

// rawRecords wraps SBS lines as gateway events from one station, ids in order.
func rawRecords(topic string, raws ...string) []*kgo.Record {
	var records []*kgo.Record
	for i, raw := range raws {
		v, _ := json.Marshal(wire.Event{StationID: "st", ID: int64(i), TS: time.Now().UTC(), Raw: raw, ReceivedAt: time.Now().UTC()})
		records = append(records, &kgo.Record{Topic: topic, Key: []byte("st"), Value: v})
	}
	return records
}

func TestRunDecodesTopic(t *testing.T) {
	cfg := testConfig(t, 1, 1)
	in, out, stateTopic := cfg.KafkaRaw, cfg.KafkaDecoded, cfg.KafkaState
	lines := []string{
		"MSG,1,1,1,A4BF41,1,2026/09/14,16:05:25.403,2026/09/14,16:05:25.428,AAL433  ,,,,,,,,,,,0",
		"MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0",
		"not an sbs line",
		"MSG,4,1,1,A51958,1,2026/09/14,15:35:33.455,2026/09/14,15:35:33.505,,,505,89,,,-64,,,,,0",
		"MSG,8,1,1,AB197E,1,2026/09/14,16:05:23.670,2026/09/14,16:05:23.684,,,,,,,,,,,,0",
	}
	kafkatest.Produce(t, rawRecords(in, lines...)...)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, slog.New(slog.DiscardHandler)) }()

	got := kafkatest.Consume(t, out, 4) // the unparseable line is skipped

	// state: one snapshot per aircraft with fields merged from its messages, keyed by ICAO
	snaps := kafkatest.Consume(t, stateTopic, 4)
	byICAO := map[string]map[string]any{}
	for _, rec := range snaps {
		var snap map[string]any
		if err := json.Unmarshal(rec.Value, &snap); err != nil {
			t.Fatal(err)
		}
		byICAO[string(rec.Key)] = snap
	}
	if byICAO["A4BF41"]["callsign"] != "AAL433" || byICAO["A22123"]["altitude"] != float64(8275) || byICAO["A51958"]["ground_speed"] != float64(505) {
		t.Fatalf("snapshots = %v", byICAO)
	}

	// expiry: with EXPIRE_SECONDS=1 the sweep tombstones every aircraft
	var tombstones int
	deadline := time.Now().Add(20 * time.Second)
	for tombstones < 4 && time.Now().Before(deadline) {
		tombstones = 0
		for _, rec := range kafkatest.ConsumeUpTo(t, stateTopic, 100, 2*time.Second) {
			if rec.Value == nil {
				tombstones++
			}
		}
	}
	if tombstones < 4 {
		t.Fatalf("tombstones = %d, want 4", tombstones)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	wantKeys := []string{"A4BF41", "A22123", "A51958", "AB197E"}
	for i, rec := range got {
		if string(rec.Key) != wantKeys[i] {
			t.Fatalf("record %d key = %q, want %q", i, rec.Key, wantKeys[i])
		}
		var d map[string]any
		if err := json.Unmarshal(rec.Value, &d); err != nil {
			t.Fatal(err)
		}
		if d["station_id"] != "st" || d["icao"] != wantKeys[i] {
			t.Fatalf("record %d = %s", i, rec.Value)
		}
	}

	// offsets committed: a second run sees nothing new (nothing more lands on out within the window)
	ctx2, cancel2 := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel2()
	if err := Run(ctx2, cfg, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	if extra := len(kafkatest.ConsumeUpTo(t, out, 5, 3*time.Second)); extra != 4 {
		t.Fatalf("after second run: %d records on out, want 4", extra)
	}
}

// TestRunRequiresTopics: a state topic that can't take the decoded topic's partitions, or isn't compacted, fails
// Run before anything is consumed.
func TestRunRequiresTopics(t *testing.T) {
	for name, cfg := range map[string]func(*testing.T) Config{
		"fewer partitions": func(t *testing.T) Config {
			c := testConfig(t, 2, 300)
			c.KafkaState = kafkatest.Compacted(t, 1)
			return c
		},
		"not compacted": func(t *testing.T) Config { c := testConfig(t, 1, 300); c.KafkaState = kafkatest.TopicN(t, 1); return c },
	} {
		t.Run(name, func(t *testing.T) { // the subtest's t: kafkatest skips in -short, and Skip must hit the caller
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			err := Run(ctx, cfg(t), slog.New(slog.DiscardHandler))
			if err == nil || ctx.Err() != nil {
				t.Fatalf("run = %v, want a topic error before the timeout", err)
			}
			t.Log(err)
		})
	}
}

func TestStateWarmsFromCompactedTopic(t *testing.T) {
	cfg := testConfig(t, 1, 3600)
	stateTopic := cfg.KafkaState
	produceRaw := func(raws ...string) {
		t.Helper()
		kafkatest.Produce(t, rawRecords(cfg.KafkaRaw, raws...)...)
	}
	runFor := func(want int) []*kgo.Record {
		t.Helper()
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- Run(ctx, cfg, slog.New(slog.DiscardHandler)) }()
		recs := kafkatest.Consume(t, stateTopic, want)
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		return recs
	}

	// first run learns the callsign only
	produceRaw("MSG,1,1,1,A22123,1,2026/09/14,16:05:25.403,2026/09/14,16:05:25.428,AAL433  ,,,,,,,,,,,0")
	runFor(1)

	// second run sees only a position, but its snapshot must still carry the callsign learned before the restart
	produceRaw("MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0")
	recs := runFor(2)
	var snap map[string]any
	if err := json.Unmarshal(recs[1].Value, &snap); err != nil {
		t.Fatal(err)
	}
	if snap["callsign"] != "AAL433" || snap["altitude"] != float64(8275) {
		t.Fatalf("snapshot after restart = %s", recs[1].Value)
	}
}

// TestStateSurvivesRebalance runs two instances against 2-partition topics: the second joining moves a partition,
// and the first must forget those aircraft rather than expire them out from under the new owner.
func TestStateSurvivesRebalance(t *testing.T) {
	cfg := testConfig(t, 2, 2)
	out, stateTopic := cfg.KafkaDecoded, cfg.KafkaState
	icaos := []string{"A00001", "A00002", "A00003", "A00004", "A00005", "A00006", "A00007", "A00008"}
	var positions []string
	for _, icao := range icaos {
		positions = append(positions, "MSG,3,1,1,"+icao+",1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0")
	}
	producePositions := func() {
		t.Helper()
		kafkatest.Produce(t, rawRecords(cfg.KafkaRaw, positions...)...)
	}
	start := func() (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- Run(ctx, cfg, slog.New(slog.DiscardHandler)) }()
		return cancel, done
	}

	cancelA, doneA := start()
	producePositions()
	decoded, snaps := kafkatest.Consume(t, out, len(icaos)), kafkatest.Consume(t, stateTopic, len(icaos))
	partitionOf := map[string]int32{}
	for _, rec := range decoded {
		partitionOf[string(rec.Key)] = rec.Partition
	}
	if partitionOf[icaos[0]] == partitionOf[icaos[1]] && partitionOf[icaos[1]] == partitionOf[icaos[2]] && partitionOf[icaos[2]] == partitionOf[icaos[3]] {
		t.Fatalf("aircraft don't span both partitions: %v", partitionOf)
	}
	for _, rec := range snaps {
		if want := partitionOf[string(rec.Key)]; rec.Partition != want {
			t.Fatalf("snapshot for %s on partition %d, decoded on %d", rec.Key, rec.Partition, want)
		}
	}

	// second instance joins; keep every aircraft alive through the rebalance and past the first sweep after it
	cancelB, doneB := start()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		producePositions()
		time.Sleep(500 * time.Millisecond)
	}
	// anything tombstoned before production stopped was expired out from under a live feed
	stop := time.Now()
	for _, rec := range kafkatest.ConsumeUpTo(t, stateTopic, 10000, 2*time.Second) {
		if rec.Value == nil && rec.Timestamp.Before(stop) {
			t.Fatalf("live aircraft %s tombstoned on partition %d", rec.Key, rec.Partition)
		}
	}

	// silence: each instance expires its own partition's aircraft
	tombstoned := map[string]bool{}
	for deadline := time.Now().Add(25 * time.Second); len(tombstoned) < len(icaos) && time.Now().Before(deadline); {
		for _, rec := range kafkatest.ConsumeUpTo(t, stateTopic, 10000, 2*time.Second) {
			if rec.Value == nil {
				tombstoned[string(rec.Key)] = true
			}
		}
	}
	if len(tombstoned) != len(icaos) {
		t.Fatalf("tombstoned %d of %d aircraft: %v", len(tombstoned), len(icaos), tombstoned)
	}
	cancelA()
	cancelB()
	if err := errors.Join(<-doneA, <-doneB); err != nil {
		t.Fatal(err)
	}
}
