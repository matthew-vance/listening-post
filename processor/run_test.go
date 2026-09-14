package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRunDecodesTopic(t *testing.T) {
	in, out, stateTopic := testTopic(t), testTopic(t), testTopic(t)
	lines := []string{
		"MSG,1,1,1,A4BF41,1,2026/09/14,16:05:25.403,2026/09/14,16:05:25.428,AAL433  ,,,,,,,,,,,0",
		"MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0",
		"not an sbs line",
		"MSG,4,1,1,A51958,1,2026/09/14,15:35:33.455,2026/09/14,15:35:33.505,,,505,89,,,-64,,,,,0",
		"MSG,8,1,1,AB197E,1,2026/09/14,16:05:23.670,2026/09/14,16:05:23.684,,,,,,,,,,,,0",
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers...))
	if err != nil {
		t.Fatal(err)
	}
	var records []*kgo.Record
	for i, raw := range lines {
		v, _ := json.Marshal(eventRecord{StationID: "st", ID: int64(i), TS: time.Now().UTC(), Raw: raw, ReceivedAt: time.Now().UTC()})
		records = append(records, &kgo.Record{Topic: in, Key: []byte("st"), Value: v})
	}
	if err := producer.ProduceSync(t.Context(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	producer.Close()

	getenv := func(key string) string {
		return map[string]string{
			"KAFKA_BROKERS":     strings.Join(kafkaBrokers, ","),
			"KAFKA_IN":          in,
			"KAFKA_OUT":         out,
			"KAFKA_GROUP":       "g_" + in,
			"KAFKA_STATE":       stateTopic,
			"KAFKA_STATE_GROUP": "gs_" + in,
			"EXPIRE_SECONDS":    "1",
		}[key]
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, getenv, io.Discard) }()

	got := consume(t, out, 4) // the unparseable line is skipped

	// state: one snapshot per aircraft with fields merged from its messages, keyed by ICAO
	snaps := consume(t, stateTopic, 4)
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
	if u, _ := byICAO["A22123"]["updated"].([]any); len(u) == 0 {
		t.Fatal("first snapshot must list updated fields")
	}

	// expiry: with EXPIRE_SECONDS=1 the sweep tombstones every aircraft
	var tombstones int
	deadline := time.Now().Add(20 * time.Second)
	for tombstones < 4 && time.Now().Before(deadline) {
		tombstones = 0
		for _, rec := range consumeUpTo(t, stateTopic, 100, 2*time.Second) {
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
	if err := run(ctx2, getenv, io.Discard); err != nil {
		t.Fatal(err)
	}
	if extra := len(consumeUpTo(t, out, 5, 3*time.Second)); extra != 4 {
		t.Fatalf("after second run: %d records on out, want 4", extra)
	}
}

func TestStateWarmsFromCompactedTopic(t *testing.T) {
	in, out, stateTopic := testTopic(t), testTopic(t), testTopic(t)
	getenv := func(key string) string {
		return map[string]string{
			"KAFKA_BROKERS":     strings.Join(kafkaBrokers, ","),
			"KAFKA_IN":          in,
			"KAFKA_OUT":         out,
			"KAFKA_GROUP":       "g_" + in,
			"KAFKA_STATE":       stateTopic,
			"KAFKA_STATE_GROUP": "gs_" + in,
			"EXPIRE_SECONDS":    "3600",
		}[key]
	}
	produceRaw := func(raws ...string) {
		t.Helper()
		producer, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers...))
		if err != nil {
			t.Fatal(err)
		}
		defer producer.Close()
		var records []*kgo.Record
		for i, raw := range raws {
			v, _ := json.Marshal(eventRecord{StationID: "st", ID: int64(i), TS: time.Now().UTC(), Raw: raw, ReceivedAt: time.Now().UTC()})
			records = append(records, &kgo.Record{Topic: in, Key: []byte("st"), Value: v})
		}
		if err := producer.ProduceSync(t.Context(), records...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	runFor := func(want int) []*kgo.Record {
		t.Helper()
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- run(ctx, getenv, io.Discard) }()
		recs := consume(t, stateTopic, want)
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
	in, out, stateTopic := testTopicN(t, 2), testTopicN(t, 2), testTopicN(t, 2)
	getenv := func(key string) string {
		return map[string]string{
			"KAFKA_BROKERS":     strings.Join(kafkaBrokers, ","),
			"KAFKA_IN":          in,
			"KAFKA_OUT":         out,
			"KAFKA_GROUP":       "g_" + in,
			"KAFKA_STATE":       stateTopic,
			"KAFKA_STATE_GROUP": "gs_" + in,
			"EXPIRE_SECONDS":    "2",
		}[key]
	}
	icaos := []string{"A00001", "A00002", "A00003", "A00004", "A00005", "A00006", "A00007", "A00008"}
	producer, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	producePositions := func() {
		t.Helper()
		var records []*kgo.Record
		for i, icao := range icaos {
			raw := "MSG,3,1,1," + icao + ",1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0"
			v, _ := json.Marshal(eventRecord{StationID: "st", ID: int64(i), TS: time.Now().UTC(), Raw: raw, ReceivedAt: time.Now().UTC()})
			records = append(records, &kgo.Record{Topic: in, Key: []byte("st"), Value: v})
		}
		if err := producer.ProduceSync(t.Context(), records...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	start := func() (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- run(ctx, getenv, io.Discard) }()
		return cancel, done
	}

	cancelA, doneA := start()
	producePositions()
	decoded, snaps := consume(t, out, len(icaos)), consume(t, stateTopic, len(icaos))
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
	for _, rec := range consumeUpTo(t, stateTopic, 10000, 2*time.Second) {
		if rec.Value == nil && rec.Timestamp.Before(stop) {
			t.Fatalf("live aircraft %s tombstoned on partition %d", rec.Key, rec.Partition)
		}
	}

	// silence: each instance expires its own partition's aircraft
	tombstoned := map[string]bool{}
	for deadline := time.Now().Add(25 * time.Second); len(tombstoned) < len(icaos) && time.Now().Before(deadline); {
		for _, rec := range consumeUpTo(t, stateTopic, 10000, 2*time.Second) {
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
