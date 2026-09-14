package main

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRunDecodesTopic(t *testing.T) {
	in, out := testTopic(t), testTopic(t)
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
			"KAFKA_BROKERS": strings.Join(kafkaBrokers, ","),
			"KAFKA_IN":      in,
			"KAFKA_OUT":     out,
			"KAFKA_GROUP":   "g_" + in,
		}[key]
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, getenv, io.Discard) }()

	got := consume(t, out, 4) // the unparseable line is skipped
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
