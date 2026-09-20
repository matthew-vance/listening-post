package history

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthew-vance/listening-post/internal/kafkatest"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRunPersistsTraces(t *testing.T) {
	url := testDB(t)
	topic := kafkatest.Topic(t)

	var records []*kgo.Record
	for i := range 10 {
		row := sampleRow()
		row.EventTS = row.EventTS.Add(time.Duration(i) * time.Second)
		row.Icao = fmt.Sprintf("A%05d", i)
		row.LastSeen = row.LastSeen.Add(time.Duration(i) * time.Second)
		row.PositionTs = &row.LastSeen
		v, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(row.Icao), Value: v})
	}
	kafkatest.Produce(t, records...)

	group := "g_" + topic
	cfg := Config{
		KafkaBrokers: kafkatest.Brokers,
		DatabaseURL:  url,
		KafkaTopic:   topic,
		Group:        group,
		FlushRecords: 5,
		FlushSeconds: 1,
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, slog.New(slog.DiscardHandler)) }()

	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	var count int
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		time.Sleep(200 * time.Millisecond)
		if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM aircraft_traces").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 10 {
			break
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if count != 10 {
		t.Fatalf("rows = %d, want 10", count)
	}
}

func TestRunFailsOnUndecodableTrace(t *testing.T) {
	url := testDB(t)
	topic := kafkatest.Topic(t)

	row := sampleRow()
	v, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	kafkatest.Produce(t,
		&kgo.Record{Topic: topic, Key: []byte(row.Icao), Value: v},
		&kgo.Record{Topic: topic, Key: []byte("x"), Value: []byte("not a trace")},
	)

	cfg := Config{
		KafkaBrokers: kafkatest.Brokers,
		DatabaseURL:  url,
		KafkaTopic:   topic,
		Group:        "g_" + topic,
		FlushRecords: 10,
		FlushSeconds: 1,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := Run(ctx, cfg, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("Run returned nil, want an error for the undecodable trace")
	}

	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var count int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM aircraft_traces").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("rows = %d, want 0 (no flush must run past a bad record)", count)
	}
}
