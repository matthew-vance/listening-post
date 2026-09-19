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
		row.ID = fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
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
