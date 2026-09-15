package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRunArchivesTopic(t *testing.T) {
	topic := testTopic(t)
	other := "8ee63520-fa04-424f-b05a-60065c207863"

	// 25 records across two stations; ids are the record index
	producer, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers...))
	if err != nil {
		t.Fatal(err)
	}
	var records []*kgo.Record
	for i := range 25 {
		st := station
		if i%5 == 0 {
			st = other
		}
		v, _ := json.Marshal(eventRecord{StationID: st, ID: int64(i), TS: t0.Add(time.Duration(i) * time.Second), Raw: fmt.Sprintf("MSG,%d", i), ReceivedAt: t0})
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(st), Value: v})
	}
	if err := producer.ProduceSync(t.Context(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	producer.Close()

	dir := t.TempDir()
	group := "g_" + topic
	env := map[string]string{
		"KAFKA_BROKERS": strings.Join(kafkaBrokers, ","),
		"KAFKA_TOPIC":   topic,
		"KAFKA_GROUP":   group,
		"ARCHIVE_DIR":   dir,
		"FLUSH_RECORDS": "10",
		"FLUSH_SECONDS": "1",
	}
	getenv := func(key string) string { return env[key] }

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, getenv, io.Discard) }()
	var rows []row
	for deadline := time.Now().Add(20 * time.Second); len(rows) < 25 && time.Now().Before(deadline); {
		time.Sleep(200 * time.Millisecond)
		rows = readArchive(t, dir)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	rows = readArchive(t, dir)
	if len(rows) != 25 {
		t.Fatalf("archived %d rows, want 25", len(rows))
	}
	// partitions reflect event date and station; offsets are all present exactly once
	seen := map[int64]bool{}
	for _, r := range rows {
		seen[r.KafkaOffset] = true
	}
	if len(seen) != 25 {
		t.Fatalf("distinct offsets = %d, want 25 (duplicates or gaps)", len(seen))
	}
	if n := countFiles(t, filepath.Join(dir, "dt=2026-09-14", "station="+other)); n == 0 {
		t.Fatal("no files for the second station")
	}

	// offsets were committed: a second run into a fresh dir archives nothing
	fresh := t.TempDir()
	env["ARCHIVE_DIR"] = fresh
	ctx, cancel = context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := run(ctx, getenv, io.Discard); err != nil {
		t.Fatal(err)
	}
	if n := countFiles(t, fresh); n != 0 {
		t.Fatalf("second run wrote %d files, want 0 (offsets not committed?)", n)
	}
}

func readArchive(t *testing.T, dir string) []row {
	t.Helper()
	var rows []row
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return nil
		}
		got, err := parquet.ReadFile[row](path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		rows = append(rows, got...)
		return nil
	})
	return rows
}

func countFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	})
	return n
}
