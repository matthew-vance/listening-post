package archiver

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matthew-vance/listening-post/internal/kafkatest"
	"github.com/matthew-vance/listening-post/internal/pause"
	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/parquet-go/parquet-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRunArchivesTopic(t *testing.T) {
	topic := kafkatest.Topic(t)
	other := "8ee63520-fa04-424f-b05a-60065c207863"

	// 25 records across two stations; ids are the record index
	var records []*kgo.Record
	for i := range 25 {
		st := station
		if i%5 == 0 {
			st = other
		}
		v, _ := json.Marshal(wire.Event{StationID: st, ID: int64(i), TS: t0.Add(time.Duration(i) * time.Second), Raw: fmt.Sprintf("MSG,%d", i), ReceivedAt: t0})
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(st), Value: v})
	}
	kafkatest.Produce(t, records...)

	dir := t.TempDir()
	group := "g_" + topic
	cfg := Config{
		KafkaBrokers:  kafkatest.Brokers,
		KafkaRaw:      topic,
		ArchiverGroup: group,
		ArchiveDir:    dir,
		FlushRecords:  10,
		FlushSeconds:  1,
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, slog.New(slog.DiscardHandler), pause.New()) }()
	var rows []Row
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
	cfg.ArchiveDir = fresh
	ctx, cancel = context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := Run(ctx, cfg, slog.New(slog.DiscardHandler), pause.New()); err != nil {
		t.Fatal(err)
	}
	if n := countFiles(t, fresh); n != 0 {
		t.Fatalf("second run wrote %d files, want 0 (offsets not committed?)", n)
	}
}

func waitForMember(t *testing.T, group string) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(kafkatest.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	adm := kadm.NewClient(client)
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if g, err := adm.DescribeGroups(t.Context(), group); err == nil && len(g[group].Members) > 0 {
			return
		}
	}
	t.Fatalf("group %s never got a member", group)
}

func readArchive(t *testing.T, dir string) []Row {
	t.Helper()
	var rows []Row
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return nil
		}
		got, err := parquet.ReadFile[Row](path)
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

func TestRunDrainsOnPause(t *testing.T) {
	topic := kafkatest.Topic(t)
	var records []*kgo.Record
	for i := range 25 {
		v, _ := json.Marshal(wire.Event{StationID: station, ID: int64(i), TS: t0, Raw: fmt.Sprintf("MSG,%d", i), ReceivedAt: t0})
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(station), Value: v})
	}
	kafkatest.Produce(t, records...)

	dir := t.TempDir()
	// flush thresholds the test never reaches: only the pause can put the rows on disk
	cfg := Config{KafkaBrokers: kafkatest.Brokers, KafkaRaw: topic, ArchiverGroup: "g_" + topic, ArchiveDir: dir, FlushRecords: 1000, FlushSeconds: 300}

	gate := pause.New()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, slog.New(slog.DiscardHandler), gate) }()
	waitForMember(t, cfg.ArchiverGroup) // pause a running session, as the backfill does, not one that hasn't joined yet
	gate.Pause()

	var rows []Row
	for deadline := time.Now().Add(20 * time.Second); len(rows) < 25 && time.Now().Before(deadline); {
		time.Sleep(200 * time.Millisecond)
		rows = readArchive(t, dir)
	}
	if len(rows) != 25 {
		t.Fatalf("archived %d rows after pause, want 25 (the topic must be drained before the archiver leaves)", len(rows))
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while paused, want it to wait for the resume", err)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}
