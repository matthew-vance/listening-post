package main

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestGroupAndWrite(t *testing.T) {
	dir := t.TempDir()
	other := "8ee63520-fa04-424f-b05a-60065c207863"
	day2 := t0.Add(24 * time.Hour)
	rows := []row{
		{StationID: station, ID: 1, TS: t0, KafkaPartition: 1, KafkaOffset: 100},
		{StationID: other, ID: 1, TS: t0, KafkaPartition: 2, KafkaOffset: 7},
		{StationID: station, ID: 2, TS: t0.Add(time.Minute), KafkaPartition: 1, KafkaOffset: 101},
		{StationID: station, ID: 3, TS: day2, KafkaPartition: 1, KafkaOffset: 102},
		{StationID: other, ID: 2, TS: day2, KafkaPartition: 2, KafkaOffset: 8},
	}

	files, err := groupAndWrite(dir, rows)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(files)
	want := []string{
		"dt=2026-09-14/station=" + station + "/p1-000000000100-000000000101.parquet",
		"dt=2026-09-14/station=" + other + "/p2-000000000007-000000000007.parquet",
		"dt=2026-09-15/station=" + station + "/p1-000000000102-000000000102.parquet",
		"dt=2026-09-15/station=" + other + "/p2-000000000008-000000000008.parquet",
	}
	if !slices.Equal(files, want) {
		t.Fatalf("files = %v, want %v", files, want)
	}

	got, err := parquet.ReadFile[row](filepath.Join(dir, want[0]))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != 1 || got[1].ID != 2 {
		t.Fatalf("first file rows = %+v", got)
	}

	// re-processing the same records regenerates the same files: no duplicates, no leftovers
	if _, err := groupAndWrite(dir, rows); err != nil {
		t.Fatal(err)
	}
	if n := countFiles(t, dir); n != 4 {
		t.Fatalf("after rewrite: %d files, want 4 (no .tmp leftovers, no duplicates)", n)
	}
}
