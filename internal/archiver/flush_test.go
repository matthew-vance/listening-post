package archiver

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
	rows := []Row{
		{StationID: station, TS: t0.Add(time.Minute), Raw: "MSG,2"},
		{StationID: other, TS: t0, Raw: "MSG,9"},
		{StationID: station, TS: t0, Raw: "MSG,1"},
		{StationID: station, TS: day2, Raw: "MSG,3"},
		{Raw: "not json"},
	}

	files, err := groupAndWrite(dir, rows)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(files)
	want := []string{
		"dt=2026-09-14/20260914T150017.521000Z-20260914T150117.521000Z.parquet",
		"dt=2026-09-15/20260915T150017.521000Z-20260915T150017.521000Z.parquet",
		"dt=unknown/00010101T000000.000000Z-00010101T000000.000000Z.parquet",
	}
	if !slices.Equal(files, want) {
		t.Fatalf("files = %v, want %v", files, want)
	}

	// a day's file holds every station, in event-time order
	got, err := parquet.ReadFile[Row](filepath.Join(dir, want[0]))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].StationID != station || got[0].Raw != "MSG,1" || got[1].StationID != other || got[2].Raw != "MSG,2" {
		t.Fatalf("first file rows = %+v", got)
	}

	// re-processing the same records regenerates the same files: no duplicates, no leftovers
	if _, err := groupAndWrite(dir, rows); err != nil {
		t.Fatal(err)
	}
	if n := countFiles(t, dir); n != 3 {
		t.Fatalf("after rewrite: %d files, want 3 (no .tmp leftovers, no duplicates)", n)
	}
}
