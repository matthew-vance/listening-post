package archiver

import (
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestReadDayOrdersAndDedupes(t *testing.T) {
	root := t.TempDir()
	other := "8ee63520-fa04-424f-b05a-60065c207863"
	// two overlapping batches, as a crash before the offset commit leaves behind, with a second station interleaved
	// and a day either side
	for _, batch := range [][]Row{
		{{StationID: station, TS: t0, Raw: "MSG,1"}, {StationID: station, TS: t0.Add(2 * time.Second), Raw: "MSG,3"}},
		{{StationID: station, TS: t0.Add(2 * time.Second), Raw: "MSG,3"}, {StationID: other, TS: t0.Add(time.Second), Raw: "MSG,2"}},
		{{StationID: station, TS: t0.Add(-24 * time.Hour), Raw: "MSG,0"}, {Raw: "not json"}},
	} {
		if _, err := groupAndWrite(root, batch); err != nil {
			t.Fatal(err)
		}
	}

	days, err := Days(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(days) != 2 || filepath.Base(days[0]) != "dt=2026-09-13" || filepath.Base(days[1]) != "dt=2026-09-14" {
		t.Fatalf("days = %v", days)
	}

	rows, err := ReadDay(days[1])
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, r.StationID[:4]+" "+r.Raw)
	}
	if want := []string{"3ae8 MSG,1", "8ee6 MSG,2", "3ae8 MSG,3"}; !slices.Equal(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
}
