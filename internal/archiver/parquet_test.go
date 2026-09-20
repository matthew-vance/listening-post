package archiver

import (
	"bytes"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	t0      = time.Date(2026, 9, 14, 15, 0, 17, 521_000_000, time.UTC)
	station = "3ae884ac-cac2-442d-93ec-5885d868f15c"
)

func TestWriteParquetRoundTrips(t *testing.T) {
	rows := []Row{
		{StationID: station, TS: t0, Raw: "MSG,3,a"},
		{StationID: station, TS: t0.Add(time.Millisecond), Raw: "MSG,4,b"},
	}

	data, err := writeParquet(rows)
	if err != nil {
		t.Fatal(err)
	}
	got, err := parquet.Read[Row](bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("read %d rows, want 2", len(got))
	}
	for i := range rows {
		if got[i].StationID != rows[i].StationID || !got[i].TS.Equal(rows[i].TS) || got[i].Raw != rows[i].Raw {
			t.Fatalf("Row %d = %+v, want %+v", i, got[i], rows[i])
		}
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range f.Metadata().RowGroups[0].Columns {
		if c.MetaData.Codec != format.Zstd {
			t.Fatalf("column %v codec = %v, want zstd", c.MetaData.PathInSchema, c.MetaData.Codec)
		}
	}
}

func TestDecode(t *testing.T) {
	rec := &kgo.Record{
		Key:   []byte(station),
		Value: []byte(`{"station_id":"` + station + `","id":420302,"ts":"2026-09-14T15:00:17.521Z","raw":"MSG,7,1,1,A519","received_at":"2026-09-14T15:00:20.5Z"}`),
	}
	r, err := decodeRow(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.StationID != station || !r.TS.Equal(t0) || r.Raw != "MSG,7,1,1,A519" {
		t.Fatalf("decoded %+v", r)
	}
	if p := r.partition(); p != "dt=2026-09-14" {
		t.Fatalf("partition = %q", p)
	}
}

func TestDecodeGarbageIsKeptUnderUnknown(t *testing.T) {
	rec := &kgo.Record{Value: []byte("not json")}
	r, err := decodeRow(rec)
	if err == nil || r.Raw != "not json" {
		t.Fatalf("garbage record must keep its bytes and report why: %+v, %v", r, err)
	}
	if p := r.partition(); p != "dt=unknown" {
		t.Fatalf("partition = %q, want unknown", p)
	}
}
