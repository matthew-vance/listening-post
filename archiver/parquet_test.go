package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	t0      = time.Date(2026, 9, 14, 15, 0, 17, 521_000_000, time.UTC)
	station = "3ae884ac-cac2-442d-93ec-5885d868f15c"
)

func TestWriteParquetRoundTrips(t *testing.T) {
	rows := []row{
		{StationID: station, ID: 1, TS: t0, Raw: "MSG,3,a", ReceivedAt: t0.Add(time.Second), KafkaPartition: 1, KafkaOffset: 10, KafkaTimestamp: t0.Add(2 * time.Second)},
		{StationID: station, ID: 2, TS: t0.Add(time.Millisecond), Raw: "MSG,4,b", ReceivedAt: t0.Add(time.Second), KafkaPartition: 1, KafkaOffset: 11, KafkaTimestamp: t0.Add(2 * time.Second)},
	}

	data, err := writeParquet(rows)
	if err != nil {
		t.Fatal(err)
	}
	got, err := parquet.Read[row](bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("read %d rows, want 2", len(got))
	}
	for i := range rows {
		if got[i].StationID != rows[i].StationID || got[i].ID != rows[i].ID || !got[i].TS.Equal(rows[i].TS) ||
			got[i].Raw != rows[i].Raw || !got[i].ReceivedAt.Equal(rows[i].ReceivedAt) ||
			got[i].KafkaPartition != rows[i].KafkaPartition || got[i].KafkaOffset != rows[i].KafkaOffset || !got[i].KafkaTimestamp.Equal(rows[i].KafkaTimestamp) {
			t.Fatalf("row %d = %+v, want %+v", i, got[i], rows[i])
		}
	}
}

func TestDecode(t *testing.T) {
	rec := &kgo.Record{
		Partition: 2, Offset: 99, Timestamp: t0.Add(3 * time.Second),
		Key:   []byte(station),
		Value: []byte(`{"station_id":"` + station + `","id":420302,"ts":"2026-09-14T15:00:17.521Z","raw":"MSG,7,1,1,A519","received_at":"2026-09-14T15:00:20.5Z"}`),
	}
	r, err := decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.StationID != station || r.ID != 420302 || !r.TS.Equal(t0) || r.Raw != "MSG,7,1,1,A519" ||
		!r.ReceivedAt.Equal(time.Date(2026, 9, 14, 15, 0, 20, 500000000, time.UTC)) ||
		r.KafkaPartition != 2 || r.KafkaOffset != 99 || !r.KafkaTimestamp.Equal(t0.Add(3*time.Second)) {
		t.Fatalf("decoded %+v", r)
	}
	if p := r.partition(); p != "dt=2026-09-14/station="+station {
		t.Fatalf("partition = %q", p)
	}
}

func TestDecodeGarbageIsKeptUnderUnknown(t *testing.T) {
	rec := &kgo.Record{Partition: 0, Offset: 5, Timestamp: t0, Value: []byte("not json")}
	r, err := decode(rec)
	if err == nil || r.Raw != "not json" || r.KafkaOffset != 5 {
		t.Fatalf("garbage record must keep its bytes and offset and report why: %+v, %v", r, err)
	}
	if p := r.partition(); p != "dt=unknown/station=unknown" {
		t.Fatalf("partition = %q, want unknown", p)
	}
}
