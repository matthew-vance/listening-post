package main

import (
	"encoding/json"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

const rawValue = `{"station_id":"3ae884ac-cac2-442d-93ec-5885d868f15c","id":420302,"ts":"2026-09-14T15:00:17.521Z","raw":"MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0","received_at":"2026-09-14T15:00:20.5Z"}`

func TestDecodeRecord(t *testing.T) {
	out, err := decodeRecord("events.decoded", &kgo.Record{Value: []byte(rawValue)})
	if err != nil {
		t.Fatal(err)
	}
	if out.Topic != "events.decoded" || string(out.Key) != "A22123" {
		t.Fatalf("topic=%q key=%q", out.Topic, out.Key)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Value, &got); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"station_id", "id", "ts", "received_at", "message_type", "transmission_type", "icao", "altitude", "lat", "lon"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("missing %q in %s", k, out.Value)
		}
	}
	if _, ok := got["raw"]; ok {
		t.Fatal("decoded record must not carry the raw line")
	}
	if got["station_id"] != "3ae884ac-cac2-442d-93ec-5885d868f15c" || got["id"] != float64(420302) || got["altitude"] != float64(8275) {
		t.Fatalf("envelope/message mismatch: %s", out.Value)
	}
}

func TestDecodeRecordKeyFallsBackToStation(t *testing.T) {
	v := `{"station_id":"st","id":1,"ts":"2026-09-14T15:00:17Z","raw":"MSG,8,1,1,,1,2026/09/14,16:05:23.670,2026/09/14,16:05:23.684,,,,,,,,,,,,0","received_at":"2026-09-14T15:00:20Z"}`
	out, err := decodeRecord("x", &kgo.Record{Value: []byte(v)})
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Key) != "st" {
		t.Fatalf("key = %q, want station id", out.Key)
	}
}

func TestDecodeRecordErrors(t *testing.T) {
	for name, v := range map[string]string{
		"garbage envelope": "hello from compose",
		"unparseable line": `{"station_id":"st","id":1,"ts":"2026-09-14T15:00:17Z","raw":"nope","received_at":"2026-09-14T15:00:20Z"}`,
	} {
		if _, err := decodeRecord("x", &kgo.Record{Value: []byte(v)}); err == nil {
			t.Fatalf("%s: want error", name)
		}
	}
}
