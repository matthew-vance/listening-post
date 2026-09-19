package history

import (
	"os"
	"testing"
	"time"
)

// The trace wire shape is pinned by internal/wire/testdata/trace.json, produced by the Flink processor
// (flink/.../Trace.java) and consumed here. Both sides read the same fixture so they can't drift apart.
func TestDecodeTraceGolden(t *testing.T) {
	data, err := os.ReadFile("../wire/testdata/trace.json")
	if err != nil {
		t.Fatal(err)
	}
	row, err := decodeTrace(data)
	if err != nil {
		t.Fatal(err)
	}

	if row.EventStationID != "s1" || row.EventID != 42 || !row.EventTS.Equal(time.Date(2026, 9, 14, 15, 0, 2, 0, time.UTC)) || row.Icao != "A22123" {
		t.Fatalf("event_station_id=%q event_id=%d event_ts=%v icao=%q", row.EventStationID, row.EventID, row.EventTS, row.Icao)
	}
	if !row.FirstSeen.Equal(time.Date(2026, 9, 14, 15, 0, 0, 0, time.UTC)) || !row.LastSeen.Equal(time.Date(2026, 9, 14, 15, 0, 2, 0, time.UTC)) {
		t.Fatalf("first_seen=%v last_seen=%v", row.FirstSeen, row.LastSeen)
	}
	if row.PositionTs == nil || !row.PositionTs.Equal(time.Date(2026, 9, 14, 15, 0, 2, 0, time.UTC)) {
		t.Fatalf("position_ts=%v", row.PositionTs)
	}
	if row.Altitude == nil || *row.Altitude != 8275 || row.GroundSpeed == nil || *row.GroundSpeed != 117 {
		t.Fatalf("altitude=%v ground_speed=%v", row.Altitude, row.GroundSpeed)
	}
	if row.Callsign == nil || *row.Callsign != "AAL433" || row.Squawk == nil || *row.Squawk != "6653" {
		t.Fatalf("callsign=%v squawk=%v", row.Callsign, row.Squawk)
	}
	if len(row.Stations) != 2 || row.Stations[0] != "s1" || row.Stations[1] != "s2" || row.Messages != 3 {
		t.Fatalf("stations=%v messages=%d", row.Stations, row.Messages)
	}
}

func TestDecodeTraceRejectsMissingIdentity(t *testing.T) {
	for name, value := range map[string]string{
		"empty":       `{}`,
		"no station":  `{"event_id":42,"event_ts":"2026-09-14T15:00:02Z","icao":"A22123","first_seen":"2026-09-14T15:00:00Z","last_seen":"2026-09-14T15:00:01Z"}`,
		"no icao":     `{"event_station_id":"s1","event_id":42,"event_ts":"2026-09-14T15:00:02Z","first_seen":"2026-09-14T15:00:00Z","last_seen":"2026-09-14T15:00:01Z"}`,
		"no event ts": `{"event_station_id":"s1","event_id":42,"icao":"A22123","first_seen":"2026-09-14T15:00:00Z","last_seen":"2026-09-14T15:00:01Z"}`,
		"not json":    `not json`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeTrace([]byte(value)); err == nil {
				t.Fatalf("decodeTrace(%q) = nil error, want one", value)
			}
		})
	}
}
