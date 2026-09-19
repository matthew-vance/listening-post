package history

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func sampleRow() traceRow {
	callsign := "AAL433"
	altitude := int32(8275)
	speed := 117.0
	track := 240.0
	lat, lon := 40.14684, -83.17065
	rate := int32(0)
	squawk := "6653"
	alert, emergency, spi, onGround := false, false, false, false
	firstSeen := time.Date(2026, 9, 14, 15, 0, 0, 0, time.UTC)
	lastSeen := time.Date(2026, 9, 14, 15, 0, 2, 0, time.UTC)
	positionTs := lastSeen
	return traceRow{
		EventStationID: "s1",
		EventID:        42,
		EventTS:        lastSeen,
		Icao:           "A22123",
		Callsign:       &callsign,
		Altitude:       &altitude,
		GroundSpeed:    &speed,
		Track:          &track,
		Lat:            &lat,
		Lon:            &lon,
		VerticalRate:   &rate,
		Squawk:         &squawk,
		Alert:          &alert,
		Emergency:      &emergency,
		Spi:            &spi,
		OnGround:       &onGround,
		FirstSeen:      firstSeen,
		LastSeen:       lastSeen,
		PositionTs:     &positionTs,
		Stations:       []string{"s1", "s2"},
		Messages:       3,
	}
}

func TestInsertDedupes(t *testing.T) {
	pool, err := pgxpool.New(t.Context(), testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	w := &writer{pool: pool}
	row := sampleRow()
	// the same trace arriving twice (an at-least-once re-read) must land once
	if err := w.insert([]traceRow{row, row}); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM aircraft_traces WHERE event_station_id = $1 AND event_id = $2", row.EventStationID, row.EventID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows = %d, want 1 (duplicate must be ignored)", count)
	}

	// every column round-trips
	var (
		gotStation, gotIcao, gotCallsign, gotSquawk string
		gotEventID                                  int64
		gotAltitude                                 int32
		gotSpeed, gotTrack, gotLat                  float64
		gotLon                                      float64
		gotEventTs, gotFirstSeen, gotLastSeen       time.Time
		gotStations                                 []string
		gotMessages                                 int64
	)
	if err := pool.QueryRow(t.Context(), `SELECT event_station_id, event_id, event_ts, icao, callsign, altitude, ground_speed, track, lat, lon, squawk, first_seen, last_seen, stations, messages
		FROM aircraft_traces WHERE event_station_id = $1 AND event_id = $2`, row.EventStationID, row.EventID).Scan(&gotStation, &gotEventID, &gotEventTs, &gotIcao, &gotCallsign, &gotAltitude, &gotSpeed, &gotTrack, &gotLat, &gotLon, &gotSquawk, &gotFirstSeen, &gotLastSeen, &gotStations, &gotMessages); err != nil {
		t.Fatal(err)
	}
	if gotStation != "s1" || gotEventID != 42 || !gotEventTs.Equal(row.EventTS) {
		t.Fatalf("event_station_id=%q event_id=%d event_ts=%v", gotStation, gotEventID, gotEventTs)
	}
	if gotIcao != "A22123" || gotCallsign != "AAL433" || gotAltitude != 8275 || gotSpeed != 117 || gotTrack != 240 || gotLat != 40.14684 || gotLon != -83.17065 || gotSquawk != "6653" {
		t.Fatalf("row = %+v", map[string]any{"icao": gotIcao, "callsign": gotCallsign, "altitude": gotAltitude, "speed": gotSpeed, "track": gotTrack, "lat": gotLat, "lon": gotLon, "squawk": gotSquawk})
	}
	if !gotFirstSeen.Equal(row.FirstSeen) || !gotLastSeen.Equal(row.LastSeen) || len(gotStations) != 2 || gotMessages != 3 {
		t.Fatalf("first_seen=%v last_seen=%v stations=%v messages=%d", gotFirstSeen, gotLastSeen, gotStations, gotMessages)
	}
}
