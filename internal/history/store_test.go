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
		ID:           "9f8b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d",
		Icao:         "A22123",
		Callsign:     &callsign,
		Altitude:     &altitude,
		GroundSpeed:  &speed,
		Track:        &track,
		Lat:          &lat,
		Lon:          &lon,
		VerticalRate: &rate,
		Squawk:       &squawk,
		Alert:        &alert,
		Emergency:    &emergency,
		Spi:          &spi,
		OnGround:     &onGround,
		FirstSeen:    firstSeen,
		LastSeen:     lastSeen,
		PositionTs:   &positionTs,
		Stations:     []string{"s1", "s2"},
		Messages:     3,
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
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM aircraft_traces WHERE id = $1", row.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows = %d, want 1 (duplicate must be ignored)", count)
	}

	// every column round-trips
	var (
		icao, gotCallsign, gotSquawk string
		gotAltitude                  int32
		gotSpeed, gotTrack, gotLat   float64
		gotLon                       float64
		gotFirstSeen, gotLastSeen    time.Time
		gotStations                  []string
		gotMessages                  int64
	)
	if err := pool.QueryRow(t.Context(), `SELECT icao, callsign, altitude, ground_speed, track, lat, lon, squawk, first_seen, last_seen, stations, messages
		FROM aircraft_traces WHERE id = $1`, row.ID).Scan(&icao, &gotCallsign, &gotAltitude, &gotSpeed, &gotTrack, &gotLat, &gotLon, &gotSquawk, &gotFirstSeen, &gotLastSeen, &gotStations, &gotMessages); err != nil {
		t.Fatal(err)
	}
	if icao != "A22123" || gotCallsign != "AAL433" || gotAltitude != 8275 || gotSpeed != 117 || gotTrack != 240 || gotLat != 40.14684 || gotLon != -83.17065 || gotSquawk != "6653" {
		t.Fatalf("row = %+v", map[string]any{"icao": icao, "callsign": gotCallsign, "altitude": gotAltitude, "speed": gotSpeed, "track": gotTrack, "lat": gotLat, "lon": gotLon, "squawk": gotSquawk})
	}
	if !gotFirstSeen.Equal(row.FirstSeen) || !gotLastSeen.Equal(row.LastSeen) || len(gotStations) != 2 || gotMessages != 3 {
		t.Fatalf("first_seen=%v last_seen=%v stations=%v messages=%d", gotFirstSeen, gotLastSeen, gotStations, gotMessages)
	}
}
