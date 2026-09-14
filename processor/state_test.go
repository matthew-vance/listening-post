package main

import (
	"reflect"
	"testing"
	"time"
)

var base = time.Date(2026, 9, 14, 15, 0, 0, 0, time.UTC)

func msg(station string, ts time.Time, raw string) decodedRecord {
	m, err := parseSBS(raw)
	if err != nil {
		panic(err)
	}
	return decodedRecord{StationID: station, TS: ts, ReceivedAt: ts, sbsMessage: m}
}

const (
	ident    = "MSG,1,1,1,A22123,1,2026/09/14,16:05:25.403,2026/09/14,16:05:25.428,AAL433  ,,,,,,,,,,,0"
	position = "MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0"
	velocity = "MSG,4,1,1,A22123,1,2026/09/14,16:05:23.647,2026/09/14,16:05:23.684,,,117,240,,,0,,,,,0"
)

func TestApplyMergesMessageTypes(t *testing.T) {
	a := newAircraft("A22123")

	a.apply(msg("s1", base, position))
	want := []string{"altitude", "lat", "lon", "alert", "spi", "on_ground", "position_ts"}
	if !reflect.DeepEqual(a.snap.Updated, want) {
		t.Fatalf("first apply changed = %v, want %v", a.snap.Updated, want)
	}
	if !a.snap.FirstSeen.Equal(base) || !a.snap.LastSeen.Equal(base) || a.snap.Messages != 1 {
		t.Fatalf("bookkeeping: %+v", a.snap)
	}

	a.apply(msg("s1", base.Add(time.Second), velocity))
	if !reflect.DeepEqual(a.snap.Updated, []string{"ground_speed", "track", "vertical_rate"}) {
		t.Fatalf("velocity changed = %v", a.snap.Updated)
	}
	if *a.snap.Altitude != 8275 || *a.snap.GroundSpeed != 117 || *a.snap.Lat != 40.14684 {
		t.Fatalf("merged snapshot lost a field: %+v", a.snap)
	}

	a.apply(msg("s1", base.Add(2*time.Second), ident))
	if !reflect.DeepEqual(a.snap.Updated, []string{"callsign"}) {
		t.Fatalf("ident changed = %v", a.snap.Updated)
	}
	if a.snap.Callsign != "AAL433" || a.snap.Messages != 3 || !a.snap.LastSeen.Equal(base.Add(2*time.Second)) {
		t.Fatalf("after ident: %+v", a.snap)
	}
}

func TestApplyRepeatWithoutChangeIsQuiet(t *testing.T) {
	a := newAircraft("A22123")
	a.apply(msg("s1", base, velocity))
	a.apply(msg("s1", base.Add(time.Second), velocity))
	if len(a.snap.Updated) != 0 {
		t.Fatalf("identical values changed = %v, want none", a.snap.Updated)
	}
	if a.snap.Messages != 2 || !a.snap.LastSeen.Equal(base.Add(time.Second)) {
		t.Fatalf("repeat must still count and bump last_seen: %+v", a.snap)
	}

	// a repeated position is not quiet: position_ts is the staleness signal, so re-confirming it counts
	a.apply(msg("s1", base.Add(2*time.Second), position))
	a.apply(msg("s1", base.Add(3*time.Second), position))
	if !reflect.DeepEqual(a.snap.Updated, []string{"position_ts"}) {
		t.Fatalf("repeated position changed = %v, want [position_ts]", a.snap.Updated)
	}
}

func TestApplyOlderMessageCannotRegressButCanFill(t *testing.T) {
	a := newAircraft("A22123")
	a.apply(msg("s1", base.Add(time.Hour), position)) // live position at 8275

	stale := "MSG,3,1,1,A22123,1,2026/09/14,14:00:00.000,2026/09/14,14:00:00.000,,2000,,,41.0,-84.0,,,0,,0,0"
	a.apply(msg("s2", base, stale))
	if len(a.snap.Updated) != 0 {
		t.Fatalf("stale backlog changed = %v, want none", a.snap.Updated)
	}
	if *a.snap.Altitude != 8275 {
		t.Fatalf("altitude regressed to %d", *a.snap.Altitude)
	}

	// but an older message still fills fields nothing newer has set
	a.apply(msg("s2", base, velocity))
	if !reflect.DeepEqual(a.snap.Updated, []string{"ground_speed", "track", "vertical_rate"}) {
		t.Fatalf("older velocity changed = %v", a.snap.Updated)
	}
	if !reflect.DeepEqual(a.snap.Stations, []string{"s1", "s2"}) {
		t.Fatalf("stations = %v", a.snap.Stations)
	}
}

func TestExpire(t *testing.T) {
	s := newState(5 * time.Minute)
	s.apply(msg("s1", base, position))
	s.apply(msg("s1", base.Add(10*time.Minute), "MSG,3,1,1,ABCDEF,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,1000,,,40.0,-83.0,,,0,,0,0"))

	expired := s.expire(base.Add(11 * time.Minute))
	if !reflect.DeepEqual(expired, []string{"A22123"}) {
		t.Fatalf("expired = %v, want [A22123]", expired)
	}
	if _, ok := s.aircraft["A22123"]; ok {
		t.Fatal("expired aircraft still tracked")
	}
	if _, ok := s.aircraft["ABCDEF"]; !ok {
		t.Fatal("live aircraft dropped")
	}
}
