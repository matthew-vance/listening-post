package processor

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

	if !a.apply(msg("s1", base, position)) {
		t.Fatal("first apply must report a change")
	}
	if *a.snap.Altitude != 8275 || *a.snap.Lat != 40.14684 || !a.snap.PositionTS.Equal(base) {
		t.Fatalf("position not merged: %+v", a.snap)
	}
	if !a.snap.FirstSeen.Equal(base) || !a.snap.LastSeen.Equal(base) || a.snap.Messages != 1 {
		t.Fatalf("bookkeeping: %+v", a.snap)
	}

	if !a.apply(msg("s1", base.Add(time.Second), velocity)) {
		t.Fatal("velocity must report a change")
	}
	if *a.snap.Altitude != 8275 || *a.snap.GroundSpeed != 117 || *a.snap.Lat != 40.14684 {
		t.Fatalf("merged snapshot lost a field: %+v", a.snap)
	}

	if !a.apply(msg("s1", base.Add(2*time.Second), ident)) {
		t.Fatal("ident must report a change")
	}
	if a.snap.Callsign != "AAL433" || a.snap.Messages != 3 || !a.snap.LastSeen.Equal(base.Add(2*time.Second)) {
		t.Fatalf("after ident: %+v", a.snap)
	}
}

func TestApplyRepeatWithoutChangeIsQuiet(t *testing.T) {
	a := newAircraft("A22123")
	a.apply(msg("s1", base, velocity))
	if a.apply(msg("s1", base.Add(time.Second), velocity)) {
		t.Fatal("identical values must not report a change")
	}
	if a.snap.Messages != 2 || !a.snap.LastSeen.Equal(base.Add(time.Second)) {
		t.Fatalf("repeat must still count and bump last_seen: %+v", a.snap)
	}

	// a repeated position is not quiet: position_ts is the staleness signal, so re-confirming it counts
	a.apply(msg("s1", base.Add(2*time.Second), position))
	if !a.apply(msg("s1", base.Add(3*time.Second), position)) || !a.snap.PositionTS.Equal(base.Add(3*time.Second)) {
		t.Fatalf("repeated position must re-confirm position_ts: %+v", a.snap)
	}
}

func TestApplyOlderMessageCannotRegressButCanFill(t *testing.T) {
	a := newAircraft("A22123")
	a.apply(msg("s1", base.Add(time.Hour), position)) // live position at 8275

	stale := "MSG,3,1,1,A22123,1,2026/09/14,14:00:00.000,2026/09/14,14:00:00.000,,2000,,,41.0,-84.0,,,0,,0,0"
	if a.apply(msg("s2", base, stale)) {
		t.Fatal("stale backlog must not report a change")
	}
	if *a.snap.Altitude != 8275 {
		t.Fatalf("altitude regressed to %d", *a.snap.Altitude)
	}

	// but an older message still fills fields nothing newer has set
	if !a.apply(msg("s2", base, velocity)) || *a.snap.GroundSpeed != 117 {
		t.Fatalf("older velocity must fill unset fields: %+v", a.snap)
	}
	if !reflect.DeepEqual(a.snap.Stations, []string{"s1", "s2"}) {
		t.Fatalf("stations = %v", a.snap.Stations)
	}
}

func TestUnpublishedReturnsHeardButUnchanged(t *testing.T) {
	s := newState(5 * time.Minute)
	s.apply(msg("s1", base, velocity), 0)
	if _, changed := s.apply(msg("s1", base.Add(time.Second), velocity), 0); changed {
		t.Fatal("identical repeat must not report a change")
	}
	s.apply(msg("s1", base, "MSG,3,1,1,ABCDEF,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,1000,,,40.0,-83.0,,,0,,0,0"), 1)

	// only the aircraft whose last message changed nothing is owed a snapshot; ABCDEF's went out on apply
	got := s.unpublished()
	if len(got) != 1 || got[0].snap.ICAO != "A22123" || got[0].snap.Messages != 2 {
		t.Fatalf("unpublished = %v, want [A22123 with 2 messages]", got)
	}
	if again := s.unpublished(); len(again) != 0 {
		t.Fatalf("second sweep republished %v", again)
	}
}

func TestExpire(t *testing.T) {
	s := newState(5 * time.Minute)
	s.apply(msg("s1", base, position), 0)
	s.apply(msg("s1", base.Add(10*time.Minute), "MSG,3,1,1,ABCDEF,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,1000,,,40.0,-83.0,,,0,,0,0"), 1)

	expired := s.expire(base.Add(11 * time.Minute))
	if len(expired) != 1 || expired[0].snap.ICAO != "A22123" || expired[0].partition != 0 {
		t.Fatalf("expired = %v, want [A22123 on partition 0]", expired)
	}
	if _, ok := s.aircraft["A22123"]; ok {
		t.Fatal("expired aircraft still tracked")
	}
	if _, ok := s.aircraft["ABCDEF"]; !ok {
		t.Fatal("live aircraft dropped")
	}
}

func TestDropPartitions(t *testing.T) {
	s := newState(5 * time.Minute)
	s.apply(msg("s1", base, position), 0)
	s.apply(msg("s1", base, "MSG,3,1,1,ABCDEF,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,1000,,,40.0,-83.0,,,0,,0,0"), 1)

	s.drop([]int32{1})
	if _, ok := s.aircraft["ABCDEF"]; ok {
		t.Fatal("aircraft on dropped partition still tracked")
	}
	if _, ok := s.aircraft["A22123"]; !ok {
		t.Fatal("aircraft on kept partition dropped")
	}
}
