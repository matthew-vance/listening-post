package processor

import (
	"testing"
	"time"

	"github.com/matthew-vance/listening-post/internal/wire"
)

var base = time.Date(2026, 9, 14, 15, 0, 0, 0, time.UTC)

// msg builds a Decoded the way decodeRecord does, for driving state directly. Merge rules themselves are
// pinned by the golden fixtures (golden_test.go); the tests here cover the sweep and partition lifecycle.
func msg(station string, ts time.Time, raw string) wire.Decoded {
	m, err := parseSBS(raw)
	if err != nil {
		panic(err)
	}
	m.StationID, m.TS, m.ReceivedAt = station, ts, ts
	return m
}

const (
	position = "MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0"
	velocity = "MSG,4,1,1,A22123,1,2026/09/14,16:05:23.647,2026/09/14,16:05:23.684,,,117,240,,,0,,,,,0"
)

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
