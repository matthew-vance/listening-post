package processor

import (
	"reflect"
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

const other = "MSG,3,1,1,ABCDEF,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,1000,,,40.0,-83.0,,,0,,0,0"

func in(station string, ts time.Time, raw string, partition int32) decodedIn {
	return decodedIn{Decoded: msg(station, ts, raw), partition: partition}
}

// icaos summarises a fold's output as "ICAO" for a snapshot and "-ICAO" for a tombstone, in order.
func icaos(out []stateOut) []string {
	var got []string
	for _, o := range out {
		if o.snap == nil {
			got = append(got, "-"+o.icao)
		} else {
			got = append(got, o.icao)
		}
	}
	return got
}

func TestFoldEmitsChangesImmediately(t *testing.T) {
	s := newState(5 * time.Minute)
	out := s.fold([]decodedIn{in("s1", base, velocity, 0), in("s1", base.Add(time.Second), velocity, 0), in("s1", base, other, 1)}, base)
	if !reflect.DeepEqual(icaos(out), []string{"A22123", "ABCDEF"}) {
		t.Fatalf("fold = %v, want [A22123 ABCDEF]: the identical repeat must not emit", icaos(out))
	}
	if out[0].partition != 0 || out[1].partition != 1 || out[0].snap.Messages != 1 {
		t.Fatalf("out = %+v", out)
	}
}

func TestFoldRepublishesHeardButUnchangedOnTheSweep(t *testing.T) {
	s := newState(5 * time.Minute)
	s.fold([]decodedIn{in("s1", base, velocity, 0), in("s1", base.Add(time.Second), velocity, 0)}, base)

	if out := s.fold(nil, base.Add(sweepEvery-time.Second)); len(out) != 0 {
		t.Fatalf("before the sweep: emitted %v", icaos(out))
	}
	out := s.fold(nil, base.Add(sweepEvery))
	if !reflect.DeepEqual(icaos(out), []string{"A22123"}) || out[0].snap.Messages != 2 {
		t.Fatalf("on the sweep: %v %+v, want A22123 with 2 messages", icaos(out), out)
	}
	if out := s.fold(nil, base.Add(2*sweepEvery)); len(out) != 0 {
		t.Fatalf("next sweep with nothing new: emitted %v", icaos(out))
	}
}

func TestFoldChangedAndSweptInOneBatchEmitsOnce(t *testing.T) {
	s := newState(5 * time.Minute)
	s.fold(nil, base) // anchors the sweep clock
	out := s.fold([]decodedIn{in("s1", base, velocity, 0)}, base.Add(sweepEvery))
	if !reflect.DeepEqual(icaos(out), []string{"A22123"}) {
		t.Fatalf("fold = %v, want exactly one snapshot", icaos(out))
	}
}

func TestFoldExpiresOnTheSweep(t *testing.T) {
	s := newState(5 * time.Minute)
	s.fold([]decodedIn{in("s1", base, position, 0), in("s1", base.Add(10*time.Minute), other, 1)}, base.Add(10*time.Minute))
	s.fold([]decodedIn{in("s1", base, position, 0)}, base.Add(10*time.Minute)) // A22123 heard again, unchanged: dirty

	out := s.fold(nil, base.Add(11*time.Minute))
	// expired aircraft are tombstoned on their partition and not also republished
	if !reflect.DeepEqual(icaos(out), []string{"-A22123"}) || out[0].partition != 0 {
		t.Fatalf("fold = %v %+v, want [-A22123 on partition 0]", icaos(out), out)
	}
	if s.size() != 1 {
		t.Fatalf("tracked = %d, want 1 (ABCDEF)", s.size())
	}
}

func TestUntilSweepAnchorsToTheLastSweep(t *testing.T) {
	s := newState(5 * time.Minute)
	if got := s.untilSweep(base); got != sweepEvery {
		t.Fatalf("before any fold: %v, want %v", got, sweepEvery)
	}
	s.fold(nil, base)
	if got := s.untilSweep(base.Add(3 * time.Second)); got != sweepEvery-3*time.Second {
		t.Fatalf("3s after a sweep: %v, want %v", got, sweepEvery-3*time.Second)
	}
	s.fold([]decodedIn{in("s1", base, velocity, 0)}, base.Add(4*time.Second)) // a batch without a sweep doesn't reset the anchor
	if got := s.untilSweep(base.Add(5 * time.Second)); got != sweepEvery-5*time.Second {
		t.Fatalf("5s after a sweep: %v, want %v", got, sweepEvery-5*time.Second)
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
