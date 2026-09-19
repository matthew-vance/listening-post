package processor

import (
	"cmp"
	"maps"
	"slices"
	"time"

	"github.com/matthew-vance/listening-post/internal/wire"
)

type aircraft struct {
	snap      wire.Snapshot
	fieldTS   map[string]time.Time // event ts at which each field was last set
	floor     time.Time            // restored from a snapshot: every field is at least this new, exact times weren't persisted
	partition int32                // events.decoded partition its messages arrive on; its snapshots go to the same number on aircraft.state
	dirty     bool                 // heard from since its last snapshot went out
}

func newAircraft(icao string) *aircraft {
	return &aircraft{snap: wire.Snapshot{ICAO: icao}, fieldTS: map[string]time.Time{}}
}

// apply merges one decoded message and reports whether any field's value changed.
// A field updates only if the message is at least as new as the one that last set it: a station's
// stale backlog can't regress live state, but a slightly reordered message from another station
// still lands the fields the newer one lacked.
func (a *aircraft) apply(m wire.Decoded) bool {
	s := &a.snap
	if s.FirstSeen.IsZero() || m.TS.Before(s.FirstSeen) {
		s.FirstSeen = m.TS
	}
	if m.TS.After(s.LastSeen) {
		s.LastSeen = m.TS
	}
	s.Messages++
	if i, ok := slices.BinarySearch(s.Stations, m.StationID); !ok {
		s.Stations = slices.Insert(s.Stations, i, m.StationID)
	}

	changed := false
	set := func(name string, present bool, differs bool, assign func()) {
		if !present || m.TS.Before(a.fieldTS[name]) || m.TS.Before(a.floor) {
			return
		}
		a.fieldTS[name] = m.TS
		if differs {
			assign()
			changed = true
		}
	}
	set("callsign", m.Callsign != "", m.Callsign != s.Callsign, func() { s.Callsign = m.Callsign })
	set("altitude", m.Altitude != nil, !eq(s.Altitude, m.Altitude), func() { s.Altitude = m.Altitude })
	set("ground_speed", m.GroundSpeed != nil, !eq(s.GroundSpeed, m.GroundSpeed), func() { s.GroundSpeed = m.GroundSpeed })
	set("track", m.Track != nil, !eq(s.Track, m.Track), func() { s.Track = m.Track })
	set("lat", m.Lat != nil, !eq(s.Lat, m.Lat), func() { s.Lat = m.Lat })
	set("lon", m.Lon != nil, !eq(s.Lon, m.Lon), func() { s.Lon = m.Lon })
	set("vertical_rate", m.VerticalRate != nil, !eq(s.VerticalRate, m.VerticalRate), func() { s.VerticalRate = m.VerticalRate })
	set("squawk", m.Squawk != "", m.Squawk != s.Squawk, func() { s.Squawk = m.Squawk })
	set("alert", m.Alert != nil, !eq(s.Alert, m.Alert), func() { s.Alert = m.Alert })
	set("emergency", m.Emergency != nil, !eq(s.Emergency, m.Emergency), func() { s.Emergency = m.Emergency })
	set("spi", m.SPI != nil, !eq(s.SPI, m.SPI), func() { s.SPI = m.SPI })
	set("on_ground", m.OnGround != nil, !eq(s.OnGround, m.OnGround), func() { s.OnGround = m.OnGround })
	hasPosition := m.Lat != nil && m.Lon != nil
	set("position_ts", hasPosition, !m.TS.Equal(s.PositionTS), func() { s.PositionTS = m.TS })
	a.dirty = !changed // heard: a changed snapshot goes out now, an unchanged one is owed on the next sweep
	return changed
}

func eq[T comparable](a, b *T) bool {
	return a != nil && b != nil && *a == *b
}

// sweepEvery is how often fold expires silent aircraft and republishes ones heard but unchanged.
const sweepEvery = 10 * time.Second

// state is every aircraft currently tracked, plus the sweep clock.
type state struct {
	aircraft  map[string]*aircraft
	expiry    time.Duration
	lastSweep time.Time // zero until the first fold, which anchors it without sweeping
}

func newState(expiry time.Duration) *state {
	return &state{aircraft: map[string]*aircraft{}, expiry: expiry}
}

// decodedIn is one message off events.decoded with the partition it arrived on.
type decodedIn struct {
	wire.Decoded
	partition int32
}

// stateOut is one record for aircraft.state: a snapshot, or a tombstone when snap is nil.
type stateOut struct {
	icao      string
	partition int32
	snap      *wire.Snapshot
}

// fold applies a batch as of now and returns what to publish, in order: a snapshot per aircraft whose fields
// changed, then, once sweepEvery has passed since the last sweep, tombstones for aircraft silent longer than
// the expiry and snapshots for aircraft heard since their last snapshot but unchanged (so last_seen and
// messages on the topic don't go stale). now is the wall clock, compared against event time.
func (s *state) fold(batch []decodedIn, now time.Time) []stateOut {
	var out []stateOut
	for _, m := range batch {
		a, ok := s.aircraft[m.ICAO]
		if !ok {
			a = newAircraft(m.ICAO)
			s.aircraft[m.ICAO] = a
		}
		a.partition = m.partition
		if a.apply(m.Decoded) {
			snap := a.snap
			out = append(out, stateOut{icao: snap.ICAO, partition: m.partition, snap: &snap})
		}
	}
	if s.lastSweep.IsZero() {
		s.lastSweep = now
		return out
	}
	if now.Sub(s.lastSweep) < sweepEvery {
		return out
	}
	s.lastSweep = now
	for _, a := range s.expire(now) {
		out = append(out, stateOut{icao: a.snap.ICAO, partition: a.partition})
	}
	for _, a := range s.unpublished() {
		snap := a.snap
		out = append(out, stateOut{icao: snap.ICAO, partition: a.partition, snap: &snap})
	}
	return out
}

// untilSweep is how long a poll may block before the next sweep is due, anchored to the last sweep rather than
// the poll start so a trickle of traffic can't push sweeps out to ~2x sweepEvery.
func (s *state) untilSweep(now time.Time) time.Duration {
	if s.lastSweep.IsZero() {
		return sweepEvery
	}
	return sweepEvery - now.Sub(s.lastSweep)
}

// forget drops one aircraft: a tombstone seen while warming up.
func (s *state) forget(icao string) {
	delete(s.aircraft, icao)
}

func (s *state) size() int {
	return len(s.aircraft)
}

// restore seeds an aircraft from a persisted snapshot, replacing whatever was tracked for it.
func (s *state) restore(snap wire.Snapshot, partition int32) {
	a := newAircraft(snap.ICAO)
	a.snap, a.floor, a.partition = snap, snap.LastSeen, partition
	s.aircraft[snap.ICAO] = a
}

// expire drops aircraft silent for longer than the expiry and returns them, sorted by ICAO, for tombstoning.
func (s *state) expire(now time.Time) []*aircraft {
	var gone []*aircraft
	for icao, a := range s.aircraft {
		if now.Sub(a.snap.LastSeen) > s.expiry {
			gone = append(gone, a)
			delete(s.aircraft, icao)
		}
	}
	slices.SortFunc(gone, func(a, b *aircraft) int { return cmp.Compare(a.snap.ICAO, b.snap.ICAO) })
	return gone
}

// unpublished returns aircraft heard since their last snapshot went out but with nothing changed, sorted by ICAO,
// and marks them published.
func (s *state) unpublished() []*aircraft {
	var out []*aircraft
	for _, a := range s.aircraft {
		if a.dirty {
			a.dirty = false
			out = append(out, a)
		}
	}
	slices.SortFunc(out, func(a, b *aircraft) int { return cmp.Compare(a.snap.ICAO, b.snap.ICAO) })
	return out
}

// drop forgets every aircraft on the given partitions: another instance owns them now.
func (s *state) drop(partitions []int32) {
	maps.DeleteFunc(s.aircraft, func(_ string, a *aircraft) bool { return slices.Contains(partitions, a.partition) })
}
