package main

import (
	"slices"
	"time"
)

// snapshot is the wire format on aircraft.state: the full current picture of one aircraft, never a delta.
type snapshot struct {
	ICAO         string    `json:"icao"`
	Callsign     string    `json:"callsign,omitempty"`
	Altitude     *int      `json:"altitude,omitempty"`
	GroundSpeed  *float64  `json:"ground_speed,omitempty"`
	Track        *float64  `json:"track,omitempty"`
	Lat          *float64  `json:"lat,omitempty"`
	Lon          *float64  `json:"lon,omitempty"`
	VerticalRate *int      `json:"vertical_rate,omitempty"`
	Squawk       string    `json:"squawk,omitempty"`
	Alert        *bool     `json:"alert,omitempty"`
	Emergency    *bool     `json:"emergency,omitempty"`
	SPI          *bool     `json:"spi,omitempty"`
	OnGround     *bool     `json:"on_ground,omitempty"`
	FirstSeen    time.Time `json:"first_seen"`
	LastSeen     time.Time `json:"last_seen"`
	PositionTS   time.Time `json:"position_ts,omitempty"`
	Stations     []string  `json:"stations"`
	Messages     int64     `json:"messages"`
	Updated      []string  `json:"updated"` // fields that changed in this snapshot
}

type aircraft struct {
	snap    snapshot
	fieldTS map[string]time.Time // event ts at which each field was last set
}

func newAircraft(icao string) *aircraft {
	return &aircraft{snap: snapshot{ICAO: icao}, fieldTS: map[string]time.Time{}}
}

// apply merges one decoded message and records the names of fields whose value changed in snap.Updated.
// A field updates only if the message is at least as new as the one that last set it: a station's
// stale backlog can't regress live state, but a slightly reordered message from another station
// still lands the fields the newer one lacked.
func (a *aircraft) apply(m decodedRecord) {
	s := &a.snap
	if s.FirstSeen.IsZero() || m.TS.Before(s.FirstSeen) {
		s.FirstSeen = m.TS
	}
	if m.TS.After(s.LastSeen) {
		s.LastSeen = m.TS
	}
	s.Messages++
	if !slices.Contains(s.Stations, m.StationID) {
		s.Stations = append(s.Stations, m.StationID)
		slices.Sort(s.Stations)
	}

	var changed []string
	set := func(name string, present bool, differs bool, assign func()) {
		if !present || m.TS.Before(a.fieldTS[name]) {
			return
		}
		a.fieldTS[name] = m.TS
		if differs {
			assign()
			changed = append(changed, name)
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
	set("position_ts", hasPosition, hasPosition && !m.TS.Equal(s.PositionTS), func() { s.PositionTS = m.TS })

	s.Updated = changed
}

func eq[T comparable](a, b *T) bool {
	return a != nil && b != nil && *a == *b
}

// state is every aircraft currently tracked.
type state struct {
	aircraft map[string]*aircraft
	expiry   time.Duration
}

func newState(expiry time.Duration) *state {
	return &state{aircraft: map[string]*aircraft{}, expiry: expiry}
}

// apply routes a message to its aircraft, creating it on first sight, and returns the snapshot if it changed.
func (s *state) apply(m decodedRecord) (snapshot, bool) {
	a, ok := s.aircraft[m.ICAO]
	if !ok {
		a = newAircraft(m.ICAO)
		s.aircraft[m.ICAO] = a
	}
	a.apply(m)
	return a.snap, len(a.snap.Updated) > 0
}

// expire drops aircraft silent for longer than the expiry and returns their ICAOs, sorted, for tombstoning.
func (s *state) expire(now time.Time) []string {
	var gone []string
	for icao, a := range s.aircraft {
		if now.Sub(a.snap.LastSeen) > s.expiry {
			gone = append(gone, icao)
			delete(s.aircraft, icao)
		}
	}
	slices.Sort(gone)
	return gone
}
