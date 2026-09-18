// Package wire holds the record formats shared across Kafka topics: Event on events.raw, Decoded on
// events.decoded, Snapshot on aircraft.state. The Flink job (flink/) mirrors these; testdata/ holds the golden
// fixtures both implementations are checked against.
package wire

import (
	"errors"
	"strings"
	"time"
)

// Topic names are pinned: auto-create is off and compose's kafka-init declares exactly these.
const (
	RawTopic     = "events.raw"     // one record per SBS-1 line, keyed by station
	DecodedTopic = "events.decoded" // parsed lines, keyed by ICAO
	StateTopic   = "aircraft.state" // compacted: latest snapshot per aircraft, keyed by ICAO
)

// Brokers reads KAFKA_BROKERS, a comma-separated bootstrap list, from getenv.
func Brokers(getenv func(string) string) ([]string, error) {
	v := getenv("KAFKA_BROKERS")
	if v == "" {
		return nil, errors.New("KAFKA_BROKERS is not set")
	}
	return strings.Split(v, ","), nil
}

// Event is the format on events.raw: one record per SBS-1 line, keyed by station. Delivery is at-least-once:
// a station re-sends a batch whose 200 it never saw, so (station_id, id) can repeat.
type Event struct {
	StationID  string    `json:"station_id"`
	ID         int64     `json:"id"`
	TS         time.Time `json:"ts"`
	Raw        string    `json:"raw"`
	ReceivedAt time.Time `json:"received_at"`
}

// Payload is the SBS field block shared by a Decoded and a Snapshot: the twelve attributes that travel with an
// aircraft's position. A field is present iff the line carried it; pointers + omitempty keep absent fields out
// of the document.
type Payload struct {
	Callsign     string   `json:"callsign,omitempty"`
	Altitude     *int     `json:"altitude,omitempty"`     // feet
	GroundSpeed  *float64 `json:"ground_speed,omitempty"` // knots
	Track        *float64 `json:"track,omitempty"`        // degrees
	Lat          *float64 `json:"lat,omitempty"`
	Lon          *float64 `json:"lon,omitempty"`
	VerticalRate *int     `json:"vertical_rate,omitempty"` // ft/min
	Squawk       string   `json:"squawk,omitempty"`        // string: leading zeros matter
	Alert        *bool    `json:"alert,omitempty"`
	Emergency    *bool    `json:"emergency,omitempty"`
	SPI          *bool    `json:"spi,omitempty"`
	OnGround     *bool    `json:"on_ground,omitempty"`
}

// Decoded is the record on events.decoded: the Event envelope minus the line, plus the parsed SBS-1 message
// (http://woodair.net/sbs/article/barebones42_socket_data.htm). Session, aircraft, and flight ids (fields 3,
// 4, 6) are dropped: dump1090 always emits 1. Keyed by ICAO, or by station when the line names no aircraft.
type Decoded struct {
	StationID        string    `json:"station_id"`
	ID               int64     `json:"id"`
	TS               time.Time `json:"ts"`
	ReceivedAt       time.Time `json:"received_at"`
	MessageType      string    `json:"message_type"`
	TransmissionType *int      `json:"transmission_type,omitempty"`
	ICAO             string    `json:"icao,omitempty"`
	// Generated/logged are the receiver's clock in its local zone with no offset — kept verbatim, not parsed.
	// The envelope's ts (ingest clock, UTC) is the authoritative event time.
	Generated string `json:"generated,omitempty"`
	Logged    string `json:"logged,omitempty"`
	Payload
}

// Snapshot is the record on aircraft.state: the full current picture of one aircraft, never a delta.
type Snapshot struct {
	ICAO       string    `json:"icao"`
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
	PositionTS time.Time `json:"position_ts,omitzero"`
	Stations   []string  `json:"stations"`
	Messages   int64     `json:"messages"`
	Payload
}
