// Package wire holds the record formats shared across Kafka topics.
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
