// Package wire holds the record formats shared across Kafka topics.
package wire

import "time"

// Event is the format on events.raw: one record per SBS-1 line, keyed by station. Consumers dedupe on
// (station_id, id).
type Event struct {
	StationID  string    `json:"station_id"`
	ID         int64     `json:"id"`
	TS         time.Time `json:"ts"`
	Raw        string    `json:"raw"`
	ReceivedAt time.Time `json:"received_at"`
}
