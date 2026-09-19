package history

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// traceRow is the column shape of aircraft_traces, matching the Flink processor's Trace wire format
// (flink/.../Trace.java). snake_case tags are the contract with that JSON; pointers mark optional fields.
type traceRow struct {
	ID           string     `json:"id"`
	Icao         string     `json:"icao"`
	Callsign     *string    `json:"callsign"`
	Altitude     *int32     `json:"altitude"`
	GroundSpeed  *float64   `json:"ground_speed"`
	Track        *float64   `json:"track"`
	Lat          *float64   `json:"lat"`
	Lon          *float64   `json:"lon"`
	VerticalRate *int32     `json:"vertical_rate"`
	Squawk       *string    `json:"squawk"`
	Alert        *bool      `json:"alert"`
	Emergency    *bool      `json:"emergency"`
	Spi          *bool      `json:"spi"`
	OnGround     *bool      `json:"on_ground"`
	FirstSeen    time.Time  `json:"first_seen"`
	LastSeen     time.Time  `json:"last_seen"`
	PositionTs   *time.Time `json:"position_ts"`
	Stations     []string   `json:"stations"`
	Messages     int64      `json:"messages"`
}

// decodeTrace turns a topic record into a row. A value that isn't a Trace is rejected with an error naming the
// contract break; the processor is the only producer, so one means the two sides drifted.
func decodeTrace(value []byte) (traceRow, error) {
	var r traceRow
	if err := json.Unmarshal(value, &r); err != nil {
		return r, fmt.Errorf("decode trace: %w", err)
	}
	if r.ID == "" || r.Icao == "" || r.FirstSeen.IsZero() || r.LastSeen.IsZero() {
		return r, errors.New("trace missing id, icao, first_seen or last_seen")
	}
	return r, nil
}
