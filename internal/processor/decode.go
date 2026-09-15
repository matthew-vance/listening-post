package processor

import (
	"cmp"
	"encoding/json"
	"fmt"
	"time"

	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// decodedRecord is the wire format on events.decoded: the raw envelope (minus the line) plus the parsed message.
type decodedRecord struct {
	StationID  string    `json:"station_id"`
	ID         int64     `json:"id"`
	TS         time.Time `json:"ts"`
	ReceivedAt time.Time `json:"received_at"`
	sbsMessage
}

// decodeRecord turns a raw record into a ready-to-produce decoded record keyed by ICAO (station id if none).
func decodeRecord(topic string, in *kgo.Record) (*kgo.Record, error) {
	var e wire.Event
	if err := json.Unmarshal(in.Value, &e); err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	msg, err := parseSBS(e.Raw)
	if err != nil {
		return nil, err
	}
	value, err := json.Marshal(decodedRecord{StationID: e.StationID, ID: e.ID, TS: e.TS, ReceivedAt: e.ReceivedAt, sbsMessage: msg})
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(cmp.Or(msg.ICAO, e.StationID)), Value: value}, nil
}
