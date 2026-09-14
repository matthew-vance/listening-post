package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// eventRecord mirrors the gateway's wire format on events.raw.
type eventRecord struct {
	StationID  string    `json:"station_id"`
	ID         int64     `json:"id"`
	TS         time.Time `json:"ts"`
	Raw        string    `json:"raw"`
	ReceivedAt time.Time `json:"received_at"`
}

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
	var e eventRecord
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
	key := msg.ICAO
	if key == "" {
		key = e.StationID
	}
	return &kgo.Record{Topic: topic, Key: []byte(key), Value: value}, nil
}
