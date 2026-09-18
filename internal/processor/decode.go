package processor

import (
	"cmp"
	"encoding/json"
	"fmt"

	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// decodeRecord turns a raw record into a ready-to-produce decoded record keyed by ICAO (station id if none).
func decodeRecord(topic string, in *kgo.Record) (*kgo.Record, error) {
	var e wire.Event
	if err := json.Unmarshal(in.Value, &e); err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	d, err := parseSBS(e.Raw)
	if err != nil {
		return nil, err
	}
	d.StationID, d.ID, d.TS, d.ReceivedAt = e.StationID, e.ID, e.TS, e.ReceivedAt
	value, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(cmp.Or(d.ICAO, e.StationID)), Value: value}, nil
}
