package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaPublisher writes events with the client's default acks (all ISRs), which is what we need: the Pi deletes
// on our 200, so the broker must have it first.
type kafkaPublisher struct {
	client *kgo.Client
	topic  string
}

// Publish writes one record per event, keyed by station so a station's events stay ordered, and returns
// once the broker has acknowledged all of them.
func (p *kafkaPublisher) Publish(ctx context.Context, station string, receivedAt time.Time, events []event) error {
	records := make([]*kgo.Record, 0, len(events))
	for _, e := range events {
		value, err := json.Marshal(wire.Event{StationID: station, ID: e.ID, TS: e.TS, Raw: e.Raw, ReceivedAt: receivedAt})
		if err != nil {
			return fmt.Errorf("encode event %d: %w", e.ID, err)
		}
		records = append(records, &kgo.Record{Topic: p.topic, Key: []byte(station), Value: value})
	}
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", p.topic, err)
	}
	return nil
}
