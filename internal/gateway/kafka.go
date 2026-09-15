package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// openKafka connects and pings so a bad broker address fails at startup, like openDB.
// Default acks (all ISRs) are what we need: the Pi deletes on our 200, so the broker must have it first.
func openKafka(ctx context.Context, brokers []string) (*kgo.Client, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("configure kafka client: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect to kafka: %w", err)
	}
	return client, nil
}

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
