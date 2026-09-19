// Package wire holds the record format the Go services share over Kafka: Event on events.raw. The records
// downstream of it (events.decoded, aircraft.state) belong to the Flink processor (flink/); their formats live in
// its classes, and testdata/ holds the golden fixtures every cross-language contract is checked against.
package wire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RawTopic is pinned: auto-create is off and compose's kafka-init declares it (with the processor's topics, named
// in flink/.../ProcessorJob.java).
const RawTopic = "events.raw" // one record per SBS-1 line, keyed by station

// Brokers reads KAFKA_BROKERS, a comma-separated bootstrap list, from getenv.
func Brokers(getenv func(string) string) ([]string, error) {
	v := getenv("KAFKA_BROKERS")
	if v == "" {
		return nil, errors.New("KAFKA_BROKERS is not set")
	}
	return strings.Split(v, ","), nil
}

// OpenKafka connects and pings so a bad broker address fails at startup, not on the first record.
func OpenKafka(ctx context.Context, brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("configure kafka client: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect to kafka: %w", err)
	}
	return client, nil
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
