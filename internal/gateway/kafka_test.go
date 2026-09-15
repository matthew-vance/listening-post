package gateway

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/matthew-vance/listening-post/internal/kafkatest"
	"github.com/matthew-vance/listening-post/internal/wire"
)

func TestKafkaPublisherPublish(t *testing.T) {
	topic := kafkatest.Topic(t)
	client, err := openKafka(t.Context(), kafkatest.Brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	pub := &kafkaPublisher{client: client, topic: topic}

	station := "3ae884ac-cac2-442d-93ec-5885d868f15c"
	received := time.Date(2026, 9, 14, 15, 0, 20, 0, time.UTC)
	events := []event{
		{ID: 1, TS: received.Add(-3 * time.Second), Raw: "MSG,3,a"},
		{ID: 2, TS: received.Add(-2 * time.Second), Raw: "MSG,4,b"},
		{ID: 3, TS: received.Add(-1 * time.Second), Raw: "MSG,8,c"},
	}
	if err := pub.Publish(t.Context(), station, received, events); err != nil {
		t.Fatal(err)
	}

	for i, rec := range kafkatest.Consume(t, topic, 3) {
		if string(rec.Key) != station {
			t.Fatalf("record %d key = %q, want station id", i, rec.Key)
		}
		var got wire.Event
		if err := json.Unmarshal(rec.Value, &got); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		want := events[i]
		if got.StationID != station || got.ID != want.ID || !got.TS.Equal(want.TS) || got.Raw != want.Raw || !got.ReceivedAt.Equal(received) {
			t.Fatalf("record %d = %+v, want event %+v received %v", i, got, want, received)
		}
	}
}
