package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaBrokers points at the shared test broker; each test gets its own topic.
var kafkaBrokers []string

// startKafka runs a single-node KRaft broker on the real apache/kafka image. The advertised listener must be
// known before the container starts, so the host port is chosen up front and bound rather than randomized.
func startKafka(ctx context.Context) (testcontainers.Container, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	req := testcontainers.ContainerRequest{
		Image:        "apache/kafka:4.3.1",
		ExposedPorts: []string{"9094/tcp"},
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094",
			"KAFKA_ADVERTISED_LISTENERS":                     fmt.Sprintf("PLAINTEXT://localhost:9092,EXTERNAL://localhost:%d", port),
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			"KAFKA_HEAP_OPTS":                                "-Xmx256m -Xms256m",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{network.MustParsePort("9094/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)}}}
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, err
	}
	kafkaBrokers = []string{fmt.Sprintf("localhost:%d", port)}
	return c, nil
}

// testTopic creates a uniquely named topic (auto-create is off, as in production) and returns its name.
func testTopic(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("needs docker")
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	name := fmt.Sprintf("t_%d", time.Now().UnixNano())
	if _, err := kadm.NewClient(client).CreateTopic(t.Context(), 1, 1, nil, name); err != nil {
		t.Fatal(err)
	}
	return name
}

// consume reads n records from the start of a topic.
func consume(t *testing.T, topic string, n int) []*kgo.Record {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("after %d records: %v", len(out), err)
		}
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out[:n]
}

func TestKafkaPublisherPublish(t *testing.T) {
	topic := testTopic(t)
	client, err := openKafka(t.Context(), kafkaBrokers)
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

	for i, rec := range consume(t, topic, 3) {
		if string(rec.Key) != station {
			t.Fatalf("record %d key = %q, want station id", i, rec.Key)
		}
		var got struct {
			StationID  string    `json:"station_id"`
			ID         int64     `json:"id"`
			TS         time.Time `json:"ts"`
			Raw        string    `json:"raw"`
			ReceivedAt time.Time `json:"received_at"`
		}
		if err := json.Unmarshal(rec.Value, &got); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		want := events[i]
		if got.StationID != station || got.ID != want.ID || !got.TS.Equal(want.TS) || got.Raw != want.Raw || !got.ReceivedAt.Equal(received) {
			t.Fatalf("record %d = %+v, want event %+v received %v", i, got, want, received)
		}
	}
}
