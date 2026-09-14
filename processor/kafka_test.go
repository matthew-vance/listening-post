package main

// Test broker setup. Duplicated from gateway/kafka_test.go and archiver/kafka_test.go: two modules can't share a test helper without a third.

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/netip"
	"os"
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

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	kafka, err := startKafka(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "start kafka container:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = testcontainers.TerminateContainer(kafka)
	os.Exit(code)
}

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
	return consumeUpTo(t, topic, n, 10*time.Second)[:n]
}

// consumeUpTo reads up to n records from the start of a topic, returning what arrived within wait.
func consumeUpTo(t *testing.T, topic string, n int, wait time.Duration) []*kgo.Record {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), wait)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		if err := fetches.Err(); err != nil {
			t.Fatalf("after %d records: %v", len(out), err)
		}
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}
