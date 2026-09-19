// Package kafkatest runs one real Kafka broker per test binary and hands tests fresh topics on it.
package kafkatest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Brokers points at the shared test broker; each test gets its own topic.
var Brokers []string

// Main is a TestMain: it starts the Kafka broker and any extra containers in parallel, runs the tests, and tears
// them all down.
func Main(m *testing.M, extra ...func(context.Context) (testcontainers.Container, error)) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	starts := append([]func(context.Context) (testcontainers.Container, error){Start}, extra...)
	containers := make([]testcontainers.Container, len(starts))
	errs := make([]error, len(starts))
	var wg sync.WaitGroup
	for i, start := range starts {
		wg.Go(func() { containers[i], errs[i] = start(ctx) })
	}
	wg.Wait()
	code := 1
	if err := errors.Join(errs...); err != nil {
		fmt.Fprintln(os.Stderr, "start containers:", err)
	} else {
		code = m.Run()
	}
	for _, c := range containers {
		_ = testcontainers.TerminateContainer(c)
	}
	os.Exit(code)
}

// Start runs a single-node KRaft broker on the real apache/kafka image. The advertised listener must be
// known before the container starts, so the host port is chosen up front and bound rather than randomized.
func Start(ctx context.Context) (testcontainers.Container, error) {
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
	Brokers = []string{fmt.Sprintf("localhost:%d", port)}
	return c, nil
}

func newClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(Brokers...)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// Topic creates a uniquely named single-partition topic (auto-create is off, as in production) and returns its name.
func Topic(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("needs docker")
	}
	client := newClient(t)
	name := fmt.Sprintf("t_%d", time.Now().UnixNano())
	if _, err := kadm.NewClient(client).CreateTopic(t.Context(), 1, 1, nil, name); err != nil {
		t.Fatal(err)
	}
	return name
}

// Produce writes records and waits for the broker's ack.
func Produce(t *testing.T, records ...*kgo.Record) {
	t.Helper()
	client := newClient(t)
	if err := client.ProduceSync(t.Context(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// Consume reads n records from the start of a topic.
func Consume(t *testing.T, topic string, n int) []*kgo.Record {
	t.Helper()
	client := newClient(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("got %d of %d records: %v", len(out), n, ctx.Err())
		}
		if err := fetches.Err(); err != nil {
			t.Fatalf("after %d records: %v", len(out), err)
		}
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out[:n]
}
