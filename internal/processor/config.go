package processor

import (
	"cmp"
	"fmt"
	"strconv"

	"github.com/matthew-vance/listening-post/internal/wire"
)

// Config holds the processor's settings. Topics and groups are fixed (kafka-init pins the topics; the groups
// must match across instances) but stay here so tests can give each run its own.
type Config struct {
	KafkaBrokers        []string // KAFKA_BROKERS: comma-separated bootstrap brokers (required)
	KafkaRaw            string   // raw topic to read
	KafkaDecoded        string   // decoded topic to write
	ProcessorGroup      string   // decode loop consumer group
	KafkaState          string   // state topic to write
	ProcessorStateGroup string   // state loop consumer group
	ExpireSeconds       int      // EXPIRE_SECONDS: tombstone an aircraft silent this long (default 300)
}

// LoadConfig reads the processor's settings from getenv, failing on missing required variables.
func LoadConfig(getenv func(string) string) (Config, error) {
	brokers, err := wire.Brokers(getenv)
	if err != nil {
		return Config{}, err
	}
	expire, err := strconv.Atoi(cmp.Or(getenv("EXPIRE_SECONDS"), "300"))
	if err != nil {
		return Config{}, fmt.Errorf("EXPIRE_SECONDS: %w", err)
	}
	return Config{
		KafkaBrokers:        brokers,
		KafkaRaw:            wire.RawTopic,
		KafkaDecoded:        wire.DecodedTopic,
		ProcessorGroup:      "processor",
		KafkaState:          wire.StateTopic,
		ProcessorStateGroup: "processor-state",
		ExpireSeconds:       expire,
	}, nil
}
