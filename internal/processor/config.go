package processor

import (
	"cmp"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Config holds the processor's settings, read from environment variables.
type Config struct {
	KafkaBrokers        []string // KAFKA_BROKERS: comma-separated bootstrap brokers (required)
	KafkaRaw            string   // KAFKA_RAW: raw topic to read (default "events.raw")
	KafkaDecoded        string   // KAFKA_DECODED: decoded topic to write (default "events.decoded")
	ProcessorGroup      string   // PROCESSOR_GROUP: decode loop consumer group (default "processor")
	KafkaState          string   // KAFKA_STATE: state topic to write (default "aircraft.state")
	ProcessorStateGroup string   // PROCESSOR_STATE_GROUP: state loop consumer group (default "processor-state")
	ExpireSeconds       int      // EXPIRE_SECONDS: tombstone an aircraft silent this long (default 300)
}

// LoadConfig reads the processor's settings from getenv, failing on missing required variables.
func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := Config{
		KafkaBrokers:        strings.Split(getenv("KAFKA_BROKERS"), ","),
		KafkaRaw:            cmp.Or(getenv("KAFKA_RAW"), "events.raw"),
		KafkaDecoded:        cmp.Or(getenv("KAFKA_DECODED"), "events.decoded"),
		ProcessorGroup:      cmp.Or(getenv("PROCESSOR_GROUP"), "processor"),
		KafkaState:          cmp.Or(getenv("KAFKA_STATE"), "aircraft.state"),
		ProcessorStateGroup: cmp.Or(getenv("PROCESSOR_STATE_GROUP"), "processor-state"),
	}
	if len(cfg.KafkaBrokers) == 0 || cfg.KafkaBrokers[0] == "" {
		return Config{}, errors.New("KAFKA_BROKERS is not set")
	}
	expire, err := strconv.Atoi(cmp.Or(getenv("EXPIRE_SECONDS"), "300"))
	if err != nil {
		return Config{}, fmt.Errorf("EXPIRE_SECONDS: %w", err)
	}
	cfg.ExpireSeconds = expire
	return cfg, nil
}
