package gateway

import (
	"errors"

	"github.com/matthew-vance/listening-post/internal/wire"
)

// Config holds the gateway's settings. Ports and the topic are fixed (the Dockerfile, compose, and kafka-init
// pin them) but stay here so tests can point an instance at their own.
type Config struct {
	Port         string   // public API listener
	AdminPort    string   // health/readiness listener, internal only
	DatabaseURL  string   // DATABASE_URL: Postgres connection URL (required; schema applied by `just migrate`)
	KafkaBrokers []string // KAFKA_BROKERS: comma-separated bootstrap brokers (required)
	KafkaRaw     string   // topic events are published to
}

// LoadConfig reads the gateway's settings from getenv, failing on missing required variables.
func LoadConfig(getenv func(string) string) (Config, error) {
	brokers, err := wire.Brokers(getenv)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{Port: "8080", AdminPort: "9091", DatabaseURL: getenv("DATABASE_URL"), KafkaBrokers: brokers, KafkaRaw: wire.RawTopic}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is not set")
	}
	return cfg, nil
}
