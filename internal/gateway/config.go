package gateway

import (
	"cmp"
	"errors"
	"strings"
)

// Config holds the gateway's settings, read from environment variables.
type Config struct {
	Port         string   // PORT: public API listener (default "8080")
	AdminPort    string   // ADMIN_PORT: health/readiness listener, internal only (default "9091")
	DatabaseURL  string   // DATABASE_URL: Postgres connection URL (required; schema applied by `just migrate`)
	KafkaBrokers []string // KAFKA_BROKERS: comma-separated bootstrap brokers (required)
	KafkaRaw     string   // KAFKA_RAW: topic events are published to (default "events.raw")
}

// LoadConfig reads the gateway's settings from getenv, failing on missing required variables.
func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := Config{
		Port:         cmp.Or(getenv("PORT"), "8080"),
		AdminPort:    cmp.Or(getenv("ADMIN_PORT"), "9091"),
		DatabaseURL:  getenv("DATABASE_URL"),
		KafkaBrokers: strings.Split(getenv("KAFKA_BROKERS"), ","),
		KafkaRaw:     cmp.Or(getenv("KAFKA_RAW"), "events.raw"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is not set")
	}
	if len(cfg.KafkaBrokers) == 0 || cfg.KafkaBrokers[0] == "" {
		return Config{}, errors.New("KAFKA_BROKERS is not set")
	}
	return cfg, nil
}
