package history

import (
	"errors"

	"github.com/matthew-vance/listening-post/internal/wire"
)

// StateHistoryTopic is pinned: auto-create is off and compose's kafka-init declares it, alongside the topic
// names in flink/.../ProcessorJob.java.
const StateHistoryTopic = "aircraft.state_history"

// Config holds the history writer's settings. The topic and group are fixed but stay here so tests can use
// their own.
type Config struct {
	KafkaBrokers []string // KAFKA_BROKERS: comma-separated bootstrap brokers (required)
	DatabaseURL  string   // DATABASE_URL: Postgres connection URL (required; schema applied by `just migrate`)
	KafkaTopic   string   // topic of Traces to persist
	Group        string   // consumer group
	FlushRecords int      // insert a batch after this many records
	FlushSeconds int      // or after this long since the last insert
}

// LoadConfig reads the history writer's settings from getenv, failing on missing required variables.
func LoadConfig(getenv func(string) string) (Config, error) {
	brokers, err := wire.Brokers(getenv)
	if err != nil {
		return Config{}, err
	}
	// ponytail: flush thresholds are fixed; read them from env if a deployment ever needs to tune them.
	cfg := Config{KafkaBrokers: brokers, DatabaseURL: getenv("DATABASE_URL"), KafkaTopic: StateHistoryTopic, Group: "history", FlushRecords: 1000, FlushSeconds: 5}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is not set")
	}
	return cfg, nil
}
