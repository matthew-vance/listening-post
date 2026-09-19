package archiver

import (
	"errors"

	"github.com/matthew-vance/listening-post/internal/wire"
)

// Config holds the archiver's settings. The topic and group are fixed but stay here so tests can use their own.
type Config struct {
	KafkaBrokers  []string // KAFKA_BROKERS: comma-separated bootstrap brokers (required)
	KafkaRaw      string   // topic to archive
	ArchiverGroup string   // consumer group
	ArchiveDir    string   // ARCHIVE_DIR: root directory for Parquet files (required)
	FlushRecords  int      // write a batch after this many records
	FlushSeconds  int      // or after this long since the last write
}

// LoadConfig reads the archiver's settings from getenv, failing on missing required variables.
func LoadConfig(getenv func(string) string) (Config, error) {
	brokers, err := wire.Brokers(getenv)
	if err != nil {
		return Config{}, err
	}
	// ponytail: flush thresholds are fixed; read them from env if a deployment ever needs to tune them.
	cfg := Config{KafkaBrokers: brokers, KafkaRaw: wire.RawTopic, ArchiverGroup: "archiver", ArchiveDir: getenv("ARCHIVE_DIR"), FlushRecords: 10000, FlushSeconds: 300}
	if cfg.ArchiveDir == "" {
		return Config{}, errors.New("ARCHIVE_DIR is not set")
	}
	return cfg, nil
}
