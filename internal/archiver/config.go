package archiver

import (
	"cmp"
	"errors"
	"fmt"
	"strconv"

	"github.com/matthew-vance/listening-post/internal/wire"
)

// Config holds the archiver's settings. The topic and group are fixed but stay here so tests can use their own.
type Config struct {
	KafkaBrokers  []string // KAFKA_BROKERS: comma-separated bootstrap brokers (required)
	KafkaRaw      string   // topic to archive
	ArchiverGroup string   // consumer group
	ArchiveDir    string   // ARCHIVE_DIR: root directory for Parquet files (required)
	FlushRecords  int      // FLUSH_RECORDS: write a batch after this many records (default 10000)
	FlushSeconds  int      // FLUSH_SECONDS: or after this long since the last write (default 300)
}

// LoadConfig reads the archiver's settings from getenv, failing on missing required variables.
func LoadConfig(getenv func(string) string) (Config, error) {
	brokers, err := wire.Brokers(getenv)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{KafkaBrokers: brokers, KafkaRaw: wire.RawTopic, ArchiverGroup: "archiver", ArchiveDir: getenv("ARCHIVE_DIR")}
	if cfg.ArchiveDir == "" {
		return Config{}, errors.New("ARCHIVE_DIR is not set")
	}
	if cfg.FlushRecords, err = strconv.Atoi(cmp.Or(getenv("FLUSH_RECORDS"), "10000")); err != nil {
		return Config{}, fmt.Errorf("FLUSH_RECORDS: %w", err)
	}
	if cfg.FlushSeconds, err = strconv.Atoi(cmp.Or(getenv("FLUSH_SECONDS"), "300")); err != nil {
		return Config{}, fmt.Errorf("FLUSH_SECONDS: %w", err)
	}
	return cfg, nil
}
