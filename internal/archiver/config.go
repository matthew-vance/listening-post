package archiver

import (
	"cmp"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Config holds the archiver's settings, read from environment variables.
type Config struct {
	KafkaBrokers  []string // KAFKA_BROKERS: comma-separated bootstrap brokers (required)
	KafkaRaw      string   // KAFKA_RAW: topic to archive (default "events.raw")
	ArchiverGroup string   // ARCHIVER_GROUP: consumer group (default "archiver")
	ArchiveDir    string   // ARCHIVE_DIR: root directory for Parquet files (required)
	FlushRecords  int      // FLUSH_RECORDS: write a batch after this many records (default 10000)
	FlushSeconds  int      // FLUSH_SECONDS: or after this long since the last write (default 300)
}

// LoadConfig reads the archiver's settings from getenv, failing on missing required variables.
func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := Config{
		KafkaBrokers:  strings.Split(getenv("KAFKA_BROKERS"), ","),
		KafkaRaw:      cmp.Or(getenv("KAFKA_RAW"), "events.raw"),
		ArchiverGroup: cmp.Or(getenv("ARCHIVER_GROUP"), "archiver"),
		ArchiveDir:    getenv("ARCHIVE_DIR"),
	}
	if len(cfg.KafkaBrokers) == 0 || cfg.KafkaBrokers[0] == "" {
		return Config{}, errors.New("KAFKA_BROKERS is not set")
	}
	if cfg.ArchiveDir == "" {
		return Config{}, errors.New("ARCHIVE_DIR is not set")
	}
	flushRecords, err := strconv.Atoi(cmp.Or(getenv("FLUSH_RECORDS"), "10000"))
	if err != nil {
		return Config{}, fmt.Errorf("FLUSH_RECORDS: %w", err)
	}
	flushSeconds, err := strconv.Atoi(cmp.Or(getenv("FLUSH_SECONDS"), "300"))
	if err != nil {
		return Config{}, fmt.Errorf("FLUSH_SECONDS: %w", err)
	}
	cfg.FlushRecords = flushRecords
	cfg.FlushSeconds = flushSeconds
	return cfg, nil
}
