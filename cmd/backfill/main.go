// Command backfill replays the archive (raw SBS-1 events in Hive-partitioned Parquet) back onto events.raw in
// event-time order, so the processor can re-fold the whole history into Traces. It is the replay half of
// scripts/backfill.sh; run it standalone with KAFKA_BROKERS (default localhost:9094) and ARCHIVE_DIR (default
// archive).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/parquet-go/parquet-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

// row mirrors the archive's Parquet schema (internal/archiver/parquet.go), the contract DuckDB/pyarrow read.
type row struct {
	StationID      string    `parquet:"station_id"`
	ID             int64     `parquet:"id"`
	TS             time.Time `parquet:"ts,timestamp(microsecond)"`
	Raw            string    `parquet:"raw"`
	ReceivedAt     time.Time `parquet:"received_at,timestamp(microsecond)"`
	KafkaPartition int32     `parquet:"kafka_partition"`
	KafkaOffset    int64     `parquet:"kafka_offset"`
	KafkaTimestamp time.Time `parquet:"kafka_timestamp,timestamp(microsecond)"`
}

const batchSize = 5000

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()

	archive := os.Getenv("ARCHIVE_DIR")
	if archive == "" {
		archive = "archive"
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9094"
	}

	rows, err := readArchive(archive)
	if err != nil {
		logger.Error("read archive", "err", err)
		os.Exit(1)
	}
	// Event-time order is the canonical, deterministic fold order; a station's sequence and ts break any tie.
	sort.Slice(rows, func(i, j int) bool {
		if c := rows[i].TS.Compare(rows[j].TS); c != 0 {
			return c < 0
		}
		if rows[i].StationID != rows[j].StationID {
			return rows[i].StationID < rows[j].StationID
		}
		return rows[i].ID < rows[j].ID
	})
	logger.Info("read archive", "events", len(rows))

	client, err := wire.OpenKafka(ctx, strings.Split(brokers, ","))
	if err != nil {
		logger.Error("connect to kafka", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		records := make([]*kgo.Record, 0, end-start)
		for _, r := range rows[start:end] {
			value, err := json.Marshal(wire.Event{StationID: r.StationID, ID: r.ID, TS: r.TS, Raw: r.Raw, ReceivedAt: r.ReceivedAt})
			if err != nil {
				logger.Error("encode event", "err", err)
				os.Exit(1)
			}
			records = append(records, &kgo.Record{Topic: wire.RawTopic, Key: []byte(r.StationID), Value: value})
		}
		if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
			logger.Error("produce", "err", err)
			os.Exit(1)
		}
		logger.Info("replayed", "records", end, "of", len(rows))
	}
}

// readArchive loads every event, dropping the undecodable rows (empty station_id, in the dt=unknown bucket).
func readArchive(root string) ([]row, error) {
	var rows []row
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return nil
		}
		got, err := parquet.ReadFile[row](path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for _, r := range got {
			if r.StationID == "" {
				continue
			}
			rows = append(rows, r)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("no replayable events found under " + root)
	}
	return rows, nil
}
