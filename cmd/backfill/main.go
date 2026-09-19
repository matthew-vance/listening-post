// Command backfill replays the archive (raw SBS-1 events in Hive-partitioned Parquet) back onto events.raw in
// event-time order, so the processor can re-fold the whole history into Traces. It is the replay half of
// scripts/backfill.sh; run it standalone with KAFKA_BROKERS and ARCHIVE_DIR (default archive).
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/matthew-vance/listening-post/internal/archiver"
	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/parquet-go/parquet-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

const batchSize = 5000

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()

	archive := os.Getenv("ARCHIVE_DIR")
	if archive == "" {
		archive = "archive"
	}
	brokers, err := wire.Brokers(os.Getenv)
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}

	// ponytail: the whole archive is loaded and sorted in memory; walk dt=* directories one day at a time when
	// the archive outgrows RAM.
	rows, err := readArchive(archive)
	if err != nil {
		logger.Error("read archive", "err", err)
		os.Exit(1)
	}
	// Event-time order is the canonical, deterministic fold order; a station's sequence and ts break any tie.
	slices.SortFunc(rows, func(a, b archiver.Row) int {
		return cmp.Or(a.TS.Compare(b.TS), cmp.Compare(a.StationID, b.StationID), cmp.Compare(a.ID, b.ID))
	})
	logger.Info("read archive", "events", len(rows))

	client, err := wire.OpenKafka(ctx, brokers)
	if err != nil {
		logger.Error("connect to kafka", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		records := make([]*kgo.Record, 0, end-start)
		for _, r := range rows[start:end] {
			rec, err := wire.Record(wire.RawTopic, wire.Event{StationID: r.StationID, ID: r.ID, TS: r.TS, Raw: r.Raw, ReceivedAt: r.ReceivedAt})
			if err != nil {
				logger.Error("encode event", "err", err)
				os.Exit(1)
			}
			records = append(records, rec)
		}
		if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
			logger.Error("produce", "err", err)
			os.Exit(1)
		}
		logger.Info("replayed", "records", end, "of", len(rows))
	}
}

// readArchive loads every event, dropping the undecodable rows (empty station_id, in the dt=unknown bucket).
func readArchive(root string) ([]archiver.Row, error) {
	var rows []archiver.Row
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return nil
		}
		got, err := parquet.ReadFile[archiver.Row](path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for _, r := range got {
			if r.StationID != "" {
				rows = append(rows, r)
			}
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
