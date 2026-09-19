package archiver

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/matthew-vance/listening-post/internal/consume"
	"github.com/twmb/franz-go/pkg/kgo"
)

type archiver struct {
	client *kgo.Client
	logger *slog.Logger
	Config
}

// run consumes until ctx is cancelled, flushing a batch to Parquet every flushRecords or flushAfter, whichever
// comes first, and committing offsets only after the batch's files are all in place. A final flush runs on exit.
// ponytail: single goroutine, one instance; add per-partition workers when ~33 rec/s becomes thousands.
func (a *archiver) run(ctx context.Context) error {
	return consume.Batch(ctx, a.client, a.FlushRecords, a.FlushSeconds, a.decode, a.flush)
}

// decode turns every record into a row — an undecodable one keeps its bytes in raw (sushi principle) — so the
// decode error is advisory: logged, never fatal, because the archive keeps what it can't parse.
func (a *archiver) decode(rec *kgo.Record) (row, error) {
	r, err := decodeRow(rec)
	if err != nil {
		a.logger.Error("archiving undecodable record", "partition", rec.Partition, "offset", rec.Offset, "err", err)
	}
	return r, nil
}

// flush writes rows to Parquet, then commits their offsets. The commit uses a fresh context: on shutdown ctx is
// already cancelled and the files are written; losing the commit would only mean re-archiving (idempotent), but
// there's no reason to.
func (a *archiver) flush(rows []row) error {
	if len(rows) == 0 {
		return nil
	}
	files, err := groupAndWrite(a.ArchiveDir, rows)
	if err != nil {
		return err
	}
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.client.CommitUncommittedOffsets(commitCtx); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	a.logger.Info("flushed", "records", len(rows), "files", len(files))
	return nil
}
