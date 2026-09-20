package archiver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/parquet-go/parquet-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Row is the Parquet schema: an Event as its station heard it and nothing else. Flat and explicit, since it's
// the contract with DuckDB/pyarrow readers and with cmd/backfill, which reads the archive back.
type Row struct {
	StationID string    `parquet:"station_id"`
	TS        time.Time `parquet:"ts,timestamp(microsecond)"`
	Raw       string    `parquet:"raw"`
}

// decodeRow turns a topic record into a Row. A value that isn't a gateway event is still archived — sushi
// principle — with its bytes in raw and the other fields zeroed, which routes it to dt=unknown; the returned
// error says why.
func decodeRow(rec *kgo.Record) (Row, error) {
	var e wire.Event
	err := json.Unmarshal(rec.Value, &e)
	if err == nil && (e.StationID == "" || e.TS.IsZero()) {
		err = errors.New("missing station_id or ts")
	}
	if err != nil {
		return Row{Raw: string(rec.Value)}, err
	}
	return Row{StationID: e.StationID, TS: e.TS, Raw: e.Raw}, nil
}

// partition is the Hive-style directory for a Row, keyed on event date so late backlogs land in the right day.
func (r Row) partition() string {
	if r.StationID == "" || r.TS.IsZero() {
		return "dt=unknown"
	}
	return "dt=" + r.TS.UTC().Format("2006-01-02")
}

func writeParquet(rows []Row) ([]byte, error) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Row](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		return nil, fmt.Errorf("write rows: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}
