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

// Row is the Parquet schema: flat and explicit, since it's the contract with DuckDB/pyarrow readers and with
// cmd/backfill, which reads the archive back.
type Row struct {
	StationID      string    `parquet:"station_id"`
	ID             int64     `parquet:"id"`
	TS             time.Time `parquet:"ts,timestamp(microsecond)"`
	Raw            string    `parquet:"raw"`
	ReceivedAt     time.Time `parquet:"received_at,timestamp(microsecond)"`
	KafkaPartition int32     `parquet:"kafka_partition"`
	KafkaOffset    int64     `parquet:"kafka_offset"`
	KafkaTimestamp time.Time `parquet:"kafka_timestamp,timestamp(microsecond)"`
}

// decodeRow turns a topic record into a Row. A value that isn't a gateway event is still archived — sushi
// principle — with its bytes in raw and zeroed fields, which routes it to the dt=unknown/station=unknown
// partition; the returned error says why.
func decodeRow(rec *kgo.Record) (Row, error) {
	r := Row{KafkaPartition: rec.Partition, KafkaOffset: rec.Offset, KafkaTimestamp: rec.Timestamp}
	var e wire.Event
	err := json.Unmarshal(rec.Value, &e)
	if err == nil && (e.StationID == "" || e.TS.IsZero()) {
		err = errors.New("missing station_id or ts")
	}
	if err != nil {
		r.Raw = string(rec.Value)
		return r, err
	}
	r.StationID, r.ID, r.TS, r.Raw, r.ReceivedAt = e.StationID, e.ID, e.TS, e.Raw, e.ReceivedAt
	return r, nil
}

// partition is the Hive-style directory for a Row, keyed on event date so late backlogs land in the right day.
func (r Row) partition() string {
	if r.StationID == "" || r.TS.IsZero() {
		return "dt=unknown/station=unknown"
	}
	return fmt.Sprintf("dt=%s/station=%s", r.TS.UTC().Format("2006-01-02"), r.StationID)
}

func writeParquet(rows []Row) ([]byte, error) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Row](&buf)
	if _, err := w.Write(rows); err != nil {
		return nil, fmt.Errorf("write rows: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}
