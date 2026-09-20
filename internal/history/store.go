package history

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// insertSQL names every column explicitly, as the repo's migration guidance requires: a column the running
// version doesn't know about is ignored rather than selected into. ON CONFLICT DO NOTHING rides the
// (event_station_id, event_ts, icao, last_seen) unique constraint so an at-least-once re-read is idempotent.
const insertSQL = `
INSERT INTO aircraft_traces (
    event_station_id, event_ts, icao, callsign, altitude, ground_speed, track, lat, lon, vertical_rate, squawk,
    alert, emergency, spi, on_ground, first_seen, last_seen, position_ts, stations, messages
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
    $12, $13, $14, $15, $16, $17, $18, $19, $20
) ON CONFLICT DO NOTHING`

// insert sends every row in one round-trip batch. Each statement commits independently — the batch is not a
// transaction — but that's fine: ON CONFLICT DO NOTHING makes a crash mid-batch idempotent on re-read. The
// timeout bounds a stuck batch; a lost commit only re-reads.
func (w *writer) insert(rows []traceRow) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(insertSQL,
			r.EventStationID, r.EventTS, r.Icao, r.Callsign, r.Altitude, r.GroundSpeed, r.Track, r.Lat, r.Lon, r.VerticalRate, r.Squawk,
			r.Alert, r.Emergency, r.Spi, r.OnGround, r.FirstSeen, r.LastSeen, r.PositionTs, r.Stations, r.Messages,
		)
	}
	br := w.pool.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return fmt.Errorf("insert trace %d: %w", i, err)
		}
	}
	return br.Close()
}
