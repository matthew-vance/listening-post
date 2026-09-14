package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type heartbeatStore struct {
	pool *pgxpool.Pool
}

// Save inserts one heartbeat; a re-sent (station, reported_at) is silently ignored.
func (s *heartbeatStore) Save(ctx context.Context, station string, receivedAt time.Time, hb heartbeatRequest) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO heartbeats (
			station_id, reported_at, received_at, uptime_seconds, disk_free_bytes, buffer_depth,
			oldest_buffered_ts, last_publish_ts, last_event_ts, event_rate
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (station_id, reported_at) DO NOTHING`,
		station, hb.ReportedAt, receivedAt, hb.UptimeSeconds, hb.DiskFreeBytes, hb.BufferDepth,
		hb.OldestBufferedTS, hb.LastPublishTS, hb.LastEventTS, hb.EventRate,
	)
	if err != nil {
		return fmt.Errorf("insert heartbeat: %w", err)
	}
	return nil
}
