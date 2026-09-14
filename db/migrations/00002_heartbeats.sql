-- +goose Up
CREATE TABLE heartbeats (
    station_id          uuid             NOT NULL REFERENCES stations,
    reported_at         timestamptz      NOT NULL,  -- client clock
    received_at         timestamptz      NOT NULL,  -- server clock; skew = received - reported
    uptime_seconds      bigint           NOT NULL,
    disk_free_bytes     bigint           NOT NULL,
    buffer_depth        bigint           NOT NULL,
    oldest_buffered_ts  timestamptz,
    last_publish_ts     timestamptz,
    last_event_ts       timestamptz,
    event_rate          double precision,
    PRIMARY KEY (station_id, reported_at)
);
CREATE INDEX heartbeats_station_received_idx ON heartbeats (station_id, received_at DESC);

-- +goose Down
DROP TABLE heartbeats;
