-- +goose Up
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- The append-only history of an aircraft's changed snapshots (a Trace per change), for drawing flight paths.
-- The idempotency key is the identity of the raw Event that triggered the change (station, its sequence number,
-- and its event time), so at-least-once delivery and a re-fold of the same archive both dedupe on it.
CREATE TABLE aircraft_traces (
    event_station_id text        NOT NULL,  -- station whose event triggered this trace
    event_id         bigint      NOT NULL,  -- that event's sequence number
    event_ts         timestamptz NOT NULL,  -- that event's time (disambiguates a reused id after a buffer reset)
    icao          text         NOT NULL,
    callsign      text,
    altitude      integer,                 -- feet
    ground_speed  double precision,        -- knots
    track         double precision,        -- degrees
    lat           double precision,
    lon           double precision,
    vertical_rate integer,                 -- ft/min
    squawk        text,                    -- string: leading zeros matter
    alert         boolean,
    emergency     boolean,
    spi           boolean,
    on_ground     boolean,
    first_seen    timestamptz  NOT NULL,
    last_seen     timestamptz  NOT NULL,   -- hypertable time column: event time, always set on a change
    position_ts   timestamptz,             -- last position change; orders the flight path
    stations      text[]       NOT NULL DEFAULT '{}',
    messages      bigint       NOT NULL
);

SELECT create_hypertable('aircraft_traces', 'last_seen', chunk_time_interval => INTERVAL '1 day');

-- Dedupe on the triggering event: it is globally unique, so the trailing last_seen only satisfies Timescale's
-- rule that a hypertable's unique constraint must include the partition column.
ALTER TABLE aircraft_traces ADD UNIQUE (event_station_id, event_id, event_ts, last_seen);
CREATE INDEX aircraft_traces_icao_time_idx ON aircraft_traces (icao, last_seen DESC);

-- Keep everything (Q7), but compress chunks a week old: flight paths are range scans by icao.
ALTER TABLE aircraft_traces SET (timescaledb.compress, timescaledb.compress_segmentby = 'icao');
SELECT add_compression_policy('aircraft_traces', INTERVAL '7 days');

-- +goose Down
SELECT remove_compression_policy('aircraft_traces', if_exists => true);
DROP TABLE aircraft_traces;
