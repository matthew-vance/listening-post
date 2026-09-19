-- +goose Up
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- The append-only history of an aircraft's changed snapshots (a Trace per change), for drawing flight paths.
-- The idempotency key id is minted by the Flink processor (a UUID) so at-least-once delivery can dedupe on it.
CREATE TABLE aircraft_traces (
    id            uuid         NOT NULL,  -- processor-minted idempotency key
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

-- Dedupe: id is globally unique, so (id, last_seen) is unique too; Timescale requires the partition column
-- to appear in any unique constraint on a hypertable.
ALTER TABLE aircraft_traces ADD UNIQUE (id, last_seen);
CREATE INDEX aircraft_traces_icao_time_idx ON aircraft_traces (icao, last_seen DESC);

-- Keep everything (Q7), but compress chunks a week old: flight paths are range scans by icao.
ALTER TABLE aircraft_traces SET (timescaledb.compress, timescaledb.compress_segmentby = 'icao');
SELECT add_compression_policy('aircraft_traces', INTERVAL '7 days');

-- +goose Down
DROP TABLE aircraft_traces;
