-- +goose Up
-- The archive no longer keeps a station's sequence number, so a backfilled Trace can't carry one and the
-- idempotency key must be the same for live and backfilled rows: the triggering event's station and time, plus
-- the aircraft (one station can hear two aircraft in the same microsecond). last_seen stays for Timescale.
ALTER TABLE aircraft_traces DROP CONSTRAINT aircraft_traces_event_station_id_event_id_event_ts_last_see_key; -- Postgres truncates the generated name at 63 chars
ALTER TABLE aircraft_traces DROP COLUMN event_id;
ALTER TABLE aircraft_traces ADD UNIQUE (event_station_id, event_ts, icao, last_seen);

-- +goose Down
ALTER TABLE aircraft_traces DROP CONSTRAINT aircraft_traces_event_station_id_event_ts_icao_last_seen_key;
ALTER TABLE aircraft_traces ADD COLUMN event_id bigint;
ALTER TABLE aircraft_traces ADD UNIQUE (event_station_id, event_id, event_ts, last_seen);
