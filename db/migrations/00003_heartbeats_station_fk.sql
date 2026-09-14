-- Before applying: every heartbeats.station_id must exist in stations, or VALIDATE fails.
--   SELECT DISTINCT station_id FROM heartbeats h WHERE NOT EXISTS (SELECT 1 FROM stations s WHERE s.id = h.station_id);
-- Register the missing ones (as revoked if they're gone) or delete their rows first.

-- +goose Up
ALTER TABLE heartbeats
    ADD CONSTRAINT heartbeats_station_id_fkey FOREIGN KEY (station_id) REFERENCES stations NOT VALID;
-- NOT VALID skips the scan so the ADD takes only a brief lock; VALIDATE scans without blocking writes.
ALTER TABLE heartbeats VALIDATE CONSTRAINT heartbeats_station_id_fkey;

-- +goose Down
ALTER TABLE heartbeats DROP CONSTRAINT heartbeats_station_id_fkey;
