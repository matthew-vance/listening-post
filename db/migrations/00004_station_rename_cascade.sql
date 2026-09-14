-- Station names are the primary key; cascading updates make a rename a one-row UPDATE on stations.

-- +goose Up
ALTER TABLE station_tokens
    DROP CONSTRAINT station_tokens_station_id_fkey,
    ADD CONSTRAINT station_tokens_station_id_fkey FOREIGN KEY (station_id) REFERENCES stations ON UPDATE CASCADE;
ALTER TABLE heartbeats
    DROP CONSTRAINT heartbeats_station_id_fkey,
    ADD CONSTRAINT heartbeats_station_id_fkey FOREIGN KEY (station_id) REFERENCES stations ON UPDATE CASCADE;

-- +goose Down
ALTER TABLE station_tokens
    DROP CONSTRAINT station_tokens_station_id_fkey,
    ADD CONSTRAINT station_tokens_station_id_fkey FOREIGN KEY (station_id) REFERENCES stations;
ALTER TABLE heartbeats
    DROP CONSTRAINT heartbeats_station_id_fkey,
    ADD CONSTRAINT heartbeats_station_id_fkey FOREIGN KEY (station_id) REFERENCES stations;
