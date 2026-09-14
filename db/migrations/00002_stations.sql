-- +goose Up
CREATE TABLE stations (
    id          text        PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz                   -- kills every token; history stays attributable
);

CREATE TABLE station_tokens (
    token_hash  text        PRIMARY KEY,      -- hex(sha256(token)); PK is the lookup index
    station_id  text        NOT NULL REFERENCES stations,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);
CREATE INDEX station_tokens_station_idx ON station_tokens (station_id);

-- +goose Down
DROP TABLE station_tokens;
DROP TABLE stations;
