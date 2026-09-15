set dotenv-load

# Bring up the stack: postgres first, migrate, then everything else
up:
    mkdir -p archive && chmod 777 archive
    docker compose up -d --wait postgres
    just migrate
    docker compose up -d --build

# Show the newest archived Parquet files
archive-ls:
    find archive -name '*.parquet' | sort | tail -n 20

# Serve a live map of aircraft.state on localhost:8082 (dev only; reads Kafka through the compose container)
[working-directory: 'map']
map:
    python3 map.py

# List Kafka topics
kafka-topics:
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Apply pending migrations (DATABASE_URL from .env)
migrate:
    go tool goose -dir db/migrations postgres "$DATABASE_URL" up

migrate-status:
    go tool goose -dir db/migrations postgres "$DATABASE_URL" status

# Roll back the most recent migration
migrate-down:
    go tool goose -dir db/migrations postgres "$DATABASE_URL" down

# Tear down the stack
down:
    docker compose down

# Start dump1090 serving SBS-1 on localhost:30003. TZ=UTC makes its timestamps match ingest's.
dump1090:
    TZ=UTC dump1090 --net --quiet --fix --net-bind-address 127.0.0.1

# Start the ingest script (env vars documented in README)
[working-directory: 'ingest']
ingest:
    python3 ingest.py

# Start the publish script (env vars documented in README)
[working-directory: 'publish']
publish:
    python3 publish.py

# Start the heartbeat script (env vars documented in README)
[working-directory: 'heartbeat']
heartbeat:
    python3 heartbeat.py

# Register a station and mint its first token (see scripts/stations.sh)
station-add:
    scripts/stations.sh add

# Mint an additional token for rotation; <station> is a UUID or unambiguous prefix
station-token-add station:
    scripts/stations.sh token-add {{quote(station)}}

# Revoke one token by hash prefix (see station-tokens)
station-token-revoke station prefix:
    scripts/stations.sh token-revoke {{quote(station)}} {{quote(prefix)}}

station-tokens station:
    scripts/stations.sh tokens {{quote(station)}}

# Revoke a station and every token (soft: rows and heartbeats remain)
station-revoke station:
    scripts/stations.sh revoke {{quote(station)}}

station-list:
    scripts/stations.sh list

# Run all tests; add new projects as dependencies here
test: test-server test-ingest test-publish test-heartbeat test-map

# Needs Docker: starts throwaway Postgres and Kafka containers. `go test -short ./...` skips those.
test-server:
    go test ./...

[working-directory: 'ingest']
test-ingest:
    python3 -m unittest

[working-directory: 'publish']
test-publish:
    python3 -m unittest

[working-directory: 'heartbeat']
test-heartbeat:
    python3 -m unittest

[working-directory: 'map']
test-map:
    python3 -m unittest
