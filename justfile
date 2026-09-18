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

# Serve a live map of a state topic on localhost:8082 (dev only; reads Kafka through the compose container).
# `just map aircraft.state.flink` shows the Flink processor's output instead.
[working-directory: 'map']
map topic='aircraft.state':
    TOPIC={{quote(topic)}} python3 map.py

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

# Run the Pi side: ingest, publish, and heartbeat under one supervisor (env vars documented in README)
station:
    python3 -m station

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
test: test-server test-station test-map test-flink

# Needs Docker: starts throwaway Postgres and Kafka containers. `go test -short ./...` skips those.
test-server:
    go test ./...

test-station:
    python3 -m unittest discover -s station -t .

[working-directory: 'map']
test-map:
    python3 -m unittest

# Needs Docker: the image's build stage runs the JUnit tests (no JDK on the host)
test-flink:
    docker build -q --target build flink
