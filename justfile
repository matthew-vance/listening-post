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

# List Kafka topics
kafka-topics:
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Apply pending migrations (DATABASE_URL from .env)
[working-directory: 'db']
migrate:
    go tool goose -dir migrations postgres "$DATABASE_URL" up

[working-directory: 'db']
migrate-status:
    go tool goose -dir migrations postgres "$DATABASE_URL" status

# Roll back the most recent migration
[working-directory: 'db']
migrate-down:
    go tool goose -dir migrations postgres "$DATABASE_URL" down

# Tear down the stack
down:
    docker compose down

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
test: test-gateway test-archiver test-ingest test-publish test-heartbeat

[working-directory: 'gateway']
test-gateway:
    go test ./...

[working-directory: 'archiver']
test-archiver:
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
