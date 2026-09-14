set dotenv-load

# Bring up the stack
up:
    docker compose up -d --build

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

# Mint a station token; add the hash to stations.json, give the token to the station
token:
    #!/usr/bin/env sh
    token=$(openssl rand -hex 32)
    echo "token: $token"
    echo "hash:  $(printf %s "$token" | openssl dgst -sha256 | awk '{print $NF}')"

# Run all tests; add new projects as dependencies here
test: test-gateway test-ingest test-publish test-heartbeat

[working-directory: 'gateway']
test-gateway:
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
