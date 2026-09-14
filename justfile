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

# Run all tests; add new projects as dependencies here
test: test-gateway test-ingest test-publish

[working-directory: 'gateway']
test-gateway:
    go test ./...

[working-directory: 'ingest']
test-ingest:
    python3 -m unittest

[working-directory: 'publish']
test-publish:
    python3 -m unittest
