# Bring up the stack
up:
    docker compose up -d --build

# Tear down the stack
down:
    docker compose down

# Run all tests; add new projects as dependencies here
test: test-gateway

[working-directory: 'gateway']
test-gateway:
    go test ./...
