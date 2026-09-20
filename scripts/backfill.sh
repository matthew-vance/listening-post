#!/usr/bin/env bash
set -euo pipefail

# Backfill: feed the archive through the derivations again so a newly added one gains the history it missed.
# Nothing live is touched. A second processor (compose profile "backfill") folds events.raw.replay into the
# .replay twins of its topics, from empty state (its jobmanager starts by emptying its checkpoints); the history
# writer already reads aircraft.state_history.replay and its natural key makes rows it has persisted before
# no-ops. Idempotent: run it as often as you like.

cd "$(dirname "$0")/.."

# Wait until every partition of a topic has lag <= slack for a group. A group with no committed offsets yet is
# "not started", never "caught up". slack is 1 for a topic Flink writes: its transactional sink leaves a commit
# marker at the end of each partition that a read_committed consumer reads as a lag of 1 forever.
wait_lag() {
  local group="$1" topic="$2" slack="$3"
  while true; do
    behind=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
      --describe --group "$group" 2>/dev/null | awk -v t="$topic" -v s="$slack" '$2 == t && $6 ~ /^[0-9]+$/ {n++; if ($6 > s) b++} END {print (n==0 ? 1 : b+0)}')
    if [ "$behind" -eq 0 ]; then
      echo "$group caught up on $topic"
      return
    fi
    sleep 2
  done
}

echo "== starting the shadow processor =="
# no --build: it shares the live processor's image, which `just up` rebuilds
docker compose --profile backfill up -d flink-jobmanager-replay flink-taskmanager-replay >/dev/null 2>&1

echo "== replaying archive =="
KAFKA_BROKERS=localhost:9094 go run ./cmd/backfill

echo "== waiting for the fold and the history writer to drain =="
wait_lag flink-processor.replay events.raw.replay 0
wait_lag history aircraft.state_history.replay 1

echo "== stopping the shadow processor =="
docker compose --profile backfill rm -sf flink-jobmanager-replay flink-taskmanager-replay >/dev/null 2>&1

echo "== backfill complete =="
