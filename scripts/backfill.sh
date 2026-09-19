#!/usr/bin/env bash
set -euo pipefail

# Rebuild the derived state (Traces + the live picture) from the archive. Destructive and one-shot: it pauses
# ingest and the archiver via the admin endpoint, stops the processor, truncates and replays events.raw,
# truncates aircraft_traces, clears the Flink checkpoint, and lets the processor re-fold. The gateway (heartbeats)
# and history writer stay up throughout. The archive itself is never touched.

cd "$(dirname "$0")/.."

# Refuse to wipe derived state unless there is an archive to rebuild from.
if [ ! -d archive ] || [ -z "$(find archive -name '*.parquet' -print -quit 2>/dev/null)" ]; then
  echo "backfill: archive/ is missing or has no parquet files; refusing to wipe derived state" >&2
  exit 1
fi

echo "== pausing ingest and the archiver =="
# The gateway keeps serving heartbeats/readyz; the events endpoint answers 503 (stations buffer) and the
# archiver's consumer closes, so it won't re-archive the replay.
admin() { curl -sf -X POST "http://localhost:9091/$1" >/dev/null; }
# Sum the LAG column. A group with no committed offsets yet (the topic was just recreated) reports a huge lag
# rather than 0, so "not started" isn't mistaken for "caught up". max=0 for the processor (ingest is still
# paused, so its lag reaches exactly 0); a small positive threshold for the writer, which follows the live tail.
wait_lag() {
  local group="$1" max="$2"
  while true; do
    lag=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
      --describe --group "$group" 2>/dev/null | awk '$6 ~ /^[0-9]+$/ {s+=$6; n++} END {print (n==0 ? 999999999 : s)}')
    if [ "$lag" -le "$max" ]; then
      echo "$group caught up (lag $lag)"
      return
    fi
    sleep 2
  done
}
# The describe output says so once the last member has left; until then a consumer is still attached.
wait_left() {
  until docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group "$1" 2>/dev/null | grep -q "has no active members"; do
    sleep 1
  done
  echo "$1 left the group"
}
admin pause
trap 'admin resume' EXIT

echo "== waiting for the archiver to drain and leave =="
# On pause the archiver reads events.raw to its end, flushes, commits, and leaves. Both must be true before the
# truncation: lag 0 means nothing unarchived gets deleted; no members means nothing reads the replay.
wait_lag archiver 0
wait_left archiver

echo "== stopping the processor =="
docker compose stop flink-jobmanager flink-taskmanager >/dev/null

echo "== truncating events.raw =="
# Truncate, don't delete: a recreated topic has a new ID, which the gateway's producer (still up) would reject
# with UNKNOWN_TOPIC_ID until restarted — defeating keeping the gateway up. Truncating keeps the ID.
truncate_topic() {
  local topic="$1" partitions="$2" json='{"partitions":[' i
  for i in $(seq 0 $((partitions - 1))); do
    [ "$i" = "0" ] || json="$json,"
    json="$json{\"topic\":\"$topic\",\"partition\":$i,\"offset\":-1}"
  done
  json="$json],\"version\":1}"
  printf '%s' "$json" | docker compose exec -T kafka sh -c 'cat > /tmp/delete-records.json'
  docker compose exec -T kafka /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server localhost:9092 --offset-json-file /tmp/delete-records.json >/dev/null
}
truncate_topic events.raw 3

echo "== truncating traces =="
docker compose exec -T postgres psql -U postgres -d listening_post -c "TRUNCATE aircraft_traces" >/dev/null

echo "== clearing checkpoint =="
# `docker volume rm` would fail: the stopped flink containers still reference the volume. Empty its contents
# instead, so the processor's resume-from-checkpoint entrypoint finds nothing and starts fresh.
docker run --rm -v listening-post_flink-checkpoints:/data flink:2.2.1-java17 sh -c 'rm -rf /data/*' >/dev/null

echo "== replaying archive =="
KAFKA_BROKERS=localhost:9094 go run ./cmd/backfill

echo "== restarting the processor =="
docker compose up -d flink-jobmanager flink-taskmanager >/dev/null

echo "== waiting for the fold to drain =="
wait_lag flink-processor 0

echo "== pointing the archiver at the tail =="
# After the truncation the archiver's committed offset equals the topic's new log-start, which is in range, so its
# AtEnd reset won't fire and it would re-archive the replay. Reset the group to the end explicitly so it skips.
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --reset-offsets --group archiver --topic events.raw --to-latest --execute >/dev/null

echo "== resuming ingest and the archiver =="
admin resume

echo "== waiting for the history writer to drain =="
wait_lag history 10

echo "== backfill complete =="
