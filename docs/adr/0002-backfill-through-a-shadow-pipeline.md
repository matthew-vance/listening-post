# The archive holds only what stations heard; backfill runs through a shadow pipeline

The Archive (`internal/archiver/`) keeps one row per Event — `station_id`, `ts`, `raw` — under `archive/dt=<event date>/`, zstd-compressed Parquet, files named by the event-time range they hold. It dropped the station's sequence number, the gateway's receive time, and the Kafka partition/offset/timestamp it carried before (last present at commit `1f8503c`): those describe how an event travelled, not what was heard, and the only code that read them was the archiver's own idempotent file naming. Without offsets a crash can leave overlapping files, and a station's re-sent batch is archived twice, so readers dedupe on the row instead of trusting file names.

Backfill (`just backfill`, `cmd/backfill`, compose profile `backfill`) exists to give a *newly added* derivation the history it missed — not to repair data. `cmd/backfill` replays the archive in event-time order onto `events.raw.replay`; a second instance of the processor, the same image with `TOPIC_SUFFIX=.replay`, folds it from empty state into `events.decoded.replay`, `aircraft.state.replay` and `aircraft.state_history.replay`; only writers that persist append-only history read those back (the history writer subscribes to both `aircraft.state_history` and its `.replay` twin, and its natural key makes rows it already holds no-ops). Nothing live is touched: ingest keeps accepting, the archiver keeps archiving, the live processor and the map never see an old event. The previous backfill paused ingest and the archiver, stopped the processor, truncated `events.raw` and `aircraft_traces` and wiped the checkpoint, because it replayed through the live topic into the live job.

Underneath is a rule the design leans on: **backfill is for append-only outputs merged on natural keys; live state is never backfilled.** The live picture converges on its own inside the expiry window. A derivation that wants history therefore emits it to an append-only topic with a deterministic key, and its writer reads the `.replay` twin.

## Considered options

- **Replay onto the live topic with a `replay` marker.** One topic, one job. But every stateful derivation folds what it reads, so an old event for an aircraft that has expired creates it again — a ghost on the map — and the archiver would re-archive the replay. Each derivation would need to skip marked records once backfilled and consume them until then: a mode flag per derivation, flipped by the replay's end, with live and historical events for the same aircraft interleaving in one state during the window. Rejected: replay-awareness in every derivation, on the live path, and non-deterministic output while it runs.
- **A batch fold outside Flink** (a Java `main` over the archive reusing `Decode`/`Aircraft`, or Flink's batch mode). Cheapest for the one derivation that exists today, but every future derivation would need its own batch twin, which is what ADR 0001 chose Flink to avoid. Rejected.
- **Shadow pipeline** — chosen. One env var in the job, one subscription line per history writer, a compose profile. The shadow instance re-runs every derivation, not just the new one; the outputs nobody reads expire with the topic's one-day retention.

## Consequences

- The Trace's idempotency key loses `event_id`: `(event_station_id, event_ts, icao, last_seen)`, since a backfilled trace has no sequence number and live and backfilled rows must agree.
- `events.raw` is at-least-once (the gateway publishes before it answers 200), so the job dedupes per station before the fold; the archive keeps the duplicate and readers dedupe on the row.
- A backfill costs ~2 GB of RAM while it runs and nothing otherwise. Its jobmanager empties its checkpoint directory on start, so every backfill folds from empty state.
- A derivation deployed at time L and backfilled at time R has rows from both folds in (L, R); they converge once the aircraft in the sky at L expire.
- Repairing derived data (a fold bug fixed, a table lost) is not what this does. It would need the derived store cleared for the affected range first; that step is deliberately absent.
