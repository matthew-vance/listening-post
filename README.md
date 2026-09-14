# Listening Post

An ADS-B flight tracking pipeline. [dump1090](https://github.com/flightaware/dump1090) is used to receive and decode ADS-B messages into the [SBS-1 BaseStation](http://woodair.net/sbs/article/barebones42_socket_data.htm) format.

## Architecture

```
Raspberry Pi                                        Server
┌──────────┐   :30003   ┌────────┐   events.db   ┌─────────┐   POST /v1/events   ┌─────────┐
│ dump1090 │ ─────────▶ │ ingest │ ────────────▶ │ publish │ ──────────────────▶ │ gateway │
└──────────┘   SBS-1    └────────┘    SQLite     └─────────┘      HTTP :80       └─────────┘
```

Three processes run on the Pi:

- **dump1090** ([flightaware/dump1090](https://github.com/flightaware/dump1090)) — reads the SDR dongle, decodes ADS-B, and serves SBS-1 text on TCP port 30003. Not part of this repo; install from the FlightAware packages.
- **ingest** (`ingest/`, Python stdlib) — connects to dump1090 and appends every raw line to a SQLite table with a timestamp. Reconnects if dump1090 restarts.
- **publish** (`publish/`, Python stdlib) — reads batches from that table, POSTs them to the gateway, and deletes rows only after a 2xx. Retries while the gateway is unreachable.

The SQLite file is the buffer between the two: it survives Pi reboots and gateway outages, so the pipeline never loses data as long as the Pi has disk. Requirements on the Pi are just Python ≥ 3.11 and dump1090 — no packages to install.

Both scripts batch their I/O deliberately. SD cards have limited write endurance, and dump1090 can produce hundreds of lines per second; committing each one to SQLite individually would burn through a card in months. Ingest writes one transaction per `BATCH_SIZE` lines / `FLUSH_SECONDS`, and publish sends `BATCH_SIZE` events per request, so both disk writes and HTTP round-trips stay low.

The gateway (`gateway/`, Go) runs on the server via `docker compose` (`just up`), listening on port 80, and currently just validates and logs incoming batches. Its health probes are on a separate admin port that compose does not publish.

## Gateway

| Variable     | Default | Purpose                                   |
|--------------|---------|-------------------------------------------|
| `PORT`       | `8080`  | Public API (`/v1/*`)                      |
| `ADMIN_PORT` | `9091`  | Internal `/healthz` and `/readyz` probes  |

## Ingest

| Variable        | Default     | Purpose                                              |
|-----------------|-------------|------------------------------------------------------|
| `DUMP1090_HOST` | `localhost` | dump1090 host                                        |
| `DUMP1090_PORT` | `30003`     | dump1090 SBS-1 BaseStation port                      |
| `DB_PATH`       | `../events.db` | SQLite buffer file (repo root when run via `just`) |
| `LOG_LEVEL`     | `INFO`      | `DEBUG` logs every raw line; `INFO` logs each commit |
| `BATCH_SIZE`    | `100`       | Commit after this many lines                         |
| `FLUSH_SECONDS` | `5`         | Commit after this long since the last commit         |

Batching keeps SD card writes down; on power loss at most one batch is lost. A normal stop (SIGINT/SIGTERM) flushes everything.

## Publish

| Variable        | Default                 | Purpose                                              |
|-----------------|-------------------------|------------------------------------------------------|
| `DB_PATH`       | `../events.db`          | SQLite buffer file (same file ingest writes)         |
| `GATEWAY_URL`   | `http://localhost`      | Gateway base URL                                     |
| `STATION_ID`    | hostname                | Identifies this Pi; set it explicitly (stock hostname is `raspberrypi`) |
| `BATCH_SIZE`    | `500`                   | Events per POST                                      |
| `POLL_SECONDS`  | `2`                     | Sleep when the buffer is empty                       |
| `RETRY_SECONDS` | `5`                     | Sleep after a failed POST                            |
| `LOG_LEVEL`     | `INFO`                  | Logs each published batch                            |

Rows are deleted from the buffer only after the gateway returns 2xx, so delivery is at-least-once: a crash between the response and the delete re-sends that batch. `(station, id)` identifies an event uniquely across re-sends.
