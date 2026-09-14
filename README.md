# Listening Post

An ADS-B flight tracking pipeline. [dump1090](https://github.com/flightaware/dump1090) is used to recieve and decode ADS-B messages into the [SBS-1 BaseStation](http://woodair.net/sbs/article/barebones42_socket_data.htm) format.

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
| `GATEWAY_URL`   | `http://localhost`      | Gateway base URL (Traefik entrypoint)                |
| `STATION_ID`    | hostname                | Identifies this Pi; set it explicitly (stock hostname is `raspberrypi`) |
| `BATCH_SIZE`    | `500`                   | Events per POST                                      |
| `POLL_SECONDS`  | `2`                     | Sleep when the buffer is empty                       |
| `RETRY_SECONDS` | `5`                     | Sleep after a failed POST                            |
| `LOG_LEVEL`     | `INFO`                  | Logs each published batch                            |

Rows are deleted from the buffer only after the gateway returns 2xx, so delivery is at-least-once: a crash between the response and the delete re-sends that batch. `(station, id)` identifies an event uniquely across re-sends.
