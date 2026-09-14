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

