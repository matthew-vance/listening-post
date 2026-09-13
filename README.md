# Listening Post

An ADS-B flight tracking pipeline. [dump1090](https://github.com/flightaware/dump1090) is used to recieve and decode ADS-B messages into the [SBS-1 BaseStation](http://woodair.net/sbs/article/barebones42_socket_data.htm) format.

## Gateway

| Variable     | Default | Purpose                                   |
|--------------|---------|-------------------------------------------|
| `PORT`       | `8080`  | Public API (`/v1/*`)                      |
| `ADMIN_PORT` | `9091`  | Internal `/healthz` and `/readyz` probes  |
