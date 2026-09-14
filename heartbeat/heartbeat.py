import json
import logging
import os
import shutil
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

log = logging.getLogger("heartbeat")


@dataclass(frozen=True)
class BufferStats:
    depth: int
    oldest_ts: str | None
    seq: int  # last id ingest assigned; survives deletes, so it measures arrivals


@dataclass(frozen=True)
class State:
    at: datetime
    seq: int
    depth: int
    last_event_at: datetime | None
    last_publish_at: datetime | None


def read_buffer(db: sqlite3.Connection) -> BufferStats:
    # One statement = one snapshot, so depth and seq can't straddle an ingest commit.
    depth, oldest_ts, seq = db.execute(
        """
        SELECT (SELECT count(*) FROM events),
               (SELECT min(ts) FROM events),
               (SELECT seq FROM sqlite_sequence WHERE name = 'events')
        """
    ).fetchone()
    return BufferStats(depth=depth, oldest_ts=oldest_ts, seq=seq or 0)


def uptime_seconds() -> int:
    # CLOCK_BOOTTIME (Linux) counts suspend; CLOCK_MONOTONIC is boot-relative enough for macOS dev.
    return int(time.clock_gettime(getattr(time, "CLOCK_BOOTTIME", time.CLOCK_MONOTONIC)))


def build_report(
    now: datetime, uptime: int, disk_free: int, stats: BufferStats, prev: State | None
) -> tuple[dict[str, Any], State]:
    report: dict[str, Any] = {
        "reported_at": now.isoformat(),
        "uptime_seconds": uptime,
        "disk_free_bytes": disk_free,
        "buffer_depth": stats.depth,
    }
    if stats.oldest_ts is not None:
        report["oldest_buffered_ts"] = stats.oldest_ts

    if prev is None or stats.seq < prev.seq:  # first tick, or the buffer db was recreated
        return report, State(at=now, seq=stats.seq, depth=stats.depth, last_event_at=None, last_publish_at=None)

    arrived = stats.seq - prev.seq
    left = prev.depth + arrived - stats.depth  # rows publish acked since last tick, however fast it drains
    last_event_at = now if arrived > 0 else prev.last_event_at
    last_publish_at = now if left > 0 else prev.last_publish_at

    report["event_rate"] = arrived / (now - prev.at).total_seconds()
    if last_event_at is not None:
        report["last_event_ts"] = last_event_at.isoformat()
    if last_publish_at is not None:
        report["last_publish_ts"] = last_publish_at.isoformat()

    state = State(at=now, seq=stats.seq, depth=stats.depth, last_event_at=last_event_at, last_publish_at=last_publish_at)
    return report, state


def post_heartbeat(url: str, token: str, report: dict[str, Any], timeout: float) -> None:
    body = json.dumps(report).encode()
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    req = Request(f"{url}/v1/stations/heartbeat", data=body, headers=headers, method="POST")
    with urlopen(req, timeout=timeout):  # raises HTTPError on non-2xx, URLError on connection failure
        pass


def sample_buffer(db_path: str) -> BufferStats:
    # Read-only: heartbeat must never create the table or take a write lock.
    try:
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError as e:
        log.warning("buffer unreadable (%s); reporting empty", e)
        return BufferStats(depth=0, oldest_ts=None, seq=0)
    with db:
        return read_buffer(db)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    url = os.environ.get("GATEWAY_URL", "http://localhost").rstrip("/")
    token = os.environ.get("STATION_TOKEN")
    if not token:
        log.error("STATION_TOKEN is not set; mint one with `just station-add`")
        sys.exit(1)
    db_path = os.environ.get("DB_PATH", "../events.db")
    interval = float(os.environ.get("INTERVAL_SECONDS", "60"))

    log.info("reporting to %s every %ss", url, interval)
    state: State | None = None
    while True:
        stats = sample_buffer(db_path)
        disk_free = shutil.disk_usage(os.path.dirname(os.path.abspath(db_path))).free
        report, state = build_report(datetime.now(UTC), uptime_seconds(), disk_free, stats, state)
        try:
            post_heartbeat(url, token, report, timeout=10)
            log.info("sent heartbeat: depth=%d rate=%s", stats.depth, report.get("event_rate", "n/a"))
        except (HTTPError, URLError, TimeoutError) as e:
            log.warning("heartbeat failed: %s", e)
        time.sleep(interval)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        main()
    except KeyboardInterrupt:
        pass
