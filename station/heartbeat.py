import logging
import os
import shutil
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from station import buffer, gateway
from station.buffer import BufferStats

log = logging.getLogger("heartbeat")


@dataclass(frozen=True)
class State:
    at: datetime
    seq: int
    depth: int
    last_event_at: datetime | None
    last_publish_at: datetime | None


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


def sample_buffer(db_path: str) -> BufferStats:
    # A fresh read-only connection per tick: heartbeat never takes a write lock, and a buffer the supervisor
    # recreated is picked up. An unreadable buffer is a real fault: let it raise and be restarted.
    with buffer.open(db_path, readonly=True) as db:
        return buffer.sample(db)


def main() -> None:
    interval = float(os.environ.get("INTERVAL_SECONDS", "60"))
    disk_path = os.path.dirname(os.path.abspath(buffer.DB_PATH))

    log.info("reporting to %s every %ss", gateway.URL, interval)
    state: State | None = None
    while True:
        stats = sample_buffer(buffer.DB_PATH)
        disk_free = shutil.disk_usage(disk_path).free
        report, state = build_report(datetime.now(UTC), uptime_seconds(), disk_free, stats, state)
        try:
            gateway.post_json(gateway.URL, gateway.TOKEN, "/v1/stations/heartbeat", report)
            log.info("sent heartbeat: depth=%d rate=%s", stats.depth, report.get("event_rate", "n/a"))
        except gateway.POST_ERRORS as e:
            log.warning("heartbeat failed: %s", e)
        time.sleep(interval)

