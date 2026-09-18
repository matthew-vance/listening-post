import logging
import os
import socket
import sqlite3
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime, timedelta

from station import buffer

log = logging.getLogger("ingest")


def ingest(
    lines: Iterable[str],
    db: sqlite3.Connection,
    now: Callable[[], datetime],
    *,
    batch_size: int,
    flush_after: timedelta,
) -> int:
    """Write every batch_size lines or flush_after since the last write, whichever is first."""
    # Rows are buffered in memory and written in one batch; an open transaction between commits would starve publish.
    written = 0
    pending: list[tuple[str, str]] = []
    last_flush = now()

    def flush() -> None:
        nonlocal written, last_flush
        buffer.append(db, pending)
        written += len(pending)
        log.info("committed %d events (%d total this connection)", len(pending), written)
        pending.clear()
        last_flush = now()

    try:
        for line in lines:
            if pending and (len(pending) >= batch_size or now() - last_flush >= flush_after):
                flush()
            if not line:  # blank line or idle heartbeat from connect()
                continue
            pending.append((now().isoformat(), line))
            log.debug("event %s", line)
    finally:
        if pending:
            flush()  # stream closed, error, or shutdown: never drop the pending batch
    return written


def connect(host: str, port: int, idle_timeout: float) -> Iterator[str]:
    """Yield lines from the SBS feed, and "" whenever idle_timeout passes without data."""
    # ponytail: heartbeats mean a half-open peer is never detected; cap consecutive timeouts if that bites.
    with socket.create_connection((host, port), timeout=idle_timeout) as sock:
        log.info("connected to %s:%d", host, port)
        buf = bytearray()
        while True:
            try:
                chunk = sock.recv(4096)
            except TimeoutError:
                yield ""
                continue
            if not chunk:
                return
            buf += chunk
            while (nl := buf.find(b"\n")) != -1:
                line = buf[:nl].decode(errors="replace").rstrip("\r")
                del buf[: nl + 1]
                yield line


def main() -> None:
    host = os.environ.get("DUMP1090_HOST", "localhost")
    port = int(os.environ.get("DUMP1090_PORT", "30003"))
    batch_size = int(os.environ.get("INGEST_BATCH_SIZE", "100"))
    flush_after = timedelta(seconds=float(os.environ.get("FLUSH_SECONDS", "5")))
    db = buffer.open(buffer.DB_PATH)

    try:
        while True:
            try:
                lines = connect(host, port, idle_timeout=flush_after.total_seconds())
                written = ingest(lines, db, lambda: datetime.now(UTC), batch_size=batch_size, flush_after=flush_after)
                log.warning("stream closed after %d events", written)
            except OSError as e:
                log.warning("connection failed: %s", e)
            time.sleep(5)  # also covers a server that accepts then immediately closes
    finally:
        db.close()
        log.info("shut down")

