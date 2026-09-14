import logging
import os
import signal
import socket
import sqlite3
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime, timedelta

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts  TEXT    NOT NULL,
    raw TEXT    NOT NULL
)
"""

log = logging.getLogger("ingest")


def open_db(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")  # publish reads while we write
    db.execute("PRAGMA synchronous=NORMAL")  # fsync at checkpoint, not per commit; only power loss can lose rows
    db.execute(SCHEMA)
    db.commit()
    return db


def ingest(
    lines: Iterable[str],
    db: sqlite3.Connection,
    now: Callable[[], datetime],
    *,
    batch_size: int,
    flush_after: timedelta,
) -> int:
    """Write every batch_size lines or flush_after since the last write, whichever is first."""
    # Rows are buffered in memory and written in one short transaction so the write lock is
    # held for milliseconds per batch; an open transaction between commits would starve publish.
    written = 0
    pending: list[tuple[str, str]] = []
    last_flush = now()

    def flush() -> None:
        nonlocal written, last_flush
        db.executemany("INSERT INTO events (ts, raw) VALUES (?, ?)", pending)
        db.commit()
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
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    host = os.environ.get("DUMP1090_HOST", "localhost")
    port = int(os.environ.get("DUMP1090_PORT", "30003"))
    batch_size = int(os.environ.get("BATCH_SIZE", "100"))
    flush_after = timedelta(seconds=float(os.environ.get("FLUSH_SECONDS", "5")))
    db = open_db(os.environ.get("DB_PATH", "../events.db"))

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


if __name__ == "__main__":
    # SystemExit unwinds through the loop like KeyboardInterrupt does, so both paths
    # close the socket (context managers) and the db (finally) before exiting.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        main()
    except KeyboardInterrupt:
        pass
