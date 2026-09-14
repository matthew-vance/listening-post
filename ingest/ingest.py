import logging
import os
import signal
import socket
import sqlite3
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts  TEXT    NOT NULL,
    raw TEXT    NOT NULL
)
"""

log = logging.getLogger("ingest")
PROGRESS_EVERY = 1000


def open_db(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")  # publish reads while we write
    db.execute(SCHEMA)
    db.commit()
    return db


def ingest(lines: Iterable[str], db: sqlite3.Connection, now: Callable[[], datetime]) -> int:
    written = 0
    for line in lines:
        if not line:
            continue
        # ponytail: commit per line; batch every N lines if the Pi's SD card can't keep up.
        db.execute("INSERT INTO events (ts, raw) VALUES (?, ?)", (now().isoformat(), line))
        db.commit()
        written += 1
        log.debug("event %s", line)
        if written % PROGRESS_EVERY == 0:
            log.info("ingested %d events this connection", written)
    return written


def connect(host: str, port: int) -> Iterator[str]:
    with socket.create_connection((host, port)) as sock, sock.makefile("r") as stream:
        log.info("connected to %s:%d", host, port)
        for line in stream:
            yield line.rstrip("\r\n")


def utcnow() -> datetime:
    return datetime.now(UTC)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    host = os.environ.get("DUMP1090_HOST", "localhost")
    port = int(os.environ.get("DUMP1090_PORT", "30003"))
    db = open_db(os.environ.get("DB_PATH", "events.db"))

    try:
        while True:
            try:
                written = ingest(connect(host, port), db, utcnow)
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
