import logging
import os
import socket
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

from station import buffer

log = logging.getLogger("ingest")


# Reader returns the next line, or None if none arrived within timeout seconds; raises EOFError when the peer closes.
Reader = Callable[[float], str | None]


def ingest(
    read: Reader,
    db: sqlite3.Connection,
    now: Callable[[], datetime],
    *,
    batch_size: int,
    flush_after: timedelta,
) -> int:
    """Write every batch_size lines or flush_after since the last write, whichever is first, until EOF.

    Each read is bounded by the time left to the next flush, so the flush_after guarantee holds however quiet
    the feed is: this function owns the deadline, not its caller.
    """
    # Rows are buffered in memory and written in one batch; an open transaction between commits would starve publish.
    written = 0
    pending: list[tuple[str, str]] = []
    last_flush = now()

    def flush() -> None:
        nonlocal written, last_flush
        n = buffer.append(db, pending)
        written += n
        log.info("committed %d events (%d total this connection)", n, written)
        pending.clear()
        last_flush = now()

    try:
        while True:
            since = now() - last_flush
            if pending and (len(pending) >= batch_size or since >= flush_after):
                flush()
                since = timedelta(0)
            wait = flush_after - since if pending else flush_after
            try:
                line = read(wait.total_seconds())
            except EOFError:
                break
            if line is None:
                continue
            pending.append((now().isoformat(), line))
            log.debug("event %s", line)
    finally:
        if pending:
            flush()  # stream closed, error, or shutdown: never drop the pending batch
    return written


@contextmanager
def connect(host: str, port: int) -> Iterator[Reader]:
    """Connect to the SBS feed and yield a Reader over it; the socket closes with the block."""
    # ponytail: a half-open peer is never detected, every read just times out; cap consecutive Nones if that bites.
    with socket.create_connection((host, port)) as sock:
        log.info("connected to %s:%d", host, port)
        buf = bytearray()

        def read(timeout: float) -> str | None:
            while (nl := buf.find(b"\n")) == -1:
                sock.settimeout(max(timeout, 0))
                try:
                    chunk = sock.recv(4096)
                except TimeoutError:
                    return None
                if not chunk:
                    raise EOFError
                buf.extend(chunk)
            line = buf[:nl].decode(errors="replace").rstrip("\r")
            del buf[: nl + 1]
            return line

        yield read


def main() -> None:
    host = os.environ.get("DUMP1090_HOST", "localhost")
    port = int(os.environ.get("DUMP1090_PORT", "30003"))
    batch_size = int(os.environ.get("INGEST_BATCH_SIZE", "100"))
    flush_after = timedelta(seconds=float(os.environ.get("FLUSH_SECONDS", "5")))
    db = buffer.open(buffer.DB_PATH)

    try:
        while True:
            try:
                with connect(host, port) as read:
                    written = ingest(read, db, lambda: datetime.now(UTC), batch_size=batch_size, flush_after=flush_after)
                log.warning("stream closed after %d events", written)
            except OSError as e:
                log.warning("connection failed: %s", e)
            time.sleep(5)  # also covers a server that accepts then immediately closes
    finally:
        db.close()
        log.info("shut down")

