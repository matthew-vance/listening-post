"""The buffer: the SQLite file that holds events between ingest and a gateway 200.

Every station loop touches it, so this module is the only place that knows the schema. The supervisor
creates it once (create); ingest appends, publish drains, heartbeat samples, each through its own connection.
"""

import os
import sqlite3
from dataclasses import dataclass
from typing import TypedDict

DB_PATH = os.environ.get("DB_PATH", "events.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts  TEXT    NOT NULL,
    raw TEXT    NOT NULL
)
"""


class Event(TypedDict):
    id: int
    ts: str
    raw: str


@dataclass(frozen=True)
class BufferStats:
    depth: int
    oldest_ts: str | None
    seq: int  # last id ever assigned; survives deletes, so it measures arrivals


def create(path: str) -> None:
    """Create the file, schema, and WAL mode. Run once by the supervisor before any loop opens the buffer:
    switching to WAL needs an exclusive lock, which fails if another connection is mid-statement."""
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")  # persistent in the file: publish reads while ingest writes
    db.execute(SCHEMA)
    db.commit()
    db.close()


def open(path: str, *, readonly: bool = False) -> sqlite3.Connection:
    """Open an existing buffer; never creates it, so a loop started before the supervisor fails loudly.
    readonly connections can't take a write lock, which is what heartbeat wants."""
    if readonly:
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    else:
        db = sqlite3.connect(path)
        db.execute("PRAGMA synchronous=NORMAL")  # fsync at checkpoint, not per commit; only power loss can lose rows
    db.row_factory = sqlite3.Row
    return db


def append(db: sqlite3.Connection, rows: list[tuple[str, str]]) -> int:
    """Insert (ts, raw) rows in one short transaction so the write lock is held for milliseconds; returns how many.

    This is the only writer, so it is where a row is rejected: publish trusts every row it reads. A blank raw
    (the feed emits one around a reconnect) would be 422'd by the gateway and wedge the queue at its head.
    """
    rows = [row for row in rows if row[1]]
    db.executemany("INSERT INTO events (ts, raw) VALUES (?, ?)", rows)
    db.commit()
    return len(rows)


def next_batch(db: sqlite3.Connection, limit: int) -> list[Event]:
    rows = db.execute("SELECT id, ts, raw FROM events ORDER BY id LIMIT ?", (limit,)).fetchall()
    return [Event(**row) for row in rows]


def ack(db: sqlite3.Connection, batch: list[Event]) -> None:
    """Delete exactly the events of a batch next_batch handed out, in one transaction."""
    db.executemany("DELETE FROM events WHERE id = ?", [(e["id"],) for e in batch])
    db.commit()


def sample(db: sqlite3.Connection) -> BufferStats:
    # One statement = one snapshot, so depth and seq can't straddle an ingest commit.
    depth, oldest_ts, seq = db.execute(
        """
        SELECT (SELECT count(*) FROM events),
               (SELECT ts FROM events ORDER BY id LIMIT 1),
               (SELECT seq FROM sqlite_sequence WHERE name = 'events')
        """
    ).fetchone()
    return BufferStats(depth=depth, oldest_ts=oldest_ts, seq=seq or 0)
