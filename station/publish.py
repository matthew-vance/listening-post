import logging
import os
import sqlite3
import time
from collections.abc import Callable
from typing import TypedDict

from station import gateway
from station.ingest import DB_PATH

log = logging.getLogger("publish")


class Event(TypedDict):
    id: int
    ts: str
    raw: str


def open_db(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row  # schema and WAL mode are set up once by the supervisor
    return db


def next_batch(db: sqlite3.Connection, limit: int) -> list[Event]:
    rows = db.execute("SELECT id, ts, raw FROM events ORDER BY id LIMIT ?", (limit,)).fetchall()
    return [Event(id=row["id"], ts=row["ts"], raw=row["raw"]) for row in rows]


def ack(db: sqlite3.Connection, upto_id: int) -> None:
    # publish is the only deleter and always drains from the head, so id <= is safe
    db.execute("DELETE FROM events WHERE id <= ?", (upto_id,))
    db.commit()


def publish_once(db: sqlite3.Connection, post: Callable[[list[Event]], None], batch_size: int) -> int:
    batch = next_batch(db, batch_size)
    if not batch:
        return 0
    post(batch)
    ack(db, batch[-1]["id"])
    log.info("published %d events (ids %d..%d)", len(batch), batch[0]["id"], batch[-1]["id"])
    return len(batch)


def main() -> None:
    batch_size = int(os.environ.get("PUBLISH_BATCH_SIZE", "500"))
    poll = float(os.environ.get("POLL_SECONDS", "2"))
    retry = float(os.environ.get("RETRY_SECONDS", "5"))
    db = open_db(DB_PATH)

    def post(events: list[Event]) -> None:
        gateway.post_json(gateway.URL, gateway.TOKEN, "/v1/events", {"events": events})

    log.info("publishing to %s", gateway.URL)
    try:
        while True:
            try:
                if publish_once(db, post, batch_size) == 0:
                    time.sleep(poll)
            # ponytail: every failure retries forever, so a 4xx wedges the queue at its head.
            # Gateway and publish ship from one repo, so that's a deploy mismatch; add dead-lettering if it ever happens.
            except gateway.POST_ERRORS as e:
                log.warning("post failed: %s", e)
                time.sleep(retry)
    finally:
        db.close()
        log.info("shut down")

