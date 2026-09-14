import json
import logging
import os
import signal
import socket
import sqlite3
import sys
import time
from collections.abc import Callable
from typing import TypedDict
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# Keep in sync with ingest/ingest.py. Duplicated on purpose: two deployables, no shared package.
SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts  TEXT    NOT NULL,
    raw TEXT    NOT NULL
)
"""

log = logging.getLogger("publish")


class Event(TypedDict):
    id: int
    ts: str
    raw: str


def open_db(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute(SCHEMA)  # publish may start before ingest has created the table
    db.commit()
    return db


def next_batch(db: sqlite3.Connection, limit: int) -> list[Event]:
    rows = db.execute("SELECT id, ts, raw FROM events ORDER BY id LIMIT ?", (limit,)).fetchall()
    return [Event(id=row["id"], ts=row["ts"], raw=row["raw"]) for row in rows]


def ack(db: sqlite3.Connection, upto_id: int) -> None:
    # publish is the only deleter and always drains from the head, so id <= is safe
    db.execute("DELETE FROM events WHERE id <= ?", (upto_id,))
    db.commit()


def post_events(url: str, station: str, events: list[Event], timeout: float) -> None:
    body = json.dumps({"station": station, "events": events}).encode()
    req = Request(f"{url}/v1/events", data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(req, timeout=timeout):  # raises HTTPError on non-2xx, URLError on connection failure
        pass


def publish_once(db: sqlite3.Connection, post: Callable[[list[Event]], None], batch_size: int) -> int:
    batch = next_batch(db, batch_size)
    if not batch:
        return 0
    post(batch)
    ack(db, batch[-1]["id"])
    log.info("published %d events (ids %d..%d)", len(batch), batch[0]["id"], batch[-1]["id"])
    return len(batch)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    url = os.environ.get("GATEWAY_URL", "http://localhost").rstrip("/")
    station = os.environ.get("STATION_ID", socket.gethostname())
    batch_size = int(os.environ.get("BATCH_SIZE", "500"))
    poll = float(os.environ.get("POLL_SECONDS", "2"))
    retry = float(os.environ.get("RETRY_SECONDS", "5"))
    db = open_db(os.environ.get("DB_PATH", "../events.db"))

    def post(events: list[Event]) -> None:
        post_events(url, station, events, timeout=10)

    log.info("publishing as station %s to %s", station, url)
    try:
        while True:
            try:
                if publish_once(db, post, batch_size) == 0:
                    time.sleep(poll)
            # ponytail: every failure retries forever, so a 4xx wedges the queue at its head.
            # Gateway and publish ship from one repo, so that's a deploy mismatch; add dead-lettering if it ever happens.
            except (HTTPError, URLError, TimeoutError) as e:
                log.warning("post failed: %s", e)
                time.sleep(retry)
    finally:
        db.close()
        log.info("shut down")


if __name__ == "__main__":
    # SystemExit unwinds through the loop like KeyboardInterrupt does, so both paths close the db.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        main()
    except KeyboardInterrupt:
        pass
