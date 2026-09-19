import logging
import os
import sqlite3
import time
from collections.abc import Callable
from urllib.error import HTTPError

from station import buffer, gateway
from station.buffer import Event

log = logging.getLogger("publish")


def publish_once(db: sqlite3.Connection, post: Callable[[list[Event]], None], batch_size: int) -> int:
    batch = buffer.next_batch(db, batch_size)
    if not batch:
        return 0
    post(batch)
    buffer.ack(db, batch)
    log.info("published %d events (ids %d..%d)", len(batch), batch[0]["id"], batch[-1]["id"])
    return len(batch)


def main() -> None:
    batch_size = int(os.environ.get("PUBLISH_BATCH_SIZE", "500"))
    poll = float(os.environ.get("POLL_SECONDS", "2"))
    retry = float(os.environ.get("RETRY_SECONDS", "5"))
    db = buffer.open(buffer.DB_PATH)

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
                # a 4xx names what the gateway objected to; without the body all you see is the status
                body = e.read().decode(errors="replace") if isinstance(e, HTTPError) else ""
                log.warning("post failed: %s %s", e, body)
                time.sleep(retry)
    finally:
        db.close()
        log.info("shut down")

