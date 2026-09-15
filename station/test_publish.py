import sqlite3
import tempfile
import unittest
from pathlib import Path
from urllib.error import URLError

from station import ingest
from station.publish import Event, open_db, publish_once


def count(db: sqlite3.Connection) -> int:
    return db.execute("SELECT count(*) FROM events").fetchone()[0]


class PublishOnceTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = str(Path(tmp.name) / "events.db")
        ingest.open_db(path).close()  # the supervisor does this before starting publish
        self.db = open_db(path)
        self.addCleanup(self.db.close)
        self.posted: list[list[Event]] = []

    def capture(self, events: list[Event]) -> None:
        self.posted.append(events)

    def seed(self, *raws: str) -> None:
        self.db.executemany("INSERT INTO events (ts, raw) VALUES (?, ?)", [("t", r) for r in raws])
        self.db.commit()

    def test_empty_buffer_posts_nothing(self) -> None:
        self.assertEqual(publish_once(self.db, self.capture, batch_size=10), 0)
        self.assertEqual(self.posted, [])

    def test_drains_in_id_order_and_acks_each_batch(self) -> None:
        self.seed("a", "b", "c")

        self.assertEqual(publish_once(self.db, self.capture, batch_size=2), 2)
        self.assertEqual(count(self.db), 1)
        self.assertEqual(publish_once(self.db, self.capture, batch_size=2), 1)
        self.assertEqual(count(self.db), 0)
        self.assertEqual(publish_once(self.db, self.capture, batch_size=2), 0)

        self.assertEqual(
            self.posted,
            [[{"id": 1, "ts": "t", "raw": "a"}, {"id": 2, "ts": "t", "raw": "b"}], [{"id": 3, "ts": "t", "raw": "c"}]],
        )

    def test_failed_post_keeps_rows(self) -> None:
        self.seed("a", "b")

        def failing(_: list[Event]) -> None:
            raise URLError("connection refused")

        with self.assertRaises(URLError):
            publish_once(self.db, failing, batch_size=10)
        self.assertEqual(count(self.db), 2)


if __name__ == "__main__":
    unittest.main()
