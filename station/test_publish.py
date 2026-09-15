import json
import sqlite3
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError

from station.publish import Event, open_db, post_events, publish_once


def count(db: sqlite3.Connection) -> int:
    return db.execute("SELECT count(*) FROM events").fetchone()[0]


class PublishOnceTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = open_db(str(Path(tmp.name) / "events.db"))
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


class PostEventsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.requests: list[dict[str, object]] = []
        self.status = 200
        test = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers["Content-Length"]))
                test.requests.append(
                    {
                        "path": self.path,
                        "content_type": self.headers["Content-Type"],
                        "authorization": self.headers["Authorization"],
                        "body": json.loads(body),
                    }
                )
                self.send_response(test.status)
                self.end_headers()

            def log_message(self, *_: object) -> None:
                pass

        self.server = HTTPServer(("localhost", 0), Handler)
        threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.05), daemon=True).start()
        self.addCleanup(self.server.shutdown)
        self.url = f"http://localhost:{self.server.server_port}"

    def test_posts_json_batch(self) -> None:
        post_events(self.url, "secret-token", [Event(id=7, ts="2026-09-13T10:00:00+00:00", raw="MSG,3")], timeout=2)

        self.assertEqual(
            self.requests,
            [
                {
                    "path": "/v1/events",
                    "content_type": "application/json",
                    "authorization": "Bearer secret-token",
                    "body": {"events": [{"id": 7, "ts": "2026-09-13T10:00:00+00:00", "raw": "MSG,3"}]},
                }
            ],
        )

    def test_non_2xx_raises(self) -> None:
        self.status = 500
        with self.assertRaises(HTTPError) as cm:
            post_events(self.url, "secret-token", [Event(id=1, ts="t", raw="r")], timeout=2)
        cm.exception.close()  # HTTPError is file-like; unclosed it warns at GC


if __name__ == "__main__":
    unittest.main()
