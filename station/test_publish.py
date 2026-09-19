import unittest
from urllib.error import URLError

from station import buffer
from station.buffer import Event
from station.publish import publish_once
from station.test_buffer import depth, fresh_buffer


class PublishOnceTest(unittest.TestCase):
    def setUp(self) -> None:
        _, self.db = fresh_buffer(self)
        self.posted: list[list[Event]] = []

    def capture(self, events: list[Event]) -> None:
        self.posted.append(events)

    def seed(self, *raws: str) -> None:
        buffer.append(self.db, [("t", r) for r in raws])

    def test_empty_buffer_posts_nothing(self) -> None:
        self.assertEqual(publish_once(self.db, self.capture, batch_size=10), 0)
        self.assertEqual(self.posted, [])

    def test_drains_in_id_order_and_acks_each_batch(self) -> None:
        self.seed("a", "b", "c")

        self.assertEqual(publish_once(self.db, self.capture, batch_size=2), 2)
        self.assertEqual(depth(self.db), 1)
        self.assertEqual(publish_once(self.db, self.capture, batch_size=2), 1)
        self.assertEqual(depth(self.db), 0)
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
        self.assertEqual(depth(self.db), 2)


if __name__ == "__main__":
    unittest.main()
