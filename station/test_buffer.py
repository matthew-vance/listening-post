import sqlite3
import tempfile
import unittest
from pathlib import Path

from station import buffer
from station.buffer import BufferStats


def fresh_buffer(tc: unittest.TestCase) -> tuple[str, sqlite3.Connection]:
    """A created, opened buffer in a temp dir (what the supervisor does before starting any loop), torn down with tc."""
    tmp = tempfile.TemporaryDirectory()
    tc.addCleanup(tmp.cleanup)
    path = str(Path(tmp.name) / "events.db")
    buffer.create(path)
    db = buffer.open(path)
    tc.addCleanup(db.close)
    return path, db


def depth(db: sqlite3.Connection) -> int:
    return buffer.sample(db).depth


class BufferTest(unittest.TestCase):
    def setUp(self) -> None:
        self.path, self.db = fresh_buffer(self)

    def test_create_is_idempotent_and_keeps_rows(self) -> None:
        buffer.append(self.db, [("t", "r")])
        buffer.create(self.path)  # a supervisor restart must not fail or drop rows
        self.assertEqual(buffer.sample(self.db).depth, 1)

    def test_create_sets_wal_for_every_connection(self) -> None:
        self.assertEqual(self.db.execute("PRAGMA journal_mode").fetchone()[0], "wal")

    def test_open_never_creates(self) -> None:
        with self.assertRaises(sqlite3.OperationalError):
            buffer.sample(buffer.open(self.path + ".missing"))

    def test_readonly_refuses_writes(self) -> None:
        ro = buffer.open(self.path, readonly=True)
        self.addCleanup(ro.close)
        with self.assertRaises(sqlite3.OperationalError):
            buffer.append(ro, [("t", "r")])

    def test_append_rejects_blank_raw_and_returns_rows_written(self) -> None:
        self.assertEqual(buffer.append(self.db, [("t1", ""), ("t2", "a"), ("t3", "")]), 1)
        self.assertEqual([e["raw"] for e in buffer.next_batch(self.db, 10)], ["a"])

    def test_append_then_drain_in_id_order(self) -> None:
        buffer.append(self.db, [("t1", "a"), ("t2", "b"), ("t3", "c")])

        batch = buffer.next_batch(self.db, 2)
        self.assertEqual(batch, [{"id": 1, "ts": "t1", "raw": "a"}, {"id": 2, "ts": "t2", "raw": "b"}])
        buffer.ack(self.db, batch)
        self.assertEqual(buffer.next_batch(self.db, 2), [{"id": 3, "ts": "t3", "raw": "c"}])

    def test_ack_deletes_only_the_given_batch(self) -> None:
        buffer.append(self.db, [("t1", "a"), ("t2", "b"), ("t3", "c")])
        buffer.ack(self.db, [{"id": 2, "ts": "t2", "raw": "b"}])
        self.assertEqual([e["id"] for e in buffer.next_batch(self.db, 10)], [1, 3])

    def test_append_is_one_transaction_visible_to_other_connections(self) -> None:
        reader = buffer.open(self.path, readonly=True)
        self.addCleanup(reader.close)
        buffer.append(self.db, [("t", "a"), ("t", "b")])
        self.assertEqual(buffer.sample(reader).depth, 2)

    def test_sample_empty(self) -> None:
        self.assertEqual(buffer.sample(self.db), BufferStats(depth=0, oldest_ts=None, seq=0))

    def test_sample_seq_survives_deletes(self) -> None:
        buffer.append(self.db, [("t1", "a"), ("t2", "b"), ("t3", "c")])
        buffer.ack(self.db, buffer.next_batch(self.db, 1))
        self.assertEqual(buffer.sample(self.db), BufferStats(depth=2, oldest_ts="t2", seq=3))


if __name__ == "__main__":
    unittest.main()
