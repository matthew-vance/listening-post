import socket
import sqlite3
import tempfile
import threading
import time
import unittest
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

from station import buffer
from station.ingest import connect, ingest

FIXED = datetime(2026, 9, 13, 10, 0, 0, tzinfo=UTC)
BIG = 1000
NEVER = timedelta(hours=1)


def fixed_clock() -> datetime:
    return FIXED


def count(db: sqlite3.Connection) -> int:
    return db.execute("SELECT count(*) FROM events").fetchone()[0]


class IngestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        path = str(Path(self.tmp.name) / "events.db")
        buffer.create(path)
        self.db = buffer.open(path)
        self.reader = sqlite3.connect(path)  # separate connection: sees only committed rows
        self.addCleanup(self.db.close)
        self.addCleanup(self.reader.close)

    def test_writes_nonblank_lines_with_ts_and_autoincrement_id(self) -> None:
        lines = ["MSG,1,1,1,ABC123,1", "", "MSG,3,1,1,ABC123,1,,,,,35000"]

        written = ingest(lines, self.db, fixed_clock, batch_size=BIG, flush_after=NEVER)

        self.assertEqual(written, 2)
        rows = self.reader.execute("SELECT id, ts, raw FROM events ORDER BY id").fetchall()
        self.assertEqual(
            rows,
            [
                (1, FIXED.isoformat(), "MSG,1,1,1,ABC123,1"),
                (2, FIXED.isoformat(), "MSG,3,1,1,ABC123,1,,,,,35000"),
            ],
        )

    def test_commits_every_batch_size_lines(self) -> None:
        seen: list[int] = []

        def lines() -> Iterator[str]:
            for line in ["a", "b", "c"]:
                yield line
                seen.append(count(self.reader))

        ingest(lines(), self.db, fixed_clock, batch_size=2, flush_after=NEVER)

        # commit happens when the *next* item arrives after the batch fills
        self.assertEqual(seen, [0, 0, 2])
        self.assertEqual(count(self.reader), 3)

    def test_commits_after_flush_interval_even_on_heartbeats(self) -> None:
        clock = FIXED
        seen: list[int] = []

        def lines() -> Iterator[str]:
            nonlocal clock
            yield "a"
            seen.append(count(self.reader))
            clock += timedelta(seconds=6)
            yield ""  # idle heartbeat from connect()
            seen.append(count(self.reader))

        ingest(lines(), self.db, lambda: clock, batch_size=BIG, flush_after=timedelta(seconds=5))

        self.assertEqual(seen, [0, 1])

    def test_commits_pending_rows_when_stream_fails(self) -> None:
        def lines() -> Iterator[str]:
            yield "a"
            yield "b"
            raise OSError("connection reset")

        with self.assertRaises(OSError):
            ingest(lines(), self.db, fixed_clock, batch_size=BIG, flush_after=NEVER)

        self.assertEqual(count(self.reader), 2)


class ConnectTest(unittest.TestCase):
    def test_frames_lines_and_heartbeats_when_idle(self) -> None:
        server = socket.create_server(("localhost", 0))
        self.addCleanup(server.close)
        port = server.getsockname()[1]

        def serve() -> None:
            conn, _ = server.accept()
            with conn:
                conn.sendall(b"a\r\nb")  # partial second line
                time.sleep(0.3)  # longer than idle_timeout -> heartbeat
                conn.sendall(b"\nc\n")

        threading.Thread(target=serve, daemon=True).start()

        got = list(connect("localhost", port, idle_timeout=0.1))

        self.assertEqual([line for line in got if line], ["a", "b", "c"])
        self.assertIn("", got)
        self.assertEqual(got[0], "a")


if __name__ == "__main__":
    unittest.main()
