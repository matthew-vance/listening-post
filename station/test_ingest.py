import socket
import sqlite3
import tempfile
import threading
import time
import unittest
from collections.abc import Callable
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
    return buffer.sample(db).depth


class ScriptedReader:
    """A read(timeout) that plays back a script of lines, None (nothing within the timeout), or EOFError,
    recording the timeouts ingest asked for."""

    def __init__(self, *script: str | None | type[EOFError]) -> None:
        self.script = list(script)
        self.timeouts: list[float] = []
        self.on_read: Callable[[], None] = lambda: None  # hook run before each read, for observing commits

    def __call__(self, timeout: float) -> str | None:
        self.on_read()
        self.timeouts.append(timeout)
        if not self.script:
            raise EOFError
        item = self.script.pop(0)
        if item is EOFError:
            raise EOFError
        return item


class IngestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        path = str(Path(self.tmp.name) / "events.db")
        buffer.create(path)
        self.db = buffer.open(path)
        self.reader = buffer.open(path, readonly=True)  # separate connection: sees only committed rows
        self.addCleanup(self.db.close)
        self.addCleanup(self.reader.close)

    def test_writes_lines_with_ts_and_autoincrement_id_and_returns_count_at_eof(self) -> None:
        read = ScriptedReader("MSG,1,1,1,ABC123,1", "MSG,3,1,1,ABC123,1,,,,,35000")

        written = ingest(read, self.db, fixed_clock, batch_size=BIG, flush_after=NEVER)

        self.assertEqual(written, 2)
        self.assertEqual(
            buffer.next_batch(self.reader, 10),
            [
                {"id": 1, "ts": FIXED.isoformat(), "raw": "MSG,1,1,1,ABC123,1"},
                {"id": 2, "ts": FIXED.isoformat(), "raw": "MSG,3,1,1,ABC123,1,,,,,35000"},
            ],
        )

    def test_counts_only_rows_the_buffer_kept(self) -> None:
        read = ScriptedReader("", "MSG,1,1,1,ABC123,1", "")

        written = ingest(read, self.db, fixed_clock, batch_size=BIG, flush_after=NEVER)

        self.assertEqual(written, 1)

    def test_commits_every_batch_size_lines(self) -> None:
        read = ScriptedReader("a", "b", "c")
        seen: list[int] = []
        read.on_read = lambda: seen.append(count(self.reader))

        ingest(read, self.db, fixed_clock, batch_size=2, flush_after=NEVER)

        # commit happens when the *next* item is about to be read after the batch fills
        self.assertEqual(seen, [0, 0, 2, 2])
        self.assertEqual(count(self.reader), 3)

    def test_bounds_each_read_by_the_time_left_to_the_next_flush(self) -> None:
        clock = FIXED
        read = ScriptedReader("a", None, "b")
        seen: list[int] = []

        def before_read() -> None:
            nonlocal clock
            seen.append(count(self.reader))
            if read.script and read.script[0] is None:
                clock += timedelta(seconds=6)  # the None comes back after the window has passed

        read.on_read = before_read
        ingest(read, self.db, lambda: clock, batch_size=BIG, flush_after=timedelta(seconds=5))

        # nothing pending: wait a full window; "a" pending at t+0: still the full window; after the None at t+6 the
        # pending row is flushed before the next read
        self.assertEqual(read.timeouts[:2], [5.0, 5.0])
        self.assertEqual(seen, [0, 0, 1, 1])

    def test_asks_for_the_remaining_window_not_a_fresh_one(self) -> None:
        clock = FIXED
        read = ScriptedReader("a", "b")

        def before_read() -> None:
            nonlocal clock
            if read.script == ["b"]:
                clock += timedelta(seconds=3)  # "b" arrives 3s into the window opened by "a"

        read.on_read = before_read
        ingest(read, self.db, lambda: clock, batch_size=BIG, flush_after=timedelta(seconds=5))

        self.assertEqual(read.timeouts, [5.0, 5.0, 2.0])

    def test_commits_pending_rows_when_stream_fails(self) -> None:
        read = ScriptedReader("a", "b")
        read.script.append("boom")

        def failing(timeout: float) -> str | None:
            if read.script == ["boom"]:
                raise OSError("connection reset")
            return read(timeout)

        with self.assertRaises(OSError):
            ingest(failing, self.db, fixed_clock, batch_size=BIG, flush_after=NEVER)

        self.assertEqual(count(self.reader), 2)


class ConnectTest(unittest.TestCase):
    def test_frames_lines_times_out_and_reports_eof(self) -> None:
        server = socket.create_server(("localhost", 0))
        self.addCleanup(server.close)
        port = server.getsockname()[1]
        closed = threading.Event()

        def serve() -> None:
            conn, _ = server.accept()
            with conn:
                conn.sendall(b"a\r\nb")  # partial second line
                time.sleep(0.3)  # longer than the read timeout below
                conn.sendall(b"\nc\n")
                closed.wait()

        threading.Thread(target=serve, daemon=True).start()

        with connect("localhost", port) as read:
            self.assertEqual(read(1.0), "a")
            self.assertIsNone(read(0.1))  # "b" is still partial
            self.assertEqual(read(1.0), "b")
            self.assertEqual(read(1.0), "c")
            closed.set()
            with self.assertRaises(EOFError):
                read(1.0)


if __name__ == "__main__":
    unittest.main()
