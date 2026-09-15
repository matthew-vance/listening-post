import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from station.heartbeat import BufferStats, State, build_report, read_buffer, sample_buffer
from station.ingest import SCHEMA

T0 = datetime(2026, 9, 14, 14, 0, 0, tzinfo=UTC)


class ReadBufferTest(unittest.TestCase):
    def setUp(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.execute(SCHEMA)
        self.addCleanup(self.db.close)

    def test_empty_table(self) -> None:
        self.assertEqual(read_buffer(self.db), BufferStats(depth=0, oldest_ts=None, seq=0))

    def test_seq_survives_deletes(self) -> None:
        self.db.executemany("INSERT INTO events (ts, raw) VALUES (?, ?)", [("t1", "a"), ("t2", "b"), ("t3", "c")])
        self.db.execute("DELETE FROM events WHERE id = 1")
        self.db.commit()

        self.assertEqual(read_buffer(self.db), BufferStats(depth=2, oldest_ts="t2", seq=3))

    def test_sample_reports_empty_when_file_or_table_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, self.assertLogs("heartbeat", "WARNING") as logs:
            path = str(Path(tmp) / "events.db")
            self.assertEqual(sample_buffer(path).depth, 0)  # no file yet
            sqlite3.connect(path).close()  # file created by another process, table not yet
            self.assertEqual(sample_buffer(path).depth, 0)
        self.assertEqual(len(logs.output), 2)


class BuildReportTest(unittest.TestCase):
    def test_first_tick_has_no_rate_or_last_fields(self) -> None:
        stats = BufferStats(depth=5, oldest_ts="2026-09-14T13:59:00+00:00", seq=14)

        report, state = build_report(T0, uptime=100, disk_free=2**30, stats=stats, prev=None)

        self.assertEqual(
            report,
            {
                "reported_at": T0.isoformat(),
                "uptime_seconds": 100,
                "disk_free_bytes": 2**30,
                "buffer_depth": 5,
                "oldest_buffered_ts": "2026-09-14T13:59:00+00:00",
            },
        )
        self.assertEqual(state, State(at=T0, seq=14, depth=5, last_event_at=None, last_publish_at=None))

    def test_second_tick_reports_rate_and_activity(self) -> None:
        prev = State(at=T0, seq=10, depth=5, last_event_at=None, last_publish_at=None)
        now = T0 + timedelta(seconds=60)
        stats = BufferStats(depth=3, oldest_ts="x", seq=70)  # 60 arrived, 62 left

        report, state = build_report(now, uptime=1, disk_free=1, stats=stats, prev=prev)

        self.assertEqual(report["event_rate"], 1.0)
        self.assertEqual(report["last_event_ts"], now.isoformat())
        self.assertEqual(report["last_publish_ts"], now.isoformat())
        self.assertEqual(state.last_event_at, now)
        self.assertEqual(state.last_publish_at, now)

    def test_idle_tick_carries_last_fields_forward(self) -> None:
        earlier = T0 - timedelta(minutes=5)
        prev = State(at=T0, seq=70, depth=3, last_event_at=earlier, last_publish_at=earlier)
        now = T0 + timedelta(seconds=60)
        stats = BufferStats(depth=3, oldest_ts="x", seq=70)

        report, state = build_report(now, uptime=1, disk_free=1, stats=stats, prev=prev)

        self.assertEqual(report["event_rate"], 0.0)
        self.assertEqual(report["last_event_ts"], earlier.isoformat())
        self.assertEqual(report["last_publish_ts"], earlier.isoformat())
        self.assertEqual(state.last_event_at, earlier)

    def test_draining_to_empty_counts_as_publish(self) -> None:
        prev = State(at=T0, seq=70, depth=3, last_event_at=None, last_publish_at=None)
        now = T0 + timedelta(seconds=60)
        stats = BufferStats(depth=0, oldest_ts=None, seq=70)

        report, _ = build_report(now, uptime=1, disk_free=1, stats=stats, prev=prev)

        self.assertEqual(report["last_publish_ts"], now.isoformat())
        self.assertNotIn("oldest_buffered_ts", report)
        self.assertNotIn("last_event_ts", report)

    def test_recreated_buffer_resets_state(self) -> None:
        prev = State(at=T0, seq=500, depth=0, last_event_at=T0, last_publish_at=T0)
        stats = BufferStats(depth=2, oldest_ts="x", seq=2)

        report, state = build_report(T0 + timedelta(seconds=60), uptime=1, disk_free=1, stats=stats, prev=prev)

        self.assertNotIn("event_rate", report)
        self.assertIsNone(state.last_event_at)

    def test_publish_detected_when_buffer_is_empty_at_both_ticks(self) -> None:
        # publish drains faster than the heartbeat interval, so the buffer is never seen non-empty
        prev = State(at=T0, seq=70, depth=0, last_event_at=None, last_publish_at=None)
        now = T0 + timedelta(seconds=60)
        stats = BufferStats(depth=0, oldest_ts=None, seq=130)

        report, _ = build_report(now, uptime=1, disk_free=1, stats=stats, prev=prev)

        self.assertEqual(report["last_publish_ts"], now.isoformat())
        self.assertEqual(report["last_event_ts"], now.isoformat())


if __name__ == "__main__":
    unittest.main()
