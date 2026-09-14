import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from ingest import ingest, open_db

FIXED = datetime(2026, 9, 13, 10, 0, 0, tzinfo=UTC)


def fixed_clock() -> datetime:
    return FIXED


class OpenDbTest(unittest.TestCase):
    def test_creates_table_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "events.db")
            first = open_db(path)
            first.execute("INSERT INTO events (ts, raw) VALUES (?, ?)", ("t", "r"))
            first.commit()
            first.close()

            second = open_db(path)  # reopening must not fail or drop rows
            self.assertEqual(second.execute("SELECT count(*) FROM events").fetchone()[0], 1)
            second.close()


class IngestTest(unittest.TestCase):
    def test_writes_nonblank_lines_with_ts_and_autoincrement_id(self) -> None:
        db = open_db(":memory:")
        lines = ["MSG,1,1,1,ABC123,1", "", "MSG,3,1,1,ABC123,1,,,,,35000"]

        written = ingest(lines, db, fixed_clock)

        self.assertEqual(written, 2)
        rows = db.execute("SELECT id, ts, raw FROM events ORDER BY id").fetchall()
        self.assertEqual(
            rows,
            [
                (1, FIXED.isoformat(), "MSG,1,1,1,ABC123,1"),
                (2, FIXED.isoformat(), "MSG,3,1,1,ABC123,1,,,,,35000"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
