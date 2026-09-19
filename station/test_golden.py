"""The request bodies a station sends are pinned by internal/wire/testdata; the gateway's tests post the same files."""

import json
import unittest
from datetime import datetime
from pathlib import Path

from station import buffer
from station.buffer import BufferStats
from station.heartbeat import State, build_report
from station.publish import publish_once
from station.test_buffer import fresh_buffer

TESTDATA = Path(__file__).resolve().parents[1] / "internal" / "wire" / "testdata"


def load(name: str) -> object:
    return json.loads((TESTDATA / name).read_text())


def ts(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


class HeartbeatGoldenTest(unittest.TestCase):
    def test_build_report_produces_the_golden_bodies(self) -> None:
        for case in load("heartbeat.json"):
            with self.subTest(case["name"]):
                i = case["inputs"]
                prev = i["prev"] and State(
                    at=ts(i["prev"]["at"]), seq=i["prev"]["seq"], depth=i["prev"]["depth"],
                    last_event_at=ts(i["prev"]["last_event_at"]), last_publish_at=ts(i["prev"]["last_publish_at"]),
                )
                report, _ = build_report(ts(i["now"]), i["uptime"], i["disk_free"], BufferStats(**i["stats"]), prev)
                self.assertEqual(report, case["body"])


class EventsGoldenTest(unittest.TestCase):
    def test_publish_posts_the_golden_body(self) -> None:
        case = load("events.json")
        _, db = fresh_buffer(self)
        buffer.append(db, [(r["ts"], r["raw"]) for r in case["rows"]])
        posted: list[list[object]] = []
        publish_once(db, posted.append, batch_size=10)
        self.assertEqual(posted, [case["body"]["events"]])  # main() wraps the batch as {"events": ...}


if __name__ == "__main__":
    unittest.main()
