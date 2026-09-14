import unittest

from map import fold


class FoldTest(unittest.TestCase):
    def test_snapshot_then_tombstone(self) -> None:
        state: dict = {}
        fold('A22123\t{"icao": "A22123", "lat": 40.1}\n', state)
        self.assertEqual(state["A22123"]["lat"], 40.1)
        fold('A22123\t{"icao": "A22123", "lat": 40.2}\n', state)
        self.assertEqual(state["A22123"]["lat"], 40.2)
        fold("A22123\tnull\n", state)
        self.assertEqual(state, {})

    def test_garbage_is_skipped(self) -> None:
        state: dict = {}
        with self.assertLogs("map", level="WARNING"):
            fold("A22123\tnot json\n", state)
        fold("\n", state)
        self.assertEqual(state, {})
