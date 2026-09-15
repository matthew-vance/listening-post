import logging
import multiprocessing
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from station.supervisor import supervise

MARK = Path(tempfile.gettempdir()) / "station-test-mark"  # fixed name: spawn re-imports this module in the child


def touch_and_exit() -> None:  # module-level so it pickles under spawn
    with MARK.open("a") as f:
        f.write("x\n")


def quiet_supervise(targets, restart_after: float) -> None:
    logging.disable(logging.CRITICAL)  # the restart warnings are expected; keep them out of the test output
    supervise(targets, restart_after)


class SuperviseTest(unittest.TestCase):
    def test_restarts_dead_child_and_stops_on_terminate(self) -> None:
        MARK.unlink(missing_ok=True)
        sup = multiprocessing.Process(target=quiet_supervise, args=({"x": touch_and_exit}, 0.0))
        sup.start()
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and (not MARK.exists() or MARK.read_text().count("x") < 2):
                time.sleep(0.05)
            self.assertGreaterEqual(MARK.read_text().count("x"), 2, "child was not restarted")
        finally:
            sup.terminate()
            sup.join(10)
            MARK.unlink(missing_ok=True)
        self.assertEqual(sup.exitcode, 0, "SIGTERM should unwind cleanly through the finally")


class EntrypointTest(unittest.TestCase):
    def test_missing_token_exits_1(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "STATION_TOKEN"}
        proc = subprocess.run([sys.executable, "-m", "station"], env=env, capture_output=True, text=True, cwd=Path(__file__).parent.parent)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("STATION_TOKEN", proc.stderr)


if __name__ == "__main__":
    unittest.main()
