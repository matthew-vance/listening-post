"""Supervisor: runs ingest, publish, and heartbeat as child processes and restarts any that die.

Separate processes, not threads: each keeps its own SQLite connection and its own SIGTERM -> SystemExit
unwinding (which is what flushes ingest's pending batch), and heartbeat keeps reporting while another loop
is down or restarting.
"""

import logging
import multiprocessing
import os
import signal
import sys
import time
from collections.abc import Callable
from multiprocessing.connection import wait

from station import buffer, heartbeat, ingest, publish

log = logging.getLogger("station")

TARGETS: dict[str, Callable[[], None]] = {"ingest": ingest.main, "publish": publish.main, "heartbeat": heartbeat.main}


def setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def install_graceful_exit() -> None:
    # SIGTERM -> SystemExit unwinds through a loop's finally, which is what flushes ingest's pending batch.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))


def child(target: Callable[[], None]) -> None:
    # Runs in the child. Under spawn/forkserver (macOS, Python 3.14+ Linux) it inherits neither the parent's
    # logging config nor its signal handlers, so both are set here.
    setup_logging()
    install_graceful_exit()
    try:
        target()
    except KeyboardInterrupt:  # Ctrl-C reaches the whole process group
        pass


def start(name: str, target: Callable[[], None]) -> multiprocessing.Process:
    p = multiprocessing.Process(target=child, args=(target,), name=name)
    p.start()
    log.info("started %s (pid %d)", name, p.pid)
    return p


def supervise(targets: dict[str, Callable[[], None]], restart_after: float) -> None:
    """Run every target until SIGINT/SIGTERM, restarting any that exits. Never returns on its own."""
    procs = {name: start(name, target) for name, target in targets.items()}
    install_graceful_exit()
    try:
        while True:
            wait([p.sentinel for p in procs.values()])  # blocks until at least one child exits
            for name, p in procs.items():
                if not p.is_alive():
                    # ponytail: fixed delay; add backoff if a child ever flaps.
                    log.warning("%s exited with %s; restarting in %ss", name, p.exitcode, restart_after)
                    time.sleep(restart_after)
                    procs[name] = start(name, targets[name])
    finally:
        for p in procs.values():
            p.terminate()  # SIGTERM -> child's sys.exit -> its finally runs
        for p in procs.values():
            p.join(10)
            if p.is_alive():  # a child stuck in a flush ignores SIGTERM; don't orphan it
                log.error("%s still alive after SIGTERM; killing", p.name)
                p.kill()
                p.join()
        log.info("shut down")


def main() -> None:
    setup_logging()
    # Checked once here rather than in publish/heartbeat: a missing token must fail loudly, not restart-loop.
    if not os.environ.get("STATION_TOKEN"):
        log.error("STATION_TOKEN is not set; mint one with `just station-add`")
        sys.exit(1)
    buffer.create(buffer.DB_PATH)  # before any child opens it; the children never create
    try:
        supervise(TARGETS, float(os.environ.get("RESTART_SECONDS", "5")))
    except KeyboardInterrupt:
        pass

