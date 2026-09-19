"""Dev-only map of aircraft.state: folds the compacted topic into memory and serves it to map.html.

Reads via kafka-console-consumer in a throwaway kafka container so nothing needs installing on the host.
"""

import json
import logging
import os
import signal
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

log = logging.getLogger("map")

SEP = "\t"
PORT = 8082
TOPIC = os.environ.get("TOPIC", "aircraft.state")
# `docker run` (unlike `compose exec`) forwards signals into the container, so terminate() cleanly stops the consumer.
CONSUMER = [
    "docker", "run", "--rm", "--init", "--network", "listening-post_default", "apache/kafka:4.3.1",
    "/opt/kafka/bin/kafka-console-consumer.sh",
    "--bootstrap-server", "kafka:9092",
    "--topic", TOPIC,
    "--from-beginning",
    "--isolation-level", "read_committed",  # the processor writes transactionally
    "--formatter-property", "print.key=true",
    "--formatter-property", f"key.separator={SEP}",
]
HTML = (Path(__file__).parent / "map.html").read_bytes()
STATE: dict[str, dict[str, Any]] = {}
LOCK = threading.Lock()


def fold(line: str, state: dict[str, dict[str, Any]]) -> None:
    """Apply one `key<TAB>value` line from the console consumer; a null value is a tombstone."""
    icao, _, value = line.rstrip("\n").partition(SEP)
    if not icao:
        return
    if value == "null":
        state.pop(icao, None)
        return
    try:
        state[icao] = json.loads(value)
    except json.JSONDecodeError:
        log.warning("bad snapshot for %s: %r", icao, value[:80])


def consume(proc: subprocess.Popen[str]) -> None:
    assert proc.stdout is not None
    for line in proc.stdout:
        with LOCK:
            fold(line, STATE)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/state.json":
            with LOCK:
                snapshot = list(STATE.values())  # fold replaces values, never mutates them, so this is consistent
            self.respond("application/json", json.dumps(snapshot).encode())
        elif self.path == "/":
            self.respond("text/html", HTML)
        else:
            self.send_error(404)

    def respond(self, content_type: str, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass  # polling every 2s; access logs are noise


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, signal.default_int_handler)  # `kill` stops us the same way ctrl+c does
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)  # bind before spawning: a taken port must not leave a consumer behind
    proc = subprocess.Popen(CONSUMER, stdout=subprocess.PIPE, text=True)
    threading.Thread(target=consume, args=(proc,), daemon=True).start()
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log.info("map on http://localhost:%d", PORT)
    try:
        sys.exit(f"console consumer exited with {proc.wait()}")  # the map would silently go stale otherwise
    except KeyboardInterrupt:
        log.info("stopping")
        proc.terminate()
        proc.wait()


if __name__ == "__main__":
    main()
