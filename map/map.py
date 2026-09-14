"""Dev-only map of aircraft.state: folds the compacted topic into memory and serves it to map.html.

Reads via kafka-console-consumer inside the kafka container so nothing needs installing on the host.
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
CLIENT_ID = f"map-{os.getpid()}"  # tags the in-container process so shutdown can find and kill it
EXEC = ["docker", "compose", "exec", "-T", "kafka"]
CONSUMER = [
    *EXEC,
    "/opt/kafka/bin/kafka-console-consumer.sh",
    "--bootstrap-server", "localhost:9092",
    "--topic", os.environ.get("KAFKA_STATE", "aircraft.state"),
    "--from-beginning",
    "--command-property", f"client.id={CLIENT_ID}",
    "--formatter-property", "print.key=true",
    "--formatter-property", f"key.separator={SEP}",
]
ROOT = Path(__file__).parent.parent  # compose.yaml lives here


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


def consume(proc: subprocess.Popen[str], state: dict[str, dict[str, Any]], lock: threading.Lock) -> None:
    assert proc.stdout is not None
    for line in proc.stdout:
        with lock:
            fold(line, state)
    if proc.wait() != 0:
        log.error("console consumer exited with %s", proc.returncode)
        os._exit(1)  # the map would silently go stale; die so it's obvious


def server(port: int, state: dict[str, dict[str, Any]], lock: threading.Lock) -> ThreadingHTTPServer:
    html = (Path(__file__).parent / "map.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/state.json":
                with lock:
                    body = json.dumps(list(state.values())).encode()
                self.respond(200, "application/json", body)
            elif self.path == "/":
                self.respond(200, "text/html", html)
            else:
                self.respond(404, "text/plain", b"not found")

        def respond(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            pass  # polling every 2s; access logs are noise

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    signal.signal(signal.SIGTERM, signal.default_int_handler)  # `kill` stops us the same way ctrl+c does
    state: dict[str, dict[str, Any]] = {}
    lock = threading.Lock()
    port = int(os.environ.get("PORT", "8082"))
    srv = server(port, state, lock)  # bind before spawning: a taken port must not leave a consumer behind
    # Own process group so ctrl+c doesn't reach the exec client: it ignores signals and would leave the consumer
    # running inside the container anyway, so shutdown kills that directly and the client follows.
    proc = subprocess.Popen(CONSUMER, stdout=subprocess.PIPE, text=True, cwd=ROOT, start_new_session=True)
    threading.Thread(target=consume, args=(proc, state, lock), daemon=True).start()
    log.info("map on http://localhost:%d", port)
    with srv:
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            pass
    log.info("stopping")
    subprocess.run([*EXEC, "pkill", "-f", CLIENT_ID], cwd=ROOT, check=False)  # already gone is fine
    proc.wait()


if __name__ == "__main__":
    main()
