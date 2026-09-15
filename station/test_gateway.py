import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError

from station.gateway import post_json


class PostJsonTest(unittest.TestCase):
    def setUp(self) -> None:
        self.requests: list[dict[str, object]] = []
        self.status = 200
        test = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers["Content-Length"]))
                test.requests.append(
                    {
                        "path": self.path,
                        "content_type": self.headers["Content-Type"],
                        "authorization": self.headers["Authorization"],
                        "body": json.loads(body),
                    }
                )
                self.send_response(test.status)
                self.end_headers()

            def log_message(self, *_: object) -> None:
                pass

        self.server = HTTPServer(("localhost", 0), Handler)
        threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.05), daemon=True).start()
        self.addCleanup(self.server.shutdown)
        self.url = f"http://localhost:{self.server.server_port}"

    def test_posts_json_with_bearer_token(self) -> None:
        post_json(self.url, "secret-token", "/v1/things", {"events": [{"id": 7, "raw": "MSG,3"}]}, timeout=2)

        self.assertEqual(
            self.requests,
            [
                {
                    "path": "/v1/things",
                    "content_type": "application/json",
                    "authorization": "Bearer secret-token",
                    "body": {"events": [{"id": 7, "raw": "MSG,3"}]},
                }
            ],
        )

    def test_non_2xx_raises(self) -> None:
        self.status = 500
        with self.assertRaises(HTTPError) as cm:
            post_json(self.url, "secret-token", "/v1/things", {}, timeout=2)
        cm.exception.close()  # HTTPError is file-like; unclosed it warns at GC


if __name__ == "__main__":
    unittest.main()
