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
        self.reply = b""
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
                self.send_header("Content-Length", str(len(test.reply)))
                self.end_headers()
                self.wfile.write(test.reply)

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

    def test_non_2xx_raises_with_the_body_in_the_message(self) -> None:
        self.status, self.reply = 422, b'{"problems":{"events[4].raw":"must not be empty"}}'
        with self.assertRaises(HTTPError) as cm:
            post_json(self.url, "secret-token", "/v1/things", {}, timeout=2)
        self.assertEqual(cm.exception.code, 422)
        self.assertTrue(str(cm.exception).endswith(' {"problems":{"events[4].raw":"must not be empty"}}'), str(cm.exception))


if __name__ == "__main__":
    unittest.main()
