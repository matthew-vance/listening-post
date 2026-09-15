import json
from typing import Any
from urllib.request import Request, urlopen


def post_json(url: str, token: str, path: str, body: dict[str, Any], timeout: float) -> None:
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    req = Request(url + path, data=json.dumps(body).encode(), headers=headers, method="POST")
    with urlopen(req, timeout=timeout):  # raises HTTPError on non-2xx, URLError on connection failure
        pass
