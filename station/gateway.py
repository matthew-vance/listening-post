import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

URL = os.environ.get("GATEWAY_URL", "http://localhost").rstrip("/")
TOKEN = os.environ.get("STATION_TOKEN", "")  # presence is checked once, by the supervisor

# What post_json raises on failure; callers retry on exactly these.
POST_ERRORS = (URLError, TimeoutError)


def post_json(url: str, token: str, path: str, body: dict[str, Any], timeout: float = 10) -> None:
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    req = Request(url + path, data=json.dumps(body).encode(), headers=headers, method="POST")
    try:
        with urlopen(req, timeout=timeout):  # raises HTTPError on non-2xx, URLError on connection failure
            pass
    except HTTPError as e:
        with e:  # file-like; the body names what the gateway objected to, which the status alone doesn't
            e.msg = f"{e.msg} {e.read().decode(errors='replace')}"
        raise
