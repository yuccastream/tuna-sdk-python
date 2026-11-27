from __future__ import annotations

import asyncio
import contextlib
import json
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from email.message import Message
from typing import Any, TypeVar

API_TIMEOUT = 15.0
MAX_RESPONSE_SIZE = 1 << 20
MAX_RETRY_AFTER = 300.0

_PERMANENT_CODES = frozenset(
    {
        "PaymentRequired",
        "Unauthorized",
        "AuthorizationRequired",
        "Forbidden",
        "BadRequest",
        "NotFound",
        "UpgradeRequired",
    }
)

T = TypeVar("T")
Headers = Mapping[str, str] | Message


class ServerError(Exception):
    """An error response from the tuna.am API."""

    status: int
    """HTTP status code."""
    code: str
    """Machine-readable error code."""
    message: str
    """Human-readable error text."""
    request_id: str
    """Identifies the request for tuna.am support."""
    retry_after: float
    """Delay in seconds requested by the server, zero if none."""

    def __init__(self, status: int, code: str = "", message: str = "", request_id: str = "", retry_after: float = 0.0):
        super().__init__(status, code, message, request_id, retry_after)
        self.status = status
        self.code = code
        self.message = message
        self.request_id = request_id
        self.retry_after = retry_after
        self.before_first_connect = False

    def __str__(self) -> str:
        if self.request_id:
            return f"tuna: {self.message} (request id {self.request_id})"
        return f"tuna: {self.message}"

    def permanent(self) -> bool:
        if 300 <= self.status < 400:
            return True
        if self.before_first_connect and 400 <= self.status < 500 and self.status not in (408, 429):
            return True
        return self.code in _PERMANENT_CODES


def check_server_url(url: str) -> None:
    u = urllib.parse.urlsplit(url)
    if u.scheme not in ("https", "http") or not u.netloc:
        raise ValueError(f'tuna: invalid server URL "{url}": want https://host')


def server_error(status: int, headers: Headers, body: bytes) -> ServerError:
    se = ServerError(status)
    if 300 <= status < 400:
        se.message = f'server redirects to "{headers.get("Location", "")}", check the server URL'
    else:
        try:
            data = json.loads(body)
        except ValueError:
            data = None
        if isinstance(data, dict):
            se.code = str(data.get("code") or "")
            se.message = str(data.get("error") or "")
            se.request_id = str(data.get("request_id") or "")
        if not se.message:
            se.message = f"server responded with status {status}"
    try:
        sec = int(headers.get("Retry-After", ""))
    except ValueError:
        sec = 0
    if sec > 0:
        se.retry_after = min(float(sec), MAX_RETRY_AFTER)
    return se


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def _post(url: str, body: bytes, user_agent: str, context: ssl.SSLContext | None) -> tuple[int, Headers, bytes]:
    opener = urllib.request.build_opener(_NoRedirect, urllib.request.HTTPSHandler(context=context))
    req = urllib.request.Request(
        url, data=body, method="POST", headers={"User-Agent": user_agent, "Content-Type": "application/json"}
    )
    try:
        with opener.open(req, timeout=API_TIMEOUT) as resp:
            return resp.status, resp.headers, resp.read(MAX_RESPONSE_SIZE)
    except urllib.error.HTTPError as e:
        with e:
            return e.code, e.headers, e.read(MAX_RESPONSE_SIZE)


async def in_thread(fn: Callable[..., T], *args: Any) -> T:
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[T] = loop.create_future()

    def settle(result: Any, exc: BaseException | None) -> None:
        if fut.done():
            return
        if exc is not None:
            fut.set_exception(exc)
        else:
            fut.set_result(result)

    def work() -> None:
        try:
            result, exc = fn(*args), None
        except Exception as e:
            result, exc = None, e
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(settle, result, exc)

    threading.Thread(target=work, daemon=True).start()
    return await fut


async def post_json(
    server_url: str, path: str, user_agent: str, payload: Any, context: ssl.SSLContext | None
) -> dict[str, Any]:
    url = server_url.rstrip("/") + path
    status, headers, body = await in_thread(_post, url, json.dumps(payload).encode(), user_agent, context)
    if not 200 <= status <= 299:
        raise server_error(status, headers, body)
    data = json.loads(body)
    if not isinstance(data, dict):
        raise ValueError("tuna: unexpected response from the server")
    return data
