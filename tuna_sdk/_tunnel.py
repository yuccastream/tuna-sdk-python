from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import os
import platform
import random
import ssl
import sys
import unicodedata
from collections.abc import AsyncIterator, Callable, Coroutine, Generator
from typing import Any, Literal

import asyncssh

from ._api import ServerError, check_server_url, post_json

VERSION = "0.1.0"
"""The SDK version."""
DEFAULT_SERVER_URL = "https://api.tuna.am"
"""The tuna.am API used when Options.server_url is empty."""

CLIENT_VERSION = 2
SSH_USER = "tuna"
DIAL_TIMEOUT = 10.0
EVENTS_DRAIN_TIMEOUT = 1.0
EVENTS_COMMAND = "events"
MIN_BACKOFF = 1.0
MAX_BACKOFF = 15.0
KEEPALIVE_INTERVAL = 5.0
KEEPALIVE_COUNT_MAX = 2

EventType = Literal[
    "connected", "reconnected", "reconnecting", "banner", "session_expiring", "terminated", "local_error"
]

Stream = tuple["asyncssh.SSHReader[bytes]", "asyncssh.SSHWriter[bytes]"]

_background: set[asyncio.Future[Any]] = set()


def spawn(coro: Coroutine[Any, Any, Any]) -> asyncio.Future[Any]:
    task = asyncio.ensure_future(coro)
    _background.add(task)
    task.add_done_callback(_background.discard)
    return task


@dataclasses.dataclass(frozen=True)
class Event:
    """A change in the tunnel state."""

    type: str
    """One of connected, reconnected, reconnecting, banner, session_expiring, terminated, local_error."""
    url: str = ""
    """Public address, set for connected and reconnected."""
    message: str = ""
    """Human-readable text, set for banner and terminated."""
    reason: str = ""
    """Machine-readable cause, set for terminated."""
    error: BaseException | None = None
    """The cause, set for reconnecting and local_error."""
    retry_in: float = 0.0
    """Seconds before the next attempt, set for reconnecting."""
    expires_in: float = 0.0
    """Seconds left, set for session_expiring."""


@dataclasses.dataclass
class Options:
    """Tunnel settings. Only protocol is required."""

    protocol: str
    """ "http" or "tcp"."""
    api_key: str = ""
    """API key with the tunnels:connect scope. Defaults to the TUNA_API_KEY environment variable."""
    location: str = ""
    """Only for names and ports reserved in the legacy "ru" or "nl" zones. Empty lets the server pick."""
    subdomain: str = ""
    """Reserved subdomain for HTTP tunnels."""
    domain: str = ""
    """Custom domain for HTTP tunnels."""
    port: str = ""
    """Reserved TCP port or its alias."""
    proxy_protocol: int = 0
    """PROXY protocol header version (1 or 2) for TCP tunnels."""
    cidr_allow: list[str] = dataclasses.field(default_factory=list)
    """Client CIDRs allowed to reach a TCP tunnel."""
    cidr_deny: list[str] = dataclasses.field(default_factory=list)
    """Client CIDRs rejected by a TCP tunnel."""
    server_url: str = ""
    """The tuna.am API address. Defaults to DEFAULT_SERVER_URL."""
    ssl_context: ssl.SSLContext | None = None
    """TLS settings for the API. None means the system roots."""
    client_name: str = ""
    """Client name reported to the server. Defaults to "tuna-sdk-python/<VERSION>"."""
    connect_timeout: float | None = None
    """Seconds to wait for the tunnel to come up for the first time. None means no limit."""
    on_event: Callable[[Event], None] | None = None
    """Receives tunnel events on the event loop. Calls never overlap; exceptions are ignored."""


class TerminatedError(Exception):
    """The server closed the tunnel for good."""

    reason: str
    """Machine-readable cause, e.g. "deleted" or "session_limit"."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason

    def __str__(self) -> str:
        return "tuna: " + self.message

    @property
    def message(self) -> str:
        """Human-readable cause."""
        return _TERMINATE_MESSAGES.get(self.reason, f"tunnel terminated by server ({self.reason})")


_TERMINATE_MESSAGES = {
    "deleted": "tunnel was deleted",
    "suspended": "tunnel was suspended",
    "session_limit": "session time limit reached",
    "session_ended": "client session ended",
    "reconnect": "tunnel was taken over by another connection",
}


class KeepAliveTimeoutError(ConnectionError):
    """The node stopped answering keepalive requests."""

    def __init__(self) -> None:
        super().__init__("tuna: keepalive timeout")


class ClosedError(Exception):
    """The tunnel is closed."""

    def __init__(self) -> None:
        super().__init__("tuna: tunnel is closed")


def is_permanent(err: BaseException | None) -> bool:
    """Reports whether retrying after err is pointless."""
    return isinstance(err, TerminatedError) or (isinstance(err, ServerError) and err.permanent())


def retry_delay(backoff: float, err: BaseException | None) -> float:
    wait = min(MAX_BACKOFF, MIN_BACKOFF + random.uniform(0, backoff))
    if isinstance(err, ServerError) and err.retry_after > wait:
        wait = err.retry_after
    return wait


def public_url(cr: dict[str, Any], protocol: str) -> str:
    if cr.get("public_url"):
        return str(cr["public_url"])
    if protocol == "tcp":
        return f"tcp://{cr.get('domain_name', '')}:{cr.get('endpoint_port', 0)}"
    return "https://" + str(cr.get("domain_name", ""))


def split_host_port(addr: str) -> tuple[str, int]:
    host, sep, port = addr.rpartition(":")
    if not sep or not port.isdigit():
        raise ValueError(f'tuna: address "{addr}" is not host:port')
    return host.removeprefix("[").removesuffix("]"), int(port)


def parse_event(line: str) -> dict[str, Any] | None:
    try:
        ev = json.loads(line)
    except ValueError:
        return None
    return ev if isinstance(ev, dict) else None


def _os_arch() -> tuple[str, str]:
    goos = {"win32": "windows", "cygwin": "windows"}.get(sys.platform, sys.platform)
    goos = "linux" if goos.startswith("linux") else goos.rstrip("0123456789")
    m = platform.machine().lower()
    arch = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64", "i686": "386", "i386": "386"}
    return goos, arch.get(m, m)


def _connection_error(exc: BaseException | None) -> BaseException:
    if isinstance(exc, asyncssh.ConnectionLost) and "keepalive" in exc.reason:
        return KeepAliveTimeoutError()
    return ConnectionError(f"tuna: connection to node lost: {exc or 'closed'}")


class _Session:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.dead: asyncio.Future[BaseException] = loop.create_future()
        self.conn: asyncssh.SSHClientConnection | None = None
        self.events: asyncio.Task[None] | None = None
        self.active = 0
        self.closing = False

    def fail(self, err: BaseException) -> None:
        if not self.dead.done():
            self.dead.set_result(err)
        if self.conn is not None:
            self.conn.close()

    def release(self) -> None:
        self.active -= 1
        if self.closing and self.active == 0:
            self.fail(ClosedError())

    def lost(self, exc: BaseException | None) -> None:
        err = _connection_error(exc)
        if self.events is None or self.events.done():
            self.fail(err)
            return

        async def drain() -> None:
            assert self.events is not None
            await asyncio.wait([self.events], timeout=EVENTS_DRAIN_TIMEOUT)
            self.fail(err)

        spawn(drain())


class _Client(asyncssh.SSHClient):
    def __init__(self, session: _Session):
        self._session = session

    def connection_lost(self, exc: Exception | None) -> None:
        self._session.lost(exc)


class Tunnel:
    """A tunnel that receives connections arriving at its public address.

    It survives reconnects; accept fails only after close or a permanent error.
    Iterating over it with ``async for`` yields connections until it is closed.
    """

    def __init__(self, o: Options, forwards_to: str = ""):
        o = dataclasses.replace(o, cidr_allow=list(o.cidr_allow), cidr_deny=list(o.cidr_deny))
        if not o.api_key:
            o.api_key = os.environ.get("TUNA_API_KEY", "")
        if not o.api_key:
            raise ValueError("tuna: API key is required: set Options.api_key or TUNA_API_KEY")
        if o.protocol not in ("http", "tcp"):
            raise ValueError(f'tuna: unsupported protocol "{o.protocol}"')
        if not o.server_url:
            o.server_url = DEFAULT_SERVER_URL
        check_server_url(o.server_url)
        if not o.client_name:
            o.client_name = "tuna-sdk-python/" + VERSION
        if any(unicodedata.category(c) == "Cc" for c in o.client_name):
            raise ValueError("tuna: client_name must not contain control characters")
        self._o = o
        self._forwards_to = forwards_to
        self._url = ""
        self._loop = asyncio.get_running_loop()
        self._conns: asyncio.Queue[Stream | None] = asyncio.Queue()
        self._ready = asyncio.Event()
        self._done: asyncio.Future[BaseException] = self._loop.create_future()
        self._closed = False
        self._session: _Session | None = None
        self._last_err: BaseException | None = None
        self._ever_up = False
        self._session_id = ""
        self._banner_shown = False
        self._task = self._loop.create_task(self._main())

    @property
    def url(self) -> str:
        """The current public address, e.g. https://name.tuna.am or tcp://ru.tuna.am:40000."""
        return self._url

    async def accept(self) -> Stream:
        """Waits for the next connection to the public address.

        Raises ClosedError after close, or the permanent error that ended the tunnel.
        """
        item = await self._conns.get()
        if item is None:
            self._conns.put_nowait(None)
            err = self._done.result() if self._done.done() else ClosedError()
            raise err
        return item

    async def __aiter__(self) -> AsyncIterator[Stream]:
        while True:
            try:
                yield await self.accept()
            except ClosedError:
                return

    def close(self) -> None:
        """Stops accepting connections and reconnecting.

        Connections accepted earlier keep working: the tunnel stays connected to
        the node until the last of them is closed. Does not wait.
        """
        if self._closed:
            return
        self._closed = True
        self._drain()
        s = self._session
        if s is None:
            self._task.cancel()
            return
        s.closing = True
        if s.active == 0:
            s.fail(ClosedError())

    def abort(self) -> None:
        """Closes the tunnel together with every accepted connection."""
        self._closed = True
        self._drain()
        self._task.cancel()

    async def wait_closed(self) -> None:
        """Waits until the tunnel is closed or ends with an error."""
        await asyncio.shield(self._done)

    async def __aenter__(self) -> Tunnel:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.close()

    def _emit(self, e: Event) -> None:
        if self._o.on_event is None:
            return
        with contextlib.suppress(Exception):
            self._o.on_event(e)

    def _finish(self, err: BaseException) -> None:
        if not self._done.done():
            self._done.set_result(err)
        self._drain()

    def _drain(self) -> None:
        while not self._conns.empty():
            item = self._conns.get_nowait()
            if item is not None:
                item[1].close()
        self._conns.put_nowait(None)

    async def _start(self) -> None:
        waiter = asyncio.ensure_future(self._ready.wait())
        try:
            await asyncio.wait(
                [waiter, self._done], timeout=self._o.connect_timeout, return_when=asyncio.FIRST_COMPLETED
            )
        except BaseException:
            self.abort()
            raise
        finally:
            waiter.cancel()
        if self._ready.is_set():
            return
        if self._done.done():
            raise self._done.result()
        self.abort()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        msg = f"tuna: tunnel did not come up within {self._o.connect_timeout:g}s"
        if self._last_err is not None:
            msg += f" (last attempt: {self._last_err})"
        raise TimeoutError(msg)

    async def _main(self) -> None:
        try:
            await self._run()
        except asyncio.CancelledError:
            self._finish(ClosedError())
        except BaseException as e:
            self._finish(e)

    async def _run(self) -> None:
        backoff = MIN_BACKOFF
        while True:
            connected, err = await self._run_session()
            if self._closed:
                self._finish(ClosedError())
                return
            if not self._ever_up and isinstance(err, ServerError):
                err.before_first_connect = True
            self._last_err = err
            if is_permanent(err):
                if isinstance(err, TerminatedError):
                    self._emit(Event("terminated", reason=err.reason, message=err.message))
                self._finish(err)
                return
            if connected:
                backoff = MIN_BACKOFF
            wait = retry_delay(backoff, err)
            self._emit(Event("reconnecting", error=err, retry_in=wait))
            await asyncio.sleep(wait)
            backoff = min(backoff * 2, MAX_BACKOFF)

    def _request(self) -> dict[str, Any]:
        o = self._o
        goos, arch = _os_arch()
        body: dict[str, Any] = {
            "client_version": CLIENT_VERSION,
            "client_name": o.client_name,
            "sdk_version": VERSION,
            "forwards_to": self._forwards_to,
            "protocol": o.protocol,
            "location": o.location,
            "subdomain": o.subdomain,
            "domain": o.domain,
            "token": o.api_key,
            "os": goos,
            "arch": arch,
            "port_or_alias": o.port,
            "tunnel_session_id": self._session_id,
            "proxy_protocol": o.proxy_protocol,
        }
        if o.cidr_allow:
            body["cidr_allow_list"] = o.cidr_allow
        if o.cidr_deny:
            body["cidr_deny_list"] = o.cidr_deny
        return body

    async def _run_session(self) -> tuple[bool, BaseException]:
        s = _Session(self._loop)
        try:
            return await self._session_body(s)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._ever_up and s is self._session, e
        finally:
            self._session = None
            if s.conn is not None:
                s.conn.close()
            if s.events is not None:
                s.events.cancel()

    async def _session_body(self, s: _Session) -> tuple[bool, BaseException]:
        o = self._o
        cr = await post_json(o.server_url, "/v1/connect", o.client_name, self._request(), o.ssl_context)
        self._session_id = str(cr.get("tunnel_session_id") or "")
        banner = str(cr.get("banner") or "")
        if banner and not self._banner_shown:
            self._banner_shown = True
            self._emit(Event("banner", message=banner))

        try:
            keys = [asyncssh.import_public_key(str(hk.get("key", ""))) for hk in cr.get("host_keys") or []]
        except (asyncssh.KeyImportError, ValueError) as e:
            raise ValueError(f"tuna: parse host key: {e}") from e
        host, port = split_host_port(str(cr.get("address", "")))
        s.conn = await asyncio.wait_for(
            asyncssh.connect(
                host,
                port,
                username=SSH_USER,
                password=str(cr.get("token", "")),
                known_hosts=(keys, [], []),
                client_factory=lambda: _Client(s),
                client_keys=None,
                agent_path=None,
                config=None,
                gss_host=None,
                preferred_auth="password",
                connect_timeout=DIAL_TIMEOUT,
                keepalive_interval=KEEPALIVE_INTERVAL,
                keepalive_count_max=KEEPALIVE_COUNT_MAX,
            ),
            DIAL_TIMEOUT + 1,
        )

        try:
            await s.conn.start_server(self._handler_factory(s), "127.0.0.1", 0)
        except Exception as e:
            if s.dead.done():
                return False, s.dead.result()
            raise RuntimeError(f"tuna: remote forward: {e}") from e

        self._session = s
        s.events = self._loop.create_task(self._read_events(s))
        self._url = public_url(cr, o.protocol)
        if self._ever_up:
            self._emit(Event("reconnected", url=self._url))
        else:
            self._ever_up = True
            self._emit(Event("connected", url=self._url))
            self._ready.set()
        if self._closed:
            s.closing = True
            if s.active == 0:
                s.fail(ClosedError())
        return True, await s.dead

    def _handler_factory(self, s: _Session) -> Callable[[str, int], Callable[..., None]]:
        def reject(reader: asyncssh.SSHReader[bytes], writer: asyncssh.SSHWriter[bytes]) -> None:
            writer.close()

        def handler(reader: asyncssh.SSHReader[bytes], writer: asyncssh.SSHWriter[bytes]) -> None:
            s.active += 1
            spawn(self._track(s, writer))
            if s.dead.done() or self._closed:
                writer.close()
                return
            self._conns.put_nowait((reader, writer))

        def factory(orig_host: str, orig_port: int) -> Callable[..., None]:
            return reject if s.closing else handler

        return factory

    async def _track(self, s: _Session, writer: asyncssh.SSHWriter[bytes]) -> None:
        try:
            await writer.channel.wait_closed()
        finally:
            s.release()

    async def _read_events(self, s: _Session) -> None:
        assert s.conn is not None
        try:
            _, stdout, _ = await s.conn.open_session(command=EVENTS_COMMAND)
            async for line in stdout:
                ev = parse_event(line)
                if ev is None:
                    continue
                if ev.get("type") == "terminate":
                    s.fail(TerminatedError(str(ev.get("reason") or "")))
                    return
                if ev.get("type") == "session_expiring":
                    with contextlib.suppress(TypeError, ValueError):
                        self._emit(Event("session_expiring", expires_in=float(ev.get("expires_in") or 0)))
        except (asyncssh.Error, OSError):
            return

    async def _pipe(self, reader: asyncssh.SSHReader[bytes], writer: asyncssh.SSHWriter[bytes], addr: str) -> None:
        try:
            if addr.startswith("unix://"):
                conn = asyncio.open_unix_connection(addr.removeprefix("unix://"))
            else:
                conn = asyncio.open_connection(*split_host_port(addr))
            lr, lw = await asyncio.wait_for(conn, DIAL_TIMEOUT)
        except (OSError, TimeoutError, ValueError) as e:
            writer.close()
            self._emit(Event("local_error", error=e))
            return
        await pipe(reader, writer, lr, lw)


async def _copy(src: Any, dst: Any) -> None:
    with contextlib.suppress(Exception):
        while data := await src.read(32768):
            dst.write(data)
            await dst.drain()


async def pipe(
    reader: asyncssh.SSHReader[bytes],
    writer: asyncssh.SSHWriter[bytes],
    local_reader: asyncio.StreamReader,
    local_writer: asyncio.StreamWriter,
) -> None:
    tasks = [asyncio.ensure_future(_copy(reader, local_writer)), asyncio.ensure_future(_copy(local_reader, writer))]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tasks:
            t.cancel()
        writer.close()
        local_writer.close()


class _Listen:
    def __init__(self, options: Options):
        self._options = options
        self._tunnel: Tunnel | None = None

    def __await__(self) -> Generator[Any, None, Tunnel]:
        return self._open().__await__()

    async def _open(self) -> Tunnel:
        t = Tunnel(self._options)
        await t._start()
        return t

    async def __aenter__(self) -> Tunnel:
        self._tunnel = await self._open()
        return self._tunnel

    async def __aexit__(self, *exc: object) -> None:
        if self._tunnel is not None:
            self._tunnel.close()


def listen(options: Options) -> _Listen:
    """Connects the tunnel and returns it once it is up.

    Use as ``tunnel = await listen(...)`` or ``async with listen(...) as tunnel``;
    leaving the block closes the tunnel like Tunnel.close. Raises the permanent
    error if one occurs first, or TimeoutError after Options.connect_timeout.
    """
    return _Listen(options)


async def forward(options: Options, addr: str) -> None:
    """Proxies tunnel connections to addr ("host:port" or "unix:///path.sock").

    Runs until a permanent error, which is raised, or until the task is cancelled,
    which closes the tunnel with every connection.
    """
    t = Tunnel(options, addr)
    try:
        await t._start()
        while True:
            reader, writer = await t.accept()
            spawn(t._pipe(reader, writer, addr))
    finally:
        t.abort()
