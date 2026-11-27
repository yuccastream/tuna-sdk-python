"""Blocking API for code without asyncio.

The tunnel runs on an event loop in a background thread, so it also works
where a loop is already running, such as Jupyter. Options.on_event is called
from that thread.
"""

from __future__ import annotations

import asyncio
import socket
import threading
from collections.abc import Coroutine, Iterator
from typing import Any, TypeVar

from . import _tunnel
from ._tunnel import ClosedError, Options

T = TypeVar("T")

_loop: asyncio.AbstractEventLoop | None = None
_lock = threading.Lock()


def _background_loop() -> asyncio.AbstractEventLoop:
    global _loop
    with _lock:
        if _loop is None:
            _loop = asyncio.new_event_loop()
            threading.Thread(target=_loop.run_forever, name="tuna-sdk", daemon=True).start()
        return _loop


def _wait(coro: Coroutine[Any, Any, T]) -> T:
    fut = asyncio.run_coroutine_threadsafe(coro, _background_loop())
    try:
        return fut.result()
    except BaseException:
        fut.cancel()
        raise


def forward(options: Options, addr: str) -> None:
    """Proxies tunnel connections to addr ("host:port" or "unix:///path.sock").

    Blocks until a permanent error, which is raised. KeyboardInterrupt closes
    the tunnel and propagates.
    """
    _wait(_tunnel.forward(options, addr))


class Tunnel:
    """A blocking tunnel; accept returns connected sockets."""

    def __init__(self, tunnel: _tunnel.Tunnel):
        self._tunnel = tunnel

    @property
    def url(self) -> str:
        """The current public address."""
        return self._tunnel.url

    def accept(self) -> socket.socket:
        """Waits for the next connection and returns it as a blocking socket.

        Raises ClosedError after close, or the permanent error that ended the tunnel.
        """
        return _wait(self._accept())

    async def _accept(self) -> socket.socket:
        reader, writer = await self._tunnel.accept()
        app, side = socket.socketpair()
        local_reader, local_writer = await asyncio.open_connection(sock=side)
        _tunnel.spawn(_tunnel.pipe(reader, writer, local_reader, local_writer))
        return app

    def __iter__(self) -> Iterator[socket.socket]:
        while True:
            try:
                yield self.accept()
            except ClosedError:
                return

    def close(self) -> None:
        """Stops accepting and reconnecting; accepted connections keep working."""
        _background_loop().call_soon_threadsafe(self._tunnel.close)

    def abort(self) -> None:
        """Closes the tunnel together with every accepted connection."""
        _background_loop().call_soon_threadsafe(self._tunnel.abort)

    def __enter__(self) -> Tunnel:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def listen(options: Options) -> Tunnel:
    """Connects the tunnel and returns it once it is up.

    Raises the permanent error if one occurs first, or TimeoutError after
    Options.connect_timeout.
    """
    return Tunnel(_wait(_tunnel.listen(options)._open()))


__all__ = ["Tunnel", "forward", "listen"]
