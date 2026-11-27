"""Open tuna.am tunnels from Python code.

``listen`` returns a Tunnel that receives connections arriving at the tunnel's
public address as ``(reader, writer)`` streams. ``forward`` proxies them to a
local address. ``tuna_sdk.sync`` offers the same without asyncio.

A tunnel survives network failures: it reconnects with backoff and keeps its
public address. It ends on close, cancellation or a permanent error (see
is_permanent).
"""

from . import sync
from ._api import ServerError
from ._tunnel import (
    DEFAULT_SERVER_URL,
    VERSION,
    ClosedError,
    Event,
    EventType,
    KeepAliveTimeoutError,
    Options,
    TerminatedError,
    Tunnel,
    forward,
    is_permanent,
    listen,
)

__version__ = VERSION

__all__ = [
    "DEFAULT_SERVER_URL",
    "VERSION",
    "ClosedError",
    "Event",
    "EventType",
    "KeepAliveTimeoutError",
    "Options",
    "ServerError",
    "TerminatedError",
    "Tunnel",
    "forward",
    "is_permanent",
    "listen",
    "sync",
]
