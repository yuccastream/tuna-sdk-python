"""A tuna-tiny compatible tunnel client built on tuna-sdk.

    python runner.py http|tcp <addr> [-s SUBDOMAIN] [-d DOMAIN] [-p PORT]
        [--proxy-protocol N] [--cidr-allow CIDR]... [--cidr-deny CIDR]...
        [--listen] [--log-format text|json]

The API key comes from TUNA_API_KEY, the API address from TUNA_ADDR. With
--listen the runner accepts tunnel connections itself and copies them to addr.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime
import json
import os
import signal
import sys
import urllib.parse

import tuna_sdk


def local_addr(s: str) -> str:
    if s.startswith("unix://"):
        return s
    if "://" in s:
        u = urllib.parse.urlsplit(s)
        if u.scheme != "http":
            raise ValueError(f'scheme "{u.scheme}" is not supported')
        return f"{u.hostname}:{u.port or 80}"
    if ":" not in s:
        if not s.isdigit() or int(s) > 65535:
            raise ValueError(f'address "{s}" is not a port, host:port, http:// URL or unix:// path')
        return "127.0.0.1:" + s
    return s


def describe(err: BaseException | None) -> str:
    if err is None:
        return ""
    return str(err) or type(err).__name__


def printer(as_json: bool, addr: str):
    def emit(e: tuna_sdk.Event) -> None:
        if as_json:
            line = {
                "time": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "event": e.type,
                "url": e.url,
                "message": e.message,
                "reason": e.reason,
                "error": describe(e.error),
                "retry_in": e.retry_in,
                "expires_in": e.expires_in,
            }
            print(json.dumps({k: v for k, v in line.items() if v or k in ("time", "event")}), flush=True)
        elif e.type in ("connected", "reconnected"):
            print(f"Forwarding {e.url} -> {addr}", file=sys.stderr, flush=True)
        elif e.type == "reconnecting":
            print(f"Connection error ({describe(e.error)}), retry in {e.retry_in:.1f}s", file=sys.stderr, flush=True)
        elif e.type == "session_expiring":
            print(f"Session expires in {e.expires_in:g}s", file=sys.stderr, flush=True)
        elif e.type == "local_error":
            print(f"Local service unavailable: {describe(e.error)}", file=sys.stderr, flush=True)
        elif e.type in ("banner", "terminated"):
            print(e.message, file=sys.stderr, flush=True)
        else:
            print(f"Error: {describe(e.error)}", file=sys.stderr, flush=True)

    return emit


async def copy(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
    with contextlib.suppress(Exception):
        while data := await src.read(32768):
            dst.write(data)
            await dst.drain()


async def proxy(reader, writer, addr: str) -> None:
    try:
        host, _, port = addr.rpartition(":")
        local_reader, local_writer = await asyncio.open_connection(host, int(port))
    except OSError:
        writer.close()
        return
    tasks = [asyncio.ensure_future(copy(reader, local_writer)), asyncio.ensure_future(copy(local_reader, writer))]
    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for t in tasks:
        t.cancel()
    writer.close()
    local_writer.close()


async def serve(options: tuna_sdk.Options, addr: str, stop: asyncio.Event) -> None:
    async with tuna_sdk.listen(options) as tunnel:
        conns: set[asyncio.Task[None]] = set()

        async def accept() -> None:
            async for reader, writer in tunnel:
                task = asyncio.ensure_future(proxy(reader, writer, addr))
                conns.add(task)
                task.add_done_callback(conns.discard)

        acceptor = asyncio.ensure_future(accept())
        stopper = asyncio.ensure_future(stop.wait())
        await asyncio.wait([acceptor, stopper], return_when=asyncio.FIRST_COMPLETED)
        stopper.cancel()
        if acceptor.done():
            acceptor.result()
        tunnel.close()
        if conns:
            await asyncio.wait(conns, timeout=30)


async def run(options: tuna_sdk.Options, addr: str, listen: bool) -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    main = asyncio.ensure_future(serve(options, addr, stop) if listen else tuna_sdk.forward(options, addr))
    stopper = asyncio.ensure_future(stop.wait())
    await asyncio.wait([main, stopper], return_when=asyncio.FIRST_COMPLETED)
    if stop.is_set() and not listen:
        main.cancel()
    try:
        await main
    except asyncio.CancelledError:
        return 0
    except tuna_sdk.TerminatedError:
        return 1
    except Exception as e:
        if stop.is_set():
            return 0
        assert options.on_event is not None
        options.on_event(tuna_sdk.Event("error", error=e))
        return 1
    finally:
        stopper.cancel()
    return 0


def main() -> int:
    p = argparse.ArgumentParser(prog="runner")
    p.add_argument("protocol", choices=["http", "tcp"])
    p.add_argument("addr")
    p.add_argument("--api-key", default="")
    p.add_argument("-s", dest="subdomain", default=os.environ.get("TUNA_SUBDOMAIN", ""))
    p.add_argument("-d", dest="domain", default=os.environ.get("TUNA_DOMAIN", ""))
    p.add_argument("-p", dest="port", default=os.environ.get("TUNA_TCP_PORT", ""))
    p.add_argument("--proxy-protocol", type=int, default=0)
    p.add_argument("--cidr-allow", action="append", default=[])
    p.add_argument("--cidr-deny", action="append", default=[])
    p.add_argument("--listen", action="store_true")
    p.add_argument("--log-format", choices=["text", "json"], default=os.environ.get("TUNA_LOG_FORMAT", "text"))
    args = p.parse_intermixed_args()
    try:
        addr = local_addr(args.addr)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    options = tuna_sdk.Options(
        protocol=args.protocol,
        api_key=args.api_key,
        subdomain=args.subdomain,
        domain=args.domain,
        port=args.port,
        proxy_protocol=args.proxy_protocol,
        cidr_allow=args.cidr_allow,
        cidr_deny=args.cidr_deny,
        server_url=os.environ.get("TUNA_ADDR", ""),
        location=os.environ.get("TUNA_LOCATION", ""),
        on_event=printer(args.log_format == "json", addr),
    )
    return asyncio.run(run(options, addr, args.listen))


if __name__ == "__main__":
    sys.exit(main())
