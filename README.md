# tuna-sdk-python

[![PyPI](https://img.shields.io/pypi/v/tuna-sdk.svg)](https://pypi.org/project/tuna-sdk/)

Open [tuna.am](https://tuna.am) tunnels straight from Python code: expose a
local HTTP or TCP service at a public address without installing the `tuna`
CLI.

- Two ways in: `listen` hands you each incoming connection as a
  `(reader, writer)` stream pair, `forward` proxies connections to a local
  address.
- asyncio first, with a blocking `tuna_sdk.sync` API that also works in
  Jupyter.
- Survives network failures: reconnects with backoff and keeps the same
  public address.
- Small: the only dependency is [asyncssh](https://pypi.org/project/asyncssh/).
- Typed: ships `py.typed`.

The API is not stable yet: it may change before v1.

## Install

```shell
pip install tuna-sdk
```

The module name is `tuna_sdk`. Requires Python 3.10 or later.

## Authentication

Create an API key with the `tunnels:connect` scope at
[my.tuna.am](https://my.tuna.am) and pass it in `Options.api_key` or the
`TUNA_API_KEY` environment variable.

## Forward to a local service

```python
import asyncio
import tuna_sdk


def on_event(e: tuna_sdk.Event) -> None:
    if e.type == "connected":
        print("forwarding", e.url, "-> 127.0.0.1:8080")


asyncio.run(tuna_sdk.forward(tuna_sdk.Options(protocol="http", on_event=on_event), "127.0.0.1:8080"))
```

The local address is `host:port` or `unix:///path/to.sock`. `forward` runs
until the task is cancelled or the tunnel ends for good.

Without asyncio:

```python
tuna_sdk.sync.forward(tuna_sdk.Options(protocol="tcp", port="my-db"), "127.0.0.1:5432")
```

## Accept connections yourself

```python
async def main() -> None:
    async with tuna_sdk.listen(tuna_sdk.Options(protocol="tcp")) as tunnel:
        print("public URL:", tunnel.url)
        async for reader, writer in tunnel:
            asyncio.ensure_future(handle(reader, writer))
```

`reader` and `writer` are asyncssh `SSHReader` and `SSHWriter` objects with
the same methods as asyncio streams (`read`, `readline`, `write`, `drain`,
`close`). `await tunnel.accept()` returns the next pair.

`tuna_sdk.sync.listen` returns a tunnel whose `accept()` gives a regular
blocking `socket.socket`, so it plugs into `socketserver` and similar code:

```python
with tuna_sdk.sync.listen(tuna_sdk.Options(protocol="http")) as tunnel:
    print("public URL:", tunnel.url)
    for sock in tunnel:
        handle(sock)
```

Leaving the `with` block, or calling `close()`, stops accepting and
reconnecting; connections accepted earlier keep working and the tunnel stays
connected until the last of them is closed. `abort()` cuts them off too.

[`examples/runner.py`](examples/runner.py) is a complete command-line client
built on the SDK.

## Options

| Field | Meaning |
|---|---|
| `protocol` | `http` or `tcp`, required |
| `api_key` | API key, defaults to `$TUNA_API_KEY` |
| `subdomain`, `domain` | reserved subdomain or custom domain for HTTP |
| `port` | reserved TCP port or its alias |
| `proxy_protocol` | PROXY protocol header version for TCP: `1` or `2` |
| `cidr_allow`, `cidr_deny` | client CIDRs filtered by the tuna.am node, TCP only |
| `location` | only for names and ports reserved in the legacy `ru` or `nl` zones; empty lets the server pick |
| `on_event` | callback for tunnel events |
| `connect_timeout` | seconds to wait for the first connection; `None` means no limit |
| `server_url`, `ssl_context`, `client_name` | API address, TLS settings for it, client name reported to the server |

## Events and errors

`on_event` receives an `Event` with `type` one of `connected`,
`reconnecting`, `reconnected`, `banner`, `session_expiring`, `terminated` and
`local_error`. Calls never overlap and run on the event loop (for
`tuna_sdk.sync`, on its background thread), so keep the callback short.

Network failures and temporary server errors are retried: the pause grows from
1 to 15 seconds with random jitter, and `Retry-After` is respected. The tunnel
ends with an exception when retrying is pointless; check it with
`is_permanent`:

- `ServerError` — the API rejected the request: bad or revoked key, missing
  scope, unknown reserved name, unpaid plan. Has `status`, `code`, `message`
  and `request_id`;
- `TerminatedError` — the server closed the tunnel: it was deleted,
  suspended or hit the session time limit. Has `reason`.

A connection the node stopped answering is reported in a `reconnecting` event
with `KeepAliveTimeoutError`. `accept` raises `ClosedError` after `close`.

## Behaviour worth knowing

- When either side of a forwarded connection closes, the connection is closed
  in both directions.
- The `Host` header and the PROXY protocol header reach your service as sent.
- The API client is `urllib` and honours `HTTPS_PROXY`. Connections to
  tuna.am nodes are plain SSH over TCP and do not use the proxy.
- Use `ssl_context` for a private CA.

## Contributing

This repository is a read-only mirror of the SDK directory in the tuna.am
monorepo. Issues and pull requests are welcome here: accepted changes are
applied upstream and come back with the next release.

## License

[MIT](LICENSE). asyncssh is a separate package under EPL-2.0 or
GPL-2.0-or-later.
