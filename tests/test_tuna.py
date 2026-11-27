import asyncio
import http.server
import json
import threading

import pytest

import tuna_sdk
from tuna_sdk._api import server_error
from tuna_sdk._tunnel import parse_event, public_url, retry_delay, split_host_port


def test_split_host_port():
    assert split_host_port("127.0.0.1:8080") == ("127.0.0.1", 8080)
    assert split_host_port("[::1]:22") == ("::1", 22)
    with pytest.raises(ValueError):
        split_host_port("localhost")


def test_public_url_fallback():
    assert public_url({"public_url": "https://a.tuna.am", "domain_name": "b"}, "http") == "https://a.tuna.am"
    assert public_url({"domain_name": "demo.tuna.am"}, "http") == "https://demo.tuna.am"
    assert public_url({"domain_name": "ru.tuna.am", "endpoint_port": 40001}, "tcp") == "tcp://ru.tuna.am:40001"


def test_server_error_json():
    body = json.dumps({"code": "Unauthorized", "error": "bad key", "request_id": "r1"}).encode()
    se = server_error(401, {}, body)
    assert (se.status, se.code, se.message, se.request_id) == (401, "Unauthorized", "bad key", "r1")
    assert str(se) == "tuna: bad key (request id r1)"
    assert tuna_sdk.is_permanent(se)


def test_server_error_classification():
    assert not tuna_sdk.is_permanent(server_error(503, {}, b"{}"))
    html404 = server_error(404, {}, b"<html>")
    assert html404.message == "server responded with status 404"
    assert not tuna_sdk.is_permanent(html404)
    html404.before_first_connect = True
    assert tuna_sdk.is_permanent(html404)
    too_many = server_error(429, {}, b"")
    too_many.before_first_connect = True
    assert not tuna_sdk.is_permanent(too_many)
    redirect = server_error(301, {"Location": "https://x"}, b"")
    assert tuna_sdk.is_permanent(redirect)
    assert "redirects" in redirect.message
    assert tuna_sdk.is_permanent(tuna_sdk.TerminatedError("deleted"))
    assert not tuna_sdk.is_permanent(tuna_sdk.KeepAliveTimeoutError())
    assert "keepalive timeout" in str(tuna_sdk.KeepAliveTimeoutError())


def test_retry_after():
    assert server_error(503, {"Retry-After": "3"}, b"").retry_after == 3
    assert server_error(503, {"Retry-After": "3600"}, b"").retry_after == 300
    assert server_error(503, {"Retry-After": "soon"}, b"").retry_after == 0


def test_retry_delay():
    delays = {retry_delay(15, None) for _ in range(50)}
    assert all(1 <= d <= 15 for d in delays)
    assert len(delays) > 1
    assert retry_delay(1, server_error(503, {"Retry-After": "7"}, b"")) == 7


def test_terminated_message():
    assert str(tuna_sdk.TerminatedError("deleted")) == "tuna: tunnel was deleted"
    assert tuna_sdk.TerminatedError("x").message == "tunnel terminated by server (x)"


def test_parse_event():
    assert parse_event('{"type":"terminate","reason":"deleted"}') == {"type": "terminate", "reason": "deleted"}
    assert parse_event("garbage") is None
    assert parse_event("[1]") is None


def test_options_validation():
    async def check():
        with pytest.raises(ValueError, match="API key"):
            await tuna_sdk.listen(tuna_sdk.Options(protocol="http", api_key=""))
        with pytest.raises(ValueError, match="protocol"):
            await tuna_sdk.listen(tuna_sdk.Options(protocol="udp", api_key="k"))
        with pytest.raises(ValueError, match="server URL"):
            await tuna_sdk.listen(tuna_sdk.Options(protocol="http", api_key="k", server_url="ftp://x"))

    asyncio.run(check())


@pytest.fixture
def api():
    requests = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            body = json.dumps({"code": "Unauthorized", "error": "bad key"}).encode()
            self.send_response(401)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}", requests
    srv.shutdown()


def test_permanent_error_async(api):
    url, requests = api
    with pytest.raises(tuna_sdk.ServerError) as e:
        asyncio.run(tuna_sdk.forward(tuna_sdk.Options(protocol="http", api_key="k", server_url=url), "127.0.0.1:1"))
    assert e.value.code == "Unauthorized"
    body = requests[0]
    assert body["client_version"] == 2
    assert body["client_name"] == "tuna-sdk-python/" + tuna_sdk.VERSION
    assert body["sdk_version"] == tuna_sdk.VERSION
    assert body["tunnel_session_id"] == ""
    assert body["forwards_to"] == "127.0.0.1:1"
    assert len(requests) == 1


def test_permanent_error_sync(api):
    url, _ = api
    with pytest.raises(tuna_sdk.ServerError):
        tuna_sdk.sync.listen(tuna_sdk.Options(protocol="tcp", api_key="k", server_url=url))
