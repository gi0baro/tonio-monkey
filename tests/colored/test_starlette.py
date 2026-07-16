from __future__ import annotations

import threading

import pytest


pytestmark = [pytest.mark.tonio]


def _build_app():
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.responses import JSONResponse, PlainTextResponse, StreamingResponse
    from starlette.routing import Route

    from tonio_monkey.colored import starlette as _starlette  # noqa: F401

    def sync_endpoint(request):
        # A sync endpoint is dispatched via run_in_threadpool.
        return PlainTextResponse(f"sync:{threading.current_thread().name}")

    async def async_endpoint(request):
        return JSONResponse({"hello": "async"})

    async def stream_endpoint(request):
        async def body():
            for idx in range(3):
                yield f"chunk{idx}".encode()

        return StreamingResponse(body())

    class StampMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            response = await call_next(request)
            response.headers["X-Tonio"] = "1"
            return response

    return Starlette(
        routes=[
            Route("/sync", sync_endpoint),
            Route("/async", async_endpoint),
            Route("/stream", stream_endpoint),
        ],
        middleware=[Middleware(StampMiddleware)],
    )


async def _drive(app, path: str, spec_version: str = "2.4", headers=None):
    """Minimal in-process ASGI driver: one GET request, collect the response."""
    import tonio.colored as tonio

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": spec_version},
        "http_version": "1.1",
        "method": "GET",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"testserver"), *(headers or [])],
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 12345),
        "scheme": "http",
    }
    sent: list[dict] = []
    request_delivered = False
    idle = tonio.Event()

    async def receive():
        nonlocal request_delivered
        if not request_delivered:
            request_delivered = True
            return {"type": "http.request", "body": b"", "more_body": False}
        # Connected but idle client: block until the framework cancels us.
        await idle.wait(None)

    async def send(message):
        sent.append(message)

    await app(scope, receive, send)
    return sent


def _parse(sent):
    start = next(m for m in sent if m["type"] == "http.response.start")
    headers = {k.decode(): v.decode() for k, v in start["headers"]}
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return start["status"], headers, body


async def test_async_endpoint():
    status, headers, body = _parse(await _drive(_build_app(), "/async"))
    assert status == 200
    assert body == b'{"hello":"async"}'
    assert headers["x-tonio"] == "1"  # BaseHTTPMiddleware ran


async def test_sync_endpoint_runs_in_worker_thread():
    status, headers, body = _parse(await _drive(_build_app(), "/sync"))
    assert status == 200
    assert body.startswith(b"sync:")
    assert body != b"sync:MainThread"  # dispatched to a spawn_blocking worker
    assert headers["x-tonio"] == "1"


async def test_streaming_endpoint_modern_spec():
    status, _, body = _parse(await _drive(_build_app(), "/stream", spec_version="2.4"))
    assert status == 200
    assert body == b"chunk0chunk1chunk2"


async def test_streaming_endpoint_legacy_spec_race():
    # spec < 2.4 exercises the stream-vs-disconnect race path.
    status, _, body = _parse(await _drive(_build_app(), "/stream", spec_version="2.3"))
    assert status == 200
    assert body == b"chunk0chunk1chunk2"


async def test_file_response():
    import os
    import tempfile

    from starlette.responses import FileResponse

    from tonio_monkey.colored import starlette as _starlette  # noqa: F401

    fd, path = tempfile.mkstemp()
    os.write(fd, b"hello file content")
    os.close(fd)
    try:
        status, headers, body = _parse(await _drive(FileResponse(path), "/"))
        assert status == 200
        assert body == b"hello file content"
        assert headers["content-length"] == "18"
    finally:
        os.unlink(path)


async def test_file_response_range():
    import os
    import tempfile

    from starlette.responses import FileResponse

    from tonio_monkey.colored import starlette as _starlette  # noqa: F401

    fd, path = tempfile.mkstemp()
    os.write(fd, b"0123456789")
    os.close(fd)
    try:
        status, headers, body = _parse(await _drive(FileResponse(path), "/", headers=[(b"range", b"bytes=2-5")]))
        assert status == 206
        assert body == b"2345"
        assert headers["content-range"] == "bytes 2-5/10"
    finally:
        os.unlink(path)


async def test_static_files():
    import os
    import shutil
    import tempfile

    from starlette.staticfiles import StaticFiles

    from tonio_monkey.colored import starlette as _starlette  # noqa: F401

    directory = tempfile.mkdtemp()
    with open(os.path.join(directory, "index.html"), "wb") as handle:
        handle.write(b"<h1>hi</h1>")
    try:
        app = StaticFiles(directory=directory)
        status, _, body = _parse(await _drive(app, "/index.html"))
        assert status == 200
        assert body == b"<h1>hi</h1>"
    finally:
        shutil.rmtree(directory)
