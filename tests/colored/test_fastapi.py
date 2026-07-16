from __future__ import annotations

import threading

import pytest


pytestmark = [pytest.mark.tonio]


def _build_app():
    from fastapi import Depends, FastAPI
    from fastapi.responses import StreamingResponse

    from tonio_monkey.colored import fastapi as _fastapi  # noqa: F401

    app = FastAPI()

    @app.get("/async")
    async def async_endpoint():
        return {"hello": "async"}

    @app.get("/sync")
    def sync_endpoint():
        return {"thread": threading.current_thread().name}

    def sync_dependency():
        return threading.current_thread().name

    @app.get("/dep")
    async def dep_endpoint(name: str = Depends(sync_dependency)):
        return {"dep_thread": name}

    # A generator dependency: FastAPI wraps it with contextmanager and runs its
    # __enter__/__exit__ via contextmanager_in_threadpool.
    def gen_dependency():
        yield "cm-value"

    @app.get("/cmdep")
    async def cmdep_endpoint(value: str = Depends(gen_dependency)):
        return {"value": value}

    @app.get("/stream")
    async def stream_endpoint():
        async def body():
            for idx in range(3):
                yield f"chunk{idx}".encode()

        return StreamingResponse(body())

    # An async-generator endpoint: routes through FastAPI's own streaming path
    # (_async_stream_raw), which inserts a checkpoint between chunks.
    @app.get("/gen", response_class=StreamingResponse)
    async def gen_endpoint():
        for idx in range(3):
            yield f"gen{idx}".encode()

    return app


async def _drive(app, path: str):
    import tonio.colored as tonio

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "GET",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"testserver")],
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
        await idle.wait(None)

    async def send(message):
        sent.append(message)

    await app(scope, receive, send)
    return sent


def _parse(sent):
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return start["status"], body


async def test_async_endpoint():
    status, body = _parse(await _drive(_build_app(), "/async"))
    assert status == 200
    assert body == b'{"hello":"async"}'


async def test_sync_endpoint_runs_in_worker_thread():
    import json

    status, body = _parse(await _drive(_build_app(), "/sync"))
    assert status == 200
    assert json.loads(body)["thread"] != "MainThread"


async def test_sync_dependency_runs_in_worker_thread():
    import json

    status, body = _parse(await _drive(_build_app(), "/dep"))
    assert status == 200
    assert json.loads(body)["dep_thread"] != "MainThread"


async def test_contextmanager_dependency():
    status, body = _parse(await _drive(_build_app(), "/cmdep"))
    assert status == 200
    assert body == b'{"value":"cm-value"}'


async def test_streaming_endpoint():
    status, body = _parse(await _drive(_build_app(), "/stream"))
    assert status == 200
    assert body == b"chunk0chunk1chunk2"


async def test_async_generator_endpoint_checkpoint_path():
    # Exercises FastAPI's own streaming path with the between-chunk checkpoint
    # (anyio.sleep(0) -> tonio.yield_now()).
    status, body = _parse(await _drive(_build_app(), "/gen"))
    assert status == 200
    assert body == b"gen0gen1gen2"
