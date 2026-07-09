from __future__ import annotations

import pytest

from tests.colored.django import app


pytestmark = [pytest.mark.tonio]


def _http_scope(path: str) -> dict:
    return {
        "type": "http",
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


def _request_app():
    from django.core.asgi import get_asgi_application

    from tonio_monkey.colored import django as _django  # noqa: F401

    return get_asgi_application()


async def test_handle_full_response_cycle():
    app.setup()
    from asgiref.testing import ApplicationCommunicator

    comm = ApplicationCommunicator(_request_app(), _http_scope("/async/"))
    await comm.send_input({"type": "http.request", "body": b"", "more_body": False})

    start = await comm.receive_output(2)
    assert start["type"] == "http.response.start"
    assert start["status"] == 200

    body = b""
    message = await comm.receive_output(2)
    while True:
        assert message["type"] == "http.response.body"
        body += message.get("body", b"")
        if not message.get("more_body", False):
            break
        message = await comm.receive_output(2)

    assert body == b'{"hello": "async"}'
    await comm.wait(2)


async def test_handle_streaming_response():
    app.setup()
    from asgiref.testing import ApplicationCommunicator

    comm = ApplicationCommunicator(_request_app(), _http_scope("/stream/"))
    await comm.send_input({"type": "http.request", "body": b"", "more_body": False})

    start = await comm.receive_output(2)
    assert start["type"] == "http.response.start"
    assert start["status"] == 200

    body = b""
    while True:
        message = await comm.receive_output(2)
        assert message["type"] == "http.response.body"
        body += message.get("body", b"")
        if not message.get("more_body", False):
            break

    assert body == b"chunk0chunk1chunk2"
    await comm.wait(2)


async def test_handle_client_disconnect_cancels_processing():
    app.setup()
    from asgiref.testing import ApplicationCommunicator

    comm = ApplicationCommunicator(_request_app(), _http_scope("/slow/"))
    # Body is complete, then the client disconnects while the (30s) view runs.
    await comm.send_input({"type": "http.request", "body": b"", "more_body": False})
    await comm.send_input({"type": "http.disconnect"})

    # The disconnect wins the race: processing is cancelled, no response is sent.
    assert await comm.receive_nothing(0.5) is True
    # And the handler unwinds promptly instead of waiting out the 30s sleep.
    await comm.wait(2)
    assert comm.future.done()
