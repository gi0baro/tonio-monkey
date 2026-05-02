from __future__ import annotations

import os

import pytest


pytestmark = [
    pytest.mark.tonio,
    pytest.mark.skipif(
        not os.environ.get("TM_WS_ENDPOINT"),
        reason="TM_WS_ENDPOINT not set",
    ),
]

ENDPOINT = os.environ.get("TM_WS_ENDPOINT", "")


async def test_echo():
    from tonio_monkey.colored import websockets

    async with websockets.connect(ENDPOINT) as ws:
        await ws.send("foo")
        res_text = await ws.recv()
        await ws.send(b"foo")
        res_bytes = await ws.recv()

    assert res_text == "foo"
    assert res_bytes == b"foo"
