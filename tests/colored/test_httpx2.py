from __future__ import annotations

import os

import pytest


pytestmark = [
    pytest.mark.tonio,
    pytest.mark.skipif(
        not os.environ.get("TM_HTTPX_ENDPOINT"),
        reason="TM_HTTPX_ENDPOINT not set",
    ),
]

ENDPOINT = os.environ.get("TM_HTTPX_ENDPOINT", "")


async def test_streaming_echo():
    from tonio_monkey.colored import httpx2

    payload = b"x" * 1024 * 1024

    async with httpx2.AsyncClient() as client:
        async with client.stream("POST", ENDPOINT, content=payload) as response:
            assert response.status_code == 200
            body = b""
            async for chunk in response.aiter_bytes():
                body += chunk

    assert body == payload
