from __future__ import annotations

import os

import pytest


pytestmark = [
    pytest.mark.tonio,
    pytest.mark.skipif(
        not os.environ.get("TM_REDIS_DSN"),
        reason="TM_REDIS_DSN not set",
    ),
]

DSN = os.environ.get("TM_REDIS_DSN", "")


async def test_set_get():
    from tonio_monkey.colored import redis

    client = redis.asyncio.Redis.from_url(DSN)
    try:
        await client.set("tonio_monkey_test", "hello")
        result = await client.get("tonio_monkey_test")
        assert result == b"hello"
        await client.delete("tonio_monkey_test")
    finally:
        await client.aclose()
