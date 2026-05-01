from __future__ import annotations

import os

import pytest


pytestmark = [
    pytest.mark.tonio,
    pytest.mark.skipif(
        not os.environ.get("TM_PSYCOPG_DSN"),
        reason="TM_PSYCOPG_DSN not set",
    ),
]

DSN = os.environ.get("TM_PSYCOPG_DSN", "")


async def test_select_one():
    from tonio_monkey.colored import psycopg

    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            result = await cur.fetchone()

    assert result == (1,)
