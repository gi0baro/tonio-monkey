from __future__ import annotations

import pytest

from tests.colored.django import app


pytestmark = [pytest.mark.tonio]


async def test_sync_view_runs_in_worker_thread():
    app.setup()
    from django.test import AsyncClient

    from tonio_monkey.colored import django as _django  # noqa: F401

    response = await AsyncClient().get("/sync/")

    assert response.status_code == 200
    body = response.content.decode()
    assert body.startswith("sync:")
    # The sync view ran in a spawn_blocking worker, not the runtime thread.
    assert body != "sync:MainThread"
    # The sync middleware was adapted and ran too.
    assert "X-Tonio-Thread" in response


async def test_async_view():
    app.setup()
    from django.test import AsyncClient

    from tonio_monkey.colored import django as _django  # noqa: F401

    response = await AsyncClient().get("/async/")

    assert response.status_code == 200
    assert response.json() == {"hello": "async"}


async def test_async_streaming_view():
    app.setup()
    from django.test import AsyncClient

    from tonio_monkey.colored import django as _django  # noqa: F401

    response = await AsyncClient().get("/stream/")

    assert response.status_code == 200
    chunks = [chunk async for chunk in response.streaming_content]
    assert b"".join(chunks) == b"chunk0chunk1chunk2"


async def test_404_for_unknown_route():
    app.setup()
    from django.test import AsyncClient

    from tonio_monkey.colored import django as _django  # noqa: F401

    response = await AsyncClient().get("/nope/")

    assert response.status_code == 404
