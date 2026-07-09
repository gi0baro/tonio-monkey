from __future__ import annotations

import pytest

from tests.colored.django import app


pytestmark = [pytest.mark.tonio]


async def test_async_unsafe_guard():
    app.setup()
    from asgiref.sync import sync_to_async
    from django.core.exceptions import SynchronousOnlyOperation
    from django.utils.asyncio import async_unsafe

    from tonio_monkey._impl._colored import _asgiref as _impl
    from tonio_monkey.colored import django as _django  # noqa: F401

    @async_unsafe
    def sync_only():
        return "ran"

    # Inside an async context the guard fires...
    _impl.async_context.set(True)
    with pytest.raises(SynchronousOnlyOperation):
        sync_only()

    # ...but the same call routed through sync_to_async is allowed.
    assert await sync_to_async(sync_only)() == "ran"


async def test_signal_asend_mixes_sync_and_async_receivers():
    app.setup()
    from django.dispatch import Signal

    from tonio_monkey.colored import django as _django  # noqa: F401

    signal = Signal()

    async def async_receiver(signal, sender, **kwargs):
        return "async"

    def sync_receiver(signal, sender, **kwargs):
        return "sync"

    signal.connect(async_receiver)
    signal.connect(sync_receiver)

    responses = await signal.asend(sender=None)
    assert sorted(str(value) for _, value in responses) == ["async", "sync"]


# Note: the ASGI handler disconnect race (`_race`) is covered end-to-end through
# the real handler in test_django_asgi_handler.py — that exercises both race
# directions with a genuinely-running view, rather than instant-completing fakes
# (which trip an inherent, benign tonio "cancelled before first await" warning
# that `tonio.select` shares).
