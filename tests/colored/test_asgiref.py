from __future__ import annotations

import contextvars
import threading

import pytest


pytestmark = [pytest.mark.tonio]


async def test_sync_to_async_runs_in_worker_thread():
    from asgiref.sync import sync_to_async

    from tonio_monkey.colored import asgiref as _asgiref  # noqa: F401

    main = threading.current_thread()

    def fn():
        return threading.current_thread()

    other = await sync_to_async(fn)()
    assert other is not main


async def test_async_to_sync_inside_sync_to_async():
    from asgiref.sync import async_to_sync, sync_to_async

    from tonio_monkey.colored import asgiref as _asgiref  # noqa: F401

    async def doubler(x):
        return x * 2

    def view():
        return async_to_sync(doubler)(21)

    assert await sync_to_async(view)() == 42


async def test_contextvars_propagate_into_sync():
    from asgiref.sync import sync_to_async

    from tonio_monkey.colored import asgiref as _asgiref  # noqa: F401

    cvar = contextvars.ContextVar("test_cvar", default="unset")
    cvar.set("request-scope")

    def fn():
        return cvar.get()

    assert await sync_to_async(fn)() == "request-scope"


async def test_local_mutation_round_trips():
    from asgiref.local import Local
    from asgiref.sync import sync_to_async

    from tonio_monkey.colored import asgiref as _asgiref  # noqa: F401

    local = Local()

    def setter():
        local.value = "set-in-sync"

    await sync_to_async(setter)()
    assert local.value == "set-in-sync"


async def test_async_to_sync_forbidden_in_async_context():
    from asgiref.sync import async_to_sync

    from tonio_monkey._impl._colored import _asgiref as _impl
    from tonio_monkey.colored import asgiref as _asgiref  # noqa: F401

    async def noop():
        return None

    _impl.async_context.set(True)
    with pytest.raises(RuntimeError):
        async_to_sync(noop)()
