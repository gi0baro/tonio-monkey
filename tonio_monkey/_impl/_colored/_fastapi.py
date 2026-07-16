from __future__ import annotations

import typing

import anyio
import fastapi as fastapi
import fastapi.concurrency
import fastapi.dependencies.utils
import fastapi.routing
import starlette.concurrency
import tonio.colored as tonio

# Activate the starlette patch first: FastAPI is built on starlette and imports
# `run_in_threadpool`/`iterate_in_threadpool` from `starlette.concurrency` at
# import time, so those must be patched before (and FastAPI's own bound copies
# are rebound below for good measure).
from . import _starlette as _starlette


# FastAPI's own async surface beyond starlette is tiny:
#   - concurrency.contextmanager_in_threadpool -> anyio.to_thread.run_sync
#   - routing.py streaming endpoints           -> anyio.sleep (checkpoints)
#   - routing.py SSE endpoints (EventSourceResponse only) -> anyio.fail_after,
#     create_task_group, create_memory_object_stream, EndOfStream  [DEFERRED]
#
# We rebind the threadpool helpers on FastAPI's modules and install a scoped
# `anyio` proxy (to_thread.run_sync -> spawn_blocking, sleep -> tonio.sleep) on
# the two modules that touch anyio directly. SSE is not supported yet:
# `anyio.fail_after` has no standalone tonio timed-cancel equivalent, so the
# proxy delegates those attributes to the real anyio (SSE endpoints raise until
# handled).


# --- rebind threadpool helpers (import-order robustness) --------------------
for _mod in (
    fastapi.concurrency,
    fastapi.routing,
    fastapi.dependencies.utils,
):
    if hasattr(_mod, "run_in_threadpool"):
        _mod.run_in_threadpool = tonio.spawn_blocking
    if hasattr(_mod, "iterate_in_threadpool"):
        _mod.iterate_in_threadpool = starlette.concurrency.iterate_in_threadpool


# --- scoped anyio proxy -----------------------------------------------------
class _ToThreadProxy:
    run_sync = staticmethod(_starlette._to_thread_run_sync)


def _sleep(seconds: float) -> typing.Any:
    # FastAPI only ever calls `anyio.sleep(0)` — as a cooperative checkpoint so
    # cancellation can land in tight streaming loops. tonio's idiomatic
    # checkpoint is `yield_now()`; `sleep(0)` would needlessly spin the timer.
    if not seconds:
        return tonio.yield_now()
    return tonio.sleep(seconds)


class _AnyioProxy:
    to_thread = _ToThreadProxy()
    sleep = staticmethod(_sleep)

    def __init__(self, real: typing.Any) -> None:
        self._real = real

    def __getattr__(self, name: str) -> typing.Any:
        return getattr(self._real, name)


_anyio_proxy = _AnyioProxy(anyio)
fastapi.concurrency.anyio = _anyio_proxy
fastapi.routing.anyio = _anyio_proxy
