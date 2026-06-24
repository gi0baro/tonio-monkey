from __future__ import annotations

import typing

import django as django
import django.core.handlers.asgi
import django.dispatch.dispatcher
import django.utils.asyncio
import tonio.colored as tonio
import tonio.colored.exceptions as tonio_exc
from asgiref.sync import sync_to_async
from django.core import signals
from django.core.exceptions import RequestAborted
from django.core.handlers.asgi import get_script_prefix
from django.urls import set_script_prefix

# Activate the asgiref patches first: Django's async story is almost entirely
# `sync_to_async`/`async_to_sync`, which live in asgiref. This module only deals
# with the handful of spots where Django reaches for `asyncio` directly.
from . import _asgiref as _asgiref


CancelledError = tonio_exc.CancelledError


# --- django.utils.asyncio.async_unsafe -------------------------------------
#
# `async_unsafe` guards sync-only ORM code from being run in an async context.
# It detects "async" by calling `asyncio.get_running_loop()` (raises when sync).
# Under tonio there is no asyncio loop, so we swap in the tonio detector.
def _get_running_loop() -> typing.Any:
    if _asgiref.in_async_context():
        return _asgiref._async_context_sentinel
    raise RuntimeError("no running event loop")


django.utils.asyncio.get_running_loop = _get_running_loop


# --- django.dispatch.dispatcher.asyncio.gather -----------------------------
#
# The signal dispatcher fans receivers out with `asyncio.gather(*coros)`.
async def _gather(*coros: typing.Any, return_exceptions: bool = False) -> list[typing.Any]:
    if not coros:
        return []
    if return_exceptions:
        results: list[typing.Any] = [None] * len(coros)

        async def _capture(idx: int, coro: typing.Any) -> None:
            try:
                results[idx] = await coro
            except Exception as exc:
                results[idx] = exc

        await tonio.spawn.without_results(*[_capture(i, c) for i, c in enumerate(coros)])
        return results
    result = await tonio.spawn(*coros)
    return result if isinstance(result, list) else [result]


class _DispatcherAsyncioProxy:
    def __init__(self, orig: typing.Any) -> None:
        self._orig = orig

    gather = staticmethod(_gather)
    CancelledError = CancelledError

    def __getattr__(self, name: str) -> typing.Any:
        return getattr(self._orig, name)


django.dispatch.dispatcher.asyncio = _DispatcherAsyncioProxy(django.dispatch.dispatcher.asyncio)


# --- django.core.handlers.asgi.ASGIHandler ---------------------------------
#
# The ASGI handler races a "listen for disconnect" coroutine against request
# processing, then cancels the loser. Django implements this with
# `asyncio.create_task` + `asyncio.wait(FIRST_COMPLETED)` + per-task
# `cancel()`/`result()` — an abstraction that doesn't map onto tonio's
# structured concurrency (cancellation is scope-scoped, not per-task). So we
# reimplement `handle()` with the idiomatic tonio shape: a single scope that
# spawns both coroutines, a shared event awaited inside the scope, and a
# `scope.cancel()` as the last statement before leaving the block.
_OK, _EXC, _CANCELLED = 0, 1, 2


async def _race(
    handler: typing.Any,
    receive: typing.Any,
    send: typing.Any,
    request: typing.Any,
) -> tuple[tuple[int, typing.Any], tuple[int, typing.Any]]:
    disconnect_outcome = tonio.Result()
    process_outcome = tonio.Result()
    sentinel = tonio.Event()

    async def process_request() -> typing.Any:
        response = await handler.run_get_response(request)
        try:
            await handler.send_response(response, send)
        except CancelledError:
            # Client disconnected during send_response (ignore exception).
            pass
        return response

    async def run(coro: typing.Any, outcome: tonio.Result) -> None:
        # Only a branch that actually completes stores an outcome. When the
        # scope is cancelled, tonio raises `CancelledError` through the losing
        # coroutine; we deliberately let it propagate (catching `Exception`,
        # not `BaseException`) so the coroutine is finalized by the scope and
        # its `Result` is left unset (None) — that is how we recognize the
        # branch that lost the race and got cancelled.
        try:
            outcome.store((_OK, await coro))
        except Exception as exc:
            outcome.store((_EXC, exc))
        finally:
            sentinel.set()

    async with tonio.scope() as scope:
        # The disconnect listener goes first so that, should it raise, it does
        # not prevent us from cancelling process_request().
        scope.spawn(run(handler.listen_for_disconnect(receive), disconnect_outcome))
        scope.spawn(run(process_request(), process_outcome))
        await sentinel.wait(None)
        scope.cancel()

    def _outcome(result: tonio.Result) -> tuple[int, typing.Any]:
        stored = result.fetch()
        return stored if stored is not None else (_CANCELLED, None)

    return _outcome(disconnect_outcome), _outcome(process_outcome)


async def _handle(self: typing.Any, scope: typing.Any, receive: typing.Any, send: typing.Any) -> None:
    # Receive the HTTP request body as a stream object.
    try:
        body_file = await self.read_body(receive)
    except RequestAborted:
        return
    # Request is complete and can be served.
    set_script_prefix(get_script_prefix(scope))
    await signals.request_started.asend(sender=self.__class__, scope=scope)
    # Get the request and check for basic issues.
    request, error_response = self.create_request(scope, body_file)
    if request is None:
        body_file.close()
        await self.send_response(error_response, send)
        await sync_to_async(error_response.close)()
        return

    # Race a disconnect against request processing; the loser is cancelled.
    disconnect_outcome, process_outcome = await _race(self, receive, send, request)

    # Surface exceptions raised by either branch (cancellations are swallowed),
    # inspecting the disconnect listener first to mirror Django's ordering.
    for state, value in (disconnect_outcome, process_outcome):
        if state == _EXC:
            if isinstance(value, RequestAborted):
                # Ignore client disconnects.
                continue
            body_file.close()
            raise value

    # Equivalent of Django's `response = tasks[1].result()` branch.
    state, value = process_outcome
    if state == _CANCELLED:
        await signals.request_finished.asend(sender=self.__class__)
    elif state == _OK:
        await sync_to_async(value.close)()

    body_file.close()


django.core.handlers.asgi.ASGIHandler.handle = _handle


# Mark the ASGI request entry point as an async context so `async_unsafe` and
# `Local(thread_critical=True)` behave correctly for code reached from a
# request (sync views are flipped back to a sync context by `sync_to_async`).
_orig_asgi_call = django.core.handlers.asgi.ASGIHandler.__call__


async def _asgi_call(self: typing.Any, scope: typing.Any, receive: typing.Any, send: typing.Any) -> typing.Any:
    _asgiref.async_context.set(True)
    return await _orig_asgi_call(self, scope, receive, send)


django.core.handlers.asgi.ASGIHandler.__call__ = _asgi_call
