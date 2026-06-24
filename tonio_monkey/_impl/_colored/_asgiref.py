from __future__ import annotations

import contextvars
import functools
import typing

import asgiref as asgiref
import asgiref.local
import asgiref.sync
import asgiref.testing
import tonio.colored as tonio
import tonio.colored.exceptions as tonio_exc
import tonio.colored.sync.channel as channel
import tonio.colored.time as tonio_time
from asgiref.compatibility import guarantee_single_callable
from asgiref.sync import _restore_context


# `asgiref` is built entirely on top of `asyncio`: `AsyncToSync` schedules the
# awaitable on a running event loop (or spins a new one in a thread) and
# `SyncToAsync` offloads sync callables through `loop.run_in_executor`. None of
# that machinery exists under the tonio runtime, so we replace the two hot
# methods with tonio-native implementations:
#
#   sync_to_async(fn)()   ->  await tonio.spawn_blocking(fn, ...)
#   async_to_sync(coro)() ->  tonio.block_on(coro)
#
# Everything else on the classes (context restoration, `__get__` method
# binding, `markcoroutinefunction`, the `executor`/`context` kwargs) is left
# untouched, so already-bound `from asgiref.sync import sync_to_async` imports
# keep working: we only swap the dispatch primitive underneath.


# Tracks whether the current logical execution is running "inside" the tonio
# runtime as async code. tonio has no notion of a thread-bound event loop the
# way asyncio does, so we maintain the async/sync distinction ourselves:
#
#   - set True when entering async code (`async_to_sync` wrapper, and the
#     patched ASGI handler entry point in `_django`);
#   - set False inside the `sync_to_async` worker, so sync code (and Django's
#     `async_unsafe` guard) sees a synchronous context.
#
# It is a contextvar so it rides along tonio's context propagation across
# `spawn`/`spawn_blocking` (the runtime must be created with `context=True`,
# which both the ASGI server and the test runner do).
async_context: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "tonio_monkey_asgiref_async_context", default=False
)


def in_async_context() -> bool:
    return async_context.get()


def _async_to_sync_call(self: typing.Any, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
    __traceback_hide__ = True  # noqa: F841

    # You cannot block on the runtime from a thread that is itself running async
    # code — just like asgiref forbids `async_to_sync` inside a running loop.
    if async_context.get():
        raise RuntimeError(
            "You cannot use async_to_sync while running async code on the tonio "
            "runtime - just await the async function directly."
        )

    # Wrap the captured context in a list so the inner coroutine can reassign it
    # to propagate contextvar mutations back to this (sync) caller.
    context = [contextvars.copy_context()]

    async def wrapper() -> typing.Any:
        __traceback_hide__ = True  # noqa: F841
        _restore_context(context[0])
        token = async_context.set(True)
        try:
            return await self.awaitable(*args, **kwargs)
        finally:
            async_context.reset(token)
            context[0] = contextvars.copy_context()

    try:
        return tonio.block_on(wrapper())
    finally:
        _restore_context(context[0])


async def _sync_to_async_call(self: typing.Any, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
    __traceback_hide__ = True  # noqa: F841

    # asgiref's `thread_sensitive` machinery pins sync code to a single shared
    # thread (for asyncio's loop-bound executors). tonio's blocking threadpool
    # already runs each call on a dedicated worker, so we route every call
    # through `spawn_blocking` regardless of `thread_sensitive`/`executor`.
    context = self.context if self.context is not None else contextvars.copy_context()
    child = functools.partial(self.func, *args, **kwargs)
    outer_async = async_context.get()

    def runner() -> typing.Any:
        __traceback_hide__ = True  # noqa: F841

        def inner() -> typing.Any:
            # Sync code must observe a synchronous context even though the
            # context was copied from an async frame.
            async_context.set(False)
            return child()

        return context.run(inner)

    try:
        return await tonio.spawn_blocking(runner)
    finally:
        if self.context is None:
            # Propagate contextvar mutations made by the sync callable back to
            # the async caller (mirrors asgiref's own behaviour)...
            _restore_context(context)
        # ...but don't let the worker's synchronous marker leak back.
        async_context.set(outer_async)


asgiref.sync.AsyncToSync.__call__ = _async_to_sync_call
asgiref.sync.SyncToAsync.__call__ = _sync_to_async_call


# `asgiref.local.Local(thread_critical=True)` decides between thread-local and
# contextvar storage by probing for a running asyncio loop. Re-point its
# `asyncio` reference so that probe reflects the tonio async/sync state.
class _LocalAsyncioProxy:
    def __init__(self, orig: typing.Any) -> None:
        self._orig = orig

    def get_running_loop(self) -> typing.Any:
        if async_context.get():
            return _async_context_sentinel
        raise RuntimeError("no running event loop")

    def __getattr__(self, name: str) -> typing.Any:
        return getattr(self._orig, name)


_async_context_sentinel = object()

asgiref.local.asyncio = _LocalAsyncioProxy(asgiref.local.asyncio)


# --- asgiref.testing.ApplicationCommunicator -------------------------------
#
# The testing harness Django and Channels use to drive an ASGI app at the
# message level. asgiref runs the app as an `asyncio.create_task` future (with
# `done()`/`result()`/`cancel()`), pumps messages through two `asyncio.Queue`s,
# and bounds receives with a deadline timeout. We reimplement it on tonio:
#   - the app runs as a `_CommTask` (a scope-backed, externally cancellable
#     background task with a result handle);
#   - the queues are tonio unbounded channels;
#   - receives are bounded with `tonio.colored.time.timeout`.
_TASK_OK = 0
_TASK_ERR = 1
_unset = object()


class _CommTask:
    """A long-lived background task with an outcome handle and external cancel.

    tonio processes cancellation when a scope exits, so the driver coroutine
    parks on a dedicated `_exit` event (set either when the body finishes or
    when `cancel()` is requested) and only then leaves the scope — calling
    `scope.cancel()` as the last statement inside the block when a cancellation
    was requested. A cancelled body never stores an outcome (`_result` stays
    unset), which is how `result()` recognizes cancellation.
    """

    def __init__(self, coro: typing.Any) -> None:
        self._done = tonio.Event()
        self._exit = tonio.Event()
        self._result = tonio.Result()
        self._cancel_requested = False
        self._scope = tonio.scope()

        async def body() -> None:
            try:
                self._result.store((_TASK_OK, await coro))
            except Exception as exc:
                self._result.store((_TASK_ERR, exc))
            finally:
                self._exit.set()

        async def driver() -> None:
            async with self._scope:
                self._scope.spawn(body())
                await self._exit.wait(None)
                if self._cancel_requested:
                    self._scope.cancel()
            self._done.set()

        # Run the app in a fresh context so testing-scope contextvars don't leak
        # into it (matches asgiref's `contextvars.Context().run(...)`).
        contextvars.Context().run(tonio.spawn.without_tracking, driver())

    def done(self) -> bool:
        return self._done.is_set()

    def cancel(self) -> None:
        if not self._done.is_set():
            self._cancel_requested = True
            self._exit.set()

    def result(self) -> typing.Any:
        stored = self._result.fetch()
        if stored is None:
            raise tonio_exc.CancelledError()
        state, value = stored
        if state == _TASK_ERR:
            raise value
        return value

    def __await__(self) -> typing.Any:
        return self._await().__await__()

    async def _await(self) -> typing.Any:
        await self._done.wait(None)
        return self.result()


class TonioApplicationCommunicator:
    def __init__(self, application: typing.Any, scope: typing.Any) -> None:
        self.application = guarantee_single_callable(application)
        self.scope = scope
        self._input_sender, self._input_receiver = channel.unbounded()
        self._output_sender, self._output_receiver = channel.unbounded()
        self._task: _CommTask | None = None
        self._peeked: typing.Any = _unset

    @property
    def future(self) -> _CommTask:
        if self._task is None:

            async def receive() -> typing.Any:
                return await self._input_receiver.receive()

            async def send(message: typing.Any) -> None:
                self._output_sender.send(message)

            self._task = _CommTask(self.application(self.scope, receive, send))
        return self._task

    async def send_input(self, message: typing.Any) -> None:
        if self.future.done():
            self.future.result()
        self._input_sender.send(message)

    async def receive_output(self, timeout: float = 1) -> typing.Any:
        if self._peeked is not _unset:
            message, self._peeked = self._peeked, _unset
            return message
        if self.future.done():
            self.future.result()
        result, completed = await tonio_time.timeout(self._output_receiver.receive(), timeout)
        if not completed:
            if self.future.done():
                self.future.result()
            else:
                self.future.cancel()
            raise TimeoutError("receive_output timed out")
        return result

    async def receive_nothing(self, timeout: float = 0.1, interval: float = 0.01) -> bool:
        if self._peeked is not _unset:
            return False
        if self.future.done():
            self.future.result()
        result, completed = await tonio_time.timeout(self._output_receiver.receive(), timeout)
        if completed:
            self._peeked = result
            return False
        return True

    async def wait(self, timeout: float = 1) -> None:
        async def _drain() -> None:
            try:
                await self.future
            except tonio_exc.CancelledError:
                pass

        _, completed = await tonio_time.timeout(_drain(), timeout)
        if not completed and not self.future.done():
            self.future.cancel()
            try:
                await self.future
            except tonio_exc.CancelledError:
                pass
        if self.future.done():
            try:
                self.future.result()
            except tonio_exc.CancelledError:
                pass

    def stop(self, exceptions: bool = True) -> None:
        if self._task is None:
            return
        if not self._task.done():
            self._task.cancel()
        elif exceptions:
            self._task.result()

    def __del__(self) -> None:
        try:
            self.stop(exceptions=False)
        except RuntimeError:
            pass


asgiref.testing.ApplicationCommunicator = TonioApplicationCommunicator
