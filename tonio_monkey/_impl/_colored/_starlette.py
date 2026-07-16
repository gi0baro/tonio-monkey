from __future__ import annotations

import contextlib
import typing
import warnings

import anyio
import starlette as starlette
import starlette._exception_handler
import starlette._utils
import starlette.background
import starlette.concurrency
import starlette.datastructures
import starlette.endpoints
import starlette.middleware.base
import starlette.middleware.errors
import starlette.requests
import starlette.responses
import starlette.routing
import starlette.staticfiles
import tonio.colored as tonio
import tonio.colored.sync.channel as channel
import tonio.colored.time as tonio_time
from anyio import BrokenResourceError, EndOfStream  # inert exception classes only
from starlette.concurrency import _next, _StopIteration
from starlette.exceptions import StarletteDeprecationWarning
from starlette.middleware.base import _CachedRequest, _StreamingResponse
from starlette.requests import ClientDisconnect


# Starlette does no `asyncio` of its own — it is built entirely on `anyio`
# (task groups, memory object streams, `to_thread`, cancel scopes). Rather than
# provide an anyio backend, we rewrite the handful of starlette internals that
# touch anyio so they use tonio primitives directly:
#
#   anyio.to_thread.run_sync        -> tonio.spawn_blocking
#   anyio.create_task_group         -> tonio.scope() (see _TaskGroup)
#   anyio.create_memory_object_stream -> tonio unbounded channel (see _MemoryStream)
#   anyio.Event                     -> tonio.Event
#   anyio.CancelScope (standalone)  -> a tonio non-blocking receive
#
# One semantic gap drives the shape below: anyio task groups cancel the *host*
# task (the code inside `async with`), but tonio's `scope.cancel()` only cancels
# spawned children. Starlette's two "first-of-two-wins" sites rely on that host
# cancellation, so those are rewritten as explicit two-child races instead of a
# generic task group.
#
# FileResponse/StaticFiles are bridged onto the blocking threadpool (see the
# filesystem section) until tonio ships a native filesystem module. NOT patched:
# the TestClient (anyio blocking portal + threaded runtime — a separate effort;
# ASGI-level testing works via a plain in-process ASGI driver).


# --- concurrency ------------------------------------------------------------
# `run_in_threadpool(func, *args, **kwargs)` is exactly `tonio.spawn_blocking`
# (which already accepts args/kwargs), so it's rebound to it directly below —
# no wrapper coroutine needed.
async def _iterate_in_threadpool(iterator: typing.Iterable[typing.Any]) -> typing.AsyncIterator[typing.Any]:
    as_iterator = iter(iterator)
    while True:
        try:
            yield await tonio.spawn_blocking(_next, as_iterator)
        except _StopIteration:
            break


async def _run_until_first_complete(*args: typing.Any) -> None:
    warnings.warn(
        "run_until_first_complete is deprecated and will be removed in a future version.",
        StarletteDeprecationWarning,
        stacklevel=2,
    )
    await tonio.select(*[func(**kwargs) for func, kwargs in args])


# --- task group (anyio.create_task_group / create_collapsing_task_group) -----
class _CancelScope:
    def __init__(self, scope: typing.Any) -> None:
        self._scope = scope
        self._cancelled = False

    def cancel(self) -> None:
        if not self._cancelled:
            self._cancelled = True
            self._scope.cancel()


class _TaskGroup:
    """Minimal anyio-compatible task group over a tonio scope.

    Covers starlette's non-race usage: spawn background children with
    `start_soon`, join them on exit, and surface child exceptions as a
    `BaseExceptionGroup` (so `create_collapsing_task_group` can collapse a
    single exception). `cancel_scope.cancel()` cancels the spawned children.
    """

    def __init__(self) -> None:
        self._scope = tonio.scope()
        self.cancel_scope = _CancelScope(self._scope)
        self._errors: list[BaseException] = []

    async def __aenter__(self) -> _TaskGroup:
        await self._scope.__aenter__()
        return self

    def start_soon(self, func: typing.Callable[..., typing.Any], *args: typing.Any) -> None:
        async def wrap() -> None:
            try:
                await func(*args)
            except Exception as exc:
                self._errors.append(exc)
                self.cancel_scope.cancel()

        self._scope.spawn(wrap())

    async def __aexit__(self, exc_type: typing.Any, exc_value: typing.Any, traceback: typing.Any) -> None:
        await self._scope.__aexit__(exc_type, exc_value, traceback)
        if self._errors and exc_type is None:
            raise BaseExceptionGroup("unhandled errors in a TaskGroup", self._errors)


@contextlib.asynccontextmanager
async def _create_collapsing_task_group() -> typing.AsyncGenerator[_TaskGroup, None]:
    try:
        async with _TaskGroup() as task_group:
            yield task_group
    except BaseExceptionGroup as excs:
        if len(excs.exceptions) != 1:
            raise
        exc = excs.exceptions[0]
        context = None if exc.__suppress_context__ else exc.__context__
        raise exc from exc.__cause__ or context


# --- memory object stream (anyio.create_memory_object_stream) ----------------
_STREAM_CLOSE = object()


class _MemoryStreamState:
    def __init__(self) -> None:
        self.sender, self.receiver = channel.unbounded()
        self.send_closed = False
        self.recv_closed = False


class _SendStream:
    def __init__(self, state: _MemoryStreamState) -> None:
        self._state = state

    async def send(self, item: typing.Any) -> None:
        if self._state.recv_closed:
            raise BrokenResourceError
        self._state.sender.send(item)

    def close(self) -> None:
        if not self._state.send_closed:
            self._state.send_closed = True
            self._state.sender.send(_STREAM_CLOSE)

    def __enter__(self) -> _SendStream:
        return self

    def __exit__(self, *exc: typing.Any) -> None:
        self.close()


class _ReceiveStream:
    def __init__(self, state: _MemoryStreamState) -> None:
        self._state = state

    async def receive(self) -> typing.Any:
        item = await self._state.receiver.receive()
        if item is _STREAM_CLOSE:
            raise EndOfStream
        return item

    def close(self) -> None:
        self._state.recv_closed = True

    def __enter__(self) -> _ReceiveStream:
        return self

    def __exit__(self, *exc: typing.Any) -> None:
        self.close()

    def __aiter__(self) -> _ReceiveStream:
        return self

    async def __anext__(self) -> typing.Any:
        try:
            return await self.receive()
        except EndOfStream:
            raise StopAsyncIteration


def _memory_object_stream() -> tuple[_SendStream, _ReceiveStream]:
    state = _MemoryStreamState()
    return _SendStream(state), _ReceiveStream(state)


# --- StreamingResponse.__call__ (responses.py) ------------------------------
async def _streaming_response_call(self: typing.Any, scope: typing.Any, receive: typing.Any, send: typing.Any) -> None:
    if scope["type"] == "websocket":
        send = self._wrap_websocket_denial_send(send)
        await self.stream_response(send)
        if self.background is not None:
            await self.background()
        return

    spec_version = tuple(map(int, scope.get("asgi", {}).get("spec_version", "2.0").split(".")))

    if spec_version >= (2, 4):
        try:
            await self.stream_response(send)
        except OSError:
            raise ClientDisconnect()
    else:
        # Race streaming against a client disconnect; the first to finish cancels
        # the other (replaces anyio's host-task cancellation).
        await tonio.select(self.stream_response(send), self.listen_for_disconnect(receive))

    if self.background is not None:
        await self.background()


# --- Request.is_disconnected (requests.py) ----------------------------------
async def _request_is_disconnected(self: typing.Any) -> bool:
    if not self._is_disconnected:
        message: typing.Any = {}
        # Non-blocking receive: if a message isn't immediately available, move on
        # (replaces the `anyio.CancelScope` self-cancel idiom).
        result, completed = await tonio_time.timeout(self._receive(), 0)
        if completed and result is not None:
            message = result
        if message.get("type") == "http.disconnect":
            self._is_disconnected = True
    return self._is_disconnected


# --- BaseHTTPMiddleware.__call__ (middleware/base.py) ------------------------
async def _base_http_middleware_call(
    self: typing.Any, scope: typing.Any, receive: typing.Any, send: typing.Any
) -> None:
    if scope["type"] != "http":
        await self.app(scope, receive, send)
        return

    request = _CachedRequest(scope, receive)
    wrapped_receive = request.wrapped_receive
    response_sent = tonio.Event()
    app_exc: Exception | None = None
    exception_already_raised = False

    send_stream, recv_stream = _memory_object_stream()

    async def call_next(request: typing.Any) -> typing.Any:
        async def receive_or_disconnect() -> typing.Any:
            if response_sent.is_set():
                return {"type": "http.disconnect"}

            # anyio races this receive against `response_sent` so a receive that
            # outlives the response unblocks. We can't: nesting this select inside
            # another (e.g. StreamingResponse's disconnect race, which calls this
            # via listen_for_disconnect) intermittently deadlocks tonio's nested
            # scope cancellation. Instead the app task is cancelled once the
            # response is sent (see __call__), which serves the same purpose.
            message = await wrapped_receive()

            if response_sent.is_set():
                return {"type": "http.disconnect"}

            return message

        async def send_no_error(message: typing.Any) -> None:
            try:
                await send_stream.send(message)
            except BrokenResourceError:
                # recv_stream has been closed, i.e. response_sent has been set.
                return

        async def coro() -> None:
            nonlocal app_exc

            with send_stream:
                try:
                    await self.app(scope, receive_or_disconnect, send_no_error)
                except Exception as exc:
                    app_exc = exc

        task_group.start_soon(coro)

        try:
            message = await recv_stream.receive()
            info = message.get("info", None)
            if message["type"] == "http.response.debug" and info is not None:
                message = await recv_stream.receive()
        except EndOfStream:
            if app_exc is not None:
                nonlocal exception_already_raised
                exception_already_raised = True
                raise app_exc from app_exc.__cause__ or app_exc.__context__
            raise RuntimeError("No response returned.")

        assert message["type"] == "http.response.start"

        async def body_stream() -> typing.AsyncGenerator[typing.Any, None]:
            async for message in recv_stream:
                if message["type"] == "http.response.pathsend":
                    yield message
                    break
                assert message["type"] == "http.response.body", f"Unexpected message: {message}"
                body = message.get("body", b"")
                if body:
                    yield body
                if not message.get("more_body", False):
                    break

        response = _StreamingResponse(status_code=message["status"], content=body_stream(), info=info)
        response.raw_headers = message["headers"]
        return response

    with recv_stream, send_stream:
        async with _create_collapsing_task_group() as task_group:
            response = await self.dispatch_func(request, call_next)
            await response(scope, wrapped_receive, send)
            response_sent.set()
            recv_stream.close()
            # Stop the app task if it outlived the response (e.g. still blocked
            # on receive) so the task group can exit — replaces anyio's
            # response_sent race in receive_or_disconnect.
            task_group.cancel_scope.cancel()
    if app_exc is not None and not exception_already_raised:
        raise app_exc


# --- filesystem: FileResponse / StaticFiles ---------------------------------
# These call anyio directly: `anyio.to_thread.run_sync` for stat/lookup and
# `anyio.open_file` for streaming file bytes. Until tonio ships a filesystem
# module we bridge both onto the blocking threadpool — stats via spawn_blocking,
# and file reads through an async wrapper that offloads each blocking read/seek
# to spawn_blocking. A scoped `anyio` proxy is installed on the two modules that
# use it, leaving every other anyio attribute untouched.
class _AsyncFile:
    def __init__(self, file: typing.Any) -> None:
        self._file = file

    def read(self, size: int = -1) -> bytes:
        return tonio.spawn_blocking(self._file.read, size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return tonio.spawn_blocking(self._file.seek, offset, whence)

    async def __aenter__(self) -> _AsyncFile:
        return self

    async def __aexit__(self, *exc: typing.Any) -> None:
        await tonio.spawn_blocking(self._file.close)


async def _open_file(path: typing.Any, mode: str = "rb", *args: typing.Any, **kwargs: typing.Any) -> _AsyncFile:
    file = await tonio.spawn_blocking(open, path, mode, *args, **kwargs)
    return _AsyncFile(file)


def _to_thread_run_sync(
    func: typing.Callable[..., typing.Any],
    *args: typing.Any,
    abandon_on_cancel: bool = False,
    cancellable: bool | None = None,
    limiter: typing.Any = None,
) -> typing.Any:
    return tonio.spawn_blocking(func, *args)


class _ToThreadProxy:
    run_sync = staticmethod(_to_thread_run_sync)


class _AnyioFilesystemProxy:
    to_thread = _ToThreadProxy()
    open_file = staticmethod(_open_file)

    def __init__(self, real: typing.Any) -> None:
        self._real = real

    def __getattr__(self, name: str) -> typing.Any:
        return getattr(self._real, name)


# --- install rewrites -------------------------------------------------------
# `run_in_threadpool` is imported by name across many modules; rebind each.
for _mod in (
    starlette.concurrency,
    starlette.background,
    starlette._exception_handler,
    starlette.routing,
    starlette.endpoints,
    starlette.datastructures,
    starlette.middleware.errors,
):
    _mod.run_in_threadpool = tonio.spawn_blocking

starlette.concurrency.iterate_in_threadpool = _iterate_in_threadpool
starlette.responses.iterate_in_threadpool = _iterate_in_threadpool
starlette.concurrency.run_until_first_complete = _run_until_first_complete

for _mod in (
    starlette._utils,
    starlette.responses,
    starlette.middleware.base,
):
    _mod.create_collapsing_task_group = _create_collapsing_task_group

starlette.responses.StreamingResponse.__call__ = _streaming_response_call
starlette.requests.Request.is_disconnected = _request_is_disconnected
starlette.middleware.base.BaseHTTPMiddleware.__call__ = _base_http_middleware_call

# Bridge FileResponse/StaticFiles anyio filesystem calls onto the blocking pool.
_anyio_filesystem_proxy = _AnyioFilesystemProxy(anyio)
starlette.responses.anyio = _anyio_filesystem_proxy
starlette.staticfiles.anyio = _anyio_filesystem_proxy
