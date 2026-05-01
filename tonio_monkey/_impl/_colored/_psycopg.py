from __future__ import annotations

import socket
import typing

import psycopg
import psycopg._acompat
import psycopg._conninfo_attempts_async
import psycopg.waiting
import tonio.colored as tonio
import tonio.colored.sync as sync
import tonio.colored.time as time
from psycopg import _conninfo_utils as _connutils, errors as e
from psycopg._enums import Ready, Wait
from tonio._colored._net._socket import getaddrinfo
from tonio._tonio import get_runtime


WAIT_R = Wait.R
WAIT_W = Wait.W
READY_R = Ready.R
READY_W = Ready.W

T = typing.TypeVar("T")


async def _wait_async(gen: typing.Any, fileno: int, interval: float = 0.0) -> typing.Any:
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")

    runtime = get_runtime()
    timeout = round(max(0, interval * 1_000_000)) if interval else None
    ev_r, ev_w = None, None

    try:
        s = next(gen)
        while True:
            reader = s & WAIT_R
            writer = s & WAIT_W
            if not (reader or writer):
                raise e.InternalError(f"bad poll status: {s}")

            ready = 0
            if reader and writer:
                if ev_r is None:
                    ev_r = runtime._io_event_r(fileno)
                if ev_w is None:
                    ev_w = runtime._io_event_w(fileno)
                await tonio.select(
                    ev_r.waiter(timeout),
                    ev_w.waiter(timeout),
                )
                if ev_r.is_set():
                    ready |= READY_R
                    ev_r = None
                if ev_w.is_set():
                    ready |= READY_W
                    ev_w = None
            elif reader:
                if ev_r is None:
                    ev_r = runtime._io_event_r(fileno)
                await ev_r.waiter(timeout)
                if ev_r.is_set():
                    ready |= READY_R
                    ev_r = None
            elif writer:
                if ev_w is None:
                    ev_w = runtime._io_event_w(fileno)
                await ev_w.waiter(timeout)
                if ev_w.is_set():
                    ready |= READY_W
                    ev_w = None

            s = gen.send(ready)

    except OSError as ex:
        raise e.OperationalError("connection socket closed") from ex
    except StopIteration as ex:
        rv: typing.Any = ex.value
        return rv


async def _wait_conn_async(gen: typing.Any, interval: float = 0.0) -> typing.Any:
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")

    runtime = get_runtime()
    timeout = round(max(0, interval * 1_000_000)) if interval else None
    ev_r, ev_w = None, None

    try:
        fileno, s = next(gen)
        while True:
            reader = s & WAIT_R
            writer = s & WAIT_W
            if not (reader or writer):
                raise e.InternalError(f"bad poll status: {s}")

            # FIXME: we need to wait for any of the events, not just the reader
            ready = 0
            if reader and writer:
                if ev_r is None:
                    ev_r = runtime._io_event_r(fileno)
                if ev_w is None:
                    ev_w = runtime._io_event_w(fileno)
                await tonio.select(
                    ev_r.waiter(timeout),
                    ev_w.waiter(timeout),
                )
                if ev_r.is_set():
                    ready |= READY_R
                    ev_r = None
                if ev_w.is_set():
                    ready |= READY_W
                    ev_w = None
            elif reader:
                if ev_r is None:
                    ev_r = runtime._io_event_r(fileno)
                await ev_r.waiter(timeout)
                if ev_r.is_set():
                    ready |= READY_R
                    ev_r = None
            elif writer:
                if ev_w is None:
                    ev_w = runtime._io_event_w(fileno)
                await ev_w.waiter(timeout)
                if ev_w.is_set():
                    ready |= READY_W
                    ev_w = None

            fileno, s = gen.send(ready)

    except StopIteration as ex:
        rv: typing.Any = ex.value
        return rv


class TonioAQueue(typing.Generic[T]):
    def __init__(self, maxsize: int = 0) -> None:
        if maxsize > 0:
            self._sender, self._receiver = sync.channel(maxsize)
            self._bounded = True
        else:
            self._sender, self._receiver = sync.unbounded_channel()
            self._bounded = False

    async def put(self, item: T) -> None:
        if self._bounded:
            await self._sender.send(item)
        else:
            self._sender.send(item)

    async def get(self) -> T:
        return await self._receiver.receive()


def _aspawn(
    f: typing.Callable[..., typing.Coroutine[typing.Any, typing.Any, None]],
    args: tuple[typing.Any, ...] = (),
    name: str | None = None,
) -> typing.Coroutine[typing.Any, typing.Any, None]:
    return f(*args)


async def _agather(*tasks: typing.Any, timeout: float | None = None) -> None:
    if not tasks:
        return
    coro = tonio.spawn.without_results(*tasks)
    if timeout is not None:
        await time.timeout(coro, timeout)
    else:
        await coro


async def _resolve_hostnames(params: typing.Any) -> list[dict[str, typing.Any]]:
    host = _connutils.get_param(params, "host")
    if not host or host.startswith("/") or host[1:2] == ":":
        return [params]

    if _connutils.get_param(params, "hostaddr"):
        return [params]

    if _connutils.is_ip_address(host):
        return [{**params, "hostaddr": host}]

    if not (port := _connutils.get_param(params, "port")):
        port_def = _connutils.get_param_def("port")
        port = port_def and port_def.compiled or "5432"

    ans = await getaddrinfo(host, port, proto=socket.IPPROTO_TCP, type=socket.SOCK_STREAM)

    return [{**params, "hostaddr": item[4][0]} for item in ans]


psycopg.waiting.wait_async = _wait_async
psycopg.waiting.wait_conn_async = _wait_conn_async
psycopg._acompat.ALock = sync.Lock
psycopg._acompat.AQueue = TonioAQueue
psycopg._acompat.aspawn = _aspawn
psycopg._acompat.agather = _agather
psycopg._conninfo_attempts_async._resolve_hostnames = _resolve_hostnames
