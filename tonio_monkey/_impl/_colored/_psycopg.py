from __future__ import annotations

import socket
import typing

import psycopg
import psycopg._acompat
import psycopg._conninfo_attempts_async
import psycopg.waiting
import tonio.colored as tonio
import tonio.colored.io as io
import tonio.colored.sync as sync
import tonio.colored.sync.channel as channel
import tonio.colored.time as time
from psycopg import _conninfo_utils as _connutils, errors as e
from psycopg._enums import Ready, Wait
from tonio._colored._net._socket import getaddrinfo


WAIT_R = Wait.R
WAIT_W = Wait.W
READY_R = Ready.R
READY_W = Ready.W

T = typing.TypeVar("T")


async def _wait_async(gen: typing.Any, fileno: int, interval: float = 0.0) -> typing.Any:
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")

    timeout = interval if interval else None
    reg = io.register(fileno)

    try:
        s = next(gen)
        while True:
            reader = s & WAIT_R
            writer = s & WAIT_W
            if not (reader or writer):
                raise e.InternalError(f"bad poll status: {s}")

            if reader and writer:
                w_r = reg.arm_r(timeout)
                w_w = reg.arm_w(timeout)
                if w_r is not None and w_w is not None:
                    await tonio.select(w_r, w_w)
            elif reader:
                if (w_r := reg.arm_r(timeout)) is not None:
                    await w_r
            elif writer:
                if (w_w := reg.arm_w(timeout)) is not None:
                    await w_w

            ready = 0
            if reader and reg.consume_r():
                ready |= READY_R
            if writer and reg.consume_w():
                ready |= READY_W

            s = gen.send(ready)

    except OSError as ex:
        raise e.OperationalError("connection socket closed") from ex
    except StopIteration as ex:
        rv: typing.Any = ex.value
        return rv
    finally:
        reg.close()


async def _wait_conn_async(gen: typing.Any, interval: float = 0.0) -> typing.Any:
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")

    timeout = interval if interval else None
    reg = None
    reg_fileno = None

    try:
        fileno, s = next(gen)
        while True:
            reader = s & WAIT_R
            writer = s & WAIT_W
            if not (reader or writer):
                raise e.InternalError(f"bad poll status: {s}")

            # NOTE: the fd can change between rounds (multi-host attempts)
            if reg is None or reg_fileno != fileno:
                if reg is not None:
                    reg.close()
                reg = io.register(fileno)
                reg_fileno = fileno

            if reader and writer:
                w_r = reg.arm_r(timeout)
                w_w = reg.arm_w(timeout)
                if w_r is not None and w_w is not None:
                    await tonio.select(w_r, w_w)
            elif reader:
                if (w_r := reg.arm_r(timeout)) is not None:
                    await w_r
            elif writer:
                if (w_w := reg.arm_w(timeout)) is not None:
                    await w_w

            ready = 0
            if reader and reg.consume_r():
                ready |= READY_R
            if writer and reg.consume_w():
                ready |= READY_W

            fileno, s = gen.send(ready)

    except StopIteration as ex:
        rv: typing.Any = ex.value
        return rv
    finally:
        if reg is not None:
            reg.close()


class TonioAQueue(typing.Generic[T]):
    def __init__(self, maxsize: int = 0) -> None:
        if maxsize > 0:
            self._sender, self._receiver = channel.channel(maxsize)
            self._bounded = True
        else:
            self._sender, self._receiver = channel.unbounded()
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
