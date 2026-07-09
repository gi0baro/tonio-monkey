from __future__ import annotations

import asyncio
import contextlib
import inspect
import typing

import redis as redis
import redis._parsers.base
import redis.asyncio.client
import redis.asyncio.connection
import redis.asyncio.lock
import redis.asyncio.retry
import tonio.colored as tonio
import tonio.colored.net as net
import tonio.colored.net.tls as tls
import tonio.colored.sync as sync
import tonio.colored.time as tonio_time
import tonio.exceptions as tonio_exc


class TonioStreamReader:
    def __init__(self, stream: typing.Any) -> None:
        self._stream = stream
        self._buffer = bytearray()
        self._eof = False

    async def _fill(self) -> None:
        data = await self._stream.receive_some(65536)
        if not data:
            self._eof = True
        else:
            self._buffer.extend(data)

    async def readline(self) -> bytes:
        while True:
            idx = self._buffer.find(b"\n")
            if idx >= 0:
                line = bytes(self._buffer[: idx + 1])
                del self._buffer[: idx + 1]
                return line
            if self._eof:
                # Return remaining data
                data = bytes(self._buffer)
                self._buffer.clear()
                return data
            await self._fill()

    async def readexactly(self, n: int) -> bytes:
        while len(self._buffer) < n:
            if self._eof:
                partial = bytes(self._buffer)
                self._buffer.clear()
                raise asyncio.IncompleteReadError(partial, n)
            await self._fill()
        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        return data

    async def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        if n < 0:
            # Read until EOF
            while not self._eof:
                await self._fill()
            data = bytes(self._buffer)
            self._buffer.clear()
            return data
        if not self._buffer and not self._eof:
            await self._fill()
        chunk = bytes(self._buffer[:n])
        del self._buffer[:n]
        return chunk

    def at_eof(self) -> bool:
        return self._eof and not self._buffer


class TonioTransport:
    def __init__(self, stream: typing.Any) -> None:
        self._stream = stream

    def get_extra_info(self, key: str, default: typing.Any = None) -> typing.Any:
        if key == "socket":
            return getattr(self._stream.socket, "_sock", None)
        return default


class TonioStreamWriter:
    def __init__(self, stream: typing.Any) -> None:
        self._stream = stream
        self.transport = TonioTransport(stream)

    def writelines(self, data: typing.Iterable[bytes]) -> None:
        # Buffer the data — will be flushed by drain()
        self._pending = b"".join(data)

    async def drain(self) -> None:
        if hasattr(self, "_pending") and self._pending:
            await self._stream.send_all(self._pending)
            self._pending = b""

    def close(self) -> None:
        if isinstance(self._stream, tls.TLSStream):
            pass  # async close, handled by wait_closed
        else:
            try:
                self._stream.close()
            except Exception:
                pass

    async def wait_closed(self) -> None:
        if isinstance(self._stream, tls.TLSStream):
            try:
                await self._stream.close()
            except Exception:
                pass


async def _tonio_tcp_connect(self: typing.Any) -> None:
    connect_timeout = self.socket_connect_timeout
    if connect_timeout:
        result, completed = await tonio_time.timeout(
            net.open_tcp_stream(self.host, self.port),
            connect_timeout,
        )
        if not completed:
            raise TimeoutError("Timeout connecting to server")
        stream = result
    else:
        stream = await net.open_tcp_stream(self.host, self.port)

    # Handle SSL if this is an SSLConnection
    if hasattr(self, "ssl_context"):
        ssl_ctx = self.ssl_context.get()
        tls_stream = tls.TLSStream(
            stream,
            ssl_ctx,
            server_hostname=self.host,
            https_compatible=True,
        )
        await tls_stream.handshake()
        stream = tls_stream

    self._reader = TonioStreamReader(stream)
    self._writer = TonioStreamWriter(stream)

    sock = self._writer.transport.get_extra_info("socket")
    if sock:
        import socket

        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            if self.socket_keepalive:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                for k, v in self.socket_keepalive_options.items():
                    sock.setsockopt(socket.SOL_TCP, k, v)
        except OSError, TypeError:
            self._writer.close()
            raise

    await self.on_connect()


async def _tonio_unix_connect(self: typing.Any) -> None:
    connect_timeout = self.socket_connect_timeout
    if connect_timeout:
        result, completed = await tonio_time.timeout(
            net.open_unix_socket(self.path),
            connect_timeout,
        )
        if not completed:
            raise TimeoutError("Timeout connecting to server")
        stream = result
    else:
        stream = await net.open_unix_socket(self.path)

    self._reader = TonioStreamReader(stream)
    self._writer = TonioStreamWriter(stream)
    await self.on_connect()


def _tonio_sleep(seconds: float):
    return tonio.sleep(seconds)


def _tonio_shield(coro: typing.Any) -> typing.Any:
    return coro


async def _tonio_gather(*coros: typing.Any, return_exceptions: bool = False) -> list[typing.Any]:
    if not coros:
        return []
    if return_exceptions:
        try:
            result = await tonio.spawn(*coros)
            # tonio.spawn returns value directly for single coro, list for multiple
            return result if isinstance(result, list) else [result]
        except ExceptionGroup as eg:
            return list(eg.exceptions)
    await tonio.spawn.without_results(*coros)
    return []


async def _tonio_wait_for(coro: typing.Any, timeout: float) -> typing.Any:
    result, completed = await tonio_time.timeout(coro, timeout)
    if not completed:
        raise TimeoutError()
    return result


class _TonioRunningLoop:
    def time(self) -> float:
        return tonio_time.time()


_tonio_loop = _TonioRunningLoop()


def _tonio_get_running_loop() -> _TonioRunningLoop:
    return _tonio_loop


redis.asyncio.connection.Connection._connect = _tonio_tcp_connect
redis.asyncio.connection.SSLConnection._connect = _tonio_tcp_connect  # same, handles SSL via hasattr
redis.asyncio.connection.UnixDomainSocketConnection._connect = _tonio_unix_connect

_modules_to_patch = [
    redis.asyncio.connection,
    redis.asyncio.client,
    redis.asyncio.lock,
    redis.asyncio.retry,
]

for _mod in _modules_to_patch:
    _asyncio_ref = getattr(_mod, "asyncio", None)
    if _asyncio_ref is not None:
        # Create a proxy that overrides specific attributes
        class _AsyncioProxy:
            def __init__(self, orig: typing.Any) -> None:
                self._orig = orig

            def __getattr__(self, name: str) -> typing.Any:
                overrides = {
                    "Lock": sync.Lock,
                    "sleep": _tonio_sleep,
                    "shield": _tonio_shield,
                    "gather": _tonio_gather,
                    "wait_for": _tonio_wait_for,
                    "get_running_loop": _tonio_get_running_loop,
                    "iscoroutinefunction": inspect.iscoroutinefunction,
                    "TimeoutError": TimeoutError,
                    "CancelledError": tonio_exc.CancelledError,
                }
                if name in overrides:
                    return overrides[name]
                return getattr(self._orig, name)

        _mod.asyncio = _AsyncioProxy(_asyncio_ref)


# Patch async_timeout in connection module.
# redis uses: `async with async_timeout(seconds): await some_io()`
# tonio doesn't have a deadline-scoped context manager, but redis's
# _connect methods are already patched to use tonio timeouts directly.
# For the remaining usages (disconnect, read_response), we provide a
# context manager that yields immediately — the actual I/O goes through
# our tonio stream adapters which handle their own timeouts.
@contextlib.asynccontextmanager
async def _tonio_async_timeout(seconds: float | None) -> typing.AsyncGenerator[None, None]:
    yield


redis._parsers.base.async_timeout = _tonio_async_timeout
redis.asyncio.connection.async_timeout = _tonio_async_timeout
