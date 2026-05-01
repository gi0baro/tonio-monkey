from __future__ import annotations

import ssl
import typing

import httpcore
import httpcore._backends.auto
import httpcore._synchronization
import httpx as httpx
import tonio.colored as tonio
import tonio.colored.net as net
import tonio.colored.net.tls
import tonio.colored.sync as sync
import tonio.colored.time
import tonio.exceptions
from httpcore._backends.base import SOCKET_OPTION, AsyncNetworkBackend, AsyncNetworkStream
from httpcore._exceptions import (
    ConnectError,
    ConnectTimeout,
    PoolTimeout,
    ReadError,
    ReadTimeout,
    WriteError,
    WriteTimeout,
    map_exceptions,
)
from httpcore._utils import is_socket_readable


class TonioStream(AsyncNetworkStream):
    def __init__(
        self,
        stream: typing.Any,
        *,
        socket_stream: typing.Any | None = None,
    ) -> None:
        self._stream = stream
        self._socket_stream = socket_stream or stream

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        exc_map: dict[type[Exception], type[Exception]] = {
            tonio.exceptions.ResourceBroken: ReadError,
            OSError: ReadError,
        }
        with map_exceptions(exc_map):
            if timeout is None:
                return await self._stream.receive_some(max_bytes)
            result, completed = await tonio.time.timeout(self._stream.receive_some(max_bytes), timeout)
            if not completed:
                raise ReadTimeout("Timed out")
            return result

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not buffer:
            return
        exc_map: dict[type[Exception], type[Exception]] = {
            tonio.exceptions.ResourceBroken: WriteError,
            OSError: WriteError,
        }
        with map_exceptions(exc_map):
            if timeout is None:
                await self._stream.send_all(buffer)
                return
            _, completed = await tonio.time.timeout(self._stream.send_all(buffer), timeout)
            if not completed:
                raise WriteTimeout("Timed out")

    async def aclose(self) -> None:
        if isinstance(self._stream, net.tls.TLSStream):
            await self._stream.close()
        else:
            self._stream.close()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        exc_map: dict[type[Exception], type[Exception]] = {
            tonio.exceptions.ResourceBroken: ConnectError,
            OSError: ConnectError,
            ssl.SSLError: ConnectError,
        }
        tls_stream = net.tls.TLSStream(
            self._stream,
            ssl_context,
            server_hostname=server_hostname,
            https_compatible=True,
        )
        with map_exceptions(exc_map):
            if timeout is None:
                await tls_stream.handshake()
            else:
                _, completed = await tonio.time.timeout(tls_stream.handshake(), timeout)
                if not completed:
                    raise ConnectTimeout("Timed out")
        return TonioStream(tls_stream, socket_stream=self._socket_stream)

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            if isinstance(self._stream, net.tls.TLSStream):
                return self._stream._ssl
            return None
        if info == "client_addr":
            return self._socket_stream.socket.getsockname()
        if info == "server_addr":
            return self._socket_stream.socket.getpeername()
        if info == "socket":
            return self._socket_stream.socket._sock
        if info == "is_readable":
            return is_socket_readable(self._socket_stream.socket._sock)
        return None


class TonioBackend(AsyncNetworkBackend):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        exc_map: dict[type[Exception], type[Exception]] = {
            tonio.exceptions.ResourceBroken: ConnectError,
            OSError: ConnectError,
        }
        with map_exceptions(exc_map):
            if timeout is None:
                stream = await net.open_tcp_stream(host, port, local_address=local_address)
            else:
                result, completed = await tonio.time.timeout(
                    net.open_tcp_stream(host, port, local_address=local_address),
                    timeout,
                )
                if not completed:
                    raise ConnectTimeout("Timed out")
                stream = result
            for option in socket_options or []:
                stream.socket.setsockopt(*option)
        return TonioStream(stream)

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        exc_map: dict[type[Exception], type[Exception]] = {
            tonio.exceptions.ResourceBroken: ConnectError,
            OSError: ConnectError,
        }
        with map_exceptions(exc_map):
            if timeout is None:
                stream = await net.open_unix_socket(path)
            else:
                result, completed = await tonio.time.timeout(
                    net.open_unix_socket(path),
                    timeout,
                )
                if not completed:
                    raise ConnectTimeout("Timed out")
                stream = result
            for option in socket_options or []:
                stream.socket.setsockopt(*option)
        return TonioStream(stream)

    async def sleep(self, seconds: float) -> None:
        await tonio.sleep(seconds)


class TonioAsyncEvent:
    def __init__(self) -> None:
        self._event = tonio.Event()

    def set(self) -> None:
        self._event.set()

    async def wait(self, timeout: float | None = None) -> None:
        await self._event.wait(timeout)
        if timeout is not None and not self._event.is_set():
            raise PoolTimeout("Timed out")


class TonioAsyncSemaphore:
    def __init__(self, bound: int) -> None:
        self._semaphore = sync.Semaphore(bound)

    async def acquire(self) -> None:
        if event := self._semaphore.acquire():
            await event.waiter(None)

    async def release(self) -> None:
        self._semaphore.release()


class TonioAsyncShieldCancellation:
    def __enter__(self) -> TonioAsyncShieldCancellation:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: typing.Any = None,
    ) -> bool:
        if exc_type is not None and issubclass(exc_type, tonio.exceptions.CancelledError):
            return True
        return False


async def _tonio_init_backend(self: typing.Any) -> None:
    if not hasattr(self, "_backend"):
        self._backend = TonioBackend()


httpcore._backends.auto.AutoBackend._init_backend = _tonio_init_backend
httpcore._synchronization.AsyncLock = sync.Lock
httpcore._synchronization.AsyncEvent = TonioAsyncEvent
httpcore._synchronization.AsyncSemaphore = TonioAsyncSemaphore
httpcore._synchronization.AsyncShieldCancellation = TonioAsyncShieldCancellation
