from __future__ import annotations

import random
import ssl as ssl_module
import struct
import time as stdlib_time
import typing
from collections.abc import AsyncIterator, Generator, Mapping

import tonio.colored as tonio
import tonio.colored.net as net
import tonio.colored.net.tls as tls
import tonio.colored.sync as sync
import tonio.colored.sync.channel as channel
import tonio.colored.time as tonio_time
import tonio.exceptions as tonio_exc
import websockets as websockets
import websockets.asyncio.client
from websockets.client import ClientProtocol
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedOK,
)
from websockets.frames import DATA_OPCODES, CloseCode, Frame, Opcode
from websockets.http11 import USER_AGENT, Response
from websockets.typing import BytesLike, Data, DataLike, Subprotocol
from websockets.uri import parse_uri


class TonioClientConnection:
    def __init__(
        self,
        protocol: ClientProtocol,
        stream: typing.Any,
        *,
        ping_interval: float | None = 20,
        ping_timeout: float | None = 20,
        close_timeout: float | None = 10,
    ) -> None:
        self._protocol = protocol
        self._stream = stream
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._close_timeout = close_timeout

        self._close_event = tonio.Event()
        self._recv_sender, self._recv_receiver = channel.unbounded()
        self._send_lock = sync.Lock()
        self._recv_exc: BaseException | None = None
        self._closed = False
        self._pending_pings: dict[bytes, tuple[tonio.Event, float]] = {}
        self.latency: float = 0.0

        self.request = None
        self.response = None
        self.id = protocol.id
        self.protocol = protocol

    async def recv(self, decode: bool | None = None) -> Data:
        while True:
            if self._closed:
                raise self._protocol.close_exc from self._recv_exc

            try:
                frame = self._recv_receiver._receive()
                _, blocking, message = frame
                if not blocking:
                    return self._decode_frame(message, decode)
            except Exception:
                pass

            # Wait for data or close
            recv_event = tonio.Event()
            self._recv_notify = recv_event
            try:
                await recv_event.wait(None)
            finally:
                self._recv_notify = None

            if self._closed:
                raise self._protocol.close_exc from self._recv_exc

    async def send(self, message: DataLike, text: bool | None = None) -> None:
        async with self._send_lock:
            if isinstance(message, str):
                if text is False:
                    self._protocol.send_binary(message.encode())
                else:
                    self._protocol.send_text(message.encode())
            elif isinstance(message, BytesLike):
                if text is True:
                    self._protocol.send_text(message)
                else:
                    self._protocol.send_binary(message)
            elif isinstance(message, Mapping):
                raise TypeError("data is a dict-like object")
            else:
                raise TypeError("data must be str or bytes-like")
            await self._send_data()

    async def close(
        self,
        code: CloseCode | int = CloseCode.NORMAL_CLOSURE,
        reason: str = "",
    ) -> None:
        if self._closed:
            return
        try:
            async with self._send_lock:
                self._protocol.send_close(code, reason)
                await self._send_data()
        except ConnectionClosed:
            pass
        if self._close_timeout is not None:
            await self._close_event.wait(self._close_timeout)
        if not self._closed:
            self._do_close()

    async def ping(self, data: DataLike | None = None) -> tonio.Event:
        if isinstance(data, BytesLike):
            data = bytes(data)
        elif isinstance(data, str):
            data = data.encode()
        elif data is not None:
            raise TypeError("data must be str or bytes-like")

        while data is None or data in self._pending_pings:
            data = struct.pack("!I", random.getrandbits(32))

        pong_event = tonio.Event()
        self._pending_pings[data] = (pong_event, stdlib_time.monotonic())

        async with self._send_lock:
            self._protocol.send_ping(data)
            await self._send_data()

        return pong_event

    async def wait_closed(self) -> None:
        await self._close_event.wait(None)

    async def __aenter__(self) -> TonioClientConnection:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: typing.Any,
    ) -> None:
        if exc_type is None:
            await self.close()
        else:
            await self.close(CloseCode.INTERNAL_ERROR)

    async def __aiter__(self) -> AsyncIterator[Data]:
        try:
            while True:
                yield await self.recv()
        except ConnectionClosedOK:
            return

    async def _reader(self) -> None:
        try:
            while not self._closed:
                data = await self._stream.receive_some(65536)
                if not data:
                    self._protocol.receive_eof()
                    self._process_protocol()
                    break
                self._protocol.receive_data(data)
                self._process_protocol()
        except tonio_exc.ResourceBroken as exc:
            self._recv_exc = exc
        except Exception as exc:
            self._recv_exc = exc
        finally:
            self._do_close()

    def _process_protocol(self) -> None:
        events = self._protocol.events_received()
        # Flush outgoing data (e.g. automatic pong replies)
        for out in self._protocol.data_to_send():
            if out:
                self._stream.send_all_sync(out) if hasattr(self._stream, "send_all_sync") else None
        self._flush_sync()

        for event in events:
            self._process_event(event)

    def _flush_sync(self) -> None:
        # Best-effort sync flush for automatic protocol responses (pongs)
        pass

    def _process_event(self, event: typing.Any) -> None:
        if isinstance(event, Response):
            self.response = event
            return

        assert isinstance(event, Frame)
        if event.opcode in DATA_OPCODES:
            self._recv_sender.send(event)
            if hasattr(self, "_recv_notify") and self._recv_notify is not None:
                self._recv_notify.set()

        if event.opcode is Opcode.PONG:
            self._acknowledge_pings(bytes(event.data))

    def _acknowledge_pings(self, data: bytes) -> None:
        if data not in self._pending_pings:
            return

        pong_timestamp = stdlib_time.monotonic()
        ping_ids = []
        for ping_id, (pong_event, ping_timestamp) in self._pending_pings.items():
            ping_ids.append(ping_id)
            self.latency = pong_timestamp - ping_timestamp
            pong_event.set()
            if ping_id == data:
                break

        for ping_id in ping_ids:
            del self._pending_pings[ping_id]

    async def _send_data(self) -> None:
        for data in self._protocol.data_to_send():
            if data:
                await self._stream.send_all(data)

    def _do_close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._protocol.receive_eof()
        try:
            if isinstance(self._stream, tls.TLSStream):
                pass  # TLS close is async, skip in sync context
            else:
                self._stream.close()
        except Exception:
            pass
        self._close_event.set()
        # Wake recv if waiting
        if hasattr(self, "_recv_notify") and self._recv_notify is not None:
            self._recv_notify.set()
        # Fail pending pings
        for pong_event, _ in self._pending_pings.values():
            if not pong_event.is_set():
                pong_event.set()
        self._pending_pings.clear()

    async def _keepalive(self) -> None:
        assert self._ping_interval is not None
        ticker = tonio_time.interval(self._ping_interval)
        try:
            while not self._closed:
                await ticker.tick()
                if self._closed:
                    break
                try:
                    pong_event = await self.ping()
                except ConnectionClosed:
                    break

                if self._ping_timeout is not None:
                    await pong_event.wait(self._ping_timeout)
                    if not pong_event.is_set():
                        # Ping timeout — close the connection
                        async with self._send_lock:
                            self._protocol.fail(
                                CloseCode.INTERNAL_ERROR,
                                "keepalive ping timeout",
                            )
                            await self._send_data()
                        break
        except Exception:
            pass

    def _decode_frame(self, frame: Frame, decode: bool | None) -> Data:
        if frame.opcode is Opcode.TEXT:
            if decode is False:
                return frame.data
            return frame.data.decode()
        else:
            if decode is True:
                return frame.data.decode()
            return bytes(frame.data)


async def _connect_impl(
    uri: str,
    *,
    ssl: ssl_module.SSLContext | bool | None = None,
    additional_headers: typing.Any = None,
    user_agent_header: str | None = USER_AGENT,
    ping_interval: float | None = 20,
    ping_timeout: float | None = 20,
    close_timeout: float | None = 10,
    subprotocols: typing.Sequence[Subprotocol] | None = None,
    max_size: int | None = 2**20,
    **kwargs: typing.Any,
) -> TonioClientConnection:
    ws_uri = parse_uri(uri)

    protocol = ClientProtocol(
        ws_uri,
        subprotocols=subprotocols,
        max_size=max_size,
    )

    # Establish TCP connection
    stream = await net.open_tcp_stream(ws_uri.host, ws_uri.port)

    # TLS handshake if secure
    if ws_uri.secure:
        if ssl is None or ssl is True:
            ssl_ctx = ssl_module.create_default_context()
        elif isinstance(ssl, ssl_module.SSLContext):
            ssl_ctx = ssl
        else:
            raise ValueError("ssl must be a bool or SSLContext")
        tls_stream = tls.TLSStream(
            stream,
            ssl_ctx,
            server_hostname=ws_uri.host,
            https_compatible=True,
        )
        await tls_stream.handshake()
        stream = tls_stream

    # Perform WebSocket handshake
    request = protocol.connect()
    if additional_headers is not None:
        request.headers.update(additional_headers)
    if user_agent_header is not None:
        request.headers.setdefault("User-Agent", user_agent_header)
    protocol.send_request(request)

    # Send the handshake request
    for data in protocol.data_to_send():
        if data:
            await stream.send_all(data)

    # Read the handshake response
    while True:
        data = await stream.receive_some(4096)
        if not data:
            raise EOFError("connection closed during handshake")
        protocol.receive_data(data)
        events = protocol.events_received()
        # Flush any protocol output
        for out in protocol.data_to_send():
            if out:
                await stream.send_all(out)
        if events:
            break

    if protocol.handshake_exc is not None:
        stream.close() if not isinstance(stream, tls.TLSStream) else None
        raise protocol.handshake_exc

    conn = TonioClientConnection(
        protocol,
        stream,
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
        close_timeout=close_timeout,
    )
    conn.request = request
    conn.response = events[0] if events else None

    # Start background tasks
    tonio.spawn.without_tracking(conn._reader())
    if ping_interval is not None:
        tonio.spawn.without_tracking(conn._keepalive())

    return conn


class connect:
    def __init__(
        self,
        uri: str,
        *,
        ssl: ssl_module.SSLContext | bool | None = None,
        additional_headers: typing.Any = None,
        user_agent_header: str | None = USER_AGENT,
        ping_interval: float | None = 20,
        ping_timeout: float | None = 20,
        close_timeout: float | None = 10,
        subprotocols: typing.Sequence[Subprotocol] | None = None,
        max_size: int | None = 2**20,
        **kwargs: typing.Any,
    ) -> None:
        self._uri = uri
        self._kwargs = {
            "ssl": ssl,
            "additional_headers": additional_headers,
            "user_agent_header": user_agent_header,
            "ping_interval": ping_interval,
            "ping_timeout": ping_timeout,
            "close_timeout": close_timeout,
            "subprotocols": subprotocols,
            "max_size": max_size,
            **kwargs,
        }
        self._connection: TonioClientConnection | None = None

    def __await__(self) -> Generator[typing.Any, None, TonioClientConnection]:
        return self._connect().__await__()

    async def _connect(self) -> TonioClientConnection:
        self._connection = await _connect_impl(self._uri, **self._kwargs)
        return self._connection

    async def __aenter__(self) -> TonioClientConnection:
        self._connection = await _connect_impl(self._uri, **self._kwargs)
        return self._connection

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: typing.Any,
    ) -> None:
        if self._connection is not None:
            if exc_type is None:
                await self._connection.close()
            else:
                await self._connection.close(CloseCode.INTERNAL_ERROR)


websockets.asyncio.client.connect = connect
if hasattr(websockets, "connect"):
    websockets.connect = connect
