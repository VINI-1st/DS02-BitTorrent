"""Concrete TCP transport for BitTorrent peer-to-peer communication.

``TcpTransport`` wraps *asyncio* TCP streams with:

* Full BitTorrent handshake on connect / accept
* Continuous message-read loop per connection
* Periodic keep-alive sending and idle-timeout disconnection
* Configurable maximum connection limit
* Graceful shutdown of all tasks and connections
"""

from __future__ import annotations

import asyncio
import json
import logging
import struct
import time
from typing import Any

from .transport import (
    ConnectionEvent,
    PeerConnection,
    Transport,
    TransportListener,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# BitTorrent handshake constants
# ---------------------------------------------------------------------------
_PROTOCOL_STRING: bytes = b"BitTorrent protocol"
_PROTOCOL_STRING_LEN: int = len(_PROTOCOL_STRING)
_RESERVED_BYTES: bytes = b"\x00" * 8
_HANDSHAKE_LEN: int = 1 + _PROTOCOL_STRING_LEN + 8 + 20 + 20  # 68 bytes


def _build_handshake(info_hash: bytes, peer_id: bytes) -> bytes:
    """Build a 68-byte BitTorrent handshake payload.

    Format::

        <pstrlen=19><pstr="BitTorrent protocol"><reserved[8]><info_hash[20]><peer_id[20]>

    Args:
        info_hash: 20-byte torrent info hash.
        peer_id:   20-byte local peer identifier.

    Returns:
        68-byte handshake bytes.
    """
    return (
        struct.pack("B", _PROTOCOL_STRING_LEN)
        + _PROTOCOL_STRING
        + _RESERVED_BYTES
        + info_hash
        + peer_id
    )


async def _read_handshake(reader: asyncio.StreamReader) -> tuple[bytes, bytes]:
    """Read and validate a 68-byte BitTorrent handshake from *reader*.

    Returns:
        A ``(info_hash, peer_id)`` tuple extracted from the handshake.

    Raises:
        ConnectionError: On EOF, short read, or invalid protocol string.
    """
    try:
        pstrlen_bytes = await reader.readexactly(1)
        pstrlen = pstrlen_bytes[0]
        if pstrlen != _PROTOCOL_STRING_LEN:
            raise ConnectionError(
                f"Invalid handshake: pstrlen={pstrlen}, expected {_PROTOCOL_STRING_LEN}"
            )

        pstr = await reader.readexactly(pstrlen)
        if pstr != _PROTOCOL_STRING:
            raise ConnectionError(
                f"Invalid handshake: pstr={pstr!r}, expected {_PROTOCOL_STRING!r}"
            )

        _reserved = await reader.readexactly(8)  # noqa: F841
        info_hash = await reader.readexactly(20)
        peer_id = await reader.readexactly(20)
        return info_hash, peer_id
    except (asyncio.IncompleteReadError, ConnectionResetError) as exc:
        raise ConnectionError(f"Handshake failed: {exc}") from exc


# ---------------------------------------------------------------------------
# Helpers for wire-protocol framing
# ---------------------------------------------------------------------------

async def _read_message(reader: asyncio.StreamReader) -> Any:
    """Read one length-prefixed wire-protocol message.

    Wire format::

        <4-byte big-endian length><payload>

    A length of ``0`` signals a keep-alive.

    Returns:
        A decoded message object (from ``src.protocol.messages``), or a
        ``KeepAlive`` sentinel for zero-length messages.

    Raises:
        ConnectionError: On EOF or decoding failure.
    """
    # Lazy import so the module can load even if protocol isn't ready yet.
    from src.protocol.codec import decode_message, ProtocolError  # noqa: WPS433
    from src.protocol.messages import KeepAlive  # noqa: WPS433

    length_bytes = await reader.readexactly(4)
    (length,) = struct.unpack("!I", length_bytes)

    if length == 0:
        return KeepAlive()

    if length > 2**17:  # 128 KiB sanity cap (max piece = 16 KiB + overhead)
        raise ConnectionError(f"Message too large: {length} bytes")

    payload = await reader.readexactly(length)
    try:
        return decode_message(payload)
    except ProtocolError as exc:
        raise ConnectionError(f"Failed to decode message: {exc}") from exc


def _encode_message(message: Any) -> bytes:
    """Encode a wire-protocol message with a 4-byte big-endian length prefix.

    Args:
        message: A message dataclass from ``src.protocol.messages``.

    Returns:
        The length-prefixed bytes ready to write to the socket.
    """
    from src.protocol.codec import encode_message  # noqa: WPS433
    from src.protocol.messages import KeepAlive  # noqa: WPS433

    if isinstance(message, KeepAlive):
        return struct.pack("!I", 0)

    payload = encode_message(message)
    return struct.pack("!I", len(payload)) + payload


# ---------------------------------------------------------------------------
# JSON logging helper
# ---------------------------------------------------------------------------

def _log_event(peer_id: bytes, event: str, **kwargs: Any) -> None:
    """Emit a JSON-lines log entry."""
    entry: dict[str, Any] = {
        "ts": time.time(),
        "peer_id": peer_id.hex() if peer_id else "",
        "event": event,
    }
    entry.update(kwargs)
    logger.info(json.dumps(entry, default=str))


# ---------------------------------------------------------------------------
# TcpTransport
# ---------------------------------------------------------------------------

class TcpTransport(Transport):
    """Real asyncio TCP transport for BitTorrent peer communication.

    Args:
        max_connections:    Maximum number of simultaneous peer connections.
        connection_timeout: Seconds to wait for a TCP connect + handshake.
        keepalive_interval: Seconds between keep-alive probes.  Connections
                            that receive no messages for twice this interval
                            are considered dead and are disconnected.
    """

    def __init__(
        self,
        max_connections: int = 50,
        connection_timeout: float = 10.0,
        keepalive_interval: float = 120.0,
    ) -> None:
        self._max_connections: int = max_connections
        self._connection_timeout: float = connection_timeout
        self._keepalive_interval: float = keepalive_interval

        # Set during start()
        self._info_hash: bytes = b""
        self._peer_id: bytes = b""
        self._listener: TransportListener | None = None
        self._server: asyncio.Server | None = None
        self._listen_port: int = 0

        # Connection bookkeeping – keyed by ``(remote_ip, remote_port)``
        self._connections: dict[tuple[str, int], PeerConnection] = {}
        self._last_sent_keepalive: dict[tuple[str, int], float] = {}
        self._read_tasks: dict[tuple[str, int], asyncio.Task[None]] = {}

        self._keepalive_task: asyncio.Task[None] | None = None
        self._lock: asyncio.Lock = asyncio.Lock()
        self._running: bool = False

    # ------------------------------------------------------------------
    # Transport API
    # ------------------------------------------------------------------

    async def start(
        self,
        host: str,
        port: int,
        info_hash: bytes,
        peer_id: bytes,
        listener: TransportListener,
    ) -> None:
        """Start listening for incoming connections.

        Creates an asyncio TCP server on *host*:*port* and launches the
        background keep-alive task.
        """
        self._info_hash = info_hash
        self._peer_id = peer_id
        self._listener = listener
        self._running = True

        self._server = await asyncio.start_server(
            self._handle_inbound, host, port
        )

        # Resolve the actual listening port (useful when port=0).
        sockets = self._server.sockets
        if sockets:
            self._listen_port = sockets[0].getsockname()[1]
        else:
            self._listen_port = port

        self._keepalive_task = asyncio.create_task(
            self._keepalive_loop(), name="keepalive"
        )

        _log_event(
            self._peer_id,
            "transport_started",
            host=host,
            port=self._listen_port,
        )

    @property
    def listen_port(self) -> int:
        """The actual TCP port the server is bound to."""
        return self._listen_port

    async def stop(self) -> None:
        """Stop listening and close all connections."""
        self._running = False

        # Cancel the keep-alive background task.
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            self._keepalive_task = None

        # Close the TCP server.
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        # Disconnect every peer (copy the list to avoid mutation issues).
        async with self._lock:
            conns = list(self._connections.values())

        for conn in conns:
            await self._close_connection(conn, reason="transport_stopped")

        _log_event(self._peer_id, "transport_stopped")

    async def connect(self, remote_ip: str, remote_port: int) -> PeerConnection:
        """Initiate an outbound TCP connection and perform the handshake."""
        async with self._lock:
            if len(self._connections) >= self._max_connections:
                raise RuntimeError(
                    f"Max connections ({self._max_connections}) reached"
                )

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(remote_ip, remote_port),
                timeout=self._connection_timeout,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            raise ConnectionError(
                f"Failed to connect to {remote_ip}:{remote_port}: {exc}"
            ) from exc

        try:
            # Send our handshake first.
            writer.write(_build_handshake(self._info_hash, self._peer_id))
            await writer.drain()

            # Read and validate the remote handshake.
            remote_info_hash, remote_peer_id = await asyncio.wait_for(
                _read_handshake(reader),
                timeout=self._connection_timeout,
            )

            if remote_info_hash != self._info_hash:
                raise ConnectionError(
                    f"info_hash mismatch: expected {self._info_hash.hex()}, "
                    f"got {remote_info_hash.hex()}"
                )
        except ConnectionError:
            writer.close()
            await writer.wait_closed()
            raise
        except (asyncio.IncompleteReadError, OSError, asyncio.TimeoutError) as exc:
            writer.close()
            await writer.wait_closed()
            raise ConnectionError(f"Connection error to {remote_ip}:{remote_port}: {exc}") from exc
        except Exception:
            writer.close()
            await writer.wait_closed()
            raise

        now = asyncio.get_event_loop().time()
        conn = PeerConnection(
            peer_id=remote_peer_id,
            remote_ip=remote_ip,
            remote_port=remote_port,
            info_hash=remote_info_hash,
            reader=reader,
            writer=writer,
            connected_at=now,
            last_message_at=now,
        )

        await self._register_connection(conn)
        return conn

    async def send(self, conn: PeerConnection, message: Any) -> None:
        """Send a wire-protocol message to *conn*."""
        if conn.writer is None or conn.writer.is_closing():
            raise ConnectionError("Connection is closed")

        try:
            data = _encode_message(message)
            conn.writer.write(data)
            await conn.writer.drain()
        except (BrokenPipeError, ConnectionResetError, OSError) as exc:
            await self._close_connection(conn, reason=f"send_error: {exc}")
            raise ConnectionError(
                f"Failed to send to {conn.remote_ip}:{conn.remote_port}: {exc}"
            ) from exc

    async def disconnect(self, conn: PeerConnection) -> None:
        """Gracefully close *conn*."""
        await self._close_connection(conn, reason="local_disconnect")

    def get_connections(self) -> list[PeerConnection]:
        """Return a snapshot of all active connections."""
        return list(self._connections.values())

    # ------------------------------------------------------------------
    # Inbound connection handler
    # ------------------------------------------------------------------

    async def _handle_inbound(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle a new inbound TCP connection from the server callback."""
        peername = writer.get_extra_info("peername")
        remote_ip: str = peername[0] if peername else "unknown"
        remote_port: int = peername[1] if peername else 0

        # Enforce connection limit.
        async with self._lock:
            if len(self._connections) >= self._max_connections:
                _log_event(
                    self._peer_id,
                    "connection_rejected",
                    remote_ip=remote_ip,
                    remote_port=remote_port,
                    reason="max_connections",
                )
                writer.close()
                await writer.wait_closed()
                return

        try:
            # Read the remote handshake first.
            remote_info_hash, remote_peer_id = await asyncio.wait_for(
                _read_handshake(reader),
                timeout=self._connection_timeout,
            )

            if remote_info_hash != self._info_hash:
                _log_event(
                    self._peer_id,
                    "handshake_rejected",
                    remote_ip=remote_ip,
                    remote_port=remote_port,
                    reason="info_hash_mismatch",
                )
                writer.close()
                await writer.wait_closed()
                return

            # Send our handshake back.
            writer.write(_build_handshake(self._info_hash, self._peer_id))
            await writer.drain()
        except Exception as exc:
            _log_event(
                self._peer_id,
                "handshake_error",
                remote_ip=remote_ip,
                remote_port=remote_port,
                error=str(exc),
            )
            writer.close()
            await writer.wait_closed()
            return

        now = asyncio.get_event_loop().time()
        conn = PeerConnection(
            peer_id=remote_peer_id,
            remote_ip=remote_ip,
            remote_port=remote_port,
            info_hash=remote_info_hash,
            reader=reader,
            writer=writer,
            connected_at=now,
            last_message_at=now,
        )

        await self._register_connection(conn)

    # ------------------------------------------------------------------
    # Connection bookkeeping
    # ------------------------------------------------------------------

    async def _register_connection(self, conn: PeerConnection) -> None:
        """Add *conn* to the active set, start its read loop, and notify."""
        key = (conn.remote_ip, conn.remote_port)

        async with self._lock:
            self._connections[key] = conn
            task = asyncio.create_task(
                self._read_loop(conn), name=f"read-{conn.remote_ip}:{conn.remote_port}"
            )
            self._read_tasks[key] = task

        _log_event(
            self._peer_id,
            "peer_connected",
            remote_ip=conn.remote_ip,
            remote_port=conn.remote_port,
            remote_peer_id=conn.peer_id.hex(),
        )

        if self._listener is not None:
            try:
                await self._listener.on_connected(conn)
            except Exception as exc:
                logger.exception("Listener.on_connected raised: %s", exc)

    async def _close_connection(self, conn: PeerConnection, *, reason: str) -> None:
        """Remove *conn* from the active set, close the socket, and notify."""
        key = (conn.remote_ip, conn.remote_port)

        async with self._lock:
            if key not in self._connections:
                return  # Already cleaned up.
            del self._connections[key]
            self._last_sent_keepalive.pop(key, None)

            task = self._read_tasks.pop(key, None)

        # Cancel the read-loop task.
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Close the underlying socket.
        if conn.writer is not None and not conn.writer.is_closing():
            try:
                conn.writer.close()
                await conn.writer.wait_closed()
            except Exception:
                pass

        conn.reader = None
        conn.writer = None

        _log_event(
            self._peer_id,
            "peer_disconnected",
            remote_ip=conn.remote_ip,
            remote_port=conn.remote_port,
            reason=reason,
        )

        if self._listener is not None:
            try:
                await self._listener.on_disconnected(conn, reason)
            except Exception as exc:
                logger.exception("Listener.on_disconnected raised: %s", exc)

    # ------------------------------------------------------------------
    # Per-connection message read loop
    # ------------------------------------------------------------------

    async def _read_loop(self, conn: PeerConnection) -> None:
        """Continuously read messages from *conn* until EOF or error."""
        assert conn.reader is not None
        try:
            while self._running:
                msg = await _read_message(conn.reader)
                conn.last_message_at = asyncio.get_event_loop().time()

                if self._listener is not None:
                    try:
                        await self._listener.on_message(conn, msg)
                    except Exception as exc:
                        logger.exception("Listener.on_message raised: %s", exc)

        except asyncio.CancelledError:
            return
        except asyncio.IncompleteReadError:
            await self._close_connection(conn, reason="eof")
        except ConnectionError as exc:
            await self._close_connection(conn, reason=str(exc))
        except Exception as exc:
            if self._listener is not None:
                try:
                    await self._listener.on_error(conn, exc)
                except Exception:
                    logger.exception("Listener.on_error raised")
            await self._close_connection(conn, reason=f"read_error: {exc}")

    # ------------------------------------------------------------------
    # Keep-alive background task
    # ------------------------------------------------------------------

    async def _keepalive_loop(self) -> None:
        """Periodically send keep-alives and reap idle connections."""
        from src.protocol.messages import KeepAlive  # noqa: WPS433

        while self._running:
            check_interval = max(0.05, min(1.0, self._keepalive_interval / 4))
            await asyncio.sleep(check_interval)

            now = asyncio.get_event_loop().time()
            timeout_threshold = now - (self._keepalive_interval * 2)
            send_threshold = now - self._keepalive_interval

            # Snapshot to avoid mutating dict during iteration.
            async with self._lock:
                conns = list(self._connections.values())

            for conn in conns:
                # Disconnect peers that haven't sent anything recently.
                if conn.last_message_at < timeout_threshold:
                    _log_event(
                        self._peer_id,
                        "peer_timeout",
                        remote_ip=conn.remote_ip,
                        remote_port=conn.remote_port,
                    )
                    await self._close_connection(conn, reason="idle_timeout")
                    continue

                # Send keep-alive to peers we haven't heard from in a while.
                key = (conn.remote_ip, conn.remote_port)
                last_sent = self._last_sent_keepalive.get(key, 0.0)
                if conn.last_message_at <= send_threshold and (now - last_sent) >= self._keepalive_interval:
                    self._last_sent_keepalive[key] = now
                    try:
                        await self.send(conn, KeepAlive())
                    except ConnectionError:
                        pass  # _close_connection already handled inside send()
