"""Tests for the Network / Transport module (task A5).

Uses ``unittest.IsolatedAsyncioTestCase`` so that every test method runs in
its own fresh event loop.  The protocol module (``src.protocol``) is being
built in parallel, so we provide a minimal stub that is installed only when
the real module is not yet importable.
"""

from __future__ import annotations

import asyncio
import os
import struct
import sys
import time
import unittest
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Protocol stub – installed only when the real module is unavailable
# ---------------------------------------------------------------------------

def _ensure_protocol_module() -> None:
    """Create a minimal ``src.protocol`` shim if the real one isn't ready yet.

    The stub defines just enough message classes and codec functions for the
    transport layer's tests to pass.
    """
    try:
        from src.protocol.messages import KeepAlive  # noqa: F401
        from src.protocol.codec import encode_message, decode_message  # noqa: F401
        return  # Real module available — nothing to do.
    except (ImportError, ModuleNotFoundError):
        pass

    # -- Stub message classes -----------------------------------------------
    import types

    messages_mod = types.ModuleType("src.protocol.messages")

    @dataclass
    class _KeepAlive:
        """Stub KeepAlive."""

    @dataclass
    class _Choke:
        """Stub Choke."""

    @dataclass
    class _Unchoke:
        """Stub Unchoke."""

    @dataclass
    class _Interested:
        """Stub Interested."""

    @dataclass
    class _NotInterested:
        """Stub NotInterested."""

    @dataclass
    class _Have:
        """Stub Have."""
        index: int = 0

    @dataclass
    class _Bitfield:
        """Stub Bitfield."""
        bitfield: bytes = b""

    @dataclass
    class _Request:
        """Stub Request."""
        index: int = 0
        begin: int = 0
        length: int = 0

    @dataclass
    class _Piece:
        """Stub Piece."""
        index: int = 0
        begin: int = 0
        block: bytes = b""

    @dataclass
    class _Cancel:
        """Stub Cancel."""
        index: int = 0
        begin: int = 0
        length: int = 0

    @dataclass
    class _Handshake:
        """Stub Handshake."""
        info_hash: bytes = b""
        peer_id: bytes = b""

    messages_mod.KeepAlive = _KeepAlive  # type: ignore[attr-defined]
    messages_mod.Choke = _Choke  # type: ignore[attr-defined]
    messages_mod.Unchoke = _Unchoke  # type: ignore[attr-defined]
    messages_mod.Interested = _Interested  # type: ignore[attr-defined]
    messages_mod.NotInterested = _NotInterested  # type: ignore[attr-defined]
    messages_mod.Have = _Have  # type: ignore[attr-defined]
    messages_mod.Bitfield = _Bitfield  # type: ignore[attr-defined]
    messages_mod.Request = _Request  # type: ignore[attr-defined]
    messages_mod.Piece = _Piece  # type: ignore[attr-defined]
    messages_mod.Cancel = _Cancel  # type: ignore[attr-defined]
    messages_mod.Handshake = _Handshake  # type: ignore[attr-defined]

    # Message ID mapping (matching the real protocol's IDs)
    _MSG_ID: dict[type, int] = {
        _Choke: 0,
        _Unchoke: 1,
        _Interested: 2,
        _NotInterested: 3,
        _Have: 4,
        _Bitfield: 5,
        _Request: 6,
        _Piece: 7,
        _Cancel: 8,
    }

    _ID_TO_CLS: dict[int, type] = {v: k for k, v in _MSG_ID.items()}

    # -- Stub codec ---------------------------------------------------------
    codec_mod = types.ModuleType("src.protocol.codec")

    class _ProtocolError(Exception):
        """Stub ProtocolError."""

    def _encode_message(msg: Any) -> bytes:
        """Minimal encoder matching the wire spec."""
        cls = type(msg)
        msg_id = _MSG_ID.get(cls)
        if msg_id is None:
            raise _ProtocolError(f"Unknown message type: {cls}")

        payload = struct.pack("B", msg_id)

        if isinstance(msg, _Choke) or isinstance(msg, _Unchoke) or \
           isinstance(msg, _Interested) or isinstance(msg, _NotInterested):
            pass  # ID-only messages
        elif isinstance(msg, _Have):
            payload += struct.pack("!I", msg.index)
        elif isinstance(msg, _Bitfield):
            payload += msg.bitfield
        elif isinstance(msg, _Request) or isinstance(msg, _Cancel):
            payload += struct.pack("!III", msg.index, msg.begin, msg.length)
        elif isinstance(msg, _Piece):
            payload += struct.pack("!II", msg.index, msg.begin) + msg.block
        return payload

    def _decode_message(data: bytes) -> Any:
        """Minimal decoder matching the wire spec."""
        if not data:
            raise _ProtocolError("Empty payload")
        msg_id = data[0]
        body = data[1:]
        cls = _ID_TO_CLS.get(msg_id)
        if cls is None:
            raise _ProtocolError(f"Unknown message id: {msg_id}")

        if cls in (_Choke, _Unchoke, _Interested, _NotInterested):
            return cls()
        elif cls is _Have:
            (index,) = struct.unpack("!I", body[:4])
            return _Have(index=index)
        elif cls is _Bitfield:
            return _Bitfield(bitfield=body)
        elif cls in (_Request, _Cancel):
            index, begin, length = struct.unpack("!III", body[:12])
            return cls(index=index, begin=begin, length=length)
        elif cls is _Piece:
            index, begin = struct.unpack("!II", body[:8])
            return _Piece(index=index, begin=begin, block=body[8:])
        raise _ProtocolError(f"Unexpected message id: {msg_id}")

    codec_mod.encode_message = _encode_message  # type: ignore[attr-defined]
    codec_mod.decode_message = _decode_message  # type: ignore[attr-defined]
    codec_mod.ProtocolError = _ProtocolError  # type: ignore[attr-defined]

    # -- Install into sys.modules -------------------------------------------
    sys.modules.setdefault("src.protocol", types.ModuleType("src.protocol"))
    sys.modules["src.protocol.messages"] = messages_mod
    sys.modules["src.protocol.codec"] = codec_mod


# Ensure the stub is available before importing the transport.
_ensure_protocol_module()

from src.network.transport import PeerConnection, TransportListener  # noqa: E402
from src.network.tcp_transport import TcpTransport  # noqa: E402


# ---------------------------------------------------------------------------
# Mock listener that records all events
# ---------------------------------------------------------------------------

class MockListener(TransportListener):
    """Records all transport events for later assertions."""

    def __init__(self) -> None:
        self.connected: list[PeerConnection] = []
        self.disconnected: list[tuple[PeerConnection, str]] = []
        self.messages: list[tuple[PeerConnection, Any]] = []
        self.errors: list[tuple[PeerConnection, Exception]] = []

        # Asyncio events so tests can ``await`` a specific callback.
        self.connected_event: asyncio.Event = asyncio.Event()
        self.disconnected_event: asyncio.Event = asyncio.Event()
        self.message_event: asyncio.Event = asyncio.Event()
        self.error_event: asyncio.Event = asyncio.Event()

    async def on_connected(self, conn: PeerConnection) -> None:
        self.connected.append(conn)
        self.connected_event.set()

    async def on_disconnected(self, conn: PeerConnection, reason: str) -> None:
        self.disconnected.append((conn, reason))
        self.disconnected_event.set()

    async def on_message(self, conn: PeerConnection, message: Any) -> None:
        self.messages.append((conn, message))
        self.message_event.set()

    async def on_error(self, conn: PeerConnection, error: Exception) -> None:
        self.errors.append((conn, error))
        self.error_event.set()

    def reset_events(self) -> None:
        """Clear all asyncio events so they can be awaited again."""
        self.connected_event.clear()
        self.disconnected_event.clear()
        self.message_event.clear()
        self.error_event.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

INFO_HASH = b"\xaa" * 20
PEER_ID_A = b"\x01" * 20
PEER_ID_B = b"\x02" * 20


async def _wait(event: asyncio.Event, timeout: float = 5.0) -> None:
    """Wait for *event* with a generous timeout."""
    await asyncio.wait_for(event.wait(), timeout=timeout)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestTcpTransportStartStop(unittest.IsolatedAsyncioTestCase):
    """Test that a transport can start and stop cleanly."""

    async def test_start_and_stop(self) -> None:
        """Transport should bind a port and shut down without error."""
        listener = MockListener()
        transport = TcpTransport()

        await transport.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, listener)
        self.assertGreater(transport.listen_port, 0)
        self.assertEqual(transport.get_connections(), [])

        await transport.stop()
        self.assertEqual(transport.get_connections(), [])

    async def test_double_stop(self) -> None:
        """Calling stop() twice should be harmless."""
        listener = MockListener()
        transport = TcpTransport()
        await transport.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, listener)
        await transport.stop()
        await transport.stop()  # Should not raise.


class TestTcpTransportHandshake(unittest.IsolatedAsyncioTestCase):
    """Test two TcpTransports connecting to each other (full handshake)."""

    async def asyncSetUp(self) -> None:
        self.listener_a = MockListener()
        self.listener_b = MockListener()
        self.transport_a = TcpTransport()
        self.transport_b = TcpTransport()

        await self.transport_a.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, self.listener_a)
        await self.transport_b.start("127.0.0.1", 0, INFO_HASH, PEER_ID_B, self.listener_b)

    async def asyncTearDown(self) -> None:
        await self.transport_a.stop()
        await self.transport_b.stop()

    async def test_connect_and_handshake(self) -> None:
        """Transport A connects to transport B; both sides see on_connected."""
        conn = await self.transport_a.connect("127.0.0.1", self.transport_b.listen_port)

        # A's outbound connect triggers listener_a.on_connected immediately.
        await _wait(self.listener_a.connected_event)
        self.assertEqual(len(self.listener_a.connected), 1)
        self.assertEqual(conn.peer_id, PEER_ID_B)
        self.assertEqual(conn.info_hash, INFO_HASH)

        # B's inbound handler also fires on_connected.
        await _wait(self.listener_b.connected_event)
        self.assertEqual(len(self.listener_b.connected), 1)
        self.assertEqual(self.listener_b.connected[0].peer_id, PEER_ID_A)

        # Both transports report one connection.
        self.assertEqual(len(self.transport_a.get_connections()), 1)
        self.assertEqual(len(self.transport_b.get_connections()), 1)

    async def test_info_hash_mismatch_rejected(self) -> None:
        """Connection with a different info_hash should be rejected."""
        other_hash = b"\xbb" * 20
        transport_c = TcpTransport()
        listener_c = MockListener()
        await transport_c.start("127.0.0.1", 0, other_hash, b"\x03" * 20, listener_c)

        try:
            with self.assertRaises(ConnectionError):
                await transport_c.connect("127.0.0.1", self.transport_b.listen_port)
        finally:
            await transport_c.stop()


class TestTcpTransportMessaging(unittest.IsolatedAsyncioTestCase):
    """Test sending and receiving each message type through the transport."""

    async def asyncSetUp(self) -> None:
        self.listener_a = MockListener()
        self.listener_b = MockListener()
        self.transport_a = TcpTransport()
        self.transport_b = TcpTransport()

        await self.transport_a.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, self.listener_a)
        await self.transport_b.start("127.0.0.1", 0, INFO_HASH, PEER_ID_B, self.listener_b)

        self.conn_a = await self.transport_a.connect("127.0.0.1", self.transport_b.listen_port)
        await _wait(self.listener_b.connected_event)
        self.conn_b = self.listener_b.connected[0]

    async def asyncTearDown(self) -> None:
        await self.transport_a.stop()
        await self.transport_b.stop()

    async def test_send_keepalive(self) -> None:
        """KeepAlive messages (length=0) should be received."""
        from src.protocol.messages import KeepAlive

        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, KeepAlive())
        await _wait(self.listener_b.message_event)

        _, msg = self.listener_b.messages[-1]
        self.assertIsInstance(msg, KeepAlive)

    async def test_send_choke(self) -> None:
        """Choke message should round-trip correctly."""
        from src.protocol.messages import Choke

        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, Choke())
        await _wait(self.listener_b.message_event)

        _, msg = self.listener_b.messages[-1]
        self.assertIsInstance(msg, Choke)

    async def test_send_unchoke(self) -> None:
        """Unchoke message should round-trip correctly."""
        from src.protocol.messages import Unchoke

        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, Unchoke())
        await _wait(self.listener_b.message_event)

        _, msg = self.listener_b.messages[-1]
        self.assertIsInstance(msg, Unchoke)

    async def test_send_interested(self) -> None:
        """Interested message should round-trip correctly."""
        from src.protocol.messages import Interested

        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, Interested())
        await _wait(self.listener_b.message_event)

        _, msg = self.listener_b.messages[-1]
        self.assertIsInstance(msg, Interested)

    async def test_send_not_interested(self) -> None:
        """NotInterested message should round-trip correctly."""
        from src.protocol.messages import NotInterested

        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, NotInterested())
        await _wait(self.listener_b.message_event)

        _, msg = self.listener_b.messages[-1]
        self.assertIsInstance(msg, NotInterested)

    async def test_send_have(self) -> None:
        """Have message should carry the piece index."""
        from src.protocol.messages import Have

        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, Have(index=42))
        await _wait(self.listener_b.message_event)

        _, msg = self.listener_b.messages[-1]
        self.assertIsInstance(msg, Have)
        self.assertEqual(msg.index, 42)

    async def test_send_bitfield(self) -> None:
        """Bitfield message should carry the bitfield bytes."""
        from src.protocol.messages import Bitfield

        bf = b"\xff\x00\xaa"
        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, Bitfield(bitfield=bf))
        await _wait(self.listener_b.message_event)

        _, msg = self.listener_b.messages[-1]
        self.assertIsInstance(msg, Bitfield)
        self.assertEqual(msg.bitfield, bf)

    async def test_send_request(self) -> None:
        """Request message should carry index/begin/length."""
        from src.protocol.messages import Request

        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, Request(index=1, begin=0, length=16384))
        await _wait(self.listener_b.message_event)

        _, msg = self.listener_b.messages[-1]
        self.assertIsInstance(msg, Request)
        self.assertEqual(msg.index, 1)
        self.assertEqual(msg.begin, 0)
        self.assertEqual(msg.length, 16384)

    async def test_send_piece(self) -> None:
        """Piece message should carry the data block."""
        from src.protocol.messages import Piece

        block = b"\xde\xad\xbe\xef" * 100
        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, Piece(index=5, begin=0, block=block))
        await _wait(self.listener_b.message_event)

        _, msg = self.listener_b.messages[-1]
        self.assertIsInstance(msg, Piece)
        self.assertEqual(msg.index, 5)
        self.assertEqual(msg.begin, 0)
        self.assertEqual(msg.block, block)

    async def test_send_cancel(self) -> None:
        """Cancel message should carry index/begin/length."""
        from src.protocol.messages import Cancel

        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, Cancel(index=3, begin=8192, length=16384))
        await _wait(self.listener_b.message_event)

        _, msg = self.listener_b.messages[-1]
        self.assertIsInstance(msg, Cancel)
        self.assertEqual(msg.index, 3)
        self.assertEqual(msg.begin, 8192)
        self.assertEqual(msg.length, 16384)

    async def test_bidirectional_messaging(self) -> None:
        """Messages should flow in both directions."""
        from src.protocol.messages import Choke, Unchoke

        self.listener_b.reset_events()
        await self.transport_a.send(self.conn_a, Choke())
        await _wait(self.listener_b.message_event)

        self.listener_a.reset_events()
        await self.transport_b.send(self.conn_b, Unchoke())
        await _wait(self.listener_a.message_event)

        _, msg_b = self.listener_b.messages[-1]
        _, msg_a = self.listener_a.messages[-1]
        self.assertIsInstance(msg_b, Choke)
        self.assertIsInstance(msg_a, Unchoke)


class TestTcpTransportDisconnect(unittest.IsolatedAsyncioTestCase):
    """Test disconnect detection."""

    async def asyncSetUp(self) -> None:
        self.listener_a = MockListener()
        self.listener_b = MockListener()
        self.transport_a = TcpTransport()
        self.transport_b = TcpTransport()

        await self.transport_a.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, self.listener_a)
        await self.transport_b.start("127.0.0.1", 0, INFO_HASH, PEER_ID_B, self.listener_b)

        self.conn_a = await self.transport_a.connect("127.0.0.1", self.transport_b.listen_port)
        await _wait(self.listener_b.connected_event)
        self.conn_b = self.listener_b.connected[0]

    async def asyncTearDown(self) -> None:
        await self.transport_a.stop()
        await self.transport_b.stop()

    async def test_graceful_disconnect(self) -> None:
        """Disconnecting one side triggers on_disconnected on the other."""
        self.listener_b.reset_events()
        await self.transport_a.disconnect(self.conn_a)

        await _wait(self.listener_b.disconnected_event)
        self.assertGreaterEqual(len(self.listener_b.disconnected), 1)
        self.assertEqual(len(self.transport_a.get_connections()), 0)

    async def test_remote_close_detected(self) -> None:
        """When the remote side closes, the local side detects EOF."""
        self.listener_a.reset_events()
        # Close B's writer directly to simulate a crash.
        assert self.conn_b.writer is not None
        self.conn_b.writer.close()
        await self.conn_b.writer.wait_closed()

        await _wait(self.listener_a.disconnected_event)
        self.assertGreaterEqual(len(self.listener_a.disconnected), 1)

    async def test_send_to_disconnected_peer(self) -> None:
        """Sending to a disconnected peer should raise ConnectionError."""
        from src.protocol.messages import Choke

        await self.transport_a.disconnect(self.conn_a)
        await asyncio.sleep(0.1)

        with self.assertRaises(ConnectionError):
            await self.transport_a.send(self.conn_a, Choke())


class TestTcpTransportMaxConnections(unittest.IsolatedAsyncioTestCase):
    """Test the max-connections limit."""

    async def test_outbound_limit(self) -> None:
        """Outbound connect should raise RuntimeError when at limit."""
        listener_a = MockListener()
        listener_b = MockListener()
        transport_a = TcpTransport(max_connections=1)
        transport_b = TcpTransport()

        await transport_a.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, listener_a)
        await transport_b.start("127.0.0.1", 0, INFO_HASH, PEER_ID_B, listener_b)

        try:
            # First connection should succeed.
            await transport_a.connect("127.0.0.1", transport_b.listen_port)
            self.assertEqual(len(transport_a.get_connections()), 1)

            # Second connection should be rejected.
            with self.assertRaises(RuntimeError):
                await transport_a.connect("127.0.0.1", transport_b.listen_port)
        finally:
            await transport_a.stop()
            await transport_b.stop()

    async def test_inbound_limit(self) -> None:
        """Server should reject inbound connections when at limit."""
        listener_a = MockListener()
        listener_b = MockListener()
        listener_c = MockListener()
        transport_a = TcpTransport(max_connections=1)
        transport_b = TcpTransport()
        transport_c = TcpTransport()

        await transport_a.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, listener_a)
        await transport_b.start("127.0.0.1", 0, INFO_HASH, PEER_ID_B, listener_b)
        await transport_c.start("127.0.0.1", 0, INFO_HASH, b"\x03" * 20, listener_c)

        try:
            # B connects to A — should succeed (A now at max).
            await transport_b.connect("127.0.0.1", transport_a.listen_port)
            await _wait(listener_a.connected_event)
            self.assertEqual(len(transport_a.get_connections()), 1)

            # C tries to connect to A — A should reject silently.
            # The connection attempt from C's side will fail because A rejects
            # after handshake read. C will get a ConnectionError because A
            # closes the socket before sending a handshake back.
            with self.assertRaises((ConnectionError, asyncio.IncompleteReadError)):
                await transport_c.connect("127.0.0.1", transport_a.listen_port)
        finally:
            await transport_a.stop()
            await transport_b.stop()
            await transport_c.stop()


class TestTcpTransportTimeout(unittest.IsolatedAsyncioTestCase):
    """Test connection timeout handling."""

    async def test_connect_timeout(self) -> None:
        """Connecting to a non-responsive address should time out."""
        listener = MockListener()
        transport = TcpTransport(connection_timeout=1.0)
        await transport.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, listener)

        try:
            # Try to connect to a port that nobody is listening on.
            # We use a high port that is unlikely to be in use.
            # On Windows, connecting to a closed port usually gets a
            # ConnectionRefusedError rather quickly, which is fine.
            with self.assertRaises(ConnectionError):
                await transport.connect("127.0.0.1", 1)
        finally:
            await transport.stop()


class TestTcpTransportKeepalive(unittest.IsolatedAsyncioTestCase):
    """Test the keep-alive mechanism."""

    async def test_keepalive_sent(self) -> None:
        """Keep-alive should be sent after the configured interval."""
        from src.protocol.messages import KeepAlive

        # Use a very short keepalive interval so the test runs quickly.
        listener_a = MockListener()
        listener_b = MockListener()
        transport_a = TcpTransport(keepalive_interval=0.5)
        transport_b = TcpTransport(keepalive_interval=0.5)

        await transport_a.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, listener_a)
        await transport_b.start("127.0.0.1", 0, INFO_HASH, PEER_ID_B, listener_b)

        try:
            conn_a = await transport_a.connect("127.0.0.1", transport_b.listen_port)
            await _wait(listener_b.connected_event)

            # Wait long enough for the keep-alive loop to fire.
            # keepalive_interval is 0.5s, so wait ~0.8s.
            await asyncio.sleep(0.8)

            # At least one side should have received a KeepAlive.
            # (Both sides run keepalive loops, but the timing is approximate.)
            all_msgs_a = [m for _, m in listener_a.messages if isinstance(m, KeepAlive)]
            all_msgs_b = [m for _, m in listener_b.messages if isinstance(m, KeepAlive)]

            # Depending on timing, at least one side should have a KeepAlive.
            self.assertTrue(
                len(all_msgs_a) > 0 or len(all_msgs_b) > 0,
                "Expected at least one KeepAlive to be received",
            )
        finally:
            await transport_a.stop()
            await transport_b.stop()

    async def test_idle_timeout_disconnects(self) -> None:
        """A connection idle for 2x keepalive_interval should be disconnected."""
        listener_a = MockListener()
        listener_b = MockListener()
        # transport_a will detect timeout; transport_b has a long interval so
        # it won't send keepalives (simulating an idle peer).
        transport_a = TcpTransport(keepalive_interval=0.3)
        transport_b = TcpTransport(keepalive_interval=600.0)

        await transport_a.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, listener_a)
        await transport_b.start("127.0.0.1", 0, INFO_HASH, PEER_ID_B, listener_b)

        try:
            conn_a = await transport_a.connect("127.0.0.1", transport_b.listen_port)
            await _wait(listener_b.connected_event)

            # Wait for >2x keepalive (0.3 * 2 = 0.6). The keepalive loop
            # sleeps for the full interval before checking, so we need to wait
            # at least keepalive_interval + 2*keepalive_interval ≈ 0.9s.
            # Give it a generous margin.
            await asyncio.sleep(1.5)

            # transport_a should have disconnected conn_a due to idle timeout.
            # (transport_a's keepalive loop fires every 0.3s and checks for
            # last_message_at < now - 0.6s).
            # Note: transport_a first sends keepalive at 0.3s which updates
            # nothing on the receive side, then at 0.6s checks idle again.
            # Since B never sends anything back (its keepalive_interval=600),
            # A should timeout B.
            self.assertEqual(
                len(transport_a.get_connections()), 0,
                "Expected transport_a to have disconnected the idle peer",
            )
        finally:
            await transport_a.stop()
            await transport_b.stop()


class TestTcpTransportMultipleConnections(unittest.IsolatedAsyncioTestCase):
    """Test managing multiple simultaneous connections."""

    async def test_multiple_peers(self) -> None:
        """A transport should handle connections from multiple peers."""
        listener_server = MockListener()
        transport_server = TcpTransport(max_connections=10)
        await transport_server.start("127.0.0.1", 0, INFO_HASH, PEER_ID_A, listener_server)

        clients: list[tuple[TcpTransport, MockListener]] = []
        try:
            for i in range(3):
                listener = MockListener()
                t = TcpTransport()
                pid = bytes([i + 10]) * 20
                await t.start("127.0.0.1", 0, INFO_HASH, pid, listener)
                await t.connect("127.0.0.1", transport_server.listen_port)
                clients.append((t, listener))

            # Give inbound handlers time to fire.
            await asyncio.sleep(0.3)

            self.assertEqual(len(transport_server.get_connections()), 3)
        finally:
            for t, _ in clients:
                await t.stop()
            await transport_server.stop()


if __name__ == "__main__":
    unittest.main()
