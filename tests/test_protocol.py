"""Comprehensive tests for the wire protocol module (task A4).

Covers round-trip encoding/decoding for every message type, validation
of malformed inputs, async stream reading, and edge cases.
"""

from __future__ import annotations

import asyncio
import struct
import unittest
from unittest.mock import AsyncMock

from src.protocol.messages import (
    Bitfield,
    Cancel,
    Choke,
    Handshake,
    Have,
    Interested,
    KeepAlive,
    MessageType,
    NotInterested,
    Piece,
    Request,
    Unchoke,
)
from src.protocol.codec import (
    MAX_MESSAGE_LENGTH,
    ProtocolError,
    decode_handshake,
    decode_message,
    encode_handshake,
    encode_message,
    read_handshake,
    read_message,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_info_hash() -> bytes:
    """Return a deterministic 20-byte info_hash."""
    return b"\xab" * 20


def _fake_peer_id() -> bytes:
    """Return a deterministic 20-byte peer_id."""
    return b"-PY0001-" + b"\x01" * 12


async def _make_reader(data: bytes) -> asyncio.StreamReader:
    """Create an asyncio.StreamReader pre-loaded with *data* within an event loop."""
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


# ===================================================================
# Message dataclass validation tests
# ===================================================================


class TestMessageValidation(unittest.TestCase):
    """Validate dataclass __post_init__ guards."""

    def test_handshake_bad_info_hash_length(self) -> None:
        with self.assertRaises(ValueError):
            Handshake(info_hash=b"\x00" * 19, peer_id=b"\x00" * 20)

    def test_handshake_bad_peer_id_length(self) -> None:
        with self.assertRaises(ValueError):
            Handshake(info_hash=b"\x00" * 20, peer_id=b"\x00" * 21)

    def test_have_negative_index(self) -> None:
        with self.assertRaises(ValueError):
            Have(piece_index=-1)

    def test_request_negative_fields(self) -> None:
        with self.assertRaises(ValueError):
            Request(index=-1, begin=0, length=16384)
        with self.assertRaises(ValueError):
            Request(index=0, begin=-1, length=16384)
        with self.assertRaises(ValueError):
            Request(index=0, begin=0, length=-1)

    def test_piece_negative_fields(self) -> None:
        with self.assertRaises(ValueError):
            Piece(index=-1, begin=0, block=b"x")
        with self.assertRaises(ValueError):
            Piece(index=0, begin=-1, block=b"x")

    def test_cancel_negative_fields(self) -> None:
        with self.assertRaises(ValueError):
            Cancel(index=-1, begin=0, length=16384)
        with self.assertRaises(ValueError):
            Cancel(index=0, begin=-1, length=16384)
        with self.assertRaises(ValueError):
            Cancel(index=0, begin=0, length=-1)


# ===================================================================
# Handshake encoding / decoding
# ===================================================================


class TestHandshake(unittest.TestCase):
    """Handshake encode ↔ decode round-trip and error handling."""

    def test_round_trip(self) -> None:
        ih = _fake_info_hash()
        pid = _fake_peer_id()
        msg = Handshake(info_hash=ih, peer_id=pid)
        raw = encode_message(msg)
        self.assertEqual(len(raw), 68)
        decoded = decode_message(raw, is_handshake=True)
        self.assertEqual(decoded, msg)

    def test_encode_handshake_convenience(self) -> None:
        ih = _fake_info_hash()
        pid = _fake_peer_id()
        raw = encode_handshake(ih, pid)
        self.assertEqual(len(raw), 68)
        decoded = decode_handshake(raw)
        self.assertEqual(decoded.info_hash, ih)
        self.assertEqual(decoded.peer_id, pid)

    def test_encode_handshake_bad_info_hash(self) -> None:
        with self.assertRaises(ValueError):
            encode_handshake(b"\x00" * 19, b"\x00" * 20)

    def test_encode_handshake_bad_peer_id(self) -> None:
        with self.assertRaises(ValueError):
            encode_handshake(b"\x00" * 20, b"\x00" * 21)

    def test_decode_wrong_pstrlen(self) -> None:
        raw = bytearray(encode_handshake(_fake_info_hash(), _fake_peer_id()))
        raw[0] = 18  # wrong pstrlen
        with self.assertRaises(ProtocolError):
            decode_message(bytes(raw), is_handshake=True)

    def test_decode_wrong_pstr(self) -> None:
        raw = bytearray(encode_handshake(_fake_info_hash(), _fake_peer_id()))
        raw[1:20] = b"X" * 19  # wrong protocol string
        with self.assertRaises(ProtocolError):
            decode_message(bytes(raw), is_handshake=True)

    def test_decode_short_data(self) -> None:
        with self.assertRaises(ProtocolError):
            decode_message(b"\x13" + b"BitTorrent protocol" + b"\x00" * 20, is_handshake=True)

    def test_handshake_pstr_bytes(self) -> None:
        """Verify the raw handshake starts with 0x13 'BitTorrent protocol'."""
        raw = encode_handshake(_fake_info_hash(), _fake_peer_id())
        self.assertEqual(raw[0], 19)
        self.assertEqual(raw[1:20], b"BitTorrent protocol")
        self.assertEqual(raw[20:28], b"\x00" * 8)

    def test_read_handshake_async(self) -> None:
        async def _test():
            raw = encode_handshake(_fake_info_hash(), _fake_peer_id())
            reader = await _make_reader(raw)
            hs = await read_handshake(reader)
            self.assertIsInstance(hs, Handshake)
            self.assertEqual(hs.info_hash, _fake_info_hash())
        asyncio.run(_test())


# ===================================================================
# KeepAlive
# ===================================================================


class TestKeepAlive(unittest.TestCase):
    """KeepAlive encode ↔ decode round-trip."""

    def test_round_trip(self) -> None:
        msg = KeepAlive()
        raw = encode_message(msg)
        self.assertEqual(raw, b"\x00\x00\x00\x00")
        decoded = decode_message(raw)
        self.assertIsInstance(decoded, KeepAlive)

    def test_read_keepalive_async(self) -> None:
        async def _test():
            reader = await _make_reader(b"\x00\x00\x00\x00")
            msg = await read_message(reader)
            self.assertIsInstance(msg, KeepAlive)
        asyncio.run(_test())


# ===================================================================
# Simple (no-payload) messages
# ===================================================================


class TestSimpleMessages(unittest.TestCase):
    """Round-trip tests for Choke, Unchoke, Interested, NotInterested."""

    def _round_trip(self, msg_cls: type, msg_id: int) -> None:
        msg = msg_cls()
        raw = encode_message(msg)
        # length prefix should be 1 (just the msg-id byte)
        length = struct.unpack("!I", raw[:4])[0]
        self.assertEqual(length, 1)
        self.assertEqual(raw[4], msg_id)
        decoded = decode_message(raw)
        self.assertEqual(decoded, msg)

    def test_choke(self) -> None:
        self._round_trip(Choke, MessageType.CHOKE)

    def test_unchoke(self) -> None:
        self._round_trip(Unchoke, MessageType.UNCHOKE)

    def test_interested(self) -> None:
        self._round_trip(Interested, MessageType.INTERESTED)

    def test_not_interested(self) -> None:
        self._round_trip(NotInterested, MessageType.NOT_INTERESTED)


# ===================================================================
# Have
# ===================================================================


class TestHave(unittest.TestCase):
    """Have message round-trip and validation."""

    def test_round_trip(self) -> None:
        msg = Have(piece_index=42)
        raw = encode_message(msg)
        decoded = decode_message(raw)
        self.assertEqual(decoded, msg)

    def test_round_trip_zero(self) -> None:
        msg = Have(piece_index=0)
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded, msg)

    def test_round_trip_large(self) -> None:
        msg = Have(piece_index=2**32 - 1)
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded, msg)

    def test_wrong_payload_size_too_small(self) -> None:
        # Hand-craft a Have with only 3 payload bytes.
        raw = struct.pack("!IB", 4, MessageType.HAVE) + b"\x00\x00\x00"
        with self.assertRaises(ProtocolError):
            decode_message(raw)

    def test_wrong_payload_size_too_large(self) -> None:
        raw = struct.pack("!IB", 6, MessageType.HAVE) + b"\x00" * 5
        with self.assertRaises(ProtocolError):
            decode_message(raw)


# ===================================================================
# Bitfield
# ===================================================================


class TestBitfield(unittest.TestCase):
    """Bitfield message round-trip for various sizes."""

    def test_round_trip_short(self) -> None:
        msg = Bitfield(bitfield=b"\xff")
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded, msg)

    def test_round_trip_empty(self) -> None:
        msg = Bitfield(bitfield=b"")
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded, msg)

    def test_round_trip_large(self) -> None:
        bf = bytes(range(256)) * 4  # 1024 bytes
        msg = Bitfield(bitfield=bf)
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded, msg)

    def test_round_trip_single_bit(self) -> None:
        msg = Bitfield(bitfield=b"\x80")
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded.bitfield, b"\x80")


# ===================================================================
# Request
# ===================================================================


class TestRequest(unittest.TestCase):
    """Request message round-trip and validation."""

    def test_round_trip(self) -> None:
        msg = Request(index=5, begin=0, length=16384)
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded, msg)

    def test_wrong_payload_size(self) -> None:
        raw = struct.pack("!IB", 9, MessageType.REQUEST) + b"\x00" * 8
        with self.assertRaises(ProtocolError):
            decode_message(raw)


# ===================================================================
# Piece
# ===================================================================


class TestPiece(unittest.TestCase):
    """Piece message round-trip, including large blocks."""

    def test_round_trip(self) -> None:
        block = b"Hello, BitTorrent!"
        msg = Piece(index=3, begin=16384, block=block)
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded, msg)

    def test_large_block(self) -> None:
        block = bytes(range(256)) * 64  # 16 KiB
        msg = Piece(index=0, begin=0, block=block)
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded, msg)
        self.assertEqual(len(decoded.block), 16384)

    def test_empty_block(self) -> None:
        msg = Piece(index=1, begin=0, block=b"")
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded, msg)

    def test_truncated_piece_payload(self) -> None:
        # Piece needs at least 8 bytes (index + begin).
        raw = struct.pack("!IB", 5, MessageType.PIECE) + b"\x00" * 4
        with self.assertRaises(ProtocolError):
            decode_message(raw)


# ===================================================================
# Cancel
# ===================================================================


class TestCancel(unittest.TestCase):
    """Cancel message round-trip and validation."""

    def test_round_trip(self) -> None:
        msg = Cancel(index=5, begin=0, length=16384)
        decoded = decode_message(encode_message(msg))
        self.assertEqual(decoded, msg)

    def test_wrong_payload_size(self) -> None:
        raw = struct.pack("!IB", 10, MessageType.CANCEL) + b"\x00" * 9
        with self.assertRaises(ProtocolError):
            decode_message(raw)


# ===================================================================
# Error handling
# ===================================================================


class TestErrorHandling(unittest.TestCase):
    """Protocol error detection."""

    def test_unknown_message_id(self) -> None:
        raw = struct.pack("!IB", 1, 99)  # ID 99 doesn't exist
        with self.assertRaises(ProtocolError):
            decode_message(raw)

    def test_truncated_length_prefix(self) -> None:
        with self.assertRaises(ProtocolError):
            decode_message(b"\x00\x00")

    def test_truncated_body(self) -> None:
        # Length says 5 but only 1 byte of body present.
        raw = struct.pack("!I", 5) + b"\x04"
        with self.assertRaises(ProtocolError):
            decode_message(raw)

    def test_choke_with_extra_payload(self) -> None:
        raw = struct.pack("!IB", 2, MessageType.CHOKE) + b"\xff"
        with self.assertRaises(ProtocolError):
            decode_message(raw)

    def test_unchoke_with_extra_payload(self) -> None:
        raw = struct.pack("!IB", 2, MessageType.UNCHOKE) + b"\xff"
        with self.assertRaises(ProtocolError):
            decode_message(raw)

    def test_interested_with_extra_payload(self) -> None:
        raw = struct.pack("!IB", 2, MessageType.INTERESTED) + b"\xff"
        with self.assertRaises(ProtocolError):
            decode_message(raw)

    def test_not_interested_with_extra_payload(self) -> None:
        raw = struct.pack("!IB", 2, MessageType.NOT_INTERESTED) + b"\xff"
        with self.assertRaises(ProtocolError):
            decode_message(raw)

    def test_encode_unknown_type(self) -> None:
        with self.assertRaises(TypeError):
            encode_message("not a message")  # type: ignore[arg-type]


# ===================================================================
# Async read_message
# ===================================================================


class TestAsyncReadMessage(unittest.IsolatedAsyncioTestCase):
    """Test read_message with a pre-loaded StreamReader."""

    async def test_read_choke(self) -> None:
        raw = encode_message(Choke())
        reader = await _make_reader(raw)
        msg = await read_message(reader)
        self.assertIsInstance(msg, Choke)

    async def test_read_piece(self) -> None:
        block = b"\xde\xad\xbe\xef" * 100
        raw = encode_message(Piece(index=7, begin=0, block=block))
        reader = await _make_reader(raw)
        msg = await read_message(reader)
        self.assertIsInstance(msg, Piece)
        self.assertEqual(msg.block, block)

    async def test_read_multiple_messages(self) -> None:
        """Read several messages sequentially from the same stream."""
        raw = b""
        raw += encode_message(Choke())
        raw += encode_message(Have(piece_index=10))
        raw += encode_message(KeepAlive())
        raw += encode_message(Unchoke())
        reader = await _make_reader(raw)

        m1 = await read_message(reader)
        m2 = await read_message(reader)
        m3 = await read_message(reader)
        m4 = await read_message(reader)

        self.assertIsInstance(m1, Choke)
        self.assertIsInstance(m2, Have)
        self.assertEqual(m2.piece_index, 10)
        self.assertIsInstance(m3, KeepAlive)
        self.assertIsInstance(m4, Unchoke)

    async def test_read_message_too_large(self) -> None:
        """Reject messages exceeding MAX_MESSAGE_LENGTH."""
        # Craft a length prefix that is way too large.
        length = MAX_MESSAGE_LENGTH + 1
        raw = struct.pack("!I", length) + b"\x00" * length
        reader = await _make_reader(raw)
        with self.assertRaises(ProtocolError):
            await read_message(reader)


# ===================================================================
# MessageType enum
# ===================================================================


class TestMessageType(unittest.TestCase):
    """Verify MessageType enum values match the spec."""

    def test_values(self) -> None:
        self.assertEqual(MessageType.CHOKE, 0)
        self.assertEqual(MessageType.UNCHOKE, 1)
        self.assertEqual(MessageType.INTERESTED, 2)
        self.assertEqual(MessageType.NOT_INTERESTED, 3)
        self.assertEqual(MessageType.HAVE, 4)
        self.assertEqual(MessageType.BITFIELD, 5)
        self.assertEqual(MessageType.REQUEST, 6)
        self.assertEqual(MessageType.PIECE, 7)
        self.assertEqual(MessageType.CANCEL, 8)


# ===================================================================
# Full round-trip for every message type in one test
# ===================================================================


class TestFullRoundTrip(unittest.TestCase):
    """Encode then decode every message type and verify equality."""

    def test_all_messages(self) -> None:
        ih = _fake_info_hash()
        pid = _fake_peer_id()

        messages = [
            (Handshake(info_hash=ih, peer_id=pid), True),
            (KeepAlive(), False),
            (Choke(), False),
            (Unchoke(), False),
            (Interested(), False),
            (NotInterested(), False),
            (Have(piece_index=999), False),
            (Bitfield(bitfield=b"\xff\x00\xaa"), False),
            (Request(index=1, begin=0, length=16384), False),
            (Piece(index=2, begin=32768, block=b"\xca\xfe" * 50), False),
            (Cancel(index=3, begin=0, length=16384), False),
        ]

        for msg, is_hs in messages:
            with self.subTest(msg=type(msg).__name__):
                raw = encode_message(msg)
                decoded = decode_message(raw, is_handshake=is_hs)
                self.assertEqual(
                    decoded, msg,
                    f"Round-trip failed for {type(msg).__name__}"
                )


if __name__ == "__main__":
    unittest.main()
