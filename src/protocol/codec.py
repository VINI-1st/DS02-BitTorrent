"""Encode / decode BitTorrent wire-protocol messages.

This module translates between ``messages.py`` dataclasses and raw bytes
on the wire.  It also provides async helpers that read complete messages
from an ``asyncio.StreamReader``.

Wire format
-----------
* **Handshake** – ``[1B pstrlen=19][19B pstr][8B reserved][20B info_hash][20B peer_id]`` (68 bytes).
* **KeepAlive** – ``\\x00\\x00\\x00\\x00`` (4 zero bytes).
* **All other messages** – ``[4B big-endian length][1B msg-id][payload]``
  where *length* = ``len(payload) + 1``.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Union

from .messages import (
    Bitfield,
    Cancel,
    Choke,
    Handshake,
    Have,
    Interested,
    KeepAlive,
    MessageType,
    NotInterested,
    PeerMessage,
    Piece,
    Request,
    Unchoke,
)

# Protocol string used in the handshake.
_PSTR: bytes = b"BitTorrent protocol"
_PSTRLEN: int = len(_PSTR)  # 19

# Maximum message body (length prefix value) we are willing to read.
# 2 MiB should be more than enough for any legitimate message (typical
# block size is 16 KiB; piece messages carry at most one block).
MAX_MESSAGE_LENGTH: int = 2 * 1024 * 1024


class ProtocolError(Exception):
    """Raised when wire-protocol data is malformed or violates the spec."""


# -----------------------------------------------------------------------
# Encoding
# -----------------------------------------------------------------------

def encode_handshake(info_hash: bytes, peer_id: bytes) -> bytes:
    """Encode a 68-byte handshake.

    Args:
        info_hash: 20-byte torrent identifier.
        peer_id: 20-byte peer identifier.

    Returns:
        68 raw bytes ready to send over TCP.

    Raises:
        ValueError: If *info_hash* or *peer_id* are not exactly 20 bytes.
    """
    if len(info_hash) != 20:
        raise ValueError(
            f"info_hash must be exactly 20 bytes, got {len(info_hash)}"
        )
    if len(peer_id) != 20:
        raise ValueError(
            f"peer_id must be exactly 20 bytes, got {len(peer_id)}"
        )
    return (
        struct.pack("!B", _PSTRLEN)
        + _PSTR
        + b"\x00" * 8
        + info_hash
        + peer_id
    )


def encode_message(msg: PeerMessage) -> bytes:
    """Encode a message object to its wire-format byte representation.

    Handles every message type defined in ``messages.py``.

    Args:
        msg: A message dataclass instance.

    Returns:
        Complete wire bytes (including the 4-byte length prefix for
        non-handshake messages).

    Raises:
        ValueError: If the message contains invalid field values.
        TypeError: If *msg* is not a recognised message type.
    """
    if isinstance(msg, Handshake):
        return encode_handshake(msg.info_hash, msg.peer_id)

    if isinstance(msg, KeepAlive):
        return b"\x00\x00\x00\x00"

    # Build [msg_id][payload], then prepend 4-byte length.
    msg_id: int
    payload: bytes

    if isinstance(msg, Choke):
        msg_id = MessageType.CHOKE
        payload = b""
    elif isinstance(msg, Unchoke):
        msg_id = MessageType.UNCHOKE
        payload = b""
    elif isinstance(msg, Interested):
        msg_id = MessageType.INTERESTED
        payload = b""
    elif isinstance(msg, NotInterested):
        msg_id = MessageType.NOT_INTERESTED
        payload = b""
    elif isinstance(msg, Have):
        msg_id = MessageType.HAVE
        payload = struct.pack("!I", msg.piece_index)
    elif isinstance(msg, Bitfield):
        msg_id = MessageType.BITFIELD
        payload = msg.bitfield
    elif isinstance(msg, Request):
        msg_id = MessageType.REQUEST
        payload = struct.pack("!III", msg.index, msg.begin, msg.length)
    elif isinstance(msg, Piece):
        msg_id = MessageType.PIECE
        payload = struct.pack("!II", msg.index, msg.begin) + msg.block
    elif isinstance(msg, Cancel):
        msg_id = MessageType.CANCEL
        payload = struct.pack("!III", msg.index, msg.begin, msg.length)
    else:
        raise TypeError(f"Unknown message type: {type(msg).__name__}")

    length = 1 + len(payload)  # msg_id (1 byte) + payload
    return struct.pack("!IB", length, msg_id) + payload


# -----------------------------------------------------------------------
# Decoding
# -----------------------------------------------------------------------

def _decode_payload(msg_id: int, payload: bytes) -> PeerMessage:
    """Decode a payload given its message ID.

    Args:
        msg_id: The 1-byte message identifier.
        payload: Everything after the message-ID byte.

    Returns:
        The corresponding message dataclass instance.

    Raises:
        ProtocolError: On unknown ID or incorrect payload length.
    """
    try:
        mt = MessageType(msg_id)
    except ValueError:
        raise ProtocolError(f"Unknown message ID: {msg_id}")

    if mt == MessageType.CHOKE:
        if payload:
            raise ProtocolError(
                f"Choke payload must be empty, got {len(payload)} bytes"
            )
        return Choke()

    if mt == MessageType.UNCHOKE:
        if payload:
            raise ProtocolError(
                f"Unchoke payload must be empty, got {len(payload)} bytes"
            )
        return Unchoke()

    if mt == MessageType.INTERESTED:
        if payload:
            raise ProtocolError(
                f"Interested payload must be empty, got {len(payload)} bytes"
            )
        return Interested()

    if mt == MessageType.NOT_INTERESTED:
        if payload:
            raise ProtocolError(
                f"NotInterested payload must be empty, got {len(payload)} bytes"
            )
        return NotInterested()

    if mt == MessageType.HAVE:
        if len(payload) != 4:
            raise ProtocolError(
                f"Have payload must be exactly 4 bytes, got {len(payload)}"
            )
        (piece_index,) = struct.unpack("!I", payload)
        return Have(piece_index=piece_index)

    if mt == MessageType.BITFIELD:
        # Any length is valid for a bitfield.
        return Bitfield(bitfield=payload)

    if mt == MessageType.REQUEST:
        if len(payload) != 12:
            raise ProtocolError(
                f"Request payload must be exactly 12 bytes, got {len(payload)}"
            )
        index, begin, length = struct.unpack("!III", payload)
        return Request(index=index, begin=begin, length=length)

    if mt == MessageType.PIECE:
        if len(payload) < 8:
            raise ProtocolError(
                f"Piece payload must be at least 8 bytes, got {len(payload)}"
            )
        index, begin = struct.unpack("!II", payload[:8])
        block = payload[8:]
        return Piece(index=index, begin=begin, block=block)

    if mt == MessageType.CANCEL:
        if len(payload) != 12:
            raise ProtocolError(
                f"Cancel payload must be exactly 12 bytes, got {len(payload)}"
            )
        index, begin, length = struct.unpack("!III", payload)
        return Cancel(index=index, begin=begin, length=length)

    # Unreachable if MessageType is exhaustive, but guard anyway.
    raise ProtocolError(f"Unhandled message ID: {msg_id}")  # pragma: no cover


def decode_handshake(data: bytes) -> Handshake:
    """Decode a 68-byte handshake.

    Args:
        data: Exactly 68 raw bytes.

    Returns:
        A ``Handshake`` instance.

    Raises:
        ProtocolError: If the data is malformed.
    """
    if len(data) != 68:
        raise ProtocolError(
            f"Handshake must be exactly 68 bytes, got {len(data)}"
        )
    pstrlen = data[0]
    if pstrlen != _PSTRLEN:
        raise ProtocolError(
            f"Invalid pstrlen in handshake: expected {_PSTRLEN}, got {pstrlen}"
        )
    pstr = data[1:20]
    if pstr != _PSTR:
        raise ProtocolError(
            f"Invalid protocol string in handshake: {pstr!r}"
        )
    # bytes 20..27 are reserved – we ignore them.
    info_hash = data[28:48]
    peer_id = data[48:68]
    return Handshake(info_hash=info_hash, peer_id=peer_id)


def decode_message(data: bytes, *, is_handshake: bool = False) -> PeerMessage:
    """Decode wire-format bytes into a message object.

    For handshakes, pass ``is_handshake=True`` and exactly 68 bytes.
    For all other messages, pass the **complete** frame including the
    4-byte length prefix.

    Args:
        data: Raw wire bytes.
        is_handshake: Whether *data* should be interpreted as a handshake.

    Returns:
        The decoded message dataclass instance.

    Raises:
        ProtocolError: On any framing or payload error.
    """
    if is_handshake:
        return decode_handshake(data)

    if len(data) < 4:
        raise ProtocolError(
            f"Message must be at least 4 bytes (length prefix), got {len(data)}"
        )

    (length,) = struct.unpack("!I", data[:4])

    if length == 0:
        # Keep-alive.
        return KeepAlive()

    body = data[4:]
    if len(body) < length:
        raise ProtocolError(
            f"Truncated message: length prefix says {length} but only "
            f"{len(body)} body bytes available"
        )

    msg_id = body[0]
    payload = body[1:length]
    return _decode_payload(msg_id, payload)


# -----------------------------------------------------------------------
# Async stream helpers
# -----------------------------------------------------------------------

async def read_handshake(reader: asyncio.StreamReader) -> Handshake:
    """Read a 68-byte handshake from *reader*.

    Args:
        reader: An ``asyncio.StreamReader`` connected to a peer.

    Returns:
        A ``Handshake`` instance.

    Raises:
        ProtocolError: If the handshake data is malformed.
        ConnectionError: If the connection is closed before all bytes arrive.
    """
    data = await reader.readexactly(68)
    return decode_handshake(data)


async def read_message(reader: asyncio.StreamReader) -> PeerMessage:
    """Read one length-prefixed message from *reader*.

    Reads the 4-byte length prefix, then reads that many more bytes and
    decodes the result.  **Does not handle handshakes** – use
    ``read_handshake`` for the initial exchange.

    Args:
        reader: An ``asyncio.StreamReader`` connected to a peer.

    Returns:
        The decoded message.

    Raises:
        ProtocolError: If the message is malformed or exceeds
            ``MAX_MESSAGE_LENGTH``.
        ConnectionError: If the connection is closed prematurely.
    """
    length_bytes = await reader.readexactly(4)
    (length,) = struct.unpack("!I", length_bytes)

    if length == 0:
        return KeepAlive()

    if length > MAX_MESSAGE_LENGTH:
        raise ProtocolError(
            f"Message length {length} exceeds maximum {MAX_MESSAGE_LENGTH}"
        )

    body = await reader.readexactly(length)
    msg_id = body[0]
    payload = body[1:]
    return _decode_payload(msg_id, payload)
