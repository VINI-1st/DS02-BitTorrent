"""Peer core – shared wire helpers (sync sockets, spec framing).

Uses the real 68-byte handshake from ``protocol`` (Contract 3, written by A)
with a 20-byte peer id derived from the Contract 4 ``--peer-id`` string.
Short node names such as ``seed-9001`` are NUL-padded so the remote side can
recover the original name for Contract 5 logs.
"""

from __future__ import annotations

import socket
import struct

try:  # running with src/ on sys.path (tests, -m peer)
    from protocol.codec import ProtocolError, decode_handshake, decode_message, encode_handshake, encode_message
    from protocol.messages import Bitfield as MsgBitfield
except ImportError:  # pragma: no cover - fallback for `src.` prefixed imports
    from src.protocol.codec import ProtocolError, decode_handshake, decode_message, encode_handshake, encode_message
    from src.protocol.messages import Bitfield as MsgBitfield

BLOCK_SIZE = 16 * 1024
SOCKET_TIMEOUT = 8.0


class WireError(Exception):
    """Raised when framing/handshake bytes are malformed."""


def wire_peer_id(name: str) -> bytes:
    """Map a Contract 4 peer id string to 20 wire bytes (NUL-padded)."""
    raw = name.encode("utf-8")[:20]
    return raw.ljust(20, b"\x00")


def node_from_wire(pid: bytes) -> str:
    """Recover the node name from 20 wire bytes (strip NUL padding)."""
    try:
        return pid.rstrip(b"\x00").decode("utf-8") or pid.hex()
    except UnicodeDecodeError:
        return pid.hex()


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise WireError("connection closed by peer")
        buf.extend(chunk)
    return bytes(buf)


def send_msg(sock: socket.socket, msg) -> None:
    sock.sendall(encode_message(msg))


def recv_msg(sock: socket.socket):
    """Read one length-prefixed message (handshakes handled separately)."""
    try:
        header = recv_exact(sock, 4)
    except WireError as exc:
        raise WireError(str(exc)) from exc
    (length,) = struct.unpack("!I", header)
    if length == 0:
        from protocol.messages import KeepAlive  # local import for speed
        return KeepAlive()
    if length > 4 * 1024 * 1024:
        raise WireError(f"frame too large ({length})")
    body = recv_exact(sock, length)
    try:
        return decode_message(header + body)
    except Exception as exc:
        raise WireError(f"bad message: {exc}") from exc


def _read_handshake(sock: socket.socket, info_hash: bytes):
    try:
        remote = decode_handshake(recv_exact(sock, 68))
    except ProtocolError as exc:
        raise WireError(f"bad handshake: {exc}") from exc
    if remote.info_hash != info_hash:
        raise WireError("info_hash mismatch")
    return remote


def do_handshake(sock: socket.socket, info_hash: bytes, peer_id: bytes, *, client: bool) -> bytes:
    """Exchange 68-byte handshakes. Returns the remote 20-byte peer id."""
    if client:
        sock.sendall(encode_handshake(info_hash, peer_id))
        remote = _read_handshake(sock, info_hash)
    else:
        remote = _read_handshake(sock, info_hash)
        sock.sendall(encode_handshake(info_hash, peer_id))
    return remote.peer_id


def encode_bitfield(have: list[bool]) -> bytes:
    out = bytearray((len(have) + 7) // 8)
    for i, h in enumerate(have):
        if h:
            out[i // 8] |= 0x80 >> (i % 8)
    return bytes(out)


def decode_bitfield(data: bytes, n: int) -> set[int]:
    return {i for i in range(n) if i // 8 < len(data) and data[i // 8] & (0x80 >> (i % 8))}
