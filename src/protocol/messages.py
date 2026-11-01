"""Wire protocol message types for the BitTorrent-like P2P system.

Defines all peer-to-peer message types as frozen dataclasses and a
``MessageType`` enum mapping message IDs to their integer values.

Wire format overview
--------------------
* **Handshake** – 68 bytes with its own framing (see ``Handshake``).
* **KeepAlive** – four zero bytes (length prefix = 0, no message ID).
* **All others** – ``[4-byte big-endian length prefix][1-byte message ID][payload]``
  where the length prefix covers the message ID + payload.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Union


class MessageType(IntEnum):
    """One-byte message IDs used on the wire."""

    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8


# ---------------------------------------------------------------------------
# Message dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Handshake:
    """BitTorrent handshake (68 bytes on the wire).

    Attributes:
        info_hash: 20-byte SHA-1 hash identifying the torrent.
        peer_id: 20-byte identifier for the sending peer.
    """

    info_hash: bytes
    peer_id: bytes

    def __post_init__(self) -> None:
        if len(self.info_hash) != 20:
            raise ValueError(
                f"info_hash must be exactly 20 bytes, got {len(self.info_hash)}"
            )
        if len(self.peer_id) != 20:
            raise ValueError(
                f"peer_id must be exactly 20 bytes, got {len(self.peer_id)}"
            )


@dataclass(frozen=True)
class KeepAlive:
    """Keep-alive message (4 zero bytes, no message ID)."""

    pass


@dataclass(frozen=True)
class Choke:
    """Choke message (ID 0, no payload)."""

    pass


@dataclass(frozen=True)
class Unchoke:
    """Unchoke message (ID 1, no payload)."""

    pass


@dataclass(frozen=True)
class Interested:
    """Interested message (ID 2, no payload)."""

    pass


@dataclass(frozen=True)
class NotInterested:
    """Not-interested message (ID 3, no payload)."""

    pass


@dataclass(frozen=True)
class Have:
    """Have message (ID 4) – announces possession of a piece.

    Attributes:
        piece_index: Zero-based index of the piece the sender has.
    """

    piece_index: int

    def __post_init__(self) -> None:
        if self.piece_index < 0:
            raise ValueError(
                f"piece_index must be non-negative, got {self.piece_index}"
            )


@dataclass(frozen=True)
class Bitfield:
    """Bitfield message (ID 5) – announces all pieces the sender has.

    Attributes:
        bitfield: Packed bitfield bytes (one bit per piece, MSB first).
    """

    bitfield: bytes


@dataclass(frozen=True)
class Request:
    """Request message (ID 6) – asks for a block within a piece.

    Attributes:
        index: Piece index.
        begin: Byte offset within the piece.
        length: Number of bytes requested.
    """

    index: int
    begin: int
    length: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError(f"index must be non-negative, got {self.index}")
        if self.begin < 0:
            raise ValueError(f"begin must be non-negative, got {self.begin}")
        if self.length < 0:
            raise ValueError(f"length must be non-negative, got {self.length}")


@dataclass(frozen=True)
class Piece:
    """Piece message (ID 7) – delivers a block of data.

    Attributes:
        index: Piece index.
        begin: Byte offset within the piece.
        block: Raw block data.
    """

    index: int
    begin: int
    block: bytes

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError(f"index must be non-negative, got {self.index}")
        if self.begin < 0:
            raise ValueError(f"begin must be non-negative, got {self.begin}")


@dataclass(frozen=True)
class Cancel:
    """Cancel message (ID 8) – cancels a previously sent Request.

    Attributes:
        index: Piece index.
        begin: Byte offset within the piece.
        length: Number of bytes originally requested.
    """

    index: int
    begin: int
    length: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError(f"index must be non-negative, got {self.index}")
        if self.begin < 0:
            raise ValueError(f"begin must be non-negative, got {self.begin}")
        if self.length < 0:
            raise ValueError(f"length must be non-negative, got {self.length}")


# Union type covering every possible decoded message.
PeerMessage = Union[
    Handshake,
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have,
    Bitfield,
    Request,
    Piece,
    Cancel,
]
