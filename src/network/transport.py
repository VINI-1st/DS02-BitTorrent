"""Abstract transport interface for BitTorrent peer-to-peer communication.

This module defines the ``Transport`` abstraction that the peer core (Member B)
uses to send and receive wire-protocol messages.  Two concrete implementations
exist:

* ``TcpTransport`` – real TCP connections (this package, ``tcp_transport.py``)
* ``FakeTransport`` – in-memory loopback written by Member B for unit tests
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any


# ---------------------------------------------------------------------------
# Enums & data classes
# ---------------------------------------------------------------------------

class ConnectionEvent(Enum):
    """Events that the transport can emit to its listener."""

    CONNECTED = auto()
    DISCONNECTED = auto()
    MESSAGE_RECEIVED = auto()
    ERROR = auto()


@dataclass
class PeerConnection:
    """Represents a connection to a remote peer.

    Attributes:
        peer_id:        20-byte peer identifier obtained from the handshake.
        remote_ip:      IP address of the remote end.
        remote_port:    TCP port of the remote end.
        info_hash:      20-byte info_hash exchanged during the handshake.
        reader:         The ``asyncio.StreamReader`` for the connection.
        writer:         The ``asyncio.StreamWriter`` for the connection.
        connected_at:   Monotonic timestamp when the connection was established.
        last_message_at: Monotonic timestamp of the last received message.
    """

    peer_id: bytes
    remote_ip: str
    remote_port: int
    info_hash: bytes
    reader: asyncio.StreamReader | None = None
    writer: asyncio.StreamWriter | None = None
    connected_at: float = 0.0
    last_message_at: float = 0.0


# ---------------------------------------------------------------------------
# Listener (callback interface)
# ---------------------------------------------------------------------------

class TransportListener(ABC):
    """Callback interface for transport events.

    The peer core (Member B) implements this interface and passes it to
    ``Transport.start()`` so that it receives notifications about new
    connections, incoming messages, disconnects, and errors.
    """

    @abstractmethod
    async def on_connected(self, conn: PeerConnection) -> None:
        """Called when a new peer connection is established (inbound or outbound).

        Args:
            conn: The newly established ``PeerConnection``.
        """
        ...

    @abstractmethod
    async def on_disconnected(self, conn: PeerConnection, reason: str) -> None:
        """Called when a peer disconnects or the connection is lost.

        Args:
            conn:   The disconnected ``PeerConnection``.
            reason: Human-readable description of why the connection closed.
        """
        ...

    @abstractmethod
    async def on_message(self, conn: PeerConnection, message: Any) -> None:
        """Called when a complete wire-protocol message is received.

        Args:
            conn:    The ``PeerConnection`` the message arrived on.
            message: One of the message dataclasses defined in
                     ``src.protocol.messages`` (e.g. ``Choke``, ``Piece``, …).
        """
        ...

    @abstractmethod
    async def on_error(self, conn: PeerConnection, error: Exception) -> None:
        """Called when a connection-level error occurs.

        Args:
            conn:  The ``PeerConnection`` that experienced the error.
            error: The exception that was raised.
        """
        ...


# ---------------------------------------------------------------------------
# Transport (abstract base class)
# ---------------------------------------------------------------------------

class Transport(ABC):
    """Abstract transport interface.

    Concrete implementations:

    * ``TcpTransport`` – real asyncio TCP (this package)
    * ``FakeTransport`` – in-memory loopback (written by Member B)
    """

    @abstractmethod
    async def start(
        self,
        host: str,
        port: int,
        info_hash: bytes,
        peer_id: bytes,
        listener: TransportListener,
    ) -> None:
        """Start listening for incoming connections.

        Args:
            host:      Local bind address (e.g. ``"0.0.0.0"``).
            port:      Local TCP port to listen on (``0`` for OS-assigned).
            info_hash: 20-byte torrent info hash.
            peer_id:   Our 20-byte peer identifier.
            listener:  The ``TransportListener`` to notify of events.
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop listening and close **all** active connections."""
        ...

    @abstractmethod
    async def connect(self, remote_ip: str, remote_port: int) -> PeerConnection:
        """Initiate an outbound connection to a peer.

        Performs the BitTorrent handshake before returning.

        Args:
            remote_ip:   IP address of the remote peer.
            remote_port: TCP port of the remote peer.

        Returns:
            A fully-initialised ``PeerConnection``.

        Raises:
            ConnectionError: If the connection or handshake fails.
            RuntimeError:    If the max-connection limit has been reached.
        """
        ...

    @abstractmethod
    async def send(self, conn: PeerConnection, message: Any) -> None:
        """Send a wire-protocol message to a connected peer.

        Args:
            conn:    The target ``PeerConnection``.
            message: A message dataclass from ``src.protocol.messages``.

        Raises:
            ConnectionError: If the peer is no longer connected.
        """
        ...

    @abstractmethod
    async def disconnect(self, conn: PeerConnection) -> None:
        """Gracefully close a specific peer connection.

        Args:
            conn: The ``PeerConnection`` to close.
        """
        ...

    @abstractmethod
    def get_connections(self) -> list[PeerConnection]:
        """Return a snapshot of all active connections.

        Returns:
            A list of ``PeerConnection`` objects that are currently alive.
        """
        ...
