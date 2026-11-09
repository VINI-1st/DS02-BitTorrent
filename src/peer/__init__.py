"""Peer package (Role B): metainfo is in ``torrent``, storage in ``storage``,
picker in ``piece_picker``; parallel download (B4), upload (B5), failover
(B6) and the ``-m peer`` process (B7) live here.
"""

from .peer import PeerNode, main, parse_args

__all__ = ["PeerNode", "main", "parse_args"]
