"""Metainfo (B1): create, read and validate *.torrent.json files.

Format is defined in docs/contracts/metainfo-and-peer-cli.md.
"""

from .metainfo import (
    DEFAULT_PIECE_LENGTH,
    Metainfo,
    MetainfoError,
    create_metainfo,
    load_metainfo,
    save_metainfo,
)

__all__ = [
    "DEFAULT_PIECE_LENGTH",
    "Metainfo",
    "MetainfoError",
    "create_metainfo",
    "load_metainfo",
    "save_metainfo",
]
