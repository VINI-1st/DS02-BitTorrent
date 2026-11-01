import hashlib
import json
import math
import os
import re
import time
from dataclasses import dataclass, field

DEFAULT_PIECE_LENGTH = 256 * 1024
CREATED_BY = "ds02-make-torrent"

_HEX_SHA1 = re.compile(r"^[0-9a-f]{40}$")


class MetainfoError(ValueError):
    """Raised when a metainfo file is malformed or inconsistent."""


def _canonical_info(info: dict) -> bytes:
    return json.dumps(
        info, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def compute_info_hash(info: dict) -> str:
    return hashlib.sha1(_canonical_info(info)).hexdigest()


@dataclass(frozen=True)
class Metainfo:
    announce: str
    name: str
    length: int
    piece_length: int
    pieces: tuple[str, ...]
    created_by: str = CREATED_BY
    creation_date: int = field(default_factory=lambda: int(time.time()))

    @property
    def num_pieces(self) -> int:
        return len(self.pieces)

    @property
    def info(self) -> dict:
        return {
            "name": self.name,
            "length": self.length,
            "piece_length": self.piece_length,
            "pieces": list(self.pieces),
        }

    @property
    def info_hash(self) -> str:
        """Hex form, as sent to the tracker."""
        return compute_info_hash(self.info)

    @property
    def info_hash_bytes(self) -> bytes:
        """Raw 20 bytes, as used in the wire handshake."""
        return bytes.fromhex(self.info_hash)

    def piece_size(self, index: int) -> int:
        """Size in bytes of piece `index`; the last piece may be shorter."""
        if not 0 <= index < self.num_pieces:
            raise IndexError(f"piece index {index} out of range")
        if index == self.num_pieces - 1:
            return self.length - index * self.piece_length
        return self.piece_length

    def piece_hash(self, index: int) -> bytes:
        return bytes.fromhex(self.pieces[index])

    def to_dict(self) -> dict:
        return {
            "announce": self.announce,
            "created_by": self.created_by,
            "creation_date": self.creation_date,
            "info": self.info,
            "info_hash": self.info_hash,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Metainfo":
        try:
            info = data["info"]
            meta = cls(
                announce=str(data["announce"]),
                name=info["name"],
                length=info["length"],
                piece_length=info["piece_length"],
                pieces=tuple(info["pieces"]),
                created_by=data.get("created_by", ""),
                creation_date=int(data.get("creation_date", 0)),
            )
        except (KeyError, TypeError) as exc:
            raise MetainfoError(f"missing or invalid field: {exc}") from exc
        meta._validate()
        expected = data.get("info_hash")
        if expected is not None and expected != meta.info_hash:
            raise MetainfoError("info_hash does not match info")
        return meta

    def _validate(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise MetainfoError("name must be a non-empty string")
        if os.path.basename(self.name) != self.name or self.name in (".", ".."):
            raise MetainfoError("name must be a plain file name")
        if not isinstance(self.length, int) or self.length < 0:
            raise MetainfoError("length must be a non-negative integer")
        if not isinstance(self.piece_length, int) or self.piece_length <= 0:
            raise MetainfoError("piece_length must be a positive integer")
        expected = math.ceil(self.length / self.piece_length)
        if len(self.pieces) != expected:
            raise MetainfoError(
                f"expected {expected} piece hashes, got {len(self.pieces)}"
            )
        for h in self.pieces:
            if not isinstance(h, str) or not _HEX_SHA1.match(h):
                raise MetainfoError(f"invalid piece hash: {h!r}")


def hash_pieces(path: str, piece_length: int) -> list[str]:
    hashes = []
    with open(path, "rb") as f:
        while chunk := f.read(piece_length):
            hashes.append(hashlib.sha1(chunk).hexdigest())
    return hashes


def create_metainfo(
    path: str, announce: str, piece_length: int = DEFAULT_PIECE_LENGTH
) -> Metainfo:
    if piece_length <= 0:
        raise MetainfoError("piece_length must be a positive integer")
    meta = Metainfo(
        announce=announce,
        name=os.path.basename(path),
        length=os.path.getsize(path),
        piece_length=piece_length,
        pieces=tuple(hash_pieces(path, piece_length)),
    )
    meta._validate()
    return meta


def save_metainfo(meta: Metainfo, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(meta.to_dict(), f, indent=2, ensure_ascii=False)
        f.write("\n")


def load_metainfo(path: str) -> Metainfo:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise MetainfoError(f"not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise MetainfoError("metainfo must be a JSON object")
    return Metainfo.from_dict(data)
