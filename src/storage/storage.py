import hashlib
import os
import threading

from torrent import Metainfo

from .bitfield import Bitfield

PART_SUFFIX = ".part"


class StorageError(Exception):
    """Base class for storage failures."""


class PieceHashError(StorageError):
    """Piece data has the wrong size or does not match its SHA-1 in the metainfo."""


class PieceNotAvailable(StorageError):
    """Piece has not been downloaded (or stored) yet."""


class PieceStorage:
    """Reads and writes the pieces of one torrent's file inside `directory`.

    Data lives in `DIR/<name>.part` until every piece is verified, then the file
    is renamed to `DIR/<name>`. On open, any existing file is hashed piece by
    piece, so a seed or a restarted leech starts with the right bitfield.
    """

    def __init__(self, meta: Metainfo, directory: str):
        self.meta = meta
        self.final_path = os.path.join(directory, meta.name)
        self.part_path = self.final_path + PART_SUFFIX
        self._lock = threading.RLock()
        self._bitfield = Bitfield(meta.num_pieces)
        self._file = None

        os.makedirs(directory, exist_ok=True)
        self._open_existing()

    @property
    def path(self) -> str:
        return self.final_path if self.is_complete() else self._current_path

    def _open_existing(self) -> None:
        if os.path.exists(self.final_path):
            self._current_path = self.final_path
        else:
            self._current_path = self.part_path
            if not os.path.exists(self.part_path):
                open(self.part_path, "wb").close()
        self._file = open(self._current_path, "r+b")
        if self._current_path == self.part_path:
            self._file.truncate(self.meta.length)
        self._scan()
        self._finalize_if_complete()

    def _scan(self) -> None:
        for index in range(self.meta.num_pieces):
            data = self._read_raw(index)
            if self._matches(index, data):
                self._bitfield.set(index)

    def _finalize_if_complete(self) -> None:
        if not self._bitfield.is_complete() or self._current_path == self.final_path:
            return
        # Windows cannot rename an open file.
        self._file.close()
        os.replace(self.part_path, self.final_path)
        self._current_path = self.final_path
        self._file = open(self.final_path, "r+b")

    def _matches(self, index: int, data: bytes) -> bool:
        return (
            len(data) == self.meta.piece_size(index)
            and hashlib.sha1(data).digest() == self.meta.piece_hash(index)
        )

    def _read_raw(self, index: int) -> bytes:
        self._file.seek(index * self.meta.piece_length)
        return self._file.read(self.meta.piece_size(index))

    def bitfield(self) -> Bitfield:
        """Snapshot of the pieces we have."""
        with self._lock:
            return self._bitfield.copy()

    def has_piece(self, index: int) -> bool:
        with self._lock:
            return self._bitfield.has(index)

    def is_complete(self) -> bool:
        with self._lock:
            return self._bitfield.is_complete()

    def missing_pieces(self) -> list[int]:
        with self._lock:
            return list(self._bitfield.missing())

    def bytes_left(self) -> int:
        """Bytes still to download, as reported to the tracker (`left`)."""
        with self._lock:
            return sum(self.meta.piece_size(i) for i in self._bitfield.missing())

    def write_piece(self, index: int, data: bytes) -> bool:
        """Verify and store a whole piece.

        Returns True if it was stored, False if we already had it (duplicate
        deliveries are normal in endgame). Raises PieceHashError and stores
        nothing if the data is wrong.
        """
        expected_size = self.meta.piece_size(index)
        with self._lock:
            if self._bitfield.has(index):
                return False
            if not self._matches(index, data):
                raise PieceHashError(
                    f"piece {index} failed verification "
                    f"(got {len(data)} bytes, expected {expected_size})"
                )
            self._file.seek(index * self.meta.piece_length)
            self._file.write(data)
            self._file.flush()
            self._bitfield.set(index)
            self._finalize_if_complete()
            return True

    def read_piece(self, index: int) -> bytes:
        with self._lock:
            if not self._bitfield.has(index):
                raise PieceNotAvailable(f"piece {index} not available")
            return self._read_raw(index)

    def read_block(self, index: int, begin: int, length: int) -> bytes:
        """Read part of a piece, for answering a wire `request(index, begin, length)`."""
        size = self.meta.piece_size(index)
        if begin < 0 or length <= 0 or begin + length > size:
            raise ValueError(
                f"block {begin}+{length} outside piece {index} of {size} bytes"
            )
        return self.read_piece(index)[begin : begin + length]

    def close(self) -> None:
        with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None

    def __enter__(self) -> "PieceStorage":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
