import math
from typing import Iterable, Iterator


class Bitfield:
    """Fixed-size set of piece indices a peer has.

    Wire form follows BitTorrent: piece 0 is the high bit of byte 0 and the
    spare bits in the last byte are zero.
    """

    def __init__(self, size: int, have: Iterable[int] = ()):
        if size < 0:
            raise ValueError("size must be non-negative")
        self._size = size
        self._bits = bytearray(math.ceil(size / 8))
        for index in have:
            self.set(index)

    def __len__(self) -> int:
        return self._size

    def __contains__(self, index: int) -> bool:
        return self.has(index)

    def __iter__(self) -> Iterator[int]:
        return (i for i in range(self._size) if self.has(i))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Bitfield):
            return NotImplemented
        return self._size == other._size and self._bits == other._bits

    def __repr__(self) -> str:
        return f"Bitfield({self._size}, have={list(self)})"

    def _check(self, index: int) -> None:
        if not 0 <= index < self._size:
            raise IndexError(f"piece index {index} out of range")

    def has(self, index: int) -> bool:
        self._check(index)
        return bool(self._bits[index >> 3] & (0x80 >> (index & 7)))

    def set(self, index: int) -> None:
        self._check(index)
        self._bits[index >> 3] |= 0x80 >> (index & 7)

    def clear(self, index: int) -> None:
        self._check(index)
        self._bits[index >> 3] &= ~(0x80 >> (index & 7)) & 0xFF

    def count(self) -> int:
        return sum(bin(b).count("1") for b in self._bits)

    def is_complete(self) -> bool:
        return self.count() == self._size

    def missing(self) -> Iterator[int]:
        return (i for i in range(self._size) if not self.has(i))

    def copy(self) -> "Bitfield":
        other = Bitfield(self._size)
        other._bits[:] = self._bits
        return other

    def to_bytes(self) -> bytes:
        return bytes(self._bits)

    @classmethod
    def from_bytes(cls, data: bytes, size: int) -> "Bitfield":
        """Parse a wire bitfield for `size` pieces; reject wrong length or spare bits."""
        if len(data) != math.ceil(size / 8):
            raise ValueError(
                f"bitfield for {size} pieces must be {math.ceil(size / 8)} bytes, got {len(data)}"
            )
        spare = len(data) * 8 - size
        if spare and data[-1] & ((1 << spare) - 1):
            raise ValueError("spare bits in bitfield must be zero")
        bf = cls(size)
        bf._bits[:] = data
        return bf
