import os
import random
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from storage import Bitfield, PieceHashError, PieceNotAvailable, PieceStorage
from torrent import create_metainfo

ANNOUNCE = "http://127.0.0.1:8000/announce"
PIECE = 1024


class BitfieldTest(unittest.TestCase):
    def test_set_clear_has_count(self):
        bf = Bitfield(10)
        self.assertEqual(bf.count(), 0)
        bf.set(0)
        bf.set(9)
        self.assertTrue(bf.has(0) and bf.has(9) and not bf.has(5))
        self.assertEqual(list(bf), [0, 9])
        self.assertEqual(list(bf.missing()), [1, 2, 3, 4, 5, 6, 7, 8])
        bf.clear(0)
        self.assertEqual(bf.count(), 1)
        self.assertFalse(bf.is_complete())

    def test_out_of_range(self):
        bf = Bitfield(3)
        for bad in (-1, 3):
            with self.assertRaises(IndexError):
                bf.has(bad)
            with self.assertRaises(IndexError):
                bf.set(bad)

    def test_wire_format_is_msb_first(self):
        bf = Bitfield(10, have=[0, 9])
        self.assertEqual(bf.to_bytes(), bytes([0b10000000, 0b01000000]))

    def test_bytes_round_trip(self):
        bf = Bitfield(13, have=[1, 4, 12])
        self.assertEqual(Bitfield.from_bytes(bf.to_bytes(), 13), bf)

    def test_from_bytes_rejects_bad_input(self):
        with self.assertRaises(ValueError):
            Bitfield.from_bytes(b"\x00", 10)  # too short
        with self.assertRaises(ValueError):
            Bitfield.from_bytes(b"\x00\x01", 10)  # spare bit set

    def test_empty_bitfield_is_complete(self):
        self.assertTrue(Bitfield(0).is_complete())

    def test_copy_is_independent(self):
        bf = Bitfield(4, have=[1])
        other = bf.copy()
        other.set(2)
        self.assertFalse(bf.has(2))


class PieceStorageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.src_dir = os.path.join(self.tmp.name, "src")
        self.dst_dir = os.path.join(self.tmp.name, "dst")
        os.makedirs(self.src_dir)
        self.data = random.Random(1).randbytes(5 * PIECE + 100)
        self.src_path = os.path.join(self.src_dir, "f.bin")
        with open(self.src_path, "wb") as f:
            f.write(self.data)
        self.meta = create_metainfo(self.src_path, ANNOUNCE, piece_length=PIECE)

    def piece(self, i):
        return self.data[i * PIECE : (i + 1) * PIECE]

    def open_dst(self):
        storage = PieceStorage(self.meta, self.dst_dir)
        self.addCleanup(storage.close)
        return storage

    def test_seed_directory_starts_complete(self):
        storage = PieceStorage(self.meta, self.src_dir)
        self.addCleanup(storage.close)
        self.assertTrue(storage.is_complete())
        self.assertEqual(storage.bytes_left(), 0)
        self.assertEqual(storage.read_piece(5), self.piece(5))

    def test_fresh_directory_starts_empty(self):
        storage = self.open_dst()
        self.assertEqual(storage.bitfield().count(), 0)
        self.assertEqual(storage.missing_pieces(), [0, 1, 2, 3, 4, 5])
        self.assertEqual(storage.bytes_left(), len(self.data))

    def test_rejects_wrong_hash_and_stores_nothing(self):
        storage = self.open_dst()
        bad = bytes(PIECE)
        with self.assertRaises(PieceHashError):
            storage.write_piece(0, bad)
        self.assertFalse(storage.has_piece(0))
        self.assertTrue(storage.write_piece(0, self.piece(0)))

    def test_rejects_wrong_size(self):
        storage = self.open_dst()
        with self.assertRaises(PieceHashError):
            storage.write_piece(0, self.piece(0)[:-1])
        with self.assertRaises(PieceHashError):
            storage.write_piece(5, self.piece(5) + b"x")

    def test_duplicate_write_returns_false(self):
        storage = self.open_dst()
        self.assertTrue(storage.write_piece(2, self.piece(2)))
        self.assertFalse(storage.write_piece(2, self.piece(2)))

    def test_out_of_range_index(self):
        storage = self.open_dst()
        with self.assertRaises(IndexError):
            storage.write_piece(6, b"")
        with self.assertRaises(IndexError):
            storage.has_piece(-1)

    def test_read_missing_piece(self):
        storage = self.open_dst()
        with self.assertRaises(PieceNotAvailable):
            storage.read_piece(1)

    def test_read_block(self):
        storage = self.open_dst()
        storage.write_piece(1, self.piece(1))
        self.assertEqual(storage.read_block(1, 10, 20), self.piece(1)[10:30])
        for begin, length in ((-1, 5), (0, 0), (PIECE - 1, 2)):
            with self.assertRaises(ValueError):
                storage.read_block(1, begin, length)
        with self.assertRaises(PieceNotAvailable):
            storage.read_block(0, 0, 5)

    def test_out_of_order_download_reassembles_original(self):
        storage = self.open_dst()
        order = [4, 0, 5, 2, 1]
        for i in order:
            storage.write_piece(i, self.piece(i))
        self.assertFalse(storage.is_complete())
        self.assertTrue(os.path.exists(storage.part_path))
        self.assertFalse(os.path.exists(storage.final_path))
        self.assertEqual(storage.bytes_left(), PIECE)

        storage.write_piece(3, self.piece(3))
        self.assertTrue(storage.is_complete())
        self.assertFalse(os.path.exists(storage.part_path))
        with open(storage.final_path, "rb") as f:
            self.assertEqual(f.read(), self.data)
        self.assertEqual(storage.read_piece(3), self.piece(3))

    def test_resume_from_partial_download(self):
        storage = PieceStorage(self.meta, self.dst_dir)
        for i in (0, 3):
            storage.write_piece(i, self.piece(i))
        storage.close()

        storage = self.open_dst()
        self.assertEqual(list(storage.bitfield()), [0, 3])
        self.assertEqual(storage.read_piece(3), self.piece(3))

    def test_corrupted_piece_on_disk_is_dropped_on_reopen(self):
        storage = PieceStorage(self.meta, self.dst_dir)
        storage.write_piece(0, self.piece(0))
        storage.write_piece(1, self.piece(1))
        storage.close()
        with open(storage.part_path, "r+b") as f:
            f.seek(PIECE + 5)
            f.write(b"\xff\xff")

        storage = self.open_dst()
        self.assertEqual(list(storage.bitfield()), [0])

    def test_reopen_after_completion(self):
        storage = PieceStorage(self.meta, self.dst_dir)
        for i in range(6):
            storage.write_piece(i, self.piece(i))
        storage.close()
        storage = self.open_dst()
        self.assertTrue(storage.is_complete())

    def test_corrupted_seed_file_is_reported_incomplete(self):
        with open(self.src_path, "r+b") as f:
            f.seek(2 * PIECE)
            f.write(b"corrupt")
        storage = PieceStorage(self.meta, self.src_dir)
        self.addCleanup(storage.close)
        self.assertEqual(storage.missing_pieces(), [2])

    def test_empty_file(self):
        empty = os.path.join(self.src_dir, "empty.bin")
        open(empty, "wb").close()
        meta = create_metainfo(empty, ANNOUNCE, piece_length=PIECE)
        storage = PieceStorage(meta, self.dst_dir)
        self.addCleanup(storage.close)
        self.assertTrue(storage.is_complete())
        self.assertTrue(os.path.exists(storage.final_path))

    def test_exact_multiple_of_piece_length(self):
        exact = os.path.join(self.src_dir, "exact.bin")
        with open(exact, "wb") as f:
            f.write(os.urandom(2 * PIECE))
        meta = create_metainfo(exact, ANNOUNCE, piece_length=PIECE)
        with open(exact, "rb") as f:
            blob = f.read()
        storage = PieceStorage(meta, self.dst_dir)
        self.addCleanup(storage.close)
        storage.write_piece(0, blob[:PIECE])
        storage.write_piece(1, blob[PIECE:])
        self.assertTrue(storage.is_complete())


if __name__ == "__main__":
    unittest.main()
