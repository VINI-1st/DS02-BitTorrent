import hashlib
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from torrent import Metainfo, MetainfoError, create_metainfo, load_metainfo, save_metainfo
from torrent.make_torrent import main as make_torrent_main

ANNOUNCE = "http://127.0.0.1:8000/announce"


class MetainfoTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def write_file(self, name, data):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def test_pieces_and_last_piece_size(self):
        data = os.urandom(10 * 1024 + 123)
        path = self.write_file("a.bin", data)
        meta = create_metainfo(path, ANNOUNCE, piece_length=1024)

        self.assertEqual(meta.num_pieces, 11)
        self.assertEqual(meta.piece_size(0), 1024)
        self.assertEqual(meta.piece_size(10), 123)
        for i in range(meta.num_pieces):
            chunk = data[i * 1024 : (i + 1) * 1024]
            self.assertEqual(meta.piece_hash(i), hashlib.sha1(chunk).digest())

    def test_exact_multiple_has_no_extra_piece(self):
        path = self.write_file("b.bin", b"x" * 4096)
        meta = create_metainfo(path, ANNOUNCE, piece_length=1024)
        self.assertEqual(meta.num_pieces, 4)
        self.assertEqual(meta.piece_size(3), 1024)

    def test_empty_file(self):
        path = self.write_file("empty.bin", b"")
        meta = create_metainfo(path, ANNOUNCE, piece_length=1024)
        self.assertEqual(meta.num_pieces, 0)

    def test_round_trip(self):
        path = self.write_file("c.bin", os.urandom(5000))
        meta = create_metainfo(path, ANNOUNCE, piece_length=1000)
        out = os.path.join(self.tmp.name, "c.torrent.json")
        save_metainfo(meta, out)
        loaded = load_metainfo(out)

        self.assertEqual(loaded, meta)
        self.assertEqual(loaded.info_hash, meta.info_hash)
        self.assertEqual(len(loaded.info_hash_bytes), 20)

    def test_info_hash_ignores_announce(self):
        path = self.write_file("d.bin", b"hello world")
        a = create_metainfo(path, ANNOUNCE)
        b = create_metainfo(path, "http://other:9000/announce")
        self.assertEqual(a.info_hash, b.info_hash)

    def test_info_hash_changes_with_content(self):
        a = create_metainfo(self.write_file("e.bin", b"aaaa"), ANNOUNCE)
        b = create_metainfo(self.write_file("e.bin", b"aaab"), ANNOUNCE)
        self.assertNotEqual(a.info_hash, b.info_hash)

    def _saved_dict(self):
        path = self.write_file("f.bin", os.urandom(3000))
        return create_metainfo(path, ANNOUNCE, piece_length=1000).to_dict()

    def test_rejects_tampered_info(self):
        data = self._saved_dict()
        data["info"]["length"] = 2999
        with self.assertRaises(MetainfoError):
            Metainfo.from_dict(data)

    def test_rejects_wrong_piece_count(self):
        data = self._saved_dict()
        data["info"]["pieces"].pop()
        del data["info_hash"]
        with self.assertRaises(MetainfoError):
            Metainfo.from_dict(data)

    def test_rejects_bad_hash_and_missing_field(self):
        data = self._saved_dict()
        data["info"]["pieces"][0] = "zz" * 20
        del data["info_hash"]
        with self.assertRaises(MetainfoError):
            Metainfo.from_dict(data)
        with self.assertRaises(MetainfoError):
            Metainfo.from_dict({"announce": ANNOUNCE})

    def test_rejects_path_in_name(self):
        data = self._saved_dict()
        data["info"]["name"] = "../evil"
        del data["info_hash"]
        with self.assertRaises(MetainfoError):
            Metainfo.from_dict(data)

    def test_load_rejects_invalid_json(self):
        path = self.write_file("bad.torrent.json", b"{not json")
        with self.assertRaises(MetainfoError):
            load_metainfo(path)

    def test_make_torrent_cli(self):
        path = self.write_file("g.bin", os.urandom(2048))
        out = os.path.join(self.tmp.name, "g.json")
        rc = make_torrent_main([path, "--announce", ANNOUNCE, "--piece-length", "512", "-o", out])
        self.assertEqual(rc, 0)
        with open(out) as f:
            data = json.load(f)
        self.assertEqual(len(data["info"]["pieces"]), 4)
        self.assertEqual(load_metainfo(out).info_hash, data["info_hash"])


if __name__ == "__main__":
    unittest.main()
