import os
import random
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from piece_picker import PiecePicker
from storage import Bitfield


def picker(size, have=(), **kw):
    return PiecePicker(Bitfield(size, have), rng=random.Random(1), **kw)


class RarestFirstTest(unittest.TestCase):
    def test_rarest_piece_first(self):
        p = picker(4)
        p.peer_bitfield("a", Bitfield(4, [0, 1, 2, 3]))
        p.peer_bitfield("b", Bitfield(4, [0, 1, 2]))
        p.peer_bitfield("c", Bitfield(4, [0, 1]))
        # availability: 0->3, 1->3, 2->2, 3->1
        picked = p.pick("a", 4)
        self.assertEqual(picked[:2], [3, 2])
        self.assertEqual(sorted(picked[2:]), [0, 1])

    def test_only_pieces_the_peer_has(self):
        p = picker(4)
        p.peer_bitfield("a", Bitfield(4, [0, 1, 2, 3]))
        p.peer_bitfield("b", Bitfield(4, [1]))
        self.assertEqual(p.pick("b", 3), [1])

    def test_skips_owned_and_in_flight(self):
        p = picker(4, have=[0])
        p.peer_bitfield("a", Bitfield(4, [0, 1, 2, 3]))
        p.peer_bitfield("b", Bitfield(4, [0, 1, 2, 3]))
        first = p.pick("a", 2)
        second = p.pick("b", 5)
        self.assertNotIn(0, first + second)
        self.assertEqual(set(first) & set(second), set())
        self.assertEqual(sorted(first + second), [1, 2, 3])

    def test_have_updates_availability(self):
        p = picker(3)
        p.peer_bitfield("a", Bitfield(3, [0, 1, 2]))
        p.peer_bitfield("b", Bitfield(3, [0, 1]))
        self.assertEqual(p.pick("a", 1), [2])
        p.release("a", 2)
        p.peer_have("b", 2)
        p.peer_have("b", 2)  # duplicate have must not double count
        self.assertEqual(p.availability(2), 2)
        p.peer_have("c", 0)  # unknown peer gets a fresh bitfield
        self.assertEqual(p.availability(0), 3)

    def test_peer_gone_frees_pieces_and_availability(self):
        p = picker(3)
        p.peer_bitfield("a", Bitfield(3, [0, 1, 2]))
        p.peer_bitfield("b", Bitfield(3, [0, 1, 2]))
        got = p.pick("a", 2)
        self.assertEqual(sorted(p.peer_gone("a")), sorted(got))
        self.assertEqual(p.availability(0), 1)
        self.assertEqual(sorted(p.pick("b", 3)), [0, 1, 2])

    def test_unknown_peer_and_bad_bitfield(self):
        p = picker(3)
        self.assertEqual(p.pick("nobody"), [])
        with self.assertRaises(ValueError):
            p.peer_bitfield("a", Bitfield(5))

    def test_ties_are_random_but_reproducible(self):
        def first_pick(seed):
            p = PiecePicker(Bitfield(50), rng=random.Random(seed))
            p.peer_bitfield("a", Bitfield(50, range(50)))
            return p.pick("a", 5)

        self.assertEqual(first_pick(7), first_pick(7))
        self.assertNotEqual(first_pick(7), first_pick(8))

    def test_interested_in(self):
        p = picker(3, have=[0])
        p.peer_bitfield("a", Bitfield(3, [0]))
        p.peer_bitfield("b", Bitfield(3, [0, 2]))
        self.assertFalse(p.interested_in("a"))
        self.assertTrue(p.interested_in("b"))
        self.assertFalse(p.interested_in("zzz"))


class EndgameTest(unittest.TestCase):
    def test_no_duplicates_before_endgame(self):
        p = picker(4)
        p.peer_bitfield("a", Bitfield(4, range(4)))
        p.peer_bitfield("b", Bitfield(4, range(4)))
        p.pick("a", 2)
        self.assertFalse(p.is_endgame())
        self.assertEqual(len(p.pick("b", 4)), 2)

    def test_endgame_when_everything_in_flight(self):
        p = picker(2)
        p.peer_bitfield("a", Bitfield(2, [0, 1]))
        p.peer_bitfield("b", Bitfield(2, [0, 1]))
        self.assertEqual(sorted(p.pick("a", 2)), [0, 1])
        self.assertTrue(p.is_endgame())
        self.assertEqual(sorted(p.pick("b", 2)), [0, 1])
        # but a peer is never asked twice for the same piece
        self.assertEqual(p.pick("a", 2), [])
        self.assertEqual(p.pick("b", 2), [])

    def test_endgame_threshold(self):
        p = picker(5, have=[0, 1, 2], endgame_threshold=2)
        p.peer_bitfield("a", Bitfield(5, range(5)))
        p.peer_bitfield("b", Bitfield(5, range(5)))
        self.assertTrue(p.is_endgame())
        p.pick("a", 1)
        self.assertEqual(len(p.pick("b", 2)), 2)  # one duplicate + one fresh

    def test_piece_done_returns_peers_to_cancel(self):
        p = picker(1)
        p.peer_bitfield("a", Bitfield(1, [0]))
        p.peer_bitfield("b", Bitfield(1, [0]))
        p.pick("a")
        p.pick("b")
        self.assertEqual(p.piece_done(0), ["a", "b"])
        self.assertTrue(p.own.is_complete())
        self.assertFalse(p.is_endgame())
        self.assertEqual(p.pick("a"), [])

    def test_endgame_prefers_less_duplicated_pieces(self):
        p = picker(2)
        for name in "abc":
            p.peer_bitfield(name, Bitfield(2, [0, 1]))
        p.pick("a", 2)
        p.pick("b", 1)  # b duplicates one piece
        dup = [i for i in (0, 1) if len(p.in_flight(i)) == 2][0]
        self.assertEqual(p.pick("c", 1), [1 - dup])

    def test_release_makes_piece_pickable_again(self):
        p = picker(1)
        p.peer_bitfield("a", Bitfield(1, [0]))
        p.peer_bitfield("b", Bitfield(1, [0]))
        p.pick("a")
        p.release("a", 0)
        self.assertEqual(p.pick("b"), [0])


class SwarmSimulationTest(unittest.TestCase):
    def test_rare_piece_gets_spread_first(self):
        """A leecher with two sources downloads the single-copy piece before common ones."""
        n = 20
        p = picker(n)
        p.peer_bitfield("seed", Bitfield(n, range(n)))
        p.peer_bitfield("peer", Bitfield(n, range(n - 1)))  # piece 19 only on seed
        order = []
        while not p.own.is_complete():
            got = p.pick("seed", 1) or p.pick("peer", 1)
            order.extend(got)
            for i in got:
                p.piece_done(i)
        self.assertEqual(order[0], n - 1)
        self.assertEqual(sorted(order), list(range(n)))


if __name__ == "__main__":
    unittest.main()
