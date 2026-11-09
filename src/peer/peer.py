"""B4/B5/B6/B7 – real peer: parallel rarest-first download, upload, failover, CLI.

Wire: Contract 3 framing (4-byte length + 1-byte id) with the standard 68-byte
handshake from ``protocol`` (A). Storage/bitfield (B2) verify every piece,
PiecePicker (B3) chooses rarest-first with endgame, Downloader (B4) fetches
from several peers in parallel, the server side (B5) serves with choke slots
and a token-bucket throttle, and failures fall over to other sources (B6).
CLI and exit codes follow Contract 4, logs follow Contract 5 (B7).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import random
import signal
import socket
import struct
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
from pathlib import Path

# Make `src/` importable both as `-m peer` (PYTHONPATH has src/) and as
# `python src/peer/peer.py`.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from peer.downloader import Downloader, RemotePeer
    from peer.session import CHOKED, DONE, PeerSession, PieceTransfer
    from peer.uploader import FrameWriter, TokenBucket, corrupt_block, parse_corrupt_spec
    from peer.wire import (
        SOCKET_TIMEOUT,
        WireError,
        decode_bitfield,
        do_handshake,
        encode_bitfield,
        node_from_wire,
        recv_msg,
        send_msg,
        wire_peer_id,
    )
except ImportError:  # pragma: no cover
    from src.peer.downloader import Downloader, RemotePeer  # type: ignore[no-redef]
    from src.peer.session import CHOKED, DONE, PeerSession, PieceTransfer  # type: ignore[no-redef]
    from src.peer.uploader import FrameWriter, TokenBucket, corrupt_block, parse_corrupt_spec  # type: ignore[no-redef]
    from src.peer.wire import (  # type: ignore[no-redef]
        SOCKET_TIMEOUT,
        WireError,
        decode_bitfield,
        do_handshake,
        encode_bitfield,
        node_from_wire,
        recv_msg,
        send_msg,
        wire_peer_id,
    )

try:
    from torrent import MetainfoError, load_metainfo
except ImportError:  # pragma: no cover
    from src.torrent import MetainfoError, load_metainfo  # type: ignore[no-redef]

try:
    from storage import Bitfield, PieceHashError, PieceStorage
except ImportError:  # pragma: no cover
    from src.storage import Bitfield, PieceHashError, PieceStorage  # type: ignore[no-redef]

try:
    from piece_picker import PiecePicker
except ImportError:  # pragma: no cover
    from src.piece_picker import PiecePicker  # type: ignore[no-redef]

try:
    from protocol.messages import (
        Bitfield as MsgBitfield,
        Choke,
        Have,
        Interested,
        KeepAlive,
        Piece,
        Request,
        Unchoke,
    )
except ImportError:  # pragma: no cover
    from src.protocol.messages import (  # type: ignore[no-redef]
        Bitfield as MsgBitfield,
        Choke,
        Have,
        Interested,
        KeepAlive,
        Piece,
        Request,
        Unchoke,
    )

STOP_FILE_NAME = ".sim_stop"
PEER_RETRY_SEC = 2.0  # wait before reconnecting to a peer that failed
KEEPALIVE_SEC = 2.0  # must stay well below SOCKET_TIMEOUT
STARVED_ANNOUNCE_GAP_SEC = 2.0  # re-announce rate while no source can help
DEFAULT_GIVE_UP_SEC = 90.0  # exit 3 after this long without any usable source


# ---------------------------------------------------------------- logger (Contract 5)


class EventLogger:
    def __init__(self, peer_id: str, log_path: str | None = None):
        self.peer_id = peer_id
        self._lock = threading.Lock()
        self._file = open(log_path, "a", encoding="utf-8") if log_path else None

    def emit(self, event: str, chunk_index: int | None = None, **meta) -> dict:
        rec = {
            "ts": time.time(),
            "peer_id": self.peer_id,
            "event": event,
            "chunk_index": chunk_index,
            "meta": dict(meta),
        }
        line = json.dumps(rec, ensure_ascii=False)
        with self._lock:
            try:
                if self._file:
                    self._file.write(line + "\n")
                    self._file.flush()
                else:
                    print(line, flush=True)
            except (OSError, ValueError):
                pass
        return rec

    def close(self) -> None:
        if self._file:
            with self._lock:
                try:
                    self._file.close()
                except OSError:
                    pass
                self._file = None


def _configure_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (OSError, ValueError):
                pass


# ---------------------------------------------------------------- peer node


class PeerNode:
    def __init__(self, args: argparse.Namespace, meta, logger: EventLogger):
        self.args = args
        self.meta = meta
        self.log = logger
        self.peer_id = logger.peer_id
        self.role = args.role
        self.dir = Path(args.dir)
        self.n = meta.num_pieces
        self.info_hash_hex = meta.info_hash
        self.info_hash = bytes.fromhex(meta.info_hash)
        self.wire_id = wire_peer_id(self.peer_id)
        self.rng = random.Random(f"{self.peer_id}:{args.port}")
        self.bucket = TokenBucket(args.max_up_kbps)
        self.corrupt = parse_corrupt_spec(getattr(args, "corrupt_pieces", None), self.n)
        self.latency_ms = int(getattr(args, "latency_ms", 0) or 0)
        self.upload_slots = threading.BoundedSemaphore(max(1, int(getattr(args, "upload_slots", 4) or 4)))

        self.storage = PieceStorage(meta, str(self.dir))
        self.picker = PiecePicker(self.storage.bitfield(), rng=random.Random(f"pick:{self.peer_id}"))
        self.dl = Downloader(self.picker, max_parallel=int(getattr(args, "max_parallel", 4) or 4),
                             rng=random.Random(f"dl:{self.peer_id}"))

        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.complete_event = threading.Event()
        self.stop_reason = "graceful_shutdown"
        self.start_ts = time.time()
        self.interval = 2.0
        self.server: socket.socket | None = None
        self.uploaded_bytes = 0
        self.downloaded_bytes = 0
        self.clients: dict[socket.socket, FrameWriter] = {}
        self.have_queue: queue.Queue[int] = queue.Queue()
        self._completed_lock = threading.Lock()
        self.completed_announced = self.role == "seed"

    # ---------------------------------------------------------- seed check
    def seed_check(self) -> int:
        if self.storage.is_complete():
            return 0
        missing = self.storage.missing_pieces()
        self.log.emit("stopped", None, reason="seed_hash_mismatch", missing=missing[:8])
        return 2

    def open_server(self) -> bool:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if os.name != "nt":
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind((self.args.bind, self.args.port))
        except OSError as exc:
            self.log.emit("stopped", None, reason="bind_failed", port=self.args.port, error=str(exc))
            srv.close()
            return False
        srv.listen(64)
        srv.settimeout(0.3)
        self.server = srv
        return True

    # ---------------------------------------------------------- server (B5)
    def serve_forever(self) -> None:
        assert self.server is not None
        while not self.stop_event.is_set():
            try:
                conn, _ = self.server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self.handle_conn, args=(conn,), daemon=True).start()

    def _can_serve(self, index: int, begin: int, length: int) -> bool:
        if not 0 <= index < self.n or begin < 0 or length <= 0 or length > 256 * 1024:
            return False
        if begin + length > self.meta.piece_size(index):
            return False
        with self.lock:
            return self.storage.has_piece(index)

    def _end_upload(self, index: int, holding: set[int], sent: dict[int, int]) -> None:
        sent.pop(index, None)
        holding.discard(index)
        try:
            self.upload_slots.release()
        except ValueError:
            pass

    def handle_conn(self, conn: socket.socket) -> None:
        conn.settimeout(SOCKET_TIMEOUT)
        remote = "?"
        holding: set[int] = set()
        sent: dict[int, int] = {}
        out = FrameWriter(conn)
        try:
            try:
                remote_wire = do_handshake(conn, self.info_hash, self.wire_id, client=False)
            except WireError:
                return
            remote = node_from_wire(remote_wire)
            if self.latency_ms:
                time.sleep(self.latency_ms / 1000.0)
            # Under the lock: a piece stored right after this snapshot must
            # find the client registered, and `have` must follow the bitfield.
            with self.lock:
                have = [self.storage.has_piece(i) for i in range(self.n)]
                out.send(MsgBitfield(bitfield=encode_bitfield(have)))
                self.clients[conn] = out
            out.send(Unchoke())
            while not self.stop_event.is_set():
                try:
                    msg = recv_msg(conn)
                except WireError:
                    break
                if isinstance(msg, KeepAlive):
                    out.send(KeepAlive())
                    continue
                if not isinstance(msg, Request):
                    continue  # interested / have: nothing to do
                index, begin, length = msg.index, msg.begin, msg.length
                # A piece is refused once, at its first block; the client's
                # remaining pipelined blocks for it are dropped silently.
                if not self._can_serve(index, begin, length):
                    if begin == 0:
                        out.send(Choke())
                    continue
                size = self.meta.piece_size(index)
                if begin == 0:
                    if index not in holding:
                        if not self.upload_slots.acquire(blocking=False):
                            out.send(Choke())
                            continue
                        holding.add(index)
                    if self.latency_ms:
                        time.sleep(self.latency_ms / 1000.0)
                elif index not in holding:
                    continue
                try:
                    block = self.storage.read_block(index, begin, length)
                except Exception:
                    self._end_upload(index, holding, sent)
                    out.send(Choke())
                    continue
                if index in self.corrupt:
                    block = corrupt_block(block)
                self.bucket.consume(len(block))
                if self.stop_event.is_set():
                    break
                out.send(Piece(index=index, begin=begin, block=block))
                sent[index] = sent.get(index, 0) + len(block)
                if sent[index] >= size:
                    self._end_upload(index, holding, sent)
                    with self.lock:
                        self.uploaded_bytes += size
                        have_count = self.storage.bitfield().count()
                    self.log.emit("piece_uploaded", index, bytes=size, to_peer=remote,
                                  have_count=have_count, total_pieces=self.n,
                                  corrupted=index in self.corrupt)
        except (OSError, WireError, struct.error):
            pass
        finally:
            with self.lock:
                self.clients.pop(conn, None)
            for _ in holding:
                try:
                    self.upload_slots.release()
                except ValueError:
                    pass
            try:
                conn.close()
            except OSError:
                pass

    def have_broadcast_loop(self) -> None:
        """Tell connected peers about new pieces; a stuck client delays only this thread."""
        while not self.stop_event.is_set():
            try:
                index = self.have_queue.get(timeout=0.25)
            except queue.Empty:
                continue
            with self.lock:
                writers = list(self.clients.values())
            for out in writers:
                try:
                    out.send(Have(piece_index=index))
                except OSError:
                    pass  # that connection's handler cleans up

    # ---------------------------------------------------------- tracker
    def announce(self, event: str = "") -> bool:
        if not self.meta.announce:
            return False
        left = self.storage.bytes_left()
        params = {
            "info_hash": self.info_hash_hex,
            "peer_id": self.peer_id,
            "ip": self.args.ip,
            "port": str(self.args.port),
            "event": event,
            "left": str(left),
        }
        url = self.meta.announce + "?" + urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(url, timeout=1.5) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            self.log.emit("announce_failed", None, announce_event=event or "periodic",
                          error=type(exc).__name__)
            return False
        try:
            self.interval = float(body.get("interval", self.interval) or self.interval)
        except (TypeError, ValueError):
            pass
        peers = body.get("peers", []) or []
        added = 0
        for p in peers:
            try:
                pid = str(p.get("peer_id"))
                ip = str(p.get("ip", "127.0.0.1"))
                port = int(p.get("port"))
            except (TypeError, ValueError, AttributeError):
                continue
            if pid == self.peer_id:
                continue
            with self.lock:
                if pid not in self.dl.peers:
                    added += 1
                self.dl.add_or_update(pid, ip, port)
        self.log.emit("announce_ok", None, announce_event=event or "periodic",
                      peers=len(peers), new_peers=added, interval=self.interval, left=left)
        return True

    def _announce_completed(self) -> bool:
        """Send `completed` at most once, whichever thread gets here first."""
        with self._completed_lock:
            if self.completed_announced:
                return True
            if self.stop_event.is_set():  # `stopped` may already be on its way
                return False
            self.completed_announced = self.announce("completed")
            return self.completed_announced

    def _starved(self) -> bool:
        if self.role != "leech" or self.complete_event.is_set():
            return False
        with self.lock:
            return self.dl.starved()

    def announcer_loop(self) -> None:
        backoff = 0.5
        started_sent = False
        next_at = 0.0
        last_ok_at = None  # None while the tracker is failing: keep the backoff
        while not self.stop_event.is_set():
            now = time.time()
            if self.complete_event.is_set() and not self.completed_announced:
                next_at = min(next_at, now)
            elif last_ok_at is not None and self._starved():
                next_at = min(next_at, last_ok_at + STARVED_ANNOUNCE_GAP_SEC)  # B6
            if now >= next_at:
                if not started_sent:
                    ok = started_sent = self.announce("started")
                elif self.complete_event.is_set() and not self.completed_announced:
                    ok = self._announce_completed()
                else:
                    ok = self.announce("")
                if ok:
                    backoff = 0.5
                    last_ok_at = time.time()
                    next_at = last_ok_at + self.interval
                else:
                    last_ok_at = None
                    next_at = time.time() + backoff
                    backoff = min(backoff * 2, max(self.interval, 1.0))
            self.stop_event.wait(0.1)

    # ---------------------------------------------------------- client (B4+B6)
    def _connect(self, rp: RemotePeer) -> tuple[socket.socket, set[int]]:
        sock = socket.create_connection((rp.ip, rp.port), timeout=2.0)
        sock.settimeout(SOCKET_TIMEOUT)
        try:
            do_handshake(sock, self.info_hash, self.wire_id, client=True)
            first = recv_msg(sock)
            if not isinstance(first, MsgBitfield):
                raise WireError("expected bitfield")
            return sock, decode_bitfield(first.bitfield, self.n)
        except Exception:
            try:
                sock.close()
            except OSError:
                pass
            raise

    def _mark_lost(self, rp: RemotePeer, reason: str, during: str, chunk: int | None = None) -> None:
        with self.lock:
            rp.dead_until = time.time() + PEER_RETRY_SEC
            rp.bitfield = None
            # Not peer_gone(): running workers still hold their reservations
            # and release them themselves.
            self.picker.peer_bitfield(rp.peer_id, Bitfield(self.n))
            should_log = rp.ever_connected and not rp.lost_logged and not rp.banned
            if should_log:
                rp.lost_logged = True
        if should_log:
            self.log.emit("peer_lost", chunk, peer=rp.peer_id, reason=reason, during=during)

    def _downloading(self) -> bool:
        return not self.stop_event.is_set() and not self.complete_event.is_set()

    def _record_have(self, rp: RemotePeer, index: int) -> None:
        if not 0 <= index < self.n:
            return
        with self.lock:
            if rp.bitfield is not None:
                rp.bitfield.add(index)
                self.picker.peer_have(rp.peer_id, index)

    def session_loop(self, rp: RemotePeer) -> None:
        """Connect to one peer and read from it until the connection ends."""
        session = None
        failure = "closed"
        try:
            sock, bf = self._connect(rp)
            session = PeerSession(sock)
            with self.lock:
                rp.session, rp.bitfield = session, bf
                rp.ever_connected, rp.lost_logged = True, False
                self.picker.peer_bitfield(rp.peer_id, Bitfield(self.n, have=sorted(bf)))
            session.send(Interested())
            while self._downloading():
                msg = recv_msg(sock)
                if isinstance(msg, Piece):
                    session.on_piece(msg.index, msg.begin, msg.block)
                elif isinstance(msg, Have):
                    self._record_have(rp, msg.piece_index)
                elif isinstance(msg, Choke):
                    session.on_choke()
        except Exception as exc:  # any failure here only costs this one peer
            failure = type(exc).__name__
        finally:
            if self._downloading():
                chunk = session.oldest_index() if session else None
                self._mark_lost(rp, failure, "transfer" if session else "connect", chunk)
            if session:
                session.close()
            with self.lock:
                rp.session = None
                rp.session_active = False

    def session_manager_loop(self) -> None:
        """Open a connection per reachable peer; keep idle ones alive, drop stuck ones."""
        while self._downloading():
            now = time.time()
            with self.lock:
                targets = [rp for rp in self.dl.peers.values()
                           if not rp.banned and not rp.session_active and rp.dead_until <= now]
                for rp in targets:
                    rp.session_active = True
                live = [rp.session for rp in self.dl.peers.values() if rp.session]
            for rp in targets:
                threading.Thread(target=self.session_loop, args=(rp,), daemon=True).start()
            for session in live:
                if session.stalled(SOCKET_TIMEOUT):
                    session.close()
                else:
                    session.keepalive_if_idle(KEEPALIVE_SEC)
            self.stop_event.wait(0.25)
        with self.lock:
            live = [rp.session for rp in self.dl.peers.values() if rp.session]
        for session in live:
            session.close()

    def pick_work(self) -> tuple[int, RemotePeer] | None:
        with self.lock:
            work = self.dl.pick_work()
            if work is None:
                return None
            index, src = work
            prev = self.dl.last_failure.pop(index, None)
        if prev and prev[0] != src.peer_id and prev[1] in ("peer_lost", "invalid_chunk"):
            self.log.emit("switch_source", index, old_peer=prev[0], new_peer=src.peer_id, reason=prev[1])
        return index, src

    def fetch_piece(self, index: int, rp: RemotePeer) -> None:
        size = self.meta.piece_size(index)
        t0 = time.time()
        session = rp.session
        if session is None:
            return
        transfer = PieceTransfer(index, size)
        try:
            session.request(transfer)
        except OSError as exc:
            with self.lock:
                self.dl.last_failure[index] = (rp.peer_id, "peer_lost")
            self._mark_lost(rp, type(exc).__name__, "transfer", index)
            session.close()
            return
        while not transfer.finished.wait(0.25):
            if self.stop_event.is_set():
                return
        if transfer.outcome == CHOKED:
            with self.lock:
                rp.busy_until = time.time() + 0.25 + self.rng.random() * 0.25
            return
        if transfer.outcome != DONE:
            with self.lock:
                self.dl.last_failure[index] = (rp.peer_id, "peer_lost")
            return
        piece = bytes(transfer.buf)
        if hashlib.sha1(piece).hexdigest() != self.meta.pieces[index]:
            with self.lock:
                self.dl.bad_sources.setdefault(index, set()).add(rp.peer_id)
                self.dl.last_failure[index] = (rp.peer_id, "invalid_chunk")
                rp.invalid_count += 1
                if rp.invalid_count >= 2:
                    rp.banned = True
                banned = rp.banned
            self.log.emit("invalid_chunk", index, from_peer=rp.peer_id,
                          reason="sha1_mismatch", bytes=size, banned=banned)
            if banned:
                session.close()
            return
        with self.lock:
            if self.storage.has_piece(index):
                return
            try:
                stored = self.storage.write_piece(index, piece)
            except PieceHashError:
                self.dl.bad_sources.setdefault(index, set()).add(rp.peer_id)
                self.dl.last_failure[index] = (rp.peer_id, "invalid_chunk")
                self.log.emit("invalid_chunk", index, from_peer=rp.peer_id,
                              reason="sha1_mismatch", bytes=size, banned=False)
                return
            if not stored:
                return
            self.downloaded_bytes += size
            self.dl.sources_used.add(rp.peer_id)
            have_count = self.storage.bitfield().count()
            self.dl.piece_done(index)
        self.have_queue.put(index)
        self.log.emit("piece_done", index, bytes=size, from_peer=rp.peer_id,
                      elapsed_ms=round((time.time() - t0) * 1000, 1),
                      have_count=have_count, total_pieces=self.n)
        if have_count == self.n:
            self.finish_download()

    def worker_loop(self) -> None:
        while not self.stop_event.is_set() and not self.complete_event.is_set():
            work = self.pick_work()
            if work is None:
                self.stop_event.wait(0.1)
                continue
            index, rp = work
            try:
                self.fetch_piece(index, rp)
            except Exception:  # e.g. disk full: keep the worker, report on stderr
                traceback.print_exc()
                self.stop_event.wait(0.1)
            finally:
                with self.lock:
                    # fetch consumed one pipeline slot; release unless piece_done cleaned it
                    holders = self.dl.inflight.get(index)
                    if holders is not None and rp.peer_id in holders:
                        self.dl.release(rp.peer_id, index)

    def finish_download(self) -> None:
        with self.lock:
            if self.complete_event.is_set():
                return
            self.complete_event.set()
        try:
            data = b"".join(self.storage.read_piece(i) for i in range(self.n))
        except Exception as exc:
            if not self.stop_event.is_set():  # shutdown closes storage; not a real failure
                print(f"readback failed: {exc}", file=sys.stderr, flush=True)
                self.stop_reason = "readback_failed"
                self.stop_event.set()
            return
        self.log.emit("completed", None, total_pieces=self.n, bytes=len(data),
                      sha256=hashlib.sha256(data).hexdigest(),
                      download_time_sec=round(time.time() - self.start_ts, 3),
                      sources=sorted(self.dl.sources_used), path=str(self.dir / self.meta.name))
        if self.args.exit_on_complete:
            self.stop_reason = "completed"
            self._announce_completed()
            self.stop_event.set()

    # ---------------------------------------------------------- lifecycle
    def install_signal_handlers(self) -> None:
        def _on_signal(signum, _frame):
            self.stop_reason = "signal"
            self.stop_event.set()

        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            sig = getattr(signal, name, None)
            if sig is not None:
                try:
                    signal.signal(sig, _on_signal)
                except (OSError, ValueError):
                    pass

    def run(self) -> int:
        self.dir.mkdir(parents=True, exist_ok=True)
        stop_file = self.dir / STOP_FILE_NAME
        try:
            stop_file.unlink()
        except OSError:
            pass
        if self.role == "seed":
            code = self.seed_check()
            if code:
                return code
            self.complete_event.set()
        if not self.open_server():
            return 1
        self.log.emit("started", None, role=self.role, port=self.args.port,
                      max_up_kbps=self.args.max_up_kbps, num_pieces=self.n,
                      piece_length=self.meta.piece_length, info_hash=self.info_hash_hex,
                      implementation="peer-b")
        announcer = threading.Thread(target=self.announcer_loop, daemon=True)
        threads = [threading.Thread(target=self.serve_forever, daemon=True), announcer]
        if self.role == "leech":
            threads.append(threading.Thread(target=self.session_manager_loop, daemon=True))
            threads.append(threading.Thread(target=self.have_broadcast_loop, daemon=True))
            for _ in range(max(1, int(getattr(self.args, "max_parallel", 4) or 4))):
                threads.append(threading.Thread(target=self.worker_loop, daemon=True))
        for t in threads:
            t.start()
        if self.role == "leech" and self.storage.is_complete():
            self.finish_download()  # restarted with the whole file already on disk
        give_up = float(getattr(self.args, "give_up_sec", 0.0) or 0.0)
        starved_since = None
        while not self.stop_event.is_set():
            if stop_file.exists():
                self.stop_reason = "stop_file"
                break
            if give_up:
                now = time.time()
                starved_since = (starved_since or now) if self._starved() else None
                if starved_since and now - starved_since > give_up:
                    self.stop_reason = "gave_up"
                    break
            self.stop_event.wait(0.15)
        self.stop_event.set()
        completed = self.complete_event.is_set()
        announcer.join(2.0)  # a late periodic announce would re-register us after `stopped`
        try:
            self.announce("stopped")
        except Exception:
            pass
        try:
            if self.server:
                self.server.close()
        except OSError:
            pass
        with self.lock:
            have_count = self.storage.bitfield().count()
        try:
            self.storage.close()
        except Exception:
            pass
        self.log.emit("stopped", None, reason=self.stop_reason, have_count=have_count,
                      total_pieces=self.n, uploaded_bytes=self.uploaded_bytes,
                      downloaded_bytes=self.downloaded_bytes)
        if self.stop_reason in ("gave_up", "readback_failed"):
            return 3
        if self.role == "leech" and not completed:
            return 130
        return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DS02 peer (Contract 4, Role B)")
    p.add_argument("--torrent", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--dir", required=True)
    p.add_argument("--role", choices=["seed", "leech"], required=True)
    p.add_argument("--max-up-kbps", type=int, default=0)
    p.add_argument("--peer-id", default=None)
    p.add_argument("--log", default=None)
    p.add_argument("--exit-on-complete", action="store_true")
    # simulation-only / compat flags (accepted so launcher can pass them)
    p.add_argument("--corrupt-pieces", default=None)
    p.add_argument("--latency-ms", type=int, default=0)
    p.add_argument("--ip", default="127.0.0.1")
    p.add_argument("--bind", default="127.0.0.1")
    p.add_argument("--max-parallel", type=int, default=4)
    p.add_argument("--give-up-sec", type=float, default=DEFAULT_GIVE_UP_SEC,
                   help="leech exits 3 after this long with no usable source (0 = never)")
    p.add_argument("--upload-slots", type=int, default=4)
    p.add_argument("--data-seed", default="ds02")
    return p.parse_args(argv)


def _load_meta(path: str):
    try:
        return load_metainfo(path)
    except MetainfoError:
        raise
    except Exception as exc:
        raise MetainfoError(str(exc)) from exc


def main(argv: list[str] | None = None) -> int:
    _configure_utf8()
    args = parse_args(argv)
    peer_id = args.peer_id or f"{args.role}-{args.port}"
    logger = EventLogger(peer_id, args.log)
    try:
        meta = _load_meta(args.torrent)
    except MetainfoError as exc:
        logger.emit("stopped", None, reason="invalid_metainfo", error=str(exc))
        logger.close()
        return 1
    # Empty-file torrents: nothing to transfer.
    node = PeerNode(args, meta, logger)
    node.install_signal_handlers()
    try:
        if meta.num_pieces == 0:
            node.log.emit("started", None, role=args.role, port=args.port,
                          max_up_kbps=args.max_up_kbps, num_pieces=0,
                          piece_length=meta.piece_length, info_hash=meta.info_hash,
                          implementation="peer-b")
            node.announce("started")
            if args.role == "leech":
                node.announce("completed")
                node.log.emit("completed", None, total_pieces=0, bytes=0,
                              sha256=hashlib.sha256(b"").hexdigest(),
                              download_time_sec=0.0, sources=[], path=str(Path(args.dir) / meta.name))
            node.announce("stopped")
            node.log.emit("stopped", None, reason="completed" if args.role == "leech" and args.exit_on_complete else "graceful_shutdown",
                          have_count=0, total_pieces=0, uploaded_bytes=0, downloaded_bytes=0)
            return 0
        return node.run()
    finally:
        logger.close()


if __name__ == "__main__":
    sys.exit(main())
