"""StubPeer – a small but *real* BitTorrent-like peer used by Role C.

It honours Contract 4 exactly, so the simulator can later swap it for Role B's
real ``peer`` without any change:

    py src/sim/stub_peer.py --torrent PATH --port PORT --dir DIR --role {seed,leech}
                            [--max-up-kbps N] [--peer-id ID] [--log PATH] [--exit-on-complete]

Optional simulation-only flags (ignored by the contract, never required):

    --latency-ms N         extra delay before answering each piece request / handshake
    --corrupt-pieces SPEC  serve corrupted data for these pieces ("all" or "0,3,5")
    --ip ADDR              address announced to the tracker (default 127.0.0.1)
    --bind ADDR            listen address (default 127.0.0.1: no firewall prompt on Windows)
    --max-parallel N       concurrent piece downloads (default 4)
    --give-up-sec S        leech exits with code 3 if not complete after S seconds
    --upload-slots N       concurrent piece uploads; extra requests are choked (default 4)
    --data-seed STR        seed of the deterministic content generator

What it really does (no fake sleeps):
    * reads B's nested metainfo or the flat dummy format (``common.load_metainfo_any``),
    * a seed loads ``DIR/<name>`` (or generates the deterministic file if missing)
      and verifies every SHA-1 before serving (exit 2 on mismatch),
    * announces to the tracker over HTTP (Contract 2), re-announces every
      ``interval``, retries with back-off while the tracker is down,
    * talks to other peers over TCP with Contract-3-style framing
      (4-byte length + 1-byte type): handshake, bitfield, unchoke, interested,
      request(index, begin, length), piece, keepalive,
    * downloads rarest-first from several peers in parallel, verifies SHA-1 per
      piece (``invalid_chunk`` + ban after 2 bad pieces), fails over when a source
      dies (``peer_lost`` then ``switch_source``),
    * serves pieces it already has *before* finishing (``piece_uploaded``),
    * throttles total upload to ``--max-up-kbps`` KiB/s with a token bucket,
    * writes ``DIR/<name>`` atomically when complete and logs ``completed`` with
      its SHA-256.

Exit codes (Contract 4): 0 ok, 1 bad args/metainfo/port, 2 seed data mismatch,
3 could not finish (``--give-up-sec``), 130 leech interrupted before completion.

Graceful stop: SIGINT / SIGTERM / SIGBREAK (Windows) or the file ``DIR/.sim_stop``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import signal
import socket
import struct
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.sim.common import (  # noqa: E402
    DATA_SEED_DEFAULT,
    EventLogger,
    MetainfoFormatError,
    TorrentInfo,
    configure_utf8_stdio,
    deterministic_bytes,
    load_metainfo_any,
)

configure_utf8_stdio()

# --------------------------------------------------------------------------- wire

MSG_CHOKE, MSG_UNCHOKE, MSG_INTERESTED, MSG_NOT_INTERESTED = 0, 1, 2, 3
MSG_HAVE, MSG_BITFIELD, MSG_REQUEST, MSG_PIECE = 4, 5, 6, 7
MSG_HANDSHAKE = 20  # stub-only: payload = 20-byte info_hash + utf-8 peer_id
BLOCK_SIZE = 16 * 1024
SOCKET_TIMEOUT = 8.0
STOP_FILE_NAME = ".sim_stop"


class WireError(Exception):
    pass


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise WireError("connection closed by peer")
        buf.extend(chunk)
    return bytes(buf)


def send_msg(sock: socket.socket, mtype: int | None, payload: bytes = b"") -> None:
    if mtype is None:  # keepalive
        sock.sendall(struct.pack(">I", 0))
        return
    sock.sendall(struct.pack(">IB", 1 + len(payload), mtype) + payload)


def recv_msg(sock: socket.socket) -> tuple[int | None, bytes]:
    (length,) = struct.unpack(">I", recv_exact(sock, 4))
    if length == 0:
        return None, b""
    if length > 4 * 1024 * 1024:
        raise WireError(f"frame too large ({length})")
    body = recv_exact(sock, length)
    return body[0], body[1:]


def encode_bitfield(have: list[bool]) -> bytes:
    out = bytearray((len(have) + 7) // 8)
    for i, h in enumerate(have):
        if h:
            out[i // 8] |= 0x80 >> (i % 8)
    return bytes(out)


def decode_bitfield(data: bytes, n: int) -> set[int]:
    return {i for i in range(n) if i // 8 < len(data) and data[i // 8] & (0x80 >> (i % 8))}


# ----------------------------------------------------------------------- helpers


class TokenBucket:
    """Caps total upload bandwidth of one peer (shared by all connections)."""

    def __init__(self, kib_per_sec: int):
        self.rate = float(kib_per_sec) * 1024.0
        self._lock = threading.Lock()
        self._next_free = time.monotonic()

    def consume(self, nbytes: int) -> None:
        if self.rate <= 0:
            return
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next_free)
            self._next_free = start + nbytes / self.rate
            wait = self._next_free - now
        if wait > 0:
            time.sleep(wait)


@dataclass
class RemotePeer:
    peer_id: str
    ip: str
    port: int
    bitfield: set[int] | None = None
    last_poll: float = 0.0
    dead_until: float = 0.0
    ever_connected: bool = False
    lost_logged: bool = False
    invalid_count: int = 0
    banned: bool = False
    busy_until: float = 0.0


def parse_corrupt_spec(spec: str | None, n: int) -> set[int]:
    if not spec:
        return set()
    if spec.strip().lower() == "all":
        return set(range(n))
    out = set()
    for part in spec.split(","):
        part = part.strip()
        if part:
            idx = int(part)
            if 0 <= idx < n:
                out.add(idx)
    return out


# -------------------------------------------------------------------------- peer


class StubPeer:
    def __init__(self, args: argparse.Namespace, meta: TorrentInfo, logger: EventLogger):
        self.args = args
        self.meta = meta
        self.log = logger
        self.peer_id = logger.peer_id
        self.role = args.role
        self.dir = Path(args.dir)
        self.final_path = self.dir / meta.name
        self.n = meta.num_pieces
        self.rng = random.Random(f"{self.peer_id}:{args.port}")
        self.bucket = TokenBucket(args.max_up_kbps)
        self.corrupt = parse_corrupt_spec(args.corrupt_pieces, self.n)
        self.upload_slots = threading.BoundedSemaphore(max(1, args.upload_slots))

        self.lock = threading.RLock()
        self.have = [False] * self.n
        self.data: dict[int, bytes] = {}
        self.peers: dict[str, RemotePeer] = {}
        self.inflight: dict[int, str] = {}
        self.inflight_per_peer: dict[str, int] = {}
        self.bad_sources: dict[int, set[str]] = {}
        self.last_failure: dict[int, tuple[str, str]] = {}
        self.sources_used: set[str] = set()

        self.stop_event = threading.Event()
        self.complete_event = threading.Event()
        self.stop_reason = "graceful_shutdown"
        self.start_ts = time.time()
        self.interval = 2.0
        self.server: socket.socket | None = None
        self.uploaded_bytes = 0
        self.downloaded_bytes = 0

    # ------------------------------------------------------------- setup
    def load_seed_data(self) -> int:
        """Return 0 on success or the Contract 4 exit code."""
        if not self.final_path.exists():
            self.dir.mkdir(parents=True, exist_ok=True)
            blob = deterministic_bytes(self.meta.name, self.meta.length, self.args.data_seed)
            tmp = self.final_path.with_name(self.final_path.name + ".tmp")
            tmp.write_bytes(blob)
            os.replace(tmp, self.final_path)
        blob = self.final_path.read_bytes()
        if len(blob) != self.meta.length:
            self.log.emit("stopped", None, reason="seed_size_mismatch", expected=self.meta.length, actual=len(blob))
            return 2
        for i in range(self.n):
            start = i * self.meta.piece_length
            piece = blob[start : start + self.meta.piece_size(i)]
            if hashlib.sha1(piece).hexdigest() != self.meta.pieces[i]:
                self.log.emit("stopped", i, reason="seed_hash_mismatch")
                return 2
            self.data[i] = piece
            self.have[i] = True
        return 0

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

    # ------------------------------------------------------------ server side
    def serve_forever(self) -> None:
        assert self.server is not None
        while not self.stop_event.is_set():
            try:
                conn, addr = self.server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self.handle_conn, args=(conn,), daemon=True).start()

    def handle_conn(self, conn: socket.socket) -> None:
        conn.settimeout(SOCKET_TIMEOUT)
        remote = "?"
        sent_per_piece: dict[int, int] = {}
        holding: set[int] = set()
        try:
            mtype, payload = recv_msg(conn)
            if mtype != MSG_HANDSHAKE or payload[:20] != bytes.fromhex(self.meta.info_hash):
                return
            remote = payload[20:].decode("utf-8", "replace") or "?"
            if self.args.latency_ms:
                time.sleep(self.args.latency_ms / 1000.0)
            send_msg(conn, MSG_HANDSHAKE, bytes.fromhex(self.meta.info_hash) + self.peer_id.encode("utf-8"))
            with self.lock:
                bf = encode_bitfield(self.have)
            send_msg(conn, MSG_BITFIELD, bf)
            send_msg(conn, MSG_UNCHOKE)
            while not self.stop_event.is_set():
                mtype, payload = recv_msg(conn)
                if mtype != MSG_REQUEST:
                    continue  # keepalive / interested / have: nothing to do
                index, begin, length = struct.unpack(">III", payload[:12])
                with self.lock:
                    piece = self.data.get(index) if 0 <= index < self.n and self.have[index] else None
                if piece is None or begin + length > len(piece):
                    send_msg(conn, MSG_CHOKE)
                    continue
                if begin == 0:
                    if index not in holding:
                        if not self.upload_slots.acquire(blocking=False):
                            send_msg(conn, MSG_CHOKE)  # all upload slots busy (choke/unchoke)
                            continue
                        holding.add(index)
                    if self.args.latency_ms:
                        time.sleep(self.args.latency_ms / 1000.0)
                elif index not in holding:
                    continue  # rest of a piece whose first block was choked
                block = piece[begin : begin + length]
                if index in self.corrupt:
                    block = bytes(b ^ 0xA5 for b in block[:64]) + block[64:]
                self.bucket.consume(len(block))
                if self.stop_event.is_set():
                    break
                send_msg(conn, MSG_PIECE, struct.pack(">II", index, begin) + block)
                sent_per_piece[index] = sent_per_piece.get(index, 0) + len(block)
                if sent_per_piece[index] >= len(piece):
                    sent_per_piece.pop(index, None)
                    holding.discard(index)
                    self.upload_slots.release()
                    with self.lock:
                        self.uploaded_bytes += len(piece)
                        have_count = sum(self.have)
                    self.log.emit(
                        "piece_uploaded",
                        index,
                        bytes=len(piece),
                        to_peer=remote,
                        have_count=have_count,
                        total_pieces=self.n,
                        corrupted=index in self.corrupt,
                    )
        except (OSError, WireError, struct.error):
            pass
        finally:
            for _ in holding:
                self.upload_slots.release()
            try:
                conn.close()
            except OSError:
                pass

    # ------------------------------------------------------------ tracker
    def announce(self, event: str = "") -> bool:
        if not self.meta.announce:
            return False
        with self.lock:
            left = sum(self.meta.piece_size(i) for i in range(self.n) if not self.have[i])
        params = {
            "info_hash": self.meta.info_hash,
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
        except (urllib.error.URLError, OSError, ValueError) as exc:
            self.log.emit("announce_failed", None, announce_event=event or "periodic", error=type(exc).__name__)
            return False
        self.interval = float(body.get("interval", self.interval) or self.interval)
        peers = body.get("peers", []) or []
        added = 0
        with self.lock:
            for p in peers:
                pid = str(p.get("peer_id"))
                if pid == self.peer_id:
                    continue
                rp = self.peers.get(pid)
                if rp is None:
                    self.peers[pid] = RemotePeer(pid, str(p.get("ip", "127.0.0.1")), int(p.get("port")))
                    added += 1
                else:
                    rp.ip, rp.port = str(p.get("ip", rp.ip)), int(p.get("port", rp.port))
        self.log.emit(
            "announce_ok",
            None,
            announce_event=event or "periodic",
            peers=len(peers),
            new_peers=added,
            interval=self.interval,
            left=left,
        )
        return True

    def announcer_loop(self) -> None:
        backoff = 0.5
        started_sent = False
        completed_sent = self.role == "seed"
        next_at = 0.0
        while not self.stop_event.is_set():
            now = time.time()
            if now >= next_at:
                if not started_sent:
                    ev = "started"
                elif self.complete_event.is_set() and not completed_sent:
                    ev = "completed"
                else:
                    ev = ""
                if self.announce(ev):
                    started_sent = True
                    if ev == "completed":
                        completed_sent = True
                    backoff = 0.5
                    next_at = time.time() + self.interval
                else:
                    next_at = time.time() + backoff
                    backoff = min(backoff * 2, max(self.interval, 1.0))
            if self.complete_event.is_set() and not completed_sent:
                next_at = min(next_at, time.time())
            self.stop_event.wait(0.1)

    # ------------------------------------------------------------ client side
    def _connect(self, rp: RemotePeer) -> tuple[socket.socket, set[int]]:
        sock = socket.create_connection((rp.ip, rp.port), timeout=2.0)
        sock.settimeout(SOCKET_TIMEOUT)
        try:
            send_msg(sock, MSG_HANDSHAKE, bytes.fromhex(self.meta.info_hash) + self.peer_id.encode("utf-8"))
            mtype, payload = recv_msg(sock)
            if mtype != MSG_HANDSHAKE or payload[:20] != bytes.fromhex(self.meta.info_hash):
                raise WireError("bad handshake")
            mtype, payload = recv_msg(sock)
            if mtype != MSG_BITFIELD:
                raise WireError("expected bitfield")
            return sock, decode_bitfield(payload, self.n)
        except Exception:
            sock.close()
            raise

    def _mark_lost(self, rp: RemotePeer, reason: str, during: str, chunk: int | None = None) -> None:
        with self.lock:
            rp.dead_until = time.time() + max(2.0, self.interval)
            rp.bitfield = None
            should_log = rp.ever_connected and not rp.lost_logged
            if should_log:
                rp.lost_logged = True
        if should_log:
            self.log.emit("peer_lost", chunk, peer=rp.peer_id, reason=reason, during=during)

    def poller_loop(self) -> None:
        while not self.stop_event.is_set() and not self.complete_event.is_set():
            with self.lock:
                now = time.time()
                targets = [
                    rp for rp in self.peers.values()
                    if not rp.banned and rp.dead_until <= now and now - rp.last_poll >= 0.6
                ]
            for rp in targets:
                if self.stop_event.is_set():
                    return
                try:
                    sock, bf = self._connect(rp)
                    sock.close()
                    with self.lock:
                        rp.bitfield, rp.last_poll = bf, time.time()
                        rp.ever_connected, rp.lost_logged = True, False
                except (OSError, WireError, struct.error) as exc:
                    with self.lock:
                        rp.last_poll = time.time()
                    self._mark_lost(rp, type(exc).__name__, "bitfield_poll")
            self.stop_event.wait(0.25)

    def pick_work(self) -> tuple[int, RemotePeer] | None:
        with self.lock:
            now = time.time()
            usable = [
                rp for rp in self.peers.values()
                if not rp.banned and rp.dead_until <= now and rp.busy_until <= now and rp.bitfield
            ]
            if not usable:
                return None
            candidates: list[tuple[int, float, int]] = []
            for i in range(self.n):
                if self.have[i] or i in self.inflight:
                    continue
                holders = [rp for rp in usable if i in rp.bitfield and rp.peer_id not in self.bad_sources.get(i, set())]
                free = [rp for rp in holders if self.inflight_per_peer.get(rp.peer_id, 0) < 2]
                if free:
                    candidates.append((len(holders), self.rng.random(), i))
            if not candidates:
                return None
            candidates.sort()
            _, _, index = candidates[0]
            holders = [
                rp for rp in usable
                if index in rp.bitfield
                and rp.peer_id not in self.bad_sources.get(index, set())
                and self.inflight_per_peer.get(rp.peer_id, 0) < 2
            ]
            holders.sort(key=lambda rp: (self.inflight_per_peer.get(rp.peer_id, 0), self.rng.random()))
            src = holders[0]
            self.inflight[index] = src.peer_id
            self.inflight_per_peer[src.peer_id] = self.inflight_per_peer.get(src.peer_id, 0) + 1
            previous = self.last_failure.pop(index, None)
        if previous and previous[0] != src.peer_id and previous[1] in ("peer_lost", "invalid_chunk"):
            self.log.emit("switch_source", index, old_peer=previous[0], new_peer=src.peer_id, reason=previous[1])
        return index, src

    def _release(self, index: int, peer_id: str) -> None:
        with self.lock:
            self.inflight.pop(index, None)
            self.inflight_per_peer[peer_id] = max(0, self.inflight_per_peer.get(peer_id, 1) - 1)

    def fetch_piece(self, index: int, rp: RemotePeer) -> None:
        size = self.meta.piece_size(index)
        t0 = time.time()
        try:
            sock, bf = self._connect(rp)
        except (OSError, WireError, struct.error) as exc:
            with self.lock:
                self.last_failure[index] = (rp.peer_id, "peer_lost")
            self._mark_lost(rp, type(exc).__name__, "connect", index)
            return
        buf = bytearray(size)
        try:
            with self.lock:
                rp.bitfield, rp.ever_connected, rp.lost_logged = bf, True, False
            if index not in bf:
                with self.lock:
                    self.last_failure[index] = (rp.peer_id, "piece_unavailable")
                return
            mtype, _ = recv_msg(sock)  # unchoke
            send_msg(sock, MSG_INTERESTED)
            for begin in range(0, size, BLOCK_SIZE):
                send_msg(sock, MSG_REQUEST, struct.pack(">III", index, begin, min(BLOCK_SIZE, size - begin)))
            received = 0
            while received < size:
                if self.stop_event.is_set():
                    return
                mtype, payload = recv_msg(sock)
                if mtype == MSG_CHOKE:
                    with self.lock:
                        rp.busy_until = time.time() + 0.25 + self.rng.random() * 0.25
                    return  # source busy: piece is released and picked again (maybe elsewhere)
                if mtype != MSG_PIECE:
                    continue
                pidx, begin = struct.unpack(">II", payload[:8])
                block = payload[8:]
                if pidx != index:
                    continue
                buf[begin : begin + len(block)] = block
                received += len(block)
        except (OSError, WireError, struct.error) as exc:
            with self.lock:
                self.last_failure[index] = (rp.peer_id, "peer_lost")
            self._mark_lost(rp, type(exc).__name__, "transfer", index)
            return
        finally:
            sock.close()

        piece = bytes(buf)
        if hashlib.sha1(piece).hexdigest() != self.meta.pieces[index]:
            with self.lock:
                self.bad_sources.setdefault(index, set()).add(rp.peer_id)
                self.last_failure[index] = (rp.peer_id, "invalid_chunk")
                rp.invalid_count += 1
                if rp.invalid_count >= 2:
                    rp.banned = True
                banned = rp.banned
            self.log.emit(
                "invalid_chunk", index, from_peer=rp.peer_id, reason="sha1_mismatch", bytes=size, banned=banned
            )
            return

        with self.lock:
            if self.have[index]:
                return
            self.data[index] = piece
            self.have[index] = True
            self.downloaded_bytes += size
            self.sources_used.add(rp.peer_id)
            have_count = sum(self.have)
        self.log.emit(
            "piece_done",
            index,
            bytes=size,
            from_peer=rp.peer_id,
            elapsed_ms=round((time.time() - t0) * 1000, 1),
            have_count=have_count,
            total_pieces=self.n,
        )
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
            finally:
                self._release(index, rp.peer_id)

    def finish_download(self) -> None:
        with self.lock:
            if self.complete_event.is_set():
                return
            blob = b"".join(self.data[i] for i in range(self.n))
            self.complete_event.set()
        if len(blob) != self.meta.length:
            self.log.emit("stopped", None, reason="assembled_size_mismatch")
            self.stop_reason = "error"
            self.stop_event.set()
            return
        self.dir.mkdir(parents=True, exist_ok=True)
        tmp = self.final_path.with_name(self.final_path.name + ".part")
        tmp.write_bytes(blob)
        os.replace(tmp, self.final_path)
        self.log.emit(
            "completed",
            None,
            total_pieces=self.n,
            bytes=len(blob),
            sha256=hashlib.sha256(blob).hexdigest(),
            download_time_sec=round(time.time() - self.start_ts, 3),
            sources=sorted(self.sources_used),
            path=str(self.final_path),
        )
        if self.args.exit_on_complete:
            self.stop_reason = "completed"
            self.announce("completed")
            self.stop_event.set()

    # ------------------------------------------------------------ lifecycle
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
        except FileNotFoundError:
            pass
        except OSError:
            pass

        if self.role == "seed":
            code = self.load_seed_data()
            if code:
                return code
            self.complete_event.set()
        if not self.open_server():
            return 1

        self.log.emit(
            "started",
            None,
            role=self.role,
            port=self.args.port,
            max_up_kbps=self.args.max_up_kbps,
            latency_ms=self.args.latency_ms,
            num_pieces=self.n,
            piece_length=self.meta.piece_length,
            info_hash=self.meta.info_hash,
            corrupt_pieces=sorted(self.corrupt),
            implementation="stub_peer",
        )
        threads = [
            threading.Thread(target=self.serve_forever, daemon=True),
            threading.Thread(target=self.announcer_loop, daemon=True),
        ]
        if self.role == "leech":
            threads.append(threading.Thread(target=self.poller_loop, daemon=True))
            threads += [threading.Thread(target=self.worker_loop, daemon=True) for _ in range(max(1, self.args.max_parallel))]
        for t in threads:
            t.start()

        while not self.stop_event.is_set():
            if stop_file.exists():
                self.stop_reason = "stop_file"
                break
            if (
                self.role == "leech"
                and self.args.give_up_sec
                and not self.complete_event.is_set()
                and time.time() - self.start_ts > self.args.give_up_sec
            ):
                self.stop_reason = "gave_up"
                break
            self.stop_event.wait(0.15)
        self.stop_event.set()

        completed = self.complete_event.is_set()
        if self.stop_reason != "completed":
            # best effort: tell the tracker we are leaving (Contract 2 'stopped')
            self.announce("stopped")
        try:
            if self.server:
                self.server.close()
        except OSError:
            pass
        with self.lock:
            have_count = sum(self.have)
        self.log.emit(
            "stopped",
            None,
            reason=self.stop_reason,
            have_count=have_count,
            total_pieces=self.n,
            uploaded_bytes=self.uploaded_bytes,
            downloaded_bytes=self.downloaded_bytes,
        )
        if self.stop_reason == "gave_up":
            return 3
        if self.role == "leech" and not completed:
            return 130
        return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DS02 StubPeer (Contract 4 compatible)")
    # --- Contract 4 ---
    p.add_argument("--torrent", required=True, help="path to metainfo JSON (Contract 1)")
    p.add_argument("--port", type=int, required=True, help="TCP port to listen on")
    p.add_argument("--dir", required=True, help="data directory (seed reads / leech writes DIR/<name>)")
    p.add_argument("--role", choices=["seed", "leech"], required=True)
    p.add_argument("--max-up-kbps", type=int, default=0, help="upload cap in KiB/s, 0 = unlimited")
    p.add_argument("--peer-id", default=None, help="peer id used in logs and handshakes")
    p.add_argument("--log", default=None, help="JSONL log file (default stdout)")
    p.add_argument("--exit-on-complete", action="store_true", help="leech: exit 0 right after completing")
    # --- simulation-only (optional) ---
    p.add_argument("--latency-ms", type=int, default=0)
    p.add_argument("--corrupt-pieces", default=None)
    p.add_argument("--ip", default="127.0.0.1", help="address announced to the tracker")
    p.add_argument("--bind", default="127.0.0.1", help="listen address (0.0.0.0 for multi-machine runs)")
    p.add_argument("--max-parallel", type=int, default=4)
    p.add_argument("--give-up-sec", type=float, default=0.0)
    p.add_argument("--upload-slots", type=int, default=4)
    p.add_argument("--data-seed", default=DATA_SEED_DEFAULT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    peer_id = args.peer_id or f"{args.role}-{args.port}"
    logger = EventLogger(peer_id, args.log)
    try:
        meta = load_metainfo_any(args.torrent, args.data_seed)
    except (OSError, MetainfoFormatError) as exc:
        logger.emit("stopped", None, reason="invalid_metainfo", error=str(exc))
        return 1
    peer = StubPeer(args, meta, logger)
    peer.install_signal_handlers()
    try:
        return peer.run()
    finally:
        logger.close()


if __name__ == "__main__":
    sys.exit(main())
