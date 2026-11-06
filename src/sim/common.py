"""Shared helpers for Role C (simulation, metrics, tests, demo).

Contents
--------
* Contract 5 – JSON Lines event schema: constants, ``make_event`` and ``validate_event``.
* Deterministic test data: ``deterministic_bytes`` (same name+size => same bytes on
  every OS / Python version, so the seeder's file and every leecher's file can be
  compared with SHA-256).
* Contract 1 – metainfo: ``build_metainfo`` writes exactly the format of Role B
  (``docs/contracts/metainfo-and-peer-cli.md``) and ``load_metainfo_any`` reads
  both B's nested format and the legacy flat "dummy" format.
* Small file helpers (hashing, robust rmtree that survives Windows/OneDrive locks).

This module has no third-party dependencies so that ``stub_peer.py`` and
``stub_tracker.py`` can import it from a bare Python installation.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import stat
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, TextIO

REPO_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Contract 5 – event log schema
# ---------------------------------------------------------------------------

#: Events that tracker (A) and peer (B) processes emit.
CONTRACT_EVENTS: frozenset[str] = frozenset(
    {
        "started",
        "announce_ok",
        "piece_done",
        "piece_uploaded",
        "invalid_chunk",
        "peer_lost",
        "switch_source",
        "completed",
        "stopped",
    }
)

#: Optional events. ``announce_failed`` may be emitted by peers; the others are
#: only written by the simulator itself (``peer_id == "simulator"``) or by the
#: tracker supervisor. A and B are NOT required to emit any of these.
EXTENSION_EVENTS: frozenset[str] = frozenset(
    {
        "announce_failed",
        "scenario_start",
        "scenario_end",
        "torrent_created",
        "spawned",
        "process_exit",
        "churn_join",
        "churn_leave",
        "churn_crash",
        "fault_kill_peer",
        "tracker_crashed",
        "tracker_restarted",
        # emitted by Role A's real tracker (src/tracker)
        "http_request",
        "peers_expired",
    }
)

#: Event-name aliases accepted from real implementations and rewritten by the
#: launcher to the Contract 5 name (the original is kept in meta.original_event).
EVENT_ALIASES: dict[str, str] = {
    "tracker_started": "started",
    "tracker_stopped": "stopped",
}

ALL_EVENTS: frozenset[str] = CONTRACT_EVENTS | EXTENSION_EVENTS
REQUIRED_FIELDS: tuple[str, ...] = ("ts", "peer_id", "event", "chunk_index")


def make_event(
    peer_id: str,
    event: str,
    chunk_index: int | None = None,
    meta: dict[str, Any] | None = None,
    ts: float | None = None,
) -> dict[str, Any]:
    """Build one Contract 5 record."""
    return {
        "ts": round(time.time() if ts is None else ts, 4),
        "peer_id": str(peer_id),
        "event": str(event),
        "chunk_index": chunk_index,
        "meta": dict(meta or {}),
    }


def validate_event(record: Any, allow_extensions: bool = True) -> list[str]:
    """Return a list of schema problems (empty list == valid)."""
    problems: list[str] = []
    if not isinstance(record, dict):
        return ["record is not a JSON object"]
    for key in REQUIRED_FIELDS:
        if key not in record:
            problems.append(f"missing field '{key}'")
    ts = record.get("ts")
    if "ts" in record and (isinstance(ts, bool) or not isinstance(ts, (int, float))):
        problems.append("'ts' must be a number (Unix epoch seconds)")
    if "peer_id" in record and (not isinstance(record["peer_id"], str) or not record["peer_id"]):
        problems.append("'peer_id' must be a non-empty string")
    ev = record.get("event")
    if "event" in record:
        if not isinstance(ev, str):
            problems.append("'event' must be a string")
        else:
            allowed = ALL_EVENTS if allow_extensions else CONTRACT_EVENTS
            if ev not in allowed:
                problems.append(f"unknown event '{ev}'")
    ci = record.get("chunk_index", None)
    if ci is not None and (isinstance(ci, bool) or not isinstance(ci, int) or ci < 0):
        problems.append("'chunk_index' must be a non-negative int or null")
    if "meta" in record and not isinstance(record["meta"], dict):
        problems.append("'meta' must be an object")
    return problems


def read_jsonl(path: str | Path, strict: bool = False) -> list[dict[str, Any]]:
    """Read a JSON Lines file. Invalid lines are skipped unless ``strict``."""
    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                if strict:
                    raise ValueError(f"{path}:{lineno}: invalid JSON: {exc}") from exc
                continue
            if isinstance(obj, dict):
                records.append(obj)
            elif strict:
                raise ValueError(f"{path}:{lineno}: line is not a JSON object")
    return records


class EventLogger:
    """Thread-safe Contract 5 writer used by the stub processes.

    Writes to ``stream`` (stdout by default) or appends to a ``.jsonl`` file.
    """

    def __init__(self, peer_id: str, log_path: str | None = None, stream: TextIO | None = None):
        self.peer_id = peer_id
        self._lock = threading.Lock()
        self._file = open(log_path, "a", encoding="utf-8") if log_path else None
        self._stream = stream if stream is not None else sys.stdout

    def emit(self, event: str, chunk_index: int | None = None, **meta: Any) -> dict[str, Any]:
        rec = make_event(self.peer_id, event, chunk_index, meta)
        line = json.dumps(rec, ensure_ascii=False)
        with self._lock:
            target = self._file or self._stream
            try:
                target.write(line + "\n")
                target.flush()
            except (OSError, ValueError):
                pass  # stdout closed by the parent: nothing useful left to do
        return rec

    def close(self) -> None:
        if self._file:
            with self._lock:
                self._file.close()
                self._file = None


def configure_utf8_stdio() -> None:
    """Force UTF-8 on stdout/stderr (Windows consoles default to cp1252/cp437)."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (OSError, ValueError):
                pass


# ---------------------------------------------------------------------------
# Deterministic data
# ---------------------------------------------------------------------------

DATA_SEED_DEFAULT = "ds02"


def deterministic_bytes(name: str, size: int, seed: str = DATA_SEED_DEFAULT) -> bytes:
    """Pseudo-random but fully deterministic content for a shared file.

    SHAKE-256 is standardised (FIPS 202), so the output is identical on every
    platform and Python version.
    """
    if size < 0:
        raise ValueError("size must be >= 0")
    if size == 0:
        return b""
    return hashlib.shake_256(f"{seed}|{name}|{size}".encode("utf-8")).digest(size)


def write_deterministic_file(path: str | Path, size: int, seed: str = DATA_SEED_DEFAULT) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = deterministic_bytes(path.name, size, seed)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)
    return path


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(1 << 16):
            h.update(block)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Contract 1 – metainfo
# ---------------------------------------------------------------------------


def canonical_info(info: dict[str, Any]) -> bytes:
    """Canonical encoding defined by Role B (sorted keys, no spaces, UTF-8)."""
    return json.dumps(info, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def compute_info_hash(info: dict[str, Any]) -> str:
    return hashlib.sha1(canonical_info(info)).hexdigest()


def piece_hashes(data: bytes, piece_length: int) -> list[str]:
    return [
        hashlib.sha1(data[i : i + piece_length]).hexdigest()
        for i in range(0, len(data), piece_length)
    ]


@dataclass
class TorrentInfo:
    """In-memory view of a metainfo file, independent of its on-disk flavour."""

    name: str
    length: int
    piece_length: int
    pieces: list[str]
    info_hash: str
    announce: str
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def num_pieces(self) -> int:
        return len(self.pieces)

    def piece_size(self, index: int) -> int:
        if not 0 <= index < self.num_pieces:
            raise IndexError(index)
        if index == self.num_pieces - 1:
            return self.length - index * self.piece_length
        return self.piece_length


def build_metainfo(
    source_file: str | Path,
    announce: str,
    piece_length: int,
    out_path: str | Path,
    creation_date: int | None = None,
) -> TorrentInfo:
    """Create a Contract 1 metainfo exactly like B's ``make-torrent``.

    Flat aliases (``file_name``, ``file_size_bytes``, ``piece_length``,
    ``pieces``, ``tracker_url``) are added at top level for tools that expect
    the flat names; they are outside ``info`` so they never change ``info_hash``
    and B's ``Metainfo.from_dict`` ignores them.
    """
    source_file = Path(source_file)
    data = source_file.read_bytes()
    if piece_length <= 0:
        raise ValueError("piece_length must be > 0")
    info = {
        "name": source_file.name,
        "length": len(data),
        "piece_length": int(piece_length),
        "pieces": piece_hashes(data, piece_length),
    }
    ih = compute_info_hash(info)
    doc = {
        "announce": announce,
        "created_by": "ds02-sim-launcher",
        "creation_date": int(time.time()) if creation_date is None else int(creation_date),
        "info": info,
        "info_hash": ih,
        # flat aliases (convenience only, not part of info_hash)
        "file_name": info["name"],
        "file_size_bytes": info["length"],
        "piece_length": info["piece_length"],
        "pieces": info["pieces"],
        "tracker_url": announce,
    }
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return TorrentInfo(info["name"], info["length"], info["piece_length"], list(info["pieces"]), ih, announce, doc)


class MetainfoFormatError(ValueError):
    pass


def load_metainfo_any(path: str | Path, data_seed: str = DATA_SEED_DEFAULT) -> TorrentInfo:
    """Read B's nested metainfo or the legacy flat dummy format.

    * Nested (``info.name/length/piece_length/pieces``): validated like B's reader.
    * Flat (``file_name/file_size_bytes/piece_length`` [+ ``pieces``]): if
      ``pieces`` is missing they are derived from ``deterministic_bytes`` so a
      dummy torrent still yields verifiable SHA-1 hashes.
    """
    with open(path, "r", encoding="utf-8") as f:
        try:
            doc = json.load(f)
        except json.JSONDecodeError as exc:
            raise MetainfoFormatError(f"not valid JSON: {exc}") from exc
    if not isinstance(doc, dict):
        raise MetainfoFormatError("metainfo must be a JSON object")

    if isinstance(doc.get("info"), dict):
        info = doc["info"]
        try:
            name = str(info["name"])
            length = int(info["length"])
            plen = int(info["piece_length"])
            pieces = [str(p).lower() for p in info["pieces"]]
        except (KeyError, TypeError, ValueError) as exc:
            raise MetainfoFormatError(f"missing or invalid info field: {exc}") from exc
        computed = compute_info_hash(info)
        declared = doc.get("info_hash")
        if declared is not None and str(declared).lower() != computed:
            raise MetainfoFormatError("info_hash does not match info")
        announce = str(doc.get("announce") or doc.get("tracker_url") or "")
        ih = computed
    else:
        try:
            name = str(doc.get("file_name") or doc.get("name"))
            length = int(doc.get("file_size_bytes", doc.get("length")))
            plen = int(doc["piece_length"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MetainfoFormatError(f"flat metainfo missing field: {exc}") from exc
        if doc.get("pieces"):
            pieces = [str(p).lower() for p in doc["pieces"]]
        else:
            pieces = piece_hashes(deterministic_bytes(name, length, data_seed), plen)
        info = {"name": name, "length": length, "piece_length": plen, "pieces": pieces}
        ih = str(doc.get("info_hash") or compute_info_hash(info)).lower()
        announce = str(doc.get("tracker_url") or doc.get("announce") or "")

    if plen <= 0 or length < 0:
        raise MetainfoFormatError("invalid length / piece_length")
    if len(pieces) != math.ceil(length / plen):
        raise MetainfoFormatError(f"expected {math.ceil(length / plen)} piece hashes, got {len(pieces)}")
    for h in pieces:
        if len(h) != 40 or any(c not in "0123456789abcdef" for c in h):
            raise MetainfoFormatError(f"invalid piece hash {h!r}")
    if announce and not announce.rstrip("/").endswith("/announce"):
        announce = announce.rstrip("/") + "/announce"
    return TorrentInfo(name, length, plen, pieces, ih, announce, doc)


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------


def _on_rm_error(func: Callable, path: str, _exc: Any) -> None:
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except OSError:
        pass


def robust_rmtree(path: str | Path, retries: int = 8, delay: float = 0.25) -> bool:
    """Delete a directory tree, retrying while Windows / OneDrive still hold a lock."""
    path = Path(path)
    for _ in range(retries):
        if not path.exists():
            return True
        try:
            if sys.version_info >= (3, 12):
                shutil.rmtree(path, onexc=_on_rm_error)
            else:  # pragma: no cover - older Pythons
                shutil.rmtree(path, onerror=_on_rm_error)
        except OSError:
            pass
        if not path.exists():
            return True
        time.sleep(delay)
    return not path.exists()
