# Member A (Tracker & Network) — Status & Handoff Guide

## 1. Responsibilities & Ownership
Member A owns everything related to the central tracker, peer wire protocol, TCP transport layer, and tracker client library:
- `src/tracker/` (A1, A2, A3)
- `src/protocol/` (A4)
- `src/network/` (A5)
- `src/trackerclient/` (A6)
- `docs/protocol.md` & `scripts/benchmark_tracker.py` (A7)

Contracts provided to others:
- `Transport` and `TransportListener` (`src.network.transport`, implemented by `TcpTransport`) -> Used by Member B
- `TrackerClient` (`src.trackerclient.client`) -> Used by Member B
- Wire Protocol Codec & Dataclasses (`src.protocol`) -> Used by Member B
- Tracker HTTP API (`GET /announce`, `GET /scrape`) -> Used by Member B, Member C

---

## 2. Task Status Checklist

| Task | Description | Status | Deliverables |
|---|---|---|---|
| **A1** | Tracker Server (announce, scrape, swarm management) | ✅ Completed | `src/tracker/swarm.py`, `src/tracker/server.py` |
| **A2** | Peer Dead Detection & TTL (expiration cleanup, events) | ✅ Completed | Handled in `SwarmManager` + background thread |
| **A3** | Concurrency & Recovery (thread-safe swarm, restart recovery) | ✅ Completed | Threading locks, `ThreadingHTTPServer` |
| **A4** | Wire Protocol (handshake, bitfield, have, request, piece, etc.) | ✅ Completed | `src/protocol/messages.py`, `src/protocol/codec.py` |
| **A5** | Connection & Transport (`Transport` interface, `TcpTransport`) | ✅ Completed | `src/network/transport.py`, `src/network/tcp_transport.py` |
| **A6** | Tracker Client Library for Peers (periodic announce, backoff) | ✅ Completed | `src/trackerclient/client.py`, `tests/test_trackerclient.py` |
| **A7** | Protocol Documentation & Benchmarks | ✅ Completed | `docs/protocol.md`, `scripts/benchmark_tracker.py` |

**Test Summary:** 115 unit & integration tests across 4 test suites pass with 100% success rate!

---

## 3. Interfaces for Other Agents (B & C)

### For Member B (Peer Core)

#### 1. Tracker Client (`src.trackerclient`)
```python
from src.trackerclient import TrackerClient
from src.trackerclient.client import TrackerClientConfig

config = TrackerClientConfig(
    tracker_url="http://127.0.0.1:8000/announce",
    info_hash="a1b2c3d4..." * 4,  # 40-char hex string
    peer_id="peer-001",
    port=9001
)
client = TrackerClient(config)

# Async callback called whenever peer list is updated from tracker:
async def handle_peers(peers: list[dict]):
    # peers is [{"peer_id": str, "ip": str, "port": int}, ...]
    for p in peers:
        print(f"Discovered peer: {p['peer_id']} at {p['ip']}:{p['port']}")

client.on_peers_received = handle_peers

# Start announcing periodically (left = remaining bytes)
await client.start(left=1048576)

# Update remaining download progress
client.left = 524288

# When download finishes
await client.announce_completed()

# On shutdown
await client.stop()
```

#### 2. Wire Protocol Codec & Messages (`src.protocol`)
```python
from src.protocol import (
    Handshake, KeepAlive, Choke, Unchoke, Interested, NotInterested,
    Have, Bitfield, Request, Piece, Cancel,
    encode_message, decode_message, encode_handshake, decode_handshake,
    read_message, read_handshake
)

# Dataclasses:
# Handshake(info_hash: bytes[20], peer_id: bytes[20])
# Have(piece_index: int)
# Bitfield(bitfield: bytes)
# Request(index: int, begin: int, length: int)
# Piece(index: int, begin: int, block: bytes)
# Cancel(index: int, begin: int, length: int)
# Choke(), Unchoke(), Interested(), NotInterested(), KeepAlive()
```

#### 3. Transport Layer (`src.network`)
```python
from src.network import Transport, TcpTransport, TransportListener, PeerConnection

class MyPeerListener(TransportListener):
    async def on_connected(self, conn: PeerConnection) -> None:
        print(f"Connected to {conn.peer_id.hex()}")

    async def on_disconnected(self, conn: PeerConnection, reason: str) -> None:
        print(f"Disconnected from {conn.peer_id.hex()}: {reason}")

    async def on_message(self, conn: PeerConnection, message: Any) -> None:
        # message is an instance of Choke, Have, Piece, etc.
        print(f"Received {type(message).__name__}")

    async def on_error(self, conn: PeerConnection, error: Exception) -> None:
        print(f"Error on connection: {error}")

transport = TcpTransport(max_connections=50, keepalive_interval=120.0)
await transport.start("0.0.0.0", 9001, info_hash_bytes, peer_id_bytes, MyPeerListener())

# Connect to outbound peer:
conn = await transport.connect("192.168.1.10", 9002)

# Send message:
await transport.send(conn, Interested())

# Disconnect:
await transport.disconnect(conn)

# Stop:
await transport.stop()
```

---

### For Member C (Simulator / Churn)
- Tracker CLI:
  ```bash
  python -m src.tracker --host 0.0.0.0 --port 8000 --interval 30 --ttl 90
  ```
- Benchmark script:
  ```bash
  python scripts/benchmark_tracker.py --requests 500 --concurrency 20
  ```
- Endpoints:
  - `GET /announce?info_hash=...&peer_id=...&port=...&left=...&event=started|completed|stopped`
  - `GET /scrape?info_hash=...`

---

## 4. Test Verification
All 115 tests run with Python's built-in `unittest`:
```bash
python -m unittest discover -s tests -p "test_*.py" -v
```
All tests pass cleanly without third-party dependencies.
