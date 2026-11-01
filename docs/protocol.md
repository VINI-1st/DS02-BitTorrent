# DS02 – Protocol Specification

## 1. Tracker HTTP API

### 1.1 Announce

**Endpoint:** `GET /announce`

**Query Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `info_hash` | string | ✅ | Hex-encoded SHA-1 hash identifying the torrent (40 hex chars) |
| `peer_id` | string | ✅ | Unique identifier for this peer (arbitrary string) |
| `ip` | string | ❌ | Peer's IP address. Defaults to requester's IP. |
| `port` | int | ✅ | Port the peer is listening on |
| `event` | string | ❌ | One of: `started`, `completed`, `stopped`, or empty |
| `left` | int | ❌ | Bytes remaining to download. 0 = seeder. Default: 0 |

**Response (200 OK):**
```json
{
    "interval": 30,
    "complete": 5,
    "incomplete": 12,
    "peers": [
        {"peer_id": "peer-001", "ip": "192.168.1.10", "port": 9001},
        {"peer_id": "peer-002", "ip": "192.168.1.11", "port": 9002}
    ]
}
```

- `interval`: seconds until client should next announce
- `complete`: number of seeders in this swarm
- `incomplete`: number of leechers in this swarm
- `peers`: up to 50 random peers, **excluding the requesting peer**

**Events:**
- `started` — Peer has just joined. Tracker adds it to the swarm.
- `completed` — Peer has finished downloading. Marks peer as seeder.
- `stopped` — Peer is shutting down. Tracker removes it immediately.
- _(empty)_ — Periodic re-announce. Updates the peer's last-seen timestamp.

**Errors:**
- `400 Bad Request` — Missing required parameters.

### 1.2 Scrape

**Endpoint:** `GET /scrape`

**Query Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `info_hash` | string | ❌ | If provided, return stats for this swarm only |

**Response (200 OK):**
```json
{
    "files": {
        "abc123...": {
            "complete": 5,
            "incomplete": 12,
            "downloaded": 42
        }
    }
}
```

### 1.3 TTL and Peer Expiry

- Peers that do not re-announce within `TTL` seconds (default: 90s) are automatically removed.
- The tracker runs a background cleanup every `interval` seconds.
- After tracker restart, swarm state is lost but rebuilds as peers re-announce.

---

## 2. Wire Protocol (Peer-to-Peer Messages)

### 2.1 Framing

All messages (except handshake) use length-prefixed framing:

```
┌──────────────────┬──────────┬──────────┐
│ length (4 bytes) │ ID (1 b) │ payload  │
│ big-endian u32   │          │          │
└──────────────────┴──────────┴──────────┘
```

`length` = size of (ID + payload). A keepalive has length = 0 (no ID, no payload).

### 2.2 Handshake

Sent immediately upon connecting. Both sides must send a handshake before any other message.

```
┌───────────┬───────────────────────┬──────────┬───────────┬─────────┐
│ pstrlen=19│ pstr="BitTorrent      │ reserved │ info_hash │ peer_id │
│ (1 byte)  │  protocol" (19 bytes) │ (8 bytes)│ (20 bytes)│(20 bytes│
└───────────┴───────────────────────┴──────────┴───────────┴─────────┘
```

Total: 68 bytes. The connecting peer sends first; the listening peer validates `info_hash` matches, then replies.

### 2.3 Message Types

| ID | Name | Payload | Description |
|---|---|---|---|
| — | keepalive | _(none)_ | 4 zero bytes. Keeps connection alive. |
| 0 | choke | _(none)_ | Sender will not serve requests. |
| 1 | unchoke | _(none)_ | Sender will serve requests. |
| 2 | interested | _(none)_ | Sender wants pieces the receiver has. |
| 3 | not_interested | _(none)_ | Sender does not want any pieces. |
| 4 | have | `index: u32` | Sender has completed piece `index`. |
| 5 | bitfield | `bitfield: bytes` | Bitmap of pieces the sender has. Sent right after handshake. |
| 6 | request | `index: u32, begin: u32, length: u32` | Request a block within a piece. |
| 7 | piece | `index: u32, begin: u32, block: bytes` | Deliver a block of data. |
| 8 | cancel | `index: u32, begin: u32, length: u32` | Cancel a pending request. |

### 2.4 Connection Lifecycle

1. TCP connect
2. Both sides exchange handshake; validate `info_hash`
3. Both sides send `bitfield` (if they have any pieces)
4. Exchange `interested` / `not_interested` based on bitfield
5. Choking/unchoking negotiation
6. Request/piece exchange loop
7. `keepalive` every 2 minutes if no other message
8. Connection closes on: EOF, timeout (no message in 4 min), or error

---

## 3. Metainfo File Format (Contract 1 — written by B)

```json
{
    "announce": "http://127.0.0.1:8000/announce",
    "info": {
        "name": "example.txt",
        "length": 1048576,
        "piece_length": 262144,
        "pieces": [
            "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
            "109f4b3c50d7b0df729d299bc6f8e9ef9066971f",
            "3ebfa301dc59196f18593c45e519287a23297589",
            "7c211433f02024a5b14e8d1e3a3c5913a52d0f56"
        ]
    },
    "info_hash": "d14a028c2a3a2bc9476102bb288234c415a2b01f"
}
```

- `pieces`: array of hex-encoded SHA-1 hashes, one per piece
- `info_hash`: SHA-1 hash of the canonical JSON encoding of the `info` dict
- `piece_length`: typically 256 KiB (262144 bytes); last piece may be shorter

---

## 4. Peer CLI (Contract 4 — written by B)

```
python -m src.peer \
    --torrent path/to/file.torrent.json \
    --port 9001 \
    --dir ./downloads \
    --role seed|leech \
    [--max-up-kbps 500]
```

- Exits with code 0 when download completes (role=leech) or on SIGTERM (role=seed)
- Exits with code 1 on error

---

## 5. Event Log Format (Contract 5 — written by C)

One JSON object per line, written to stdout:

```json
{"ts": 1698765432.123, "peer_id": "peer-001", "event": "started", "info_hash": "abc..."}
{"ts": 1698765433.456, "peer_id": "peer-001", "event": "piece_done", "index": 0, "info_hash": "abc..."}
{"ts": 1698765434.789, "peer_id": "peer-001", "event": "peer_lost", "remote_peer_id": "peer-002", "info_hash": "abc..."}
{"ts": 1698765440.000, "peer_id": "peer-001", "event": "completed", "info_hash": "abc..."}
```

Event types:
- `started` — peer joined the swarm
- `piece_done` — piece downloaded and verified (includes `index`)
- `peer_connected` — connected to a peer (includes `remote_peer_id`)
- `peer_lost` — disconnected from a peer (includes `remote_peer_id`)
- `completed` — all pieces downloaded
- `stopped` — peer shutting down

---

## 6. Transport Interface (Contract — A provides to B)

```python
# B can use FakeTransport (in-memory) or TcpTransport (real TCP)
# Both implement the Transport ABC from src.network.transport

class Transport(ABC):
    async def start(host, port, info_hash, peer_id, listener) -> None
    async def stop() -> None
    async def connect(remote_ip, remote_port) -> PeerConnection
    async def send(conn, message) -> None
    async def disconnect(conn) -> None
    def get_connections() -> list[PeerConnection]

class TransportListener(ABC):
    async def on_connected(conn) -> None
    async def on_disconnected(conn, reason) -> None
    async def on_message(conn, message) -> None
    async def on_error(conn, error) -> None
```

---

## 7. Tracker Benchmark & Load Test Results (Task A7)

Measured using `scripts/benchmark_tracker.py` on local loopback interface:

| Metric | Measured Value |
|---|---|
| **Total Requests** | 500 requests |
| **Concurrency Level** | 20 parallel threads |
| **Throughput** | **589.0 requests / second** |
| **Success Rate** | **100%** (500 / 500 successful, 0 errors) |
| **Average Latency** | **25.54 ms** |
| **Median (p50) Latency** | **4.02 ms** |
| **95th Percentile (p95)** | **10.84 ms** |
| **99th Percentile (p99)** | **667.69 ms** |
| **Max Latency** | **672.94 ms** |

> *Note for Member C: You can incorporate these numbers and use `scripts/benchmark_tracker.py` in the evaluation section of the report.*


