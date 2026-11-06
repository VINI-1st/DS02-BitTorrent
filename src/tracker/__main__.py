"""CLI entry point for the tracker server.

Usage::

    python -m src.tracker --host 0.0.0.0 --port 8000 --interval 30 --ttl 90
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys

from .server import TrackerServer


def main() -> None:
    """Parse command-line arguments and run the tracker until interrupted."""
    parser = argparse.ArgumentParser(
        description="DS02 BitTorrent-like P2P Tracker Server",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Interface to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="TCP port to listen on (default: 8000)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Announce interval in seconds (default: 30)",
    )
    parser.add_argument(
        "--ttl",
        type=int,
        default=90,
        help="Peer TTL in seconds (default: 90)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )

    tracker = TrackerServer(
        host=args.host,
        port=args.port,
        interval=args.interval,
        ttl=args.ttl,
    )
    tracker.start()

    # Wait until Ctrl+C / SIGTERM.
    stop_event = tracker._stop_event

    def _handle_signal(signum: int, frame: object) -> None:
        tracker.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    print(f"Tracker running on {args.host}:{args.port}  (Ctrl+C to stop)")
    try:
        stop_event.wait()
    except KeyboardInterrupt:
        tracker.stop()


if __name__ == "__main__":
    main()
