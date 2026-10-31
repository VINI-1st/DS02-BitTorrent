"""Usage: python -m torrent.make_torrent FILE --announce URL [--piece-length N] [-o OUT]"""

import argparse
import sys

from .metainfo import DEFAULT_PIECE_LENGTH, MetainfoError, create_metainfo, save_metainfo


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="make-torrent", description="Create a DS02 metainfo file."
    )
    parser.add_argument("file", help="file to share")
    parser.add_argument("--announce", required=True, help="tracker announce URL")
    parser.add_argument(
        "--piece-length",
        type=int,
        default=DEFAULT_PIECE_LENGTH,
        help=f"piece size in bytes (default {DEFAULT_PIECE_LENGTH})",
    )
    parser.add_argument("-o", "--output", help="output path (default FILE.torrent.json)")
    args = parser.parse_args(argv)

    try:
        meta = create_metainfo(args.file, args.announce, args.piece_length)
    except (OSError, MetainfoError) as exc:
        print(f"make-torrent: {exc}", file=sys.stderr)
        return 1

    out = args.output or f"{args.file}.torrent.json"
    save_metainfo(meta, out)
    print(meta.info_hash)
    return 0


if __name__ == "__main__":
    sys.exit(main())
