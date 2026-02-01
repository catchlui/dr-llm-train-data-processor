from __future__ import annotations

import argparse
import sys

from .downloader import run_download


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="drdw",
        description="Data Radar Dolma Wrapper: download -> normalize/sanitize -> upload to S3",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    dl = sub.add_parser("download", help="Download datasets and upload JSONL shards to S3")
    dl.add_argument("--config", type=str, default="config.yml", help="Path to config file (default: config.yml)")
    dl.add_argument("--mode", type=str, choices=["test", "full"], help="Override config mode")
    dl.add_argument("--storage", type=str, choices=["local", "s3", "both"], help="Override storage mode")
    dl.add_argument("--s3-bucket", type=str, help="Override aws.s3_bucket")
    dl.add_argument("--region", type=str, help="Override aws.region")

    args = parser.parse_args(argv)

    if args.cmd == "download":
        overrides = {}
        if args.mode:
            overrides["mode"] = args.mode
        if args.storage:
            overrides["storage.mode"] = args.storage
        if args.s3_bucket:
            overrides["aws.s3_bucket"] = args.s3_bucket
        if args.region:
            overrides["aws.region"] = args.region
        run_download(args.config, overrides=overrides)
        return 0

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

