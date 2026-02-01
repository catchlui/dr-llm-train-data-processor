"""
Repo entrypoint for the download->normalize->upload pipeline.

Usage:
  python scripts/download.py --config configs/config.yml
"""

import argparse
import os

from data_radar_dolma_wrapper.downloader import run_download


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download datasets, normalize/sanitize, and upload to S3")
    default_cfg = "configs/config.local.yml" if os.path.exists("configs/config.local.yml") else "configs/config.yml"
    p.add_argument("--config", type=str, default=default_cfg, help="Path to config YAML")
    p.add_argument("--mode", type=str, choices=["test", "full"], help="Override config mode")
    p.add_argument("--storage", type=str, choices=["local", "s3", "both"], help="Override storage mode")
    p.add_argument("--s3-bucket", type=str, help="Override aws.s3_bucket")
    p.add_argument("--region", type=str, help="Override aws.region")
    return p.parse_args()


def main() -> None:
    args = parse_args()
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


if __name__ == "__main__":
    main()

