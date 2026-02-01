from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Dict, Iterable, Iterator, Tuple

import yaml

from .processing import DolmaToolkitPIIMasker, mask_spans, normalize_text, stable_id
from . import __version__ as WRAPPER_VERSION


def apply_limit(dataset, limit_cfg: dict, mode: str):
    """Apply test limits to dataset based on configuration"""
    if mode == "full" or limit_cfg["type"] == "none":
        return dataset
    if limit_cfg["type"] == "rows":
        return islice(dataset, limit_cfg["value"])
    if limit_cfg["type"] == "percent":
        percent = limit_cfg["value"]
        # For streaming datasets, approximate
        return islice(dataset, int(10000 * percent / 100))  # Approximate
    raise ValueError(f"Unknown limit type: {limit_cfg['type']}")


def save_jsonl_local(records, filepath: str) -> None:
    """Save records to local JSONL file"""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def upload_jsonl_to_s3(records, bucket: str, key: str) -> None:
    """Upload records to S3 as JSONL"""
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records).encode("utf-8")

    # Prefer boto3 when it imports cleanly; otherwise fall back to AWS CLI.
    try:
        import boto3  # type: ignore

        s3 = boto3.client("s3")
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        return
    except Exception:
        pass

    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl") as tmp:
        tmp.write(body)
        tmp_path = tmp.name

    try:
        s3_url = f"s3://{bucket}/{key}"
        subprocess.run(["aws", "s3", "cp", tmp_path, s3_url], check=True)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def save_records(records, mode: str, local_path: str, bucket: str, s3_key: str) -> None:
    """Save records based on storage mode"""
    if mode in ("local", "both"):
        save_jsonl_local(records, local_path)
    if mode in ("s3", "both"):
        upload_jsonl_to_s3(records, bucket, s3_key)


def _get_processing_cfg(cfg: dict) -> dict:
    return (cfg or {}).get("processing") or {}

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_run_metadata(*, cfg: dict, config_path: str) -> dict:
    pcfg = _get_processing_cfg(cfg)
    runcfg = pcfg.get("run_metadata") if isinstance(pcfg.get("run_metadata"), dict) else {}
    if not runcfg or not runcfg.get("enabled", False):
        return {}

    include = runcfg.get("include")
    include_set = set(include) if isinstance(include, list) else None

    def keep(key: str) -> bool:
        return True if include_set is None else key in include_set

    run_md: dict = {}
    if keep("run_id"):
        run_md["run_id"] = str(uuid.uuid4())
    if keep("run_started_at"):
        run_md["run_started_at"] = _iso_now()
    if keep("config_path"):
        run_md["config_path"] = config_path
    if keep("mode"):
        run_md["mode"] = cfg.get("mode")
    if keep("storage_mode"):
        run_md["storage_mode"] = (cfg.get("storage") or {}).get("mode")
    if keep("s3_bucket"):
        run_md["s3_bucket"] = (cfg.get("aws") or {}).get("s3_bucket")
    if keep("s3_prefix"):
        run_md["s3_prefix"] = (cfg.get("aws") or {}).get("s3_prefix")
    if keep("wrapper_version"):
        run_md["wrapper_version"] = WRAPPER_VERSION

    return run_md


def _build_doc(
    *,
    dataset_name: str,
    record: dict,
    dcfg: dict,
    extra_metadata: dict,
) -> dict:
    text_field = dcfg.get("text_field", "text")
    raw_text = record.get(text_field, "")
    text = raw_text if isinstance(raw_text, str) else str(raw_text)

    upstream_id = record.get("id") or record.get("_id") or record.get("doc_id")
    doc_id = str(upstream_id) if upstream_id is not None else stable_id(dataset_name, text)

    return {
        "id": doc_id,
        "text": text,
        "source": dataset_name,
        "created": record.get("created", ""),
        "added": record.get("added", ""),
        "version": record.get("version"),
        "metadata": extra_metadata or {},
    }


class ProcessingPipeline:
    def __init__(self, cfg: dict) -> None:
        self.cfg = cfg
        self._masker_cache: Dict[str, DolmaToolkitPIIMasker] = {}

    def process_doc(self, *, doc: dict) -> dict:
        pcfg = _get_processing_cfg(self.cfg)
        if not pcfg.get("enabled", False):
            return doc

        ncfg = (pcfg.get("normalize") or {}) if isinstance(pcfg.get("normalize"), dict) else {}
        if ncfg.get("enabled", True):
            doc["text"] = normalize_text(
                doc.get("text", ""),
                unicode_form=ncfg.get("unicode_form", "NFC"),
                collapse_whitespace=ncfg.get("collapse_whitespace", True),
            )

        dtcfg = (pcfg.get("dolma_toolkit") or {}) if isinstance(pcfg.get("dolma_toolkit"), dict) else {}
        if dtcfg.get("enabled", False):
            try:
                tagger_name = dtcfg.get("pii_tagger", "pii_regex_v2")
                masker = self._masker_cache.get(tagger_name)
                if masker is None:
                    masker = DolmaToolkitPIIMasker(tagger_name=tagger_name)
                    self._masker_cache[tagger_name] = masker

                spans = masker.find_pii_spans(doc_id=doc["id"], text=doc["text"], source=doc["source"])
                if spans:
                    doc["text"] = mask_spans(doc["text"], spans, mask_token=dtcfg.get("mask_token", "<PII>"))
                    md = doc.setdefault("metadata", {})
                    md["pii_masked"] = True
                    md["pii_span_count"] = len(spans)
            except Exception as e:
                md = doc.setdefault("metadata", {})
                md["dolma_toolkit_error"] = str(e)

        return doc


def process_stream(
    dataset_name: str,
    ds_iter: Iterator[dict],
    shard_size: int,
    cfg: dict,
    dcfg: dict,
    run_md: dict,
    local_subdir: str,
    s3_subdir: str,
) -> Tuple[int, int]:
    """Process streaming dataset and save in shards"""
    buffer, shard_id, total = [], 0, 0
    pipe = ProcessingPipeline(cfg)

    pcfg = _get_processing_cfg(cfg)
    global_md = (pcfg.get("metadata") or {}) if isinstance(pcfg.get("metadata"), dict) else {}
    dataset_md = (dcfg.get("metadata") or {}) if isinstance(dcfg.get("metadata"), dict) else {}
    # Merge global + dataset metadata, then attach run metadata under `run`
    extra_md = {**global_md, **dataset_md}
    if run_md:
        existing_run = extra_md.get("run")
        if isinstance(existing_run, dict):
            extra_md["run"] = {**existing_run, **run_md}
        else:
            extra_md["run"] = dict(run_md)

    for r in ds_iter:
        doc = _build_doc(dataset_name=dataset_name, record=r, dcfg=dcfg, extra_metadata=extra_md)
        doc = pipe.process_doc(doc=doc)
        buffer.append(doc)
        total += 1

        if len(buffer) >= shard_size:
            shard = f"part-{shard_id:05d}.jsonl"
            save_records(
                buffer,
                cfg["storage"]["mode"],
                os.path.join(cfg["storage"]["local_dir"], local_subdir, shard),
                cfg["aws"]["s3_bucket"],
                f"{cfg['aws']['s3_prefix']}/{s3_subdir}/{shard}",
            )
            buffer.clear()
            shard_id += 1

    if buffer:
        shard = f"part-{shard_id:05d}.jsonl"
        save_records(
            buffer,
            cfg["storage"]["mode"],
            os.path.join(cfg["storage"]["local_dir"], local_subdir, shard),
            cfg["aws"]["s3_bucket"],
            f"{cfg['aws']['s3_prefix']}/{s3_subdir}/{shard}",
        )

    return total, shard_id + 1


def run_download(config_path: str, *, overrides: dict | None = None) -> None:
    # Import lazily to avoid importing optional heavy deps at module import time.
    from datasets import load_dataset  # type: ignore

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    overrides = overrides or {}
    for dotted_key, value in overrides.items():
        # supports: "aws.s3_bucket", "aws.region", "storage.mode", "mode"
        parts = dotted_key.split(".")
        cur = cfg
        for p in parts[:-1]:
            cur = cur[p]
        cur[parts[-1]] = value

    run_md = _build_run_metadata(cfg=cfg, config_path=config_path)
    mode = cfg["mode"]
    times: Dict[str, float] = {}
    total_start = time.time()

    for name, dcfg in cfg["datasets"].items():
        start = time.time()
        try:
            if name == "sangraha":
                for lang in dcfg["languages"]:
                    lang_dcfg = dict(dcfg)
                    base_md = (dcfg.get("metadata") or {}) if isinstance(dcfg.get("metadata"), dict) else {}
                    lang_dcfg["metadata"] = {**base_md, "lang": lang}
                    ds = load_dataset(dcfg["repo"], dcfg["subset"], split=lang, streaming=True)
                    ds_iter = apply_limit(ds, dcfg["test_limit"], mode)
                    process_stream(
                        "sangraha",
                        ds_iter,
                        10_000,
                        cfg,
                        lang_dcfg,
                        run_md,
                        f"{dcfg['local_path']}/{lang}",
                        f"{dcfg['s3_path']}/{lang}",
                    )
            else:
                load_args = {"path": dcfg["repo"], "split": dcfg.get("split", "train"), "streaming": True}
                if "name" in dcfg:
                    load_args["name"] = dcfg["name"]
                ds = load_dataset(**load_args)
                ds_iter = apply_limit(ds, dcfg["test_limit"], mode)
                process_stream(name, ds_iter, 10_000, cfg, dcfg, run_md, dcfg["local_path"], dcfg["s3_path"])

            times[name] = time.time() - start
        except Exception:
            times[name] = -1

    _ = time.time() - total_start

