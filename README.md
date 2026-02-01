## Data Radar Dolma Wrapper

This repo downloads multiple LLM pretraining sources (HF streaming), runs a **normalization + Dolma-toolkit sanitization trigger** (PII masking), and writes **cleaned JSONL shards** to local disk and/or S3.

### What you asked for (implemented)

- **Dolma toolkit vendored** at `vendor/dolma/` (cloned from `https://github.com/allenai/dolma.git`).
- **Wrapper package + CLI** at `src/data_radar_dolma_wrapper/` with command `drdw`.
- **Normalization layer** (Unicode normalization + whitespace collapse) + **Dolma PII masking** using Dolma taggers (default: `pii_regex_v2`).
- Output JSONL is “document style” and ready for upload/training:
  - `id`, `text`, `source`, `metadata`, `created`, `added`, `version`
  - plus **run traceability** under `metadata.run` (timestamp, run id, etc.)

### Setup (Windows / PowerShell)

```bash
python -m venv .venv
.venv\\Scripts\\activate
python -m pip install -U pip
python -m pip install -e .
python -m pip install -r requirements.txt
```

### Setup (Linux / macOS)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e .
python -m pip install -r requirements.txt
```

### Configure

Edit `configs/config.yml`:

- `aws.s3_bucket`, `aws.region`, `aws.s3_prefix`
- `storage.mode`: `local` | `s3` | `both`
- `processing.*` (enable/disable Dolma toolkit, set mask token, etc.)
- add more datasets under `datasets:`

For local machine overrides, create `configs/config.local.yml` (this repo ships a starter template) and keep it uncommitted:

- `configs/config.local.yml` is **ignored by git** (good place for your S3 bucket, region, etc.)
- `python scripts/download.py` will prefer `configs/config.local.yml` automatically if it exists

### Output metadata (including run timestamps)

Every record includes a `metadata` object built from:

- **Global metadata**: `processing.metadata`
- **Per-dataset metadata**: `datasets.<dataset_name>.metadata`
- **Per-language metadata** (Sangraha): `lang` is injected automatically
- **Run metadata** (auto-generated): `metadata.run` (when enabled)

Enable/configure run metadata in `configs/config.yml`:

```yaml
processing:
  run_metadata:
    enabled: true
    include:
      - run_id
      - run_started_at
      - config_path
      - mode
      - storage_mode
      - s3_bucket
      - s3_prefix
      - wrapper_version
```

### Run (test mode)

```bash
drdw download --config configs/config.yml --mode test
```

Or run via the repo script:

```bash
python scripts/download.py --config configs/config.yml --mode test
```

### Run (full mode)

```bash
drdw download --config configs/config.yml --mode full
```

Or:

```bash
python scripts/download.py --config configs/config.yml --mode full
```

### Notes on Dolma usage

- The wrapper will try to import Dolma from the vendored clone (`vendor/dolma/python`) automatically.
- If you prefer the published wheel instead, you can also do:

```bash
python -m pip install dolma
```

### Where to make code changes (extending metadata + adding Dolma taggers)

#### Extend output metadata

- **Config-only changes (preferred)**:
  - Global: `configs/config.yml` → `processing.metadata`
  - Per dataset: `configs/config.yml` → `datasets.<name>.metadata`
  - Run metadata fields: `configs/config.yml` → `processing.run_metadata.include`

- **Code changes (for new auto-derived fields)**:
  - **Run-level fields** (e.g., hostname, git SHA, python version):
    - Edit `_build_run_metadata(...)` in `src/data_radar_dolma_wrapper/downloader.py`
  - **Per-record fields** (copy from HF row into `metadata`):
    - Edit `_build_doc(...)` in `src/data_radar_dolma_wrapper/downloader.py`

#### Add a new Dolma tagger / trigger

- **Where to put new tagger code**:
  - Create a module under `src/data_radar_dolma_wrapper/taggers/` (e.g. `my_taggers.py`)
  - Use Dolma’s `@add_tagger("my_tagger_name")` decorator to register it.

- **Where to wire it into the pipeline**:
  - Edit `ProcessingPipeline.process_doc(...)` in `src/data_radar_dolma_wrapper/downloader.py`
    - This is where PII masking runs today (`pii_regex_v2` + `mask_spans(...)`).
    - Add logic to load/run your new tagger and decide what to do with spans:
      - add counters to `metadata`
      - mask spans
      - drop documents

