# SouthportMarketAutomation

Automated Southport apartment market data pipeline (ingest, normalize, analyze, report).

## Usage

### 1) Set up a local environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run tests

```bash
pytest
```

### 3) Run ingest bootstrap command

Use the helper script to run the ingest module with `src/` on `PYTHONPATH`.

```bash
scripts/run_ingest.sh --source ./path/to/listings.csv
```

You can also specify output location and filename stem:

```bash
scripts/run_ingest.sh \
  --source https://example.com/feed.json \
  --output-dir data/raw \
  --filename daily_snapshot
```

### 4) Run normalization + dedup stage

Normalize a raw snapshot (JSONL), deduplicate records, and write clean JSONL/CSV outputs:

```bash
python -m normalize --date 2025-01-14
```

By default this reads `data/raw/YYYY-MM-DD.jsonl` and writes:

- `data/clean/YYYY-MM-DD.jsonl`
- `data/clean/YYYY-MM-DD.csv`

You can also provide an explicit input file:

```bash
python -m normalize --input data/raw/custom_snapshot.jsonl --date 2025-01-14
```

### Ingest CLI reference

`ingest` currently supports:

- `--source` (required): local file path or `http(s)` URL.
- `--output-dir` (optional): destination directory for generated raw output path (default: `data/raw`).
- `--filename` (optional): override output filename stem.

### Normalize CLI reference

`normalize` currently supports:

- `--input` (optional): explicit raw JSONL input path.
- `--date` (optional): run date (`YYYY-MM-DD`) used for default input and output filenames.
- `--raw-dir` (optional): base raw directory when `--input` is omitted (default: `data/raw`).
- `--clean-dir` (optional): destination directory for normalized outputs (default: `data/clean`).
