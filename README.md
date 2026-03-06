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

### Ingest CLI reference

`ingest` currently supports:

- `--source` (required): local file path or `http(s)` URL.
- `--output-dir` (optional): destination directory for generated raw output path (default: `data/raw`).
- `--filename` (optional): override output filename stem.



### Discover source candidates for Southport (or other AU areas)

Generate a structured source list with URL/site/category/confidence/notes:

```bash
PYTHONPATH=src python -m discover_sources   --area Southport   --include-expansion   --output data/sources/southport_sources.json
```

Only retain validated ingestable listing/search pages:

```bash
PYTHONPATH=src python -m discover_sources   --area Southport   --ingestable-only   --output data/sources/southport_ingestable.yaml
```

### 4) Run analysis module

Once you have normalized records (JSON or CSV), run analysis to generate
machine-readable reports (`.json`, `.csv`) and a markdown summary under
`reports/` by default.

```bash
PYTHONPATH=src python -m analyze --input data/normalized/listings.json
```

Custom output directory and filename prefix:

```bash
PYTHONPATH=src python -m analyze \
  --input data/normalized/listings.csv \
  --reports-dir reports \
  --prefix weekly_snapshot
```


### 5) Run report module

After `analyze` writes its outputs to `reports/`, run `report` to generate
final market report artifacts (`.md`, `.csv`, and `.json`) in the same directory.

```bash
PYTHONPATH=src python -m report   --reports-dir reports   --analysis-prefix market_analysis   --output-prefix market_report
```

### Full pipeline example (ingest -> analyze -> report)

```bash
scripts/run_ingest.sh --source ./path/to/listings.csv --output-dir data/raw --filename snapshot
PYTHONPATH=src python -m analyze --input data/normalized/listings.json --reports-dir reports --prefix market_analysis
PYTHONPATH=src python -m report --reports-dir reports --analysis-prefix market_analysis --output-prefix market_report
```


### 6) Run the full daily pipeline in one command

Use `scripts/run_daily.sh` to orchestrate ingest -> normalize -> analyze -> report
with stage logs, directory bootstrapping, and non-zero exits on failure.

```bash
scripts/run_daily.sh \
  --source ./path/to/listings.csv \
  --date 2025-03-05
```

Optional overrides:

```bash
scripts/run_daily.sh \
  --source https://example.com/listings.json \
  --raw-dir data/raw \
  --normalized-dir data/normalized \
  --reports-dir reports \
  --log-dir logs/daily \
  --analysis-prefix market_analysis \
  --report-prefix market_report
```

If normalized data already exists, skip ingest/normalize:

```bash
scripts/run_daily.sh --normalized-input data/normalized/listings.csv
```

To ingest from a discovered source list and iterate ingestable pages:

```bash
scripts/run_daily.sh \
  --source-list data/sources/southport_sources.json \
  --date 2025-03-05
```

Show CLI help:

```bash
scripts/run_daily.sh --help
```


## Supabase persistence

Schema migration SQL for Supabase is provided in:

- `db/migrations/001_supabase_market_tables.sql`
- `docs/supabase_schema.md`

Set credentials before running the load stage:

```bash
export SUPABASE_URL="https://<project>.supabase.co"
export SUPABASE_KEY="<service-role-or-insert-key>"
```

Run standalone loader:

```bash
PYTHONPATH=src python -m load_to_supabase   --normalized-input data/normalized/listings.json   --summary-json reports/market_analysis.json   --raw-input data/raw/listings.json   --date 2025-03-05   --source southport_daily
```

Or enable Supabase loading in the daily orchestrator:

```bash
scripts/run_daily.sh   --source ./path/to/listings.csv   --date 2025-03-05   --with-supabase   --supabase-source southport_daily
```

Without `--with-supabase`, local behavior is unchanged.
