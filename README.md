# SouthportMarketAutomation

Automated Southport apartment market data pipeline (ingest, normalize, analyze, report).

## Minimal ingest scaffold

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Sources file format

Create a text file with one source per line:

- `source_name,https://example.com/feed`
- `https://example.com` (source defaults to URL)

Lines starting with `#` and blank lines are ignored.

### Run ingest

```bash
scripts/run_ingest.sh --sources-file data/sources.txt --max-snippet-chars 300
```

This writes JSONL output to `data/raw/YYYY-MM-DD.jsonl` with fields:
`source`, `url`, `fetched_at`, `text_snippet`, `status`, `error`.

Extra options:

- `--timeout` request timeout in seconds (default `10`)
- `--retries` retry attempts after the initial request (default `2`)

### Test

```bash
pytest
```
