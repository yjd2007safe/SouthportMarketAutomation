# SouthportMarketAutomation

Automated Southport apartment market data pipeline (ingest, normalize, analyze, report).

## Minimal ingest bootstrap

### What it does
- Fetches one or more source URLs with timeout + retry handling.
- Writes newline-delimited JSON output to `data/raw/YYYY-MM-DD.jsonl`.
- Each record includes:
  - `source`
  - `url`
  - `fetched_at`
  - `text_snippet`

### Usage
```bash
python3 src/ingest.py \
  --source "https://example.com" \
  --source "https://www.python.org" \
  --timeout 10 \
  --retries 2
```

or via helper script:

```bash
scripts/run_ingest.sh --source "https://example.com"
```

### Run tests
```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```
