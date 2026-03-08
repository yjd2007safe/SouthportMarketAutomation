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


### Web listing page parsing support (run_daily)

When `run_daily.sh` ingests sources from `--source-list`, web search/listing pages are now parsed into structured listing JSON before analysis.

Supported adapters:

- **onthehouse**: implemented parser for listing/search HTML (prefers JSON-LD, with basic card-text fallback).
  - Works for both rental and **FOR SALE** search/listing routes (for example `/for-sale/qld/gold-coast/southport`).
- **realestate**: adapter placeholder present (currently returns no records).
- **domain**: adapter placeholder present (currently returns no records).

Structured output records include:

- `listing_id` (stable hash id)
- `url`
- `address` (when available)
- `rent` / `price`
- `bedrooms`
- `bathrooms` (optional)
- `size_sqft` (optional)
- `listed_date` (optional)
- `source_site`
- `raw_snippet`

Fallback behavior:

- File/JSON/CSV source modes are preserved and passed through unchanged.
- Unsupported or non-matching HTML pages normalize to an empty JSON list (`[]`) rather than raw HTML.



### Reliability updates for production extraction

- Source ingestion now routes URLs through modular fetch backends: `relay` (attached Chrome tab/session), `browser` (Playwright), `http`, and `proxy-http`.
- Domain policy defaults to browser-layer rendering for anti-bot-prone domains (for example `realestate.com.au`) with graceful fallback across backends.
- Proxy transport supports optional rotating endpoints (environment/file configured), conservative rate limiting, and bounded retry attempts.
- Persistent HTTP 429 responses are treated as blocked-source events, allowing source-list daily runs to continue with remaining sources.
- `run_daily.sh` now prints per-source diagnostics including `backend_used`, `attempts`, `stability_profile`, challenge classification/retry flags, and failure `reason` (for blocked/challenge/parse_failed), in addition to status summary (`ok`, `blocked`, `failed`, `parse_failed`).
- Onthehouse extraction now supports both JSON-LD and modern `__NEXT_DATA__`-style payloads.
- Supabase raw-load safely skips malformed/raw HTML payload files to avoid JSON decode crashes.

Fetch policy environment configuration:

```bash
# Retry/pace policy
export SMA_FETCH_MAX_ATTEMPTS=3
export SMA_FETCH_RATE_LIMIT_SECONDS=0.5
export SMA_FETCH_BACKOFF_BASE=0.5
export SMA_FETCH_JITTER_RATIO=0.2

# Domain routing
export SMA_FETCH_BROWSER_DOMAINS="realestate.com.au"
export SMA_FETCH_PROXY_DOMAINS="domain1.com,domain2.com"
export SMA_FETCH_DOMAIN_BACKENDS="realestate.com.au=browser,api.example.com=proxy-http"
export SMA_FETCH_RELAY_DOMAINS="realestate.com.au,domain.com.au,onthehouse.com.au"

# Optional rotating proxy endpoints
export SMA_FETCH_PROXY_ENDPOINTS="http://proxy-a:8080,http://proxy-b:8080"
# ...or load proxies from file (one endpoint per line)
export SMA_FETCH_PROXY_FILE="config/proxies.txt"
```

Safety note: the browser backend only performs normal page rendering/navigation and does **not** implement captcha bypass or evasion hacks.


Navigation profile support (anti-bot safe browser/relay flow):

- You can set `SMA_NAV_PROFILE=onthehouse_sale_southport` to avoid relying on fixed deep-link URLs.
- The profile starts from `https://www.onthehouse.com.au`, performs a homepage search for Southport QLD **FOR SALE** listings, waits for stable listing results, then extracts HTML for existing parsers.
- For `--source-list`, per-source metadata can override env default using `"navigation_profile": "onthehouse_sale_southport"`.
- Direct URL sources remain backward-compatible: if no navigation profile is set, fetch behaves exactly as before.

Example source-list entry:

```json
{
  "url": "https://www.onthehouse.com.au/for-sale/qld/gold-coast/southport",
  "site": "onthehouse.com.au",
  "category": "search",
  "confidence": 0.95,
  "notes": "Southport FOR SALE listings",
  "navigation_profile": "onthehouse_sale_southport"
}
```

Operator instructions for relay/browser runs with navigation profiles:

1. Keep relay-attached browser session open and authenticated before scheduled run.
2. Export `SMA_NAV_PROFILE=onthehouse_sale_southport` (or set source metadata override).
3. Use `--stability-profile slow` for fragile sessions/challenge-prone windows.
4. Verify `run_daily` logs include `nav_profile=onthehouse_sale_southport` for affected sources.


Slow-stable profile (`--stability-profile slow`) operational guidance:

- Keeps default behavior unchanged unless explicitly enabled.
- Increases wait windows and adds random jitter to pacing/backoff.
- Reduces effective backend parallelism intent to 1 for gentler crawling cadence.
- Browser/relay validation waits for listing-like selectors/content and rejects challenge pages as successful fetches.
- When challenge markers are detected, diagnostics classify provider (for example `kasada`/`incapsula`/`captcha`) and perform one optional longer cool-down retry before fallback.

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

Relay-first mode (uses attached Chrome session when supported, then falls back automatically):

```bash
scripts/run_daily.sh \
  --source-list data/sources/southport_sources.json \
  --fetch-mode relay
```

Slow-stable mode for anti-bot-sensitive sources:

```bash
scripts/run_daily.sh \
  --source-list data/sources/southport_sources.json \
  --fetch-mode relay \
  --stability-profile slow
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

Operator note for scheduled relay runs: ensure the relay tab is attached and logged in **before** the run starts (leave it ON/connected for `realestate`, `domain`, and `onthehouse` sources). If relay extraction fails, pipeline automatically falls back to browser/proxy/http backends.

Show CLI help:

```bash
scripts/run_daily.sh --help
```


### Human-in-the-loop relay fallback (priority-source challenge/blocked)

`run_daily.sh` now supports a structured manual relay handoff when the **priority source** (first source in `--source-list`, or the single `--source`) is challenge/blocked.

Behavior:

- Emits handoff artifact: `data/handoffs/pending_relay_*.json` (or `--handoff-dir`).
- Logs waiting/resumed/completed status in `run_daily` output.
- Waits for operator payload up to `--relay-timeout-seconds` (default `900`), polling every `--relay-poll-seconds` (default `5`).
- Aborts with non-zero exit and marks handoff `status=timed_out` if payload never arrives.

Stable handoff schema (`pending_relay_*.json`):

- `schema_version`
- `handoff_id`
- `status` (`pending`, `timed_out`, `invalid_payload`, `resumed`, `completed`)
- `run_date`
- `source_url`
- `required_schema.required_keys` (default: `listing_id`, `rent`, `snapshot_date`)
- `required_schema.min_records` (default: `1`)
- `expected_payload_path`

Manual relay payload schema (operator file at `expected_payload_path`):

```json
{
  "handoff_id": "relay_2025-03-05_ab12cd34",
  "source_url": "https://example.com/listings",
  "run_date": "2025-03-05",
  "listings": [
    {
      "listing_id": "lst_123",
      "rent": 2300,
      "snapshot_date": "2025-03-05",
      "first_seen": "2025-03-01",
      "last_seen": "2025-03-05",
      "bedrooms": 2,
      "size_sqft": 720
    }
  ]
}
```

Validation rules:

- Required top-level keys: `handoff_id`, `source_url`, `run_date`, `listings`.
- `handoff_id`, `source_url`, and `run_date` must match the pending handoff artifact.
- `listings` must be an array with at least `min_records`.
- Every listing must include all `required_keys` with non-empty values.

One-command resume (no failed fetch rerun):

```bash
scripts/resume_from_relay.sh   --handoff data/handoffs/pending_relay_<id>.json   --payload data/handoffs/relay_payload_<id>.json   --reports-dir reports
```

Optional with Supabase stage:

```bash
scripts/resume_from_relay.sh   --handoff data/handoffs/pending_relay_<id>.json   --payload data/handoffs/relay_payload_<id>.json   --date 2025-03-05   --with-supabase   --supabase-source southport_daily_relay
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
