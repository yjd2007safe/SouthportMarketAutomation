#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"

DEFAULT_DATE="$(date -u +%F)"
DATE="${DEFAULT_DATE}"
SOURCE=""
SOURCE_LIST=""
NORMALIZED_INPUT=""
RAW_DIR="data/raw"
NORMALIZED_DIR="data/normalized"
REPORTS_DIR="reports"
LOG_DIR="logs/daily"
ANALYSIS_PREFIX="market_analysis"
REPORT_PREFIX="market_report"
WITH_SUPABASE=0
SUPABASE_SOURCE="southport_daily"

usage() {
  cat <<'USAGE'
Usage: scripts/run_daily.sh [options]

Run the full Southport daily pipeline:
  ingest -> normalize -> analyze -> report

Options:
  --source PATH_OR_URL       Raw source path or http(s) URL for ingest stage.
  --source-list PATH         JSON/YAML/TXT source list; iterate ingest over ingestable entries.
  --normalized-input PATH    Skip ingest/normalize and use this normalized CSV/JSON.
  --date YYYY-MM-DD          Run date used in output naming (default: current UTC date).
  --raw-dir DIR              Directory for ingested raw snapshots (default: data/raw).
  --normalized-dir DIR       Directory for normalized outputs (default: data/normalized).
  --reports-dir DIR          Directory for analysis/report artifacts (default: reports).
  --log-dir DIR              Directory for run logs (default: logs/daily).
  --analysis-prefix PREFIX   Prefix for analysis outputs (default: market_analysis).
  --report-prefix PREFIX     Prefix for report outputs (default: market_report).
  --with-supabase            Run optional Supabase load stage after report.
  --supabase-source SOURCE   Source label used for Supabase upserts (default: southport_daily).
  -h, --help                 Show this help text.

Notes:
  * Provide --source, --source-list, or --normalized-input.
  * The script exits non-zero if any stage fails.
USAGE
}

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) SOURCE="${2:-}"; shift 2 ;;
    --source-list) SOURCE_LIST="${2:-}"; shift 2 ;;
    --normalized-input) NORMALIZED_INPUT="${2:-}"; shift 2 ;;
    --date) DATE="${2:-}"; shift 2 ;;
    --raw-dir) RAW_DIR="${2:-}"; shift 2 ;;
    --normalized-dir) NORMALIZED_DIR="${2:-}"; shift 2 ;;
    --reports-dir) REPORTS_DIR="${2:-}"; shift 2 ;;
    --log-dir) LOG_DIR="${2:-}"; shift 2 ;;
    --analysis-prefix) ANALYSIS_PREFIX="${2:-}"; shift 2 ;;
    --report-prefix) REPORT_PREFIX="${2:-}"; shift 2 ;;
    --with-supabase) WITH_SUPABASE=1; shift ;;
    --supabase-source) SUPABASE_SOURCE="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${SOURCE}" && -z "${SOURCE_LIST}" && -z "${NORMALIZED_INPUT}" ]]; then
  echo "Error: provide --source, --source-list, or --normalized-input." >&2
  usage >&2
  exit 2
fi

if [[ -n "${SOURCE}" && -n "${SOURCE_LIST}" ]]; then
  echo "Error: provide only one of --source or --source-list." >&2
  exit 2
fi

date -u -d "${DATE}" +%F >/dev/null 2>&1 || {
  echo "Error: invalid --date '${DATE}'. Expected YYYY-MM-DD." >&2
  exit 2
}

mkdir -p "${RAW_DIR}" "${NORMALIZED_DIR}" "${REPORTS_DIR}" "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/run_${DATE}.log"
exec > >(tee -a "${LOG_FILE}") 2>&1

log "Starting daily pipeline date=${DATE}"
log "Directories raw=${RAW_DIR} normalized=${NORMALIZED_DIR} reports=${REPORTS_DIR} logs=${LOG_DIR}"

RAW_PATH=""
NORMALIZED_PATH="${NORMALIZED_INPUT}"

if [[ -z "${NORMALIZED_INPUT}" ]]; then
  log "[stage:ingest] begin"
  SOURCE_ITEMS=()

  if [[ -n "${SOURCE_LIST}" ]]; then
    if [[ ! -f "${SOURCE_LIST}" ]]; then
      echo "Error: source list file not found: ${SOURCE_LIST}" >&2
      exit 1
    fi

    mapfile -t SOURCE_ITEMS < <(python3 - "${SOURCE_LIST}" <<'PY'
from pathlib import Path
import sys

import discover_sources

items = discover_sources.load_sources_file(Path(sys.argv[1]))
for source in discover_sources.filter_ingestable_sources(items):
    print(source["url"])
PY
)

    if [[ "${#SOURCE_ITEMS[@]}" -eq 0 ]]; then
      echo "Error: no ingestable sources found in ${SOURCE_LIST}" >&2
      exit 1
    fi
  else
    SOURCE_ITEMS=("${SOURCE}")
  fi

  NORMALIZED_PATHS=()
  RAW_PATHS=()
  SUMMARY_LINES=()
  SUCCESS_COUNT=0
  BLOCKED_COUNT=0
  FAILED_COUNT=0

  for SOURCE_ITEM in "${SOURCE_ITEMS[@]}"; do
    INGEST_RESULT="$(python3 - "${SOURCE_ITEM}" "${RAW_DIR}" "${DATE}" <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sys

import ingest
import requests as safe_requests
from urllib.parse import urlparse

source = sys.argv[1]
raw_dir = Path(sys.argv[2])
run_date = datetime.strptime(sys.argv[3], "%Y-%m-%d").replace(tzinfo=timezone.utc)

source_type, normalized_source = ingest.resolve_source(source)
output_path = ingest.create_output_path(raw_dir, normalized_source, timestamp=run_date)

try:
    if source_type == "url":
        src_suffix = Path(urlparse(normalized_source).path).suffix.lower()
        if src_suffix in {".csv", ".json", ".html", ".htm"}:
            output_path = output_path.with_suffix(src_suffix)
        body = safe_requests.fetch_text(normalized_source)
        output_path.write_text(body, encoding="utf-8")
    else:
        src_path = Path(normalized_source)
        if not src_path.exists():
            raise FileNotFoundError(f"Source file not found: {src_path}")
        if src_path.suffix.lower() in {".csv", ".json", ".html", ".htm"}:
            output_path = output_path.with_suffix(src_path.suffix.lower())
        shutil.copy2(src_path, output_path)
except safe_requests.BlockedSourceError as exc:
    print(f"STATUS=blocked\nDETAIL={exc}")
    raise SystemExit(0)
except Exception as exc:
    print(f"STATUS=failed\nDETAIL={exc}")
    raise SystemExit(0)

print(f"STATUS=ok\nRAW_PATH={output_path}")
PY
)"

    STATUS="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^STATUS=/{print $2; exit}')"
    if [[ "${STATUS}" == "ok" ]]; then
      RAW_PATH="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^RAW_PATH=/{print $2; exit}')"
      RAW_PATHS+=("${RAW_PATH}")
      log "[stage:ingest] complete source=${SOURCE_ITEM} raw_path=${RAW_PATH}"

      NORMALIZED_PATH="$(python3 - "${RAW_PATH}" "${NORMALIZED_DIR}" "${DATE}" "${SOURCE_ITEM}" <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sys
import json
from urllib.parse import urlparse

import scrape_listings

raw_path = Path(sys.argv[1])
normalized_dir = Path(sys.argv[2])
run_date = datetime.strptime(sys.argv[3], "%Y-%m-%d").replace(tzinfo=timezone.utc)
source_item = sys.argv[4]

normalized_dir.mkdir(parents=True, exist_ok=True)
source_suffix = Path(urlparse(source_item).path).suffix.lower()
is_structured = source_suffix in {".csv", ".json"}
ext = source_suffix if is_structured else ".json"
out_name = f"normalized_{raw_path.stem}_{run_date.strftime('%Y%m%dT%H%M%SZ')}{ext}"
out_path = normalized_dir / out_name

if is_structured:
    shutil.copy2(raw_path, out_path)
else:
    html = raw_path.read_text(encoding="utf-8")
    rows = scrape_listings.parse_listing_page(source_item, html)
    out_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")

print(out_path)
PY
)"
      NORMALIZED_PATHS+=("${NORMALIZED_PATH}")
      SUMMARY_LINES+=("ok|${SOURCE_ITEM}|${RAW_PATH}|${NORMALIZED_PATH}")
      SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
      log "[stage:normalize] complete source=${SOURCE_ITEM} normalized_path=${NORMALIZED_PATH}"
    elif [[ "${STATUS}" == "blocked" ]]; then
      DETAIL="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^DETAIL=/{print substr($0,8); exit}')"
      SUMMARY_LINES+=("blocked|${SOURCE_ITEM}|${DETAIL}")
      BLOCKED_COUNT=$((BLOCKED_COUNT + 1))
      log "[stage:ingest] blocked source=${SOURCE_ITEM} detail=${DETAIL}"
    else
      DETAIL="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^DETAIL=/{print substr($0,8); exit}')"
      SUMMARY_LINES+=("failed|${SOURCE_ITEM}|${DETAIL}")
      FAILED_COUNT=$((FAILED_COUNT + 1))
      log "[stage:ingest] failed source=${SOURCE_ITEM} detail=${DETAIL}"
    fi
  done

  log "[stage:source-summary] begin"
  for summary in "${SUMMARY_LINES[@]}"; do
    IFS='|' read -r state src a b <<<"${summary}"
    if [[ "${state}" == "ok" ]]; then
      log "[stage:source-summary] status=ok source=${src} raw_path=${a} normalized_path=${b}"
    else
      log "[stage:source-summary] status=${state} source=${src} detail=${a}"
    fi
  done
  log "[stage:source-summary] totals success=${SUCCESS_COUNT} blocked=${BLOCKED_COUNT} failed=${FAILED_COUNT}"

  if [[ "${#NORMALIZED_PATHS[@]}" -eq 0 ]]; then
    echo "Error: all sources blocked or failed; no normalized outputs available." >&2
    exit 1
  fi

  if [[ "${#NORMALIZED_PATHS[@]}" -gt 1 ]]; then
    COMBINED_PATH="${NORMALIZED_DIR}/normalized_${DATE}_combined.json"
    python3 - "${COMBINED_PATH}" "${NORMALIZED_PATHS[@]}" <<'PY'
from pathlib import Path
import csv
import json
import sys

output = Path(sys.argv[1])
inputs = [Path(path) for path in sys.argv[2:]]
rows = []

for path in inputs:
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            rows.extend(data)
    elif path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as fh:
            rows.extend(list(csv.DictReader(fh)))

output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
PY
    NORMALIZED_PATH="${COMBINED_PATH}"
    RAW_PATH="${RAW_PATHS[0]}"
    log "[stage:normalize] combined source list normalized_path=${NORMALIZED_PATH}"
  else
    NORMALIZED_PATH="${NORMALIZED_PATHS[0]}"
    RAW_PATH="${RAW_PATHS[0]}"
  fi
else
  log "[stage:normalize] skipped using provided normalized input path=${NORMALIZED_INPUT}"
fi

if [[ ! -f "${NORMALIZED_PATH}" ]]; then
  echo "Error: normalized input not found: ${NORMALIZED_PATH}" >&2
  exit 1
fi

log "[stage:analyze] begin"
python3 -m analyze --input "${NORMALIZED_PATH}" --reports-dir "${REPORTS_DIR}" --prefix "${ANALYSIS_PREFIX}"
log "[stage:analyze] complete"

log "[stage:report] begin"
python3 -m report --reports-dir "${REPORTS_DIR}" --analysis-prefix "${ANALYSIS_PREFIX}" --output-prefix "${REPORT_PREFIX}"
log "[stage:report] complete"

if [[ "${WITH_SUPABASE}" -eq 1 ]]; then
  log "[stage:supabase_load] begin"
  LOAD_ARGS=(
    --normalized-input "${NORMALIZED_PATH}"
    --summary-json "${REPORTS_DIR}/${ANALYSIS_PREFIX}.json"
    --date "${DATE}"
    --source "${SUPABASE_SOURCE}"
  )

  if [[ -n "${RAW_PATH}" ]]; then
    LOAD_ARGS+=(--raw-input "${RAW_PATH}")
  fi

  python3 -m load_to_supabase "${LOAD_ARGS[@]}"
  log "[stage:supabase_load] complete"
fi

log "Daily pipeline finished successfully"
