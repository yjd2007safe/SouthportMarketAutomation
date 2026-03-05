#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"

DEFAULT_DATE="$(date -u +%F)"
DATE="${DEFAULT_DATE}"
SOURCE=""
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
  * Provide either --source or --normalized-input.
  * The script exits non-zero if any stage fails.
USAGE
}

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      SOURCE="${2:-}"
      shift 2
      ;;
    --normalized-input)
      NORMALIZED_INPUT="${2:-}"
      shift 2
      ;;
    --date)
      DATE="${2:-}"
      shift 2
      ;;
    --raw-dir)
      RAW_DIR="${2:-}"
      shift 2
      ;;
    --normalized-dir)
      NORMALIZED_DIR="${2:-}"
      shift 2
      ;;
    --reports-dir)
      REPORTS_DIR="${2:-}"
      shift 2
      ;;
    --log-dir)
      LOG_DIR="${2:-}"
      shift 2
      ;;
    --analysis-prefix)
      ANALYSIS_PREFIX="${2:-}"
      shift 2
      ;;
    --report-prefix)
      REPORT_PREFIX="${2:-}"
      shift 2
      ;;
    --with-supabase)
      WITH_SUPABASE=1
      shift
      ;;
    --supabase-source)
      SUPABASE_SOURCE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${SOURCE}" && -z "${NORMALIZED_INPUT}" ]]; then
  echo "Error: provide --source or --normalized-input." >&2
  usage >&2
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
  RAW_PATH="$(python3 - "${SOURCE}" "${RAW_DIR}" "${DATE}" <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sys

import ingest
import requests as safe_requests

source = sys.argv[1]
raw_dir = Path(sys.argv[2])
run_date = datetime.strptime(sys.argv[3], "%Y-%m-%d").replace(tzinfo=timezone.utc)

source_type, normalized_source = ingest.resolve_source(source)
output_path = ingest.create_output_path(raw_dir, normalized_source, timestamp=run_date)

if source_type == "url":
    body = safe_requests.fetch_text(normalized_source)
    output_path.write_text(body, encoding="utf-8")
else:
    src_path = Path(normalized_source)
    if not src_path.exists():
        raise FileNotFoundError(f"Source file not found: {src_path}")
    if src_path.suffix.lower() in {".csv", ".json"}:
        output_path = output_path.with_suffix(src_path.suffix.lower())
    shutil.copy2(src_path, output_path)

print(output_path)
PY
)"
  log "[stage:ingest] complete raw_path=${RAW_PATH}"

  log "[stage:normalize] begin"
  NORMALIZED_PATH="$(python3 - "${RAW_PATH}" "${NORMALIZED_DIR}" "${DATE}" <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sys

raw_path = Path(sys.argv[1])
normalized_dir = Path(sys.argv[2])
run_date = datetime.strptime(sys.argv[3], "%Y-%m-%d").replace(tzinfo=timezone.utc)

normalized_dir.mkdir(parents=True, exist_ok=True)
ext = raw_path.suffix.lower() if raw_path.suffix.lower() in {".json", ".csv"} else ".json"
out_name = f"normalized_{run_date.strftime('%Y%m%dT%H%M%SZ')}{ext}"
out_path = normalized_dir / out_name
shutil.copy2(raw_path, out_path)
print(out_path)
PY
)"
  log "[stage:normalize] complete normalized_path=${NORMALIZED_PATH}"
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
