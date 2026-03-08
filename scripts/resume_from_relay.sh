#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"

HANDOFF=""
PAYLOAD=""
NORMALIZED_DIR="data/normalized"
REPORTS_DIR="reports"
DATE="$(date -u +%F)"
ANALYSIS_PREFIX="market_analysis"
REPORT_PREFIX="market_report"
WITH_SUPABASE=0
SUPABASE_SOURCE="southport_daily_relay"

usage() {
  cat <<'USAGE'
Usage: scripts/resume_from_relay.sh --handoff PATH --payload PATH [options]

Ingest a manual relay payload and resume pipeline stages:
  normalize(materialize) -> analyze -> report -> optional supabase

Options:
  --handoff PATH           Pending handoff artifact JSON.
  --payload PATH           Manual relay extraction payload JSON.
  --normalized-dir DIR     Output dir for normalized relay JSON (default: data/normalized).
  --reports-dir DIR        Reports dir (default: reports).
  --date YYYY-MM-DD        Run date for optional supabase stage (default: today UTC).
  --analysis-prefix NAME   Analysis artifact prefix (default: market_analysis).
  --report-prefix NAME     Report artifact prefix (default: market_report).
  --with-supabase          Run load_to_supabase after report.
  --supabase-source NAME   Source label for Supabase (default: southport_daily_relay).
USAGE
}

log() { printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --handoff) HANDOFF="${2:-}"; shift 2 ;;
    --payload) PAYLOAD="${2:-}"; shift 2 ;;
    --normalized-dir) NORMALIZED_DIR="${2:-}"; shift 2 ;;
    --reports-dir) REPORTS_DIR="${2:-}"; shift 2 ;;
    --date) DATE="${2:-}"; shift 2 ;;
    --analysis-prefix) ANALYSIS_PREFIX="${2:-}"; shift 2 ;;
    --report-prefix) REPORT_PREFIX="${2:-}"; shift 2 ;;
    --with-supabase) WITH_SUPABASE=1; shift ;;
    --supabase-source) SUPABASE_SOURCE="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "${HANDOFF}" || -z "${PAYLOAD}" ]]; then
  echo "Error: --handoff and --payload are required." >&2
  usage >&2
  exit 2
fi

mkdir -p "${NORMALIZED_DIR}" "${REPORTS_DIR}"

log "[stage:relay_resume] validating handoff=${HANDOFF} payload=${PAYLOAD}"
python3 -m relay_handoff validate --handoff "${HANDOFF}" --payload "${PAYLOAD}"

log "[stage:relay_resume] materializing normalized records"
NORMALIZED_PATH="$(python3 -m relay_handoff materialize --handoff "${HANDOFF}" --payload "${PAYLOAD}" --normalized-dir "${NORMALIZED_DIR}")"
python3 - "${HANDOFF}" "${PAYLOAD}" "${NORMALIZED_PATH}" <<'PY'
from pathlib import Path
import relay_handoff
import sys

relay_handoff.mark_handoff_status(
    Path(sys.argv[1]),
    status="resumed",
    note=f"Resume command accepted payload {sys.argv[2]} and wrote {sys.argv[3]}",
)
PY
log "[stage:relay_resume] resumed normalized_path=${NORMALIZED_PATH}"

log "[stage:analyze] begin"
python3 -m analyze --input "${NORMALIZED_PATH}" --reports-dir "${REPORTS_DIR}" --prefix "${ANALYSIS_PREFIX}"
log "[stage:analyze] complete"

log "[stage:report] begin"
REPORT_ARGS=(
  --reports-dir "${REPORTS_DIR}"
  --analysis-prefix "${ANALYSIS_PREFIX}"
  --output-prefix "${REPORT_PREFIX}"
  --date "${DATE}"
  --source "${SUPABASE_SOURCE}"
  --report-type "${REPORT_PREFIX}"
  --report-version "v1"
  --local-output-mode "none"
)
if [[ "${WITH_SUPABASE}" -eq 1 ]]; then
  REPORT_ARGS+=(--persist-supabase)
fi
python3 -m report "${REPORT_ARGS[@]}"
log "[stage:report] complete"

if [[ "${WITH_SUPABASE}" -eq 1 ]]; then
  log "[stage:supabase_load] begin"
  python3 -m load_to_supabase \
    --normalized-input "${NORMALIZED_PATH}" \
    --summary-json "${REPORTS_DIR}/${ANALYSIS_PREFIX}.json" \
    --date "${DATE}" \
    --source "${SUPABASE_SOURCE}"
  log "[stage:supabase_load] complete"
fi

python3 - "${HANDOFF}" <<'PY'
from pathlib import Path
import relay_handoff
import sys

relay_handoff.mark_handoff_status(Path(sys.argv[1]), status="completed", note="Resume pipeline completed")
PY
log "Relay resume pipeline finished successfully"
