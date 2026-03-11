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
HANDOFF_DIR="data/handoffs"
ANALYSIS_PREFIX="market_analysis"
REPORT_PREFIX="market_report"
WITH_SUPABASE=1
SUPABASE_SOURCE="southport_daily"
REPORT_LOCAL_OUTPUT_MODE="none"
FETCH_MODE="auto"
STABILITY_PROFILE="default"
RELAY_TIMEOUT_SECONDS=900
RELAY_POLL_SECONDS=5

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
  --handoff-dir DIR          Directory for manual relay handoff artifacts (default: data/handoffs).
  --analysis-prefix PREFIX   Prefix for analysis outputs (default: market_analysis).
  --report-prefix PREFIX     Prefix for report outputs (default: market_report).
  --with-supabase            Enable Supabase stages (default: enabled).
  --no-supabase              Disable Supabase stages.
  --report-local-output-mode MODE  Local report artifacts: none|persist|temp (default: none).
  --supabase-source SOURCE   Source label used for Supabase upserts (default: southport_daily).
  --fetch-mode MODE          Fetch mode for URL sources: auto|relay (default: auto).
  --stability-profile NAME   Fetch stability profile: default|slow (default: default).
                            Navigation profile comes from SMA_NAV_PROFILE or source-list metadata.navigation_profile.
  --relay-timeout-seconds N  Timeout while waiting for manual relay payload (default: 900).
  --relay-poll-seconds N     Poll interval while waiting for relay payload (default: 5).
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
    --handoff-dir) HANDOFF_DIR="${2:-}"; shift 2 ;;
    --analysis-prefix) ANALYSIS_PREFIX="${2:-}"; shift 2 ;;
    --report-prefix) REPORT_PREFIX="${2:-}"; shift 2 ;;
    --with-supabase) WITH_SUPABASE=1; shift ;;
    --no-supabase) WITH_SUPABASE=0; shift ;;
    --report-local-output-mode) REPORT_LOCAL_OUTPUT_MODE="${2:-}"; shift 2 ;;
    --supabase-source) SUPABASE_SOURCE="${2:-}"; shift 2 ;;
    --fetch-mode) FETCH_MODE="${2:-}"; shift 2 ;;
    --stability-profile) STABILITY_PROFILE="${2:-}"; shift 2 ;;
    --relay-timeout-seconds) RELAY_TIMEOUT_SECONDS="${2:-}"; shift 2 ;;
    --relay-poll-seconds) RELAY_POLL_SECONDS="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "${FETCH_MODE}" != "auto" && "${FETCH_MODE}" != "relay" ]]; then
  echo "Error: invalid --fetch-mode '${FETCH_MODE}'. Expected auto or relay." >&2
  exit 2
fi

if [[ "${STABILITY_PROFILE}" != "default" && "${STABILITY_PROFILE}" != "slow" ]]; then
  echo "Error: invalid --stability-profile '${STABILITY_PROFILE}'. Expected default or slow." >&2
  exit 2
fi

if [[ "${REPORT_LOCAL_OUTPUT_MODE}" != "none" && "${REPORT_LOCAL_OUTPUT_MODE}" != "persist" && "${REPORT_LOCAL_OUTPUT_MODE}" != "temp" ]]; then
  echo "Error: invalid --report-local-output-mode '${REPORT_LOCAL_OUTPUT_MODE}'. Expected none, persist, or temp." >&2
  exit 2
fi

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

mkdir -p "${RAW_DIR}" "${NORMALIZED_DIR}" "${REPORTS_DIR}" "${LOG_DIR}" "${HANDOFF_DIR}"
LOG_FILE="${LOG_DIR}/run_${DATE}.log"
exec > >(tee -a "${LOG_FILE}") 2>&1

log "Starting daily pipeline date=${DATE}"
log "Directories raw=${RAW_DIR} normalized=${NORMALIZED_DIR} reports=${REPORTS_DIR} logs=${LOG_DIR} handoffs=${HANDOFF_DIR} fetch_mode=${FETCH_MODE} stability_profile=${STABILITY_PROFILE} report_local_output_mode=${REPORT_LOCAL_OUTPUT_MODE} with_supabase=${WITH_SUPABASE}"

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
    url = str(source["url"]).strip()
    nav_profile = str(source.get("navigation_profile", "")).strip()
    print(f"{url}	{nav_profile}")
PY
)

    if [[ "${#SOURCE_ITEMS[@]}" -eq 0 ]]; then
      echo "Error: no ingestable sources found in ${SOURCE_LIST}" >&2
      exit 1
    fi
  else
    SOURCE_ITEMS=("${SOURCE}	${SMA_NAV_PROFILE:-}")
  fi

  IFS=$'\t' read -r PRIORITY_SOURCE PRIORITY_NAV_PROFILE <<<"${SOURCE_ITEMS[0]}"
  PRIORITY_RELAY_NEEDED=0
  PRIORITY_RELAY_RESOLVED=0
  PRIORITY_HANDOFF_PATH=""
  PRIORITY_HANDOFF_REASON=""

  NORMALIZED_PATHS=()
  NORMALIZED_META=()
  RAW_PATHS=()
  SUMMARY_LINES=()
  SUCCESS_COUNT=0
  BLOCKED_COUNT=0
  FAILED_COUNT=0
  PARSE_FAILED_COUNT=0
  CHALLENGE_BLOCKED_COUNT=0

  for SOURCE_ENTRY in "${SOURCE_ITEMS[@]}"; do
    IFS=$'\t' read -r SOURCE_ITEM SOURCE_NAV_PROFILE <<<"${SOURCE_ENTRY}"
    INGEST_RESULT="$(python3 - "${SOURCE_ITEM}" "${RAW_DIR}" "${DATE}" "${FETCH_MODE}" "${STABILITY_PROFILE}" "${SOURCE_NAV_PROFILE}" <<'PY'
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
fetch_mode = sys.argv[4]
stability_profile = sys.argv[5]
source_nav_profile = (sys.argv[6] or "").strip()

source_type, normalized_source = ingest.resolve_source(source)
output_path = ingest.create_output_path(raw_dir, normalized_source, timestamp=run_date)

backend = "local-file"
attempts = 1
outcome = "ok"
detail = ""
diag_stability_profile = stability_profile
diag_challenge = ""
diag_challenge_retry_attempted = False

try:
    if source_type == "url":
        src_suffix = Path(urlparse(normalized_source).path).suffix.lower()
        if src_suffix in {".csv", ".json", ".html", ".htm"}:
            output_path = output_path.with_suffix(src_suffix)
        fetch_result = safe_requests.fetch_with_policy(
            normalized_source,
            fetch_mode=fetch_mode,
            stability_profile=stability_profile,
            navigation_profile=source_nav_profile or None,
        )
        body = fetch_result.text
        backend = fetch_result.diagnostics.backend
        attempts = fetch_result.diagnostics.attempts
        outcome = fetch_result.diagnostics.outcome
        detail = fetch_result.diagnostics.detail
        diag_stability_profile = fetch_result.diagnostics.stability_profile
        diag_challenge = fetch_result.diagnostics.challenge_detected
        diag_challenge_retry_attempted = fetch_result.diagnostics.challenge_retry_attempted
        output_path.write_text(body, encoding="utf-8")
    else:
        src_path = Path(normalized_source)
        if not src_path.exists():
            raise FileNotFoundError(f"Source file not found: {src_path}")
        if src_path.suffix.lower() in {".csv", ".json", ".html", ".htm"}:
            output_path = output_path.with_suffix(src_path.suffix.lower())
        shutil.copy2(src_path, output_path)
except safe_requests.BlockedSourceError as exc:
    print(
        "\n".join(
            [
                "STATUS=blocked",
                f"DETAIL={exc}",
                f"BACKEND_USED={exc.backend}",
                f"ATTEMPTS={exc.attempts}",
                "OUTCOME=blocked",
            ]
        )
    )
    raise SystemExit(0)
except Exception as exc:
    print(
        "\n".join(
            [
                "STATUS=failed",
                f"DETAIL={exc}",
                f"BACKEND_USED={backend}",
                f"ATTEMPTS={attempts}",
                "OUTCOME=failed",
            ]
        )
    )
    raise SystemExit(0)

print(
    "\n".join(
        [
            "STATUS=ok",
            f"RAW_PATH={output_path}",
            f"BACKEND_USED={backend}",
            f"ATTEMPTS={attempts}",
            f"OUTCOME={outcome}",
            f"DETAIL={detail}",
            f"STABILITY_PROFILE={diag_stability_profile}",
            f"CHALLENGE={diag_challenge}",
            f"CHALLENGE_RETRY_ATTEMPTED={diag_challenge_retry_attempted}",
            f"NAV_PROFILE={source_nav_profile}",
        ]
    )
)
PY
)"

    STATUS="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^STATUS=/{print $2; exit}')"
    BACKEND_USED="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^BACKEND_USED=/{print $2; exit}')"
    ATTEMPTS="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^ATTEMPTS=/{print $2; exit}')"
    OUTCOME="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^OUTCOME=/{print $2; exit}')"
    DETAIL="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^DETAIL=/{print substr($0,8); exit}')"
    DIAG_STABILITY_PROFILE="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^STABILITY_PROFILE=/{print $2; exit}')"
    DIAG_CHALLENGE="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^CHALLENGE=/{print $2; exit}')"
    DIAG_CHALLENGE_RETRY="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^CHALLENGE_RETRY_ATTEMPTED=/{print $2; exit}')"
    DIAG_NAV_PROFILE="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^NAV_PROFILE=/{print $2; exit}')"

    if [[ "${STATUS}" == "ok" ]]; then
      RAW_PATH="$(printf '%s\n' "${INGEST_RESULT}" | awk -F= '/^RAW_PATH=/{print $2; exit}')"
      RAW_PATHS+=("${RAW_PATH}")
      log "[stage:ingest] complete source=${SOURCE_ITEM} raw_path=${RAW_PATH} backend_used=${BACKEND_USED} attempts=${ATTEMPTS} outcome=${OUTCOME} stability_profile=${DIAG_STABILITY_PROFILE} challenge=${DIAG_CHALLENGE} challenge_retry_attempted=${DIAG_CHALLENGE_RETRY} nav_profile=${DIAG_NAV_PROFILE}"

      NORMALIZE_RESULT="$(python3 - "${RAW_PATH}" "${NORMALIZED_DIR}" "${DATE}" "${SOURCE_ITEM}" <<'PY'
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
    status = "ok"
    parsed_count = -1
    block_reason = ""
else:
    html = raw_path.read_text(encoding="utf-8")
    challenge_provider = scrape_listings.detect_challenge_page(html)
    rows = scrape_listings.parse_listing_page(source_item, html)
    parsed_count = len(rows)
    if challenge_provider and parsed_count == 0:
        status = "blocked"
        block_reason = f"challenge:{challenge_provider}"
    else:
        status = "ok" if parsed_count >= 1 else "parse_failed"
        block_reason = ""
    out_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")

print(
    "\n".join(
        [
            f"STATUS={status}",
            f"NORMALIZED_PATH={out_path}",
            f"PARSED_COUNT={parsed_count}",
            f"BLOCK_REASON={block_reason}",
        ]
    )
)
PY
)"
      NORMALIZED_STATUS="$(printf '%s\n' "${NORMALIZE_RESULT}" | awk -F= '/^STATUS=/{print $2; exit}')"
      NORMALIZED_PATH="$(printf '%s\n' "${NORMALIZE_RESULT}" | awk -F= '/^NORMALIZED_PATH=/{print $2; exit}')"
      PARSED_COUNT="$(printf '%s\n' "${NORMALIZE_RESULT}" | awk -F= '/^PARSED_COUNT=/{print $2; exit}')"
      BLOCK_REASON="$(printf '%s\n' "${NORMALIZE_RESULT}" | awk -F= '/^BLOCK_REASON=/{print substr($0,14); exit}')"

      if [[ "${NORMALIZED_STATUS}" == "ok" ]]; then
        NORMALIZED_PATHS+=("${NORMALIZED_PATH}")
        NORMALIZED_META+=("${NORMALIZED_PATH}|${SOURCE_ITEM}")
        SUMMARY_LINES+=("ok|${SOURCE_ITEM}|${RAW_PATH}|${NORMALIZED_PATH}|${BACKEND_USED}|${ATTEMPTS}|${OUTCOME}|${DETAIL}|${DIAG_NAV_PROFILE}|${PARSED_COUNT}")
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        log "[stage:normalize] complete source=${SOURCE_ITEM} normalized_path=${NORMALIZED_PATH} parsed_count=${PARSED_COUNT}"
      elif [[ "${NORMALIZED_STATUS}" == "blocked" ]]; then
        SUMMARY_LINES+=("blocked|${SOURCE_ITEM}|${BLOCK_REASON}|${BACKEND_USED}|${ATTEMPTS}|${OUTCOME}")
        CHALLENGE_BLOCKED_COUNT=$((CHALLENGE_BLOCKED_COUNT + 1))
        log "[stage:normalize] blocked source=${SOURCE_ITEM} normalized_path=${NORMALIZED_PATH} parsed_count=${PARSED_COUNT} reason=${BLOCK_REASON}"
        if [[ "${SOURCE_ITEM}" == "${PRIORITY_SOURCE}" ]]; then
          PRIORITY_RELAY_NEEDED=1
          PRIORITY_HANDOFF_REASON="${BLOCK_REASON}"
        fi
      else
        SUMMARY_LINES+=("parse_failed|${SOURCE_ITEM}|parse_failed:parsed_records=${PARSED_COUNT}|${BACKEND_USED}|${ATTEMPTS}|${OUTCOME}")
        PARSE_FAILED_COUNT=$((PARSE_FAILED_COUNT + 1))
        log "[stage:normalize] parse_failed source=${SOURCE_ITEM} normalized_path=${NORMALIZED_PATH} parsed_count=${PARSED_COUNT}"
      fi
    elif [[ "${STATUS}" == "blocked" ]]; then
      SUMMARY_LINES+=("blocked|${SOURCE_ITEM}|${DETAIL}|${BACKEND_USED}|${ATTEMPTS}|${OUTCOME}")
      BLOCKED_COUNT=$((BLOCKED_COUNT + 1))
      log "[stage:ingest] blocked source=${SOURCE_ITEM} backend_used=${BACKEND_USED} attempts=${ATTEMPTS} outcome=${OUTCOME} detail=${DETAIL}"
      if [[ "${SOURCE_ITEM}" == "${PRIORITY_SOURCE}" ]]; then
        PRIORITY_RELAY_NEEDED=1
        PRIORITY_HANDOFF_REASON="${DETAIL}"
      fi
    else
      SUMMARY_LINES+=("failed|${SOURCE_ITEM}|${DETAIL}|${BACKEND_USED}|${ATTEMPTS}|${OUTCOME}")
      FAILED_COUNT=$((FAILED_COUNT + 1))
      log "[stage:ingest] failed source=${SOURCE_ITEM} backend_used=${BACKEND_USED} attempts=${ATTEMPTS} outcome=${OUTCOME} detail=${DETAIL}"
    fi
  done



  if [[ "${PRIORITY_RELAY_NEEDED}" -eq 1 ]]; then
    PRIORITY_HANDOFF_PATH="$(python3 -m relay_handoff create --source-url "${PRIORITY_SOURCE}" --run-date "${DATE}" --reason "${PRIORITY_HANDOFF_REASON}" --handoff-dir "${HANDOFF_DIR}")"
    EXPECTED_PAYLOAD_PATH="$(python3 - "${PRIORITY_HANDOFF_PATH}" <<'PY'
from pathlib import Path
import json
import sys

handoff = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(handoff["expected_payload_path"])
PY
)"

    log "[stage:relay_handoff] waiting handoff_path=${PRIORITY_HANDOFF_PATH} expected_payload_path=${EXPECTED_PAYLOAD_PATH} timeout_seconds=${RELAY_TIMEOUT_SECONDS}"

    START_TS="$(date +%s)"
    while [[ ! -f "${EXPECTED_PAYLOAD_PATH}" ]]; do
      NOW_TS="$(date +%s)"
      ELAPSED="$((NOW_TS - START_TS))"
      if [[ "${ELAPSED}" -ge "${RELAY_TIMEOUT_SECONDS}" ]]; then
        python3 - "${PRIORITY_HANDOFF_PATH}" <<'PY'
from pathlib import Path
import relay_handoff
import sys

relay_handoff.mark_handoff_status(Path(sys.argv[1]), status="timed_out", note="No relay payload received before timeout")
PY
        echo "Error: relay payload not received before timeout (${RELAY_TIMEOUT_SECONDS}s)." >&2
        exit 1
      fi
      log "[stage:relay_handoff] waiting elapsed_seconds=${ELAPSED}"
      sleep "${RELAY_POLL_SECONDS}"
    done

    if ! python3 -m relay_handoff validate --handoff "${PRIORITY_HANDOFF_PATH}" --payload "${EXPECTED_PAYLOAD_PATH}"; then
      python3 - "${PRIORITY_HANDOFF_PATH}" <<'PY'
from pathlib import Path
import relay_handoff
import sys

relay_handoff.mark_handoff_status(Path(sys.argv[1]), status="invalid_payload", note="Relay payload failed validation")
PY
      echo "Error: relay payload validation failed for ${EXPECTED_PAYLOAD_PATH}" >&2
      exit 1
    fi

    RELAY_NORMALIZED_PATH="$(python3 -m relay_handoff materialize --handoff "${PRIORITY_HANDOFF_PATH}" --payload "${EXPECTED_PAYLOAD_PATH}" --normalized-dir "${NORMALIZED_DIR}")"
    NORMALIZED_PATHS+=("${RELAY_NORMALIZED_PATH}")
    NORMALIZED_META+=("${RELAY_NORMALIZED_PATH}|${PRIORITY_SOURCE}")
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    PRIORITY_RELAY_RESOLVED=1
    python3 - "${PRIORITY_HANDOFF_PATH}" "${EXPECTED_PAYLOAD_PATH}" "${RELAY_NORMALIZED_PATH}" <<'PY'
from pathlib import Path
import relay_handoff
import sys

relay_handoff.mark_handoff_status(
    Path(sys.argv[1]),
    status="completed",
    note=f"Payload accepted: {sys.argv[2]}; normalized output: {sys.argv[3]}",
)
PY
    log "[stage:relay_handoff] resumed source=${PRIORITY_SOURCE} relay_payload=${EXPECTED_PAYLOAD_PATH} normalized_path=${RELAY_NORMALIZED_PATH}"
  fi

  log "[stage:source-summary] begin"
  for summary in "${SUMMARY_LINES[@]}"; do
    IFS='|' read -r state src a b c d e f g h <<<"${summary}"
    if [[ "${state}" == "ok" ]]; then
      log "[stage:source-summary] status=ok source=${src} raw_path=${a} normalized_path=${b} backend_used=${c} attempts=${d} outcome=${e} reason=${f} nav_profile=${g} parsed_count=${h}"
    else
      log "[stage:source-summary] status=${state} source=${src} reason=${a} backend_used=${b} attempts=${c} outcome=${d}"
    fi
  done
  log "[stage:source-summary] totals success=${SUCCESS_COUNT} blocked=${BLOCKED_COUNT} challenge_blocked=${CHALLENGE_BLOCKED_COUNT} failed=${FAILED_COUNT} parse_failed=${PARSE_FAILED_COUNT}"
  if [[ "${PRIORITY_RELAY_NEEDED}" -eq 1 ]]; then
    log "[stage:relay_handoff] status needed=${PRIORITY_RELAY_NEEDED} resolved=${PRIORITY_RELAY_RESOLVED} handoff_path=${PRIORITY_HANDOFF_PATH}"
  fi

  if [[ "${#NORMALIZED_PATHS[@]}" -eq 0 ]]; then
    echo "Error: all sources blocked/parse_failed/failed; no normalized outputs available." >&2
    exit 1
  fi

  if [[ "${#NORMALIZED_PATHS[@]}" -gt 1 ]]; then
    COMBINED_PATH="${NORMALIZED_DIR}/normalized_${DATE}_combined.json"
    python3 - "${COMBINED_PATH}" "${NORMALIZED_META[@]}" <<'PY'
from pathlib import Path
import csv
import json
import sys
from urllib.parse import urlparse

from record_cleaning import normalize_and_dedupe_records

output = Path(sys.argv[1])
meta_items = sys.argv[2:]
rows = []

for item in meta_items:
    path_text, source_url = item.split("|", 1)
    path = Path(path_text)
    site = (urlparse(source_url).hostname or "").lower()

    source_rows = []
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            source_rows.extend(data)
    elif path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as fh:
            source_rows.extend(list(csv.DictReader(fh)))

    rows.extend(normalize_and_dedupe_records(source_rows, source_url=source_url, source_site=site))

rows = normalize_and_dedupe_records(rows)
output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
PY
    NORMALIZED_PATH="${COMBINED_PATH}"
    if [[ "${#RAW_PATHS[@]}" -gt 0 ]]; then
      RAW_PATH="${RAW_PATHS[0]}"
    fi
    log "[stage:normalize] combined source list normalized_path=${NORMALIZED_PATH}"
  else
    NORMALIZED_PATH="${NORMALIZED_PATHS[0]}"
    if [[ "${#RAW_PATHS[@]}" -gt 0 ]]; then
      RAW_PATH="${RAW_PATHS[0]}"
    fi
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

log "[stage:report] schedule evaluation begin"
mapfile -t REPORT_MODES < <(python3 - "${DATE}" <<'PY'
from datetime import datetime
import sys

run_date = sys.argv[1]


def fallback_modes(day):
    modes = []
    if day.weekday() == 5:
        modes.append("weekly")
    if day.day == 1:
        modes.append("monthly")
    return modes


try:
    from reporting_schedule import determine_report_modes
except Exception as exc:
    print(
        f"[stage:report] schedule evaluation import failed: {exc}; using fallback evaluator",
        file=sys.stderr,
    )
    day = datetime.strptime(run_date, "%Y-%m-%d").date()
    modes = fallback_modes(day)
else:
    try:
        modes = determine_report_modes(run_date)
    except Exception as exc:
        print(
            f"[stage:report] schedule evaluation failed: {exc}; using fallback evaluator",
            file=sys.stderr,
        )
        day = datetime.strptime(run_date, "%Y-%m-%d").date()
        modes = fallback_modes(day)

for mode in modes:
    print(mode)
PY
)
if [[ "${#REPORT_MODES[@]}" -eq 0 ]]; then
  log "[stage:report] skipped (non-Saturday and not first day of month)"
else
  for MODE in "${REPORT_MODES[@]}"; do
    for PRODUCT in exec detailed; do
      REPORT_TYPE="${MODE}_sales_report_${PRODUCT}"
      OUTPUT_PREFIX="${REPORT_PREFIX}_${MODE}_${PRODUCT}"
      REPORT_ARGS=(
        --reports-dir "${REPORTS_DIR}"
        --analysis-prefix "${ANALYSIS_PREFIX}"
        --output-prefix "${OUTPUT_PREFIX}"
        --date "${DATE}"
        --source "${SUPABASE_SOURCE}"
        --report-type "${REPORT_TYPE}"
        --report-version "v3"
        --report-mode "${MODE}"
        --report-product "${PRODUCT}"
        --records-input "${NORMALIZED_PATH}"
        --local-output-mode "${REPORT_LOCAL_OUTPUT_MODE}"
      )
      if [[ "${WITH_SUPABASE}" -eq 1 ]]; then
        REPORT_ARGS+=(--persist-supabase)
      fi
      log "[stage:report] begin mode=${MODE} product=${PRODUCT}"
      python3 -m report "${REPORT_ARGS[@]}"
      log "[stage:report] complete mode=${MODE} product=${PRODUCT}"
    done
  done
fi

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
