#!/usr/bin/env bash
set -euo pipefail

# Daily task preset (confirmed 2026-03-09):
# - Browser Relay CDP port: 18792
# - Sources: realestate + domain only

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export SMA_RELAY_CDP_URL="${SMA_RELAY_CDP_URL:-http://127.0.0.1:18792}"
export SMA_FETCH_RELAY_DOMAINS="realestate.com.au,domain.com.au"

set -a
. /home/oc/.openclaw/workspace/secrets/supabase_shared.env
set +a

RUN_DATE="${1:-$(date -u +%F)}"

scripts/run_daily.sh \
  --source-list data/sources/southport_relay_two_sites.json \
  --fetch-mode relay \
  --date "$RUN_DATE"
