#!/usr/bin/env bash
set -euo pipefail

RUNTIME_DIRS=("data" "logs" "reports")
TIMESTAMP="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
STASH_MSG="auto_dev_pipeline preflight runtime stash ${TIMESTAMP}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[preflight] error: must run inside a git repository" >&2
  exit 1
fi

cd "$(git rev-parse --show-toplevel)"

echo "[preflight] checking worktree hygiene for runtime artifacts: ${RUNTIME_DIRS[*]}"

STATUS_ALL="$(git status --porcelain)"
STATUS_RUNTIME="$(git status --porcelain -- "${RUNTIME_DIRS[@]}" || true)"

if [[ -z "${STATUS_ALL}" ]]; then
  echo "[preflight] working tree is clean"
  exit 0
fi

STATUS_NON_RUNTIME="$(printf '%s\n' "${STATUS_ALL}" | rg -v '^.. (data|logs|reports)/' || true)"

if [[ -n "${STATUS_NON_RUNTIME}" ]]; then
  echo "[preflight] error: non-runtime changes detected; refusing to auto-stash" >&2
  echo "[preflight] review these paths before running auto_dev_pipeline develop:" >&2
  printf '%s\n' "${STATUS_NON_RUNTIME}" >&2
  exit 2
fi

if [[ -z "${STATUS_RUNTIME}" ]]; then
  echo "[preflight] runtime directories are clean"
  exit 0
fi

echo "[preflight] runtime changes detected; stashing runtime artifacts"
printf '%s\n' "${STATUS_RUNTIME}"

git stash push -u -m "${STASH_MSG}" -- "${RUNTIME_DIRS[@]}" >/dev/null

echo "[preflight] stashed runtime artifacts as: ${STASH_MSG}"
echo "[preflight] latest stash: $(git stash list -n 1 --pretty='%gd %s')"
