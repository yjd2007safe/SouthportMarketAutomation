#!/usr/bin/env bash
set -euo pipefail

# Allowlist any intentionally versioned runtime-adjacent files here.
ALLOW_REGEX='^(data/(README\.md|\.gitkeep|docs/|fixtures/|sources/)|logs/(README\.md|\.gitkeep)|reports/(README\.md|\.gitkeep))'

tracked_runtime="$(git ls-files | rg '^(data|logs|reports)/' || true)"
if [[ -z "${tracked_runtime}" ]]; then
  echo "[hygiene] pass: no tracked files found under runtime directories"
  exit 0
fi

unexpected="$(printf '%s\n' "${tracked_runtime}" | rg -v "${ALLOW_REGEX}" || true)"
if [[ -n "${unexpected}" ]]; then
  echo "[hygiene] fail: unexpected tracked runtime artifacts detected:" >&2
  printf '%s\n' "${unexpected}" >&2
  echo "[hygiene] keep runtime outputs untracked; move fixtures/docs to allowlisted paths." >&2
  exit 1
fi

echo "[hygiene] pass: tracked runtime files are only allowlisted docs/fixtures"
