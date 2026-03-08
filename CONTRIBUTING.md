# Contributing

## Runtime artifact hygiene (required)

This repository treats `data/`, `logs/`, and `reports/` as runtime-output directories.
Generated files in these directories must not be committed during normal development.

Rules:

1. Keep runtime outputs untracked.
2. Only commit intentionally versioned fixtures/docs in allowlisted paths (for example `data/fixtures/`, `data/docs/`, or `data/sources/` when curated source manifests are required).
3. Before running `auto_dev_pipeline develop`, run `scripts/auto_dev_preflight.sh`.

Preflight behavior is explicit and safe:

- Clean repo: exits successfully with no changes.
- Dirty non-runtime files: exits non-zero and refuses to stash.
- Dirty runtime files only: auto-stashes runtime files (tracked/untracked) using a timestamped message.

Use this check in local or CI validation:

```bash
scripts/check_runtime_hygiene.sh
```

It fails if unexpected tracked files appear under runtime directories.
