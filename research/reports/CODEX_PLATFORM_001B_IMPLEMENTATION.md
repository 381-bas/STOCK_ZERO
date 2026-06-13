# CODEX PLATFORM 001B Implementation

Phase: `CODEX_PLATFORM_001B_WORKTREE_PILOT_AND_PLATFORM005B_CORRECTION`

Branch: `codex/PLATFORM_005B-load-observation-correction`

Worktree: `C:\Users\basti\Desktop\STOCK_ZERO_WORKTREES\PLATFORM_005B`

Baseline: `7cb5942055402474b7f3c7223a4b6f3f719aa17d`

Quality target: `Q4_DECISION_GRADE`

## Purpose

This pilot implements the hybrid worktree topology approved in 001A and corrects the `/sz-load-observation` safety blockers identified in PLATFORM005.

The primary checkout at `C:\Users\basti\Desktop\STOCK_ZERO` remains the `LOCAL_OPERATIONAL` lane and was not modified by this worktree.

## Primary Checkout Fingerprint

Before creating the worktree:

- HEAD: `7cb5942055402474b7f3c7223a4b6f3f719aa17d`
- branch: `main`
- local `origin/main`: `7cb5942055402474b7f3c7223a4b6f3f719aa17d`
- tracked dirty file hash: `aca053b97879cf9169752a40d9d00f375314cea7d3a8cec16e5202cc629af478  scripts/load_control_gestion_raw_v17.py`
- destination path existed: no
- checkout size: 4,965,930,797 bytes

Inherited untracked roots observed:

- `evidence/`
- `research/claude/`
- `sql/09_control_gestion_kpione2_raw_dryrun_draft.sql`
- `sql/10_control_gestion_kpione2_forward_recovery_lab_draft.sql`

After validation, the primary checkout remained on `main` at the same HEAD, with the same tracked dirty file hash and the same inherited untracked roots.

No stash, reset, clean, checkout, pull, rebase, merge, or push was run against the primary checkout.

## Worktree Creation

The worktree was created from `origin/main` with:

```powershell
git worktree add -b codex/PLATFORM_005B-load-observation-correction C:\Users\basti\Desktop\STOCK_ZERO_WORKTREES\PLATFORM_005B origin/main
```

Creation time: 0.325 seconds.

Initial worktree size: 1,907,002 bytes.

Current measured worktree size after tests: 2,059,519 bytes.

Absent after creation and audit:

- `.env`
- `.local_secrets`
- `data`
- `.venv`
- `.codex`

The worktree is intentionally left in place for inspection.

## Platform Tooling

Created:

- `scripts/sz_worktree_audit.ps1`
- `scripts/sz_local_env_setup.ps1`
- `tests/test_sz_worktree_tooling.py`

The audit script is read-only and emits JSON with branch, HEAD, status, staged files, modified files, untracked files, worktree list, and presence-only checks for ignored/local directories.

The setup script defaults to dry-run, creates no files by default, installs nothing, does not read secrets, does not touch Git, does not start DB/Docker/Streamlit, and emits JSON.

No `.codex/` directory was created.

## PLATFORM005B Corrections

Corrected:

- `PLATFORM005-F001`: path scope is checked before open; external paths, traversal, symlinks, `.env`, `.local_secrets`, credentials, `data/`, and `evidence/` are rejected.
- `PLATFORM005-F002`: strict validation covers counts, booleans, hashes, dates, weeks, arrays, rollback flags, and unknown-field handling.
- `PLATFORM005-F003`: CLI/JSON consistency covers source, effective week, operation type, and input SHA-256.
- `PLATFORM005-F004`: `observation_id` is event-scoped over source, week, operation, input hash, and normalized `recorded_at`.
- `PLATFORM005-F005`: Shape A and Shape B are fail-closed for incomplete, accidental, conflicting, or row-level payloads.
- `PLATFORM005-F006`: privacy checks use concrete sensitive keys and patterns; free-text notes/reasons are replaced by technical codes.
- `PLATFORM005-F007`: label/operation contradictions are rejected by an explicit matrix.
- `PLATFORM005-F008`: evidence refs have length, traversal, backslash, URL, and research-path existence checks.
- `PLATFORM005-F009`: argparse and runtime errors are emitted as JSON on stdout with stable categories and empty stderr.

## Validation

Executed inside the worktree:

- `powershell -NoProfile -ExecutionPolicy Bypass -File scripts\sz_worktree_audit.ps1 -Pretty`
- `powershell -NoProfile -ExecutionPolicy Bypass -File scripts\sz_local_env_setup.ps1 -DryRun -Pretty`
- `python scripts/sz_preflight.py --phase generic --root . --skip-db --json-out $env:TEMP\sz_preflight_platform001b.json`
- `python -m py_compile scripts/sz_load_observation.py`
- `python -m unittest tests.test_sz_worktree_tooling -v`
- `python -m unittest tests.test_sz_load_observation -v`
- `python -m unittest tests.test_sz_worktree_tooling tests.test_sz_load_observation -v`
- `git diff --check`

Results:

- preflight final verdict: `warn`
- scanner syntax errors: 0
- scanner read errors: 0
- blockers: none
- expected warnings: dirty worktree before commits and missing local kernel files in the clean worktree
- setup dry-run: OK, 1.181 seconds, no venv, no installs, no DB, no Docker
- tooling tests: 20 collected, 20 passed
- observation tests: 115 collected, 115 passed, 1 skipped because Windows symlink privilege was unavailable
- combined tests: 135 collected, 135 passed, 1 skipped
- manual draft/validate: draft exit 0 twice, validate exit 0, deterministic output true
- ledger SHA-256 before/after: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- `git diff --check`: exit 0, with CRLF normalization warnings only

The manual draft used a temporary synthetic JSON under `research/` because productive CLI input intentionally rejects external temp paths. The temporary files were removed immediately after validation.

## Safety

No DB access, DB writes, Docker, SQL, loaders, product refresh, secrets printing, main push, merge, or force push occurred.

The branch push remains branch-only and pending at this evidence point.

## Inspection

Review branch:

```powershell
git -C C:\Users\basti\Desktop\STOCK_ZERO_WORKTREES\PLATFORM_005B status --short
git -C C:\Users\basti\Desktop\STOCK_ZERO_WORKTREES\PLATFORM_005B log --oneline --decorate -5
```

Do not merge this branch before independent validation.
