# CODEX PLATFORM 001B Implementation

Phase: `CODEX_PLATFORM_001B_WORKTREE_PILOT_AND_PLATFORM005B_CORRECTION`

Status: `COMPLETED_PENDING_INDEPENDENT_REVIEW`

Evidence updated by: `CODEX_PLATFORM_001D_WORKTREE_TOOLING_CORRECTION_NO_MERGE`

Branch: `codex/PLATFORM_005B-load-observation-correction`

Baseline: `7cb5942055402474b7f3c7223a4b6f3f719aa17d`

Quality target: `Q4_DECISION_GRADE`

## Summary

The 001B pilot created the isolated implementation worktree, preserved the operational checkout during that phase, added safe worktree tooling, and corrected `PLATFORM005-F001` through `PLATFORM005-F009`.

This report was finalized during 001D to remove stale pending states and to sanitize local absolute paths from versioned evidence.

## Scope

Changed files in 001B:

- `research/CODEX_PLATFORM_001A_BASTIAN_DECISIONS.json`
- `scripts/sz_worktree_audit.ps1`
- `scripts/sz_local_env_setup.ps1`
- `tests/test_sz_worktree_tooling.py`
- `research/CODEX_PLATFORM_001B_IMPLEMENTATION.json`
- `research/reports/CODEX_PLATFORM_001B_IMPLEMENTATION.md`
- `.claude/skills/sz-load-observation/SKILL.md`
- `scripts/sz_load_observation.py`
- `tests/test_sz_load_observation.py`

No `.codex/`, `.venv`, ignored data, secrets, DB, Docker, SQL, loaders, merge, or main push were used by 001B.

## Commits

- `ea226a684c5dd07fd3448ed5a19ceb475b5f731b` - `research: record Codex worktree platform decisions`
- `ca35f3fc74309148ac55ef0b3ff639527adb7970` - `platform: add safe Codex worktree pilot`
- `3277692c95bfc706498028eb271c7a71b84620b3` - `tooling: correct load observation safety blockers`
- `08d186741934bdf7ec4c1347752d48370744efaa` - `platform: generalize safe worktree tooling` added by 001D
- `research: finalize worktree pilot evidence` is represented by `evidence_commit_self_reference=RECORDED_BY_SUCCESSOR_PHASE`; the original reviewed branch head was `6a965ec4b5b02d6de9d839f475a63697ac226f15`.

The branch `codex/PLATFORM_005B-load-observation-correction` was pushed. Main was not pushed and no merge was performed.

## PLATFORM005B

Corrected:

- `F001`: path scope checked before open; external paths, traversal, symlinks, `.env`, `.local_secrets`, credentials, `data/`, and `evidence/` rejected.
- `F002`: strict counts, booleans, hashes, dates, weeks, arrays, rollback flags, and unknown-field handling.
- `F003`: CLI/JSON consistency for source, effective week, operation type, and input SHA-256.
- `F004`: event-scoped `observation_id` over source, week, operation, input hash, and normalized `recorded_at`.
- `F005`: Shape A and Shape B fail closed for incomplete, accidental, conflicting, or row-level payloads.
- `F006`: privacy checks use concrete sensitive keys and patterns; notes/reasons are technical codes.
- `F007`: explicit label/operation matrix.
- `F008`: evidence refs enforce length, traversal, backslash, URL, and research-path existence checks.
- `F009`: argparse/runtime errors emit JSON on stdout with stable categories and empty stderr.

## Validation

001B validation:

- tooling tests: 20 collected, 20 passed
- observation tests: 115 collected, 115 passed, 1 skipped for unavailable Windows symlink privilege
- combined tests: 135 collected, 135 passed, 1 skipped
- ledger SHA-256 stayed `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`

001D regression after tooling correction:

- tooling tests in branch mode: 25 collected, 23 passed, 2 skipped because detached-only cases are exercised in the detached review worktree
- observation tests: 115 collected, 115 passed, 1 skipped
- ledger SHA-256 stayed `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`

## Kernels

`kernels_missing_in_clean_worktree = expected_external_dependency`.

This does not affect:

- load observation skill tests
- worktree tooling read-only behavior
- `py_compile`

It can affect:

- full preflight modes requiring project kernels
- historical or productive phases that require local kernels

Future proposal only: `KERNEL_REFERENCE_ROOT`. It is not implemented or configured in this phase.

## Safety

No DB access, DB writes, Docker, SQL, loaders, product refresh, secrets printing, main push, merge, or force push occurred.

The finalized evidence intentionally redacts local absolute paths.
