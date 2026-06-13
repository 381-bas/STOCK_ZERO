# CODEX PLATFORM 001D Tooling Correction

Phase: `CODEX_PLATFORM_001D_WORKTREE_TOOLING_CORRECTION_NO_MERGE`

Branch: `codex/PLATFORM_005B-load-observation-correction`

Baseline before this phase: `3277692c95bfc706498028eb271c7a71b84620b3`

Quality target: `Q4_DECISION_GRADE`

## Purpose

This phase corrects the five findings from `CODEX_PLATFORM_001C_DETACHED_REVIEW_AND_CLEANUP_NO_WRITE` without changing the already validated load observation skill files.

Modified files are limited to:

- `scripts/sz_worktree_audit.ps1`
- `scripts/sz_local_env_setup.ps1`
- `tests/test_sz_worktree_tooling.py`
- `research/CODEX_PLATFORM_001B_IMPLEMENTATION.json`
- `research/reports/CODEX_PLATFORM_001B_IMPLEMENTATION.md`
- `research/CODEX_PLATFORM_001D_TOOLING_CORRECTION.json`
- `research/reports/CODEX_PLATFORM_001D_TOOLING_CORRECTION.md`

## Corrections

`sz_worktree_audit.ps1`:

- removed the hardcoded pilot branch
- added `-ExpectedBranch`
- added `-AllowDetached`
- added `-IncludeAbsolutePaths`
- keeps `-ExpectedBaseline`, `-RequireClean`, and `-Pretty`
- blocks detached HEAD by default with `detached_not_allowed`
- allows detached HEAD explicitly with `-AllowDetached`
- compares baseline to exact HEAD
- redacts absolute paths by default
- emits normalized Git command results with success, exit code, and safe error category
- remains read-only

`sz_local_env_setup.ps1`:

- keeps default dry-run behavior
- adds `environment_source`
- adds `environment_reproducible`
- fails `-RunImportSmoke` without `.venv` by default using `worktree_venv_required`
- allows system Python smoke only with `-AllowSystemPythonSmoke`
- marks system Python smoke as not reproducible
- marks worktree `.venv` smoke as reproducible
- does not install requirements
- does not reveal absolute Python executable paths in public JSON

`tests/test_sz_worktree_tooling.py`:

- expands coverage to 25 tests
- covers branch expected match/mismatch, optional branch, baseline match/mismatch, clean checks, path redaction, absolute path switch, normalized Git failure, setup dry-run, import smoke policy, system Python warning, worktree `.venv` smoke, no installs, no DB, no Docker, no data copy, no `.codex/`, no kernel copy, and protected checkout status preservation
- detached-only tests are exercised in the detached review worktree after push

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

## Validation Before Evidence Commit

Branch-mode validation:

- tooling tests: 25 collected, 23 passed, 0 failed, 2 skipped for detached-only paths
- skill regression: 115 collected, 115 passed, 0 failed, 1 skipped for unavailable Windows symlink privilege
- ledger SHA-256 remained `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- generic preflight: `warn`, no blockers, expected missing-kernel warnings
- `git diff --check`: no whitespace errors; CRLF normalization warnings only

## Safety Notes

No DB, Docker, loaders, SQL, data, secrets, main push, merge, or force push were used.

The primary checkout was not modified by this phase. However, its HEAD changed externally during the run from `7cb5942055402474b7f3c7223a4b6f3f719aa17d` to `e5939a9b8fa553b87b997dca46839fa783f6b05e`. The tracked dirty file hash and inherited untracked roots stayed unchanged. No revert was attempted.

## Remaining Handoff

After the evidence commit and branch push, run a detached review worktree from the new branch HEAD:

- audit with `-AllowDetached` and `-ExpectedBaseline`
- tooling tests
- skill tests
- ledger hash check
- clean removal of the detached review worktree without `git worktree prune`

The exact final branch head is reported in the final Codex screen output because a commit cannot reliably embed its own SHA in its tracked contents.
