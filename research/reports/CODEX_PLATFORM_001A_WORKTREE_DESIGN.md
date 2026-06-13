# CODEX PLATFORM 001A Worktree And Local Environment Design

Phase: `CODEX_PLATFORM_001A_WORKTREE_AND_LOCAL_ENVIRONMENT_DESIGN_NO_APPLY`

Verdict: `DESIGN_READY`

Quality: `Q4_DECISION_GRADE`

Confidence: `HIGH`

## Guardrails

This phase did not create worktrees, create branches, modify `.codex/`, install dependencies, copy `.venv`, copy data, run Docker, access DB, modify loaders/SQL/app/tests, or apply configuration. Secrets, `.env`, `.env.*`, `.local_secrets` content, heavy data payloads, DB dumps, and real workbook contents were not read.

Persistent files created:

- `research/CODEX_PLATFORM_001A_WORKTREE_DESIGN.json`
- `research/reports/CODEX_PLATFORM_001A_WORKTREE_DESIGN.md`

## Current State

- Branch: `main`
- HEAD: `5c5a19dab46fbea754ea105573003b6747c814dc`
- Local `origin/main`: `5c5a19dab46fbea754ea105573003b6747c814dc`
- Existing worktrees: 1
- `.codex/` in repo: absent
- Primary checkout dirty: yes
- Preflight: `warn`, with `git_worktree_not_clean` and `kernel_02_head_mismatch`

Dirty checkout classification:

- `scripts/load_control_gestion_raw_v17.py`: tracked modified, `UNKNOWN` pending owning-phase review.
- `evidence/`: untracked plus ignored heavy artifacts, `RESEARCH_UNABSORBED`.
- `research/claude/`: untracked, `RESEARCH_UNABSORBED`.
- `sql/09_control_gestion_kpione2_raw_dryrun_draft.sql`: untracked, `UNKNOWN`.
- `sql/10_control_gestion_kpione2_forward_recovery_lab_draft.sql`: untracked, `UNKNOWN`.

Approximate local footprint:

- checkout total: 4.96 GB
- `.git`: 1.30 GB
- `.venv`: 429 MB
- `data`: 56 MB
- `evidence`: 3.18 GB
- `research`: 832 KB
- `sql`: 218 KB

These numbers matter: a new worktree gives a separate tracked checkout, not a clone of ignored local state.

## Recommendation

Use `OPTION_E_HYBRID`.

Keep `C:\Users\basti\Desktop\STOCK_ZERO` as `LOCAL_OPERATIONAL`, the Bastian-owned checkout for real workbooks, local secrets, productive commands when explicitly authorized, and final inspection.

Use short-lived worktrees for:

- `CODEX_IMPLEMENTATION`: scoped code/research artifact changes.
- `INDEPENDENT_REVIEW`: clean validation from an exact commit.
- `PLATFORM_TOOLING`: setup/action scripts and safe tooling while recurrence is still being proven.

Create one `LOCAL_LAB` worktree only when Docker/PostgreSQL work is explicitly authorized. Do not make it permanent until a real lab phase proves that setup time and hidden-state management justify it.

## Why Not Local Checkout Only

The current checkout already shows the failure mode: unrelated tracked, untracked, ignored, data, evidence, and local lab state coexist. Worktrees will not repair this inherited debt, but they can stop future phases from adding to it.

The design separates two problems:

- future isolation: solved by per-risk worktrees and branch discipline;
- inherited local debt: solved by a separate classification/absorption/archive phase.

## Lanes

`LOCAL_OPERATIONAL`: main checkout. Real data and secrets may exist here, but only Bastian or explicitly authorized phases should use them.

`CODEX_IMPLEMENTATION`: one branch per write worktree, normally `codex/<phase_id>-<slug>`. No real data by default.

`INDEPENDENT_REVIEW`: exact commit, preferably detached HEAD. No branch unless the review itself is authorized to create report artifacts.

`LOCAL_LAB`: one owner at a time, explicit local PostgreSQL/Docker contract, no automatic start/stop, no volume deletion without exact authorization.

`PLATFORM_TOOLING`: starts as ephemeral. Becomes permanent only after reuse proves value.

## Baseline Policy

New implementation starts from local-known `origin/main` or an explicit commit. If network sync is needed, ask before fetch/pull/rebase.

Independent validation starts from the exact commit under review and records `baseline_commit`.

Historical reproduction never rebases the target commit.

DB lab work starts from an explicit lab contract commit and must prove rebuildability instead of trusting hidden local state.

Dirty local changes are allowed only in `LOCAL_OPERATIONAL`, and only after listing the inherited files.

## Branch Policy

Use one branch per write worktree. Never check out the same branch in two worktrees.

Recommended prefixes:

- `codex/<phase_id>-<slug>` for implementation/platform work.
- `research/<phase_id>-<slug>` for research artifact branches.
- `lab/<phase_id>-<slug>` for local lab work.

Use detached HEAD for read-only review and historical reproduction. Create a branch only when writes/commits are authorized.

Direct push to `main` can continue only for exact-scope, explicitly authorized research artifacts or small validated changes. Larger/riskier changes should use branch/PR.

## Ignored And Local Files

- `.env`, `.env.*`, `.local_secrets`: `COPY_NEVER`.
- `.venv`: `RECREATE`; never copy blindly.
- `data`: `EXTERNAL_REFERENCE`; use `STOCK_ZERO_DATA_ROOT` or explicit read-only paths only when authorized.
- `evidence`: `SHARED_READ_ONLY` for heavy historical artifacts; new evidence must be phase-scoped.
- caches and temp outputs: `WORKTREE_LOCAL`.
- Docker volumes/local PostgreSQL state: `UNKNOWN`; never delete automatically.

Handoff is not a mechanism for moving ignored files.

## Python Strategy

Select `PY_C_CENTRAL_CACHE_PER_WORKTREE_VENV`.

Use a `.venv` per worktree for isolation, with shared pip/cache directories to avoid needless download cost. The current main `.venv` is Python 3.12.10 and should not be copied. The devcontainer uses a Python 3.11 image, so parity-sensitive local work should prefer Python 3.12 unless a phase explicitly targets devcontainer behavior.

## Data Strategy

Default: no data access.

Use tracked or synthetic fixtures for tests. Use `STOCK_ZERO_DATA_ROOT` as the future standard pointer for authorized read-only worktree access to real data. Avoid symlinks/junctions initially because Windows privileges and deletion semantics add risk.

Tasks needing real data, loaders, DB writes, or long background execution must stay explicitly authorized and should not become Local Environment buttons.

## Secret Strategy

No secrets are copied into worktrees. Presence checks must be boolean-only.

Lane defaults:

- `CODEX_IMPLEMENTATION`, `INDEPENDENT_REVIEW`, `PLATFORM_TOOLING`: no credentials.
- `LOCAL_LAB`: local PostgreSQL DSN only when lab phase authorizes it; `DB_URL_CODEX_RO` only for explicit read-only extracts.
- `LOCAL_OPERATIONAL`: may receive `DB_URL_APP`, `DB_URL_LOAD`, or `DB_URL_CODEX_RO` only under exact phase authorization.

## Docker And PostgreSQL

Docker is shared across worktrees. That means a worktree is not a service boundary.

Use one `LOCAL_LAB` owner at a time. Name projects, containers, schemas, ports, and volumes by phase. Never run `down -v` without exact authorization. Prove clean-room reproducibility with rebuild scripts, manifests, hashes, and recorded inputs.

## Local Environment Design

No `.codex/` schema was invented because no repo `.codex/` exists to verify locally. In 001B, the schema must be verified through the Codex UI before committing any `.codex` configuration.

Initial safe actions:

- Git Status
- Preflight Generic
- Scanner Only
- Unit Tests Targeted
- JSON Validate Research Artifact

Conditional actions:

- Preflight Control Gestion No DB
- Full Tests
- Route Source Check
- Route Dry Run
- Canonical Builder Local
- Streamlit
- Docker Lab Start
- Git Push

Forbidden actions:

- Loader Apply
- Product Refresh
- Supabase Write
- Docker volume deletion
- Git reset/clean/stash
- `git add .` or `git add -A`
- Secret print
- Automatic data copy

## Handoff Checklist

Before handoff:

- baseline commit
- worktree path
- branch or detached HEAD
- `git status --short`
- diff stat
- exact staged set
- tests/preflight
- files modified
- commits
- ignored dependencies
- next action

Uncommitted or untracked changes are never assumed to transfer cleanly.

## Cleanup And Recovery

Remove ephemeral worktrees only after confirming status, branch commits, untracked files, ignored artifacts, and Bastian review pointers.

Never automatically delete:

- `data/`
- `.env*`
- `.local_secrets/`
- Docker volumes
- local PostgreSQL directories
- unclassified untracked research/evidence

Emergency policy:

- lost worktree: check branches and reflog first;
- branch locked by another worktree: use a different branch or close the inactive worktree after status check;
- rebase conflict: preserve branch and ask;
- incomplete handoff: stop integration and rebuild the checklist;
- setup failure: leave worktree intact and capture the failure;
- obsolete baseline: ask whether to restart, merge, or rebase.

## First Pilot

Recommended pilot: `PLATFORM_005B` correction of `/sz-load-observation`.

Reason: it is scoped, currently has a validated blocker set, uses targeted tests, avoids DB/Docker/data, and can test creation, setup, isolation, commit, handoff, and cleanup.

Not recommended as first pilot:

- `CG005F` revalidation: useful, but may depend on inherited evidence context.
- `CG005G` local lab: too much Docker/PostgreSQL state for pilot one.
- small documentation task: too low risk to prove the platform.
- no pilot: unnecessary because `PLATFORM_005B` is a good candidate.

## Metrics

Track:

- cross-phase file collisions
- files outside scope
- setup time
- disk usage delta
- rework rounds
- repeated prompt length
- test reproducibility
- handoff failures
- dirty-main incidents
- cleanup time

The platform progresses only when it reduces observable cost or risk.

## Critique

A worktree can absolutely add complexity without solving the real problem. It isolates tracked working files; it does not isolate secrets, real data, Docker, PostgreSQL, evidence payloads, or human operational decisions.

The main checkout remains necessary for Bastian operational work and local-only files. Ignored files make reproducibility deceptive unless every phase declares what ignored dependencies it used. Codex-managed Local Environment details may not transfer to Claude, so shared rules must live in Git-visible scripts and docs.

Do not automate loader apply, refresh, DB writes, Supabase changes, Docker volume deletion, force push, broad cleanup, data copy, or secret injection.

The lowest sufficient plan is the hybrid: ephemeral worktrees for implementation/review, main for operation, one explicit lab later.

## 001B Implementation Design

Do not execute yet.

001B should verify the Codex Local Environment schema in the UI, then create only approved minimal setup/action files. Candidate files:

- `scripts/sz_local_env_setup.ps1`
- `scripts/sz_worktree_audit.ps1`
- `.codex/*` only after schema verification
- `research/CODEX_PLATFORM_001B_IMPLEMENTATION.json`
- `research/reports/CODEX_PLATFORM_001B_IMPLEMENTATION.md`

Expected initial actions:

- Git Status
- Preflight Generic
- Scanner Only
- Unit Tests Targeted

First worktree candidate: `codex/PLATFORM_005B-load-observation-correction`.

Rollback: exact files only, no data/secret deletion, preserve branch if setup fails.

Expected commit: `platform: add safe Codex worktree environment`.

## Bastian Decisions

1. Approve the hybrid topology.
2. Decide whether direct `main` pushes remain allowed for exact-scope research artifacts.
3. Approve `PLATFORM_005B` as first pilot or choose another.
4. Decide whether `LOCAL_LAB` should ever be permanent before CG005G.
5. Decide whether `STOCK_ZERO_DATA_ROOT` becomes the standard local data pointer.
6. Confirm whether `.codex` Local Environment config may be versioned after UI schema verification.
