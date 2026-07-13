# KPIONE DB Transition Roadmap

Directive: `KPIONE_DB_TRANSITION_016_019_LOCK_V1`

Authority: repository path plus Git commit history.

Status: `ACTIVE_LOCKED`

016B result: `IDEMPOTENCY_CONTRACT_SELECTED`

016B source authority: `PHOTO_EXPORT_SELECTED_AS_FUTURE_PRODUCTIVE_AUTHORITY`

Route A status: `HISTORICAL_BOOTSTRAP_AND_COMPATIBILITY_REFERENCE`

017 result: `RUNNER_AND_REHEARSAL_READY`

Next permitted unit: `018_PRODUCTIVE_APPLY`

018 authorization: `false`

Productive apply authorization: `false`

## Operating Model V2

The roadmap uses four operational units instead of standalone closeout and subphase PRs. Git and GitHub are the authority for branches, commits, changed files, PRs, and merge history. Future work should reference repository paths and commit history instead of manually propagating directive SHA256 values.

Normal repo-only units do not require a separate closeout phase. A unit is complete when its objective is satisfied, tests pass, the PR is merged, and `PROJECT_STATE` is updated.

## Roadmap

| unit | purpose | status |
| --- | --- | --- |
| `016B_IDENTITY_GRAIN_AND_IDEMPOTENCY_CONTRACT` | Select `photo-excel-admin_*.xlsx / Fotos` as future productive authority and keep `data/CUMPLIMIENTO_FRECUENCIA.xlsx / DB (KPIONE2.0)` as historical bootstrap/parity reference only. | Selected and closed. |
| `017_APPLY_RUNNER_AND_REHEARSAL` | Build and rehearse the folder-based Route B ingestion runner after 016B selects the contract. Old 017A, 017B, and 017C are internal gates. | Completed; runner and local rehearsal ready. |
| `018_PRODUCTIVE_APPLY` | Execute the productive apply gate only after explicit productive authorization. | Not authorized. |
| `019_POST_APPLY_TRIAL` | Run correction, monitoring trial, and final transition decision. | Not authorized. |

Compatibility:

- Old `016C` is absorbed into the `016B` exit gate.
- Old `017A`, `017B`, and `017C` are absorbed into internal gates of `017`.
- Historical evidence keeps its original names.
- Route A is not the target architecture.

## Risk Classes

| class | minimum governance |
| --- | --- |
| `R1_REPO_SAFE` | PR + tests. |
| `R2_ARCHITECTURE` | ADR + PR + tests. |
| `R3_PRODUCTIVE` | Authorization + plan + precheck + execution evidence + postcheck. |
| `R4_DESTRUCTIVE` | Explicit authorization + impact analysis + backup + recovery plan. |

## Safety Gates

- No Supabase access is authorized.
- SQL and DB access remain unauthorized outside the completed ephemeral local PostgreSQL rehearsal.
- No app runtime, loader, or further contracts 013-016 technical artifact changes are authorized without a separate prompt.
- No apply, cutover, retention change, cleanup, archive, drop, or destructive action is authorized.
- Completing `017` does not authorize `018` productive apply.
