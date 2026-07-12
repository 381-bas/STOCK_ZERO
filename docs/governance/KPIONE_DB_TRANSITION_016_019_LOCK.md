# KPIONE DB Transition Roadmap

Directive: `KPIONE_DB_TRANSITION_016_019_LOCK_V1`

Authority: repository path plus Git commit history.

Status: `ACTIVE_LOCKED`

016B result: `IDEMPOTENCY_CONTRACT_SELECTED`

Next permitted unit: `017_APPLY_RUNNER_AND_REHEARSAL`

017 authorization: `false`

Productive apply authorization: `false`

## Operating Model V2

The roadmap uses four operational units instead of standalone closeout and subphase PRs. Git and GitHub are the authority for branches, commits, changed files, PRs, and merge history. Future work should reference repository paths and commit history instead of manually propagating directive SHA256 values.

Normal repo-only units do not require a separate closeout phase. A unit is complete when its objective is satisfied, tests pass, the PR is merged, and `PROJECT_STATE` is updated.

## Roadmap

| unit | purpose | status |
| --- | --- | --- |
| `016B_IDENTITY_GRAIN_AND_IDEMPOTENCY_CONTRACT` | Decide source grain, persisted grain, destination or staging boundary, authoritative identity, idempotency behavior, correction/replay behavior, and readiness for runner construction. | Selected and closed. |
| `017_APPLY_RUNNER_AND_REHEARSAL` | Build or rehearse the runner after 016B selects the contract. Old 017A, 017B, and 017C are internal gates. | Next permitted unit, not authorized. |
| `018_PRODUCTIVE_APPLY` | Execute productive apply only after explicit productive authorization. | Not authorized. |
| `019_POST_APPLY_TRIAL` | Monitor trial outcome and decide final transition posture. | Not authorized. |

Compatibility:

- Old `016C` is absorbed into the `016B` exit gate.
- Old `017A`, `017B`, and `017C` are absorbed into internal gates of `017`.
- Historical evidence keeps its original names.

## Risk Classes

| class | minimum governance |
| --- | --- |
| `R1_REPO_SAFE` | PR + tests. |
| `R2_ARCHITECTURE` | ADR + PR + tests. |
| `R3_PRODUCTIVE` | Authorization + plan + precheck + execution evidence + postcheck. |
| `R4_DESTRUCTIVE` | Explicit authorization + impact analysis + backup + recovery plan. |

## Safety Gates

- No Supabase access is authorized.
- No SQL execution is authorized.
- No DB reads or writes are authorized.
- No app runtime, loader, or further contracts 013-016 technical artifact changes are authorized without a separate prompt.
- No apply, cutover, retention change, cleanup, archive, drop, or destructive action is authorized.
- Selecting `016B` does not open, design, implement, or execute `017`.
