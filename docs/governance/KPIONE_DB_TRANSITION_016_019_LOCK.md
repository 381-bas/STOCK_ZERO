# KPIONE DB Transition 016-019 Lock

Directive ID: `KPIONE_DB_TRANSITION_016_019_LOCK_V1`

Status: `ACTIVE_LOCKED`

Original activation branch: `lab/FAST_REFORM_016_kpione_monthly_db_precheck_read_only`

016A amendment branch: `lab/FAST_REFORM_016A_directive_amendment_legacy_closeout`

Base: `main@26fed014a2429811723ea35218a8619d9178efea`

Current phase: `016A`

Allowed next phase: `016B`

Retention policy: `DEFERRED_UNTIL_POST_TRIAL`

Destructive action authorized: `false`

## Integrity Contract

Canonical hash method: `SHA256_OF_CANONICAL_GIT_BLOB_BYTES_LF`

Canonical source: Git index or committed blob bytes with LF line endings.

Working-tree checkout bytes are not authoritative. A Windows CRLF checkout can produce a different SHA256 without semantic drift.

Previous canonical SHA for the pre-amendment directive: `cce9eea337c07c56b722968beaa7eb481e79028b60fa218e20875fb71be2e46e`

## Phase Table

| phase | objective | entry gate | exit gate | writes allowed | authorization required |
| --- | --- | --- | --- | --- | --- |
| 016 | Complete read-only DB precheck evidence for KPIONE monthly transition without writes. | Branch `lab/FAST_REFORM_016_kpione_monthly_db_precheck_read_only` at `main@986d4b0`; `DB_URL_CODEX_RO` only if DB is touched. | Evidence is aggregate, read-only guardrails pass, discrepancy and `NO_SOURCE_SIGNAL` caveats are documented. | Repo-only evidence/script/test writes already authorized for 016; no DB writes. | Bastian/ChatGPT explicit authorization for any additional file outside 016 scope. |
| 016A | Formally close phase 016 evidence, register PR #44 and merge 26fed01, reconcile the canonical roadmap hash method, and transition governance to allow a separately authorized 016B design phase. | Phase 016 evidence is merged in PR #44; repository main is 26fed01; hash reconciliation classified the prior mismatch as line-ending-only; Bastian and ChatGPT explicitly authorize this amendment. | Directive, human roadmap, PROJECT_STATE, PROJECT_LEDGER, derived roadmap constants, tests, and 016A evidence agree on current_phase 016A, allowed_next_phase 016B, canonical Git-blob SHA method, and all no-apply guardrails. No DB access or 016B work occurred. | Only the nine explicitly authorized repository files recorded in the 016A closeout evidence. No DB writes. | Explicit separate Bastian and ChatGPT prompt before any 016B work. |
| 016B | Design exact overlap/idempotency subphase using persisted identities or safe staging contract. | 016A evidence accepted. | Exact comparison strategy is documented with cost, indexes, and no row-level export. | Design artifacts only. | Explicit 016B reprompt. |
| 016C | Close pre-apply readiness decision for KPIONE June plan. | 016B design accepted. | Apply is either blocked with evidence or promoted to 017A with gates. | Governance/evidence artifacts only. | Explicit 016C reprompt. |
| 017A | Prepare apply design and rollback design without execution. | 016C promotes the work to 017A. | Apply/rollback plan is auditable and no execution has occurred. | Design artifacts and tests only. | Explicit 017A reprompt. |
| 017B | Implement apply runner or staging mechanism under no-apply guardrails. | 017A design accepted. | Runner/tests exist and default to no-apply. | Repo code/tests/evidence only; no DB writes. | Explicit 017B reprompt. |
| 017C | Dry-run apply rehearsal and final apply readiness gate. | 017B implementation accepted. | Dry-run evidence proves scope, idempotency, rollback path, and safety gates. | Read-only/dry-run evidence only. | Explicit 017C reprompt. |
| 018 | Execute approved productive apply only if separately authorized. | 017C readiness accepted and explicit productive apply authorization granted. | Apply result, rollback readiness, and post-apply validation documented. | DB writes only if exact command, role, phase, rollback, and confirmation are authorized. | Separate Bastian productive apply authorization. |
| 019 | Post-apply trial, monitoring, and final transition decision. | 018 apply completed or explicitly skipped with evidence. | Trial outcome accepted; deferred retention/destructive topics may be reconsidered only after this phase. | Monitoring/evidence; destructive actions remain disallowed unless a new directive amendment exists. | Explicit 019 reprompt and any separate production authorization. |

## NON_NEGOTIABLE_SEQUENCE

The sequence is locked:

1. `016`
2. `016A`
3. `016B`
4. `016C`
5. `017A`
6. `017B`
7. `017C`
8. `018`
9. `019`

Only one phase is active: `016A`. The only allowed next phase is `016B`.

`allowed_next_phase = 016B` does not authorize 016B. Phase 016B requires a separate explicit Bastian and ChatGPT prompt after 016A evidence is accepted.

## 016 And 016A Status

Phase `016` is closed by PR #44 merged at `26fed014a2429811723ea35218a8619d9178efea`.

Phase `016A` is a governance closeout and directive amendment phase. It does not access Supabase, execute SQL, write to DB, apply, cut over, modify app runtime, modify loaders, modify contracts 014/015, or perform destructive legacy action.

## DEFERRED_UNTIL_POST_TRIAL

The following topics are deferred until after phase `019` closes:

- `retention_policy_changes`
- `cleanup_drop_archive_deprecate`
- `destructive_sql`
- `loader_productive_refactor`
- `app_runtime_source_switch`
- `contracts_014_015_semantic_changes`
- `materialized_view_refresh_policy_changes`
- `historical_backfill_beyond_approved_kpione_transition`

## DEVIATION_REQUIRES_DIRECTIVE_AMENDMENT

Any deviation from the locked sequence, active phase, allowed next phase, deferred topics, or destructive-action ban requires an explicit amendment to `KPIONE_DB_TRANSITION_016_019_LOCK_V1` before work continues.

An amendment must include:

- `directive_id`
- `reason`
- `changed_sequence_or_gate`
- `risk_assessment`
- `authorization_source`
- external registration of the new reproducible SHA256

## Hard Restrictions

- No DB writes.
- No SQL execution for this amendment.
- No Supabase mutation or access.
- No loader modification.
- No app runtime modification.
- No contracts 014/015 modification.
- No destructive action.
- No 016B design, implementation, execution, or PR readiness change in this phase.
- No automatic merge of the resulting pull request.
