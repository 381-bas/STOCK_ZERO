# 013 Closeout - Route B ORANGE No-Apply Dry-Run Claude Review

## Verdict

GO_WITH_LIMITS

## Phase

FAST_REFORM_013_ROUTE_B_ORANGE_NO_APPLY_DRY_RUN_CLAUDE_REVIEW

## Branch

lab/FAST_REFORM_013_route_b_orange_no_apply_dry_run_claude_review

## HEAD before closeout commit

4b0de6d

## Closed UTC

2026-07-02T03:58:14+00:00

## What was completed

- Opened 013 as ORANGE no-apply/dry-run phase.
- Integrated Claude as mandatory adversarial reviewer before Codex implementation.
- Prepared Claude review packet and scorecard.
- Recorded Claude initial review: APPROVE_WITH_WARNINGS.
- Implemented new local no-apply validator: scripts/validate_route_b_denominator_dry_run.py.
- Added focused tests: tests/test_route_b_denominator_dry_run.py.
- Added dry-run integration and denominator reconciliation evidence.
- Recorded Claude post-implementation audit: APPROVE_WITH_WARNINGS.
- Applied micro-fix from Claude post-audit: explicit route_row_count_unchanged comparison.
- Registered real photo-to-route normalization parity as future RED blocker.

## Test evidence

Command:

    python -m pytest tests/test_kpione2_photo_grain.py tests/test_route_b_denominator_dry_run.py -q

Observed result:

    22 passed, 8 subtests passed

## Productive scope

No productive action was authorized or executed.

- No Supabase writes.
- No SQL apply.
- No DDL.
- No DB reads/writes.
- No data movement.
- No backfill.
- No production cutover.
- No productive loader modification.
- No active contract semantic modification.
- No app/runtime modification.

## Evidence basis

The final 013 verdict is based on the new Route B denominator dry-run validator and focused tests, not on the older loader dry-run verdict alone.

The validator operates only on controlled local JSON input and emits structured safety evidence:

- db_access.used=false
- sql_apply=false
- writes_executed=false

## Key controls established

- photo_row -> event_row -> day_presence is tested structurally.
- day_presence_is_binary is not treated as proof by itself.
- photo_rows != distinct_event_ids is not treated as proof by itself.
- blank/null cod_rt and cliente_norm block.
- unmatched photo day-presence blocks instead of creating route denominator rows.
- seven local-sample metrics are explicitly defined: EXIGIDAS, VISITA, VISITA_REALIZADA, VISITA_REALIZADA_RAW, VISITA_REALIZADA_CAP, PENDIENTE and ALERTA.

## Important limitation

denominator_delta_zero=true in 013 is true by construction of the local model where route rows are the sole denominator authority.

The falsifiable protection in 013 is:

- no unmatched photo day-presence
- golden per-grain deltas
- structural event-to-day-presence checks
- blocking null/blank keys
- blocking invalid photo-count anomalies

## Deferred RED blockers

Before any productive apply or cutover, a future RED gate must explicitly validate:

- real photo-to-route normalization parity against the productive route/compliance path
- productive before/after reconciliation
- SQL/DDL apply plan
- rollback/cutover plan
- no denominator drift in real mart/view semantics
- no accidental widening of route grains from Route B photo evidence

## Final status

READY_FOR_PULL_REQUEST

## Recommended next phase after merge

ROUTE_B_RED_APPLY_GATE_OR_CONTROL_GESTION_VISIBLE_PRODUCT_SLICE
