# Claude Review Scorecard - 013

## Claude verdict

- Verdict: APPROVE_WITH_WARNINGS
- Review date UTC: recorded during FAST_REFORM_013_ROUTE_B_ORANGE_NO_APPLY_DRY_RUN_CLAUDE_REVIEW

## Scoring

| Dimension | Score 0-3 | Notes |
|---|---:|---|
| Preserves no-apply scope | 3 | Claude confirmed no active scope violation, with hard restrictions maintained. |
| Detects denominator risk | 3 | Strong finding: denominator risk is most likely through joins, normalization and week alignment, not raw counting alone. |
| Identifies missing tests | 3 | Clear adversarial tests proposed for day_presence, 1-photo events, null keys, normalization and golden reconciliation. |
| Separates 013 scope from future RED scope | 3 | Productive reconciliation, SQL apply, backfill and cutover are correctly deferred. |
| Gives implementable guidance for Codex | 3 | New validator + targeted tests, without touching loaders/contracts/SQL. |
| Avoids overengineering | 2 | Useful but strict; some file-freeze recommendations are accepted for 013 only, not as permanent doctrine. |

## Findings classification

### Valid findings to incorporate

- Existing day_presence_is_binary evidence is tautological and must not be treated as sufficient proof.
- The anti "one Excel photo row = one visit" check is too weak if based only on aggregate inequality.
- Null or blank denominator keys such as cod_rt and cliente_norm need blocking guards in local validation.
- Denominator risk is mainly join/normalization/week alignment risk, not only row-count risk.
- 013 needs an explicit local-sample definition for the seven protected outputs before Codex implementation.
- Codex should create a new no-apply validator and targeted tests.
- Codex must not edit productive loaders, active contracts, SQL, Supabase or production runtime.

### Findings rejected or limited

- Treating tests/test_cg_route_weekly_local_lab.py and scripts/cg_route_weekly_local_lab.py as permanently untouchable is not adopted as permanent doctrine.
- For 013, the restriction is accepted tactically to keep the scope surgical.

### Findings deferred to future phase

- Reconciliation against Supabase/productive objects.
- SQL/DDL apply.
- Backfill.
- Production cutover.
- Productive rollback execution.
- Active contract semantic changes.
- Productive loader changes.
- RED gate approval.

## ChatGPT/Bastian decision

ACCEPT_WITH_SCOPE_CONTROL.

Claude review is strong enough to pass to Codex, but only under the following filter:

- Implement only local no-apply/dry-run validation.
- Prefer a new script over editing existing loader files.
- Add or extend focused tests under Route B/photo-grain scope.
- Define local-sample metrics for EXIGIDAS, VISITA, VISITA_REALIZADA, VISITA_REALIZADA_RAW, VISITA_REALIZADA_CAP, PENDIENTE and ALERTA.
- Emit structured evidence declaring db_access.used=false, sql_apply=false and writes_executed=false.
- Preserve photo_row -> event_row -> day_presence.
- Stop on any denominator delta not explained by controlled local sample semantics.

## Codex implementation filter

Codex may implement only items classified as inside 013 ORANGE_NO_APPLY_DRY_RUN scope.

Allowed next implementation shape:

- New local validator script.
- New or extended Route B tests.
- Research evidence under research/013_route_b_orange_no_apply_dry_run_claude_review.
- No loader edits.
- No active contract edits.
- No SQL/apply/Supabase/backfill/cutover.
