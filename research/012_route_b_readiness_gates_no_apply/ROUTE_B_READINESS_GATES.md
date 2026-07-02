# Route B Readiness Gates - No Apply

## Context

Phase: FAST_REFORM_012_ROUTE_B_READINESS_GATES_NO_APPLY
Branch: lab/FAST_REFORM_012_route_b_readiness_gates_no_apply
HEAD: e2fb111
Generated UTC: 2026-07-02T01:22:04Z

## Purpose

Convert unresolved Route B warnings from prior planning/audit phases into explicit readiness gates before any implementation that can affect productive runtime, active loader behavior, active contracts, SQL apply, Supabase writes, data movement, backfill or cutover.

## Evidence used

- research/012_route_b_readiness_gates_no_apply/SOURCE_INVENTORY.md
- research/012_route_b_readiness_gates_no_apply/EXISTING_TEST_RUN.md
- tests/test_kpione2_photo_grain.py
- tests/test_cg_route_weekly_local_lab.py

## Test evidence

Command:

python -m pytest tests/test_kpione2_photo_grain.py tests/test_cg_route_weekly_local_lab.py -q

Observed result:

19 passed, 1 skipped, 10 subtests passed
Exit code: 0
Runtime: approximately 2m24s.

Interpretation:

- Existing local grain/control tests pass.
- This supports readiness analysis.
- This does not authorize productive writes, productive loader changes, SQL apply, Supabase writes, backfill or cutover.

---

# Gate 1 - Compliance denominator reconciliation

## Question

Can Route B proceed toward implementation without changing the active compliance denominator or active fulfillment semantics?

## Protected metrics

Route B must preserve the active meaning of:

- EXIGIDAS
- VISITA
- VISITA_REALIZADA
- VISITA_REALIZADA_RAW
- VISITA_REALIZADA_CAP
- PENDIENTE
- ALERTA

## Required invariant

Route B photo data must preserve the grain chain:

photo_row -> event_row -> day_presence

Compliance must continue operating at day-presence semantics, not photo-row semantics.

## Explicit risk

The critical failure mode is:

one Excel photo row = one visit

That would inflate evidence and distort compliance.

## Readiness status

PASS_FOR_NO_APPLY_READINESS

## Evidence

The existing local tests passed. The source inventory confirms the active contract and prior audits identify the same grain risk.

## Condition for future ORANGE implementation

Future implementation must include before/after reconciliation showing no unintended change in weekly denominator and fulfillment outputs for selected controlled samples.

Minimum future comparison:

before Route B active path vs after Route B active path
by semana_inicio, cod_rt, cliente_norm
metrics: EXIGIDAS/VISITA, VISITA_REALIZADA_RAW, VISITA_REALIZADA_CAP, PENDIENTE, ALERTA

## Blocker condition

Any implementation that maps photo rows directly to visits is NO_GO.

---

# Gate 2 - Forward-only / backfill scope decision

## Decision

FORWARD_ONLY_DEFAULT

## Rule

Route B implementation should enter as forward-only by default.

## Reason

Backfill can alter historical compliance interpretation and must not be mixed with initial implementation.

## Backfill classification

Historical backfill is a separate higher-risk phase.

Required before any backfill:

- explicit business reason
- affected date range
- expected metric deltas
- rollback plan
- Bastian approval
- separate PR/fase if risk scope changes
- no silent overwrite of historical interpretation

## Readiness status

PASS_WITH_LIMIT

## Condition for future ORANGE implementation

Implementation may proceed only as forward-only unless a separate authorization explicitly promotes backfill.

---

# Gate 3 - Rollback and production cutover

## Decision

ROLLBACK_REQUIRED_BEFORE_PRODUCTIVE_ACTIVATION

## Required cutover model

Future implementation must define:

- activation method
- deactivation method
- default inactive/safe state
- before/after comparison
- evidence required to continue
- evidence requiring rollback
- owner of final productive authorization

## Required rollback model

At minimum:

code rollback = git revert
DB rollback = committed no-apply rollback SQL before apply phase
runtime rollback = feature flag/source order exclusion/default inactive path
data rollback = not applicable unless data movement/backfill is explicitly authorized

## Readiness status

PASS_WITH_LIMIT

## Blocker condition

No productive cutover can occur without a committed rollback path and explicit RED authorization.

---

# Overall readiness verdict

GO_WITH_LIMITS

## Meaning

Route B may proceed to a later controlled implementation phase only if the next phase respects these limits:

- no automatic backfill
- no direct photo-row-to-visit mapping
- no productive writes without explicit authorization
- no modification of productive loader without ORANGE scope
- no SQL/Supabase apply without RED authorization
- denominator reconciliation must be evidenced before productive activation
- rollback/cutover must exist before productive activation

## Recommended next phase

FAST_REFORM_013_ROUTE_B_ORANGE_IMPLEMENTATION_NO_APPLY_OR_DRY_RUN_INTEGRATION

Only after 012 is reviewed and merged.

## Not authorized by this verdict

- SQL apply
- Supabase writes
- productive loader modification
- active contract semantic modification
- production cutover
- data movement
- historical backfill
