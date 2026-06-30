# 010G Route B Integration Planning — No Apply

## Purpose

Plan the future integration of Route B into the CONTROL_GESTIÓN production flow.

This phase is planning-only.

## Current state

Route B dry-run is approved:

- `source_row_number` satisfied in dry-run.
- Claude post-audit verdict: `APPROVE`.
- Dry-run verdict: `PASS_ROUTE_B_DRY_RUN`.
- 22 blocking flags pass.
- 11 tests pass.

## Scope

Allowed:

- document integration plan
- document risks
- document validation gates
- define future ORANGE/RED split

Forbidden:

- DB apply
- SQL apply against Supabase real
- production loader modification
- active contract modification
- productive view modification
- data movement
- production cutover

## Output files

- `ROUTE_B_INTEGRATION_PLAN.md`
- `RISK_MATRIX.md`
- `VALIDATION_GATES.md`
