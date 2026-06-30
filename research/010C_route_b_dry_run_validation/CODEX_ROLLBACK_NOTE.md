# CODEX ROLLBACK NOTE - 010C Route B

## Current Phase Rollback

Current work is additive and local-file only. Rollback is a Git revert of:

- `scripts/load_kpione2_photo_from_excel.py`
- `tests/test_kpione2_photo_grain.py`
- `sql/15_kpione2_photo_raw_ddl.sql`
- `sql/16_kpione2_photo_raw_ddl_rollback.sql`
- `research/010C_route_b_dry_run_validation/*`

No DB state exists to unwind because no DB apply or SQL apply was performed.
The dry-run JSON is evidence only and can be removed or regenerated without affecting data state.

## Future SQL Rollback

If a later RED-authorized phase applies the review DDL, rollback must use the dedicated review-only rollback file:

- `sql/16_kpione2_photo_raw_ddl_rollback.sql`

That file also starts with `-- NO APPLY` and must not be executed without separate explicit authorization.

## Productive Loader

No rollback path is needed for `scripts/load_control_gestion_raw_v17.py` because this phase did not touch it.
