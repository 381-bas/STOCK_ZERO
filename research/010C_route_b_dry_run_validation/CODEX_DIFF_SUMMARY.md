# CODEX DIFF SUMMARY - 010C Route B

## Created

- `scripts/load_kpione2_photo_from_excel.py`: additive dry-run loader for KPIONE2 photo export grain validation.
- `tests/test_kpione2_photo_grain.py`: unit and real workbook tests for photo_row to event_row to day_presence behavior.
- `sql/15_kpione2_photo_raw_ddl.sql`: review-only additive DDL, guarded by `-- NO APPLY`.
- `sql/16_kpione2_photo_raw_ddl_rollback.sql`: review-only rollback DDL, guarded by `-- NO APPLY`.
- `research/010C_route_b_dry_run_validation/CODEX_CONTRACT_IMPACT.md`: contract impact note.
- `research/010C_route_b_dry_run_validation/CODEX_ROLLBACK_NOTE.md`: rollback note.
- `research/010C_route_b_dry_run_validation/CODEX_DIFF_SUMMARY.md`: this summary.
- `research/010C_route_b_dry_run_validation/CODEX_DRY_RUN_OUTPUT.json`: generated dry-run validation output.

## Not Modified

- `scripts/load_control_gestion_raw_v17.py`
- `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- Productive compliance views
- Governance locks and authority files

## Apply Status

- `db_apply = false`
- `sql_apply = false`
- `data_movement = false`
