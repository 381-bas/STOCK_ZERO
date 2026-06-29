# CODEX CONTRACT IMPACT - 010C Route B

## Scope

This note covers additive Route B dry-run work for `photo-excel-admin_1782440454408.xlsx`.

## Active Contract

- Contract file: `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- Contract status: ACTIVE
- Contract modified: no

## Required Grain

- `input_grain = photo_row`
- `normalized_grain = event_row`
- `compliance_grain = day_presence`
- `forbidden_assumption = one_excel_row_equals_one_visit`

## Impact

- The new loader reads the photo export and groups photo rows by trimmed `ID` before deriving event rows.
- Day presence is derived from event rows as binary presence per `(fecha, cod_rt, cliente_norm)`.
- Photo-level fields remain excluded from event identity hashing: photo count, link, upload timestamp, hour, and task label.
- The productive loader `scripts/load_control_gestion_raw_v17.py` is not modified or imported.
- The active contract JSON is not modified.

## Evidence Targets

- `photo_rows = 37908`
- `distinct_event_ids = 5892`
- `fecha_min = 2026-06-20`
- `fecha_max = 2026-06-24`
- `db_apply = false`
- `sql_apply = false`

## Dry-Run Result

- Output file: `research/010C_route_b_dry_run_validation/CODEX_DRY_RUN_OUTPUT.json`
- Verdict: `PASS_ROUTE_B_DRY_RUN`
- Productive loader touched: false
