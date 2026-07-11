# 015A Monthly input layout traceability — No Apply

## Verdict

CONTRACT_PROMOTED_FROM_PRIOR_EVIDENCE_NO_APPLY.

## Scope

This 015A artifact promotes an existing rule from prior versioned evidence into a reusable monthly input layout contract. It does not rewrite 014C, 014D or 014E history.

## Promoted authority

- Source: `scripts/validate_june_data_foundation_gate_014A_no_apply.py`
- Prior text: `Semana operativa lunes-domingo; asignación al mes con >=4 días.`
- Prior example: `2026-06-29..2026-07-05` assigned to `2026-07`.
- Supporting reports:
  - `research/014C_KPIONE_RAW_EXPORT_VALIDATOR_NO_APPLY/014C_kpione_raw_export_report.md`
  - `research/014D_KPIONE_RAW_EXPORT_REMEDIATION_NO_APPLY/014D_remediation_report.md`

## Reusable rule

- Semana operativa: lunes a domingo.
- `week_start` se calcula como lunes desde `Fecha` interna del archivo.
- Una semana pertenece al mes que contiene al menos 4 días de esa semana.
- Si una semana cruza dos meses, se asigna al mes con 4 o más días.
- La carpeta mensual agrupa inputs, pero no gobierna pertenencia semanal.

## June 2026 examples

| Week | Assigned operational month |
|---|---|
| 2026-06-01..2026-06-07 | 2026-06 |
| 2026-06-08..2026-06-14 | 2026-06 |
| 2026-06-15..2026-06-21 | 2026-06 |
| 2026-06-22..2026-06-28 | 2026-06 |
| 2026-06-29..2026-07-05 | 2026-07 |

## Input layout

- Legacy supported shape: `data/photo-excel-admin_*.xlsx`
- Monthly supported shape: `data/kpione_photo_reports/YYYY-MM/photo-excel-admin_*.xlsx`
- Active 2026-06 shape: `data/kpione_photo_reports/2026-06/photo-excel-admin_*.xlsx`
- `source_file_id` remains the numeric token in `photo-excel-admin_{source_file_id}.xlsx`.
- The versioned JSON manifest is the authority for `source_file_id`, `relative_path`, `sha256`, `size_bytes`, `row_count`, `fecha_min`, `fecha_max`, and role for the 11 photo reports.

## Local traceability evidence

- `pre_count=11`
- `post_count=11`
- Root `data/photo-excel-admin_*.xlsx` count after move: `0`
- Monthly `data/kpione_photo_reports/2026-06/photo-excel-admin_*.xlsx` count after move: `11`
- Operator-reported comparison: `file_name`, `sha256`, `size_bytes` without differences.
- Local CSV evidence:
  - `research/015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY/015_kpione_photo_reports_2026_06_pre_move_manifest.csv`
  - `research/015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY/015_kpione_photo_reports_2026_06_post_move_manifest.csv`
- The CSV files remain auxiliary local evidence; the JSON manifest is now autosufficient for the 11 file identities and hashes.

## Photo report manifest counts

- Total photo reports: `11`
- `include_candidate`: `9`
- `quarantine_truncation`: `1`
- `compare_only`: `1`

## Ruta rutero reference

Existing June route files:

| File | Week label | Week start | Assigned month |
|---|---|---|---|
| `data/RUTA_RUTERO/06 - JUNIO/RUTA_RUTEROS_JUNIO_S1.xlsx` | S1 | 2026-06-01 | 2026-06 |
| `data/RUTA_RUTERO/06 - JUNIO/RUTA_RUTEROS_JUNIO_S2.xlsx` | S2 | 2026-06-08 | 2026-06 |
| `data/RUTA_RUTERO/06 - JUNIO/RUTA_RUTEROS_JUNIO_S3.xlsx` | S3 | 2026-06-15 | 2026-06 |
| `data/RUTA_RUTERO/06 - JUNIO/RUTA_RUTEROS_JUNIO_S4.xlsx` | S4 | 2026-06-22 | 2026-06 |

S1-S4 belong to `2026-06`. The week `2026-06-29..2026-07-05` belongs operationally to `2026-07`.

## Transition week

The transition week is declared explicitly instead of inventing a June S5:

- `week_start`: `2026-06-29`
- `week_end`: `2026-07-05`
- `assigned_operational_month`: `2026-07`
- `days_in_june`: `2`
- `days_in_july`: `5`
- `ruta_file_in_june_layout`: `null`
- `status`: `pending_july_route_reference`
- `blocking_for_015a_traceability`: `false`
- `required_before_future_operational_load`: `true`

## Guardrails

- No Supabase.
- No DB connection.
- No SQL apply.
- No DDL.
- No productive loader changes.
- No app runtime switch.
- No data movement.
- No git add all.

## Future Supabase readiness, no apply

Before any future apply, require a versioned manifest with per-file `source_path`, `source_file_id`, `sha256`, `row_count`, `fecha_min`, `fecha_max`, schema signature, and route-week reconciliation. This 015A artifact is not an apply authorization.
