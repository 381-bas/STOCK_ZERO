# 015B KPIONE monthly input validation - No Apply

## Executive summary

- Operational month: `2026-06`
- Validation mode: `open`
- As-of date: `2026-07-11`
- Operational period status: `CLOSED_ELIGIBLE`
- Verdict: `WARN`
- Photo reports: `11`
- RUTA_RUTERO files: `4`
- Blockers: `0`
- Warnings: `1`
- Operational coverage: `2026-06-01..2026-06-28`
- Required coverage through: `2026-06-28`
- Pending future coverage: `None..None`
- Expected operational weeks: `4`

## Discovered inputs

- Monthly photo dir: `data/kpione_photo_reports/2026-06`
- Required calendar months now: `2026-06`
- Required calendar months at close: `2026-06`
- Ruta month dir: `data/RUTA_RUTERO/06 - JUNIO`
- Reference manifest present: `True`
- Reference manifest: `research/015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY/015_monthly_input_layout_manifest_2026_06.json`

## Photo reports

| source_file_id | file | role | rows | fecha_min | fecha_max | outside_calendar | selected_rows | adjacent_rows |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1781973512473 | photo-excel-admin_1781973512473.xlsx | include_candidate | 46078 | 2026-06-15 | 2026-06-19 | 0 | 46078 | 0 |
| 1781975989376 | photo-excel-admin_1781975989376.xlsx | include_candidate | 46807 | 2026-06-01 | 2026-06-07 | 0 | 46807 | 0 |
| 1781976368641 | photo-excel-admin_1781976368641.xlsx | quarantine_truncation | 50001 | 2026-06-08 | 2026-06-13 | 0 | 50000 | 0 |
| 1781976423312 | photo-excel-admin_1781976423312.xlsx | include_candidate | 107 | 2026-06-14 | 2026-06-14 | 0 | 107 | 0 |
| 1782012877303 | photo-excel-admin_1782012877303.xlsx | compare_only | 8372 | 2026-06-08 | 2026-06-08 | 0 | 8372 | 0 |
| 1782440454408 | photo-excel-admin_1782440454408.xlsx | include_candidate | 37908 | 2026-06-20 | 2026-06-24 | 0 | 37908 | 0 |
| 1783219885210 | photo-excel-admin_1783219885210.xlsx | include_candidate | 8372 | 2026-06-08 | 2026-06-08 | 0 | 8372 | 0 |
| 1783219914054 | photo-excel-admin_1783219914054.xlsx | include_candidate | 8672 | 2026-06-13 | 2026-06-13 | 0 | 8672 | 0 |
| 1783220085725 | photo-excel-admin_1783220085725.xlsx | include_candidate | 40488 | 2026-06-24 | 2026-06-28 | 0 | 40488 | 0 |
| 1783220157694 | photo-excel-admin_1783220157694.xlsx | include_candidate | 15889 | 2026-06-29 | 2026-06-30 | 0 | 0 | 15889 |
| 1783220552913 | photo-excel-admin_1783220552913.xlsx | include_candidate | 34838 | 2026-06-09 | 2026-06-12 | 0 | 34838 | 0 |

## Adjacent operational rows

| source_calendar_month | assigned_month | rows | status | source_files |
| --- | --- | --- | --- | --- |
| 2026-06 | 2026-07 | 15889 | valid_carry_forward | 1783220157694 |

## RUTA_RUTERO

| file | week_label | week_start | assigned_month | rows |
| --- | --- | --- | --- | --- |
| RUTA_RUTEROS_JUNIO_S1.xlsx | S1 | 2026-06-01 | 2026-06 | 3469 |
| RUTA_RUTEROS_JUNIO_S2.xlsx | S2 | 2026-06-08 | 2026-06 | 3542 |
| RUTA_RUTEROS_JUNIO_S3.xlsx | S3 | 2026-06-15 | 2026-06 | 3606 |
| RUTA_RUTEROS_JUNIO_S4.xlsx | S4 | 2026-06-22 | 2026-06 | 4056 |

## Operational weeks

| week_start | week_end | assigned_month | rows | source_files |
| --- | --- | --- | --- | --- |
| 2026-06-01 | 2026-06-07 | 2026-06 | 46807 | 1781975989376 |
| 2026-06-08 | 2026-06-14 | 2026-06 | 110361 | 1781976368641,1781976423312,1782012877303,1783219885210,1783219914054,1783220552913 |
| 2026-06-15 | 2026-06-21 | 2026-06 | 54726 | 1781973512473,1782440454408 |
| 2026-06-22 | 2026-06-28 | 2026-06 | 69748 | 1782440454408,1783220085725 |

## Transition week

- `week_start`: `2026-06-29`
- `week_end`: `2026-07-05`
- `assigned_operational_month`: `2026-07`
- `days_in_june`: `2`
- `days_in_july`: `5`
- `ruta_file_in_june_layout`: `None`
- `status`: `pending_july_route_reference`
- `blocking_for_015a_traceability`: `False`
- `required_before_future_operational_load`: `True`

## Blockers

- None

## Warnings

- `photo_invalid_date_rows:photo-excel-admin_1781976368641.xlsx:1`

## Guardrails no-apply

- DB access used: `False`
- Supabase used: `False`
- SQL apply: `False`
- DDL: `False`
- Productive loader run: `False`
- Data movement: `False`
