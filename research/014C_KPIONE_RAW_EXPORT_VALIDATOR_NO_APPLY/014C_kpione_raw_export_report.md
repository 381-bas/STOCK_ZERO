# 014C KPIONE Raw Export Validator — No Apply

## Resumen ejecutivo

- Verdict: `RAW_EXPORTS_BLOCKED_BY_TRUNCATION_OR_CONFLICT`
- Raw files inspected: 6
- Raw rows: 189273
- Canonical local rows after exact-row dedupe: 182783
- Distinct event IDs: 28087
- Covered June days: 24
- Missing June days: 6

## Guardrails

| Guardrail | State |
|---|---|
| DB / Supabase access | not used |
| SQL / DDL apply | not used |
| Productive loader / refresh | not used |
| UX modification | not used |
| Input file mutation or movement | not used |
| Output | lightweight JSON/Markdown only |

## Manifest por archivo

| File | Rows | Distinct IDs | Fecha min | Fecha max | truncation_suspect | Critical missing |
|---|---:|---:|---|---|---|---|
| photo-excel-admin_1781973512473.xlsx | 46078 | 6997 | 2026-06-15 | 2026-06-19 | NO | none |
| photo-excel-admin_1781975989376.xlsx | 46807 | 7101 | 2026-06-01 | 2026-06-07 | NO | none |
| photo-excel-admin_1781976368641.xlsx | 50001 | 7832 | 2026-06-08 | 2026-06-13 | YES | none |
| photo-excel-admin_1781976423312.xlsx | 107 | 14 | 2026-06-14 | 2026-06-14 | NO | none |
| photo-excel-admin_1782012877303.xlsx | 8372 | 1295 | 2026-06-08 | 2026-06-08 | NO | none |
| photo-excel-admin_1782440454408.xlsx | 37908 | 5892 | 2026-06-20 | 2026-06-24 | NO | none |

## Truncation and duplicate gates

- Threshold: rows >= 50000
- Suspect files: 1
- Suspect IDs: 1781976368641
- Overlapping IDs: 1044
- same_id_same_hash: 1043
- same_id_diff_hash conflicts: 1
- event_stable_hash conflicts: 0

## Cobertura junio

| Week | Start | End | Status | Missing days |
|---|---|---|---|---|
| S1 | 2026-06-01 | 2026-06-07 | COMPLETE | none |
| S2 | 2026-06-08 | 2026-06-14 | COMPLETE | none |
| S3 | 2026-06-15 | 2026-06-21 | COMPLETE | none |
| S4 | 2026-06-22 | 2026-06-28 | PARTIAL | 2026-06-25, 2026-06-26, 2026-06-27, 2026-06-28 |

- Covered days: 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-06, 2026-06-07, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-13, 2026-06-14, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-20, 2026-06-21, 2026-06-22, 2026-06-23, 2026-06-24
- Missing days: 2026-06-25, 2026-06-26, 2026-06-27, 2026-06-28, 2026-06-29, 2026-06-30
- Missing 2026-06-25..2026-06-30: 2026-06-25, 2026-06-26, 2026-06-27, 2026-06-28, 2026-06-29, 2026-06-30
- Week 2026-06-29..2026-07-05 belongs to July: yes

## Paridad 2026-06-01 contra legacy

- raw_id_count: 1224
- legacy_id_count: 1221
- matched_id_count: 1220
- raw_only_count: 4
- legacy_only_count: 1
- match_rate: 0.9992
- informative threshold >= 0.99: met
- raw-only sample (max 20): 871237, 875271, 875273, 877542
- legacy-only sample (max 20): 855811

## Replica local de VISITA

- Formula: `1 / count(Codigo Local, Fecha, Marca)`, with `Local` fallback when Codigo Local is blank.
- Eligible rows: 182782
- Groups / VISITA sum: 27920 / 27920.000000
- Local fallback groups: 8
- Every group sums to 1: true

## Blockers

- truncation_suspect:photo-excel-admin_1781976368641.xlsx
- same_id_diff_hash_conflicts:1

## Warnings

- dedupe_silent_candidate_ids:1043
- june_missing_days:2026-06-25,2026-06-26,2026-06-27,2026-06-28,2026-06-29,2026-06-30
- june_operational_weeks_incomplete
- raw_blank_id_rows:photo-excel-admin_1781976368641.xlsx:1
- raw_invalid_date_rows:photo-excel-admin_1781976368641.xlsx:1

## Decision recomendada

Resolve truncation/conflict blockers and rerun 014C before any loader design.

## Siguiente fase propuesta

`014C_REMEDIATE_RAW_EXPORT_GAPS_OR_BLOCKERS_NO_APPLY`

## Declaracion explicita

This validation used no Supabase, no DB, no SQL/DDL apply, no productive loader,
no refresh, no UX modification, no backfill, no cutover and no data movement.
The raw exports and legacy master were read-only inputs.
