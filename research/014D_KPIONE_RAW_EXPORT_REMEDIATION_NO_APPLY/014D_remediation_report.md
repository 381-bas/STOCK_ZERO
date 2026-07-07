# 014D KPIONE Raw Export Remediation — No Apply

## Resumen ejecutivo

- Verdict: `REMEDIATION_READY_FOR_DRY_RUN_LOADER_WITH_WARNINGS`
- Candidate files: 9
- Candidate canonical rows: 229070
- Candidate distinct event IDs: 35287
- Candidate coverage: 30/30 calendar days
- ID 862144: `resolved_by_excluding_truncated`

## Candidate set

| Source file ID | File | Rows | Fecha min | Fecha max | Truncation |
|---|---|---:|---|---|---|
| 1781975989376 | photo-excel-admin_1781975989376.xlsx | 46807 | 2026-06-01 | 2026-06-07 | False |
| 1783219885210 | photo-excel-admin_1783219885210.xlsx | 8372 | 2026-06-08 | 2026-06-08 | False |
| 1783220552913 | photo-excel-admin_1783220552913.xlsx | 34838 | 2026-06-09 | 2026-06-12 | False |
| 1783219914054 | photo-excel-admin_1783219914054.xlsx | 8672 | 2026-06-13 | 2026-06-13 | False |
| 1781976423312 | photo-excel-admin_1781976423312.xlsx | 107 | 2026-06-14 | 2026-06-14 | False |
| 1781973512473 | photo-excel-admin_1781973512473.xlsx | 46078 | 2026-06-15 | 2026-06-19 | False |
| 1782440454408 | photo-excel-admin_1782440454408.xlsx | 37908 | 2026-06-20 | 2026-06-24 | False |
| 1783220085725 | photo-excel-admin_1783220085725.xlsx | 40488 | 2026-06-24 | 2026-06-28 | False |
| 1783220157694 | photo-excel-admin_1783220157694.xlsx | 15889 | 2026-06-29 | 2026-06-30 | False |

## Archivos en cuarentena / compare-only

| Source file ID | File | Role | Rows |
|---|---|---|---:|
| 1781976368641 | photo-excel-admin_1781976368641.xlsx | quarantine_truncation | 50001 |
| 1782012877303 | photo-excel-admin_1782012877303.xlsx | compare_only | 8372 |

## Cobertura global

- Baseline 014C covered days: 24
- Baseline 014C missing: 2026-06-25, 2026-06-26, 2026-06-27, 2026-06-28, 2026-06-29, 2026-06-30
- Candidate covered days: 30
- Candidate missing: none
- S1-S4 complete: true
- June calendar complete: true
- 2026-06-29..30 are reported in June calendar; their operational week belongs to July.

## Resolucion del truncado y comparisons

| Comparison | Matched IDs | Same hash | Different hash | Stable conflicts |
|---|---:|---:|---:|---:|
| truncated_06_08_13_vs_replacements | 7832 | 7828 | 4 | 3 |
| old_06_08_patch_vs_new_06_08 | 1295 | 1295 | 0 | 0 |
| old_06_20_24_vs_new_06_24_28_on_06_24 | 1579 | 1579 | 0 | 0 |

## Resolucion ID 862144

- Classification: `resolved_by_excluding_truncated`

| Role | Source file ID | Row | Fecha | N Fotos | Tipo tarea |
|---|---|---:|---|---|---|
| quarantine_truncation | 1781976368641 | 49999 | 2026-06-08 | 1/4 | Antes de reposicion |
| quarantine_truncation | 1781976368641 | 50000 | 2026-06-08 | 2/4 | Despues de reposicion |
| quarantine_truncation | 1781976368641 | 50001 | 2026-06-08 | 3/4 | Panoramica |
| compare_only | 1782012877303 | 6489 | 2026-06-08 | 1/4 | Antes de reposicion |
| compare_only | 1782012877303 | 6490 | 2026-06-08 | 2/4 | Despues de reposicion |
| compare_only | 1782012877303 | 6491 | 2026-06-08 | 3/4 | Panoramica |
| compare_only | 1782012877303 | 6492 | 2026-06-08 | 4/4 | Bodega |
| include_candidate | 1783219885210 | 6489 | 2026-06-08 | 1/4 | Antes de reposicion |
| include_candidate | 1783219885210 | 6490 | 2026-06-08 | 2/4 | Despues de reposicion |
| include_candidate | 1783219885210 | 6491 | 2026-06-08 | 3/4 | Panoramica |
| include_candidate | 1783219885210 | 6492 | 2026-06-08 | 4/4 | Bodega |

Full lightweight row details, links and hashes are preserved in the JSON manifest.

## Dedupe / overlaps del candidate

- overlapping IDs: 1579
- same_id_same_hash: 1579
- same_id_diff_hash: 0
- event_stable_hash conflicts: 0
- exact duplicate photo rows removed: 10089

## VISITA candidate

- Formula: `1 / count(Codigo Local, Fecha, Marca)`, with `Local` fallback.
- group_count: 34996
- visit_sum: 34996.000000
- groups where sum != 1: 0
- fallback group count: 14

## Paridad legacy 2026-06-01

- raw_id_count: 1224
- legacy_id_count: 1221
- matched_id_count: 1220
- raw_only_count: 4
- legacy_only_count: 1
- match_rate: 0.9992

## Blockers

- none

## Warnings

- candidate_dedupe_same_hash_ids:1579
- candidate_exact_duplicate_rows_removed:10089
- candidate_expected_overlap_dates:2026-06-24
- compare_only_diff_hash:truncated_06_08_13_vs_replacements:4
- noncandidate_blank_id_rows:photo-excel-admin_1781976368641.xlsx:1
- noncandidate_invalid_date_rows:photo-excel-admin_1781976368641.xlsx:1

## Decision recomendada

Proceed to a local dry-run loader design with the documented overlap warnings.

## Siguiente fase

`014E_KPIONE_RAW_DRY_RUN_LOADER_NO_APPLY`

## Declaracion no-apply

No Supabase, no DB, no SQL/DDL apply, no productive loader, no refresh, no UX
modification, no backfill, no cutover and no data movement were used. Baseline
014C and every Excel input remained read-only.
