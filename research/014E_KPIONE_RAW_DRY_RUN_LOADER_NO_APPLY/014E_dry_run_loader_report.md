# 014E KPIONE Raw Dry-Run Loader — No Apply

## Resumen ejecutivo

- Verdict: `DRY_RUN_READY_WITH_WARNINGS`
- Dry-run batch: `014E_1113973b41ee7618b4132006`
- Candidate files used: 9
- Source rows: 239159
- Would stage rows: 229070
- Distinct event IDs: 35287
- Coverage: 30/30 days

## Input manifest 014D

- Path: `research/014D_KPIONE_RAW_EXPORT_REMEDIATION_NO_APPLY/014D_remediation_manifest.json`
- SHA256: `b73483701f0dd767115a3d86d89fed2890b23da4d9269d959cb539e52d8ca089`
- Phase: `014D_KPIONE_RAW_EXPORT_REMEDIATION_NO_APPLY`
- Verdict: `REMEDIATION_READY_FOR_DRY_RUN_LOADER_WITH_WARNINGS`

## Candidate set usado

| Source ID | File | Rows | SHA match |
|---|---|---:|---|
| 1781975989376 | photo-excel-admin_1781975989376.xlsx | 46807 | true |
| 1783219885210 | photo-excel-admin_1783219885210.xlsx | 8372 | true |
| 1783220552913 | photo-excel-admin_1783220552913.xlsx | 34838 | true |
| 1783219914054 | photo-excel-admin_1783219914054.xlsx | 8672 | true |
| 1781976423312 | photo-excel-admin_1781976423312.xlsx | 107 | true |
| 1781973512473 | photo-excel-admin_1781973512473.xlsx | 46078 | true |
| 1782440454408 | photo-excel-admin_1782440454408.xlsx | 37908 | true |
| 1783220085725 | photo-excel-admin_1783220085725.xlsx | 40488 | true |
| 1783220157694 | photo-excel-admin_1783220157694.xlsx | 15889 | true |

## Archivos excluidos

| Source ID | File | Role | Reason |
|---|---|---|---|
| 1781976368641 | photo-excel-admin_1781976368641.xlsx | quarantine_truncation | manifest_role:quarantine_truncation |
| 1782012877303 | photo-excel-admin_1782012877303.xlsx | compare_only | manifest_role:compare_only |

## Integridad SHA256 / manifest

- Candidate entries consistent: true
- All files exist: true
- All SHA256 match: true
- All row counts match: true
- Candidate truncation count: 0

## Payload dry-run

- Grain: raw/event-photo
- Payload columns: event_id, source_file_id, source_file_name, source_file_sha256, source_row_number, fecha, week_start, cod_rt, local_nombre, cliente_norm, reponedor, tipo_tarea, n_fotos, link_foto, event_stable_hash, photo_row_hash, dry_run_batch_id
- Source rows total: 239159
- Exact duplicates removed: 10089
- Would stage rows: 229070
- Date range: 2026-06-01..2026-06-30
- Payload rows persisted: false
- Official compliance calculated: false

## Dedupe

- Candidate same_id_same_hash: 1579
- Candidate same_id_diff_hash: 0
- event_stable_hash conflicts: 0
- Same local/date/brand with different ID preserved: true

## Cobertura

- Covered days: 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-06, 2026-06-07, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-13, 2026-06-14, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-20, 2026-06-21, 2026-06-22, 2026-06-23, 2026-06-24, 2026-06-25, 2026-06-26, 2026-06-27, 2026-06-28, 2026-06-29, 2026-06-30
- Missing days: none
- S1-S4 complete: true
- Calendar June complete: true

## VISITA validation

- Derived validation only; not an official/productive load field.
- group_count: 34996
- visit_sum: 34996.000000
- groups where sum != 1: 0
- local fallback groups: 14

## Would-insert summary by week

| Week | Rows | Event IDs | Photo hashes | Derived VISITA |
|---|---:|---:|---:|---:|
| 2026-06-01 | 46807 | 7101 | 46807 | 7069.000000 |
| 2026-06-08 | 51989 | 8097 | 51989 | 8026.000000 |
| 2026-06-15 | 54726 | 8294 | 54726 | 8250.000000 |
| 2026-06-22 | 59659 | 9306 | 59659 | 9194.000000 |
| 2026-06-29 | 15889 | 2489 | 15889 | 2457.000000 |

## Would-insert summary by day

| Day | Rows | Event IDs | Derived VISITA |
|---|---:|---:|---:|
| 2026-06-01 | 8075 | 1224 | 1219.000000 |
| 2026-06-02 | 7555 | 1162 | 1155.000000 |
| 2026-06-03 | 7816 | 1208 | 1202.000000 |
| 2026-06-04 | 7430 | 1118 | 1109.000000 |
| 2026-06-05 | 8689 | 1331 | 1327.000000 |
| 2026-06-06 | 7162 | 1050 | 1049.000000 |
| 2026-06-07 | 80 | 8 | 8.000000 |
| 2026-06-08 | 8372 | 1295 | 1294.000000 |
| 2026-06-09 | 8031 | 1282 | 1275.000000 |
| 2026-06-10 | 8387 | 1362 | 1335.000000 |
| 2026-06-11 | 8527 | 1340 | 1320.000000 |
| 2026-06-12 | 9893 | 1493 | 1487.000000 |
| 2026-06-13 | 8672 | 1311 | 1305.000000 |
| 2026-06-14 | 107 | 14 | 10.000000 |
| 2026-06-15 | 9695 | 1476 | 1462.000000 |
| 2026-06-16 | 8511 | 1298 | 1292.000000 |
| 2026-06-17 | 9418 | 1442 | 1434.000000 |
| 2026-06-18 | 8516 | 1262 | 1257.000000 |
| 2026-06-19 | 9938 | 1519 | 1511.000000 |
| 2026-06-20 | 8550 | 1286 | 1283.000000 |
| 2026-06-21 | 98 | 11 | 11.000000 |
| 2026-06-22 | 9982 | 1556 | 1552.000000 |
| 2026-06-23 | 9189 | 1460 | 1453.000000 |
| 2026-06-24 | 10089 | 1579 | 1570.000000 |
| 2026-06-25 | 9605 | 1490 | 1461.000000 |
| 2026-06-26 | 11033 | 1711 | 1679.000000 |
| 2026-06-27 | 9660 | 1498 | 1468.000000 |
| 2026-06-28 | 101 | 12 | 11.000000 |
| 2026-06-29 | 6361 | 989 | 962.000000 |
| 2026-06-30 | 9528 | 1500 | 1495.000000 |

## Would-insert summary by cliente (first 20)

| Cliente | Rows | Event IDs | Derived VISITA |
|---|---:|---:|---:|
| ABEJA DORADA | 2386 | 489 | 489.000000 |
| ALUSWEET | 5985 | 940 | 929.000000 |
| ASMODEE | 4383 | 663 | 658.000000 |
| BERRYSUR | 4303 | 665 | 661.000000 |
| BESHOS | 6295 | 1433 | 1421.000000 |
| BIGU | 3071 | 691 | 689.000000 |
| BREDEN MASTER | 11056 | 880 | 847.000000 |
| BY MARIA | 2585 | 549 | 548.000000 |
| CALIFORNIA | 737 | 138 | 138.000000 |
| CALLAQUI | 2920 | 550 | 548.000000 |
| CASO Y CIA | 33692 | 3415 | 3377.000000 |
| CINNABON | 2049 | 481 | 470.000000 |
| CORRALES DEL SUR | 7788 | 522 | 520.000000 |
| CUK | 10924 | 1712 | 1683.000000 |
| DEJAPOO | 1812 | 440 | 439.000000 |
| DERAIZ | 2486 | 442 | 439.000000 |
| DUSOLEIL | 806 | 104 | 104.000000 |
| ECOCULTIVA | 2157 | 275 | 271.000000 |
| EL GAJO | 3083 | 632 | 630.000000 |
| EVERSKIN | 1328 | 333 | 333.000000 |

## Blockers

- none

## Warnings

- expected_exact_overlap_rows_removed:10089
- expected_same_id_same_hash:1579
- input_manifest_014D_has_documented_warnings
- manifest_non_candidate_files_excluded:2

## Decision recomendada

Proceed to DB staging design with exact-overlap warnings documented.

## Siguiente fase propuesta

`014F_KPIONE_RAW_DB_STAGING_DESIGN_NO_APPLY`

## Declaracion no-apply

No Supabase, no DB connection, no SQL/DDL apply, no productive loader, no
refresh, no UX modification and no data movement were used. The dry-run payload
was held in memory and only lightweight aggregate JSON/Markdown was written.
