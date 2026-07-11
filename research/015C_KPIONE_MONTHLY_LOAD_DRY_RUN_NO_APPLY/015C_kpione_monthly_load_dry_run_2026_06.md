# 015C KPIONE monthly load dry-run no apply

## Verdict

- Verdict: `WARN`
- Operational month: `2026-06`
- Validation mode: `close`
- As-of date: `2026-07-11`

## Guardrails

- DB access: `False`
- Supabase access: `False`
- SQL apply: `False`
- DDL: `False`
- Data movement: `False`
- Payload rows persisted: `False`

## Authorities

- 015B validation: `research/015B_KPIONE_MONTHLY_INPUT_VALIDATOR_NO_APPLY/015B_kpione_monthly_input_validation_2026_06.json`
- 015A manifest: `research/015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY/015_monthly_input_layout_manifest_2026_06.json`
- Grain contract: `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- Operational calendar contract: `contracts/control_gestion/operational_calendar_contract_v1.json`
- Dedupe authority: `014C/014E historical dry-run`

## Selected and excluded files

- include_candidate: `9`
- compare_only: `1`
- quarantine_truncation: `1`
- unclassified: `0`

## 014E baseline versus 015C result

- 014E source rows: `239159`
- 014E would stage calendar rows: `229070`
- 014E exact duplicates removed: `10089`
- 015C would stage operational rows: `213181`
- 015C carry-forward out rows: `15889`

## Row accounting

| metric | value |
| --- | --- |
| carry_backfill_out_rows | 0 |
| carry_forward_out_rows | 15889 |
| distinct_event_ids_would_stage | 32798 |
| exact_duplicate_rows_detected | 20178 |
| exact_duplicate_rows_removed | 10089 |
| invalid_date_rows_eligible | 0 |
| normalized_candidate_rows | 239159 |
| source_rows_selected_files | 239159 |
| survivor_rows_after_dedupe | 229070 |
| would_stage_rows | 213181 |

## Operational partition

- Target month rows: `213181`
- Target dates: `2026-06-01..2026-06-28`
- Target weeks: `2026-06-01, 2026-06-08, 2026-06-15, 2026-06-22`
- Carry-forward out: `15889`
- Carry-backfill out: `0`

## Dedupe

- Key: `['event_id', 'photo_row_hash']`
- Sort before keep: `['source_file_id', 'source_row_number']`
- Keep: `first`
- Rows participating in exact duplicate groups: `20178`
- Exact duplicate rows removed: `10089`
- Distinct event IDs with same hash fingerprint across files: `1579`
- Distinct event IDs with different hash fingerprints across files: `0`
- Distinct event IDs with stable-hash conflicts: `0`

## Duplicate metric definitions

| Metric | Unit | Exact meaning |
| --- | --- | --- |
| exact_duplicate_rows_detected | rows | All rows participating in exact duplicate groups identified by event_id plus photo_row_hash, including both retained and removed rows. Universe: normalized_candidate_rows. |
| exact_duplicate_rows_removed | rows | Rows removed after sorting by source_file_id and source_row_number and keeping the first row for each event_id plus photo_row_hash key. Universe: normalized_candidate_rows. |
| same_id_same_hash_count | distinct_event_ids | Distinct event IDs present in more than one source file whose per-file photo-row-hash fingerprint is identical. Universe: event_ids_present_in_multiple_source_files. |
| same_id_diff_hash_count | distinct_event_ids | Distinct event IDs present in more than one source file whose per-file photo-row-hash fingerprint differs. Universe: event_ids_present_in_multiple_source_files. |
| event_stable_hash_conflict_count | distinct_event_ids | Distinct event IDs associated with more than one event_stable_hash. Universe: normalized_candidate_rows. |
| cross_file_exact_photo_row_count | distinct_duplicate_keys | Distinct event_id plus photo_row_hash keys present in more than one source file. Universe: normalized_candidate_rows. |

## Batch plan

- Mode: `logical_batch_only`
- Batch count: `1`
- Batch row count: `213181`
- Deterministic payload sha256: `1f99a2b3a34fc7f721a0ea5d5836e340def688516edd871f2eeb710d496b34e4`

## Hashes

- load_plan_sha256: `e634b4720b66db7a3036302e2553c53d7a93475257bb27410804f6b9efe8f7cd`
- dry_run_batch_id: `015C_e634b4720b66db7a3036302e`

## Blockers

- None

## Warnings

- `exact_duplicate_rows_removed:10089`
- `excluded_invalid_date_rows:photo-excel-admin_1781976368641.xlsx:1`
- `input_validation_verdict_warn`
- `non_candidate_files_excluded:2`
- `photo_invalid_date_rows:photo-excel-admin_1781976368641.xlsx:1`
- `same_id_same_hash:1579`

## No-apply declaration

This artifact is an aggregate local dry-run. No DB connection, SQL apply, productive loader run, data movement, or row-level payload export was performed.
