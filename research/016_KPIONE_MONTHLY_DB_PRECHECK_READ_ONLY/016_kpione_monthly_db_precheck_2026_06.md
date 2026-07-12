# 016 KPIONE monthly DB precheck read-only

- Verdict: `WARN`
- Apply gate: `BLOCKED_FOR_IDEMPOTENCY_CONTRACT`
- Coarse overlap: `NO_SOURCE_SIGNAL`
- Exact overlap: `UNVERIFIABLE`
- Target: `cg_raw.kpione2_raw`

## Roadmap compliance

- roadmap_lock_id: `KPIONE_DB_TRANSITION_016_019_LOCK_V1`
- roadmap_lock_sha256: `cce9eea337c07c56b722968beaa7eb481e79028b60fa218e20875fb71be2e46e`
- roadmap_lock_commit: `5c0aa19ac753c21aa9bb43b6fdd72a927b694a5f`
- current_phase: `016`
- expected_next_phase: `016A`
- roadmap_compliance: `COMPLIANT`
- deviations_detected: `[]`

## Guardrails

- DB access used: `True`
- transaction_read_only: `True`
- default_transaction_read_only: `True`
- write privileges present: `False`
- rollback completed: `True`
- writes attempted: `False`

## Load plan

- load_plan_sha256: `e634b4720b66db7a3036302e2553c53d7a93475257bb27410804f6b9efe8f7cd`
- would_stage_rows: `213181`
- carry_forward_out_rows: `15889`

## Database state

- approximate_total_rows: `45736`
- exact_total_rows: `45736`
- fecha_min: `2026-04-07`
- fecha_max: `2026-06-01`
- batch_ids: `["38"]`
- range_summary: `{"rows_july": 0, "rows_june_carry_forward_window": 0, "rows_june_operational": 1221}`

## Historical state discrepancy

- target: `cg_raw.kpione2_raw`
- historical_row_count: `526022`
- historical_batch_count: `19`
- latest_historical_batch_id: `38`
- latest_historical_batch_loaded_rows: `45736`
- current_exact_row_count: `45736`
- current_batch_ids: `["38"]`
- current_fecha_min: `2026-04-07`
- current_fecha_max: `2026-06-01`
- classification: `TABLE_STATE_CHANGED`
- resolution_status: `DOCUMENTED_NOT_RECONSTRUCTED`
- legacy_migration_authority: `False`
- blocker_for_new_design: `False`
- warning_condition: `True`

## Source signal interpretation

- policy: `NO_SOURCE_SIGNAL DOES NOT IMPLY FRESH.`
- coarse_classification: `NO_SOURCE_SIGNAL`
- proves_freshness: `False`
- proves_no_overlap: `False`
- exact_identity_available: `False`
- exact_overlap_classification: `UNVERIFIABLE`
- apply_gate: `BLOCKED_FOR_IDEMPOTENCY_CONTRACT`
- explanation: Absence of legacy source-file matches or dry_run_batch_id does not prove that equivalent rows are absent.

## Identity feasibility

- exact_comparison_potentially_feasible: `False`
- operationally_unsafe_reason: `legacy_source_file_source_row_requires_manifest_mapping`

## Blockers

- None

## Warnings

- `exact_duplicate_rows_removed:10089`
- `excluded_invalid_date_rows:photo-excel-admin_1781976368641.xlsx:1`
- `input_validation_verdict_warn`
- `non_candidate_files_excluded:2`
- `photo_invalid_date_rows:photo-excel-admin_1781976368641.xlsx:1`
- `same_id_same_hash:1579`
- `historical_state_discrepancy:TABLE_STATE_CHANGED`

## Query audit

- `target_exists` rows=`1` duration_ms=`178` error=`None`
- `target_privileges` rows=`1` duration_ms=`167` error=`None`
- `target_columns` rows=`34` duration_ms=`163` error=`None`
- `target_constraints` rows=`3` duration_ms=`172` error=`None`
- `target_indexes` rows=`6` duration_ms=`220` error=`None`
- `target_triggers` rows=`0` duration_ms=`272` error=`None`
- `target_owner_rls_size` rows=`1` duration_ms=`316` error=`None`
- `target_policies` rows=`0` duration_ms=`309` error=`None`
- `target_dependencies` rows=`1` duration_ms=`321` error=`None`
- `state_total_minmax` rows=`1` duration_ms=`359` error=`None`
- `state_date_counts` rows=`1` duration_ms=`266` error=`None`
- `state_range_summary` rows=`1` duration_ms=`297` error=`None`
- `state_week_counts` rows=`1` duration_ms=`269` error=`None`
- `state_null_counts` rows=`1` duration_ms=`253` error=`None`
- `json_identity_key_presence` rows=`1` duration_ms=`278` error=`None`
- `source_file_scope_counts` rows=`0` duration_ms=`238` error=`None`
- `batch_scope_counts` rows=`0` duration_ms=`228` error=`None`
- `recent_batch_counts` rows=`1` duration_ms=`229` error=`None`
- `state_batch_ids` rows=`1` duration_ms=`243` error=`None`

Evidence is aggregate only. No payload_json rows, URLs, secrets, or row-level DB exports are included.
