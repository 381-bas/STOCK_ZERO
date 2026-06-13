# CODEX CG005B Route Replacement Contract

Phase: `FASE_CG005B_ROUTE_WEEK_REPLACEMENT_CONTRACT_NO_APPLY`

Status: `NEEDS_BASTIAN_DECISION`

This is a design contract only. It does not authorize implementation, loader execution, SQL execution, DDL, Supabase writes, refreshes, commits beyond this research artifact, or production apply.

## Executive Decision

The current route loader cannot safely apply `ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1` as-is. The unsafe behavior is technical and already demonstrated by CG005A:

- workbook rows: `3542`;
- current `public.ruta_rutero` rows: `3568`;
- stale `source_row` rows that would survive plain upsert: `26`;
- same `source_row` mapped to a different `COD_RT + CLIENTE` grain: `3342`;
- same grain moved to a different `source_row`: `3237`;
- public loader upsert key: `source + source_row`;
- effective week is inferred from `loaded_at`.

The selected technical strategy is `OPTION_E_HYBRID`: immutable batch history plus full replacement of the current public route surface plus explicit week-batch assignment.

The implementation contract is technically ready after Bastian decides four business semantics: non-exact duplicate groups, exact duplicate treatment, frequency versus day authority, and the 70 bajas.

## Evidence Used

Local workbook:

- `data/DB_GLOBAL_INVENTARIO.xlsx`, sheet `RUTA_RUTERO`;
- SHA-256 `454F3CF414B031FB793CA6298CF03638C2EDE3B53B918E6ACAB636C9B4B6AD83`;
- rows `3542`;
- schema signature `832a275e3a2d24e9c1b0f5b38d2083e94bc91e0165e7f4e6a0ba20c27f58f232`;
- source-check `warn`, blockers `0`;
- extra columns `EQUIPO`, `GGG-VT`, `MOD_NUM`, classified `IGNORED_SAFE`.

DB read-only:

- command: `scripts/cg_readonly_extract.py route-preflight`;
- role `stock_zero_codex_ro`;
- transaction read-only `on`;
- writes attempted `false`;
- DSN printed `false`;
- route batches `19`;
- latest batch `19`;
- latest batch rows `3568`;
- current public rows `3568`;
- resolved target week `2026-06-08` rows `3535`;
- resolved target week duplicate logical grains `0`.

Static repo evidence:

- `scripts/load_ruta_rutero_from_excel.py` writes `public.ruta_rutero`, `cg_core.ruta_rutero_load_rows`, and `cg_core.ruta_rutero_load_batch`.
- The public current surface uses `ON CONFLICT (source, source_row)`.
- `source_row` is `IdRow` if present, otherwise file row index plus two.
- `effective_week_start` is inferred from `loaded_at` in `America/Santiago`.
- `scripts/cg_canonical_build_local.py` already models explicit `week_start` and `source_ruta_batch_ids` through `route_source_plan`.
- `scripts/refresh_control_gestion_v2_incremental.py` reads `cg_core.v_rr_frecuencia_base_resuelta_v2` and carries both weekly count and day plan fields.
- `app/db.py` uses `public.ruta_rutero` through `RUTA_TABLE` for inventory/client selectors and uses CG v2 views for Control Gestion.

## A. Duplicate Classification

The logical compliance grain is `COD_RT + CLIENTE`.

Observed duplicate groups:

| Metric | Count |
| --- | ---: |
| Duplicate groups | 34 |
| Duplicate excess rows | 34 |
| Exact duplicate excess | 3 |
| Non-exact groups | 31 |
| Unexplained groups | 0 |

Class counts are overlapping because one group can differ by more than one dimension:

| Class | Groups |
| --- | ---: |
| `EXACT_DUPLICATE` | 3 |
| `DIFFERENT_ROUTE_PERSON` | 31 |
| `DIFFERENT_MODALITY` | 30 |
| `DIFFERENT_FREQUENCY` | 19 |
| `DIFFERENT_DAY_PATTERN` | 26 |
| `DIFFERENT_LOCAL_METADATA` | 0 |
| `MULTIPLE_OPERATIONAL_ROWS` | 31 |
| `UNEXPLAINED` | 0 |

Answers:

- `COD_RT + CLIENTE` is the logical compliance grain for resolved weekly Control Gestion.
- More than one physical row can plausibly exist in the workbook as operational detail, but the resolved weekly surface must collapse deterministically to one logical record per `effective_week_start + COD_RT + CLIENTE` unless Bastian approves multirow semantics.
- The 3 exact duplicate excess rows can be excluded from current/resolved surfaces without changing business fields, but they should still be preserved in immutable batch history unless Bastian says otherwise.
- The 31 non-exact groups are distinguished by route/person fields, `MODALIDAD`, day pattern, and `VECES POR SEMANA`.
- A stable physical key is not required for replacement semantics. Batch id plus `source_row` is enough for audit. Replacement must be snapshot-scoped, not `source_row`-upsert-scoped.

## B. Frequency And Days

Observed mismatches between `VECES POR SEMANA` and sum of `LUNES..DOMINGO`: `1309`.

Buckets:

| Bucket | Rows |
| --- | ---: |
| `frequency_greater_than_days` | 44 |
| `frequency_less_than_days` | 1265 |
| `zero_frequency_with_days` | 0 |
| `positive_frequency_without_days` | 1 |
| `non_binary_day_flags` | 0 |
| `monthly_or_exception_pattern` | 1309 |
| `unexplained` | 0 |

Current mart behavior:

- weekly count authority is `visitas_exigidas_semana`, sourced from `VECES POR SEMANA`;
- day plan fields are carried as `LUNES_PLAN..DOMINGO_PLAN` and compared with actual day flags.

Canonical builder behavior:

- route snapshot carries `frecuencia` from `veces_por_semana`;
- route snapshot also carries `lunes..domingo` as day-plan fields.

Recommendation:

- Use `VECES POR SEMANA` as weekly obligation count.
- Use `LUNES..DOMINGO` as day distribution/plan.
- Treat mismatches as warning/contract exception, not as a load blocker, because both current mart and canonical builder preserve both surfaces.

## C. Load Strategy Alternatives

`OPTION_A_ROW_UPSERT`: rejected.

It is low complexity and low code impact, but it cannot represent full replacement. CG005A demonstrated stale rows and massive `source_row` identity drift.

`OPTION_B_BUSINESS_KEY_UPSERT`: rejected for now.

It aligns partly with the compliance grain, but the workbook has 34 duplicate `COD_RT + CLIENTE` groups. Upserting by business key would collapse or overwrite physical operational rows before Bastian decides semantics.

`OPTION_C_FULL_SOURCE_REPLACEMENT`: valid but incomplete.

It solves stale public rows and reordering risk, but it does not by itself pin the effective week or preserve the route lineage needed by Control Gestion.

`OPTION_D_IMMUTABLE_BATCH_ONLY`: valid long-term but not minimal.

It is strong for audit and weekly route lineage, but current runtime and cliente/inventory surfaces still consume `public.ruta_rutero`.

`OPTION_E_HYBRID`: selected.

This combines:

- immutable `cg_core.ruta_rutero_load_batch`;
- immutable `cg_core.ruta_rutero_load_rows`;
- full replacement of `public.ruta_rutero` for the current `source`;
- explicit `effective_week_start -> ruta_batch_id` assignment.

This is the best fit for compatibility, rollback, auditability, and weekly route policy.

## D. Explicit Week Strategy

Continuing `loaded_at` inference is rejected. It can assign the wrong week if the load runs later, and it cannot express retroactive replacement.

A manifest-only strategy is also rejected as sole authority because DB views and refresh flows need DB-visible truth.

Adding `effective_week_start` to `ruta_rutero_load_batch` is useful but too rigid by itself. It does not model reassignments and rollback as clearly as a separate assignment.

Selected minimum strategy: create a week-batch assignment surface in a future implementation phase.

Minimum conceptual columns:

- `effective_week_start`;
- `route_policy_version`;
- `ruta_batch_id`;
- `assignment_status`;
- `input_file_name`;
- `input_file_sha256`;
- `schema_signature`;
- `resolved_surface_hash`;
- `assigned_at`;
- `assigned_by`;
- `replaces_ruta_batch_id`;
- `rollback_of_assignment_id`;
- `notes`.

`v_ruta_rutero_load_batch_week_v2` or a successor should prefer explicit assignments. `loaded_at` may remain only as a visible legacy/backfill fallback.

## E. Transaction Replacement Contract

Default mode must be no-write.

Pseudoflow for a future authorized implementation:

1. Run source-check and block on critical missing columns.
2. Compute workbook SHA-256 and schema signature before DB writes.
3. Normalize workbook into staging and classify exact/grain duplicates.
4. Require `--effective-week-start 2026-06-08`; validate Monday and week range `2026-06-08..2026-06-14`.
5. Require explicit confirmation token `ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1`.
6. `BEGIN`.
7. Create route batch metadata in pending/cancelled-safe state.
8. Insert immutable history rows for the batch.
9. Validate history row count, row hashes, source row uniqueness, and accepted row count.
10. Replace `public.ruta_rutero` current source slice from staging/history. Delete only current rows for the source, never history.
11. Write or activate explicit week-batch assignment.
12. Run resolved-view postcheck for target week.
13. Finalize batch status `ok`.
14. `COMMIT`.
15. Emit JSON with batch id, assignment id, hashes, validation status, rollback metadata, and observation draft.

Rollback before commit:

- any source-check blocker;
- hash mismatch;
- invalid/missing effective week;
- unexplained duplicate groups;
- Bastian decision required but missing;
- history count/hash mismatch;
- current replacement count/hash mismatch;
- assignment failure;
- resolved-view postcheck failure;
- any DB exception.

What may be deleted:

- only `public.ruta_rutero` current rows for the source being replaced, inside the transaction.

What must never be deleted by this flow:

- `cg_core.ruta_rutero_load_rows`;
- `cg_core.ruta_rutero_load_batch`;
- prior assignment records;
- raw CG tables;
- mart facts/materialized views outside a separately authorized refresh phase.

Postcommit rollback:

1. Read previous active assignment.
2. Create rollback metadata referencing the failed assignment.
3. Replace `public.ruta_rutero` from previous approved batch/history or an apply-time backup.
4. Reactivate previous assignment.
5. Mark failed assignment superseded/rolled back.
6. Run the same resolved-view postcheck.

## F. Proposed CLI

Proposed shape:

```powershell
python scripts/load_ruta_rutero_from_excel.py `
  --excel data/DB_GLOBAL_INVENTARIO.xlsx `
  --sheet RUTA_RUTERO `
  --source DB_GLOBAL_INVENTARIO.xlsx:RUTA_RUTERO `
  --effective-week-start YYYY-MM-DD `
  --dry-run `
  --confirm-weekly-replacement ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1 `
  --json-out <path>
```

Modes:

- `--source-check-only`: no write, validates workbook shape.
- `--dry-run`: default, no write, computes planned replacement and validations.
- `--apply`: write path, not authorized by this contract.

Required for future `--apply`:

- `--effective-week-start`;
- `--apply`;
- `--confirm-weekly-replacement ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1`;
- `--expected-workbook-sha256`;
- `--json-out`.

Minimum JSON output:

- mode;
- source;
- effective week;
- input hash;
- schema signature;
- input/accepted/rejected rows;
- duplicate metrics;
- planned public delete/insert counts;
- batch id;
- assignment id;
- post-load validation;
- rollback metadata;
- `dsn_printed=false`.

## G. Impact

| Consumer | Classification | Impact |
| --- | --- | --- |
| `app/db.py` via `RUTA_TABLE` / `public.ruta_rutero` | compatible if hybrid keeps public current surface | No immediate runtime change if public current surface is fully replaced per source. |
| `sql/03_cliente_scope_materialized_views.sql` | compatible if public current surface replaced | Refresh not authorized here. |
| `scripts/refresh_control_gestion_v2_incremental.py` | requires DB view contract change | Script can stay stable if `v_rr_frecuencia_base_resuelta_v2` columns remain compatible. |
| `scripts/cg_canonical_build_local.py` | compatible | Already uses explicit `week_start` and `source_ruta_batch_ids`. |
| `scripts/cg_readonly_extract.py route-preflight` | compatible with future extension | Can later report assignment-table metrics. |
| `cg_core.v_ruta_rutero_load_batch_week_v2` | requires change | Must prefer explicit assignment over `loaded_at`. |
| `cg_core.v_rr_frecuencia_base_resuelta_v2` | requires change | Must resolve from explicit assignment for forward loads. |
| SQL drafts `04/05/08/09` | legacy/draft | Evidence only; not runtime authority. |

## H. Bastian Decisions

`BD-CG005B-001`: decide whether the 31 non-exact duplicate `COD_RT + CLIENTE` groups are legitimate multiple operational rows or workbook defects.

Technical default: preserve in history and block automatic logical collapse until approved.

`BD-CG005B-002`: decide whether the 3 exact duplicate rows may be excluded from current/resolved surfaces while preserved in immutable history.

Technical default: exact duplicate dedupe is safe for current/resolved surfaces after approval.

`BD-CG005B-003`: decide business authority when `VECES POR SEMANA` differs from the sum of `LUNES..DOMINGO`.

Technical default: `VECES POR SEMANA` is weekly count authority; `LUNES..DOMINGO` is day-plan authority; mismatch is a warning.

`BD-CG005B-004`: decide whether the 70 bajas versus current surface are expected removals for week `2026-06-08`.

Technical default: treat bajas as expected full-snapshot removals only after approval.

## Final Contract State

`NEEDS_BASTIAN_DECISION`

There are no remaining technical blockers in the contract design. Implementation should not begin until the four Bastian decisions are resolved in a follow-up authorization phase.
