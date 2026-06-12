# CODEX C005 Build Provenance Audit

Phase: `FASE_C005_BUILD_PROVENANCE_AND_RESIDUAL_PARITY_AUDIT_NO_SUPABASE_WRITE`

Verdict: `PARTIAL`

Supabase writes: none. Productive files modified: none.

## Scope

This was a forensic phase. Codex used the existing local PostgreSQL parity container and read-only Supabase checks only through the allowlisted role. No loaders, product refreshes, SQL migrations, retention, cleanup, kernels, evidence, runtime code, AI memory, or backlog files were modified.

The only repository files written by this phase are:

- `research/C005_BUILD_PROVENANCE_AUDIT.json`
- `research/reports/CODEX_C005_BUILD_PROVENANCE_AUDIT.md`

## Baseline Provenance

Daily baseline relation: `cg_mart.fact_cg_visita_dia_resuelta_v2`.

The daily baseline has 21,348 rows for `2026-05-11` through `2026-06-01`. It is not a single homogeneous rebuild: `mart_loaded_at` spans seven distinct materialization timestamps.

| mart_loaded_at UTC | Rows | Date range |
| --- | ---: | --- |
| 2026-05-19T15:10:13.668515Z | 7327 | 2026-05-11..2026-05-17 |
| 2026-05-19T17:18:15.738586Z | 1291 | 2026-05-18 |
| 2026-05-20T18:06:57.777227Z | 1130 | 2026-05-19 |
| 2026-05-24T19:57:42.670246Z | 1706 | 2026-05-20..2026-05-21 |
| 2026-05-25T13:30:02.901957Z | 2137 | 2026-05-22..2026-05-24 |
| 2026-05-26T15:05:05.957508Z | 1172 | 2026-05-25 |
| 2026-06-02T20:22:46.239731Z | 6585 | 2026-05-26..2026-06-01 |

Weekly baseline relation: `cg_mart.fact_cg_out_weekly_v2`.

The weekly export has 20,138 rows and no exported `mart_loaded_at` column, so its exact materialization timestamp cannot be proven from the 9C7A4 evidence alone.

| Week | Rows |
| --- | ---: |
| 2026-04-27 | 3364 |
| 2026-05-04 | 3329 |
| 2026-05-11 | 3328 |
| 2026-05-18 | 3318 |
| 2026-05-25 | 3319 |
| 2026-06-01 | 3480 |

Available commits around the first daily materializations include `750b17b`, `dcc4adf`, `919b5e5`, `7c34450`, and `b288958`. The first daily load occurred after `7c34450` and before `b288958` in local time; later loads occurred after additional refresh/loader changes. This supports a mixed historical build context rather than a single current-HEAD rebuild.

## Provenance Matrix

| Component | Baseline version | Current version | Assessment | Impact |
| --- | --- | --- | --- | --- |
| Daily precedence view | 9C7A4 DDL | Same local DDL observed | Same schema | Does not explain the 23 KPIONE2 counter diffs. |
| Daily fact builder | Seven materialization timestamps | HEAD helper and loader `v17_9C2A` | Different or mixed | Exact replay needs per-load build context. |
| Route resolved view | Per-grain composite winner | Same current view; C004 used narrower snapshot | C004 differs | Explains 71 missing weekly keys. |
| Weekly builder | `fact_cg_out_weekly_v2` export | `_cg_weekly_stage` from route + daily fact | Partially same logic | Failed because C004 route surface was wrong. |
| Source precedence | KPIONE2 > POWER_APP, KPIONE audit-only | Same | Same | All daily 56 are KPIONE2 -> KPIONE2. |
| Normalization | DDL trim/upper | Same plus local text casts | Mostly same | Only NULL/empty comparison noise found for fuentes. |
| `FUENTES_REPORTADAS_SEMANA` | NULL for no source | C004 staged empty string | NULL/empty drift | 4,048 direct diffs vanish after `COALESCE`. |
| `RUTA_DUPLICADA_FLAG` | Baseline route/fact materialization | C004 route snapshot | Different by route surface | Secondary symptom of route mismatch. |

## Daily 56

Best C004 variant audited: `after_missing_key`.

The 56 daily residual keys produce 176 differing cells. All 56 have source pair `KPIONE2 -> KPIONE2`; source precedence is not the cause.

| Class | Keys | Diff cells | Status |
| --- | ---: | ---: | --- |
| `ROUTE_PERSON_FIELDS` | 33 | 88 | Partial |
| `AUDIT_FIELDS` | 23 | 65 | Validated |
| `SOURCE_FIELDS` | 23 | 23 | Validated |

Top columns:

| Column | Keys |
| --- | ---: |
| `kpione2_rows_dia` | 23 |
| `local_nombre` | 23 |
| `persona_conflicto_rows_dia` | 23 |
| `raw_evidence_count` | 23 |
| `same_source_multimark` | 19 |
| `reponedor_scope` | 10 |
| `reponedor_scope_norm` | 10 |
| `rutero` | 10 |
| `gestor` | 9 |
| `gestor_norm` | 9 |
| `modalidad` | 9 |
| `supervisor` | 7 |
| `jefe_operaciones` | 1 |

By week:

| Week | Keys | Diff cells |
| --- | ---: | ---: |
| 2026-05-11 | 9 | 9 |
| 2026-05-18 | 29 | 94 |
| 2026-05-25 | 7 | 7 |
| 2026-06-01 | 11 | 66 |

For the 23 counter/source residual keys, the cause is validated:

- KPIONE2 raw batch 33 matches baseline counts for all 23 keys.
- KPIONE2 raw batches 34 through 38 match local counts for all 23 keys.
- Batch 34 adds exactly one raw row per affected key.

For the 33 route/person residual keys, the cause is only partial:

- Local values have exact matches in available route rows.
- Baseline values have no exact route-row match against available historical route batches.
- Best baseline route scores are incomplete, ranging from 2 to 9 matched fields out of 10.

This points to historical route/person materialization not fully reproducible from the currently available route rows, or to a missing build-context detail.

## Weekly 71

The 71 missing weekly keys are now explained as a C004 route-surface issue, not as route absence.

| Week | Missing keys | Cause |
| --- | ---: | --- |
| 2026-05-11 | 30 | Present in batches 8-11; current resolved view keeps batch 11 winners; C004 snapshot used only batch 12. |
| 2026-06-01 | 41 | Present in batch 15; current resolved view keeps batch 15 winners for that subset; C004 snapshot kept only 3 batch-15 overrides and otherwise used batch 17. |

All 71 keys:

- exist in historical route rows;
- exist in `cg_core.v_rr_frecuencia_base_resuelta_v2`;
- do not exist in `c004_g0.route_week_snapshot_resolved`;
- have `RUTA_DUPLICADA_FLAG=0`;
- have `GESTION_COMPARTIDA_FLAG_CALC=0`.

Exclusive class assigned: `HISTORICAL_ROUTE_VERSION`.

Rejected as primary causes:

- `CLIENT_NORMALIZATION`
- `LOCAL_CLIENT_REKEYING`
- `SHARED_MANAGEMENT_COLLAPSE`
- `ZERO_VISIT_ROUTE_ONLY`
- `ROUTE_KEY_DROPPED_BY_DEDUP`

## Fuentes Reportadas

The 4,048 `FUENTES_REPORTADAS_SEMANA` differences are not semantic.

| Classification | Rows |
| --- | ---: |
| `NULL_EMPTY` | 4048 |
| `SEMANTIC_SOURCE_SET` | 0 |
| `FORMAT_ONLY` | 0 |
| `ORDER_ONLY` | 0 |

Baseline stores `NULL`; C004 staged stores empty string. After `COALESCE(value, '')`, differences are zero.

This should be treated as comparison normalization or NULL/empty output drift, not as a source-set parity failure.

After normalizing `FUENTES_REPORTADAS_SEMANA`, weekly still has 166 common keys with semantic value differences, mostly route/person, route duplicate, plan, and visit columns. That remaining set depends on resolving the route surface, not on source-set logic.

## Route Provenance

Bastian's business policy remains closed:

`ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1`

C005 did not create a new route surface.

The technical baseline rule inferred from evidence is a per-grain composite resolved view, not a single batch per week:

- `v_rr_frecuencia_base_resuelta_v2` ranks by `effective_week_start`, `cod_rt`, `cliente_norm`, `loaded_at DESC`, and `ruta_batch_id DESC`.
- For 2026-05-11, current resolved route uses 30 rows from batch 11 and 3298 rows from batch 12.
- For 2026-06-01, current resolved route uses 44 rows from batch 15 and 3437 rows from batch 18.
- C004's experimental snapshot used only batch 12 for 2026-05-11, and batch 15/17 for 2026-06-01.

Therefore, C004's weekly staged failure is explained by an overly narrow route snapshot. The remaining daily route/person differences still require exact historical build context or additional route lineage evidence.

## Next Recipe

1. Pin raw source batch lineage by affected date. Include KPIONE2 batch 33 for the 23 daily counter residual keys and batch 31 for the previously recovered missing key.
2. Rebuild daily from pinned raw lineage and compare before route changes.
3. Use resolved per-grain route lineage from `v_rr_frecuencia_base_resuelta_v2` as the technical replay surface, recording `ruta_batch_id` per week, `cod_rt`, and `cliente_norm`.
4. Normalize NULL and empty string for `FUENTES_REPORTADAS_SEMANA` in parity comparison.
5. Rebuild weekly from pinned daily plus resolved route lineage, not from the narrowed C004 route snapshot.
6. Run clean-room reproducibility only after daily value parity and weekly key/value parity are exact.

## Final Assessment

`PARTIAL`: C005 explains the weekly 71 and the 4,048 fuentes diffs, and validates the root cause for 23 of the daily 56. The remaining 33 daily route/person keys are materially narrowed but not fully provenance-confirmed because available historical route rows do not exactly reproduce baseline values.
