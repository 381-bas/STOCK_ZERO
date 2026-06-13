# CODEX C007 Canonical Builder Validation

Phase: `FASE_C007_CANONICAL_BUILDER_PACKAGING_AND_SEMANTIC_PARITY_NO_SUPABASE_WRITE`

Verdict: `SHADOW_READY`

Supabase writes: none. Productive runtime files modified: none. Local writes were limited to PostgreSQL schemas `c007_canonical` and `c007_clean_room`, plus the four authorized C007 repository files.

## Scope

C007 packages the C006 local forward contract as reusable tooling and validates semantic readiness before any forward deployment or Supabase cleanup is considered.

Files written by this phase:

- `scripts/cg_canonical_build_local.py`
- `tests/test_cg_canonical_build_local.py`
- `research/C007_CANONICAL_BUILDER_VALIDATION.json`
- `research/reports/CODEX_C007_CANONICAL_BUILDER_VALIDATION.md`

No loaders, product refresh flows, Supabase writes, runtime app files, productive SQL, kernels, AI memory, data, evidence, legacy facts, cleanup, or retention actions were modified.

## Builder Contract

The builder accepts a build context JSON and writes only to a local PostgreSQL schema. It rejects DSNs containing Supabase/pooler hosts or non-local hosts, and it does not contain credentials.

Core protections:

| Protection | Status |
| --- | --- |
| Build context JSON accepted | yes |
| `latest/current/implicit/auto winner` tokens blocked in extracted context | yes |
| Raw batch registry rows validated | yes |
| Raw snapshot hashes validated | yes |
| Route snapshot constructed from declared route batches | yes |
| Daily output constructed | yes |
| Weekly output constructed | yes |
| Loaders executed | no |
| Product refresh executed | no |
| Supabase writes | no |

Hash contract:

| Hash | Daily columns | Weekly columns |
| --- | --- | --- |
| `key_hash` | `fecha_visita`, `cod_rt`, `cliente_norm` | `SEMANA_INICIO`, `COD_RT`, `CLIENTE_NORM_FILTER` |
| `business_semantic_hash` | all daily output columns except `run_no`, `match_quality`, `registro_fuera_cruce` | all weekly output columns except `run_no` and derived filter/key mirror columns |
| `technical_full_row_hash` | all daily output columns except `run_no` | all weekly output columns except `run_no` |

Algorithm: `md5(string_agg(md5(concat_ws(chr(31), normalized_columns)), '' order by key_columns))`.

## Raw Lineage

| Source | Batch | Loaded rows | Selected window rows before route match | Represented raw rows | Snapshot hash valid |
| --- | ---: | ---: | ---: | ---: | --- |
| `KPIONE` | 19 | 75124 | 0 | 0 | yes |
| `KPIONE2` | 38 | 45736 | 14289 | 7579 | yes |
| `POWER_APP` | 26 | 5497 | 0 | 0 | yes |

`KPIONE2` has 14289 positive raw rows in the selected date window before route matching. The canonical daily output represents 7579 raw evidence rows after applying the declared route surface and day/key aggregation.

## Route Lineage

| Week | Rows | Route batches | C007 surface hash | C006 reference hash |
| --- | ---: | --- | --- | --- |
| `2026-05-18` | 3318 | `{13}` | `a106c7d04b614730108ac6b00710ebdf` | `d77324afd998852dd636092ff2bf538f` |
| `2026-06-01` | 3481 | `{15,18}` | `db089d4a3a830a4d7280fbac67ae3afb` | `3cb74f4e4c830d0cd953dbb5dd8008ed` |

C007 defines its own route row hash contract, so these surface hashes differ from C006's reference hashes. This is metadata drift only: the C007 daily and weekly outputs compare exactly against C006.

## Reexecution

`c007_canonical` was built three times. `c007_clean_room` was built once from the same context.

| Output | Rows | Duplicate keys | Key hash | Business hash | Technical hash |
| --- | ---: | ---: | --- | --- | --- |
| Daily | 7484 | 0 | `d3047ebd1212494648d2dc67988246e5` | `4837c0ab073d765f177641355178a51f` | `a6a49057ece44629fe6c26987761524a` |
| Weekly | 6799 | 0 | `dc9e0538b4ca27f772f2fa3c5467758e` | `4569d8b74285a073c2f152491657a9c2` | `fcb38f928c2fdda2c0ee8ee23496071c` |

All three canonical runs produced one variant for rows, key hash, business semantic hash, and technical full row hash. Clean-room output matched `c007_canonical` exactly.

C006 output comparison:

| Comparison | Difference count |
| --- | ---: |
| C007 daily minus C006 daily | 0 |
| C006 daily minus C007 daily | 0 |
| C007 weekly minus C006 weekly | 0 |
| C006 weekly minus C007 weekly | 0 |

## Semantic Parity

Comparison surface: `c007_canonical.daily_canonical` run 1 versus `c002_b0.baseline_daily` for weeks `2026-05-18` and `2026-06-01`.

| Metric | Count |
| --- | ---: |
| Common keys | 7484 |
| Missing from canonical | 0 |
| Extra in canonical | 0 |
| Technical-only keys | 0 |
| NULL/empty-only keys | 0 |
| Normalization-only keys | 7484 |
| Business semantic diff keys | 1211 |
| Business semantic diff cells | 3599 |

Classified differences:

| Class | Keys | Cells | Operational impact |
| --- | ---: | ---: | --- |
| `TECHNICAL_ONLY` | 0 | 0 | none |
| `NULL_EMPTY_ONLY` | 0 | 0 | none after normalization |
| `NORMALIZATION` | 7484 | 14968 | not counted as business semantic; no visit/source/route assignment/alert impact |
| `ROUTE_PERSON` | 1188 | 3511 | ownership/display/filter attribution differs from legacy |
| `SOURCE_METRIC` | 23 | 23 | KPIONE2 audit/source count differs |
| `VISIT_METRIC` | 23 | 65 | audit/multimark/person-conflict counters differ |
| `ALERT` | 0 | 0 | none |
| `OTHER` | 0 | 0 | none |

Column detail:

| Column | Class | Keys | Cells |
| --- | --- | ---: | ---: |
| `match_quality` | `NORMALIZATION` | 7484 | 7484 |
| `registro_fuera_cruce` | `NORMALIZATION` | 7484 | 7484 |
| `gestor` | `ROUTE_PERSON` | 1182 | 1182 |
| `gestor_norm` | `ROUTE_PERSON` | 1182 | 1182 |
| `supervisor` | `ROUTE_PERSON` | 707 | 707 |
| `jefe_operaciones` | `ROUTE_PERSON` | 433 | 433 |
| `local_nombre` | `ROUTE_PERSON` | 7 | 7 |
| `kpione2_rows_dia` | `SOURCE_METRIC` | 23 | 23 |
| `raw_evidence_count` | `VISIT_METRIC` | 23 | 23 |
| `same_source_multimark` | `VISIT_METRIC` | 19 | 19 |
| `persona_conflicto_rows_dia` | `VISIT_METRIC` | 23 | 23 |

Interpretation:

- The `NORMALIZATION` differences are not operational business differences.
- The `ROUTE_PERSON` differences are expected under the forward route snapshot and are covered by the C005/C006 finding that legacy route/person provenance is not exactly reproducible from preserved history.
- The `SOURCE_METRIC` and `VISIT_METRIC` differences are the already-validated raw snapshot drift: KPIONE2 batch 33 matched legacy while batches 34 through 38 match the canonical local count.
- No `ALERT` differences were found.

The 7484 raw value differences are therefore not blindly accepted: they split into normalization noise, explained route/person lineage, and explained source/visit audit drift.

## Source Coverage

The new unittest fixture layer covers eight operational scenarios:

| Scenario | Status |
| --- | --- |
| `KPIONE2_ONLY` | pass |
| `POWER_APP_FALLBACK` | pass |
| `KPIONE2_POWER_APP_OVERLAP` | pass |
| `KPIONE_AUDIT_ONLY` | pass |
| `DOUBLE_MARK` | pass |
| `TRIPLE_SOURCE_OVERLAP` | pass |
| `EVIDENCE_OUTSIDE_ROUTE` | pass |
| `ROUTE_WITHOUT_EVIDENCE` | pass |

## Tests

Command:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

Result:

| Metric | Count |
| --- | ---: |
| Collected | 37 |
| Passed | 36 |
| Failed | 0 |
| Skipped | 1 |

The skipped test is the existing Windows symlink test from the read-only extractor suite; it requires a privilege unavailable in this session.

## Warnings

- Preflight warnings were non-blocking: `git_worktree_not_clean` and `kernel_02_head_mismatch`.
- C007 route surface hashes differ from C006 route surface hashes because C007 formalizes a new route row hash definition; C006 daily and weekly outputs still compare exact.
- One existing Windows symlink test was skipped due missing privilege.

## Final Assessment

`SHADOW_READY`: the canonical builder is persistent, local-only, tested, deterministic across repeated runs, exact in clean-room, exact against C006 daily/weekly outputs, and the legacy semantic differences are classified and explained without authorizing Supabase cleanup or rewriting historical facts.
