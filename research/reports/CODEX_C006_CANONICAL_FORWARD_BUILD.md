# CODEX C006 Canonical Forward Build

Phase: `FASE_C006_CANONICAL_FORWARD_BUILD_CONTRACT_LOCAL_ONLY`

Verdict: `CANONICAL_BUILD_REPRODUCIBLE`

Legacy forensics status: `CLOSED_WITH_DOCUMENTED_NON_REPRODUCIBILITY`

Supabase writes: none. Productive files modified: none. Local writes were limited to Docker PostgreSQL schemas `c006_canonical` and `c006_clean_room`, plus this phase's two research artifacts.

## Scope

C006 validates a forward canonical Control Gestion build contract. It does not rewrite historical productive facts and does not turn legacy unreproducible route/person values into future rules.

The two repository files written by this phase are:

- `research/C006_CANONICAL_FORWARD_BUILD.json`
- `research/reports/CODEX_C006_CANONICAL_FORWARD_BUILD.md`

No loaders, product refresh flows, SQL migrations, cleanup, retention, runtime code, kernels, data, evidence, AI memory, or backlog files were modified.

## Safety Checks

Supabase access was checked through the read-only role only:

| Check | Value |
| --- | --- |
| Current user | `stock_zero_codex_ro` |
| Default transaction read-only | `on` |
| Statement timeout | `15s` |
| MV count sample | `13339` |
| ALERTA counts | `CUMPLE=6650`, `INCUMPLE=6689` |

The generic Control Gestion preflight completed with non-blocking warnings only:

- `git_worktree_not_clean`
- `kernel_02_head_mismatch`

Those warnings were already expected for this lane.

## Build Context

| Field | Value |
| --- | --- |
| Build ID | `CG_CANONICAL_BUILD_CONTEXT_V1_C006_W20260518_W20260601` |
| Build mode | `FORWARD_CANONICAL_LOCAL_LAB` |
| Build created at | `2026-06-12T02:32:52Z` / `2026-06-11T22:32:52-04:00` |
| Affected date window | `2026-05-18..2026-06-07` |
| Affected weeks | `2026-05-18`, `2026-06-01` |
| Raw batch merge policy | `ONE_EXACT_BATCH_PER_SOURCE; NO_MIX_FOR_LEGACY_PARITY` |
| Source precedence version | `CG_SOURCE_PRECEDENCE_V2_KPIONE2_POWER_APP_KPIONE_AUDIT` |
| Daily builder version | `CG_DAILY_CANONICAL_PINNED_RAW_ROUTE_V1` |
| Weekly builder version | `CG_WEEKLY_FROM_CANONICAL_DAILY_ROUTE_V1` |
| Route lineage manifest hash | `87d7d4540c8ca1a87751f8349075904f` |
| Input manifest hash | `296a3d8c2550c2821546128491b38483` |
| Daily output hash | `fdde399cd6f74d5cdc4fb5a251b4a40c` |
| Weekly output hash | `6180e2f8c4f4bd133053421c32219788` |

The context was checked for implicit selector language. Tokens such as `latest`, `current`, `implicit`, or `auto winner` were not present in the build-context rule fields.

## Raw Lineage

The canonical build pins exact raw source batches and does not use a runtime latest-batch selector.

| Source | Batch | Type | Loaded rows | Selected-window rows | Snapshot hash |
| --- | ---: | --- | ---: | ---: | --- |
| `KPIONE` | 19 | `ACCUMULATIVE_SNAPSHOT` | 75124 | 0 | `8931d1f7d05701fa9731e0dd1b160155e1127977a475ec43399c2eac9d7f9cf5` |
| `KPIONE2` | 38 | `CORRECTION` | 45736 | 7686 | `1d031758011678c52fada8712e91807d68ebc825064865829fa32c7de6f9e675` |
| `POWER_APP` | 26 | `ACCUMULATIVE_SNAPSHOT` | 5497 | 0 | `fe4eaf1d3f7ca384eecddfff6a1cbe10427d512df38f3dbb150d443592f036a9` |

KPIONE and POWER_APP are explicitly registered in lineage but have no rows in the selected C006 week window. That is documented, not inferred away.

## Route Lineage

Route lineage uses `ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1` and is pinned by week.

| Week | Rows | Source route batches | Snapshot version | Surface hash |
| --- | ---: | --- | --- | --- |
| `2026-05-18` | 3318 | `{13}` | `ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1\|C006\|2026-05-18` | `d77324afd998852dd636092ff2bf538f` |
| `2026-06-01` | 3481 | `{15,18}` | `ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1\|C006\|2026-06-01` | `3cb74f4e4c830d0cd953dbb5dd8008ed` |

The route snapshot contains 6799 rows and zero duplicate keys.

## Daily Build

Daily canonical output is built from pinned raw lineage joined to the pinned route snapshot. It does not use legacy daily fact rows as input.

| Run | Rows | Duplicate keys | Key hash | Full row hash |
| ---: | ---: | ---: | --- | --- |
| 1 | 7484 | 0 | `a48c9f3e18668ebfdec729fa4fe79d3e` | `fdde399cd6f74d5cdc4fb5a251b4a40c` |
| 2 | 7484 | 0 | `a48c9f3e18668ebfdec729fa4fe79d3e` | `fdde399cd6f74d5cdc4fb5a251b4a40c` |
| 3 | 7484 | 0 | `a48c9f3e18668ebfdec729fa4fe79d3e` | `fdde399cd6f74d5cdc4fb5a251b4a40c` |

Run 1 by week:

| Week | Rows | Useful days |
| --- | ---: | ---: |
| `2026-05-18` | 6264 | 6264 |
| `2026-06-01` | 1220 | 1220 |

Internal checks for run 1:

| Check | Count |
| --- | ---: |
| Invalid useful day | 0 |
| Bad raw evidence count | 0 |
| Invalid source flags | 0 |
| Bad precedence | 0 |

All daily output rows are won by `KPIONE2` under the declared source precedence, with 7579 raw evidence rows represented in 7484 canonical daily rows.

## Weekly Build

Weekly canonical output is built from canonical daily output plus the pinned route snapshot. It does not use legacy weekly fact rows as input.

| Run | Rows | Duplicate keys | Key hash | Full row hash |
| ---: | ---: | ---: | --- | --- |
| 1 | 6799 | 0 | `af8c2414b4e57b57d66c2dfa0f27b0f6` | `6180e2f8c4f4bd133053421c32219788` |
| 2 | 6799 | 0 | `af8c2414b4e57b57d66c2dfa0f27b0f6` | `6180e2f8c4f4bd133053421c32219788` |
| 3 | 6799 | 0 | `af8c2414b4e57b57d66c2dfa0f27b0f6` | `6180e2f8c4f4bd133053421c32219788` |

Run 1 by week:

| Week | Rows | Visits realized |
| --- | ---: | ---: |
| `2026-05-18` | 3318 | 6264 |
| `2026-06-01` | 3481 | 1220 |

Internal checks for run 1:

| Check | Count |
| --- | ---: |
| Bad visit cap | 0 |
| Bad pending visits | 0 |
| Bad alerta | 0 |

`FUENTES_REPORTADAS_SEMANA` distribution:

| Source set | Rows |
| --- | ---: |
| empty string | 2906 |
| `KPIONE2` | 3893 |

## Clean-Room Rebuild

A second schema, `c006_clean_room`, rebuilt the same outputs from the same declared contract.

| Output | Canonical | Clean room | Match |
| --- | --- | --- | --- |
| Daily rows | 7484 | 7484 | yes |
| Daily key hash | `a48c9f3e18668ebfdec729fa4fe79d3e` | `a48c9f3e18668ebfdec729fa4fe79d3e` | yes |
| Daily full row hash | `fdde399cd6f74d5cdc4fb5a251b4a40c` | `fdde399cd6f74d5cdc4fb5a251b4a40c` | yes |
| Weekly rows | 6799 | 6799 | yes |
| Weekly key hash | `af8c2414b4e57b57d66c2dfa0f27b0f6` | `af8c2414b4e57b57d66c2dfa0f27b0f6` | yes |
| Weekly full row hash | `6180e2f8c4f4bd133053421c32219788` | `6180e2f8c4f4bd133053421c32219788` | yes |

## Legacy Diagnostics

Legacy comparisons were retained as diagnostics only.

Daily diagnostic for the selected C006 weeks:

| Metric | Count |
| --- | ---: |
| Baseline rows | 7484 |
| Canonical rows | 7484 |
| Missing from canonical | 0 |
| Extra in canonical | 0 |
| Common keys with value differences | 7484 |

Weekly diagnostic for the selected C006 weeks:

| Metric | Count |
| --- | ---: |
| Baseline rows | 6798 |
| Canonical rows | 6799 |
| Missing from canonical | 0 |
| Extra in canonical | 1 |

These legacy differences are not a gate for C006. C005 closed the legacy route/person forensics as documented non-reproducibility for values that cannot be recovered exactly from preserved provenance. The C006 contract is therefore a forward reproducibility contract: explicit date window, explicit raw lineage, explicit route lineage, deterministic daily output, deterministic weekly output, and clean-room reproducibility.

## Warnings

- Preflight warnings were non-blocking: `git_worktree_not_clean` and `kernel_02_head_mismatch`.
- Local heavy diagnostic queries from earlier attempts were cancelled in the Docker lab before the final optimized build; no product data was changed.
- KPIONE and POWER_APP pinned source batches are registered in lineage but have zero rows in the selected C006 week window.
- Legacy parity diagnostics remain different by design and are not a gate for the forward canonical contract.

## Final Assessment

`CANONICAL_BUILD_REPRODUCIBLE`: C006 defines a local forward canonical build context with explicit input lineage and reproducible daily and weekly outputs across repeated runs and a clean-room schema. Historical productive facts remain preserved; no rewrite, cleanup, or retention action is authorized by this result.
