# CODEX C004 G0 Execution

Phase: `FASE_C004_CODEX_G0_CLOSURE_WEEK_RETROACTIVE_POLICY_LOCAL_ONLY`

Verdict: `ROOT_CAUSE_EXTENDED`

Route business policy applied: `ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1`

## Scope

Codex executed the C004 lab only in local PostgreSQL, schema `c004_g0`, on container `stock_zero_cg_parity_pg`. Supabase access was limited to the read-only preflight lane (`stock_zero_codex_ro`, read-only transaction state on). No Supabase writes, loaders, product refresh, code changes, SQL product changes, evidence changes, kernel changes, cleanup, retention, or deletion were performed.

The only repository outputs from this phase are:

- `research/C004_CODEX_G0_EXECUTION.json`
- `research/reports/CODEX_C004_G0_EXECUTION.md`

## Preflight

`scripts/codex_ro_env_check.py` confirmed:

- `CURRENT_USER=stock_zero_codex_ro`
- `DEFAULT_TRANSACTION_READ_ONLY=on`
- `STATEMENT_TIMEOUT=15s`
- DSN was not printed

`scripts/sz_preflight.py --phase control_gestion_v2 --root .` returned `warn` with no blockers. Warnings were the pre-existing `git_worktree_not_clean` and `kernel_02_head_mismatch`.

## Claude Proposal Audit

| Proposal | Decision | Reason |
| --- | --- | --- |
| Missing key forensics | `ACCEPT` | The missing daily key was traced independently through baseline, raw, latest raw scope, event scope, route scope, and local daily surfaces. |
| Value diff matrix | `ACCEPT` | A local matrix was built and grouped by affected columns and keys. |
| Route policy options | `ACCEPT_WITH_CORRECTION` | Claude proposed options, but Bastian supplied the binding rule `ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1`. The lab applied that rule instead of reopening policy selection. |
| Weekly from daily staged | `ACCEPT` | Weekly was built from staged daily and route surfaces, not from `cg_mart.v_cg_out_weekly_v2`. |
| Clean-room reproducibility | `NOT_NEEDED` | Exact daily and weekly parity did not close, so clean-room replay is premature. |

## Missing Key

The single missing daily key is:

| fecha_visita | cod_rt | cliente_norm |
| --- | --- | --- |
| 2026-05-15 | J988 | THE POWER OF FOOD |

Classification: `RAW_SUPERSESSION`.

Evidence:

- Baseline contains one KPIONE2 row for the key.
- `cg_raw.kpione2_raw` contains one exact historical candidate in batch `31`, id `299566`, source row `31440`.
- The latest KPIONE2 batch contains zero exact candidates.
- The post-latest-batch event scope contains zero rows for the key.
- Route scope exists for week `2026-05-11` in batches `8, 9, 10, 11, 12`.
- Therefore the row is not absent from raw and not blocked by route scope; it is present in older accumulated raw but superseded by the latest snapshot surface used by the local reconstruction.

Recovering this key closes daily key parity, but not value parity.

## Route Snapshot

Local experimental surface: `c004_g0.route_week_snapshot_resolved`

Classification: `COMPOSITE_WEEKLY_SNAPSHOT`, with remaining ambiguity because parity still does not close.

Surface hash: `089a9b098b913ea2ea23c051e0e01f01`

| Week | Rows | Source batches | Source duplicate delta |
| --- | ---: | --- | ---: |
| 2026-05-11 | 3298 | 12 | 30 |
| 2026-05-18 | 3318 | 13 | 30 |
| 2026-05-25 | 3319 | 14 | 30 |
| 2026-06-01 | 3439 | 15, 17 | 33 |

The 2026-06-01 surface is composite: most keys align closer to batch 17, while a small ECOCULTIVA subset aligned better to batch 15. This supports the need for an explicit weekly route version contract, but the local surface is not an approved product rule.

## Daily Results

| Variant | Rows | Missing | Extra | Duplicates | Value diffs | Full hash parity |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `local_route16_dedup` | 21347 | 1 | 0 | 0 | 56 | false |
| `after_missing_key` | 21348 | 0 | 0 | 0 | 56 | false |
| `after_route_snapshot` | 21348 | 0 | 0 | 0 | 299 | false |

Best daily result: `after_missing_key`.

Daily key parity closes after recovering the missing key, but 56 value differences remain. Applying the experimental route snapshot increases value differences to 299. The route-snapshot value matrix reports 1696 diff cells across 276 daily keys, concentrated in route/person columns:

| Column | Diff rows |
| --- | ---: |
| `reponedor_scope` | 253 |
| `reponedor_scope_norm` | 253 |
| `rutero` | 253 |
| `gestor` | 236 |
| `gestor_norm` | 236 |
| `modalidad` | 236 |
| `supervisor` | 206 |
| `local_nombre` | 23 |

## Weekly Results

Weekly was built from staged daily plus the local route snapshot, mirroring the product `_cg_weekly_stage` direction and avoiding `cg_mart.v_cg_out_weekly_v2`.

Compared weeks: `2026-05-11`, `2026-05-18`, `2026-05-25`, `2026-06-01`.

Candidate weekly key:

- `SEMANA_INICIO`
- `COD_RT`
- `CLIENTE_NORM_FILTER`

The candidate key has zero duplicates in both baseline and staged output.

| Surface | Rows |
| --- | ---: |
| Baseline weekly subset | 13445 |
| Local staged weekly | 13374 |

Key diff:

| Side | Rows |
| --- | ---: |
| Missing from staged | 71 |
| Extra in staged | 0 |

Missing by week:

| Week | Missing |
| --- | ---: |
| 2026-05-11 | 30 |
| 2026-06-01 | 41 |

Common-key values also differ:

- affected common keys: 4170
- total diff cells: 5125
- columns with diffs: 23

Top weekly value differences:

| Column | Diff rows |
| --- | ---: |
| `FUENTES_REPORTADAS_SEMANA` | 4048 |
| `RUTA_DUPLICADA_FLAG` | 123 |
| `REPONEDOR` | 111 |
| `GESTOR` | 107 |
| `GESTOR_NORM_FILTER` | 107 |
| `RUTERO` | 107 |
| `RUTERO_NORM_FILTER` | 107 |
| `MODALIDAD` | 103 |

Weekly parity is not closed.

## Weekly Timing

Local hardware:

- CPU: Intel(R) Core(TM) i5-1035G1 CPU @ 1.00GHz
- cores/logical processors: 4/8
- memory: 7.78 GB
- OS: Windows 11 Home Single Language 10.0.26200
- Docker Desktop: 4.76.0
- Docker Engine: 29.5.2
- PostgreSQL: 17.10

| Run | Seconds | Rows | Rows/sec |
| --- | ---: | ---: | ---: |
| 1 | 10.1917013 | 13374 | 1312.24410982296 |
| 2 | 8.5519564 | 13374 | 1563.8526875558 |
| 3 | 7.5867757 | 13374 | 1762.80419098195 |

Median: 8.5519564 seconds. Max: 10.1917013 seconds.

Evidence-based local lab limit proposed: 30 seconds for this four-week staged weekly build on this hardware. Re-baseline if hardware, Docker allocation, or build scope changes.

## Clean Room

Clean-room replay was not executed. The hard precondition was not met because daily value parity and weekly key/value parity did not close in the primary C004 lab.

## Conclusion

C004 extends the root-cause map but does not close G0:

1. The single missing daily key is `RAW_SUPERSESSION`.
2. Recovering that key closes daily key parity.
3. Daily value parity remains open with 56 value differences in the best variant.
4. The route weekly snapshot is composite and unresolved; applying it worsens daily value parity.
5. Weekly staged build is fast and avoids the expensive raw weekly view, but weekly parity remains open with 71 missing keys and common-key value differences.

Next step: ChatGPT/Bastian review before deciding whether the next lane should isolate the remaining 56 daily value differences, formalize a route snapshot authority table, or reframe weekly parity around an explicit build-context contract.
