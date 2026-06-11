# CODEX C002 B0 Parity Root Cause

Phase: `FASE_C002_B0_FUNCTIONAL_PARITY_ROOTCAUSE_NO_SUPABASE_WRITE`  
Verdict: `ROOT_CAUSE_CONFIRMED`  
Supabase writes: `false`  
Local PostgreSQL writes: `true` in experimental schema `c002_b0` only.

## Scope And Integrity

Codex reused the existing 9C7A3/9C7A4 evidence. No full Supabase extraction was repeated. The 9C7A3 export manifest hash matched `7526ea7ea080ea018a035c220c20defb4c3acee7c33ce73752ad4e93383f2e63`, and the 9C7A4 baseline manifest hash matched `1e6879d14cc5a84f85da765c77b77408cfdbfffd2c25af8de31f943b858cfb37`. Local PostgreSQL ran in `stock_zero_cg_parity_pg` on `127.0.0.1:55432` with PostgreSQL 17.10.

Loaded local counts matched the checkpoint: `kpione_raw=707321`, `kpione2_raw=526022`, `power_app_raw=42366`, `ruta_rows=64346`, `ruta_batch=19`, `batch_registry=37`, and initial `parity.local_daily=34817`.

## Daily Root Cause

The real daily key for this diagnosis is:

`fecha_visita + cod_rt + cliente_norm`

That key is unique in both baseline and local daily surfaces. It produced:

- baseline daily rows: 21,348
- initial local daily rows: 34,817
- extra local keys: 13,470
- missing baseline keys: 1
- net surplus: 13,469
- duplicate keys: 0 on both sides

All 13,470 extra keys are dates before the productive daily baseline window: 2026-04-27 through 2026-05-10. They split by week as:

- 2026-04-27: 6,249
- 2026-05-04: 7,221

The local reconstruction used latest raw batches that are accumulative/mixed. The latest batches contained older dates:

- KPIONE batch 19: latest selected, full snapshot accumulative, matched dates 2026-04-27..2026-05-03
- KPIONE2 batch 38: latest selected, mixed, matched dates 2026-04-27..2026-06-01
- POWER_APP batch 26: latest selected, full snapshot accumulative, matched dates 2026-04-27..2026-05-10

So the daily surplus is not caused by selecting multiple raw batches. It is caused by reconstructing from the complete latest raw snapshots without the productive affected-date/window contract that the daily baseline reflects.

## Controlled Experiments

| Experiment | Rows | Extra | Missing | Duplicates | Value diffs | Result |
|---|---:|---:|---:|---:|---:|---|
| Initial key diff | 34,817 | 13,470 | 1 | 0 | 1,204 | FAIL |
| Window only: `fecha_visita >= 2026-05-11` | 21,347 | 0 | 1 | 0 | 1,204 | Surplus removed |
| Precedence view + window | 21,347 | 0 | 1 | 0 | 1,227 | Surface aligned, not closed |
| Route16 dedup for week 2026-06-01 | 21,347 | 0 | 1 | 0 | 56 | Root cause confirmed, still open |

The window correction alone removes all extra keys. The remaining differences are not the surplus cause. They are a separate route snapshot/value problem.

## Hypotheses

- H1 latest batch selection: REJECTED. The latest selector chooses KPIONE 19, KPIONE2 38, POWER_APP 26 exactly once each.
- H2 accumulative raw treated as incremental: CONFIRMED. It explains all 13,470 extra keys.
- H3 ruta_batch divergence: PARTIAL. It explains most route attribute differences for week 2026-06-01, not the row surplus.
- H4 route multiplication: REJECTED for surplus. The real daily key has zero duplicate keys.
- H5 payload_json vs physical columns: PARTIAL, not surplus cause.
- H6 KPIONE2 SP Item ID physical NULL with payload present: CONFIRMED, but not surplus cause. It reinforces that payload_json is required.
- H7 local DDL mismatch: PARTIAL. Required v2 views exist, but local parity initially compared the core daily view, while the productive daily fact aligns with the precedence view and mart timestamp.
- H8 omitted productive filter/dependency: CONFIRMED. The daily productive window starts at 2026-05-11.
- H9 raw/ruta temporal mismatch: PARTIAL. Baseline route attributes for 2026-06-01 match ruta_batch 16/17 far better than local ruta_batch 18.

## Weekly Timeout

Weekly remains open. A one-week `EXPLAIN ANALYZE` for `SEMANA_INICIO=2026-05-11` timed out at 300 seconds, and an explicit one-week materialization timed out at 180 seconds. The full weekly materialization previously timed out at 900 seconds.

The plan without execution shows the reason: the week filter does not cheaply materialize a small weekly subset. The view enters the raw/event-scope pipeline first, including:

- `WindowAgg` and `Sort` over `v_cg_evento_scope_v2`
- `Append` over raw source branches
- computed payload/date/week expressions before filtering
- `Hash Join` from raw evidence to route frequency base
- weekly `Sorted Aggregate` with multiple `string_agg(DISTINCT ...)` expressions

The practical fix for parity is not a product index change yet. It is to build weekly from staged local daily/route surfaces, then compare. Local-only index candidates are recorded in the JSON.

## G0 Status

G0 is not closed. The root cause is confirmed, but exact parity still needs:

1. Resolve the single missing KPIONE2 key on 2026-05-15.
2. Define the authoritative route snapshot policy for week 2026-06-01.
3. Rebuild weekly from staged daily/route surfaces and recompare.

No storage cleanup, retention, payload_json removal, product refresh, or Supabase write is authorized by this result.
