# CG005N-Q Supabase transition decision package

## Status

- Verdict: `DECISION_PACKAGE_READY_NO_EXECUTION`
- Quality: `Q4_DECISION_GRADE`
- Baseline: `c54e2933251122375336590a931bdbc1e514718a`
- Lab branch head: `36178d88a7d81e0355616d1a2f07ed755c0fea04`
- Target week: `2026-06-08`
- Supabase access used: read-only only, `stock_zero_codex_ro`, transaction read-only `on`
- Productive writes authorized: `false`

## Read-only catalog result

| Object | Classification | Notes |
|---|---|---|
| `cg_core.ruta_rutero_week_assignment` | `CREATE_REQUIRED` | Assignment table absent; created only in CG005N after separate authorization. |
| `cg_core.v_ruta_rutero_load_batch_week_v2` | `CREATE_OR_REPLACE_REQUIRED` | Existing legacy view present; SQL 11 adds explicit assignment precedence. |
| `cg_core.v_ruta_rutero_latest_week_batch_v2` | `CREATE_OR_REPLACE_REQUIRED` | Required selector over explicit/legacy week view. |
| `cg_core.v_rr_frecuencia_base_resuelta_v2` | `CREATE_OR_REPLACE_REQUIRED` | Existing view present; SQL 11 prevents legacy backfill for assigned weeks. |
| `public.ruta_rutero` | `ALREADY_COMPATIBLE` | Required current-route columns are present. |
| `cg_core.ruta_rutero_load_batch` | `ALTER_REQUIRED` | Status lifecycle check must include guarded pending/failed states. |
| `cg_core.ruta_rutero_load_rows` | `ALREADY_COMPATIBLE` | History columns required by loader/postchecks are present. |

Aggregates observed: total batches 19, latest batch 19, current rows 3568, history rows 64346, target week legacy batches 1. No rows, people, stores, payloads, DSNs or credentials were extracted.

## CG005N - DDL contract

CG005N is a separate DDL-only authorization. It creates the assignment table, applies lifecycle compatibility, replaces the week-batch and resolved-route views, grants read access, and runs read-only postchecks. It does not authorize dry-run, apply, cleanup or retention.

Stop on SQL hash mismatch, catalog drift, unknown material dependency, DDL role mismatch or any postcheck failure. Rollback DDL must restore previous view definitions and may drop the assignment table only if no productive apply occurred.

## CG005O - Productive dry-run contract

CG005O recalculates the workbook SHA-256 immediately before execution. It uses sheet `RUTA_RUTERO`, source `DB_GLOBAL_INVENTARIO.xlsx:RUTA_RUTERO`, effective week `2026-06-08`, source-check and dry-run only. It must report altas, bajas, cambios, duplicates, planned assignment and rollback target. It performs no DB writes.

## CG005P - Guarded apply contract

CG005P requires a later, independent Bastian authorization after CG005O review. It requires an explicit confirm token, expected workbook hash, explicit week, explicit source, authorized write role, single transaction, advisory lock, full source replacement, assignment lifecycle and full postchecks. It aborts on hash/week/source mismatch, source-check failure, lock failure, postcheck mismatch, unexpected rows or missing rollback target.

## CG005Q - Postvalidation contract

CG005Q verifies batch/history/current rows, exactly one ACTIVE assignment, resolved and weekly views, hashes, missing/extra/duplicate grains, stale source rows, other weeks unchanged and route-impact handoff.

## Risk matrix

| Riesgo | Prevenci?n | Evidencia | Abort |
|---|---|---|---|
| DDL incompatible | cat?logo read-only | object diff | s? |
| semana incorrecta | argumento expl?cito | 2026-06-08 | s? |
| archivo cambiado | SHA-256 recalculado | expected hash | s? |
| stale rows | replacement completo | postcheck | s? |
| legacy backfill | assignment expl?cito | weekly view | s? |
| concurrencia | advisory lock | lab real | s? |
| rollback defectuoso | B->A probado | local lab | s? |
| impacto inesperado CG | shadow posterior | impact diff | s? |
| secreto expuesto | env only/redaction | scan | s? |

## Proposed commands - not executed

```powershell
# CG005N command
python scripts/apply_cg005n_ddl.py --db $env:DB_URL_DDL --sql sql/11_control_gestion_route_week_replacement_contract.sql --expected-sql-sha256 $env:CG005N_SQL11_SHA256

# CG005N postcheck
python scripts/cg_readonly_extract.py route-preflight --db $env:DB_URL_CODEX_RO --effective-week-start 2026-06-08

# CG005O dry-run
python scripts/load_ruta_rutero_from_excel.py --db $env:DB_URL_CODEX_RO --excel $env:CG005_ROUTE_WORKBOOK --sheet RUTA_RUTERO --source-check-only --effective-week-start 2026-06-08 --json-out $env:TEMP\cg005o_source_check.json
python scripts/load_ruta_rutero_from_excel.py --dry-run --excel $env:CG005_ROUTE_WORKBOOK --sheet RUTA_RUTERO --effective-week-start 2026-06-08 --expected-workbook-sha256 $env:CG005O_RECALCULATED_WORKBOOK_SHA256 --json-out $env:TEMP\cg005o_dry_run.json

# CG005P apply template
python scripts/load_ruta_rutero_from_excel.py --apply --db $env:DB_URL_ROUTE_WRITE --excel $env:CG005_ROUTE_WORKBOOK --sheet RUTA_RUTERO --effective-week-start 2026-06-08 --expected-workbook-sha256 $env:CG005O_RECALCULATED_WORKBOOK_SHA256 --confirm-weekly-replacement $env:CG005P_CONFIRM_WEEKLY_REPLACEMENT --json-out $env:TEMP\cg005p_apply.json

# CG005Q postvalidation
python scripts/cg_readonly_extract.py route-preflight --db $env:DB_URL_CODEX_RO --effective-week-start 2026-06-08 --json-out $env:TEMP\cg005q_postvalidation.json
```

## Authorizations

```json
{
  "cg005n_ddl_authorized": false,
  "cg005o_dry_run_authorized": false,
  "cg005p_apply_authorized": false,
  "cg005q_postvalidation_authorized": false,
  "supabase_writes_authorized": false,
  "cleanup_authorized": false,
  "retention_authorized": false,
  "decision_required_from": "Bastian"
}
```
