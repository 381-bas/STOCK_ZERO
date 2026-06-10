# CODEX C001 Independent Validation

Phase: `FASE_AI_C001_CODEX_INDEPENDENT_VALIDATION_AND_MEMORY_PROMOTION`  
Runtime baseline: `2c135a7be1d813a2c914f43b26b0579504a7f8a5`  
Claude C001 commit: `ebdd99ffc2a4981c9129a8571a192d1e582078a7`  
Codex tooling commit: `f05e74c46b5dd12bd5b8d2a91def206755a75deb`  
DB access: read-only only through `codex_ro_env_check.py`, `sz_preflight.py`, and allowlisted `cg_readonly_extract.py c001-profile`. No row-level DB results are stored here.

## Summary

Codex reviewed 30 Claude C001 findings independently. Classification counts:

- VALIDATED: 11
- PARTIAL: 13
- DISPUTED: 5
- REJECTED: 1

No runtime implementation is authorized by this report.

## Que acerto Claude

- `DB_GLOBAL_INVENTARIO.BASE` has four formula-derived columns and the loader reads saved values, not live Excel formulas.
- `duplicados` is technically redundant for the loader because Python dedupe and the DB conflict key handle duplicates.
- `CUMPLIMIENTO_FRECUENCIA.xlsx` feeds the committed CG loader only through `DB (KPIONE)`, `DB (KPIONE2.0)`, and `DB (POWER_APP)`.
- The ruta loader writes `public.ruta_rutero`, `cg_core.ruta_rutero_load_rows`, and `cg_core.ruta_rutero_load_batch`; `row_hash` is analysis metadata, not the UPSERT key.
- The CG raw loader uses hash-skip and `cg_audit.batch_registry` as batch metadata.
- The ten named `app/db.py` functions have no static callers and no known dynamic dispatch pattern in this pass.
- `app/Home.py` defers db/export/screen imports inside `main()`.

## Que exagero

- `DB_GLOBAL_HISTORICO.xlsx` looks like an external archive, but its construction and operational authority are not proven.
- The manual weekly workflow and recalculation-time estimates are plausible but not independently observed.
- Raw/payload retention windows like 12 weeks or route retention like 52 loads need policy and consumer evidence, not fixed numbers from this pass.
- `app/db.py` complexity is real, but f-string SQL/import patterns are not high risk by themselves without a concrete failure path.

## Que fue incorrecto

- `DB_GLOBAL_INVENTARIO.DB_HISTORICA` does not have the same 12-column schema as `BASE`; it has a 10-column table structure.
- The claimed `CG_V2_*` constant list is wrong. Actual constants include `CG_V2_DETALLE_VIEW`, `CG_V2_MULTI_MARCAJE_VIEW`, `CG_V2_DAILY_EVIDENCE_VIEW`, and route/audit views not listed by Claude.
- `payload_json` is not zero-consumer. Read-only DB profile found consumers in `cg_core` and `cg_mart` view definitions.
- Current aggregate DB evidence does not support retiring `DB_GLOBAL_HISTORICO.xlsx`; `public.fact_stock_venta` covers 2026-05-02 to 2026-06-07, while the external historical workbook range checked was 2026-03-03 to 2026-04-06.

## Que no pudo demostrarse

- Whether `DESARROLLO_CONTROL_GESTION` and `OUT` are still used manually.
- Whether `DB_GLOBAL_HISTORICO.xlsx` is authoritative for any business workflow.
- Whether `OTROS` has the asserted 2 percent non-empty hit rate; K-column formula cache was absent in XML inspection.
- Whether all SQL drafts have or have not been applied to the live DB. File names and git status are not sufficient proof.

## Que necesita decision de Bastian

- Remove or keep workbook `duplicados` as a manual-user column.
- Define the authoritative historical stock/sales source.
- Decide whether manual CG sheets `DESARROLLO_CONTROL_GESTION` and `OUT` still matter.
- Choose retention policy for route history and CG raw data after full DB cataloging.
- Approve any future cleanup of static-only `app/db.py` functions.

## Portafolio validado de simplificacion

- Documentation-only simplification: record sheet roles and actual `CG_V2_*` constants.
- Parity-test simplification: compute `COD_RT` from same-workbook `RUTA_RUTERO` first, then evaluate a versioned route snapshot.
- Cleanup-candidate simplification: static-only `app/db.py` functions, pending approval and runtime smoke.
- Governance simplification: historical Excel archive authority, pending Bastian decision.

## Orden recomendado de futuras implementaciones

1. No-code governance: confirm manual use and source authority with Bastian.
2. Add read-only parity tests for `COD_RT` and, later, `OTROS`.
3. Build an authoritative DB object and view-dependency catalog.
4. Only after parity/catalog evidence, design cleanup PRs for docs, static-only functions, or retention.
5. Do not remove `payload_json` or retire historical files from current evidence.

## Recommendation Classification

- R01 remove `duplicados`: `REQUIRES_BASTIAN_OPERATIONAL_CONFIRMATION`.
- R02 mtime preflight: `REFORMULATE`; mtime alone is weak.
- R03 document CG sheet roles: `VALIDATED_TECHNICALLY`.
- R04 route retention 52 loads: `REFORMULATE`.
- R05 cg_raw retention 12 weeks: `REFORMULATE`.
- R06 CG env startup probe: `REFORMULATE` with actual constants.
- R07 Python COD_RT offload: `REQUIRES_PARITY_TEST`.
- R08 remove `payload_json`: `REJECTED`.
- R09 zero-caller cleanup: `VALIDATED_TECHNICALLY_REQUIRES_APPROVAL`.
- R10 retire historical Excel: `REJECTED_FOR_NOW_REQUIRES_BASTIAN`.
