# CODEX_C003_HORIZON_CAPABILITY_VALIDATION

Phase: FASE_AI_C003_CODEX_VALIDATE_AND_PROMOTE_PROJECT_HORIZON
Date: 2026-06-11
Runtime code baseline: 2c135a7be1d813a2c914f43b26b0579504a7f8a5
DB access: none
Docker/loaders/SQL: not executed

## Verdict

Codex validates Claude C003 as useful and promotable with corrections. The promoted horizon is an ACTIVE_DIRECTION contract, not an implementation authorization. The capability map keeps STOCK_ZERO as a legacy-stable app, separates durable contracts from evidence snapshots, and makes future capabilities explicit without presenting them as implemented.

## Horizon Corrections Applied

- DB catalog work can advance in parallel with H0 when explicitly read-only and authorized. Open G0 blocks retention, deletion, cleanup, and raw archival execution, not every research/catalog task.
- Bastian decides semantic route validity. Codex resolves which technical route snapshot produced each materialization.
- payload_json is REQUIRED_CURRENT_SURFACE and can be replaced only after equivalent consumers and parity are proven. It is not canonical forever.
- Dates, row counts, and batch identifiers are evidence snapshots. Stable contracts use parameters: start_date, end_date, batch identifiers, route snapshot, build version, and expected parity.
- No second repository, no big-bang rewrite, and no obsolete framing for STOCK_ZERO are authorized. STOCK_ZERO moves gradually toward LEGACY_STABLE.

## Capability Validation

Claude proposed 16 capabilities. Codex validated the 16 with corrections and added 9 missing or under-specified capabilities: b2b_source_ingestion, exports_and_reporting, identity_and_access, observability_and_runtime_health, kpi_composition, alert_generation, decision_and_action_tracking, configuration_and_contract_registry, and control_gestion_mart_persistence.

Key reclassifications:

- RUTA_RUTERO is shared reference data, not exclusive Control Gestion.
- inventory_ingestion is a domain/business capability; the current Excel loader is legacy implementation.
- readonly_extraction is INTERNAL_TOOLING.
- laboratory_reproducibility is LABORATORY.
- source_precedence is a generic engine with Retail Trust configuration.
- payload_json remains a required current compatibility surface, replaceable only with parity.

## Evidence Checked

- app/screens/control_gestion.py: V2 normal path, USE_CG_V2 switch, B3 read path and fallback warning path.
- app/db.py: B3 and V2 contract functions, CG_V2_OUT_WEEKLY_VIEW consumers, ranking/export selectors.
- app/services/stock.py and app/screens/reposicion.py: reposicion uses stock_service, which delegates to app/db.py.
- scripts/load_control_gestion_raw_v17.py: source sheets, batch registry, sheet hash, payload_json, KPIONE2 fields.
- scripts/load_ruta_rutero_from_excel.py: Route Master current table and history rows.
- scripts/cg_readonly_extract.py and tests/test_cg_readonly_extract.py: read-only extraction safety contract.
- research/C001_CODEX_VALIDATION_MATRIX.json and research/C002_B0_PARITY_ROOTCAUSE.json: validated payload_json consumers, route history evidence, G0 root cause, and weekly timeout.

## Required Future Reading

Future Claude and Codex tasks must read these before research or implementation in this lane: research/AI_PROJECT_HORIZON.json, research/AI_CAPABILITY_MAP.json, research/AI_SHARED_MEMORY.json, and research/AI_BACKLOG.json. These files do not grant runtime, DB, loader, SQL, commit, or push permissions by themselves.

## Not Authorized

This phase did not modify app, scripts, SQL, tests, kernels, data, evidence, or Claude proposal files. It did not access DB, Docker, Supabase, loaders, or SQL. No capability implementation is authorized by the promoted contracts.
