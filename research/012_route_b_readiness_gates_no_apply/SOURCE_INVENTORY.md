# 012 Route B Source Inventory

## Run context

- Branch: `lab/FAST_REFORM_012_route_b_readiness_gates_no_apply`
- HEAD: `e2fb111`
- Generated UTC: `2026-07-02T01:01:23+00:00`

## Purpose

Read-only extraction of existing Route B research, tests, scripts and contracts to support readiness gates.

## Files reviewed

- `contracts/control_gestion/kpione2_photo_export_contract_v1.json` (3041 bytes)
- `research/010A_route_b_implementation/CLAUDE_AUDIT_BRIEF.md` (807 bytes)
- `research/010A_route_b_implementation/CODEX_EXECUTION_BRIEF.md` (829 bytes)
- `research/010A_route_b_implementation/README.md` (1157 bytes)
- `research/010B_route_b_claude_audit/CLAUDE_ROUTE_B_AUDIT.md` (18133 bytes)
- `research/010C_route_b_dry_run_validation/CLAUDE_POST_AUDIT.md` (17491 bytes)
- `research/010C_route_b_dry_run_validation/CODEX_CONTRACT_IMPACT.md` (1336 bytes)
- `research/010C_route_b_dry_run_validation/CODEX_DIFF_SUMMARY.md` (1151 bytes)
- `research/010C_route_b_dry_run_validation/CODEX_DRY_RUN_OUTPUT.json` (8816 bytes)
- `research/010C_route_b_dry_run_validation/CODEX_ROLLBACK_NOTE.md` (984 bytes)
- `research/010D_route_b_source_row_number_lock/README.md` (1448 bytes)
- `research/010E_route_b_source_row_number/CLAUDE_POST_AUDIT.md` (17828 bytes)
- `research/010E_route_b_source_row_number/CODEX_DIFF_SUMMARY.md` (1103 bytes)
- `research/010E_route_b_source_row_number/CODEX_DRY_RUN_OUTPUT.json` (11569 bytes)
- `research/010E_route_b_source_row_number/CODEX_SOURCE_ROW_NUMBER_NOTE.md` (2402 bytes)
- `research/010F_route_b_source_row_number_post_audit_close/README.md` (1177 bytes)
- `research/010G_route_b_integration_planning_no_apply/README.md` (845 bytes)
- `research/010G_route_b_integration_planning_no_apply/RISK_MATRIX.md` (1288 bytes)
- `research/010G_route_b_integration_planning_no_apply/ROUTE_B_INTEGRATION_PLAN.md` (2126 bytes)
- `research/010G_route_b_integration_planning_no_apply/VALIDATION_GATES.md` (1332 bytes)
- `research/010H_route_b_integration_plan_cross_audit/CLAUDE_CROSS_AUDIT.md` (17652 bytes)
- `research/012_route_b_readiness_gates_no_apply/README.md` (1003 bytes)
- `scripts/cg_route_weekly_local_lab.py` (54302 bytes)
- `scripts/load_control_gestion_raw_v17.py` (60923 bytes)
- `scripts/load_kpione2_photo_from_excel.py` (25059 bytes)
- `scripts/refresh_control_gestion_v2_incremental.py` (65309 bytes)
- `scripts/refresh_control_gestion_v2_mv.py` (12020 bytes)
- `tests/test_cg_route_weekly_local_lab.py` (10416 bytes)
- `tests/test_kpione2_photo_grain.py` (10554 bytes)

## Keyword evidence extracts

### `contracts/control_gestion/kpione2_photo_export_contract_v1.json`

Matches:
- L2: "artifact": "kpione2_photo_export_contract_v1",
- L13: "grain_contract": {
- L14: "input_grain": "photo_row",
- L15: "normalized_grain": "event_row",
- L16: "compliance_grain": "day_presence",
- L23: "denominator_mapping": {
- L24: "Codigo Local": "cod_rt",
- L32: "Codigo Local",
- L44: "reason": "photo sequence/count denominator"

### `research/010A_route_b_implementation/CLAUDE_AUDIT_BRIEF.md`

Headings:
- L1: # Claude Audit Brief — 010A Route B
- L3: ## Role
- L7: ## Audit target
- L11: ## Questions to answer
- L21: ## Required output

Matches:
- L18: 6. What rollback notes are required?

### `research/010A_route_b_implementation/CODEX_EXECUTION_BRIEF.md`

Headings:
- L1: # Codex Execution Brief — 010A Route B
- L3: ## Role
- L7: ## Current phase
- L11: ## After 010A merge
- L15: ## Required constraints
- L25: ## Expected future evidence

Matches:
- L30: - rollback note

### `research/010A_route_b_implementation/README.md`

Headings:
- L1: # 010A Route B Implementation Lock
- L3: ## Phase
- L7: ## Base
- L11: ## Purpose
- L23: ## Active governance
- L30: ## Route B hard contract
- L40: ## Agent model
- L47: ## Non-goals

Matches:
- L13: This phase prepares Route B implementation work under the active delegated agent authority model.
- L28: - `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- L53: - No production cutover.

### `research/010B_route_b_claude_audit/CLAUDE_ROUTE_B_AUDIT.md`

Headings:
- L1: # CLAUDE ROUTE B AUDIT
- L2: ## Phase: FAST_REFORM_010B_ROUTE_B_CODEX_IMPLEMENTATION_AFTER_CLAUDE_AUDIT
- L13: ## Context
- L29: ## Q1: Does Route B preserve photo_row â†’ event_row â†’ day_presence?
- L65: ## Q2: Risk of relapsing into one_excel_row_equals_one_visit?
- L69: ### Risk R1 â€” Direct row-to-presence mapping (CRITICAL)
- L81: ### Risk R2 â€” Incorrect aggregation key (MEDIUM)
- L86: ### Risk R3 â€” Day presence from event count, not binary (LOW-MEDIUM)
- L91: ### Risk R4 â€” Hash contamination from photo-level columns (LOW)
- L100: ## Q3: Classification by authority level
- L102: ### GREEN â€” Total autonomy (no approval needed, no cross-audit)
- L115: ### YELLOW â€” Controlled autonomy (no approval, but gated by phase)
- L125: ### ORANGE â€” Cross-audit required before PR is merge-ready
- L139: ### RED â€” BastiÃ¡n explicit authorization required
- L153: ## Q4: What can Codex do without asking for authorization?
- L182: ## Q5: What requires cross-audit?
- L187: ### CA1 â€” Grain aggregation correctness (HIGHEST PRIORITY)
- L190: # Correct pattern (groupby ID, not per-row)
- L195: # Forbidden: one row = one visit
- L201: ### CA2 â€” day_presence derivation

Matches:
- L6: **Active contract:** `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- L15: Route B is the proposed implementation path for ingesting KPIONE2 photo export data from
- L17: locked the grain contract and produced briefs. This audit, produced in 010B, answers the
- L21: - `kpione2_photo_export_contract_v1.json` â€” active contract (ACTIVE, do not modify)
- L22: - `load_control_gestion_raw_v17.py` â€” existing KPIONE2 raw loader (guarded)
- L33: The active contract (`kpione2_photo_export_contract_v1.json`) correctly encodes the three-level chain:
- L39: | `day_presence` | One presence record per unique `(Fecha, Codigo Local, Marca)` per event |
- L42: - `Foto NÂº/Total` â€” photo sequence denominator, varies within an event
- L73: The existing `load_kpione2` function in `load_control_gestion_raw_v17.py` (lines 712â€“733)
- L74: sets `visita_value = 1 if has_evidence or (visita_numeric > 0) else 0` **per row**.
- L75: This pattern is safe for the `DB (KPIONE2.0)` sheet where each row IS one visit summary,
- L78: Codex MUST NOT copy this row-level visita_value derivation to the photo loader.
- L82: If event aggregation groups by `(Codigo Local, Marca, Fecha)` only â€” without including `ID` â€”
- L88: boolean (present/absent), the compliance denominator would be inflated.
- L94: that are semantically identical, causing phantom duplicates.
- L107: | Create new loader script (skeleton) | `scripts/load_kpione2_photo_from_excel.py` | New file, no DB execution |
- L108: | Create tests | `tests/test_kpione2_photo_grain.py` (or similar) | Unit tests with fixture data |
- L109: | Create SQL DDL file (no apply) | `sql/15_kpione2_photo_raw_ddl.sql` | File only, NO execution |
- L115: ### YELLOW â€” Controlled autonomy (no approval, but gated by phase)
- L119: | New loader complete implementation | `scripts/load_kpione2_photo_from_excel.py` | After GREEN skeleton passes grain tests |
- L131: | Modifying existing KPIONE2 loader | `scripts/load_control_gestion_raw_v17.py` | Guarded loader â€” live compliance pipeline |
- L135: | Modifying compliance views | any `cg_core.*` or `cg_mart.*` SQL | Route B grain must not break existing compliance |
- L136: | Changing `KPIONE2_NUMERIC_COLUMNS` | `load_control_gestion_raw_v17.py:31` | Changes what gets null-coerced |
- L137: | Modifying `event_stable_hash_columns` | `kpione2_photo_export_contract_v1.json` | Active contract change â€” requires cross-audit |
- L144: | SQL apply against Supabase real | Production data |
- L146: | Modifying `kpione2_photo_export_contract_v1.json` | Active contract â€” governance |
- L147: | Merge to main | Final gate |
- L158: 1. **Create** `scripts/load_kpione2_photo_from_excel.py`
- L161: - Must validate grain against `kpione2_photo_export_contract_v1.json` blockings:
- L167: 3. **Create** `sql/15_kpione2_photo_raw_ddl.sql` â€” DDL file ONLY, header MUST start with
- L174: `data/photo-excel-admin_1782440454408.xlsx` and record the grain validation output
- L187: ### CA1 â€” Grain aggregation correctness (HIGHEST PRIORITY)
- L197: visita_value = 1 if bool(rec.get("Link Foto")) else 0
- L203: `(Fecha, Codigo Local, Marca)`, not a count of events.
- L207: (adding a new `KPIONE2_PHOTO` source), Claude must audit the integration point to confirm
- L208: it does not break the existing KPIONE2 flow or compliance calculations.
- L224: 4. Modifying `kpione2_photo_export_contract_v1.json` (active contract status)
- L227: 7. Any schema change to existing tables (`kpione2_raw`, `ruta_rutero`, etc.)
- L229: These blocks hold regardless of any chat authorization. They require a committed phase gate or
- L240: | E1 | Dry-run output matching 009F grain counts | `research/010B_route_b_claude_audit/CODEX_GRAIN_DRY_RUN.md` | `photo_rows=37908`, `distinct_event_ids=5892` (or clearly explained deviation) |
- L243: | E4 | Rollback note | PR description or dedicated file | How to undo without DB apply |
- L244: | E5 | Test output | Terminal output or CI | All grain tests passing against fixture data |
- L252: ## Q8: What rollback must exist?
- L255: KPIONE2 loading path. This guarantees rollback at zero cost.
- L257: ### Code rollback
- L258: `git revert` of the new loader script. The existing `load_kpione2` function in
- L259: `load_control_gestion_raw_v17.py` and `kpione2_raw` table remain unchanged and functional.
- L261: ### DB rollback (for future apply phase)
- L262: Before any DB apply is authorized, a rollback script must exist as a committed SQL file:
- L263: - `sql/16_kpione2_photo_raw_ddl_rollback.sql` â€” `DROP TABLE IF EXISTS cg_raw.kpione2_photo_raw`
- L266: ### Integration rollback (for loader integration phase â€” ORANGE+)
- L268: behind a flag (`--force-source` must exclude `KPIONE2_PHOTO` from `SOURCE_ORDER` unless
- L271: ### No rollback needed for dry-run or test-only work
- L272: Pure test files and the new loader script with `--dry-run` only: rollback is `git revert`.
- L282: - `scripts/load_kpione2_photo_from_excel.py` â€” new photo loader
- L283: - `sql/15_kpione2_photo_raw_ddl.sql` â€” DDL only, must have `-- NO APPLY` header
- L284: - `sql/16_kpione2_photo_raw_ddl_rollback.sql` â€” rollback DDL, must have `-- NO APPLY` header
- L285: - `tests/test_kpione2_photo_grain.py` â€” unit tests (or equivalent test path)
- L287: - `research/010B_route_b_claude_audit/CODEX_GRAIN_DRY_RUN.md`
- L290: ### MAY MODIFY (YELLOW, gated)
- L316: - `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- L347: | Integration path breaks existing KPIONE2 | ORANGE risk | Additive-only implementation; CA3 cross-audit |
- L359: "grain_chain_contractually_sound": true,
- L360: "grain_chain_proven_in_code": false,

### `research/010C_route_b_dry_run_validation/CLAUDE_POST_AUDIT.md`

Headings:
- L1: # CLAUDE POST-AUDIT — 010C Route B Dry-Run Validation
- L2: ## Phase: FAST_REFORM_010C_ROUTE_B_REVIEW_AND_DRY_RUN_VALIDATION
- L14: ## Scope
- L23: ## Files examined
- L45: ## Q1 — Does the loader preserve photo_row → event_row → day_presence?
- L74: ## Q2 — Does the loader avoid one_excel_row_equals_one_visit?
- L92: ## Q3 — Is the SQL review-only with -- NO APPLY?
- L122: ## Q4 — Any DB apply, SQL apply, DB connection, DSN, psycopg, sqlalchemy, or real writes?
- L168: ## Q5 — Was load_control_gestion_raw_v17.py touched or imported?
- L186: ## Q6 — Was the active contract touched?
- L203: ## Q7 — Do the tests cover the main risks?
- L230: ## Q8 — Is the dry-run evidence sufficient?
- L266: ## Q9 — Residual risks before merge
- L268: ### W1 — INFORMATIONAL: Governance files modified in same commit as implementation
- L283: ### W2 — MEDIUM: `source_row_number` defined in DDL but not produced by dry-run loader
- L296: ### W3 — LOW: CA3 (pipeline integration audit) still pending
- L303: ### W4 — LOW: `--apply` guard has no implementation behind it
- L309: ### W5 — LOW: R2 aggregation key not explicitly isolated in a test
- L317: ## Audit verdict
- L379: ## Summary

Matches:
- L9: **Active contract:** `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- L30: | `contracts/control_gestion/kpione2_photo_export_contract_v1.json` | Active contract |
- L34: | `scripts/load_kpione2_photo_from_excel.py` | New additive loader |
- L35: | `tests/test_kpione2_photo_grain.py` | Grain unit tests |
- L36: | `sql/15_kpione2_photo_raw_ddl.sql` | Review-only DDL |
- L37: | `sql/16_kpione2_photo_raw_ddl_rollback.sql` | Review-only rollback DDL |
- L40: | `research/010C_route_b_dry_run_validation/CODEX_ROLLBACK_NOTE.md` | Rollback note |
- L49: The three-level chain is implemented correctly in `scripts/load_kpione2_photo_from_excel.py`:
- L82: - There is no per-row `visita_value` derivation of the type found in `load_control_gestion_raw_v17.py`
- L98: `sql/15_kpione2_photo_raw_ddl.sql`:
- L105: `sql/16_kpione2_photo_raw_ddl_rollback.sql`:
- L108: -- REVIEW ROLLBACK ONLY
- L114: - `rollback.startswith("-- NO APPLY")`
- L115: - DDL contains `create table if not exists cg_raw.kpione2_photo_raw`
- L116: - Rollback contains `drop table if exists cg_raw.kpione2_photo_raw`
- L126: **Imports audit** (`load_kpione2_photo_from_excel.py` lines 1–14):
- L191: 1. `git diff main..HEAD` does not include `contracts/control_gestion/kpione2_photo_export_contract_v1.json`.
- L215: | `test_real_workbook_matches_required_010c_evidence` | Real-data grain parity | Matches 009F baseline: photo_rows=37908, event_ids=5892 |
- L219: R2 would manifest if the groupby used `(Codigo Local, Marca, Fecha)` without `ID`, collapsing
- L234: **Grain parity with 009F baseline:**
- L283: ### W2 — MEDIUM: `source_row_number` defined in DDL but not produced by dry-run loader
- L285: `sql/15_kpione2_photo_raw_ddl.sql` defines:
- L287: source_row_number bigint not null
- L289: The current loader (`analyze_photo_dataframe`) produces aggregate-level output, not per-row
- L291: will need to emit a per-row payload that includes `source_row_number` (the original Excel row
- L325: "grain_chain_preserved": true,
- L341: "cross_audit_ca1_grain_aggregation": "CLOSED",
- L345: "warnings": [
- L354: "summary": "source_row_number defined in DDL but not produced by dry-run loader; blocks DB apply if not resolved"
- L373: "verdict": "APPROVE_WITH_WARNINGS"
- L383: The grain chain is preserved in code, in tests, and in the dry-run output. The forbidden
- L392: The five warnings are all LOW or INFORMATIONAL severity. None block merge. **W2** (the
- L393: `source_row_number` gap) is the only item that must be resolved before a future DB apply

### `research/010C_route_b_dry_run_validation/CODEX_CONTRACT_IMPACT.md`

Headings:
- L1: # CODEX CONTRACT IMPACT - 010C Route B
- L3: ## Scope
- L7: ## Active Contract
- L13: ## Required Grain
- L20: ## Impact
- L28: ## Evidence Targets
- L37: ## Dry-Run Result

Matches:
- L9: - Contract file: `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- L13: ## Required Grain
- L15: - `input_grain = photo_row`
- L16: - `normalized_grain = event_row`
- L17: - `compliance_grain = day_presence`

### `research/010C_route_b_dry_run_validation/CODEX_DIFF_SUMMARY.md`

Headings:
- L1: # CODEX DIFF SUMMARY - 010C Route B
- L3: ## Created
- L14: ## Not Modified
- L21: ## Apply Status

Matches:
- L5: - `scripts/load_kpione2_photo_from_excel.py`: additive dry-run loader for KPIONE2 photo export grain validation.
- L6: - `tests/test_kpione2_photo_grain.py`: unit and real workbook tests for photo_row to event_row to day_presence behavior.
- L7: - `sql/15_kpione2_photo_raw_ddl.sql`: review-only additive DDL, guarded by `-- NO APPLY`.
- L8: - `sql/16_kpione2_photo_raw_ddl_rollback.sql`: review-only rollback DDL, guarded by `-- NO APPLY`.
- L10: - `research/010C_route_b_dry_run_validation/CODEX_ROLLBACK_NOTE.md`: rollback note.
- L17: - `contracts/control_gestion/kpione2_photo_export_contract_v1.json`

### `research/010C_route_b_dry_run_validation/CODEX_DRY_RUN_OUTPUT.json`

Matches:
- L8: "Codigo Local",
- L29: "Codigo Local",
- L93: "grain_contract_match": true,
- L107: "grain_contract": {
- L108: "compliance_grain": "day_presence",
- L110: "input_grain": "photo_row",
- L111: "normalized_grain": "event_row"
- L113: "loader_name": "load_kpione2_photo_from_excel",
- L164: "cod_rt": "Codigo Local",
- L307: "warnings": [],

### `research/010C_route_b_dry_run_validation/CODEX_ROLLBACK_NOTE.md`

Headings:
- L1: # CODEX ROLLBACK NOTE - 010C Route B
- L3: ## Current Phase Rollback
- L16: ## Future SQL Rollback
- L24: ## Productive Loader

Matches:
- L1: # CODEX ROLLBACK NOTE - 010C Route B
- L3: ## Current Phase Rollback
- L5: Current work is additive and local-file only. Rollback is a Git revert of:
- L7: - `scripts/load_kpione2_photo_from_excel.py`
- L8: - `tests/test_kpione2_photo_grain.py`
- L9: - `sql/15_kpione2_photo_raw_ddl.sql`
- L10: - `sql/16_kpione2_photo_raw_ddl_rollback.sql`
- L16: ## Future SQL Rollback
- L18: If a later RED-authorized phase applies the review DDL, rollback must use the dedicated review-only rollback file:
- L20: - `sql/16_kpione2_photo_raw_ddl_rollback.sql`
- L26: No rollback path is needed for `scripts/load_control_gestion_raw_v17.py` because this phase did not touch it.

### `research/010D_route_b_source_row_number_lock/README.md`

Headings:
- L1: # 010D Route B Source Row Number Lock
- L3: ## Purpose
- L7: ## Closed phase
- L17: ## Current Route B state
- L36: ## Source row number lock
- L51: ## RED remains blocked

Matches:
- L1: # 010D Route B Source Row Number Lock
- L5: This phase closes 010C after merge and records the main technical warning from Claude post-audit.
- L24: - review-only rollback SQL
- L32: No production loader was modified.
- L36: ## Source row number lock
- L38: Claude post-audit warning W2 is now a blocking prerequisite before real writes.
- L40: Before any future DB apply, SQL apply, loader write, production cutover or incremental real ingestion, Route B must define and validate `source_row_number`.
- L44: - `source_row_number` must identify the original Excel row position.
- L45: - `source_row_number` must be stable within the source workbook/sheet.
- L46: - `source_row_number` must support raw traceability.
- L47: - `source_row_number` must not replace event identity.
- L49: - Grain remains `photo_row -> event_row -> day_presence`.
- L56: - production cutover

### `research/010E_route_b_source_row_number/CLAUDE_POST_AUDIT.md`

Headings:
- L1: # CLAUDE POST-AUDIT — 010E Route B Source Row Number Dry-Run Patch
- L2: ## Phase: FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH
- L15: ## Scope
- L24: ## Files examined
- L44: ## Q1 — Does source_row_number correctly represent the original Excel row after the header?
- L78: ## Q2 — Is source_row_number stable within the same workbook/sheet?
- L107: ## Q3 — Does source_row_number enable photo_row → Excel row traceability?
- L136: ## Q4 — Does source_row_number NOT replace event identity?
- L159: ## Q5 — Is event identity still ID + SP Item ID?
- L177: ## Q6 — Does the grain remain photo_row → event_row → day_presence?
- L202: ## Q7 — Are photo_rows=37908 and distinct_event_ids=5892 maintained?
- L219: ## Q8 — Are db_apply=false, sql_apply=false, productive_loader_touched=false?
- L247: ## Q9 — No DB clients, DSN, real writes, or SQL apply?
- L275: ## Q10 — Do the new tests cover risk W2?
- L312: ## Q11 — Residual risks
- L314: ### R1 — INFORMATIONAL: DDL and loader are now aligned; apply path is future work
- L323: ### R2 — LOW: Full-sheet single read assumed by numbering
- L332: ### R3 — LOW: Trace manifest is only verifiable by re-running
- L339: ### R4 — INFORMATIONAL: governance files modified in same commit as implementation
- L346: ### R5 — UNCHANGED: CA3 (pipeline integration) still pending for a future ORANGE phase

Matches:
- L1: # CLAUDE POST-AUDIT — 010E Route B Source Row Number Dry-Run Patch
- L2: ## Phase: FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH
- L5: **Branch:** `lab/FAST_REFORM_010E_route_b_source_row_number_dry_run_patch`
- L9: **Active contract:** `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- L11: **Prior warning closed:** W2 — `source_row_number` defined in DDL but not produced by dry-run loader
- L31: | `contracts/control_gestion/kpione2_photo_export_contract_v1.json` | Active contract |
- L35: | `research/010D_route_b_source_row_number_lock/README.md` | 010D lock README |
- L36: | `scripts/load_kpione2_photo_from_excel.py` | Modified additive loader |
- L37: | `tests/test_kpione2_photo_grain.py` | Modified grain tests |
- L38: | `research/010E_route_b_source_row_number/CODEX_DRY_RUN_OUTPUT.json` | 010E dry-run evidence |
- L39: | `research/010E_route_b_source_row_number/CODEX_SOURCE_ROW_NUMBER_NOTE.md` | Source row number design note |
- L40: | `research/010E_route_b_source_row_number/CODEX_DIFF_SUMMARY.md` | Diff summary |
- L44: ## Q1 — Does source_row_number correctly represent the original Excel row after the header?
- L54: `assign_excel_source_row_numbers()` (lines 246–252) assigns a contiguous range starting at
- L57: numbered["_source_row_number"] = pd.RangeIndex(
- L69: | `source_row_number_min` | 2 | First data row in the Excel worksheet |
- L70: | `source_row_number_max` | 37909 | Last data row (37908 photo rows + 1 header offset) |
- L74: The arithmetic is correct: `source_row_number_max = photo_rows + EXCEL_HEADER_ROW_NUMBER = 37908 + 1 = 37909`. ✓
- L78: ## Q2 — Is source_row_number stable within the same workbook/sheet?
- L82: `assign_excel_source_row_numbers()` derives row numbers from `pd.RangeIndex` applied to the
- L86: Test `test_source_row_number_is_stable_within_workbook_sheet` (lines 107–116) explicitly
- L107: ## Q3 — Does source_row_number enable photo_row → Excel row traceability?
- L113: "mapping_cardinality": "one_photo_row_to_one_source_row_number",
- L120: (source_row_number, event_id, sp_item_id, photo_row_hash)
- L125: - Row 2: `event_id=883726`, `source_row_number=2`
- L126: - Row 37909: `event_id=877678`, `source_row_number=37909`
- L131: The flag `source_row_number_unique=true` confirms no two photo rows share the same number —
- L136: ## Q4 — Does source_row_number NOT replace event identity?
- L147: - `EVENT_STABLE_KEYS` (lines 74–86) is identical to the 010C version; `source_row_number`
- L150: `first_source_row_number` and `last_source_row_number` as provenance spans per event —
- L153: completely independent of `source_row_number`.
- L155: `source_row_number` flows into `photo_row_traceability` only. It is provenance metadata.
- L171: `kpione2_photo_export_contract_v1.json`. No change.
- L177: ## Q6 — Does the grain remain photo_row → event_row → day_presence?
- L179: **CONFIRMED — grain chain is fully preserved.**
- L181: `GRAIN_CONTRACT` constant (lines 30–35) is unchanged:
- L183: GRAIN_CONTRACT = {
- L184: "input_grain": "photo_row",
- L185: "normalized_grain": "event_row",
- L186: "compliance_grain": "day_presence",
- L214: The `source_row_number` patch introduces only provenance metadata. It does not filter, dedup,
- L215: or re-aggregate photo rows. All 18 original blocking flags from 010C remain true.
- L262: | `assign_excel_source_row_numbers()` | 246–252 | None — pure `pd.RangeIndex` assignment |
- L279: W2 was: *"`source_row_number` defined in DDL but not produced by dry-run loader; blocks DB
- L288: | `test_source_row_number_maps_each_photo_row_to_excel_origin` | 6-row fixture → source_row_numbers 2–7; all 4 new flags true; `event_identity=["ID","SP Item ID"]`; `event_identity_replaced=False` |
- L289: | `test_source_row_number_is_stable_within_workbook_sheet` | Scrambled pandas index produces identical `trace_manifest_sha256` and `sample_rows` |
- L295: | `test_cli_writes_json_without_db` | `source_row_number_min=2`, `source_row_number_max=7`, `photo_rows_mapped=6` |
- L296: | `test_real_workbook_matches_required_010c_evidence` | `source_row_number_min=2`, `source_row_number_max=37909`, `source_row_number_distinct=37908`, `source_row_number_null_rows=0`, `photo_rows_mapped=37908`, `event_identity_replaced=False` |
- L300: "source_row_number_present",
- L301: "source_row_number_complete",
- L302: "source_row_number_unique",
- L303: "source_row_number_matches_excel_rows",
- L316: `sql/15_kpione2_photo_raw_ddl.sql` defines `source_row_number bigint not null` with a
- L317: uniqueness constraint `(source_file_sha256, source_sheet, source_row_number)`. The loader now
- L318: produces `source_row_number` values that match this schema. The DDL and loader are aligned.
- L325: `assign_excel_source_row_numbers()` assigns row numbers based on the length of the DataFrame
- L357: | `source_row_number` defined in DDL | YES (sql/15_*) | YES (unchanged) |
- L358: | `source_row_number` produced by loader | NO (W2) | **CLOSED — YES** |
- L361: | DB apply gate still required | YES | YES (unchanged) |
- L369: "audit_phase": "FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH",
- L372: "prior_warning_w2_closed": true,
- L375: "source_row_number_represents_excel_row_after_header": true,
- L376: "source_row_number_stable_within_workbook_sheet": true,
- L377: "source_row_number_enables_traceability": true,
- L378: "source_row_number_does_not_replace_event_identity": true,
- L380: "grain_remains_photo_row_event_row_day_presence": true,
- L429: The 010E patch is minimal, correct, and closes the only medium-severity warning (W2) left
- L432: `source_row_number` is correctly defined as the 1-based Excel worksheet row after header
- L434: (37,908 of 37,908). It does not participate in event identity. The event identity, grain

### `research/010E_route_b_source_row_number/CODEX_DIFF_SUMMARY.md`

Headings:
- L1: # 010E Route B Diff Summary
- L3: ## Modified
- L14: ## Created
- L20: ## Explicitly Not Modified
- L28: ## Safety

Matches:
- L5: - `scripts/load_kpione2_photo_from_excel.py`
- L6: - assigns 1-based Excel `source_row_number` values after sheet read
- L10: - `tests/test_kpione2_photo_grain.py`
- L16: - `research/010E_route_b_source_row_number/CODEX_DRY_RUN_OUTPUT.json`
- L17: - `research/010E_route_b_source_row_number/CODEX_SOURCE_ROW_NUMBER_NOTE.md`
- L18: - `research/010E_route_b_source_row_number/CODEX_DIFF_SUMMARY.md`
- L23: - `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- L25: - SQL DDL and rollback files

### `research/010E_route_b_source_row_number/CODEX_DRY_RUN_OUTPUT.json`

Matches:
- L8: "Codigo Local",
- L29: "Codigo Local",
- L41: "_source_row_number"
- L94: "grain_contract_match": true,
- L105: "source_row_number_complete": true,
- L106: "source_row_number_matches_excel_rows": true,
- L107: "source_row_number_present": true,
- L108: "source_row_number_unique": true,
- L112: "grain_contract": {
- L113: "compliance_grain": "day_presence",
- L115: "input_grain": "photo_row",
- L116: "normalized_grain": "event_row"
- L118: "loader_name": "load_kpione2_photo_from_excel",
- L143: "source_row_number_distinct": 37908,
- L144: "source_row_number_max": 37909,
- L145: "source_row_number_min": 2,
- L146: "source_row_number_null_rows": 0,
- L167: "source_row_number": "one_based_excel_worksheet_row_after_header_resolution"
- L169: "phase": "FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH",
- L178: "mapping_cardinality": "one_photo_row_to_one_source_row_number",
- L184: "source_row_number": 2,
- L190: "source_row_number": 3,
- L196: "source_row_number": 4,
- L202: "source_row_number": 37907,
- L208: "source_row_number": 37908,
- L214: "source_row_number": 37909,
- L218: "source_row_number_field": "source_row_number",
- L220: "trace_manifest_algorithm": "sha256(source_row_number,event_id,sp_item_id,photo_row_hash)",
- L227: "cod_rt": "Codigo Local",
- L252: "first_source_row_number": 37905,
- L254: "last_source_row_number": 37909,
- L278: "first_source_row_number": 37900,
- L280: "last_source_row_number": 37904,
- L304: "first_source_row_number": 37889,
- L306: "last_source_row_number": 37899,
- L330: "first_source_row_number": 37865,
- L332: "last_source_row_number": 37888,
- L357: "first_source_row_number": 37857,
- L359: "last_source_row_number": 37864,
- L380: "warnings": [],

### `research/010E_route_b_source_row_number/CODEX_SOURCE_ROW_NUMBER_NOTE.md`

Headings:
- L1: # 010E Route B Source Row Number Note
- L3: ## Scope
- L8: ## Definition
- L21: ## Traceability Evidence
- L39: ## Contract Impact
- L52: ## Risk Classification
- L63: ## Rollback

Matches:
- L1: # 010E Route B Source Row Number Note
- L10: `source_row_number` is the 1-based physical row number in the source Excel worksheet after
- L26: - `source_row_number_distinct = 37908`
- L27: - `source_row_number_null_rows = 0`
- L28: - `source_row_number_min = 2`
- L29: - `source_row_number_max = 37909`
- L31: - `source_row_number_matches_excel_rows = true`
- L35: `source_row_number, event_id, sp_item_id, photo_row_hash`
- L43: - input grain remains `photo_row`
- L44: - normalized grain remains `event_row`
- L45: - compliance grain remains `day_presence`
- L48: - `source_row_number` is provenance metadata and does not participate in event identity
- L63: ## Rollback
- L65: Rollback is local and additive:
- L67: 1. Revert the 010E changes to `scripts/load_kpione2_photo_from_excel.py`.
- L71: No DB or SQL rollback is required because `db_apply=false`, `sql_apply=false`, and no real write

### `research/010F_route_b_source_row_number_post_audit_close/README.md`

Headings:
- L1: # 010F Route B Source Row Number Post-Audit Close
- L3: ## Purpose
- L7: ## Closed phase
- L17: ## Audit result
- L38: ## Route B state after 010F
- L50: ## Remaining gates

Matches:
- L1: # 010F Route B Source Row Number Post-Audit Close
- L5: Close the 010E source_row_number dry-run patch after merge to main.
- L9: `FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH`
- L21: `research/010E_route_b_source_row_number/CLAUDE_POST_AUDIT.md`
- L30: - `source_row_number` is produced by dry-run loader.
- L34: - Grain remains `photo_row -> event_row -> day_presence`.
- L46: No production loader was modified.
- L50: ## Remaining gates
- L52: Future integration into production pipeline is ORANGE.
- L54: Any DB apply, SQL apply against Supabase real or production cutover remains RED and requires explicit Bastián authorization.

### `research/010G_route_b_integration_planning_no_apply/README.md`

Headings:
- L1: # 010G Route B Integration Planning — No Apply
- L3: ## Purpose
- L9: ## Current state
- L19: ## Scope
- L38: ## Output files

Matches:
- L5: Plan the future integration of Route B into the CONTROL_GESTIÓN production flow.
- L13: - `source_row_number` satisfied in dry-run.
- L25: - document validation gates
- L32: - production loader modification
- L36: - production cutover
- L42: - `VALIDATION_GATES.md`

### `research/010G_route_b_integration_planning_no_apply/RISK_MATRIX.md`

Headings:
- L1: # Route B Integration Risk Matrix

Matches:
- L6: | R2 | Losing Excel traceability | HIGH | Controlled in dry-run | Keep `source_row_number`; enforce uniqueness per file/sheet |
- L10: | R6 | Writing duplicate raw rows | HIGH | Future risk | Require unique `(source_file_sha256, source_sheet, source_row_number)` |
- L12: | R8 | Inconsistent forward-only vs backfill scope | MEDIUM | Open | Define scope before any apply |
- L14: | R10 | Rollback not ready before apply | HIGH | Future risk | Rollback plan required before RED apply |

### `research/010G_route_b_integration_planning_no_apply/ROUTE_B_INTEGRATION_PLAN.md`

Headings:
- L1: # Route B Integration Plan — No Apply
- L3: ## Integration principle
- L19: ## Non-goals for this phase
- L29: ## Future integration stages
- L31: ### Stage 1 — Adapter design
- L50: ### Stage 2 — Test-only adapter
- L63: ### Stage 3 — ORANGE production loader review
- L69: ### Stage 4 — RED apply planning
- L75: ## Future decision points

Matches:
- L5: Route B must enter production through a controlled adapter layer, not by silently replacing existing productive logic.
- L11: The production chain must preserve:
- L13: - input grain: `photo_row`
- L14: - normalized grain: `event_row`
- L15: - compliance grain: `day_presence`
- L17: - provenance: `source_row_number`
- L39: - source row number
- L60: - source_row_number uniqueness remains stable
- L61: - event/day aggregates remain unchanged
- L63: ### Stage 3 — ORANGE production loader review
- L79: 3. Decide rollback mechanism before any write.
- L80: 4. Decide whether historical backfill is allowed or forward-only.
- L81: 5. Decide production cutover criteria.

### `research/010G_route_b_integration_planning_no_apply/VALIDATION_GATES.md`

Headings:
- L1: # Route B Validation Gates
- L3: ## GREEN gates
- L13: ## ORANGE gates
- L32: ## RED gates
- L44: ## Minimum pre-apply checklist

Matches:
- L1: # Route B Validation Gates
- L3: ## GREEN gates
- L5: Allowed without production change:
- L13: ## ORANGE gates
- L20: - creating adapter code intended for production path
- L27: - rollback path
- L32: ## RED gates
- L39: - production cutover
- L48: - source_row_number present
- L49: - source_row_number unique per source file/sheet
- L51: - grain unchanged
- L54: - rollback SQL reviewed

### `research/010H_route_b_integration_plan_cross_audit/CLAUDE_CROSS_AUDIT.md`

Headings:
- L1: # CLAUDE CROSS-AUDIT — 010G Route B Integration Plan
- L2: ## Phase: FAST_REFORM_010H_ROUTE_B_INTEGRATION_PLAN_CROSS_AUDIT
- L15: ## Scope and method
- L42: ## Q1 — Does the 010G plan respect the KPIONE2 photo export contract?
- L66: ## Q2 — Does it preserve the grain photo_row → event_row → day_presence?
- L82: ## Q3 — Does it preserve event identity ID + SP Item ID?
- L93: ## Q4 — Is source_row_number correctly protected as traceability, not identity?
- L113: ## Q5 — Are the risks sufficient?
- L136: ## Q6 — Are the GREEN / ORANGE / RED gates well separated?
- L155: ## Q7 — Is touching load_control_gestion_raw_v17.py correctly forbidden in this stage?
- L170: ## Q8 — Are DB apply and SQL apply correctly forbidden?
- L185: ## Q9 — Does the plan clearly distinguish local test-only adapter vs future ORANGE integration vs future RED apply?
- L205: ## Q10 — Is any gate missing before a future implementation phase?
- L211: ### G-MISSING-1 (MEDIUM) — Window / batch-set declaration gate
- L219: ### G-MISSING-2 (HIGH) — Compliance-denominator reconciliation gate
- L230: ### G-MISSING-3 (LOW–MEDIUM) — Re-ingestion / idempotency policy gate
- L237: ### Adequately covered (no gate needed)
- L245: ## Q11 — Residual risks
- L262: ## Cross-audit verdict
- L310: ## Summary and recommendation

Matches:
- L7: **Active contract:** `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- L10: - `research/010E_route_b_source_row_number/CLAUDE_POST_AUDIT.md` (verdict APPROVE, W2 closed)
- L17: This is a **planning-phase cross-audit**. 010G produced no code and no production change — it
- L19: (per `AGENT_AUTHORITY_MATRIX_V2` ORANGE rule and the 010G→010H gate) is to confirm the plan is
- L20: contract-faithful and that the gate structure protecting future implementation is complete
- L31: research/010G_route_b_integration_planning_no_apply/VALIDATION_GATES.md
- L33: No production file was touched. Confirmed via last-touch commits:
- L35: - `contracts/control_gestion/kpione2_photo_export_contract_v1.json` → last touched `7cfba7a` (009F; **not** in 010G)
- L36: - `scripts/load_kpione2_photo_from_excel.py` → last touched `cd33e66` (010E; **not** in 010G)
- L42: ## Q1 — Does the 010G plan respect the KPIONE2 photo export contract?
- L46: `ROUTE_B_INTEGRATION_PLAN.md` (lines 11–18) declares the production chain must preserve exactly
- L49: | Contract element (`kpione2_photo_export_contract_v1.json`) | Plan statement |
- L51: | `grain_contract.input_grain = photo_row` | "input grain: `photo_row`" |
- L52: | `grain_contract.normalized_grain = event_row` | "normalized grain: `event_row`" |
- L53: | `grain_contract.compliance_grain = day_presence` | "compliance grain: `day_presence`" |
- L55: | (010E provenance addition) | "provenance: `source_row_number`" |
- L66: ## Q2 — Does it preserve the grain photo_row → event_row → day_presence?
- L73: `VALIDATION_GATES.md` (lines 50–51) requires "grain unchanged" and "day_presence count validated"
- L76: The plan does not propose any alternative grain or any short-circuit from photo_row directly to
- L77: day_presence. Stage 2 (test-only adapter) requires "event/day aggregates remain unchanged"
- L78: (line 62). The grain is protected at plan, risk, and gate level.
- L93: ## Q4 — Is source_row_number correctly protected as traceability, not identity?
- L97: The plan lists `source_row_number` under **"provenance"** (line 17), held separate from the
- L99: `source row number`, `event identity`, `photo row hash`, and `stable event hash` as **distinct**
- L103: - R2 (HIGH): "Keep `source_row_number`; enforce uniqueness per file/sheet" — treats it as a
- L105: - R6 (HIGH): "Require unique `(source_file_sha256, source_sheet, source_row_number)`" — this is
- L108: This is fully consistent with the 010E post-audit finding that `source_row_number` is provenance
- L119: productive-loader change (R4), premature SQL apply (R5), duplicate raw rows (R6), productive view
- L120: regression (R9), and rollback-not-ready (R10). R7 (chunked-read offset) directly continues the
- L121: 010E residual risk R2 — good continuity. R8 (forward-only vs backfill scope) is a real
- L126: - No risk for **compliance-denominator reconciliation** when Route B `day_presence` begins
- L128: - No risk for **re-ingestion / idempotency** behavior on duplicate file load (R6 names the
- L136: ## Q6 — Are the GREEN / ORANGE / RED gates well separated?
- L140: `VALIDATION_GATES.md` maps cleanly onto the active authority model:
- L142: | Gate | Plan content | Matrix alignment |
- L145: | ORANGE | modify `load_control_gestion_raw_v17.py`; change productive ingestion; change compliance logic; adapter code for production path; change compliance-defining outputs | Matches `ORANGE.allowed_only_after_cross_audit` (loader logic, compliance logic, c
- L146: | RED | DB apply; SQL apply real; data movement; production cutover; destructive cleanup; secret change; real incremental ingestion | Matches `RED.blocked_without_bastian` exactly |
- L148: The ORANGE gate lists the required evidence (contract impact note, diff summary, rollback path,
- L163: + `VALIDATION_GATES.md` ORANGE gate (first item)
- L175: of the order lock and phase lock, in the RED gate of `VALIDATION_GATES.md`, and as R5 (HIGH, "RED
- L181: rollback for RED actions." Correctly forbidden.
- L194: | Stage 2 — Test-only adapter | GREEN/YELLOW | "No DB writes"; transform payload → candidate rows in-memory; assert counts/uniqueness/aggregates stable |
- L195: | Stage 3 — ORANGE production loader review | ORANGE (cross-audit required) | Decide whether v17 loader is patched or Route B stays separate; "touches productive ingestion logic" |
- L199: test-only adapter (Stage 2) is explicitly walled from the production path (Stage 3) and from any
- L201: consistent across plan, phase lock, and gates document.
- L205: ## Q10 — Is any gate missing before a future implementation phase?
- L207: **YES — three gates should be added before an implementation phase is authorized.** None block
- L211: ### G-MISSING-1 (MEDIUM) — Window / batch-set declaration gate
- L216: exports has no temporal contract. **A "declared window + batch set" gate must be added to the
- L219: ### G-MISSING-2 (HIGH) — Compliance-denominator reconciliation gate
- L220: This is the highest-impact omission. The contract's `denominator_mapping`
- L221: (`Codigo Local → cod_rt`, `Marca → cliente_norm`) ties Route B `day_presence` to the route
- L222: compliance denominator. When Route B `day_presence` eventually **feeds** compliance, there is no
- L223: gate requiring that the new presence signal **reconciles with the existing KPIONE2 visit-based
- L224: compliance** (parity, or an explicit, authorized delta) before cutover. The plan's decision point
- L226: **numeric reconciliation gate**. **Add a gate: "Route B day_presence reconciled against existing
- L227: compliance denominator with signed parity evidence" before any compliance view consumes Route B
- L230: ### G-MISSING-3 (LOW–MEDIUM) — Re-ingestion / idempotency policy gate
- L231: R6 names the unique constraint `(source_file_sha256, source_sheet, source_row_number)` but neither
- L232: the plan nor the gates specify the **loader's behavior on conflict** (reject / skip / upsert) when
- L234: rather than a defined idempotent outcome. **Add a gate defining the re-ingestion policy and
- L237: ### Adequately covered (no gate needed)
- L238: - Rollback-before-apply: covered (R10 + checklist "rollback SQL reviewed").
- L249: | RR1 | Compliance reconciliation gate absent (G-MISSING-2) | HIGH | Must exist before Route B feeds any compliance view; biggest pre-implementation gap |
- L252: | RR4 | Forward-only vs backfill scope still Open (R8) | MEDIUM | Plan flags it as a decision point but no gate forces resolution before apply |
- L253: | RR5 | Multi-file `day_presence` accumulation not addressed | MEDIUM | `source_row_number` resets per file (correctly), but cross-file presence aggregation for compliance is unspecified; tied to RR1/RR2 |
- L271: "production_loader_touched": false,
- L277: "grain_preserved_photo_event_day": true,
- L279: "source_row_number_is_traceability_not_identity": true,
- L281: "green_orange_red_gates_well_separated": true,
- L285: "missing_gates_before_implementation": [
- L289: "summary": "Window / batch-set declaration gate (CLAUDE.md temporal-contract hard rule)"
- L294: "summary": "Compliance-denominator reconciliation gate before Route B day_presence feeds compliance"
- L299: "summary": "Re-ingestion / idempotency policy gate verified in Stage 2 harness"
- L304: "verdict": "APPROVE_WITH_WARNINGS"
- L312: The 010G plan is contract-faithful and structurally sound. It preserves the grain
- L314: provenance role of `source_row_number` (traceability, never identity). The GREEN/ORANGE/RED gate
- L315: separation is clean and matches `AGENT_AUTHORITY_MATRIX_V2`. Touching the production loader and
- L320: **Verdict: APPROVE_WITH_WARNINGS.** The planning phase is correctly merged and safe. Before any
- L321: implementation phase (Stage 2 and beyond) is authorized, three gates must be added to the

### `research/012_route_b_readiness_gates_no_apply/README.md`

Headings:
- L1: # FAST_REFORM_012_ROUTE_B_READINESS_GATES_NO_APPLY
- L3: ## Purpose
- L7: ## Unit of value
- L11: ## Required gates
- L17: ## Non-scope
- L27: ## KERNEL alignment
- L31: ## Opened from

Matches:
- L1: # FAST_REFORM_012_ROUTE_B_READINESS_GATES_NO_APPLY
- L5: Convert Route B unresolved warnings into readiness gates before any implementation that touches productive runtime, active contracts, loaders, SQL apply, Supabase writes or data movement.
- L9: Route B receives a readiness verdict: GO, NO_GO or GO_WITH_LIMITS.
- L11: ## Required gates
- L13: 1. Compliance denominator reconciliation.
- L14: 2. Forward-only/backfill scope decision.
- L15: 3. Rollback and production cutover gate.
- L24: - No backfill execution.
- L25: - No production cutover.
- L34: - Branch: `lab/FAST_REFORM_012_route_b_readiness_gates_no_apply`

### `scripts/cg_route_weekly_local_lab.py`

Headings:
- L1: #!/usr/bin/env python3
- L2: # -*- coding: utf-8 -*-

Matches:
- L50: ROLLBACK_CONFIRM_TOKEN = "ROUTE_WEEK_ROLLBACK_V1"
- L110: "## Gates",
- L297: rollback_matches = list(re.finditer(r"(?im)^\s*rollback\s*;\s*$", text))
- L298: if not begin_match or not rollback_matches:
- L299: raise LabError("sql11_missing_begin_rollback_wrapper")
- L300: rollback_match = rollback_matches[-1]
- L301: if begin_match.end() >= rollback_match.start():
- L303: body = text[begin_match.end() : rollback_match.start()].strip() + "\n"
- L309: "begin_rollback_wrapper": True,
- L354: conn.rollback()
- L381: conn_a.rollback()
- L384: conn_b.rollback()
- L399: conn_d.rollback()
- L400: conn_c.rollback()
- L439: "exact_duplicate_excess": int(plan["exact_duplicate_excess"]),
- L440: "grain_duplicate_groups": int(plan["grain_duplicate_groups"]),
- L486: def run_loader_rollback(loader, dsn: str, failed_assignment_id: int, expected_current_surface_hash: str, temp_dir: Path, label: str) -> dict:
- L487: json_out = temp_dir / f"{label}_rollback.json"
- L493: "--rollback-weekly-replacement",
- L498: "--confirm-rollback",
- L499: loader.ROLLBACK_CONFIRM_TOKEN,
- L514: raise LabError(f"{label}_loader_rollback_failed", detail) from exc
- L516: if result.get("mode") != "rollback" or not result.get("writes_executed"):
- L517: raise LabError("loader_rollback_failed")
- L562: def batch_grains(dsn: str, batch_id: int) -> set[tuple[str, str]]:
- L586: def resolved_grain_diff(dsn: str, batch_id: int) -> dict:
- L587: assigned = batch_grains(dsn, batch_id)
- L659: removal_grains = {
- L677: for day in ("LUNES", "MARTES", "MIERCOLES", "JUEVES", "VIERNES", "SABADO", "DOMINGO"):
- L703: "DOMINGO": 0,
- L704: "VISITA MENSUAL": 0,
- L729: synthetic_grain = ("LAB_ONLY_RT_20260608", "LAB_ONLY_CLIENTE")
- L733: "removed_logical_grains": len(removal_grains),
- L740: "removal_grains_count": len(removal_grains),
- L741: "synthetic_grain_marker_hash": hashlib.sha256("|".join(synthetic_grain).encode("utf-8")).hexdigest().upper(),
- L743: profile["_removal_grains_internal"] = removal_grains
- L744: profile["_synthetic_grain_internal"] = synthetic_grain
- L750: diff = resolved_grain_diff(dsn, batch_id)
- L767: "exact_duplicate_excess": int(apply_result["exact_duplicate_excess"]),
- L773: "grain_diff": diff,
- L781: diff = resolved_grain_diff(dsn, batch_id)
- L782: grains = batch_grains(dsn, batch_id)
- L783: removed = snapshot_profile["_removal_grains_internal"]
- L784: synthetic = snapshot_profile["_synthetic_grain_internal"]
- L802: count(*) filter (where veces_por_semana = 1 and lunes = 1 and martes = 0 and miercoles = 0 and jueves = 0 and viernes = 0 and sabado = 0 and domingo = 0)::bigint
- L813: and len(removed - grains) == snapshot_profile["removed_logical_grains"]
- L814: and synthetic in grains
- L838: "grain_diff": diff,
- L842: def validate_rollback(dsn: str, rollback_result: dict, snapshot_a: dict, snapshot_b: dict) -> dict:
- L844: diff = resolved_grain_diff(dsn, snapshot_a["batch_id"])
- L861: and rollback_result["restored_current_surface_hash"].upper() == snapshot_a["current_surface_hash"].upper()
- L865: raise LabError("rollback_validation_failed")
- L868: "rollback_restored_a": True,
- L871: "reactivated_assignment_id": int(rollback_result["reactivated_assignment_id"]),
- L872: "restored_ruta_batch_id": int(rollback_result["restored_ruta_batch_id"]),
- L873: "current_surface_hash": rollback_result["restored_current_surface_hash"],
- L874: "grain_diff": diff,
- L879: original = loader.run_rollback_postcheck
- L886: loader.run_rollback_postcheck = fail_postcheck
- L891: loader.run_weekly_replacement_rollback(
- L897: confirm_token=ROLLBACK_CONFIRM_TOKEN,
- L903: loader.run_rollback_postcheck = original
- L920: "rollback_executed": True,
- L943: rollback_result = run_loader_rollback(
- L951: rollback = validate_rollback(dsn, rollback_result, snapshot_a, snapshot_b)
- L977: "exact_duplicate_excess": snapshot_a["exact_duplicate_excess"],
- L980: "grain_diff": snapshot_a["grain_diff"],
- L995: "grain_diff": snapshot_b["grain_diff"],
- L1002: "rollback_restored_a": rollback["rollback_restored_a"],
- L1003: "postchecks_before_commit": rollback["postchecks_before_commit"],
- L1005: "rollback": rollback,
- L1015: "b_removed_logical_grains": safe_b_profile["removed_logical_grains"],
- L1017: "rollback_restored_a": rollback["rollback_restored_a"],
- L1037: "input_rows": lab_summary["cg005j"]["snapshot_a_rows"] + lab_summary["cg005j"]["exact_duplicate_excess"],
- L1040: "exact_duplicate_rows": lab_summary["cg005j"]["exact_duplicate_excess"],
- L1041: "grain_duplicate_rows": 0,
- L1056: "rollback_required": True,
- L1057: "rollback_executed": True,
- L1059: "removed_key_count": lab_summary["cg005k"]["snapshot_b"]["removed_logical_grains"],
- L1164: "warnings": [],

### `scripts/load_control_gestion_raw_v17.py`

Headings:
- L1: # -*- coding: utf-8 -*-

Matches:
- L20: SOURCE_ORDER = ["KPIONE", "KPIONE2", "POWER_APP"]
- L23: "KPIONE2": "DB (KPIONE2.0)",
- L24: "POWER_APP": "DB (POWER_APP)",
- L28: "KPIONE2": ["Codigo Local", "Marca", "Reponedor", "Fecha", "VISITA"],
- L30: KPIONE2_NUMERIC_COLUMNS = ["VISITA"]
- L198: def calc_week_iso(fecha_visita: date | None) -> int | None:
- L199: if fecha_visita is None:
- L201: return int(fecha_visita.isocalendar().week)
- L242: warnings: list[str],
- L246: payload["warnings"] = warnings
- L250: elif warnings:
- L266: warnings: list[str],
- L268: mixed_type_warning_key: str | None = None,
- L304: if mixed_type_warning_key and len(raw_types) > 1:
- L305: warnings.append(mixed_type_warning_key)
- L311: warnings.append(issue)
- L318: "sheet_scope": ["DB (KPIONE)", "DB (KPIONE2.0)", "DB (POWER_APP)"],
- L321: "warnings": [],
- L328: warnings: list[str] = []
- L334: return finalize_source_check(payload, blockers, warnings, notes)
- L341: return finalize_source_check(payload, blockers, warnings, notes)
- L345: "DB (KPIONE2.0)": ["Codigo Local", "Marca", "Reponedor", "Fecha", "VISITA"],
- L347: for sheet in ("DB (KPIONE)", "DB (KPIONE2.0)", "DB (POWER_APP)"):
- L353: return finalize_source_check(payload, blockers, warnings, notes)
- L371: warnings=warnings,
- L382: warnings=warnings,
- L384: notes.append("DB (KPIONE): SEMANA and VISITAS SEMANA are not date fields")
- L390: kpione2_df = read_excel_sheet(workbook_path, "DB (KPIONE2.0)", dtype=str)
- L391: kpione2_df.columns = [str(c).strip() for c in kpione2_df.columns]
- L392: payload["rows_checked"]["DB (KPIONE2.0)"] = int(len(kpione2_df))
- L393: payload["numeric_empty_to_null_count"]["DB (KPIONE2.0)"] = numeric_empty_to_null_counts(
- L394: kpione2_df,
- L395: KPIONE2_NUMERIC_COLUMNS,
- L397: missing = [c for c in required_sheets["DB (KPIONE2.0)"] if c not in kpione2_df.columns]
- L399: blockers.append("missing_critical_columns:DB (KPIONE2.0):" + ",".join(missing))
- L403: key="DB (KPIONE2.0).Fecha",
- L404: series=kpione2_df["Fecha"],
- L409: warnings=warnings,
- L411: mixed_type_warning_key="mixed_date_types:DB (KPIONE2.0).Fecha",
- L413: notes.append("DB (KPIONE2.0): SEMANA is not a date field")
- L415: blockers.append(f"sheet_read_error:DB (KPIONE2.0):{type(exc).__name__}")
- L419: power_raw = read_excel_sheet(workbook_path, "DB (POWER_APP)", header=None, dtype=str)
- L421: payload["rows_checked"]["DB (POWER_APP)"] = max(int(len(power_raw) - 1), 0)
- L425: warnings.append("power_app_unnamed_headers_present")
- L427: warnings.append("power_app_numeric_headers_present")
- L429: power_df = read_power_app_sheet(workbook_path)
- L439: blockers.append("missing_critical_columns:DB (POWER_APP):" + ",".join(missing))
- L443: key="DB (POWER_APP).Creado",
- L449: warnings=warnings,
- L454: key="DB (POWER_APP).FECHA",
- L460: warnings=warnings,
- L462: notes.append("DB (POWER_APP): SEM/SEMANA are not date fields")
- L464: blockers.append(f"sheet_read_error:DB (POWER_APP):{type(exc).__name__}")
- L468: return finalize_source_check(payload, blockers, warnings, notes)
- L480: def read_kpione2_df(excel_path: Path) -> pd.DataFrame:
- L481: df = read_excel_sheet(excel_path, SOURCE_TO_SHEET["KPIONE2"], dtype=str)
- L483: missing = [c for c in SOURCE_REQUIRED_COLUMNS["KPIONE2"] if c not in df.columns]
- L485: raise ValueError(f"KPIONE2 missing required columns: {missing}")
- L491: warnings: list[str] = []
- L502: warnings=warnings,
- L513: warnings=warnings,
- L515: elif source_key == "KPIONE2":
- L518: key="DB (KPIONE2.0).Fecha",
- L524: warnings=warnings,
- L526: mixed_type_warning_key="mixed_date_types:DB (KPIONE2.0).Fecha",
- L528: elif source_key == "POWER_APP":
- L531: key="DB (POWER_APP).Creado",
- L537: warnings=warnings,
- L542: key="DB (POWER_APP).FECHA",
- L548: warnings=warnings,
- L550: return payload["date_ranges"], warnings
- L556: if source_key == "KPIONE2":
- L558: elif source_key == "POWER_APP":
- L576: warnings: list[str] = []
- L578: warnings.append(f"incremental_date_parse_errors:{source_key}:{driver_column}:{parse_errors}")
- L579: return sorted(dates), warnings
- L585: if source_key == "KPIONE2":
- L586: return read_kpione2_df(excel_path)
- L587: if source_key == "POWER_APP":
- L588: return read_power_app_sheet(excel_path)

### `scripts/load_kpione2_photo_from_excel.py`

Headings:
- L1: # scripts/load_kpione2_photo_from_excel.py
- L2: # -*- coding: utf-8 -*-

Matches:
- L1: # scripts/load_kpione2_photo_from_excel.py
- L19: LOADER_NAME = "load_kpione2_photo_from_excel"
- L20: PHASE = "FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH"
- L21: LOCAL_TZ = ZoneInfo("America/Santiago")
- L25: DEFAULT_CONTRACT = Path("contracts/control_gestion/kpione2_photo_export_contract_v1.json")
- L30: GRAIN_CONTRACT = {
- L31: "input_grain": "photo_row",
- L32: "normalized_grain": "event_row",
- L33: "compliance_grain": "day_presence",
- L42: "cod_rt": ["codigo local"],
- L97: "grain_contract_match",
- L115: "source_row_number_present",
- L116: "source_row_number_complete",
- L117: "source_row_number_unique",
- L118: "source_row_number_matches_excel_rows",
- L246: def assign_excel_source_row_numbers(df: pd.DataFrame) -> pd.DataFrame:
- L248: numbered["_source_row_number"] = pd.RangeIndex(
- L258: "_source_row_number",
- L263: for source_row_number, event_id, sp_item_id, photo_row_hash in df[trace_columns].itertuples(
- L268: int(source_row_number),
- L280: ["_source_row_number", "_event_id", "_sp_item_id", "_photo_row_hash"]
- L283: "_source_row_number": "source_row_number",
- L293: record["source_row_number"] = int(record["source_row_number"])
- L316: df = assign_excel_source_row_numbers(df)
- L335: "grain_contract": GRAIN_CONTRACT,
- L341: "warnings": [],
- L344: contract_grain = contract.get("grain_contract") or {}
- L345: if contract_grain != GRAIN_CONTRACT:
- L346: base_payload["errors"].append("grain_contract_mismatch")
- L351: "grain_contract_match": contract_grain == GRAIN_CONTRACT,
- L376: source_row_numbers = df["_source_row_number"]
- L377: source_row_number_null_rows = int(source_row_numbers.isna().sum())
- L378: source_row_number_distinct = int(source_row_numbers.nunique(dropna=True))
- L379: source_row_number_min = int(source_row_numbers.min()) if photo_rows else None
- L380: source_row_number_max = int(source_row_numbers.max()) if photo_rows else None
- L381: expected_source_row_numbers = pd.Series(
- L388: source_row_number_matches_excel_rows = source_row_numbers.reset_index(drop=True).equals(
- L389: expected_source_row_numbers
- L396: valid_events[["_event_id", "_sp_item_id"]].drop_duplicates().shape[0]
- L418: first_source_row_number=("_source_row_number", "min"),
- L419: last_source_row_number=("_source_row_number", "max"),
- L488: "source_row_number_null_rows": source_row_number_null_rows,
- L489: "source_row_number_distinct": source_row_number_distinct,
- L490: "source_row_number_min": source_row_number_min,
- L491: "source_row_number_max": source_row_number_max,
- L495: "grain_contract_match": contract_grain == GRAIN_CONTRACT,
- L513: "source_row_number_present": "_source_row_number" in df.columns,
- L514: "source_row_number_complete": source_row_number_null_rows == 0,
- L515: "source_row_number_unique": source_row_number_distinct == photo_rows,
- L516: "source_row_number_matches_excel_rows": source_row_number_matches_excel_rows,
- L529: "source_row_number": "one_based_excel_worksheet_row_after_header_resolution",
- L534: "source_row_number_field": "source_row_number",
- L538: "mapping_cardinality": "one_photo_row_to_one_source_row_number",
- L539: "photo_rows_mapped": photo_rows - source_row_number_null_rows,
- L542: "trace_manifest_algorithm": "sha256(source_row_number,event_id,sp_item_id,photo_row_hash)",
- L619: parser = argparse.ArgumentParser(description="Dry-run KPIONE2 photo export grain validator.")

### `scripts/refresh_control_gestion_v2_incremental.py`

Headings:
- L1: #!/usr/bin/env python
- L2: # -*- coding: utf-8 -*-

Matches:
- L20: DAILY_SOURCE = "cg_core.v_cg_visita_dia_precedencia_v2"
- L21: DAILY_FACT = "cg_mart.fact_cg_visita_dia_resuelta_v2"
- L28: "fecha_visita",
- L45: "tiene_kpione2",
- L46: "tiene_power_app",
- L47: "tiene_kpione1",
- L48: "power_app_fallback",
- L49: "kpione1_audit_only",
- L55: "kpione2_rows_dia",
- L56: "power_app_rows_dia",
- L89: "VISITA",
- L90: "VISITA_REALIZADA",
- L92: "ALERTA",
- L94: "DIAS_KPIONE2",
- L95: "DIAS_POWER_APP",
- L100: "VISITA_REALIZADA_RAW",
- L101: "VISITA_REALIZADA_CAP",
- L103: "RUTA_DUPLICADA_FLAG",
- L104: "RUTA_DUPLICADA_ROWS",
- L110: "ALERTA_NORM_FILTER",
- L112: "VISITAS_PENDIENTES_CALC",
- L316: SELECT unnest(%s::date[]) AS fecha_visita
- L319: v.fecha_visita::date AS fecha_visita,
- L322: COALESCE(SUM(v.tiene_kpione2), 0)::bigint AS tiene_kpione2,
- L323: COALESCE(SUM(v.tiene_power_app), 0)::bigint AS tiene_power_app,
- L324: COALESCE(SUM(v.kpione1_audit_only), 0)::bigint AS kpione1_audit_only,
- L325: COALESCE(SUM(v.power_app_fallback), 0)::bigint AS power_app_fallback,
- L327: COUNT(*) FILTER (WHERE v.fuente_ganadora = 'KPIONE2')::bigint AS kpione2_winner_rows,
- L328: COUNT(*) FILTER (WHERE v.fuente_ganadora = 'POWER_APP')::bigint AS power_app_winner_rows,
- L332: ON ad.fecha_visita = v.fecha_visita
- L333: GROUP BY v.fecha_visita::date
- L334: ORDER BY v.fecha_visita::date
- L344: date_trunc('week', v.fecha_visita)::date AS semana_inicio,
- L348: ON aw.semana_inicio = date_trunc('week', v.fecha_visita)::date
- L349: GROUP BY date_trunc('week', v.fecha_visita)::date
- L350: ORDER BY date_trunc('week', v.fecha_visita)::date
- L362: COALESCE(SUM(COALESCE(f.visitas_exigidas_semana, 0)), 0)::bigint AS visita
- L391: f.visitas_exigidas_semana,
- L392: f.ruta_duplicada_flag,
- L393: f.ruta_duplicada_rows
- L401: COALESCE(b.visitas_exigidas_semana, 0)::bigint AS visita,
- L402: COALESCE(SUM(d.useful_day), 0)::bigint AS visita_realizada_raw,
- L405: COALESCE(b.visitas_exigidas_semana, 0)::bigint
- L406: ) AS visita_realizada_cap,
- L407: COALESCE(b.ruta_duplicada_flag, 0)::integer AS ruta_duplicada_flag,
- L408: COALESCE(b.ruta_duplicada_rows, 0)::integer AS ruta_duplicada_rows,
- L430: b.visitas_exigidas_semana,
- L431: b.ruta_duplicada_flag,
- L432: b.ruta_duplicada_rows
- L437: COALESCE(SUM(visita), 0)::bigint AS visita,
- L438: COALESCE(SUM(visita_realizada_raw), 0)::bigint AS visita_realizada_raw,
- L439: COALESCE(SUM(visita_realizada_cap), 0)::bigint AS visita_realizada_cap,
- L440: COALESCE(SUM(GREATEST(visita - visita_realizada_cap, 0)), 0)::bigint AS visitas_pendientes_calc,
- L441: COUNT(*) FILTER (WHERE visita_realizada_raw >= visita)::bigint AS cumple_rows,
- L442: COUNT(*) FILTER (WHERE visita_realizada_raw < visita)::bigint AS incumple_rows,
- L444: WHEN ruta_duplicada_flag = 1
- L445: OR ruta_duplicada_rows > 1
- L463: COALESCE(SUM(COALESCE(m."VISITA", 0)), 0)::bigint AS visita,
- L464: COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
- L465: COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
- L467: m."VISITAS_PENDIENTES_CALC",
- L468: GREATEST(COALESCE(m."VISITA", 0) - COALESCE(m."VISITA_REALIZADA_CAP", 0), 0)
- L469: )), 0)::bigint AS visitas_pendientes_calc,
- L471: WHERE COALESCE(m."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(m."ALERTA" AS text), '')))) = 'CUMPLE'
- L474: WHERE COALESCE(m."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(m."ALERTA" AS text), '')))) = 'INCUMPLE'
- L479: WHEN COALESCE(m."RUTA_DUPLICADA_FLAG", 0) = 1
- L480: OR COALESCE(m."RUTA_DUPLICADA_ROWS", 0) > 1
- L502: COALESCE(SUM(COALESCE(r."VISITA", 0)), 0)::bigint AS visita,
- L503: COALESCE(SUM(COALESCE(r."VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
- L504: COALESCE(SUM(COALESCE(r."VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
- L506: r."VISITAS_PENDIENTES_CALC",
- L507: GREATEST(COALESCE(r."VISITA", 0) - COALESCE(r."VISITA_REALIZADA_CAP", 0), 0)
- L508: )), 0)::bigint AS visitas_pendientes_calc,
- L510: WHERE COALESCE(r."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(r."ALERTA" AS text), '')))) = 'CUMPLE'
- L513: WHERE COALESCE(r."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(r."ALERTA" AS text), '')))) = 'INCUMPLE'
- L518: WHEN COALESCE(r."RUTA_DUPLICADA_FLAG", 0) = 1
- L519: OR COALESCE(r."RUTA_DUPLICADA_ROWS", 0) > 1
- L539: SELECT unnest(%s::date[]) AS fecha_visita
- L542: date_trunc('week', v.fecha_visita)::date AS semana_inicio,
- L543: v.fecha_visita::date AS fecha_visita,

### `scripts/refresh_control_gestion_v2_mv.py`

Headings:
- L1: #!/usr/bin/env python
- L2: # -*- coding: utf-8 -*-

Matches:
- L69: COALESCE(SUM(COALESCE("VISITA", 0)), 0)::bigint AS visita_plan,
- L70: COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
- L71: COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
- L73: COALESCE(SUM(GREATEST(COALESCE("VISITA", 0) - COALESCE("VISITA_REALIZADA_CAP", 0), 0)), 0)::bigint AS visitas_pendientes,
- L74: COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(CAST("ALERTA" AS text), ''))) = 'CUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS cumple_rows,
- L75: COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(CAST("ALERTA" AS text), ''))) = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS incumple_rows,
- L78: WHEN COALESCE("RUTA_DUPLICADA_FLAG", 0) = 1
- L79: OR COALESCE("RUTA_DUPLICADA_ROWS", 0) > 1
- L90: COALESCE(SUM(COALESCE("VISITA", 0)), 0)::bigint AS visita_plan,
- L91: COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
- L92: COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
- L94: COALESCE(SUM(COALESCE("VISITAS_PENDIENTES_CALC", GREATEST(COALESCE("VISITA", 0) - COALESCE("VISITA_REALIZADA_CAP", 0), 0))), 0)::bigint AS visitas_pendientes,
- L95: COALESCE(SUM(CASE WHEN COALESCE("ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST("ALERTA" AS text), '')))) = 'CUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS cumple_rows,
- L96: COALESCE(SUM(CASE WHEN COALESCE("ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST("ALERTA" AS text), '')))) = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS incumple_rows,
- L101: WHEN COALESCE("RUTA_DUPLICADA_FLAG", 0) = 1
- L102: OR COALESCE("RUTA_DUPLICADA_ROWS", 0) > 1
- L113: (mv.visita_plan - src.visita_plan)::bigint AS visita_diff,
- L114: (mv.visita_realizada_raw - src.visita_realizada_raw)::bigint AS visita_realizada_raw_diff,
- L115: (mv.visita_realizada_cap - src.visita_realizada_cap)::bigint AS visita_realizada_cap_diff,
- L117: (mv.visitas_pendientes - src.visitas_pendientes)::bigint AS visitas_pendientes_diff,
- L180: conn.rollback()
- L206: conn.rollback()
- L229: conn.rollback()

### `tests/test_cg_route_weekly_local_lab.py`

Matches:
- L62: "exact_duplicate_excess": 1,
- L69: "removed_logical_grains": 2,
- L121: self.assertTrue(meta["begin_rollback_wrapper"])
- L124: self.assertNotRegex(body.lower(), r"rollback\s*;\s*$")
- L157: "DOMINGO": 0,
- L158: "VISITA MENSUAL": 0,
- L178: self.assertEqual(profile["removed_logical_grains"], 2)
- L236: conn.rollback()

### `tests/test_kpione2_photo_grain.py`

Matches:
- L18: import load_kpione2_photo_from_excel as loader
- L24: "grain_contract": dict(loader.GRAIN_CONTRACT),
- L35: "Codigo Local",
- L61: class Kpione2PhotoGrainTests(unittest.TestCase):
- L86: def test_source_row_number_maps_each_photo_row_to_excel_origin(self):
- L91: self.assertEqual(metrics["source_row_number_min"], 2)
- L92: self.assertEqual(metrics["source_row_number_max"], 7)
- L93: self.assertEqual(metrics["source_row_number_distinct"], 6)
- L94: self.assertEqual(metrics["source_row_number_null_rows"], 0)
- L95: self.assertTrue(payload["flags"]["source_row_number_present"])
- L96: self.assertTrue(payload["flags"]["source_row_number_complete"])
- L97: self.assertTrue(payload["flags"]["source_row_number_unique"])
- L98: self.assertTrue(payload["flags"]["source_row_number_matches_excel_rows"])
- L103: [row["source_row_number"] for row in traceability["sample_rows"]],
- L107: def test_source_row_number_is_stable_within_workbook_sheet(self):
- L177: self.assertEqual(payload["metrics"]["source_row_number_min"], 2)
- L178: self.assertEqual(payload["metrics"]["source_row_number_max"], 7)
- L188: contract_path=ROOT / "contracts" / "control_gestion" / "kpione2_photo_export_contract_v1.json",
- L194: self.assertEqual(payload["metrics"]["source_row_number_min"], 2)
- L195: self.assertEqual(payload["metrics"]["source_row_number_max"], 37909)
- L196: self.assertEqual(payload["metrics"]["source_row_number_distinct"], 37908)
- L197: self.assertEqual(payload["metrics"]["source_row_number_null_rows"], 0)
- L206: ddl = (ROOT / "sql" / "15_kpione2_photo_raw_ddl.sql").read_text(encoding="utf-8")
- L207: rollback = (ROOT / "sql" / "16_kpione2_photo_raw_ddl_rollback.sql").read_text(encoding="utf-8")
- L209: self.assertTrue(rollback.startswith("-- NO APPLY"))
- L210: self.assertIn("create table if not exists cg_raw.kpione2_photo_raw", ddl)
- L211: self.assertIn("drop table if exists cg_raw.kpione2_photo_raw", rollback)
- L214: source = (ROOT / "scripts" / "load_kpione2_photo_from_excel.py").read_text(encoding="utf-8")
