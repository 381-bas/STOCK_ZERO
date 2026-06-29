# CLAUDE ROUTE B AUDIT
## Phase: FAST_REFORM_010B_ROUTE_B_CODEX_IMPLEMENTATION_AFTER_CLAUDE_AUDIT

**Branch:** `lab/FAST_REFORM_010B_route_b_claude_audit_and_codex_plan`  
**Auditor:** Claude (critical_auditor_contract_reviewer)  
**Active contract:** `contracts/control_gestion/kpione2_photo_export_contract_v1.json`  
**Baseline commit:** `36654d1` (main, 009G activation)  
**Audit date:** 2026-06-29  
**Status:** FINAL — NO IMPLEMENTATION AUTHORIZED BY THIS FILE

---

## Context

Route B is the proposed implementation path for ingesting KPIONE2 photo export data from
`photo-excel-admin_*.xlsx` (sheet `Fotos`) into the compliance pipeline. The 010A phase
locked the grain contract and produced briefs. This audit, produced in 010B, answers the
ten governance questions before Codex may begin implementation.

The audit is based on:
- `kpione2_photo_export_contract_v1.json` — active contract (ACTIVE, do not modify)
- `load_control_gestion_raw_v17.py` — existing KPIONE2 raw loader (guarded)
- `cg_route_weekly_local_lab.py` — existing route lab (reference for lab patterns)
- `AGENT_AUTHORITY_MATRIX_V2.json` — active authority model
- `FAST_REFORM_010A_route_b_implementation_lock.json` — phase lock

---

## Q1: Does Route B preserve photo_row → event_row → day_presence?

**FINDING: The CONTRACT is sound. The IMPLEMENTATION does not yet exist.**

The active contract (`kpione2_photo_export_contract_v1.json`) correctly encodes the three-level chain:

| Level | Contract definition |
|---|---|
| `photo_row` | Each row in the `Fotos` sheet of `photo-excel-admin_*.xlsx` |
| `event_row` | Rows grouped by `ID` + `SP Item ID` (the `event_identity` columns) |
| `day_presence` | One presence record per unique `(Fecha, Codigo Local, Marca)` per event |

The contract correctly **excludes** from the stable event hash:
- `Foto Nº/Total` — photo sequence denominator, varies within an event
- `Link Foto` — per-photo URL, photo-level
- `Hora` — varies within event ID
- `Tipo de Tarea` — task label varies by photo within same visit event
- `Fecha de subida` — upload metadata, must not affect visit identity

Derivations are specified at the event level:
- `n_fotos_calculado = max(parse_total(Foto Nº/Total)) per ID`
- `photo_rows = count rows per ID`
- `tipos_de_tarea = sorted distinct list of Tipo de Tarea per ID`
- `hora_primera_foto = min(Hora) per ID`

**CONCLUSION:** If Codex implements strictly per contract, the chain is preserved.
The chain is contractually correct. It has not been proven in code yet — that is 010B's job.

The 009F evidence baseline that must be matched:
- `photo_rows: 37908`
- `distinct_event_ids: 5892`
- `fecha_min: 2026-06-20`
- `fecha_max: 2026-06-24`

---

## Q2: Risk of relapsing into one_excel_row_equals_one_visit?

**FINDING: HIGH RISK at four specific implementation boundaries.**

### Risk R1 — Direct row-to-presence mapping (CRITICAL)
If the new loader iterates over photo rows and emits one `day_presence` record per row
(without grouping by `ID` first), the forbidden assumption is violated.

The existing `load_kpione2` function in `load_control_gestion_raw_v17.py` (lines 712–733)
sets `visita_value = 1 if has_evidence or (visita_numeric > 0) else 0` **per row**.
This pattern is safe for the `DB (KPIONE2.0)` sheet where each row IS one visit summary,
but it is **FORBIDDEN** for the photo export sheet where each row is one photo.

Codex MUST NOT copy this row-level visita_value derivation to the photo loader.
The correct pattern: group by `ID`, then derive `has_visit = count(photo_rows) > 0`.

### Risk R2 — Incorrect aggregation key (MEDIUM)
If event aggregation groups by `(Codigo Local, Marca, Fecha)` only — without including `ID` —
two different events (different visits to the same store on the same day) would be collapsed.
The correct key is `ID` (and optionally `SP Item ID` as secondary).

### Risk R3 — Day presence from event count, not binary (LOW-MEDIUM)
If `day_presence` is computed as the count of distinct events per day rather than as a
boolean (present/absent), the compliance denominator would be inflated.
Day presence must be: `1` if any event occurred on that date at that store/brand, `0` otherwise.

### Risk R4 — Hash contamination from photo-level columns (LOW)
If photo-level columns (`Hora`, `Tipo de Tarea`, `Link Foto`) are included in the
`event_stable_hash_columns` computation, distinct hash values would be produced for events
that are semantically identical, causing phantom duplicates.

The contract explicitly excludes these — Codex must not add them back.

---

## Q3: Classification by authority level

### GREEN — Total autonomy (no approval needed, no cross-audit)

| Area | Files | Notes |
|---|---|---|
| Read existing scripts | all `scripts/*.py` | Read-only investigation |
| Create new loader script (skeleton) | `scripts/load_kpione2_photo_from_excel.py` | New file, no DB execution |
| Create tests | `tests/test_kpione2_photo_grain.py` (or similar) | Unit tests with fixture data |
| Create SQL DDL file (no apply) | `sql/15_kpione2_photo_raw_ddl.sql` | File only, NO execution |
| Create research evidence | `research/010B_route_b_claude_audit/*` | Research tree write |
| Create dry-run helper | Part of new loader script with `--dry-run` flag | No DB writes |
| Run source-check on example file | Loopback/local only | Read-only file validation |
| Commit to lab branch | `lab/FAST_REFORM_010B_*` | Lab branch only |

### YELLOW — Controlled autonomy (no approval, but gated by phase)

| Area | Files | Condition |
|---|---|---|
| New loader complete implementation | `scripts/load_kpione2_photo_from_excel.py` | After GREEN skeleton passes grain tests |
| Modified scanner for new file pattern | `scripts/scanner.py` | Only if scanner_only phase authorizes |
| SQL dry-run helpers | New files under `sql/` | No execution, file creation only |
| Push feature branch | lab branch | Phase lock must be active |
| Open PR | GitHub | For Claude + Bastián review |

### ORANGE — Cross-audit required before PR is merge-ready

The following areas require **both** Claude and Codex sign-off before merge:

| Area | Files | Why ORANGE |
|---|---|---|
| Modifying existing KPIONE2 loader | `scripts/load_control_gestion_raw_v17.py` | Guarded loader — live compliance pipeline |
| Modifying incremental refresh | `scripts/refresh_control_gestion_v2_incremental.py` | Downstream compliance calculation |
| Adding new source to `SOURCE_ORDER` | `load_control_gestion_raw_v17.py:21` | Changes loader behavior for ALL sources |
| Adding new DB table to raw schema | `sql/15_*` when it touches `cg_raw.*` | Modifies data contract surface |
| Modifying compliance views | any `cg_core.*` or `cg_mart.*` SQL | Route B grain must not break existing compliance |
| Changing `KPIONE2_NUMERIC_COLUMNS` | `load_control_gestion_raw_v17.py:31` | Changes what gets null-coerced |
| Modifying `event_stable_hash_columns` | `kpione2_photo_export_contract_v1.json` | Active contract change — requires cross-audit |

### RED — Bastián explicit authorization required

| Action | Why RED |
|---|---|
| DB schema apply (CREATE TABLE, ALTER TABLE) | Irreversible without explicit DROP |
| SQL apply against Supabase real | Production data |
| Any loader execution producing real writes | Live DB writes |
| Modifying `kpione2_photo_export_contract_v1.json` | Active contract — governance |
| Merge to main | Final gate |
| Deleting or archiving existing raw data | Data loss risk |
| Rotating or changing credentials | Security |

---

## Q4: What can Codex do without asking for authorization?

Codex may proceed without additional authorization for all GREEN items and the following
YELLOW items (which are within the 010B phase scope):

1. **Create** `scripts/load_kpione2_photo_from_excel.py`
   - Must implement `photo_row → event_row → day_presence` grouping
   - Must include `--dry-run` flag (no DB execution without explicit apply flag)
   - Must validate grain against `kpione2_photo_export_contract_v1.json` blockings:
     `photo_rows_match`, `distinct_event_ids_match`, `no_null_event_id_rows`, etc.

2. **Create** tests for the new loader against fixture data (copy of 009F evidence file or
   synthetic fixture with known photo/event counts)

3. **Create** `sql/15_kpione2_photo_raw_ddl.sql` — DDL file ONLY, header MUST start with
   `-- NO APPLY` to signal it is not execution-ready

4. **Create** `research/010B_route_b_claude_audit/CODEX_EXECUTION_PLAN.md` with a step-by-step
   implementation plan citing this audit

5. **Run** the new loader's `--dry-run` against the example file
   `data/photo-excel-admin_1782440454408.xlsx` and record the grain validation output

6. **Commit** all of the above to the 010B lab branch

Codex must NOT touch any ORANGE or RED areas without an explicit separate authorization.

---

## Q5: What requires cross-audit?

The following items require Claude to review Codex's implementation and produce a signed
audit response before the PR can be considered merge-ready:

### CA1 — Grain aggregation correctness (HIGHEST PRIORITY)
Codex must show that the `photo_row → event_row` grouping is implemented as:
```python
# Correct pattern (groupby ID, not per-row)
event_df = df.groupby("ID").agg(...)
```
And NOT as:
```python
# Forbidden: one row = one visit
for rec in df.to_dict(orient="records"):
    visita_value = 1 if bool(rec.get("Link Foto")) else 0
```
Claude must verify this pattern in the actual code before ORANGE sign-off.

### CA2 — day_presence derivation
Codex must show the day_presence derivation produces a binary (0/1) per
`(Fecha, Codigo Local, Marca)`, not a count of events.

### CA3 — Integration path with existing pipeline
If Codex proposes to integrate the photo loader output into `load_control_gestion_raw_v17.py`
(adding a new `KPIONE2_PHOTO` source), Claude must audit the integration point to confirm
it does not break the existing KPIONE2 flow or compliance calculations.

### CA4 — Contract impact note
Before any PR that touches ORANGE areas is merged, Codex must produce a contract impact note
stating: (a) which contract fields are affected, (b) what evidence confirms parity with 009F.

---

## Q6: What remains blocked as RED?

The following are unconditionally blocked until Bastián provides explicit written authorization
in the issue/PR for the specific action:

1. `DB apply` — any DDL or DML execution against Supabase real
2. `SQL apply` — execution of any `.sql` file against Supabase
3. Loader execution with real DB writes (no `--dry-run` flag, `--apply` flag without explicit auth)
4. Modifying `kpione2_photo_export_contract_v1.json` (active contract status)
5. Merge to main
6. Deletion or archival of any existing `cg_raw.*` data
7. Any schema change to existing tables (`kpione2_raw`, `ruta_rutero`, etc.)

These blocks hold regardless of any chat authorization. They require a committed phase gate or
explicit PR comment from Bastián.

---

## Q7: Minimum evidence Codex must produce

Before the 010B PR is ready for review, Codex must produce ALL of the following:

| # | Evidence artifact | Location | Requirement |
|---|---|---|---|
| E1 | Dry-run output matching 009F grain counts | `research/010B_route_b_claude_audit/CODEX_GRAIN_DRY_RUN.md` | `photo_rows=37908`, `distinct_event_ids=5892` (or clearly explained deviation) |
| E2 | Diff summary | PR description | List every file created/modified with one-line rationale |
| E3 | Contract impact note | `research/010B_route_b_claude_audit/CODEX_CONTRACT_IMPACT.md` | States which contract fields are satisfied, which are pending |
| E4 | Rollback note | PR description or dedicated file | How to undo without DB apply |
| E5 | Test output | Terminal output or CI | All grain tests passing against fixture data |
| E6 | No-DB-apply evidence | Embedded in dry-run output | Explicit `db_apply: false` in loader output JSON |
| E7 | ORANGE audit response | File or PR comment | Claude sign-off on each ORANGE item touched |

If any of E1–E7 is missing, the PR is not merge-ready.

---

## Q8: What rollback must exist?

Route B must be implemented **additively** — it must not modify or replace the existing
KPIONE2 loading path. This guarantees rollback at zero cost.

### Code rollback
`git revert` of the new loader script. The existing `load_kpione2` function in
`load_control_gestion_raw_v17.py` and `kpione2_raw` table remain unchanged and functional.

### DB rollback (for future apply phase)
Before any DB apply is authorized, a rollback script must exist as a committed SQL file:
- `sql/16_kpione2_photo_raw_ddl_rollback.sql` — `DROP TABLE IF EXISTS cg_raw.kpione2_photo_raw`
- This file must also start with `-- NO APPLY`

### Integration rollback (for loader integration phase — ORANGE+)
If Route B is integrated into `load_control_gestion_raw_v17.py`, the integration must be
behind a flag (`--force-source` must exclude `KPIONE2_PHOTO` from `SOURCE_ORDER` unless
explicitly requested). This allows reverting to the pre-integration behavior without code changes.

### No rollback needed for dry-run or test-only work
Pure test files and the new loader script with `--dry-run` only: rollback is `git revert`.
No DB state to unwind.

---

## Q9: What files can Codex touch?

In the `lab/FAST_REFORM_010B_route_b_claude_audit_and_codex_plan` branch, Codex may:

### CREATE (new files, GREEN/YELLOW)
- `scripts/load_kpione2_photo_from_excel.py` — new photo loader
- `sql/15_kpione2_photo_raw_ddl.sql` — DDL only, must have `-- NO APPLY` header
- `sql/16_kpione2_photo_raw_ddl_rollback.sql` — rollback DDL, must have `-- NO APPLY` header
- `tests/test_kpione2_photo_grain.py` — unit tests (or equivalent test path)
- `research/010B_route_b_claude_audit/CODEX_EXECUTION_PLAN.md`
- `research/010B_route_b_claude_audit/CODEX_GRAIN_DRY_RUN.md`
- `research/010B_route_b_claude_audit/CODEX_CONTRACT_IMPACT.md`

### MAY MODIFY (YELLOW, gated)
- `scripts/scanner.py` — ONLY if the scanner needs to discover the new photo export file pattern, AND only after Claude reviews the change

### MUST NOT TOUCH WITHOUT ORANGE AUTHORIZATION
- `scripts/load_control_gestion_raw_v17.py`
- `scripts/refresh_control_gestion_v2_incremental.py`
- `scripts/refresh_control_gestion_v2_mv.py`
- `scripts/cg_canonical_build_local.py`
- `scripts/cg_route_weekly_local_lab.py`
- Any SQL file without `-- NO APPLY` header

---

## Q10: What files must Codex NOT touch?

The following files are blocked from Codex modification in 010B:

### Guarded loaders (ORANGE boundary — loader logic change)
- `scripts/load_control_gestion_raw_v17.py`
- `scripts/refresh_control_gestion_v2_incremental.py`
- `scripts/refresh_control_gestion_v2_mv.py`
- `scripts/load_fact_from_excel.py`
- `scripts/load_ruta_rutero_from_excel.py`
- `scripts/cliente_mvs.py`

### Active contract (RED — governance)
- `contracts/control_gestion/kpione2_photo_export_contract_v1.json`

### Governance artifacts (RED — governance layer)
- `governance/AGENT_AUTHORITY_MATRIX_V2.json`
- `governance/ACTIVE_ORDER_LOCK.json`
- `governance/PROJECT_STATUS_INDEX.json`
- `governance/EXECUTION_DOCTRINE.md`
- `governance/AGENT_ACCESS_POLICY.json`
- `governance/phase_locks/FAST_REFORM_010A_route_b_implementation_lock.json`

### SQL files with potential live references
- Any SQL file that does NOT start with `-- NO APPLY`
- `sql/11_control_gestion_route_week_replacement_contract.sql` (active contract SQL)

### Research shared memory (requires explicit authorization per research-memory.md rules)
- `research/AI_SHARED_MEMORY.json`
- `research/AI_BACKLOG.json`
- `research/AI_PROJECT_HORIZON.json`
- `research/AI_CAPABILITY_MAP.json`
- `research/AI_FINDINGS_LEDGER.jsonl` (append-only; only Claude may PROPOSE, Codex may VALIDATE when authorized)

---

## Summary risk matrix

| Risk | Level | Mitigation |
|---|---|---|
| photo-row treated as visit-row | RED risk in implementation | Mandatory groupby `ID` pattern; CA1 cross-audit |
| Wrong aggregation key (no event ID) | ORANGE risk | CA2 cross-audit; test with multi-event same-day fixture |
| Day presence as count not binary | ORANGE risk | CA2 cross-audit; contract spec is clear |
| Photo-level columns in hash | YELLOW risk | Excluded list is in contract; tests must verify |
| Integration path breaks existing KPIONE2 | ORANGE risk | Additive-only implementation; CA3 cross-audit |
| Accidental DB apply | RED governance | `--dry-run` default; no apply without `--confirm-apply` flag |

---

## Audit verdict

```json
{
  "audit_phase": "FAST_REFORM_010B_ROUTE_B_CODEX_IMPLEMENTATION_AFTER_CLAUDE_AUDIT",
  "auditor": "Claude",
  "contract_valid": true,
  "grain_chain_contractually_sound": true,
  "grain_chain_proven_in_code": false,
  "implementation_exists": false,
  "forbidden_assumption_risk": "HIGH_AT_ROW_LEVEL_IMPLEMENTATION",
  "cross_audit_required_before_merge": true,
  "codex_green_yellow_work_unblocked": true,
  "codex_orange_work_blocked_pending_cross_audit": true,
  "red_actions_blocked_without_bastian": true,
  "db_apply": false,
  "sql_apply": false,
  "loader_patch": false,
  "files_modified": [],
  "verdict": "AUDIT_COMPLETE_CODEX_MAY_BEGIN_GREEN_YELLOW_WORK"
}
```

---

*This file is a versioned audit artifact. It does not authorize implementation.
All ORANGE changes require Claude review of Codex's actual code before PR is merge-ready.
All RED actions require Bastián's explicit written authorization.*
