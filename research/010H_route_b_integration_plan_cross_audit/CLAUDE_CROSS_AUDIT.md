# CLAUDE CROSS-AUDIT — 010G Route B Integration Plan
## Phase: FAST_REFORM_010H_ROUTE_B_INTEGRATION_PLAN_CROSS_AUDIT

**Auditor:** Claude (critical_auditor_contract_reviewer)
**Audit target:** Phase 010G, merged to main via PR #32 @ `11295f7`
**Audit branch:** `lab/FAST_REFORM_010H_route_b_integration_plan_cross_audit`
**Active contract:** `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
**Audit date:** 2026-06-29
**Prior audit references:**
- `research/010E_route_b_source_row_number/CLAUDE_POST_AUDIT.md` (verdict APPROVE, W2 closed)
- `research/010C_route_b_dry_run_validation/CLAUDE_POST_AUDIT.md`

---

## Scope and method

This is a **planning-phase cross-audit**. 010G produced no code and no production change — it
produced three planning documents plus a README and governance updates. The cross-audit's job
(per `AGENT_AUTHORITY_MATRIX_V2` ORANGE rule and the 010G→010H gate) is to confirm the plan is
contract-faithful and that the gate structure protecting future implementation is complete
**before** any implementation phase is authorized.

**Merge-scope verification (`git diff 904205a..11295f7`):** the 010G merge changed only:
```
governance/ACTIVE_ORDER_LOCK.json
governance/PROJECT_STATUS_INDEX.json
governance/phase_locks/FAST_REFORM_010G_route_b_integration_planning_no_apply.json
research/010G_route_b_integration_planning_no_apply/README.md
research/010G_route_b_integration_planning_no_apply/RISK_MATRIX.md
research/010G_route_b_integration_planning_no_apply/ROUTE_B_INTEGRATION_PLAN.md
research/010G_route_b_integration_planning_no_apply/VALIDATION_GATES.md
```
No production file was touched. Confirmed via last-touch commits:
- `scripts/load_control_gestion_raw_v17.py` → last touched `8c311e2` (pre-Route-B; **not** in 010G)
- `contracts/control_gestion/kpione2_photo_export_contract_v1.json` → last touched `7cfba7a` (009F; **not** in 010G)
- `scripts/load_kpione2_photo_from_excel.py` → last touched `cd33e66` (010E; **not** in 010G)

The Route B loader and contract are byte-identical to the versions approved in the 010E audit.

---

## Q1 — Does the 010G plan respect the KPIONE2 photo export contract?

**CONFIRMED.**

`ROUTE_B_INTEGRATION_PLAN.md` (lines 11–18) declares the production chain must preserve exactly
the contract's invariants:

| Contract element (`kpione2_photo_export_contract_v1.json`) | Plan statement |
|---|---|
| `grain_contract.input_grain = photo_row` | "input grain: `photo_row`" |
| `grain_contract.normalized_grain = event_row` | "normalized grain: `event_row`" |
| `grain_contract.compliance_grain = day_presence` | "compliance grain: `day_presence`" |
| `event_identity = [ID, SP Item ID]` | "event identity: `ID + SP Item ID`" |
| (010E provenance addition) | "provenance: `source_row_number`" |

The plan's "Non-goals" section (lines 20–28) explicitly forbids modifying the active contract.
The "Integration principle" (line 4) requires a controlled adapter layer "not by silently
replacing existing productive logic" — consistent with the contract's `forbidden_assumption`
discipline.

No contract field is renamed, dropped, or redefined. The plan is contract-faithful.

---

## Q2 — Does it preserve the grain photo_row → event_row → day_presence?

**CONFIRMED.**

The three-level chain is stated as a hard preservation requirement in both the plan
(lines 7–18) and the risk matrix (R1: "Preserve `photo_row -> event_row -> day_presence`;
tests must reject one-row-one-visit", severity HIGH). The pre-apply checklist in
`VALIDATION_GATES.md` (lines 50–51) requires "grain unchanged" and "day_presence count validated"
before any apply.

The plan does not propose any alternative grain or any short-circuit from photo_row directly to
day_presence. Stage 2 (test-only adapter) requires "event/day aggregates remain unchanged"
(line 62). The grain is protected at plan, risk, and gate level.

---

## Q3 — Does it preserve event identity ID + SP Item ID?

**CONFIRMED.**

Event identity is named identically in the plan (line 16), the risk matrix (R3, severity HIGH:
"Event identity remains `ID + SP Item ID`"), and the pre-apply checklist ("event identity
unchanged", line 49). This matches `contract.event_identity = ["ID", "SP Item ID"]` and the
010E-verified loader behavior (`event_identity_replaced: false`).

---

## Q4 — Is source_row_number correctly protected as traceability, not identity?

**CONFIRMED.**

The plan lists `source_row_number` under **"provenance"** (line 17), held separate from the
"event identity" line (line 16). The adapter mapping in Stage 1 (lines 36–48) enumerates
`source row number`, `event identity`, `photo row hash`, and `stable event hash` as **distinct**
mapping areas — provenance and identity are not conflated.

The risk matrix reinforces this:
- R2 (HIGH): "Keep `source_row_number`; enforce uniqueness per file/sheet" — treats it as a
  traceability/provenance key.
- R6 (HIGH): "Require unique `(source_file_sha256, source_sheet, source_row_number)`" — this is
  a **row-provenance** uniqueness constraint, not an event-identity constraint.

This is fully consistent with the 010E post-audit finding that `source_row_number` is provenance
metadata that does not participate in `EVENT_STABLE_KEYS` or the event hash. Correctly protected.

---

## Q5 — Are the risks sufficient?

**MOSTLY — strong coverage, with gaps noted in Q10/Q11.**

`RISK_MATRIX.md` covers ten risks (R1–R10). The HIGH-severity set correctly captures the
structural dangers: visit-row collapse (R1), traceability loss (R2), identity break (R3), early
productive-loader change (R4), premature SQL apply (R5), duplicate raw rows (R6), productive view
regression (R9), and rollback-not-ready (R10). R7 (chunked-read offset) directly continues the
010E residual risk R2 — good continuity. R8 (forward-only vs backfill scope) is a real
operational risk and correctly flagged as MEDIUM/Open.

**Gaps (not yet in the matrix), elaborated in Q10–Q11:**
- No risk for **window / batch-set declaration** (CLAUDE.md hard rule on temporal contracts).
- No risk for **compliance-denominator reconciliation** when Route B `day_presence` begins
  feeding compliance (the highest-impact integration risk).
- No risk for **re-ingestion / idempotency** behavior on duplicate file load (R6 names the
  constraint but not the loader's conflict policy).

These are future-phase risks; their absence does not invalidate the planning phase but must be
closed before implementation. See Q10.

---

## Q6 — Are the GREEN / ORANGE / RED gates well separated?

**CONFIRMED — clean separation, consistent with AGENT_AUTHORITY_MATRIX_V2.**

`VALIDATION_GATES.md` maps cleanly onto the active authority model:

| Gate | Plan content | Matrix alignment |
|---|---|---|
| GREEN | read governance/contracts/evidence; generate planning docs; plan test harness | Matches `GREEN.allowed` (read, research notes, plans) |
| ORANGE | modify `load_control_gestion_raw_v17.py`; change productive ingestion; change compliance logic; adapter code for production path; change compliance-defining outputs | Matches `ORANGE.allowed_only_after_cross_audit` (loader logic, compliance logic, contracts) and requires the `ORANGE.minimum_evidence` set |
| RED | DB apply; SQL apply real; data movement; production cutover; destructive cleanup; secret change; real incremental ingestion | Matches `RED.blocked_without_bastian` exactly |

The ORANGE gate lists the required evidence (contract impact note, diff summary, rollback path,
local + dry-run output, Claude/Codex cross-audit), which is the matrix's `minimum_evidence`. No
GREEN item leaks an ORANGE/RED capability, and no ORANGE item is mis-labeled as GREEN. The
separation is correct.

---

## Q7 — Is touching load_control_gestion_raw_v17.py correctly forbidden in this stage?

**CONFIRMED — forbidden at four independent layers.**

1. `PROJECT_STATUS_INDEX.forbidden_until_close` → "modify scripts/load_control_gestion_raw_v17.py"
2. `ACTIVE_ORDER_LOCK.forbidden_now` → same
3. `phase_locks/010G.forbidden_actions` → same
4. `ROUTE_B_INTEGRATION_PLAN.md` Non-goals (line 23) + `RISK_MATRIX.md` R4 (HIGH, "Blocked in 010G")
   + `VALIDATION_GATES.md` ORANGE gate (first item)

The plan defers any productive-loader change to **Stage 3 (ORANGE)** and only after cross-audit.
Merge-scope verification confirms the file was not touched. Correctly forbidden and respected.

---

## Q8 — Are DB apply and SQL apply correctly forbidden?

**CONFIRMED — RED at every layer.**

DB apply and SQL apply against Supabase real appear in the `forbidden_now` / `forbidden_actions`
of the order lock and phase lock, in the RED gate of `VALIDATION_GATES.md`, and as R5 (HIGH, "RED
blocked") in the risk matrix. The phase lock's `red_future_scope` explicitly enumerates
"SQL apply against Supabase real", "DB writes to cg_raw", "real incremental ingestion". The
PROJECT_STATUS_INDEX records `db_apply: false`, `sql_apply: false` for the planning phase.

Consistent with `AGENT_AUTHORITY_MATRIX_V2.RED.rule`: "Git history is not considered sufficient
rollback for RED actions." Correctly forbidden.

---

## Q9 — Does the plan clearly distinguish local test-only adapter vs future ORANGE integration vs future RED apply?

**CONFIRMED — the four-stage ladder is unambiguous.**

`ROUTE_B_INTEGRATION_PLAN.md` (lines 30–74) defines a monotonic escalation:

| Stage | Authority | Boundary |
|---|---|---|
| Stage 1 — Adapter design | GREEN (planning) | Field mapping document only |
| Stage 2 — Test-only adapter | GREEN/YELLOW | "No DB writes"; transform payload → candidate rows in-memory; assert counts/uniqueness/aggregates stable |
| Stage 3 — ORANGE production loader review | ORANGE (cross-audit required) | Decide whether v17 loader is patched or Route B stays separate; "touches productive ingestion logic" |
| Stage 4 — RED apply planning | RED (Bastián authorization) | DB apply plan; "DB apply remains RED" |

The phase lock's `orange_future_scope` and `red_future_scope` corroborate this split. The
test-only adapter (Stage 2) is explicitly walled from the production path (Stage 3) and from any
write (Stage 4). The distinction the question asks about is made cleanly and is internally
consistent across plan, phase lock, and gates document.

---

## Q10 — Is any gate missing before a future implementation phase?

**YES — three gates should be added before an implementation phase is authorized.** None block
the planning PR (which is correctly merged), but the cross-audit's purpose is to require these be
in place before Stage 2+ work begins.

### G-MISSING-1 (MEDIUM) — Window / batch-set declaration gate
`CLAUDE.md` hard rule: *"Never treat `latest` as a temporal contract. Builds must declare window,
raw batch set, weekly route snapshot, precedence version, build version."* The integration plan
and pre-apply checklist do **not** require the ingestion to declare which window / raw batch set /
weekly route snapshot a Route B load belongs to. Without this, multi-week accumulation of photo
exports has no temporal contract. **A "declared window + batch set" gate must be added to the
pre-apply checklist before Stage 2 designs the adapter.**

### G-MISSING-2 (HIGH) — Compliance-denominator reconciliation gate
This is the highest-impact omission. The contract's `denominator_mapping`
(`Codigo Local → cod_rt`, `Marca → cliente_norm`) ties Route B `day_presence` to the route
compliance denominator. When Route B `day_presence` eventually **feeds** compliance, there is no
gate requiring that the new presence signal **reconciles with the existing KPIONE2 visit-based
compliance** (parity, or an explicit, authorized delta) before cutover. The plan's decision point
1 ("standalone vs integrate") and Stage 3 acknowledge the integration choice but stop short of a
**numeric reconciliation gate**. **Add a gate: "Route B day_presence reconciled against existing
compliance denominator with signed parity evidence" before any compliance view consumes Route B
output.** This aligns with the project's standing G0-parity discipline.

### G-MISSING-3 (LOW–MEDIUM) — Re-ingestion / idempotency policy gate
R6 names the unique constraint `(source_file_sha256, source_sheet, source_row_number)` but neither
the plan nor the gates specify the **loader's behavior on conflict** (reject / skip / upsert) when
the same workbook is presented twice. A unique constraint alone turns a re-run into a hard error
rather than a defined idempotent outcome. **Add a gate defining the re-ingestion policy and
verifying it in the Stage 2 test-only harness.**

### Adequately covered (no gate needed)
- Rollback-before-apply: covered (R10 + checklist "rollback SQL reviewed").
- RLS / read grants: covered (checklist "RLS/read grants reviewed").
- Schema review: covered (checklist "DB target schema reviewed").
- Chunked-read offset: covered (R7).

---

## Q11 — Residual risks

| ID | Residual risk | Severity | Note |
|---|---|---|---|
| RR1 | Compliance reconciliation gate absent (G-MISSING-2) | HIGH | Must exist before Route B feeds any compliance view; biggest pre-implementation gap |
| RR2 | No temporal window / batch-set declaration (G-MISSING-1) | MEDIUM | Violates CLAUDE.md temporal-contract hard rule if carried into ingestion |
| RR3 | Re-ingestion/idempotency policy undefined (G-MISSING-3) | LOW–MEDIUM | Unique constraint without conflict policy → re-run errors |
| RR4 | Forward-only vs backfill scope still Open (R8) | MEDIUM | Plan flags it as a decision point but no gate forces resolution before apply |
| RR5 | Multi-file `day_presence` accumulation not addressed | MEDIUM | `source_row_number` resets per file (correctly), but cross-file presence aggregation for compliance is unspecified; tied to RR1/RR2 |
| RR6 | Stage 2 adapter is described, not yet specified in testable detail | LOW | Acceptable for a planning phase; Stage 2 itself must produce the harness |
| RR7 | Governance index lag | INFORMATIONAL | `PROJECT_STATUS_INDEX.active_phase` still shows 010G (not yet promoted to 010H); expected — index promotion happens in the next closeout. Not a defect. |

All residual risks are **future-phase** risks. None require any change to the merged 010G
artifacts to be safe today, because 010G is planning-only with no apply surface.

---

## Cross-audit verdict

```json
{
  "audit_phase": "FAST_REFORM_010H_ROUTE_B_INTEGRATION_PLAN_CROSS_AUDIT",
  "auditor": "Claude",
  "audit_target_phase": "FAST_REFORM_010G_ROUTE_B_INTEGRATION_PLANNING_NO_APPLY",
  "audit_target_main_commit": "11295f7",
  "merge_scope_planning_only": true,
  "production_loader_touched": false,
  "active_contract_touched": false,
  "productive_views_touched": false,
  "sql_apply": false,
  "db_apply": false,
  "plan_respects_contract": true,
  "grain_preserved_photo_event_day": true,
  "event_identity_id_plus_sp_item_id": true,
  "source_row_number_is_traceability_not_identity": true,
  "risks_sufficient": "MOSTLY_WITH_GAPS",
  "green_orange_red_gates_well_separated": true,
  "productive_loader_correctly_forbidden": true,
  "db_and_sql_apply_correctly_forbidden": true,
  "test_only_vs_orange_vs_red_clearly_distinguished": true,
  "missing_gates_before_implementation": [
    {
      "id": "G-MISSING-1",
      "severity": "MEDIUM",
      "summary": "Window / batch-set declaration gate (CLAUDE.md temporal-contract hard rule)"
    },
    {
      "id": "G-MISSING-2",
      "severity": "HIGH",
      "summary": "Compliance-denominator reconciliation gate before Route B day_presence feeds compliance"
    },
    {
      "id": "G-MISSING-3",
      "severity": "LOW_MEDIUM",
      "summary": "Re-ingestion / idempotency policy gate verified in Stage 2 harness"
    }
  ],
  "residual_risks": ["RR1","RR2","RR3","RR4","RR5","RR6","RR7"],
  "blockers": [],
  "verdict": "APPROVE_WITH_WARNINGS"
}
```

---

## Summary and recommendation

The 010G plan is contract-faithful and structurally sound. It preserves the grain
(`photo_row → event_row → day_presence`), the event identity (`ID + SP Item ID`), and the
provenance role of `source_row_number` (traceability, never identity). The GREEN/ORANGE/RED gate
separation is clean and matches `AGENT_AUTHORITY_MATRIX_V2`. Touching the production loader and
applying DB/SQL are forbidden at multiple layers and were respected — the merge touched only
governance and research files. The four-stage escalation (design → test-only adapter → ORANGE
loader review → RED apply) is unambiguous.

**Verdict: APPROVE_WITH_WARNINGS.** The planning phase is correctly merged and safe. Before any
implementation phase (Stage 2 and beyond) is authorized, three gates must be added to the
validation-gate set — most importantly **G-MISSING-2, the compliance-denominator reconciliation
gate** — together with **G-MISSING-1 (window/batch-set declaration)** and **G-MISSING-3
(re-ingestion idempotency policy)**. These are pre-implementation requirements, not defects in the
merged planning artifacts.

---

*This file is a versioned cross-audit artifact. It does not authorize implementation, DB apply,
SQL apply, loader changes, or any RED action. All ORANGE work requires Claude/Codex cross-audit;
all RED actions require Bastián's explicit written authorization.*
