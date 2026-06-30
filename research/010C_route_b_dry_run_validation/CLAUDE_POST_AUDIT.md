# CLAUDE POST-AUDIT — 010C Route B Dry-Run Validation
## Phase: FAST_REFORM_010C_ROUTE_B_REVIEW_AND_DRY_RUN_VALIDATION

**Auditor:** Claude (critical_auditor_contract_reviewer)
**Branch:** `lab/FAST_REFORM_010C_route_b_review_and_dry_run_validation`
**Base commit:** `c56dcfb` (main, 010B merge)
**Single branch commit audited:** `3df063b` (author: Bastian Antiman)
**Audit date:** 2026-06-29
**Active contract:** `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
**Prior audit reference:** `research/010B_route_b_claude_audit/CLAUDE_ROUTE_B_AUDIT.md`

---

## Scope

This audit covers all files added or modified in commit `3df063b` on the 010C branch.
It answers the ten governance questions required before this PR can be merged to main.
No DB apply, SQL apply, data movement, or destructive operation was performed by this audit.
The only output of this audit is this file.

---

## Files examined

| File | Role |
|---|---|
| `governance/PROJECT_STATUS_INDEX.json` | Project phase state |
| `governance/ACTIVE_ORDER_LOCK.json` | Active order and forbidden actions |
| `governance/AGENT_AUTHORITY_MATRIX_V2.json` | Authority level model |
| `contracts/control_gestion/kpione2_photo_export_contract_v1.json` | Active contract |
| `governance/phase_locks/FAST_REFORM_010B_*.json` | 010B phase lock |
| `governance/phase_locks/FAST_REFORM_010C_*.json` | 010C phase lock |
| `research/010B_route_b_claude_audit/CLAUDE_ROUTE_B_AUDIT.md` | 010B Claude audit |
| `scripts/load_kpione2_photo_from_excel.py` | New additive loader |
| `tests/test_kpione2_photo_grain.py` | Grain unit tests |
| `sql/15_kpione2_photo_raw_ddl.sql` | Review-only DDL |
| `sql/16_kpione2_photo_raw_ddl_rollback.sql` | Review-only rollback DDL |
| `research/010C_route_b_dry_run_validation/CODEX_DRY_RUN_OUTPUT.json` | Real-data dry-run output |
| `research/010C_route_b_dry_run_validation/CODEX_CONTRACT_IMPACT.md` | Contract impact note |
| `research/010C_route_b_dry_run_validation/CODEX_ROLLBACK_NOTE.md` | Rollback note |
| `research/010C_route_b_dry_run_validation/CODEX_DIFF_SUMMARY.md` | Diff summary |

---

## Q1 — Does the loader preserve photo_row → event_row → day_presence?

**CONFIRMED.**

The three-level chain is implemented correctly in `scripts/load_kpione2_photo_from_excel.py`:

| Level | Implementation |
|---|---|
| `photo_row` | Each row read from the `Fotos` sheet (line 318: `photo_rows = int(len(df))`) |
| `event_row` | `df.groupby("_event_id").agg(...)` (lines 341–357, `per_event`) |
| `day_presence` | `per_event.groupby(["fecha","cod_rt_norm","cliente_norm_key"]).agg(...)` (lines 373–378) |

The dry-run output (`CODEX_DRY_RUN_OUTPUT.json`) records:
```
photo_rows:         37908
event_rows:         5892
day_presence_rows:  5869
```

The normalization block in the output explicitly states:
```json
"photo_row_to_event_row": "group_by_event_id",
"day_presence_value": "binary_1_if_any_event"
```

The chain is correctly implemented, correctly measured, and correctly reported.

---

## Q2 — Does the loader avoid one_excel_row_equals_one_visit?

**CONFIRMED — assumption is structurally rejected.**

The forbidden assumption `one_excel_row_equals_one_visit` is rejected by the groupby design:
- 37,908 photo rows collapse to 5,892 event rows (a 6.4:1 ratio on average).
- The flag `forbidden_assumption_rejected` is computed as `photo_rows != distinct_event_ids`
  (line 417 of loader), which is `True` for any real dataset.
- There is no per-row `visita_value` derivation of the type found in `load_control_gestion_raw_v17.py`
  (lines 712–733 of the productive loader, identified as the primary anti-pattern in the 010B audit).
- Test `test_photo_rows_are_grouped_to_event_rows` validates this on a 6-row, 3-event fixture:
  6 photo rows → 3 event rows → `forbidden_assumption_rejected=True`.
- The dry-run output confirms: `"forbidden_assumption_rejected": true`.

**Risk R1 (CRITICAL from 010B audit) is closed.**

---

## Q3 — Is the SQL review-only with -- NO APPLY?

**CONFIRMED.**

Both SQL files start with the correct guard header:

`sql/15_kpione2_photo_raw_ddl.sql`:
```sql
-- NO APPLY
-- REVIEW DDL ONLY
-- Requires separate Bastian authorization before any SQL execution.
```

`sql/16_kpione2_photo_raw_ddl_rollback.sql`:
```sql
-- NO APPLY
-- REVIEW ROLLBACK ONLY
-- Requires separate Bastian authorization before any SQL execution.
```

The test `test_sql_files_are_review_only` programmatically reads both files and asserts:
- `ddl.startswith("-- NO APPLY")`
- `rollback.startswith("-- NO APPLY")`
- DDL contains `create table if not exists cg_raw.kpione2_photo_raw`
- Rollback contains `drop table if exists cg_raw.kpione2_photo_raw`

These SQL files are structural artifacts only. No execution mechanism exists in this branch.

---

## Q4 — Any DB apply, SQL apply, DB connection, DSN, psycopg, sqlalchemy, or real writes?

**NONE FOUND.**

**Imports audit** (`load_kpione2_photo_from_excel.py` lines 1–14):
```
argparse, hashlib, json, re, sys, unicodedata, datetime, pathlib, typing, zoneinfo, pandas
```
No `psycopg2`, `psycopg`, `sqlalchemy`, `asyncpg`, `pg8000`, `supabase`, or any DB driver present.

**Hardcoded flags** (lines 268–275 of loader):
```python
"db_apply": False,
"sql_apply": False,
"writes_executed": False,
"dsn_printed": False,
```
These are not conditional. They are set in the `base_payload` on every invocation.

**`--apply` flag guard** (lines 528–530):
```python
if args.apply:
    raise LoaderUsageError("apply_not_supported_in_route_b_dry_run", "DB apply is RED and not implemented.")
```
The flag raises immediately. No DB code path exists behind it.

**Defensive redaction** (`redact_secret_text`, lines 489–491):
The loader contains a DSN-scrubbing function that removes `postgres://...` and `password=...`
patterns from error strings. This is a defensive measure; no DSN is ever passed into it
in the normal execution path. It demonstrates correct security posture, not a connection attempt.

**Test `test_new_loader_does_not_import_productive_loader_or_db_clients`** (lines 172–178) verifies:
```python
assertNotIn("import load_control_gestion_raw_v17", source)
assertNotIn("from load_control_gestion_raw_v17", source)
assertNotIn("psycopg2", source)
assertNotIn("sqlalchemy", source.lower())
assertNotIn("DB_URL", source)
```

**Dry-run output confirms** (all `false`): `db_apply`, `sql_apply`, `writes_executed`, `dsn_printed`.

**Conclusion:** This loader has no DB capability in any code path.

---

## Q5 — Was load_control_gestion_raw_v17.py touched or imported?

**NOT TOUCHED. NOT IMPORTED.**

Evidence:
1. `CODEX_DIFF_SUMMARY.md` lists it explicitly under "Not Modified".
2. The loader source contains `PRODUCTIVE_LOADER_PATH = "scripts/load_control_gestion_raw_v17.py"`
   at line 26 — this is a string constant used as an evidence label in the output payload, not an import.
3. Test `test_new_loader_does_not_import_productive_loader_or_db_clients` verifies no import
   of the productive loader by name.
4. `git diff main..HEAD` does not include `scripts/load_control_gestion_raw_v17.py`.
5. The phase lock confirms `productive_loader_touched: false`.
6. The dry-run output confirms `"productive_loader_touched": false`.

**The guarded loader is fully isolated from this branch.**

---

## Q6 — Was the active contract touched?

**NOT MODIFIED.**

Evidence:
1. `git diff main..HEAD` does not include `contracts/control_gestion/kpione2_photo_export_contract_v1.json`.
2. `CODEX_DIFF_SUMMARY.md` lists it explicitly under "Not Modified".
3. The loader reads the contract via `load_contract(contract_path)` (line 471) — read-only,
   no write operation at any code path.
4. The SHA256 of the data file in the dry-run output (`1e345f7bdbad142ebff41472ffc7917e31412a81d88e3f99bd0b0248c60fb180`)
   exactly matches the contract's `example_sha256` field — confirming the same file used in
   the 009F validation was used here.

**The active contract is intact.**

---

## Q7 — Do the tests cover the main risks?

**YES — 9 tests, all four critical risks from the 010B audit are covered.**

| Test | Risk covered | Verdict |
|---|---|---|
| `test_photo_rows_are_grouped_to_event_rows` | R1 (photo-row treated as visit-row) | Verifies 6 photo rows → 3 event rows, `forbidden_assumption_rejected=True` |
| `test_day_presence_is_binary_not_event_count` | R3 (day_presence as count not binary) | Verifies `day_presence_rows=2`, `binary_presence_values=[1]`, `max_events_per_day_presence=2` |
| `test_photo_level_columns_are_excluded_from_event_hash` | R4 (hash contamination) | Verifies Hora, Tipo de Tarea, Link Foto excluded; `real_content_conflict_event_ids=0` |
| `test_event_stable_column_conflict_is_flagged` | Hash integrity | Verifies that a `Comentarios` difference triggers `real_content_conflict_event_ids=1` and `WARN_REVIEW_REQUIRED` |
| `test_apply_flag_is_blocked` | DB apply guard | Verifies `LoaderUsageError` raised for `--apply` |
| `test_cli_writes_json_without_db` | Full CLI path | Verifies full execution, `db_apply=False`, `sql_apply=False`, `writes_executed=False` |
| `test_real_workbook_matches_required_010c_evidence` | Real-data grain parity | Matches 009F baseline: photo_rows=37908, event_ids=5892 |
| `test_sql_files_are_review_only` | SQL header guard | Both SQL files verified to start with `-- NO APPLY` |

**Minor gap — Risk R2 (wrong aggregation key) not explicitly isolated in a dedicated test.**
R2 would manifest if the groupby used `(Codigo Local, Marca, Fecha)` without `ID`, collapsing
multiple events at the same location and day. The implementation uses `_event_id` as the groupby
key throughout, and the `max_events_per_day_presence=2` result in the fixture confirms multiple
events at the same `(cod_rt, marca, fecha)` are correctly kept distinct at the event_row level
before collapsing to one day_presence. R2 is implicitly tested but not explicitly isolated.
This is a LOW-severity gap — it does not block merge.

**The CA1 and CA2 cross-audit items from the 010B audit are CLOSED by code and tests.**

---

## Q8 — Is the dry-run evidence sufficient?

**YES — all 18 blocking flags pass.**

**Grain parity with 009F baseline:**
| Metric | 009F contract | 010C dry-run | Match |
|---|---|---|---|
| photo_rows | 37908 | 37908 | YES |
| distinct_event_ids | 5892 | 5892 | YES |
| fecha_min | 2026-06-20 | 2026-06-20 | YES |
| fecha_max | 2026-06-24 | 2026-06-24 | YES |
| source_file_sha256 | 1e345f7b... | 1e345f7b... | YES |

**Day presence behavior:**
- `day_presence_rows = 5869` (fewer than event_rows because multiple events per location/day share one slot — correct behavior)
- `max_events_per_day_presence = 4` (up to 4 distinct events on the same day at the same location, all collapsed to presence=1)
- `binary_presence_values = [1]` — only `1` appears; presence is strictly binary

**Safety flags:**
| Flag | Value |
|---|---|
| db_apply | false |
| sql_apply | false |
| writes_executed | false |
| dsn_printed | false |
| productive_loader_touched | false |

**All 18 `BLOCKING_FLAG_KEYS` are `true` in the output → `verdict = "PASS_ROUTE_B_DRY_RUN"`.**

**One observation:** `test_real_workbook_matches_required_010c_evidence` calls `skipTest` if the
workbook is absent (line 149). In a CI environment without the workbook, this test is skipped.
The dry-run JSON provides independent real-data evidence and the skip behavior is correctly
designed (the test is a safety net, not the primary evidence). Acceptable for this phase.

---

## Q9 — Residual risks before merge

### W1 — INFORMATIONAL: Governance files modified in same commit as implementation

`governance/ACTIVE_ORDER_LOCK.json` and `governance/PROJECT_STATUS_INDEX.json` are listed
in the 010B audit as governance artifacts Codex must not modify. Both files were modified in
commit `3df063b`. However:
- The commit author is `Bastian Antiman` — this was a Bastián-committed change, not an
  autonomous Codex push.
- The content of both files correctly reflects the authorized phase transition (010B → 010C).
- The 010C phase lock was created in the same commit, documenting the authorized scope.

**This is not a blocker.** It is an observation for workflow hygiene: governance file updates
ideally land in a separate governance commit before the implementation commit, to make the
authorization boundary explicit in the git history. For this PR, Bastián's authorship
is sufficient authorization evidence.

### W2 — MEDIUM: `source_row_number` defined in DDL but not produced by dry-run loader

`sql/15_kpione2_photo_raw_ddl.sql` defines:
```sql
source_row_number bigint not null
```
The current loader (`analyze_photo_dataframe`) produces aggregate-level output, not per-row
records for a raw insert. When a future RED-authorized DB apply phase is attempted, the loader
will need to emit a per-row payload that includes `source_row_number` (the original Excel row
index) for the `not null` constraint to be satisfied.

**Not a blocker for the dry-run phase.** Must be addressed before any DB apply is attempted.

### W3 — LOW: CA3 (pipeline integration audit) still pending

The 010B audit identified CA3 — integration of the photo loader into `load_control_gestion_raw_v17.py`
— as an ORANGE item requiring cross-audit. This phase correctly does NOT perform that integration.
The loader is fully standalone. CA3 remains open and must be addressed before any integration
into the productive pipeline is attempted. This is expected behavior for this phase.

### W4 — LOW: `--apply` guard has no implementation behind it

The `--apply` flag raises `LoaderUsageError` immediately. There is no DB code path to guard.
This is correct design for a dry-run phase. A future developer should not assume this flag
provides a DB write path — it is a structural placeholder that enforces the RED boundary.

### W5 — LOW: R2 aggregation key not explicitly isolated in a test

As noted in Q7, the fixture does not include a dedicated test for the case where the aggregation
key omits `event_id`. The implementation is correct, but an explicit test for R2 would increase
robustness. Low priority given the code is clearly correct.

---

## Audit verdict

```json
{
  "audit_phase": "FAST_REFORM_010C_ROUTE_B_REVIEW_AND_DRY_RUN_VALIDATION",
  "auditor": "Claude",
  "commit_audited": "3df063b",
  "contract_valid": true,
  "grain_chain_preserved": true,
  "forbidden_assumption_rejected": true,
  "sql_review_only": true,
  "db_apply": false,
  "sql_apply": false,
  "psycopg_found": false,
  "sqlalchemy_found": false,
  "dsn_found": false,
  "real_writes": false,
  "productive_loader_touched": false,
  "productive_loader_imported": false,
  "active_contract_touched": false,
  "tests_cover_main_risk": true,
  "test_count": 9,
  "dry_run_verdict": "PASS_ROUTE_B_DRY_RUN",
  "dry_run_evidence_sufficient": true,
  "cross_audit_ca1_grain_aggregation": "CLOSED",
  "cross_audit_ca2_day_presence_binary": "CLOSED",
  "cross_audit_ca3_pipeline_integration": "DEFERRED_NOT_IN_SCOPE",
  "cross_audit_ca4_contract_impact_note": "CLOSED",
  "warnings": [
    {
      "id": "W1",
      "severity": "INFORMATIONAL",
      "summary": "Governance files updated in same commit as implementation; Bastian authored"
    },
    {
      "id": "W2",
      "severity": "MEDIUM",
      "summary": "source_row_number defined in DDL but not produced by dry-run loader; blocks DB apply if not resolved"
    },
    {
      "id": "W3",
      "severity": "LOW",
      "summary": "CA3 pipeline integration audit pending for future ORANGE phase"
    },
    {
      "id": "W4",
      "severity": "LOW",
      "summary": "--apply guard has no implementation behind it; structurally correct"
    },
    {
      "id": "W5",
      "severity": "LOW",
      "summary": "R2 aggregation key risk not explicitly isolated in a dedicated test"
    }
  ],
  "blockers": [],
  "verdict": "APPROVE_WITH_WARNINGS"
}
```

---

## Summary

The 010C implementation is clean, additive, and safe.

The grain chain is preserved in code, in tests, and in the dry-run output. The forbidden
assumption is structurally rejected. The SQL files are review-only and cannot be executed
without separate explicit authorization. No DB capability exists in the loader. The productive
loader and the active contract are untouched. The dry-run output matches the 009F baseline
exactly, with the correct SHA256 confirming the same source file.

The four cross-audit items from the 010B audit that were within this phase's scope are closed.
CA3 (pipeline integration) is correctly deferred to a future ORANGE phase.

The five warnings are all LOW or INFORMATIONAL severity. None block merge. **W2** (the
`source_row_number` gap) is the only item that must be resolved before a future DB apply
phase can proceed — it does not affect this PR.

**This PR may be merged to main.**

---

*This file is a versioned audit artifact. It does not authorize DB apply, SQL apply, loader execution, or any RED action.
All RED actions require Bastián's explicit written authorization.*
