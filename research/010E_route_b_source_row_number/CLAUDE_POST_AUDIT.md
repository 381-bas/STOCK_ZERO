# CLAUDE POST-AUDIT — 010E Route B Source Row Number Dry-Run Patch
## Phase: FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH

**Auditor:** Claude (critical_auditor_contract_reviewer)
**Branch:** `lab/FAST_REFORM_010E_route_b_source_row_number_dry_run_patch`
**Base commit:** `e80d0f7` (main, 010D merge)
**Single branch commit audited:** `cd33e66` (author: Bastian Antiman)
**Audit date:** 2026-06-29
**Active contract:** `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
**Prior audit reference:** `research/010C_route_b_dry_run_validation/CLAUDE_POST_AUDIT.md`
**Prior warning closed:** W2 — `source_row_number` defined in DDL but not produced by dry-run loader

---

## Scope

This audit covers all files added or modified in commit `cd33e66` on the 010E branch.
It answers the twelve governance questions required before this PR can be merged to main.
No DB apply, SQL apply, data movement, or destructive operation was performed.
The only output of this audit is this file.

---

## Files examined

| File | Role |
|---|---|
| `governance/PROJECT_STATUS_INDEX.json` | Project phase state |
| `governance/ACTIVE_ORDER_LOCK.json` | Active order and forbidden actions |
| `governance/AGENT_AUTHORITY_MATRIX_V2.json` | Authority level model |
| `contracts/control_gestion/kpione2_photo_export_contract_v1.json` | Active contract |
| `governance/phase_locks/FAST_REFORM_010D_*.json` | 010D phase lock (source of lock) |
| `governance/phase_locks/FAST_REFORM_010E_*.json` | 010E phase lock |
| `research/010C_route_b_dry_run_validation/CLAUDE_POST_AUDIT.md` | 010C Claude post-audit (W2 origin) |
| `research/010D_route_b_source_row_number_lock/README.md` | 010D lock README |
| `scripts/load_kpione2_photo_from_excel.py` | Modified additive loader |
| `tests/test_kpione2_photo_grain.py` | Modified grain tests |
| `research/010E_route_b_source_row_number/CODEX_DRY_RUN_OUTPUT.json` | 010E dry-run evidence |
| `research/010E_route_b_source_row_number/CODEX_SOURCE_ROW_NUMBER_NOTE.md` | Source row number design note |
| `research/010E_route_b_source_row_number/CODEX_DIFF_SUMMARY.md` | Diff summary |

---

## Q1 — Does source_row_number correctly represent the original Excel row after the header?

**CONFIRMED.**

The loader defines two constants (lines 27–28):
```python
EXCEL_HEADER_ROW_NUMBER = 1
EXCEL_FIRST_DATA_ROW_NUMBER = EXCEL_HEADER_ROW_NUMBER + 1  # = 2
```

`assign_excel_source_row_numbers()` (lines 246–252) assigns a contiguous range starting at
`EXCEL_FIRST_DATA_ROW_NUMBER`:
```python
numbered["_source_row_number"] = pd.RangeIndex(
    start=EXCEL_FIRST_DATA_ROW_NUMBER,
    stop=EXCEL_FIRST_DATA_ROW_NUMBER + len(numbered),
)
```

`build_dry_run_payload()` reads the workbook with `header=EXCEL_HEADER_ROW_NUMBER - 1` (= 0),
confirming Excel worksheet row 1 is the column header and row 2 is the first data row.

Dry-run evidence confirms:
| Metric | Value | Semantics |
|---|---|---|
| `source_row_number_min` | 2 | First data row in the Excel worksheet |
| `source_row_number_max` | 37909 | Last data row (37908 photo rows + 1 header offset) |
| `first_data_row_number` | 2 | Documented in `photo_row_traceability` block |
| `excel_header_row_number` | 1 | Documented in `photo_row_traceability` block |

The arithmetic is correct: `source_row_number_max = photo_rows + EXCEL_HEADER_ROW_NUMBER = 37908 + 1 = 37909`. ✓

---

## Q2 — Is source_row_number stable within the same workbook/sheet?

**CONFIRMED — stable by design, verified by test.**

`assign_excel_source_row_numbers()` derives row numbers from `pd.RangeIndex` applied to the
DataFrame in the order returned by `pd.read_excel` — which always reads rows in physical sheet
order. The assignment is independent of the pandas DataFrame index.

Test `test_source_row_number_is_stable_within_workbook_sheet` (lines 107–116) explicitly
validates index-independence:
```python
original = sample_photo_df()
reindexed = original.copy()
reindexed.index = [90, 70, 50, 30, 10, 0]

first = self._payload(original)["photo_row_traceability"]
second = self._payload(reindexed)["photo_row_traceability"]

self.assertEqual(first["trace_manifest_sha256"], second["trace_manifest_sha256"])
self.assertEqual(first["sample_rows"], second["sample_rows"])
```

Both a naturally-indexed DataFrame and a scrambled-index DataFrame produce identical
`trace_manifest_sha256` values and identical `sample_rows`. The numbering is stable.

The phase lock documents: `"stability_scope": "source_workbook_and_sheet"`.

---

## Q3 — Does source_row_number enable photo_row → Excel row traceability?

**CONFIRMED — 1:1 cardinality, full-coverage manifest.**

The `photo_row_traceability` block in the payload documents:
```json
"mapping_cardinality": "one_photo_row_to_one_source_row_number",
"photo_rows_mapped": 37908,
"stability_scope": "source_workbook_and_sheet"
```

The trace manifest SHA-256 (`trace_manifest_sha256`) is computed by `source_trace_manifest_sha256()` (lines 255–275) as SHA-256 over the concatenation of every tuple:
```
(source_row_number, event_id, sp_item_id, photo_row_hash)
```
across all 37,908 rows. This is a compact but complete proof of the full mapping.

The `sample_rows` in the payload includes the first 3 and last 3 rows of the workbook:
- Row 2: `event_id=883726`, `source_row_number=2`
- Row 37909: `event_id=877678`, `source_row_number=37909`

Both the manifest and the samples are emitted in the evidence JSON (`CODEX_DRY_RUN_OUTPUT.json`),
allowing an auditor to verify the boundary rows and trust the manifest covers the interior.

The flag `source_row_number_unique=true` confirms no two photo rows share the same number —
1:1 cardinality is enforced as a blocking validation.

---

## Q4 — Does source_row_number NOT replace event identity?

**CONFIRMED — event identity is untouched.**

In the payload:
```json
"event_identity_replaced": false,
"event_identity": ["ID", "SP Item ID"]
```

In the code:
- `EVENT_STABLE_KEYS` (lines 74–86) is identical to the 010C version; `source_row_number`
  is not in this list and does not participate in `_event_stable_hash`.
- `per_event` aggregation (lines 414–431) groups by `_event_id` and adds
  `first_source_row_number` and `last_source_row_number` as provenance spans per event —
  audit metadata only, not identity fields.
- `day_presence` aggregation (lines 448–453) groups by `["fecha","cod_rt_norm","cliente_norm_key"]`,
  completely independent of `source_row_number`.

`source_row_number` flows into `photo_row_traceability` only. It is provenance metadata.

---

## Q5 — Is event identity still ID + SP Item ID?

**CONFIRMED — unchanged from the active contract.**

The `normalization` block in both the 010C and 010E dry-run outputs is consistent:
```json
"event_identity": ["ID", "SP Item ID"],
"event_key": "trim(ID)"
```

`EVENT_STABLE_KEYS` (lines 74–86) includes `event_id` (→ "ID") and `sp_item_id`
(→ "SP Item ID") as the first two entries — these are the event identity columns per
`kpione2_photo_export_contract_v1.json`. No change.

The phase lock records: `"event_identity_remains": ["ID", "SP Item ID"]`.

---

## Q6 — Does the grain remain photo_row → event_row → day_presence?

**CONFIRMED — grain chain is fully preserved.**

`GRAIN_CONTRACT` constant (lines 30–35) is unchanged:
```python
GRAIN_CONTRACT = {
    "input_grain": "photo_row",
    "normalized_grain": "event_row",
    "compliance_grain": "day_presence",
    "forbidden_assumption": "one_excel_row_equals_one_visit",
}
```

The three-level aggregation pipeline is unchanged:
1. `photo_row`: every row from `pd.read_excel` (line 375: `photo_rows = int(len(df))`)
2. `event_row`: `valid_events.groupby("_event_id").agg(...)` (lines 414–431, `per_event`)
3. `day_presence`: `per_event.groupby(["fecha","cod_rt_norm","cliente_norm_key"]).agg(...)` (lines 448–453)

Dry-run confirms the chain: `photo_rows=37908` → `event_rows=5892` → `day_presence_rows=5869`.

The `normalization` block still records `"day_presence_value": "binary_1_if_any_event"`.

---

## Q7 — Are photo_rows=37908 and distinct_event_ids=5892 maintained?

**CONFIRMED — no regression from the 010C and 009F baselines.**

| Metric | 009F baseline | 010C dry-run | 010E dry-run | Match |
|---|---|---|---|---|
| `photo_rows` | 37908 | 37908 | 37908 | ✓ |
| `distinct_event_ids` | 5892 | 5892 | 5892 | ✓ |
| `fecha_min` | 2026-06-20 | 2026-06-20 | 2026-06-20 | ✓ |
| `fecha_max` | 2026-06-24 | 2026-06-24 | 2026-06-24 | ✓ |
| `source_file_sha256` | 1e345f7b... | 1e345f7b... | 1e345f7b... | ✓ |

The `source_row_number` patch introduces only provenance metadata. It does not filter, dedup,
or re-aggregate photo rows. All 18 original blocking flags from 010C remain true.

---

## Q8 — Are db_apply=false, sql_apply=false, productive_loader_touched=false?

**CONFIRMED — all three false.**

`base_payload` in `analyze_photo_dataframe()` (lines 320–342) hardcodes:
```python
"db_apply": False,
"sql_apply": False,
"writes_executed": False,
"dsn_printed": False,
"productive_loader_touched": False,
```

These values are unconditional and appear in the output regardless of path taken.

The 010E dry-run output confirms all three:
```json
"db_apply": false,
"sql_apply": false,
"productive_loader_touched": false,
"writes_executed": false
```

`git diff main..HEAD` does not include `scripts/load_control_gestion_raw_v17.py`.
`CODEX_DIFF_SUMMARY.md` lists it explicitly under "Explicitly Not Modified".

---

## Q9 — No DB clients, DSN, real writes, or SQL apply?

**CONFIRMED — none found.**

**Imports in the updated loader (lines 4–16):**
```
argparse, hashlib, json, re, sys, unicodedata, datetime, pathlib, typing, zoneinfo, pandas
```
No `psycopg2`, `psycopg`, `sqlalchemy`, `asyncpg`, `pg8000`, `supabase`, `aiohttp`, or any
DB driver is present. The three new functions added in 010E use only `hashlib`, `json`, and `pandas`.

**New functions examined for DB risk:**

| Function | Lines | DB risk |
|---|---|---|
| `assign_excel_source_row_numbers()` | 246–252 | None — pure `pd.RangeIndex` assignment |
| `source_trace_manifest_sha256()` | 255–275 | None — SHA-256 over in-memory data |
| `source_trace_sample()` | 278–294 | None — DataFrame slice and rename |

Test `test_new_loader_does_not_import_productive_loader_or_db_clients` (lines 213–219) is
unchanged and still verifies absence of `psycopg2`, `sqlalchemy`, `DB_URL`, and productive
loader imports.

**`--apply` guard is unchanged** — raises `LoaderUsageError` immediately with no DB code
path behind it (lines 629–631).

---

## Q10 — Do the new tests cover risk W2?

**YES — W2 from the 010C post-audit is CLOSED by this patch.**

W2 was: *"`source_row_number` defined in DDL but not produced by dry-run loader; blocks DB
apply if not resolved."*

This patch closes W2 by:

**Two dedicated new tests:**

| Test | What it verifies |
|---|---|
| `test_source_row_number_maps_each_photo_row_to_excel_origin` | 6-row fixture → source_row_numbers 2–7; all 4 new flags true; `event_identity=["ID","SP Item ID"]`; `event_identity_replaced=False` |
| `test_source_row_number_is_stable_within_workbook_sheet` | Scrambled pandas index produces identical `trace_manifest_sha256` and `sample_rows` |

**Extensions to existing tests:**

| Test | Added assertions |
|---|---|
| `test_cli_writes_json_without_db` | `source_row_number_min=2`, `source_row_number_max=7`, `photo_rows_mapped=6` |
| `test_real_workbook_matches_required_010c_evidence` | `source_row_number_min=2`, `source_row_number_max=37909`, `source_row_number_distinct=37908`, `source_row_number_null_rows=0`, `photo_rows_mapped=37908`, `event_identity_replaced=False` |

**Four new blocking flags** added to `BLOCKING_FLAG_KEYS`:
```python
"source_row_number_present",
"source_row_number_complete",
"source_row_number_unique",
"source_row_number_matches_excel_rows",
```

All four are `true` in the 010E dry-run output. The verdict is `PASS_ROUTE_B_DRY_RUN`.

Total: **11 tests passing** (9 from 010C + 2 new).

---

## Q11 — Residual risks

### R1 — INFORMATIONAL: DDL and loader are now aligned; apply path is future work

`sql/15_kpione2_photo_raw_ddl.sql` defines `source_row_number bigint not null` with a
uniqueness constraint `(source_file_sha256, source_sheet, source_row_number)`. The loader now
produces `source_row_number` values that match this schema. The DDL and loader are aligned.

The DB apply path itself does not exist yet — a future RED-authorized phase must build it.
This is expected behavior and no longer a blocker.

### R2 — LOW: Full-sheet single read assumed by numbering

`assign_excel_source_row_numbers()` assigns row numbers based on the length of the DataFrame
after a full `pd.read_excel` call. If a future apply phase were to use chunked or paginated
reads, the row number offset would need to be carried across chunks. The current implementation
assumes a single complete read of the `Fotos` sheet, which is the correct approach for files
of the current size (37,908 rows). This is a design note for the future apply phase, not a
dry-run blocker.

### R3 — LOW: Trace manifest is only verifiable by re-running

`trace_manifest_sha256` covers all 37,908 mapping tuples but cannot be independently
verified without re-running the loader against the same workbook. The `sample_rows` (first 3
and last 3) provide human-readable boundary verification. At DB apply time, the manifest can
be re-verified before committing rows. Acceptable for a dry-run phase.

### R4 — INFORMATIONAL: governance files modified in same commit as implementation

`governance/ACTIVE_ORDER_LOCK.json` and `governance/PROJECT_STATUS_INDEX.json` were updated
in the same commit `cd33e66`. The commit author is `Bastian Antiman` — authorized governance
update. Content correctly reflects the phase transition (010D → 010E, with 010D marked as
merged). Same pattern as observed in 010C (W1 there). Not a blocker.

### R5 — UNCHANGED: CA3 (pipeline integration) still pending for a future ORANGE phase

The loader remains fully standalone. No integration into `load_control_gestion_raw_v17.py`
was attempted. This is correct scope for 010E. CA3 remains deferred.

---

## W2 closure summary

| Item | 010C status | 010E status |
|---|---|---|
| `source_row_number` defined in DDL | YES (sql/15_*) | YES (unchanged) |
| `source_row_number` produced by loader | NO (W2) | **CLOSED — YES** |
| Blocking flags in dry-run output | 18/18 | **22/22 (18 + 4 new)** |
| Tests for traceability | 0 | **2 new + 2 extended** |
| DB apply gate still required | YES | YES (unchanged) |

---

## Audit verdict

```json
{
  "audit_phase": "FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH",
  "auditor": "Claude",
  "commit_audited": "cd33e66",
  "prior_warning_w2_closed": true,
  "contract_valid": true,
  "contract_modified": false,
  "source_row_number_represents_excel_row_after_header": true,
  "source_row_number_stable_within_workbook_sheet": true,
  "source_row_number_enables_traceability": true,
  "source_row_number_does_not_replace_event_identity": true,
  "event_identity_remains_id_plus_sp_item_id": true,
  "grain_remains_photo_row_event_row_day_presence": true,
  "photo_rows_37908_maintained": true,
  "distinct_event_ids_5892_maintained": true,
  "db_apply": false,
  "sql_apply": false,
  "productive_loader_touched": false,
  "db_clients_found": false,
  "dsn_found": false,
  "real_writes": false,
  "tests_cover_w2_risk": true,
  "test_count": 11,
  "blocking_flags_passed": 22,
  "dry_run_verdict": "PASS_ROUTE_B_DRY_RUN",
  "residual_risks": [
    {
      "id": "R1",
      "severity": "INFORMATIONAL",
      "summary": "DDL and loader aligned; DB apply path is future RED-authorized work"
    },
    {
      "id": "R2",
      "severity": "LOW",
      "summary": "Chunked reads would require row-number offset; current single-read assumption is correct for this file size"
    },
    {
      "id": "R3",
      "severity": "LOW",
      "summary": "Trace manifest verifiable only by re-running; boundary samples cover human inspection"
    },
    {
      "id": "R4",
      "severity": "INFORMATIONAL",
      "summary": "Governance files updated in same commit; Bastian authored — authorized"
    },
    {
      "id": "R5",
      "severity": "INFORMATIONAL",
      "summary": "CA3 pipeline integration deferred to future ORANGE phase; correct scope"
    }
  ],
  "blockers": [],
  "verdict": "APPROVE"
}
```

---

## Summary

The 010E patch is minimal, correct, and closes the only medium-severity warning (W2) left
open from the 010C post-audit.

`source_row_number` is correctly defined as the 1-based Excel worksheet row after header
resolution. It is stable, independently of pandas index. It maps 1:1 to every photo row
(37,908 of 37,908). It does not participate in event identity. The event identity, grain
chain, and all 009F/010C baseline metrics are preserved exactly.

No DB capability was added. No productive loader was touched. No active contract was modified.
All 22 blocking flags pass. The dry-run verdict is `PASS_ROUTE_B_DRY_RUN`.

**This PR may be merged to main.**

---

*This file is a versioned audit artifact. It does not authorize DB apply, SQL apply, loader execution, or any RED action.
All RED actions require Bastián's explicit written authorization.*
