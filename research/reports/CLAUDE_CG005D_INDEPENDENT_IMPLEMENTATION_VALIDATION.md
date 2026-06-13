# CLAUDE CG005D — Independent Implementation Validation (No Apply)

**Task:** `CLAUDE_CG005D_INDEPENDENT_IMPLEMENTATION_VALIDATION`
**Validated commit:** `074ee5ba24fc02099205b25569a5374d3870ac72`
**HEAD at validation:** `1e08ac2`
**Verdict:** `BLOCKED_BEFORE_DDL`
**Quality:** `Q4_DECISION_GRADE` achieved · confidence **HIGH**
**Authority:** `ddl_authorized=false`, `apply_authorized=false`, `db_access=none`. No DB / Docker / Supabase / loader / `--apply` / SQL execution. No productive files modified.

This is an independent validation pending Codex re-validation. It does not authorize the DDL, an apply, or a refresh.

---

## Verdict rationale

Integrity is genuinely strong, but the **DDL-plus-loader unit cannot yet perform a correct, reproducible, reversible weekly replacement**. The earliest failing gate is the DDL itself (the resolved-view semantics and the loader/view column contract), so the verdict is `BLOCKED_BEFORE_DDL`; apply is independently blocked as well.

Three blockers, four highs, two mediums, two lows.

| Sev | Count | IDs |
| --- | ---: | --- |
| BLOCKER | 3 | F-01, F-02, F-03 |
| HIGH | 4 | F-04, F-05, F-06, F-07 |
| MEDIUM | 2 | F-08, F-09 |
| LOW | 2 | F-10, F-11 |

## Validation A — Integrity (PASS)

- `python -m py_compile scripts/load_ruta_rutero_from_excel.py` → exit **0**.
- Weekly suite → **Ran 46, OK** (0 failures, 0 skips; `test_46` executed because the workbook was present and its hash matched — handled entirely by the test; `data/` was not read by this validation).
- Read-only extractor suite → **Ran 21, OK (skipped=1)** — the skip is the known Windows symlink-privilege test.
- Default is no-write (`validate_cli_args` coerces non-`--apply` to dry-run; `test_01` asserts `connect_db` not called).
- `--apply` requires week + workbook SHA-256 + exact confirm token + `--json-out` + explicit `--db_url` + postcheck enabled.
- No `shell=True`; no free-SQL CLI (`test_31`); DELETE is parameterized by `source`; DSN redacted (`test_28/29`).
- `sql/11` begins with `-- NO APPLY IN CG005C` and is wrapped `begin; … rollback;`.

**Integrity tests pass but mock the DB and assert on SQL-file substrings** — they do not exercise the resolver semantics, the `cod_rt_norm` contract, the lifecycle supersede, or rollback restoration. Green tests therefore do **not** establish apply-readiness.

## Validation B — Replacement semantics

| Point | Class |
| --- | --- |
| 1. explicit batch used exclusively as authoritative snapshot | **PARTIAL** |
| 2. deleted grain cannot reappear from a legacy batch of the same week | **FAILED** |
| 3. the 70 deleted grains cease to exist in the resolved surface | **FAILED** |
| 4. historical batches audit-only, do not silently fill gaps | **FAILED** |
| 5. resolved view yields one grain `week + COD_RT + CLIENTE` | **PROVEN** |

Root cause (**F-01**): `v_rr_frecuencia_base_resuelta_v2` joins `v_ruta_rutero_load_batch_week_v2`, which is the **union** of explicit-assignment and legacy-inferred week mappings. `legacy_inferred` excludes a batch only when *that* batch has an ACTIVE assignment — not when the *week* is explicitly assigned. So legacy batch 19 (`loaded_at` 2026-06-08 → Santiago Monday 2026-06-08) keeps feeding the explicitly-assigned week, and any grain dropped in the new batch survives from batch 19. The resolved view reads immutable history (`cg_core.ruta_rutero_load_rows`), so deleting public rows does nothing to suppress it, and `run_postcheck` has no stale/baja check to catch it.

## Validation C — Assignment lifecycle (cannot execute the cycle)

The required cycle (A ACTIVE → B supersedes A → A SUPERSEDED → B ACTIVE → rollback reactivates A) is **not implemented** (**F-03**):

- `create_week_assignment` only INSERTs status `ACTIVE` and is called with `replaces_ruta_batch_id=None` (loader:1110).
- No statement ever sets a prior ACTIVE assignment to `SUPERSEDED`.
- A second same-week apply inserts a second ACTIVE row → violates the unique partial index `ux_ruta_rutero_week_assignment_active` → `IntegrityError` → rollback.
- No locking (no advisory lock / `SELECT … FOR UPDATE`).

Only the **first-ever** load to a fresh week succeeds; the retroactive *replacement* the policy is named for does not.

## Validation D — Multirow precedence (source_row / order dependent; not approved)

`logical_rank` ORDER BY = EXPLICIT first, `visitas` desc, day-sum desc, **`source_row` asc**, `row_hash` asc.

Isolated simulation of the ORDER BY (no productive file modified):

| Fixture | Result |
| --- | --- |
| F-D1 same rows, inverted order (exact dup) | ORDER-INDEPENDENT (exact dedup collapses to identical content) |
| F-D2 same freq & days, different persons | **ORDER-DEPENDENT** (winner flips REPO_A ↔ REPO_B with `source_row`) |
| F-D3 different frequency | ORDER-INDEPENDENT (business field decides) |

So precedence is stable only when a business field (`visitas`/day-sum) differs; for the person-only-differs class it depends on `source_row`, contradicting the contract's "`source_row` must not be material," and it collapses multirow groups while **`BD-CG005B-001` is still open** (**F-07**).

## Validation E — Hashes and invariants

Six hashes inventoried (workbook SHA-256; schema signature; `row_hash`; Python current-surface SHA-256 over `source_row|row_hash`; DB current-surface MD5 over `row_hash`; assignment `resolved_surface_hash` = the Python one).

- **The written hash cannot be compared exactly to a DB recomputation** (**F-06**): Python SHA-256 over `source_row|row_hash` vs DB MD5 over `row_hash` alone — different algorithm and columns — and no equality check exists. `resolved_surface_hash` is also misnamed: it is the *current public* surface, not the *resolved weekly* view.
- **`POSTCHECK_CONTRACT` over-claims** (**F-05**): 10 names declared; `run_postcheck` implements 6; `current_surface_hash_matches` and `no_stale_rows_from_previous_snapshot` are **not** implemented; the duplicate-grain check references `cod_rt_norm`, which the view does not expose. A name in the contract does not prove the check exists.

## Validation F — Transaction

- **Atomic**: `autocommit=False`; all writes in one transaction; `conn.commit()` once; `except: conn.rollback(); raise` (`test_22/23/24/25`). Failure after DELETE or after the assignment rolls back cleanly.
- `verify_db_contract` enforces a writable connection (`show transaction_read_only`).
- **Not concurrency-safe** (**F-08**): no advisory lock; two same-week applies serialize via row locks + the unique index, risking deadlock/opaque failure (fails closed).
- Minor (**F-11**): redundant explicit `BEGIN` after `autocommit=False` and after read queries (nested-BEGIN warning; cosmetic).

## Validation G — Rollback (interface only)

- Interface present; arguments validated (week/db_url/failed_assignment_id/expected hash/confirm token).
- SQL rollback is **commented design only**.
- **Restoration not implemented and not tested** (**F-04**): after `verify_db_contract`, `run_weekly_replacement_rollback` unconditionally raises `rollback_execution_requires_separate_authorization`. It never restores `public.ruta_rutero`, reactivates the previous assignment, or marks the failed assignment `ROLLED_BACK`. `test_40` only checks the function is callable.

## Validation H — CLI guards

| Guard | Class |
| --- | --- |
| skip source-check | **PARTIAL** — `--skip-source-check` not rejected with `--apply` (**F-09**) |
| wrong week | **PARTIAL** — Monday-only; not cross-checked vs workbook/contract (**F-10**) |
| omit hash | STRONG |
| omit postcheck | STRONG |
| incorrect confirm token | STRONG |
| implicit DB URL | STRONG |
| incompatible DB objects | STRONG but INCONSISTENT (requires `cod_rt_norm`; see F-02) |
| accidental legacy path | STRONG (suppressed no-op flags; old write path removed) |

## Validation I — Compatibility

- **`public.ruta_rutero` columns: COMPATIBLE.** The new `PUBLIC_INSERT_SQL` column list is identical to the prior loader revision; neither writes `*_norm` columns, so `app/db.py` (`rr.cliente_norm`) and `sql/03` MV behavior is unchanged by column shape (the change is the write *strategy*, not the shape). This is **not** a regression.
- **Resolved view vs refresh: COMPATIBLE.** `refresh_control_gestion_v2_incremental.py` reads `f.cod_rt` and `f.cliente_norm`; `sql/11` exposes both.
- **Resolved view vs loader: INCOMPATIBLE** (**F-02**). Loader/postcheck require `cod_rt_norm`; the view exposes `cod_rt`. `verify_db_contract` returns `missing_resolved_view_column:cod_rt_norm` and aborts apply; the duplicate-grain postcheck would raise `UndefinedColumn`.
- **Canonical builder: COMPATIBLE** (already uses an explicit route plan; does not consume the assignment table).
- **Tracked views: REQUIRES_CHANGE** — `sql/11` redefines the two `cg_core` views to depend on the new assignment table; that replacement carries F-01 and F-02.

## Findings matrix (summary)

| ID | Sev | Component | One-line | blocks_ddl | blocks_apply |
| --- | --- | --- | --- | :-: | :-: |
| F-01 | BLOCKER | resolved view | legacy-inferred batches backfill deleted grains in an assigned week | ✓ | ✓ |
| F-02 | BLOCKER | loader vs view | loader/postcheck require `cod_rt_norm`; view exposes `cod_rt` | ✓ | ✓ |
| F-03 | BLOCKER | apply/assignment | no supersede; `replaces=None`; 2nd same-week apply violates unique ACTIVE | ✗ | ✓ |
| F-04 | HIGH | rollback | postcommit restoration is an unimplemented stub | ✗ | ✓ |
| F-05 | HIGH | postcheck | `POSTCHECK_CONTRACT` over-claims; hash/stale checks absent; broken column | ✗ | ✓ |
| F-06 | HIGH | hash contract | written hash not comparable to any DB hash; misnamed | ✗ | ✓ |
| F-07 | HIGH | resolver tiebreak | `source_row`-dependent multirow collapse; `BD-CG005B-001` open | ✗ | ✓ |
| F-08 | MEDIUM | apply | no advisory lock / concurrency control | ✗ | ✗ |
| F-09 | MEDIUM | CLI | `--skip-source-check` not rejected with `--apply` | ✗ | ✗ |
| F-10 | LOW | week guard | week not cross-checked vs workbook/contract | ✗ | ✗ |
| F-11 | LOW | transaction | redundant explicit `BEGIN` | ✗ | ✗ |

## Readiness against the four objectives

1. **Future DDL application** — **BLOCKED**: the resolved view (F-01) and the loader/view column contract (F-02) must change first.
2. **DB-aware dry-run** — dry-run path is safe and deterministic, but a DB-aware dry-run would still surface F-02 at `verify_db_contract`.
3. **Controlled weekly apply** — **BLOCKED**: F-01..F-07.
4. **Postcommit rollback** — **BLOCKED**: not implemented (F-04).

## What is genuinely good (do not regress)

Strong no-write default; strong apply guards for hash/token/week/db_url/postcheck; parameterized source-scoped DELETE with no `ON CONFLICT` upsert; atomic transaction that fails closed; immutable history preserved; DSN redaction; suppressed legacy flags; a sound assignment-table DDL (Monday + status checks, unique ACTIVE index, FKs); and a correct one-grain-per-week resolution shape.

## Required corrections before re-validation (not implemented here)

1. Resolve each week from the single winning batch (join `v_ruta_rutero_latest_week_batch_v2`) or exclude `legacy_inferred` for weeks with an ACTIVE assignment; add a resolved-grain-vs-assigned-batch postcheck (F-01).
2. Reconcile the resolved-view column contract (`cod_rt` vs `cod_rt_norm`) across loader, postcheck, and view (F-02).
3. Implement in-transaction supersede of the prior ACTIVE assignment and set `replaces_ruta_batch_id` (F-03).
4. Implement and test postcommit rollback restoration (F-04).
5. Implement the declared hash/stale postchecks with one canonical Python↔SQL surface hash (F-05, F-06).
6. Make the multirow tiebreak `source_row`-independent or block collapse until `BD-CG005B-001` is decided (F-07).
7. Lower-priority: advisory lock (F-08); reject `--skip-source-check` with `--apply` (F-09); week cross-check (F-10); drop redundant `BEGIN` (F-11).

## Unresolved (need an authorized read-only DB phase)

- Whether the currently-deployed resolved view exposes `cod_rt_norm` (changes when F-02 manifests).
- Exact count of multirow groups that tie on `visitas` and day-sum (the F-07 order-dependent subset).
- Confirm `public.ruta_rutero.*_norm` are generated columns (assumed from identical old/new insert lists).

---

**Next step:** ChatGPT / Bastián review; Codex independent re-validation.
