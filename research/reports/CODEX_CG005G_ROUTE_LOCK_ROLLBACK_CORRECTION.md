# CG005G route lock and rollback correction

## Verdict

READY_FOR_TARGETED_CLAUDE_REVALIDATION.

This phase corrected only the two open CG005F findings:

- F-08 advisory lock invalid `bigint,bigint` signature.
- F-04 rollback missing full postcheck before commit.

It did not reopen F-01, F-02, F-03, F-05, F-06, F-07, F-09, F-10 or F-11.

## Baseline

- Worktree: `C:/Users/basti/Desktop/STOCK_ZERO_WORKTREES/CG005G`
- Branch: `codex/CG005G-route-lock-rollback-correction`
- Baseline: `origin/main` at `e368f83c673509304a40992114046a5eafe11d6c`
- Implementation commit: `c440a253dbf0a35a388b8879c7e9627804991a56`

## F-08 correction

The advisory lock now uses a deterministic single signed bigint key and SQL shape:

```sql
select pg_advisory_xact_lock(%s)
```

The key is derived from SHA-256 over the weekly replacement contract marker, route policy version, source and effective week. It does not use Python native `hash()`, and both apply and rollback acquire the lock through the same helper.

## F-04 correction

Rollback now validates before commit:

- previous assignment is `ACTIVE`;
- failed assignment is `ROLLED_BACK`;
- exactly one `ACTIVE` assignment exists for the week;
- restored current hash matches the previous assignment hash;
- restored current row count matches the previous batch after exact duplicate exclusion;
- current surface contains no exact duplicate row_hash excess;
- weekly view points exclusively to the restored batch for the target week;
- resolved grains have zero missing, zero extra and zero duplicates;
- resolved hash matches the previous assignment hash.

Any failure raises before `commit`, rolls back the transaction and does not report success.

## Validation

- `python -m py_compile scripts/load_ruta_rutero_from_excel.py`: PASS.
- `python -m unittest discover -s tests -p "test_load_ruta_rutero_weekly_replace.py" -v`: 84 collected, 83 passed, 1 skipped, 0 failed.
- `python -m unittest discover -s tests -p "test_cg_readonly_extract.py" -v`: 21 collected, 20 passed, 1 skipped, 0 failed.
- `git diff --check`: PASS.

The skipped route test is the existing authorized-workbook check. The skipped read-only extractor test is the existing Windows symlink privilege skip.

## Safety

- No DB access.
- No Docker.
- No DDL.
- No SQL execution.
- No loader apply.
- No real rollback.
- No Supabase.
- No operational checkout modification.

## Metrics

- elapsed_minutes: 8
- files_outside_scope: 0
- unique_tests: 105
- test_executions: 5
- implementation_rounds: 1
- worktree_collisions: 0
- dirty_main_incidents: 0
- material_defects_closed: 2
