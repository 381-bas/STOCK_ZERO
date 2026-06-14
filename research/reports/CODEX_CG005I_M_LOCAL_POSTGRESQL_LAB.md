# CG005J fix assignment insert arity and resume local lab

- Verdict: `PARTIAL`
- Correction commit: `e4651c50d918be6cd7d56fc0545cb83284209cd1`
- PostgreSQL: `17.10 (Debian 17.10-1.pgdg13+1)`
- Supabase contacted: `False`
- Main push/merge: `False`

## Root Cause

`create_week_assignment` had 10 `%s` placeholders for 11 parameters. The corrected query has 11 placeholders, with `assigned_by`, `replaces_ruta_batch_id`, and `notes` preserved in their intended positions. Local integration confirmed assignment `notes` persisted.

## CG005I-M

- CG005I: passed; SQL 11 applied locally and advisory-lock concurrency was proven.
- CG005J: passed; Snapshot A assignment is ACTIVE, notes persisted, hashes/rows/week view/grain checks passed.
- CG005K: passed; Snapshot B superseded A, became ACTIVE, and matched hashes/grain checks.
- CG005L: passed; rollback restored A and the failure database stayed fail-closed with B ACTIVE.
- CG005M: passed; clean-room run business signatures matched.

## Platform 008 Blocker

`sz_load_observation draft` rejected the candidate with `invalid_technical_code` on `input_file_name`. No ledger write occurred and no gates were opened.

## Attempts

1. `eb1433ff9edbca32fcc5b266d324737e0b1811d1`: BLOCK, assignment insert placeholder arity.
2. `e4651c50d918be6cd7d56fc0545cb83284209cd1`: PARTIAL, CG005I-M validated, PLATFORM_008 blocked.

## Safety

No DSN, password, row payload, customer, store, address, person values, or personal paths are recorded in this report/evidence.
