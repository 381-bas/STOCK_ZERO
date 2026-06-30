# Route B Validation Gates

## GREEN gates

Allowed without production change:

- read governance files
- read contracts
- read dry-run evidence
- generate planning documents
- create local-only test harness plans

## ORANGE gates

Require cross-audit before proceeding:

- modifying `scripts/load_control_gestion_raw_v17.py`
- changing productive ingestion behavior
- changing compliance calculation logic
- creating adapter code intended for production path
- modifying expected outputs that define compliance behavior

Required evidence:

- contract impact note
- diff summary
- rollback path
- local test output
- dry-run output
- Claude/Codex cross-audit

## RED gates

Require explicit Bastián authorization:

- DB apply
- SQL apply against Supabase real
- data movement
- production cutover
- destructive cleanup
- credential or secret change
- real incremental ingestion

## Minimum pre-apply checklist

Before any future DB apply:

- source_row_number present
- source_row_number unique per source file/sheet
- event identity unchanged
- grain unchanged
- raw row count validated
- day_presence count validated
- rollback SQL reviewed
- DB target schema reviewed
- RLS/read grants reviewed
- dry-run and test harness PASS
- explicit written authorization recorded
