# Codex Execution Brief — 010A Route B

## Role

Codex acts as implementation executor only after the 010A lock is merged.

## Current phase

Do not implement Route B during 010A.

## After 010A merge

Codex may work in a new explicit implementation branch, subject to the active authority matrix.

## Required constraints

- Preserve `photo_row -> event_row -> day_presence`.
- Do not assume `one_excel_row_equals_one_visit`.
- Do not apply DB changes.
- Do not execute SQL against Supabase real.
- Do not patch loader logic unless ORANGE audit has been completed.
- Do not merge to main.
- Do not delete worktrees.

## Expected future evidence

- diff summary
- tests or dry-run validation
- contract impact note
- rollback note
- Claude audit response for ORANGE areas
- no-DB-apply evidence
