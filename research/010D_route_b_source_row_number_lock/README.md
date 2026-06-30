# 010D Route B Source Row Number Lock

## Purpose

This phase closes 010C after merge and records the main technical warning from Claude post-audit.

## Closed phase

`FAST_REFORM_010C_ROUTE_B_REVIEW_AND_DRY_RUN_VALIDATION`

- PR: #28
- Main merge commit: `d834055`
- Included commits:
  - `3df063b`
  - `0c11930`

## Current Route B state

010C merged:

- additive dry-run loader
- unit tests
- review-only SQL DDL
- review-only rollback SQL
- dry-run evidence
- Claude post-audit

No DB apply was performed.

No SQL apply was performed.

No production loader was modified.

No active contract was modified.

## Source row number lock

Claude post-audit warning W2 is now a blocking prerequisite before real writes.

Before any future DB apply, SQL apply, loader write, production cutover or incremental real ingestion, Route B must define and validate `source_row_number`.

Minimum rule:

- `source_row_number` must identify the original Excel row position.
- `source_row_number` must be stable within the source workbook/sheet.
- `source_row_number` must support raw traceability.
- `source_row_number` must not replace event identity.
- Event identity remains `ID + SP Item ID`.
- Grain remains `photo_row -> event_row -> day_presence`.

## RED remains blocked

- DB apply
- SQL apply against Supabase real
- data movement
- production cutover
- destructive cleanup
- worktree deletion
