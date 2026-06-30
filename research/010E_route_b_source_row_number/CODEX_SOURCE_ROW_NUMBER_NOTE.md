# 010E Route B Source Row Number Note

## Scope

This patch adds dry-run traceability only. It does not add a DB client, SQL execution, data
movement, or a productive loader integration.

## Definition

`source_row_number` is the 1-based physical row number in the source Excel worksheet after
header resolution:

- worksheet header row: `1`
- first data row: `2`
- assignment for a full sheet read: `2..(photo_rows + 1)`
- stability scope: the same source workbook and sheet read in the same row order

The value is assigned after `pandas.read_excel` reads the complete `Fotos` sheet. It does not
depend on the pandas DataFrame index.

## Traceability Evidence

The 010E dry-run proves:

- `photo_rows = 37908`
- `source_row_number_distinct = 37908`
- `source_row_number_null_rows = 0`
- `source_row_number_min = 2`
- `source_row_number_max = 37909`
- `photo_rows_mapped = 37908`
- `source_row_number_matches_excel_rows = true`

The evidence includes first/last row samples and a SHA-256 manifest over every mapping tuple:

`source_row_number, event_id, sp_item_id, photo_row_hash`

This keeps the evidence compact while covering all photo rows.

## Contract Impact

The active contract is not modified.

- input grain remains `photo_row`
- normalized grain remains `event_row`
- compliance grain remains `day_presence`
- forbidden assumption remains `one_excel_row_equals_one_visit`
- event identity remains `ID + SP Item ID`
- `source_row_number` is provenance metadata and does not participate in event identity

The existing event and day-presence metrics remain unchanged from the contractual baseline.

## Risk Classification

This is an ORANGE loader-logic change under `AGENT_AUTHORITY_MATRIX_V2`. It requires cross-audit
before the phase is merge-ready.

Residual risks:

- The numbering assumes the contract workbook format with the header on worksheet row 1.
- The trace manifest validates a dry-run mapping; it is not evidence of a DB insert.
- Productive pipeline integration remains outside this phase.

## Rollback

Rollback is local and additive:

1. Revert the 010E changes to `scripts/load_kpione2_photo_from_excel.py`.
2. Revert the 010E test additions.
3. Remove the 010E research artifacts.

No DB or SQL rollback is required because `db_apply=false`, `sql_apply=false`, and no real write
path was executed.
