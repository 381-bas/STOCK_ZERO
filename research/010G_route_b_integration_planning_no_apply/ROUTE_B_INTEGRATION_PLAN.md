# Route B Integration Plan — No Apply

## Integration principle

Route B must enter production through a controlled adapter layer, not by silently replacing existing productive logic.

The current dry-run chain is:

`Excel Fotos photo_row -> event_row -> day_presence`

The production chain must preserve:

- input grain: `photo_row`
- normalized grain: `event_row`
- compliance grain: `day_presence`
- event identity: `ID + SP Item ID`
- provenance: `source_row_number`

## Non-goals for this phase

This phase does not:

- modify `scripts/load_control_gestion_raw_v17.py`
- write to Supabase
- apply SQL
- change active contracts
- change productive views

## Future integration stages

### Stage 1 — Adapter design

Define how dry-run payload fields map to future raw table columns.

Required mapping areas:

- source file metadata
- source sheet
- source row number
- event identity
- photo row hash
- stable event hash
- normalized cod_rt
- normalized cliente
- fecha
- week_start
- photo-level fields
- event-level derived fields

### Stage 2 — Test-only adapter

Create a local-only adapter test harness.

No DB writes.

Expected result:

- dry-run payload can be transformed into candidate raw rows
- row counts remain stable
- source_row_number uniqueness remains stable
- event/day aggregates remain unchanged

### Stage 3 — ORANGE production loader review

Only after cross-audit, define whether `load_control_gestion_raw_v17.py` needs a patch or whether Route B remains separate.

This stage requires ORANGE review because it touches productive ingestion logic.

### Stage 4 — RED apply planning

Only after ORANGE approval, prepare a DB apply plan.

DB apply remains RED and requires explicit Bastián authorization.

## Future decision points

1. Keep Route B as standalone loader or integrate into existing v17 loader.
2. Decide raw target table and schema ownership.
3. Decide rollback mechanism before any write.
4. Decide whether historical backfill is allowed or forward-only.
5. Decide production cutover criteria.
