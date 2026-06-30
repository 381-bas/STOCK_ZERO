# Route B Integration Risk Matrix

| ID | Risk | Severity | Current status | Mitigation |
|---|---|---:|---|---|
| R1 | Treating Excel photo rows as visits | HIGH | Controlled | Preserve `photo_row -> event_row -> day_presence`; tests must reject one-row-one-visit |
| R2 | Losing Excel traceability | HIGH | Controlled in dry-run | Keep `source_row_number`; enforce uniqueness per file/sheet |
| R3 | Breaking event identity | HIGH | Controlled | Event identity remains `ID + SP Item ID` |
| R4 | Touching productive loader too early | HIGH | Blocked in 010G | Any change to `load_control_gestion_raw_v17.py` is ORANGE |
| R5 | Applying SQL before review | HIGH | RED blocked | SQL apply requires explicit Bastián authorization |
| R6 | Writing duplicate raw rows | HIGH | Future risk | Require unique `(source_file_sha256, source_sheet, source_row_number)` |
| R7 | Chunked read offset error | MEDIUM | Future risk | Carry offset if chunked reads are introduced |
| R8 | Inconsistent forward-only vs backfill scope | MEDIUM | Open | Define scope before any apply |
| R9 | Productive view regression | HIGH | Blocked | No productive views touched in planning phase |
| R10 | Rollback not ready before apply | HIGH | Future risk | Rollback plan required before RED apply |
