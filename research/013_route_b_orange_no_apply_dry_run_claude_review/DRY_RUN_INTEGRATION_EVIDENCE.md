# Route B ORANGE Dry-Run Integration Evidence

## Status

- Phase: `FAST_REFORM_013_ROUTE_B_ORANGE_NO_APPLY_DRY_RUN_CLAUDE_REVIEW`
- Mode: `ORANGE_NO_APPLY_DRY_RUN`
- Evidence date: 2026-07-01 America/Santiago
- Result: implementation and focused local tests complete
- Phase close: **not requested and not performed**

## Scope implemented

- Added `scripts/validate_route_b_denominator_dry_run.py`.
- Added `tests/test_route_b_denominator_dry_run.py`.
- Imported the existing `load_kpione2_photo_from_excel` analyzer without modifying it.
- Kept the active contract, productive loaders, SQL, weekly lab and productive runtime untouched.
- Accepted local input is a JSON object with `route_rows` and raw `photo_rows`.
- The CLI reads one local fixture and emits JSON to stdout. It has no JSON-output writer.

## Structured safety evidence

Every success, block and handled input-error payload declares:

```json
{
  "mode": "ORANGE_NO_APPLY_DRY_RUN",
  "db_access": {
    "used": false
  },
  "sql_apply": false,
  "writes_executed": false
}
```

Static isolation tests parse the validator imports and reject:

- `psycopg` / `psycopg2`
- `sqlalchemy`
- `supabase`
- `load_control_gestion_raw_v17`
- inventory/ruta loaders
- incremental/full refresh helpers
- `DB_URL`
- file-output calls through `Path.write_text`

The only existing Route B loader import is the explicitly allowed local analyzer
`load_kpione2_photo_from_excel`.

## Claude findings incorporated

| Finding | Implemented control |
|---|---|
| Binary-presence evidence was tautological | `day_presence_constant_check_used_as_proof=false`; proof is based on structural event-to-key cardinality and distinct grouped keys. |
| `photo_rows != distinct_event_ids` was a weak proxy | `legacy_aggregate_inequality_used_as_proof=false`; one-photo-per-event fixtures pass when the structural mapping is valid. |
| Blank denominator keys were unguarded | Blank/null `cod_rt` and `cliente_norm` block on both photo and route inputs. |
| Seven metrics lacked a local definition | The payload emits definitions for EXIGIDAS, VISITA, VISITA_REALIZADA, RAW, CAP, PENDIENTE and ALERTA. |
| Join/normalization/week alignment could change the denominator | Route and photo keys share the imported normalizer; unmatched photo day-presence blocks; Monday week starts and Sunday/Monday boundaries are tested. |
| Photo-count anomalies needed adversarial coverage | `0/0`, missing total, `3/2`, row/declared-total mismatch and exact duplicate photo rows block. |

## Grain evidence

The validator preserves:

```text
photo_row -> event_row -> day_presence
```

Structural checks prove:

1. every nonblank photo row maps to one trimmed event ID;
2. each event has one SP item, date, Monday week, normalized `cod_rt` and normalized `cliente_norm`;
3. event rows group to distinct `(week, date, cod_rt, cliente_norm)` day-presence keys;
4. photo day-presence can enrich only an existing route grain;
5. Route B never creates or expands denominator rows.

The adversarial three-event fixture produces:

- photo rows: 3
- event rows: 3
- day-presence rows: 1
- max events in that day-presence: 3

The legitimate one-photo-per-event fixture also passes, despite
`photo_rows == distinct_event_ids`.

## Focused test evidence

Command executed with the repository virtual environment:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_kpione2_photo_grain.py tests/test_route_b_denominator_dry_run.py -q
```

Observed after the validator, all adversarial tests and both evidence files
were present:

```text
22 passed, 8 subtests passed
```

The only warning was `PytestCacheWarning` because the existing `.pytest_cache`
directory is not writable. It did not affect collection, execution or results.

## Explicit non-actions

- No DB or Supabase access.
- No SQL or DDL apply.
- No data movement.
- No backfill.
- No production cutover.
- No loader, active-contract, weekly-lab or governance changes.
- No git staging, commit or push.
