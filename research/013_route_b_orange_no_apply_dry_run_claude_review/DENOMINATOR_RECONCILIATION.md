# Route B Local-Sample Denominator Reconciliation

## Scope

This is a controlled local-sample definition for phase 013. It is not a
productive compliance contract, a database comparison or authorization to
apply Route B.

Reconciliation grain:

```text
(semana_inicio, cod_rt, cliente_norm)
```

Route rows are the sole denominator authority. Photo evidence may change only
the numerator-derived metrics of a route grain that already exists.

## Seven protected metrics

| Metric | Controlled local-sample definition |
|---|---|
| `EXIGIDAS` | Nonnegative integer supplied by the route fixture; unchanged before/after. |
| `VISITA` | Exact local alias of `EXIGIDAS`; unchanged before/after. |
| `VISITA_REALIZADA` | Count of distinct day-presence dates; equal to RAW in this sample. |
| `VISITA_REALIZADA_RAW` | Count of distinct dates in the before/after day-presence set. |
| `VISITA_REALIZADA_CAP` | `min(VISITA_REALIZADA_RAW, VISITA)`. |
| `PENDIENTE` | `max(VISITA - VISITA_REALIZADA_CAP, 0)`. |
| `ALERTA` | `CUMPLE` when RAW is at least VISITA; otherwise `INCUMPLE`. |

`before` is the route fixture's `existing_presence_dates`.

`after` is the set union of those dates and Route B day-presence dates.
Multiple events or photo rows on the same route/date contribute one presence.

## Golden controlled sample

### Grain A

- Key: `(2026-06-15, 100, MARCA A)`
- Route demand: 2
- Existing presence: `2026-06-16`
- Route B input: 3 distinct events on `2026-06-17`
- Structural Route B contribution: one new day-presence, not three visits

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| EXIGIDAS | 2 | 2 | 0 |
| VISITA | 2 | 2 | 0 |
| VISITA_REALIZADA | 1 | 2 | +1 |
| VISITA_REALIZADA_RAW | 1 | 2 | +1 |
| VISITA_REALIZADA_CAP | 1 | 2 | +1 |
| PENDIENTE | 1 | 0 | -1 |
| ALERTA | INCUMPLE | CUMPLE | controlled transition |

### Grain B

- Key: `(2026-06-22, 200, MARCA B)`
- Route demand: 1
- Existing presence: none
- Route B input: 1 event on Monday `2026-06-22`

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| EXIGIDAS | 1 | 1 | 0 |
| VISITA | 1 | 1 | 0 |
| VISITA_REALIZADA | 0 | 1 | +1 |
| VISITA_REALIZADA_RAW | 0 | 1 | +1 |
| VISITA_REALIZADA_CAP | 0 | 1 | +1 |
| PENDIENTE | 1 | 0 | -1 |
| ALERTA | INCUMPLE | CUMPLE | controlled transition |

### Aggregate result

- Route rows before: 2
- Route rows after: 2
- EXIGIDAS total before/after: 3 / 3
- VISITA total before/after: 3 / 3
- Denominator delta: 0
- Changes outside numerator metrics: none
- Unmatched Route B day-presence: none

The golden test asserts the per-grain values, not only aggregate totals.

## Structural anti-inflation proof

The validator does not infer correctness from either of these legacy signals:

- assigning `presence=1` and checking that it equals 1;
- requiring `photo_rows != distinct_event_ids`.

Instead it verifies:

- every photo row belongs to exactly one nonblank event ID;
- every event resolves to exactly one date/week/key tuple;
- day-presence is the set of distinct event tuples at daily grain;
- three events on the same day/key collapse to one day-presence;
- a legitimate one-photo-per-event sample remains valid.

## Join, normalization and week controls

- Route and photo keys use the same imported `normalize_key` implementation.
- Accents, case and repeated surrounding/interior spaces reconcile in the
  adversarial test.
- Leading zeroes in `cod_rt` are preserved.
- Blank/null route or photo `cod_rt` and `cliente_norm` block.
- A photo day-presence with no matching route grain blocks instead of creating
  a denominator row.
- `semana_inicio` must be a Monday.
- Sunday `2026-06-21` maps to week `2026-06-15`.
- Monday `2026-06-22` maps to week `2026-06-22`.
- One event spanning that Sunday/Monday boundary blocks as a multi-week event.

## Blocking anomaly coverage

The local validator blocks:

- missing/invalid `Foto N/Total`;
- nonpositive total such as `0/0`;
- sequence greater than total such as `3/2`;
- event photo-row count different from declared total;
- exact duplicate photo rows, including a case where the declared total alone
  would otherwise equal the row count;
- event identity/content conflicts inherited from the imported analyzer.

## Safety declaration

```json
{
  "db_access": {
    "used": false
  },
  "sql_apply": false,
  "writes_executed": false
}
```

Productive reconciliation, DB comparison, SQL/DDL, backfill, cutover and
rollback execution remain deferred to a future explicitly authorized RED
phase.

## Deferred RED blocker

Real photo-to-route normalization parity remains a future RED gate blocker.
This 013 validator applies the imported photo normalizer consistently to both
local fixture sides, which is sufficient for controlled local validation. It
does not prove parity against the productive route pipeline or SQL/view
normalization. Before any productive apply, Route B must compare photo-side
normalization against the real route/compliance normalization path and block on
unresolved key drift.
