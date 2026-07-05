# 014F KPIONE Raw DB Staging Design — Patched, No Apply

## Status and authority boundary

Phase: `014F_PATCH_SQL_DESIGN_BEFORE_014G_NO_APPLY`

Verdict: `STAGING_DESIGN_PATCHED_READY_FOR_014G`

This remains a documentary proposal. No DB connection, no SQL/DDL apply, no
Supabase access, no loader or refresh execution, no data movement, no active
contract change and no UX change occurred.

Review artifacts:

- `sql/proposed/014F_kpione_raw_staging_design_NO_APPLY.sql`
- `sql/proposed/014F_validation_queries_NO_APPLY.sql`

The schema proposal keeps its required NO-APPLY header, `BEGIN` and terminal
`ROLLBACK`. The validation pack is SELECT-only and requires a separate,
explicitly authorized future phase. Neither file was executed.

## Evidence inputs

The patch preserves the committed evidence boundary:

- 014E manifest SHA256:
  `2c8afff44a171176af082241ad6e2a94be6a6d12be4228f3411a44edbbad8245`
- 014D remediation manifest SHA256:
  `b73483701f0dd767115a3d86d89fed2890b23da4d9269d959cb539e52d8ca089`
- dry-run batch: `014E_1113973b41ee7618b4132006`
- 9 candidate files
- 239,159 source rows
- 10,089 exact duplicate photo rows removed
- 229,070 would-stage rows
- 35,287 distinct event IDs
- coverage from 2026-06-01 through 2026-06-30
- zero candidate same-ID/different-hash conflicts

No 014C, 014D or 014E artifact was modified.

## Patched audit findings

| Finding | Patched design |
|---|---|
| `n_fotos` was too strict for raw evidence | Store `n_fotos_raw`, `photo_sequence` and `photo_total`; retain anomalies such as `0/0` |
| Rollback implied deletion | Add explicit `STAGED` / `ROLLED_BACK` tombstone state |
| File identity was duplicated in staging | Add `cg_raw.kpione_raw_ingest_batch_file_v1` and a composite FK |
| Active batches could be re-staged | Add partial unique indexes for active month and manifest SHA |
| Dedupe wording was ambiguous | Align dry-run key and staging operational uniqueness |
| Cross-row/cross-table rules were prose only | Add a separate validation query pack |
| Index set was broad and unscoped | Keep only batch-scoped access paths and required uniques |
| Verdict taxonomy was frozen prematurely | Keep `verdict text NOT NULL` without an enumerating CHECK |

## Layer boundary

```text
manifest-selected raw exports
    -> batch + batch-file evidence registry
    -> raw event-photo staging
    -> derived event/day-presence validation
    -> future compatible compliance mart
    -> future app integration
```

014F designs the registry and raw staging boundary only. It does not own
official VISITA, route joins, compliance denominators/numerators, cap, pending,
alert, mart activation or app source switching.

## Proposed batch registry

Table: `cg_raw.kpione_raw_ingest_batch_v1`

Grain: one row per explicit `batch_id`.

The existing manifest, source-count, row-count, coverage and executor evidence
is retained. The patch adds:

- `status text NOT NULL DEFAULT 'STAGED'`
- `rolled_back_at timestamptz`
- `rolled_back_by text`

`status` is restricted to `STAGED` or `ROLLED_BACK`. A staged row has both
rollback fields null. A rolled-back row has a timestamp and nonblank actor.
`verdict` remains required evidence text but has no rigid taxonomy CHECK.

Two partial unique indexes protect the active surface:

- `candidate_manifest_sha256` where `status = 'STAGED'`
- `month` where `status = 'STAGED'`

This prevents accidental active re-stage while allowing a future reviewed
replacement only after the prior batch has been tombstoned.

## Proposed batch-file registry

Table: `cg_raw.kpione_raw_ingest_batch_file_v1`

Grain: one row per `(batch_id, source_file_id)`.

Columns:

- batch and source-file IDs;
- source filename and SHA256;
- explicit role;
- row and distinct-event counts;
- minimum and maximum date.

Allowed roles:

- `include_candidate`
- `quarantine_truncation`
- `compare_only`
- `rejected_schema`

The primary key is `(batch_id, source_file_id)`. Its batch FK uses
`ON DELETE RESTRICT`. SHA format, nonnegative row counts and a coherent optional
date range are checked. `distinct_event_ids` may be null for files that cannot
produce a trustworthy count.

The registry-level `source_file_ids` remains the ordered candidate set from the
manifest. The validation pack reconciles it with child rows whose role is
`include_candidate`.

## Proposed raw event-photo staging

Table: `cg_raw.kpione_raw_event_photo_staging_v1`

Grain:

`one normalized raw event-photo row after exact dedupe, scoped by batch_id`

File name and file SHA move to the batch-file registry. Staging retains
`source_file_id` and references the child registry through:

```text
(batch_id, source_file_id)
    -> cg_raw.kpione_raw_ingest_batch_file_v1(batch_id, source_file_id)
```

Both the direct batch FK and composite file FK use `ON DELETE RESTRICT`.

### Lossless raw photo-number contract

The integer-only `n_fotos` proposal is removed. It becomes:

- `n_fotos_raw text`
- `photo_sequence integer`
- `photo_total integer`

Raw text is retained even when parsing is incomplete or anomalous. Parsed
values are nullable. When both exist, only `photo_sequence <= photo_total` is
enforced. There is deliberately no `photo_total > 0` rule: values such as
`0/0` are stored and surfaced by validation queries/gates instead of being
rejected at raw staging.

## Dedupe and conflict contract

The 014E dry-run exact-dedupe key is:

`event_id + photo_row_hash`

The staging operational unique is:

`batch_id + photo_row_hash`

These are equivalent inside a batch because `photo_row_hash` includes
`event_id` in its normalized hash payload. That inclusion must remain part of
the future loader/hash contract.

Additional rules:

- same local/date/brand with different event IDs remains valid;
- same event ID with different `event_stable_hash` is a blocking validation
  result, not a declarative CHECK;
- no implicit overwrite or latest-batch-wins rule exists across batches.

## Reduced index set

Retained/proposed access paths:

- unique `(batch_id, photo_row_hash)`
- unique `(batch_id, source_file_id, source_row_number)`
- index `(batch_id, fecha)`
- index `(batch_id, event_id)`
- partial unique active manifest SHA
- partial unique active month
- batch-file primary key `(batch_id, source_file_id)`

The isolated batch, source-file, verdict, fecha, week, cliente and cod_rt
indexes were removed. The composite indexes match the batch-scoped review and
validation queries; broader indexes require measured future demand.

## Validation query pack

`sql/proposed/014F_validation_queries_NO_APPLY.sql` documents read-only checks
for:

1. zero event IDs with multiple stable hashes inside a batch;
2. all staging dates inside registry coverage;
3. staging count equal to `registry.staged_rows`;
4. no staging source-file orphan;
5. Monday-aligned `week_start`;
6. valid file roles and exact candidate-file consistency;
7. a future manifest/daily-total reconciliation placeholder.

The final placeholder is intentionally non-executable because 014F has no
authorized immutable manifest projection to join. Inventing one would create
false assurance.

## Tombstone rollback

`batch_id` remains the rollback boundary, but rollback no longer means deleting
raw evidence.

A future authorized rollback must:

1. isolate downstream use of the exact batch;
2. transition the registry from `STAGED` to `ROLLED_BACK`;
3. set `rolled_back_at` and `rolled_back_by`;
4. preserve batch-file and staging evidence;
5. validate that downstream activation excludes the tombstoned batch.

All FKs use `RESTRICT`. There is no cascade, implicit overwrite or destructive
rollback SQL in 014F.

## Risks remaining for 014G

- The future loader must preserve the documented hash payload exactly.
- Cross-batch precedence and downstream activation remain undefined.
- Manifest/daily-total reconciliation needs an authorized immutable projection.
- A future migration must review privileges/RLS and operational naming.
- Partial active-month uniqueness assumes one active candidate batch per month;
  014G must confirm that product policy.
- The SQL and validation queries remain unexecuted proposals.

## Next phase

`014G_KPIONE_RAW_STAGING_SQL_ADVERSARIAL_REVIEW_NO_APPLY`

014G should challenge table names, lifecycle semantics, hash equivalence,
partial uniqueness, query completeness, transaction protocol and rollback
activation. It still must not apply SQL.
