# 014F DB Staging Design Patch Report — No Apply

## Executive summary

Verdict:

`STAGING_DESIGN_PATCHED_READY_FOR_014G`

Claude's blocking design findings were patched across the documentary DDL,
manifest, governance record and static contract. A SELECT-only validation pack
was added. No DB connection, no SQL/DDL apply, no Supabase access, no loader,
refresh, UX, contract or data change occurred.

## Preserved evidence

| Input | SHA256 | Verdict |
|---|---|---|
| 014E dry-run manifest | `2c8afff44a171176af082241ad6e2a94be6a6d12be4228f3411a44edbbad8245` | `DRY_RUN_READY_WITH_WARNINGS` |
| 014D remediation manifest | `b73483701f0dd767115a3d86d89fed2890b23da4d9269d959cb539e52d8ca089` | `REMEDIATION_READY_FOR_DRY_RUN_LOADER_WITH_WARNINGS` |

The design remains tied to 9 candidate files, 239,159 source rows, 10,089
exact duplicates, 229,070 would-stage rows, 35,287 distinct event IDs and full
June 2026 coverage.

## Patched schema

### Batch registry

`cg_raw.kpione_raw_ingest_batch_v1` now has a lifecycle tombstone:

- `status` defaults to `STAGED`;
- rollback changes it to `ROLLED_BACK`;
- `rolled_back_at` and `rolled_back_by` must match the state;
- `verdict` remains non-null but is not constrained to a premature taxonomy.

Partial unique indexes prevent two `STAGED` rows for the same month or
candidate-manifest SHA.

### Batch-file registry

`cg_raw.kpione_raw_ingest_batch_file_v1` owns source filename, SHA, role,
row count, distinct-event count and date range at grain
`(batch_id, source_file_id)`.

Roles distinguish candidate, quarantine, compare-only and schema-rejected
files. Staging uses a composite FK to this registry.

### Event-photo staging

`cg_raw.kpione_raw_event_photo_staging_v1` keeps raw event-photo grain and no
longer duplicates filename/SHA. The `n_fotos integer` proposal is replaced by:

- `n_fotos_raw text`
- `photo_sequence integer`
- `photo_total integer`

Parsed fields are nullable. If both exist, sequence may not exceed total.
There is no positivity constraint, so anomalies such as `0/0` remain auditable
raw evidence and are reported by later gates.

## Dedupe alignment

The dry-run key is `event_id + photo_row_hash`; the staging operational unique
is `batch_id + photo_row_hash`. The two align because `photo_row_hash` includes
`event_id` in its normalized input.

Same event ID with different stable hashes is validated by query, not encoded
as a misleading row-level constraint. Distinct event IDs at the same daily
business grain remain valid.

## Reduced indexes

The patched proposal keeps:

- unique batch/photo hash;
- unique batch/source-row identity;
- `(batch_id, fecha)`;
- `(batch_id, event_id)`;
- active-manifest and active-month partial uniques;
- the batch-file composite primary key.

Unscoped batch, source-file, verdict, fecha, week, cliente and cod_rt indexes
were removed pending measured workloads.

## Validation pack

`sql/proposed/014F_validation_queries_NO_APPLY.sql` covers:

- same event ID with different stable hashes;
- coverage violations;
- staging-versus-registry counts;
- orphan source files;
- Monday week alignment;
- allowed roles and candidate-file consistency.

It also documents, without inventing executable dependencies, the future
manifest/daily-total reconciliation gate. The pack was not executed.

## Tombstone rollback

Rollback preserves evidence: the future authorized operation changes registry
state to `ROLLED_BACK`, records actor/time and ensures downstream consumers
exclude that batch. Child rows remain protected by `ON DELETE RESTRICT`.

## Mart boundary

The patched staging remains upstream of event/day-presence derivation, route
reconciliation and any compatible compliance mart. It does not recalculate or
activate Control Gestión semantics, and it does not change the app.

## Remaining warnings

- SQL and validation queries are unexecuted proposals.
- Cross-batch activation and precedence require a later contract.
- Manifest/daily-total comparison still needs an immutable manifest projection.
- 014G must verify that one active batch per month is the intended policy.

## Next phase

`014G_KPIONE_RAW_STAGING_SQL_ADVERSARIAL_REVIEW_NO_APPLY`

The patched design is ready for adversarial static review, not application.
