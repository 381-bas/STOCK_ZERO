# 014F/014G KPIONE Raw DB Staging Design — Minor Patches, No Apply

## Status and authority boundary

Phase: `014G_PATCH_SQL_REVIEW_MINOR_FINDINGS_NO_APPLY`

Final verdict candidate:

`014G_SQL_REVIEW_MICRO_PATCH_APPLIED_READY_FOR_PR_CLOSEOUT`

This remains a documentary proposal. No DB connection, no SQL/DDL apply, no
Supabase access, no loader or refresh execution, no data movement, no active
contract change and no UX change occurred.

Review artifacts:

- `sql/proposed/014F_kpione_raw_staging_design_NO_APPLY.sql`
- `sql/proposed/014F_validation_queries_NO_APPLY.sql`

The schema proposal retains its NO-APPLY header, `BEGIN` and terminal
`ROLLBACK`. The validation pack is SELECT-only. Neither file was executed.

## Evidence boundary

The design remains tied to:

- 014E manifest SHA256:
  `2c8afff44a171176af082241ad6e2a94be6a6d12be4228f3411a44edbbad8245`
- 014D remediation manifest SHA256:
  `b73483701f0dd767115a3d86d89fed2890b23da4d9269d959cb539e52d8ca089`
- dry-run batch `014E_1113973b41ee7618b4132006`
- 9 candidate files
- 239,159 source rows
- 10,089 exact duplicates
- 229,070 would-stage rows
- 35,287 distinct event IDs
- June 2026 coverage

No 014C, 014D or 014E artifact was modified.

## Minor findings applied

1. The future photo parser now has an explicit lossless anomaly convention.
2. Registry coverage is constrained to its calendar month.
3. `candidate_source_file_ids` replaces the ambiguous source-file array name.
4. Batch-file evidence adds `schema_signature` and `staged_row_count`.
5. Validation queries target live `STAGED` batches and state tombstone scope.
6. FK/CHECK duplicates are marked defense in depth, not primary evidence.
7. File coverage, per-file row counts and photo anomalies gain explicit checks.
8. Immutable manifest/daily reconciliation is a future apply blocker.
9. Static parity maps every 014E payload field to the proposed DDL.
10. Per-file reconciliation separates candidates from non-candidate evidence.
11. Photo anomaly profiling is separated from parsed invariant violations.

## Layer and grain boundary

```text
immutable manifest evidence
    -> batch + batch-file registry
    -> raw event-photo staging
    -> derived event/day-presence validation
    -> future compatible compliance mart
    -> future app integration
```

Staging grain remains one normalized raw event-photo row after exact dedupe,
scoped by `batch_id`. This layer does not own official VISITA, route joins,
compliance calculations, mart activation or app source switching.

## Batch registry

Table: `cg_raw.kpione_raw_ingest_batch_v1`

Grain: one row per explicit `batch_id`.

`candidate_source_file_ids` is a copied assertion of the exact
`include_candidate` IDs. It is not the file authority. The authoritative
per-file record is `cg_raw.kpione_raw_ingest_batch_file_v1`; quarantine,
compare-only and schema-rejected files live there and never in
`candidate_source_file_ids`.

The cardinality CHECK reconciles `candidate_source_file_ids` with
`source_files_count`. The validation pack reconciles the array with child rows
whose role is `include_candidate`.

### Monthly v1 coverage

`kpione_raw_ingest_batch_coverage_month_ck` requires both `coverage_start` and
`coverage_end` to resolve to registry `month`.

v1 assumes one complete monthly batch inside one calendar month. Partial
incremental batches and multi-month coverage are outside v1. Daily completeness
still belongs to immutable-manifest reconciliation, not this row-level CHECK.

### Tombstone lifecycle

`status` remains `STAGED` or `ROLLED_BACK`, with coherent
`rolled_back_at`/`rolled_back_by`. Partial unique indexes prevent two live
batches for the same month or candidate-manifest SHA. Child evidence remains
protected by `ON DELETE RESTRICT`.

## Batch-file authority

Table: `cg_raw.kpione_raw_ingest_batch_file_v1`

Grain: one row per `(batch_id, source_file_id)`.

It owns filename, source SHA, role, source row count, distinct event count,
file date range, `schema_signature` and `staged_row_count`.

014E does not emit a schema signature or per-file staged count. Therefore both
new fields are nullable in this documentary DDL:

- non-null `schema_signature` must be lowercase 64-character hex;
- non-null `staged_row_count` must be nonnegative and no greater than
  `row_count`.

A future apply gate must require both fields for every `include_candidate`
file. Nullability here records the evidence gap; it is not permission to apply
without that evidence.

## Lossless photo anomaly contract

Staging keeps:

- `n_fotos_raw text`
- `photo_sequence integer`
- `photo_total integer`

There is no positivity CHECK. The only parsed-pair CHECK is:

```text
photo_sequence IS NULL
OR photo_total IS NULL
OR photo_sequence <= photo_total
```

Future loader convention:

- always preserve `n_fotos_raw` intact;
- if parsing is valid, populate sequence and total;
- if raw text exists but parsing detects an anomaly such as `3/2`, set both
  parsed fields to `NULL` and keep the row;
- raw present with parsed NULL is an anomaly signal;
- report anomalies through validation queries/gates, never reject them from
  raw staging.

This also preserves values such as `0/0` for explicit reporting.

## Dedupe and source parity

The 014E dry-run dedupe key is `event_id + photo_row_hash`. Operational staging
uniqueness is `batch_id + photo_row_hash`, equivalent because the normalized
`photo_row_hash` input includes `event_id`.

The static parity contract maps:

- event/source/date/business/hash payload fields to staging;
- source filename and SHA to batch-file authority;
- `n_fotos` to the lossless three-field transformation;
- `dry_run_batch_id` to staging and registry `batch_id`;
- generated `loaded_at` and `loader_version` to explicit future-loader
  metadata, not 014E payload.

No 014E payload column is silently discarded.

## Validation query pack

All live-batch checks explicitly use `status = 'STAGED'`. `ROLLED_BACK`
tombstones are excluded and require separately authorized historical-audit
semantics.

Primary evidence checks cover:

- stable-hash conflicts per event;
- staging coverage and registry counts;
- candidate-file identity and role consistency;
- required future candidate file evidence;
- batch-file ranges inside registry coverage;
- staged-row reconciliation for each `include_candidate` file;
- zero staging rows for every non-candidate file;
- photo parsing/count anomaly profiles and parsed invariant violations.

Non-candidates may exist in the batch-file authority with quarantine,
compare-only or rejected roles, but they must never produce staging rows.

The `PHOTO ANOMALY PROFILE / REVIEW REQUIRED` may be nonzero because raw
anomalies are intentionally preserved. It does not automatically block raw
staging. It does block mart/compliance activation until reviewed under a future
authorized phase. `PARSED INVARIANT VIOLATION` remains expected zero.

Checks for orphan files, Monday week alignment and invalid roles duplicate
declarative FK/CHECK constraints. They are labeled defense in depth and are not
counted as primary evidence.

## Future apply blocker

The manifest/daily-total query remains deliberately non-executable because no
authorized immutable manifest projection exists.

`FUTURE APPLY BLOCKER CONDITION`: no future apply may proceed until the exact
manifest SHA, per-file row/distinct-event totals and daily totals reconcile
against an immutable projection. No table or join is invented in 014G.

## Remaining warnings

- SQL proposed and validation queries were not executed.
- Immutable manifest projection reconciliation remains a future apply blocker.
- Cross-batch activation and precedence remain a future contract.
- Photo anomaly profile requires future review before mart activation.

## Closeout direction

The SQL design is ready for PR 14 closeout review, not application. Any DB or
Supabase action requires a new, explicitly authorized phase.
