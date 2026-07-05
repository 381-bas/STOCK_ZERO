# 014G Minor SQL Review Patch Report — No Apply

## Executive summary

Final verdict candidate:

`014G_SQL_REVIEW_MICRO_PATCH_APPLIED_READY_FOR_PR_CLOSEOUT`

The minor 014G findings were applied to the documentary DDL, SELECT-only
validation pack, manifest and static contract. No DB connection, no SQL/DDL
apply, no Supabase access, no loader, refresh, UX, contract or data change
occurred.

## Patches applied

- Added the future loader convention for raw-present/parsed-NULL photo
  anomalies, including the `3/2` example.
- Added `kpione_raw_ingest_batch_coverage_month_ck`; v1 is calendar-month
  scoped and excludes partial incremental or multi-month coverage.
- Renamed the candidate array to `candidate_source_file_ids`.
- Defined batch-file authority for candidates and non-candidate roles.
- Added nullable `schema_signature` and `staged_row_count` with format/count
  constraints and a future candidate-file apply gate.
- Scoped live validation queries to `status = 'STAGED'`.
- Marked checks duplicating FK/CHECK enforcement as defense in depth.
- Added file-range, per-file staged-count and photo anomaly reconciliation.
- Elevated immutable manifest/daily reconciliation to a
  `FUTURE APPLY BLOCKER CONDITION`.
- Added static 014E payload-to-DDL parity coverage.
- Split per-file reconciliation into candidate count matching and
  non-candidate zero-staging evidence.
- Split preserved photo anomaly profiling from the expected-zero parsed
  invariant violation.

## Evidence interpretation

014E contains the complete payload column list but no `schema_signature` and no
per-file post-dedupe staged count. Those new fields are therefore nullable in
the proposal. A future apply must populate and validate both for
`include_candidate` files; their current nullability does not weaken that gate.

`candidate_source_file_ids` is only the copied candidate assertion. The
batch-file child table is authoritative for all files and roles. Non-candidates
remain outside the array.

## Photo anomaly behavior

`n_fotos_raw` is always retained. Valid parses populate `photo_sequence` and
`photo_total`. If a raw value such as `3/2` is anomalous, a future loader keeps
the raw value and writes both parsed fields as NULL. Raw-present/parsed-NULL is
then reported as an anomaly rather than rejected.

There is no positivity CHECK. `0/0`, parsed-null signals, impossible
sequence/total order and event-row-count mismatches are surfaced by the
validation pack.

## Validation semantics

Live checks filter `status = 'STAGED'`; tombstones are intentionally outside
this operational gate. Orphan-file, week-start and invalid-role queries are
defense in depth only because declarative constraints already enforce them.

Primary evidence includes registry and per-file counts, file coverage,
candidate identity, required file metadata, event hash stability and photo
anomaly reconciliation.

Candidate files reconcile their staging counts against `staged_row_count`.
Non-candidate files may remain in batch-file evidence but must always reconcile
to zero staging rows.

The photo anomaly profile may legitimately be nonzero because raw anomalies
are preserved. Those profile counts require future review and block
mart/compliance activation until resolved or explicitly accepted. Only
`photo_sequence_greater_than_total` is a parsed invariant violation with an
expected-zero result.

## Payload parity

The static mapping consumes the actual 014E `payload_columns` list and proves:

- every payload field has a declared registry, batch-file or staging target;
- every critical staging field has a payload or generated origin;
- filename/SHA relocation, photo parsing and generated loader metadata are
  intentional and documented.

## Remaining warnings

- SQL proposed and validation queries were not executed.
- Immutable manifest projection reconciliation remains a future apply blocker.
- Cross-batch activation and precedence remain a future contract.
- Photo anomaly profile requires future review before mart activation.

## Closeout

The design is ready for PR 14 closeout review. It is not authorized for DB,
Supabase or SQL application.
