-- 014F VALIDATION QUERIES NO-APPLY ONLY
-- DO NOT RUN WITHOUT AUTHORIZED FUTURE PHASE
-- Supabase apply not authorized
--
-- Read-only proposal. Every query must be scoped to an explicitly authorized
-- batch_id in a future phase. No query in this file was executed in 014F/014G.
-- All checks below target live batches with status = 'STAGED'. Tombstoned
-- ROLLED_BACK batches are intentionally excluded and require separate audit
-- semantics if a later phase authorizes historical validation.

-- Replace the sentinel only in an authorized future review.

-- 1. PRIMARY EVIDENCE. Expected: zero rows.
-- One event_id must not map to multiple event_stable_hash values in a live batch.
SELECT
    s.batch_id,
    s.event_id,
    count(DISTINCT s.event_stable_hash) AS stable_hash_count
FROM cg_raw.kpione_raw_event_photo_staging_v1 AS s
JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
    ON b.batch_id = s.batch_id
   AND b.status = 'STAGED'
WHERE s.batch_id = '<AUTHORIZED_BATCH_ID>'
GROUP BY s.batch_id, s.event_id
HAVING count(DISTINCT s.event_stable_hash) > 1;

-- 2. PRIMARY EVIDENCE. Expected: zero rows.
-- Every staging fecha must remain inside the live registry coverage.
SELECT
    s.batch_id,
    s.source_file_id,
    s.source_row_number,
    s.fecha,
    b.coverage_start,
    b.coverage_end
FROM cg_raw.kpione_raw_event_photo_staging_v1 AS s
JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
    ON b.batch_id = s.batch_id
   AND b.status = 'STAGED'
WHERE s.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND s.fecha NOT BETWEEN b.coverage_start AND b.coverage_end;

-- 3. PRIMARY EVIDENCE.
-- Expected: one row with count_matches_registry = true.
SELECT
    b.batch_id,
    b.staged_rows AS expected_staged_rows,
    count(s.batch_id) AS actual_staged_rows,
    count(s.batch_id) = b.staged_rows AS count_matches_registry
FROM cg_raw.kpione_raw_ingest_batch_v1 AS b
LEFT JOIN cg_raw.kpione_raw_event_photo_staging_v1 AS s
    ON s.batch_id = b.batch_id
WHERE b.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND b.status = 'STAGED'
GROUP BY b.batch_id, b.staged_rows;

-- 4. DEFENSE IN DEPTH ONLY — not primary evidence.
-- This duplicates the composite FK. Expected: zero rows.
SELECT
    s.batch_id,
    s.source_file_id,
    count(*) AS orphan_rows
FROM cg_raw.kpione_raw_event_photo_staging_v1 AS s
JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
    ON b.batch_id = s.batch_id
   AND b.status = 'STAGED'
LEFT JOIN cg_raw.kpione_raw_ingest_batch_file_v1 AS f
    ON f.batch_id = s.batch_id
   AND f.source_file_id = s.source_file_id
WHERE s.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND f.source_file_id IS NULL
GROUP BY s.batch_id, s.source_file_id;

-- 5. DEFENSE IN DEPTH ONLY — not primary evidence.
-- This duplicates kpione_raw_event_photo_week_start_ck. Expected: zero rows.
SELECT
    s.batch_id,
    s.source_file_id,
    s.source_row_number,
    s.fecha,
    s.week_start
FROM cg_raw.kpione_raw_event_photo_staging_v1 AS s
JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
    ON b.batch_id = s.batch_id
   AND b.status = 'STAGED'
WHERE s.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND s.week_start <> date_trunc('week', s.fecha)::date;

-- 6a. DEFENSE IN DEPTH ONLY — not primary evidence.
-- This duplicates the batch-file role CHECK. Expected: zero rows.
SELECT
    f.batch_id,
    f.source_file_id,
    f.role
FROM cg_raw.kpione_raw_ingest_batch_file_v1 AS f
JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
    ON b.batch_id = f.batch_id
   AND b.status = 'STAGED'
WHERE f.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND f.role NOT IN (
      'include_candidate',
      'quarantine_truncation',
      'compare_only',
      'rejected_schema'
  );

-- 6b. PRIMARY EVIDENCE.
-- Expected: one row with candidate_file_count_matches = true.
SELECT
    b.batch_id,
    b.source_files_count AS expected_candidate_files,
    count(*) FILTER (WHERE f.role = 'include_candidate') AS actual_candidate_files,
    b.source_files_count
        = count(*) FILTER (WHERE f.role = 'include_candidate')
        AS candidate_file_count_matches
FROM cg_raw.kpione_raw_ingest_batch_v1 AS b
LEFT JOIN cg_raw.kpione_raw_ingest_batch_file_v1 AS f
    ON f.batch_id = b.batch_id
WHERE b.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND b.status = 'STAGED'
GROUP BY b.batch_id, b.source_files_count;

-- 6c. PRIMARY EVIDENCE. Expected: zero rows.
-- candidate_source_file_ids is a candidate-copy assertion. batch_file is the
-- per-file authority; non-candidates exist there with their explicit roles.
WITH expected AS (
    SELECT
        b.batch_id,
        unnest(b.candidate_source_file_ids) AS source_file_id
    FROM cg_raw.kpione_raw_ingest_batch_v1 AS b
    WHERE b.batch_id = '<AUTHORIZED_BATCH_ID>'
      AND b.status = 'STAGED'
),
actual AS (
    SELECT
        f.batch_id,
        f.source_file_id
    FROM cg_raw.kpione_raw_ingest_batch_file_v1 AS f
    JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
        ON b.batch_id = f.batch_id
       AND b.status = 'STAGED'
    WHERE f.batch_id = '<AUTHORIZED_BATCH_ID>'
      AND f.role = 'include_candidate'
)
SELECT
    coalesce(e.batch_id, a.batch_id) AS batch_id,
    coalesce(e.source_file_id, a.source_file_id) AS source_file_id,
    CASE
        WHEN e.source_file_id IS NULL THEN 'unexpected_child_candidate'
        WHEN a.source_file_id IS NULL THEN 'missing_child_candidate'
    END AS mismatch
FROM expected AS e
FULL OUTER JOIN actual AS a
    ON a.batch_id = e.batch_id
   AND a.source_file_id = e.source_file_id
WHERE e.source_file_id IS NULL
   OR a.source_file_id IS NULL;

-- 6d. PRIMARY EVIDENCE. Expected: zero rows.
-- Staging may use include_candidate files only.
SELECT
    s.batch_id,
    s.source_file_id,
    f.role,
    count(*) AS staged_rows
FROM cg_raw.kpione_raw_event_photo_staging_v1 AS s
JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
    ON b.batch_id = s.batch_id
   AND b.status = 'STAGED'
JOIN cg_raw.kpione_raw_ingest_batch_file_v1 AS f
    ON f.batch_id = s.batch_id
   AND f.source_file_id = s.source_file_id
WHERE s.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND f.role <> 'include_candidate'
GROUP BY s.batch_id, s.source_file_id, f.role;

-- 6e. FUTURE APPLY GATE. Expected: zero rows.
-- 014E did not emit these fields, but future live candidate files must have
-- schema and per-file staging evidence before apply can be authorized.
SELECT
    f.batch_id,
    f.source_file_id,
    f.schema_signature,
    f.staged_row_count,
    f.fecha_min,
    f.fecha_max
FROM cg_raw.kpione_raw_ingest_batch_file_v1 AS f
JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
    ON b.batch_id = f.batch_id
   AND b.status = 'STAGED'
WHERE f.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND f.role = 'include_candidate'
  AND (
      f.schema_signature IS NULL
      OR f.staged_row_count IS NULL
      OR f.fecha_min IS NULL
      OR f.fecha_max IS NULL
  );

-- 7. PRIMARY EVIDENCE. Expected: zero rows.
-- Each available per-file date range must stay inside live batch coverage.
SELECT
    f.batch_id,
    f.source_file_id,
    f.fecha_min,
    f.fecha_max,
    b.coverage_start,
    b.coverage_end
FROM cg_raw.kpione_raw_ingest_batch_file_v1 AS f
JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
    ON b.batch_id = f.batch_id
   AND b.status = 'STAGED'
WHERE f.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND (
      (
          f.fecha_min IS NOT NULL
          AND f.fecha_min NOT BETWEEN b.coverage_start AND b.coverage_end
      )
      OR (
          f.fecha_max IS NOT NULL
          AND f.fecha_max NOT BETWEEN b.coverage_start AND b.coverage_end
      )
  );

-- 8a. PRIMARY EVIDENCE.
-- Expected: one row per include_candidate file with per_file_count_matches = true.
SELECT
    f.batch_id,
    f.source_file_id,
    f.role,
    f.staged_row_count AS expected_staged_rows,
    count(s.batch_id) AS actual_staged_rows,
    f.staged_row_count IS NOT NULL
        AND f.staged_row_count = count(s.batch_id)
        AS per_file_count_matches
FROM cg_raw.kpione_raw_ingest_batch_file_v1 AS f
JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
    ON b.batch_id = f.batch_id
   AND b.status = 'STAGED'
LEFT JOIN cg_raw.kpione_raw_event_photo_staging_v1 AS s
    ON s.batch_id = f.batch_id
   AND s.source_file_id = f.source_file_id
WHERE f.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND f.role = 'include_candidate'
GROUP BY
    f.batch_id,
    f.source_file_id,
    f.role,
    f.staged_row_count;

-- 8b. PRIMARY EVIDENCE.
-- Expected: one row per non-candidate file with
-- non_candidate_has_zero_staged_rows = true. Non-candidates may exist in
-- batch_file but must never produce staging rows.
SELECT
    f.batch_id,
    f.source_file_id,
    f.role,
    count(s.batch_id) AS actual_staged_rows,
    count(s.batch_id) = 0 AS non_candidate_has_zero_staged_rows
FROM cg_raw.kpione_raw_ingest_batch_file_v1 AS f
JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
    ON b.batch_id = f.batch_id
   AND b.status = 'STAGED'
LEFT JOIN cg_raw.kpione_raw_event_photo_staging_v1 AS s
    ON s.batch_id = f.batch_id
   AND s.source_file_id = f.source_file_id
WHERE f.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND f.role <> 'include_candidate'
GROUP BY
    f.batch_id,
    f.source_file_id,
    f.role;

-- 9a. PHOTO ANOMALY PROFILE / REVIEW REQUIRED.
-- These preserved raw anomalies may be nonzero; there is no global expected-zero
-- assertion. They do not automatically block raw staging, but they block
-- mart/compliance activation until reviewed in an authorized future phase.
WITH live_rows AS (
    SELECT s.*
    FROM cg_raw.kpione_raw_event_photo_staging_v1 AS s
    JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
        ON b.batch_id = s.batch_id
       AND b.status = 'STAGED'
    WHERE s.batch_id = '<AUTHORIZED_BATCH_ID>'
),
event_total_mismatches AS (
    SELECT
        batch_id,
        event_id
    FROM live_rows
    GROUP BY batch_id, event_id
    HAVING max(photo_total) IS NOT NULL
       AND count(*) <> max(photo_total)
)
SELECT
    'raw_present_with_parsed_null' AS anomaly,
    count(*) AS affected_rows_or_events
FROM live_rows
WHERE n_fotos_raw IS NOT NULL
  AND (photo_sequence IS NULL OR photo_total IS NULL)
UNION ALL
SELECT
    'photo_total_zero',
    count(*)
FROM live_rows
WHERE photo_total = 0
UNION ALL
SELECT
    'event_row_count_differs_from_photo_total',
    count(*)
FROM event_total_mismatches;

-- 9b. PARSED INVARIANT VIOLATION / Expected zero.
-- A nonzero result means the declarative parsed-pair invariant was bypassed or
-- disabled and must block further activation.
WITH live_rows AS (
    SELECT s.*
    FROM cg_raw.kpione_raw_event_photo_staging_v1 AS s
    JOIN cg_raw.kpione_raw_ingest_batch_v1 AS b
        ON b.batch_id = s.batch_id
       AND b.status = 'STAGED'
    WHERE s.batch_id = '<AUTHORIZED_BATCH_ID>'
)
SELECT
    'photo_sequence_greater_than_total' AS invariant_violation,
    count(*) AS affected_rows
FROM live_rows
WHERE photo_sequence > photo_total;

-- 10. FUTURE APPLY BLOCKER CONDITION — NOT EXECUTED:
-- No future apply may proceed without reconciling the immutable candidate
-- manifest projection, its exact SHA, per-file row_count/distinct_event_ids and
-- daily totals. No authorized projection table exists in 014G, so this remains
-- deliberately non-executable; inventing a table or join would be false evidence.
