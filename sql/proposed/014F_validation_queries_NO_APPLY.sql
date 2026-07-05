-- 014F VALIDATION QUERIES NO-APPLY ONLY
-- DO NOT RUN WITHOUT AUTHORIZED FUTURE PHASE
-- Supabase apply not authorized
--
-- Read-only proposal. Every query must be scoped to an explicitly authorized
-- batch_id in a future phase. No query in this file was executed in 014F.

-- Replace the sentinel only in an authorized future review.
-- Expected result for checks 1, 2, 4, 5 and 6a/6c: zero rows.

-- 1. One event_id must not map to multiple event_stable_hash values in a batch.
SELECT
    batch_id,
    event_id,
    count(DISTINCT event_stable_hash) AS stable_hash_count
FROM cg_raw.kpione_raw_event_photo_staging_v1
WHERE batch_id = '<AUTHORIZED_BATCH_ID>'
GROUP BY batch_id, event_id
HAVING count(DISTINCT event_stable_hash) > 1;

-- 2. Every staging fecha must remain inside the registry coverage.
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
WHERE s.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND s.fecha NOT BETWEEN b.coverage_start AND b.coverage_end;

-- 3. Expected: one row with count_matches_registry = true.
SELECT
    b.batch_id,
    b.staged_rows AS expected_staged_rows,
    count(s.batch_id) AS actual_staged_rows,
    count(s.batch_id) = b.staged_rows AS count_matches_registry
FROM cg_raw.kpione_raw_ingest_batch_v1 AS b
LEFT JOIN cg_raw.kpione_raw_event_photo_staging_v1 AS s
    ON s.batch_id = b.batch_id
WHERE b.batch_id = '<AUTHORIZED_BATCH_ID>'
GROUP BY b.batch_id, b.staged_rows;

-- 4. Every staging source_file_id must exist in the batch-file registry.
SELECT
    s.batch_id,
    s.source_file_id,
    count(*) AS orphan_rows
FROM cg_raw.kpione_raw_event_photo_staging_v1 AS s
LEFT JOIN cg_raw.kpione_raw_ingest_batch_file_v1 AS f
    ON f.batch_id = s.batch_id
   AND f.source_file_id = s.source_file_id
WHERE s.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND f.source_file_id IS NULL
GROUP BY s.batch_id, s.source_file_id;

-- 5. week_start must be the Monday associated with fecha.
SELECT
    batch_id,
    source_file_id,
    source_row_number,
    fecha,
    week_start
FROM cg_raw.kpione_raw_event_photo_staging_v1
WHERE batch_id = '<AUTHORIZED_BATCH_ID>'
  AND week_start <> date_trunc('week', fecha)::date;

-- 6a. Defensive role-domain check.
SELECT
    batch_id,
    source_file_id,
    role
FROM cg_raw.kpione_raw_ingest_batch_file_v1
WHERE batch_id = '<AUTHORIZED_BATCH_ID>'
  AND role NOT IN (
      'include_candidate',
      'quarantine_truncation',
      'compare_only',
      'rejected_schema'
  );

-- 6b. Expected: one row with candidate_file_count_matches = true.
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
GROUP BY b.batch_id, b.source_files_count;

-- 6c. Registry candidate IDs and include_candidate child IDs must match.
WITH expected AS (
    SELECT
        b.batch_id,
        unnest(b.source_file_ids) AS source_file_id
    FROM cg_raw.kpione_raw_ingest_batch_v1 AS b
    WHERE b.batch_id = '<AUTHORIZED_BATCH_ID>'
),
actual AS (
    SELECT
        f.batch_id,
        f.source_file_id
    FROM cg_raw.kpione_raw_ingest_batch_file_v1 AS f
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

-- 6d. Expected: zero rows. Staging may use include_candidate files only.
SELECT
    s.batch_id,
    s.source_file_id,
    f.role,
    count(*) AS staged_rows
FROM cg_raw.kpione_raw_event_photo_staging_v1 AS s
JOIN cg_raw.kpione_raw_ingest_batch_file_v1 AS f
    ON f.batch_id = s.batch_id
   AND f.source_file_id = s.source_file_id
WHERE s.batch_id = '<AUTHORIZED_BATCH_ID>'
  AND f.role <> 'include_candidate'
GROUP BY s.batch_id, s.source_file_id, f.role;

-- 7. FUTURE PLACEHOLDER — NOT EXECUTED:
-- Reconcile per-file manifest row_count/distinct_event_ids and daily totals
-- against an authorized immutable manifest projection. The exact manifest
-- projection/table contract does not exist in 014F, so inventing an executable
-- join here would create false assurance. 014G must review this gate, and a
-- later authorized phase must bind it to exact manifest SHA evidence.
