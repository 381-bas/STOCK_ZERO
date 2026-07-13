BEGIN;

CREATE SCHEMA IF NOT EXISTS cg_raw;
CREATE SCHEMA IF NOT EXISTS cg_core;

CREATE TABLE IF NOT EXISTS cg_raw.kpione_raw_ingest_batch_v1 (
    batch_id uuid PRIMARY KEY,
    runner_execution_id uuid NOT NULL,
    semantic_plan_hash text NOT NULL CHECK (semantic_plan_hash ~ '^[0-9a-f]{64}$'),
    status text NOT NULL CHECK (status IN ('DISCOVERED','VALIDATING','VALIDATED','STAGING','STAGED','ACTIVE','QUARANTINED','SUPERSEDED','ROLLED_BACK','FAILED')),
    coverage_start date NOT NULL,
    coverage_end date NOT NULL CHECK (coverage_end >= coverage_start),
    file_count integer NOT NULL CHECK (file_count > 0),
    row_count integer NOT NULL CHECK (row_count > 0),
    event_count integer NOT NULL CHECK (event_count > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    validated_at timestamptz,
    activated_at timestamptz,
    rolled_back_at timestamptz,
    supersedes_batch_id uuid REFERENCES cg_raw.kpione_raw_ingest_batch_v1(batch_id),
    error_summary text
);

CREATE TABLE IF NOT EXISTS cg_raw.kpione_raw_ingest_batch_file_v1 (
    batch_id uuid NOT NULL REFERENCES cg_raw.kpione_raw_ingest_batch_v1(batch_id),
    source_file_sha256 text NOT NULL CHECK (source_file_sha256 ~ '^[0-9a-f]{64}$'),
    source_file_name text NOT NULL,
    source_sheet text NOT NULL,
    file_size bigint NOT NULL CHECK (file_size > 0),
    coverage_start date NOT NULL,
    coverage_end date NOT NULL,
    row_count integer NOT NULL,
    event_count integer NOT NULL,
    validation_status text NOT NULL,
    quarantine_reason text,
    PRIMARY KEY (batch_id, source_file_sha256, source_sheet)
);

CREATE INDEX IF NOT EXISTS kpione_batch_file_source_version_idx
    ON cg_raw.kpione_raw_ingest_batch_file_v1(source_file_sha256, source_sheet);

CREATE TABLE IF NOT EXISTS cg_raw.kpione_raw_event_photo_staging_v1 (
    staging_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id uuid NOT NULL REFERENCES cg_raw.kpione_raw_ingest_batch_v1(batch_id),
    source_file_sha256 text NOT NULL,
    source_sheet text NOT NULL,
    source_row_number integer NOT NULL CHECK (source_row_number >= 2),
    source_row_identity text NOT NULL CHECK (source_row_identity ~ '^[0-9a-f]{64}$'),
    event_id text NOT NULL,
    sp_item_id text NOT NULL,
    source_payload jsonb NOT NULL,
    photo_row_hash text NOT NULL CHECK (photo_row_hash ~ '^[0-9a-f]{64}$'),
    event_stable_hash text NOT NULL CHECK (event_stable_hash ~ '^[0-9a-f]{64}$'),
    event_date date NOT NULL,
    location_key text NOT NULL,
    cliente_norm text NOT NULL,
    duplicate_classification text NOT NULL,
    conflict_classification text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (batch_id, source_row_identity)
);

CREATE INDEX IF NOT EXISTS kpione_staging_event_idx
    ON cg_raw.kpione_raw_event_photo_staging_v1(event_date, event_id);
CREATE INDEX IF NOT EXISTS kpione_staging_batch_idx
    ON cg_raw.kpione_raw_event_photo_staging_v1(batch_id);
CREATE INDEX IF NOT EXISTS kpione_staging_photo_identity_idx
    ON cg_raw.kpione_raw_event_photo_staging_v1(event_id, photo_row_hash);

CREATE OR REPLACE VIEW cg_core.kpione_event_normalized_v1 AS
SELECT s.event_id,
       min(s.sp_item_id) AS sp_item_id,
       min(s.event_date) AS event_date,
       min(s.location_key) AS location_key,
       min(s.cliente_norm) AS cliente_norm,
       min(s.event_stable_hash) AS event_stable_hash,
       count(DISTINCT s.photo_row_hash)::integer AS photo_count,
       array_agg(DISTINCT s.batch_id ORDER BY s.batch_id) AS source_batch_ids
FROM cg_raw.kpione_raw_event_photo_staging_v1 s
JOIN cg_raw.kpione_raw_ingest_batch_v1 b USING (batch_id)
WHERE b.status = 'ACTIVE' AND s.duplicate_classification = 'UNIQUE'
GROUP BY s.event_id;

CREATE OR REPLACE VIEW cg_core.kpione_day_presence_v1 AS
SELECT event_date AS fecha, location_key, cliente_norm, 1::integer AS presence,
       count(*)::integer AS event_count
FROM cg_core.kpione_event_normalized_v1
GROUP BY event_date, location_key, cliente_norm;

COMMIT;
