-- 014F NO-APPLY PROPOSAL ONLY
-- DO NOT RUN IN PRODUCTION
-- Supabase apply not authorized
--
-- Patched static design for 014G review. This file has not been executed.
-- Any future apply requires a separate authorized phase, reviewed migration,
-- validation evidence, rollback evidence and explicit approval.

BEGIN;

CREATE TABLE cg_raw.kpione_raw_ingest_batch_v1 (
    batch_id text PRIMARY KEY,
    month date NOT NULL,
    candidate_manifest_sha256 text NOT NULL,
    source_file_ids text[] NOT NULL,
    source_files_count integer NOT NULL,
    source_rows_total bigint NOT NULL,
    exact_duplicates_removed bigint NOT NULL,
    staged_rows bigint NOT NULL,
    coverage_start date NOT NULL,
    coverage_end date NOT NULL,
    verdict text NOT NULL,
    status text NOT NULL DEFAULT 'STAGED',
    rolled_back_at timestamptz,
    rolled_back_by text,
    created_at timestamptz NOT NULL DEFAULT now(),
    applied_by text NOT NULL,
    notes text,

    CONSTRAINT kpione_raw_ingest_batch_month_start_ck
        CHECK (month = date_trunc('month', month)::date),
    CONSTRAINT kpione_raw_ingest_batch_manifest_sha_ck
        CHECK (candidate_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT kpione_raw_ingest_batch_source_files_count_ck
        CHECK (
            source_files_count > 0
            AND cardinality(source_file_ids) = source_files_count
        ),
    CONSTRAINT kpione_raw_ingest_batch_counts_ck
        CHECK (
            source_rows_total >= 0
            AND exact_duplicates_removed >= 0
            AND staged_rows >= 0
            AND staged_rows + exact_duplicates_removed = source_rows_total
        ),
    CONSTRAINT kpione_raw_ingest_batch_coverage_ck
        CHECK (coverage_start <= coverage_end),
    CONSTRAINT kpione_raw_ingest_batch_status_ck
        CHECK (status IN ('STAGED', 'ROLLED_BACK')),
    CONSTRAINT kpione_raw_ingest_batch_rollback_state_ck
        CHECK (
            (
                status = 'STAGED'
                AND rolled_back_at IS NULL
                AND rolled_back_by IS NULL
            )
            OR (
                status = 'ROLLED_BACK'
                AND rolled_back_at IS NOT NULL
                AND nullif(btrim(rolled_back_by), '') IS NOT NULL
            )
        ),
    CONSTRAINT kpione_raw_ingest_batch_applied_by_ck
        CHECK (nullif(btrim(applied_by), '') IS NOT NULL)
);

CREATE UNIQUE INDEX kpione_raw_ingest_batch_staged_manifest_uq
    ON cg_raw.kpione_raw_ingest_batch_v1 (candidate_manifest_sha256)
    WHERE status = 'STAGED';

CREATE UNIQUE INDEX kpione_raw_ingest_batch_staged_month_uq
    ON cg_raw.kpione_raw_ingest_batch_v1 (month)
    WHERE status = 'STAGED';

CREATE TABLE cg_raw.kpione_raw_ingest_batch_file_v1 (
    batch_id text NOT NULL,
    source_file_id text NOT NULL,
    source_file_name text NOT NULL,
    source_file_sha256 text NOT NULL,
    role text NOT NULL,
    row_count bigint NOT NULL,
    distinct_event_ids bigint,
    fecha_min date,
    fecha_max date,

    CONSTRAINT kpione_raw_ingest_batch_file_pk
        PRIMARY KEY (batch_id, source_file_id),
    CONSTRAINT kpione_raw_ingest_batch_file_batch_fk
        FOREIGN KEY (batch_id)
        REFERENCES cg_raw.kpione_raw_ingest_batch_v1 (batch_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,
    CONSTRAINT kpione_raw_ingest_batch_file_sha_ck
        CHECK (source_file_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT kpione_raw_ingest_batch_file_role_ck
        CHECK (
            role IN (
                'include_candidate',
                'quarantine_truncation',
                'compare_only',
                'rejected_schema'
            )
        ),
    CONSTRAINT kpione_raw_ingest_batch_file_row_count_ck
        CHECK (row_count >= 0),
    CONSTRAINT kpione_raw_ingest_batch_file_distinct_events_ck
        CHECK (distinct_event_ids IS NULL OR distinct_event_ids >= 0),
    CONSTRAINT kpione_raw_ingest_batch_file_fecha_range_ck
        CHECK (
            fecha_min IS NULL
            OR fecha_max IS NULL
            OR fecha_min <= fecha_max
        )
);

CREATE TABLE cg_raw.kpione_raw_event_photo_staging_v1 (
    batch_id text NOT NULL,
    event_id text NOT NULL,
    source_file_id text NOT NULL,
    source_row_number bigint NOT NULL,
    fecha date NOT NULL,
    week_start date NOT NULL,
    cod_rt text,
    local_nombre text,
    cliente_norm text NOT NULL,
    reponedor text,
    tipo_tarea text,
    n_fotos_raw text,
    photo_sequence integer,
    photo_total integer,
    link_foto text,
    event_stable_hash text NOT NULL,
    photo_row_hash text NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    loader_version text NOT NULL,

    CONSTRAINT kpione_raw_event_photo_batch_fk
        FOREIGN KEY (batch_id)
        REFERENCES cg_raw.kpione_raw_ingest_batch_v1 (batch_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,
    CONSTRAINT kpione_raw_event_photo_batch_file_fk
        FOREIGN KEY (batch_id, source_file_id)
        REFERENCES cg_raw.kpione_raw_ingest_batch_file_v1 (
            batch_id,
            source_file_id
        )
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,
    CONSTRAINT kpione_raw_event_photo_batch_hash_uq
        UNIQUE (batch_id, photo_row_hash),
    CONSTRAINT kpione_raw_event_photo_source_row_uq
        UNIQUE (batch_id, source_file_id, source_row_number),
    CONSTRAINT kpione_raw_event_photo_source_row_ck
        CHECK (source_row_number >= 2),
    CONSTRAINT kpione_raw_event_photo_event_hash_ck
        CHECK (event_stable_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT kpione_raw_event_photo_photo_hash_ck
        CHECK (photo_row_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT kpione_raw_event_photo_week_start_ck
        CHECK (week_start = date_trunc('week', fecha)::date),
    CONSTRAINT kpione_raw_event_photo_location_ck
        CHECK (
            nullif(btrim(cod_rt), '') IS NOT NULL
            OR nullif(btrim(local_nombre), '') IS NOT NULL
        ),
    CONSTRAINT kpione_raw_event_photo_sequence_ck
        CHECK (
            photo_sequence IS NULL
            OR photo_total IS NULL
            OR photo_sequence <= photo_total
        ),
    CONSTRAINT kpione_raw_event_photo_loader_version_ck
        CHECK (nullif(btrim(loader_version), '') IS NOT NULL)
);

CREATE INDEX kpione_raw_event_photo_batch_fecha_idx
    ON cg_raw.kpione_raw_event_photo_staging_v1 (batch_id, fecha);

CREATE INDEX kpione_raw_event_photo_batch_event_id_idx
    ON cg_raw.kpione_raw_event_photo_staging_v1 (batch_id, event_id);

-- Raw photo numbering is intentionally lossless. n_fotos_raw retains source
-- text; photo_sequence/photo_total are nullable parsed values. Anomalies such as
-- 0/0 are stored and surfaced by validation gates instead of rejected here.
--
-- 014E exact dedupe uses (event_id, photo_row_hash). Operational uniqueness is
-- (batch_id, photo_row_hash) because photo_row_hash includes event_id in the
-- normalized hash payload. The companion validation pack detects any event_id
-- mapped to multiple event_stable_hash values; this is not a declarative CHECK.
--
-- Conceptual rollback is a tombstone transition: update one STAGED registry row
-- to ROLLED_BACK with rolled_back_at/by after downstream isolation. Child rows
-- remain immutable evidence; all FKs use RESTRICT and no destructive SQL is
-- authorized by this proposal.

ROLLBACK;
