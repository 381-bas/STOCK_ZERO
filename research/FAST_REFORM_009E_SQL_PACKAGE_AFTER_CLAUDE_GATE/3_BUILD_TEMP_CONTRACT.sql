-- FAST_REFORM_009E / 3_BUILD_TEMP_CONTRACT
-- Issue: #18
-- Mode: SQL_PACKAGE_REVIEW_ONLY
--
-- Purpose:
--   Define the temp-table contract and transformation checks for Route-B apply.
--
-- Important:
--   This file is a contract for the same DB session used by 4_APPLY.
--   It creates TEMP tables only. It creates no persistent staging table.
--   Load data from local artifacts through the client inside the same transaction.
--
-- Input artifacts:
--   data/normalized/fast_reform_009c_route_b_20260620_2345_events.parquet
--   data/exports/fast_reform_009c_route_b_20260620_2345_file_audit.csv
--   data/exports/fast_reform_009c_route_b_20260620_2345_day_coverage.csv
--
-- Route decision:
--   route_decision = A_FREEZE_009E_TO_EXISTING_5_FILE_ARTIFACT
--   artifact_name = fast_reform_009c_route_b_20260620_2345
--   expected_artifact_event_rows = 22195
--   expected_apply_scope_event_rows = 15198
--   expected_deferred_source_event_rows = 6997
--   expected_file_manifest_rows = 5
--   expected_day_coverage_rows = 19
--   Do not regenerate or include newer local files inside 009E.
--
-- Source visibility decision:
--   artifact_source_coverage_max_date = 2026-06-19
--   source_coverage_max_date column/value in 009E means artifact coverage.
--   current_local_folder_source_coverage_max_date: 2026-06-24
--   new_file_deferred_to_next_phase = photo-excel-admin_1782440454408.xlsx
--   new_file_coverage = 2026-06-20..2026-06-24
--   apply_scope_weeks = [2026-06-01, 2026-06-08]
--   deferred_source_weeks = [2026-06-15]
--     2026-06-15 = DEFERRED_SOURCE_WEEK_DENOMINATOR_MISSING
--   pending_source_weeks_009F = [2026-06-22]
--     2026-06-22 has partial source in the deferred new file but is outside 009E.
--   future_not_loaded_weeks = [2026-06-29]
--     2026-06-29 = FUTURE_WEEK_DENOMINATOR_MISSING_OR_NOT_READY
--
-- Client-load note:
--   Postgres cannot read a local parquet file by itself. The manual apply client
--   must stream parquet rows into pg_temp.route_b_events_input, and stream the
--   local CSV audit files into the other pg_temp tables. Do not persist those rows
--   in cg_reform.route_b_*_stage tables.
--
-- manual_execution_order:
--   1. BEGIN is opened by this file before any ON COMMIT DROP temp object.
--   2. Load temp data in the same session/transaction.
--   3. Run 4_APPLY_ONE_TRANSACTION.sql as a transaction body with no BEGIN/COMMIT.
--   4. Review in-transaction validation output/errors.
--   5. COMMIT manually only if all gates pass, otherwise ROLLBACK.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '180s';

create temp table if not exists pg_temp.route_b_events_input (
  event_key text not null,
  run_id text not null,
  sp_item_id text,
  cod_rt text,
  marca text,
  holding text,
  fecha date not null,
  hora text,
  reponedor text,
  tipo_tarea text,
  comentarios text,
  n_fotos_calculado integer not null,
  photo_rows bigint not null,
  source_file_count integer not null,
  source_files text,
  source_file_hashes text,
  first_source_generated_at timestamptz,
  last_source_generated_at timestamptz,
  min_source_row integer,
  max_source_row integer,
  content_hash_event text,
  week_start date not null
) on commit drop;

create temp table if not exists pg_temp.route_b_file_manifest_input (
  source_file text not null,
  file_hash text not null,
  filename_epoch_ms bigint not null,
  source_generated_at timestamptz not null,
  docprops_created_at timestamptz,
  filename_docprops_delta_seconds numeric,
  sheet text not null,
  photo_rows bigint not null,
  distinct_event_keys bigint not null,
  missing_event_key_rows bigint not null,
  min_fecha date,
  max_fecha date,
  covered_dates text,
  candidate_state text not null,
  is_export_cap_split boolean not null,
  is_split_complement boolean not null,
  complements_date date,
  complements_file_hashes text
) on commit drop;

create temp table if not exists pg_temp.route_b_day_coverage_input (
  coverage_date date not null,
  photo_rows bigint not null,
  combined_event_rows bigint not null,
  distinct_event_keys bigint not null,
  source_files text,
  source_file_count integer not null,
  same_day_multi_file boolean not null,
  exact_duplicate_event_instances bigint not null,
  real_content_conflict_event_ids bigint not null
) on commit drop;

-- Optional conflict input is expected to be empty after corrected event hashing.
create temp table if not exists pg_temp.route_b_conflicts_input (
  event_key text,
  content_hash_event_count integer,
  source_files text,
  dates text,
  content_hash_events text,
  per_file_json jsonb
) on commit drop;

-- Normalized build view from temp input.
-- content_hash_identifier intentionally excludes n_fotos_calculado and all per-photo columns.
create temp view pg_temp.route_b_events_built as
select
  event_key,
  run_id,
  sp_item_id,
  upper(nullif(trim(cod_rt), '')) as cod_rt,
  upper(nullif(trim(marca), '')) as cliente_norm,
  upper(nullif(trim(holding), '')) as holding_norm,
  fecha,
  hora,
  upper(nullif(trim(reponedor), '')) as reponedor_norm,
  upper(nullif(trim(tipo_tarea), '')) as tipo_tarea,
  comentarios,
  n_fotos_calculado,
  source_file_count,
  string_to_array(nullif(source_file_hashes, ''), ',') as source_file_hashes,
  first_source_generated_at,
  last_source_generated_at as source_generated_at,
  min_source_row,
  max_source_row,
  week_start,
  md5(concat_ws('|',
    coalesce(nullif(trim(event_key), ''), '<NULL>'),
    coalesce(nullif(trim(sp_item_id), ''), '<NULL>'),
    coalesce(upper(nullif(trim(cod_rt), '')), '<NULL>'),
    coalesce(upper(nullif(trim(marca), '')), '<NULL>'),
    coalesce(upper(nullif(trim(holding), '')), '<NULL>'),
    coalesce(fecha::text, '<NULL>'),
    coalesce(nullif(trim(hora), ''), '<NULL>'),
    coalesce(upper(nullif(trim(reponedor), '')), '<NULL>'),
    coalesce(upper(nullif(trim(tipo_tarea), '')), '<NULL>'),
    coalesce(nullif(trim(comentarios), ''), '<NULL>')
  )) as content_hash_identifier
from pg_temp.route_b_events_input;

-- Local invariants that the apply file must check before target writes:
--   artifact input count(*) = 22195
--   artifact input count(distinct event_key) = 22195
--   apply-scope input count(*) = 15198
--   deferred-source input count(*) = 6997
--   no event_key spans multiple fecha/week_start
--   no unresolved scope mapping when joined to denominator
--   corrected identifier conflicts = 0
