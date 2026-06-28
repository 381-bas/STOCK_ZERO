-- FAST_REFORM_009E / 2_ADDITIVE_DDL_CG_REFORM_ONLY
-- Issue: #18
-- Mode: SQL_PACKAGE_REVIEW_ONLY
--
-- Purpose:
--   Add only the approved columns required for Route-B current-surface replace.
--
-- Route decision:
--   route_decision = A_FREEZE_009E_TO_EXISTING_5_FILE_ARTIFACT
--   artifact_name = fast_reform_009c_route_b_20260620_2345
--   artifact_source_coverage_max_date = 2026-06-19
--   current_local_folder_source_coverage_max_date: 2026-06-24
--   new_file_deferred_to_next_phase = photo-excel-admin_1782440454408.xlsx
--   pending_source_weeks_009F = [2026-06-22]
--   future_not_loaded_weeks = [2026-06-29]
--   source_coverage_max_date column represents artifact_source_coverage_max_date
--   for this frozen 009E package.
--
-- Guards:
--   - execute only after explicit authorization
--   - cg_reform only
--   - no persistent stage tables
--   - no public.* / cg_mart.* / cg_raw.* changes
--   - no grants / app cutover / reclaim

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

-- Ledger columns. Store pointers/hashes, not the full manifest JSON blob.
alter table cg_reform.report_registry
  add column if not exists source_run_id text,
  add column if not exists source_month date,
  add column if not exists source_artifact text,
  add column if not exists manifest_uri text,
  add column if not exists manifest_sha256 text,
  add column if not exists source_generated_at_min timestamptz,
  add column if not exists source_generated_at_max timestamptz,
  -- 009E artifact coverage; not the mutable local folder coverage.
  add column if not exists source_coverage_max_date date,
  add column if not exists route_decision text,
  add column if not exists rebuild_strategy text,
  add column if not exists replace_scope_weeks date[];

-- File/chunk evidence. No Supabase staging table is created.
alter table cg_reform.chunk_registry
  add column if not exists source_generated_at timestamptz,
  add column if not exists docprops_created_at timestamptz,
  add column if not exists candidate_state text,
  add column if not exists is_export_cap_split boolean not null default false,
  add column if not exists is_split_complement boolean not null default false,
  add column if not exists complements_date date,
  add column if not exists complements_file_hashes text[];

-- Current canonical surface lineage. Keep hashes, not source filenames.
alter table cg_reform.canonical_hot_keys
  add column if not exists source_run_id text,
  add column if not exists source_month date,
  add column if not exists source_generated_at timestamptz,
  add column if not exists source_file_hashes text[],
  add column if not exists source_file_count integer,
  add column if not exists n_fotos_calculado integer;

-- Quarantine lineage.
alter table cg_reform.quarantine_min
  add column if not exists source_run_id text,
  add column if not exists source_month date,
  add column if not exists source_generated_at timestamptz,
  add column if not exists source_file_hashes text[];

-- Weekly representation of day-level load coverage. Day-level detail remains local-only.
alter table cg_reform.report_existence_week
  add column if not exists loaded_days integer,
  add column if not exists expected_days integer,
  add column if not exists not_loaded_days integer,
  add column if not exists week_load_status text;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'report_registry_source_month_first_day_chk'
      and conrelid = 'cg_reform.report_registry'::regclass
  ) then
    alter table cg_reform.report_registry
      add constraint report_registry_source_month_first_day_chk
      check (source_month is null or source_month = date_trunc('month', source_month)::date);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'report_registry_rebuild_strategy_chk'
      and conrelid = 'cg_reform.report_registry'::regclass
  ) then
    alter table cg_reform.report_registry
      add constraint report_registry_rebuild_strategy_chk
      check (
        rebuild_strategy is null
        or rebuild_strategy in ('MONTH_TO_DATE_REBUILD', 'HOT_3W_REBUILD', 'ROUTE_B_PARTIAL_JUNE_REBUILD')
      );
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'canonical_hot_keys_source_month_first_day_chk'
      and conrelid = 'cg_reform.canonical_hot_keys'::regclass
  ) then
    alter table cg_reform.canonical_hot_keys
      add constraint canonical_hot_keys_source_month_first_day_chk
      check (source_month is null or source_month = date_trunc('month', source_month)::date);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'report_existence_week_load_status_chk'
      and conrelid = 'cg_reform.report_existence_week'::regclass
  ) then
    alter table cg_reform.report_existence_week
      add constraint report_existence_week_load_status_chk
      check (week_load_status is null or week_load_status in ('FULL', 'PARTIAL', 'NONE'));
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'report_existence_week_loaded_days_chk'
      and conrelid = 'cg_reform.report_existence_week'::regclass
  ) then
    alter table cg_reform.report_existence_week
      add constraint report_existence_week_loaded_days_chk
      check (
        loaded_days is null
        or expected_days is null
        or not_loaded_days is null
        or (
          loaded_days >= 0
          and expected_days >= 0
          and not_loaded_days >= 0
          and loaded_days + not_loaded_days = expected_days
        )
      );
  end if;
end $$;

create index if not exists ix_report_registry_source_run
  on cg_reform.report_registry (source_run_id);

create index if not exists ix_report_registry_source_month
  on cg_reform.report_registry (source_month, built_at);

create index if not exists ix_chunk_registry_source_generated_at
  on cg_reform.chunk_registry (report_id, source_generated_at);

create index if not exists ix_canonical_hot_keys_source_month
  on cg_reform.canonical_hot_keys (source_month, week_start, cod_rt, cliente_norm);

create index if not exists ix_canonical_hot_keys_source_run
  on cg_reform.canonical_hot_keys (source_run_id);

create index if not exists ix_quarantine_min_source_run
  on cg_reform.quarantine_min (source_run_id, reason);

create index if not exists ix_report_existence_week_load_status
  on cg_reform.report_existence_week (week_load_status, week_start);

commit;
