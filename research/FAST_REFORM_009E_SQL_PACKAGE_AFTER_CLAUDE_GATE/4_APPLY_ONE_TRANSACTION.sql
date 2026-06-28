-- FAST_REFORM_009E / 4_APPLY_ONE_TRANSACTION
-- Issue: #18
-- Mode: SQL_PACKAGE_REVIEW_ONLY
--
-- Purpose:
--   One transaction apply plan for Route-B current-surface replace.
--
-- Route decision:
--   route_decision = A_FREEZE_009E_TO_EXISTING_5_FILE_ARTIFACT
--   artifact_name = fast_reform_009c_route_b_20260620_2345
--   artifact_source_coverage_max_date = 2026-06-19
--   source_coverage_max_date column/value in 009E means artifact coverage.
--   current_local_folder_source_coverage_max_date: 2026-06-24
--   new_file_deferred_to_next_phase = photo-excel-admin_1782440454408.xlsx
--   new_file_coverage = 2026-06-20..2026-06-24
--   pending_source_weeks_009F = [2026-06-22]
--   future_not_loaded_weeks = [2026-06-29]
--   expected_artifact_event_rows = 22195
--   expected_apply_scope_event_rows = 15198
--   expected_unresolved_scope_rows = 53
--   expected_mapped_canonical_rows = 15145
--   expected_deferred_source_event_rows = 6997
--   expected_file_manifest_rows = 5
--   expected_day_coverage_rows = 19
--   cg_reform.report_registry empty = FIRST_ROUTE_B_PARTIAL_BUILD.
--
-- Guards:
--   - execute only after explicit Bastian authorization
--   - writes only cg_reform.*
--   - TEMP tables only for staging, on commit drop
--   - no persistent stage tables
--   - no cg_raw.kpione2_raw mutation
--   - no public.* / cg_mart.* changes
--   - no grants / app cutover / reclaim
--
-- manual_execution_order:
--   1. run 3_BUILD_TEMP_CONTRACT.sql first; it opens BEGIN in this same session.
--   2. confirm the transaction remains open.
--   3. client-load normalized_events.parquet into pg_temp.route_b_events_input.
--   4. client-load file_audit.csv into pg_temp.route_b_file_manifest_input.
--   5. client-load day_coverage.csv into pg_temp.route_b_day_coverage_input.
--   6. leave pg_temp.route_b_conflicts_input empty unless a regenerated corrected
--      conflict file contains true identifier conflicts.
--   7. run the SQL below.
--   8. COMMIT manually only after the validation block passes; otherwise ROLLBACK.
--
-- This file is an internal transaction body. It intentionally has no BEGIN/COMMIT.

set local lock_timeout = '5s';
set local statement_timeout = '180s';

-- Safety constants.
create temp table pg_temp.route_b_apply_params (
  target_run_id text primary key,
  target_report_id text not null,
  source_month date not null,
  manifest_uri text not null,
  manifest_sha256 text,
  source_artifact text not null,
  replace_scope_weeks date[] not null,
  loaded_min_date date not null,
  loaded_max_date date not null,
  source_coverage_max_date date not null,
  deferred_source_weeks date[] not null,
  future_not_loaded_weeks date[] not null,
  expected_artifact_event_rows bigint not null,
  expected_apply_scope_event_rows bigint not null,
  expected_unresolved_scope_rows bigint not null,
  expected_mapped_canonical_rows bigint not null,
  expected_deferred_source_event_rows bigint not null,
  route_decision text not null
) on commit drop;

insert into pg_temp.route_b_apply_params values (
  'fast_reform_009c_route_b_20260620_2345',
  'route_b_report_existence_first_202606_20260620_2345',
  date '2026-06-01',
  'data/manifests/fast_reform_009c_route_b_20260620_2345.json',
  null,
  'data/normalized/fast_reform_009c_route_b_20260620_2345_events.parquet',
  array[
    date '2026-06-01',
    date '2026-06-08'
  ],
  date '2026-06-01',
  date '2026-06-14',
  date '2026-06-19',
  array[date '2026-06-15'],
  array[date '2026-06-29'],
  22195,
  15198,
  53,
  15145,
  6997,
  'ROUTE_B_PARTIAL_JUNE_REBUILD_SOURCE_VISIBLE_DENOMINATOR_GATED'
);

-- Partial June policy:
--   artifact_source_coverage_max_date = 2026-06-19.
--   source_coverage_max_date column/value in 009E represents artifact coverage.
--   Only weeks with confirmed Supabase denominator are in replace_scope_weeks.
--   2026-06-15 is deferred because local source exists but denominator is missing.
--   2026-06-22 is deferred to 009F with partial source from photo-excel-admin_1782440454408.xlsx.
--   2026-06-29 is future/not-ready and excluded from destructive scope.
do $$
declare
  v_bad_excluded_scope_weeks bigint;
  v_missing_apply_denominator_weeks bigint;
begin
  select count(*) into v_bad_excluded_scope_weeks
  from pg_temp.route_b_apply_params p
  cross join unnest(p.deferred_source_weeks || p.future_not_loaded_weeks) as excluded_week(week_start)
  where excluded_week.week_start = any(p.replace_scope_weeks);

  if v_bad_excluded_scope_weeks <> 0 then
    raise exception
      'Route-B partial June scope includes deferred/future excluded weeks: %',
      v_bad_excluded_scope_weeks;
  end if;

  select count(*) into v_missing_apply_denominator_weeks
  from (
    select w.week_start
    from pg_temp.route_b_apply_params p
    cross join unnest(p.replace_scope_weeks) as w(week_start)
    left join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
      on rr.effective_week_start::date = w.week_start
    group by w.week_start
    having count(rr.effective_week_start) = 0
  ) missing;

  if v_missing_apply_denominator_weeks <> 0 then
    raise exception
      'Route-B partial June rebuild cannot apply weeks with missing denominator; missing apply weeks=%',
      v_missing_apply_denominator_weeks;
  end if;
end $$;

-- In-transaction snapshots for rollback evidence. TEMP only.
create temp table pg_temp.route_b_pre_replace_canonical_snapshot
on commit drop as
select c.*
from cg_reform.canonical_hot_keys c
cross join pg_temp.route_b_apply_params p
where c.week_start = any(p.replace_scope_weeks);

create temp table pg_temp.route_b_pre_replace_existence_snapshot
on commit drop as
select e.*
from cg_reform.report_existence_week e
cross join pg_temp.route_b_apply_params p
where e.week_start = any(p.replace_scope_weeks);

create temp table pg_temp.route_b_pre_replace_quarantine_snapshot
on commit drop as
select q.*
from cg_reform.quarantine_min q
cross join pg_temp.route_b_apply_params p
where q.report_id = p.target_report_id
   or q.source_run_id = p.target_run_id
   or q.week_start = any(p.replace_scope_weeks);

-- Blocking validations on temp input.
do $$
declare
  v_rows bigint;
  v_distinct_keys bigint;
  v_multi_date_keys bigint;
  v_multi_week_keys bigint;
  v_identifier_conflicts bigint;
  v_file_rows bigint;
  v_day_rows bigint;
  v_expected_artifact_event_rows bigint;
  v_expected_apply_scope_event_rows bigint;
  v_expected_unresolved_scope_rows bigint;
  v_expected_mapped_canonical_rows bigint;
  v_expected_deferred_source_event_rows bigint;
  v_apply_scope_rows bigint;
  v_deferred_source_rows bigint;
  v_unresolved_scope bigint;
  v_stale_conflict_rows bigint;
begin
  select
    expected_artifact_event_rows,
    expected_apply_scope_event_rows,
    expected_unresolved_scope_rows,
    expected_mapped_canonical_rows,
    expected_deferred_source_event_rows
  into
    v_expected_artifact_event_rows,
    v_expected_apply_scope_event_rows,
    v_expected_unresolved_scope_rows,
    v_expected_mapped_canonical_rows,
    v_expected_deferred_source_event_rows
  from pg_temp.route_b_apply_params;

  select count(*), count(distinct event_key)
  into v_rows, v_distinct_keys
  from pg_temp.route_b_events_built;

  if v_rows <> v_expected_artifact_event_rows or v_distinct_keys <> v_expected_artifact_event_rows then
    raise exception 'Route-B temp input row guard failed: rows=%, distinct_keys=%', v_rows, v_distinct_keys;
  end if;

  select count(*) into v_apply_scope_rows
  from pg_temp.route_b_events_built e
  cross join pg_temp.route_b_apply_params p
  where e.week_start = any(p.replace_scope_weeks);

  if v_apply_scope_rows <> v_expected_apply_scope_event_rows then
    raise exception 'Route-B apply-scope input guard failed: rows=% expected=%', v_apply_scope_rows, v_expected_apply_scope_event_rows;
  end if;

  if v_expected_mapped_canonical_rows + v_expected_unresolved_scope_rows <> v_expected_apply_scope_event_rows then
    raise exception
      'Route-B mapped plus unresolved scope constants do not equal apply scope: mapped=% unresolved=% apply=%',
      v_expected_mapped_canonical_rows,
      v_expected_unresolved_scope_rows,
      v_expected_apply_scope_event_rows;
  end if;

  select count(*) into v_deferred_source_rows
  from pg_temp.route_b_events_built e
  cross join pg_temp.route_b_apply_params p
  where e.week_start = any(p.deferred_source_weeks);

  if v_deferred_source_rows <> v_expected_deferred_source_event_rows then
    raise exception 'Route-B deferred-source input guard failed: rows=% expected=%', v_deferred_source_rows, v_expected_deferred_source_event_rows;
  end if;

  select count(*) into v_multi_date_keys
  from (
    select event_key
    from pg_temp.route_b_events_built
    group by event_key
    having count(distinct fecha) > 1
  ) s;

  select count(*) into v_multi_week_keys
  from (
    select event_key
    from pg_temp.route_b_events_built
    group by event_key
    having count(distinct week_start) > 1
  ) s;

  if v_multi_date_keys <> 0 or v_multi_week_keys <> 0 then
    raise exception 'Route-B event_key scope guard failed: multi_date=%, multi_week=%', v_multi_date_keys, v_multi_week_keys;
  end if;

  select count(*) into v_identifier_conflicts
  from (
    select event_key
    from pg_temp.route_b_events_built
    group by event_key
    having count(distinct content_hash_identifier) > 1
  ) s;

  if v_identifier_conflicts <> 0 then
    raise exception 'Route-B identifier content conflicts remain after corrected collapse: %', v_identifier_conflicts;
  end if;

  select count(*) into v_file_rows from pg_temp.route_b_file_manifest_input;
  if v_file_rows <> 5 then
    raise exception 'Route-B file manifest guard failed: % file rows', v_file_rows;
  end if;

  select count(*) into v_day_rows from pg_temp.route_b_day_coverage_input;
  if v_day_rows <> 19 then
    raise exception 'Route-B day coverage guard failed: % loaded day rows', v_day_rows;
  end if;

  select count(*) into v_stale_conflict_rows
  from pg_temp.route_b_conflicts_input;

  if v_stale_conflict_rows <> 0 then
    raise exception
      'Route-B conflicts input must be empty after corrected hash/collapse; stale conflict rows=%',
      v_stale_conflict_rows;
  end if;

  select count(*) into v_unresolved_scope
  from pg_temp.route_b_events_built e
  cross join pg_temp.route_b_apply_params p
  left join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
    on rr.effective_week_start::date = e.week_start
   and upper(trim(rr.cod_rt)) = e.cod_rt
   and upper(trim(rr.cliente_norm)) = e.cliente_norm
  where e.week_start = any(p.replace_scope_weeks)
    and rr.cod_rt is null;

  if v_unresolved_scope <> v_expected_unresolved_scope_rows then
    raise exception
      'Route-B unresolved scope row guard changed: % expected=%',
      v_unresolved_scope,
      v_expected_unresolved_scope_rows;
  end if;
end $$;

-- Insert report ledger.
insert into cg_reform.report_registry (
  report_id,
  model_name,
  conflict_policy,
  source_system,
  source_relation,
  event_key_expression,
  event_key_scope,
  scope_denominator,
  hot_weeks,
  freshness_threshold_days,
  loaded_min_date,
  loaded_max_date,
  max_loaded_week,
  hot_start_week,
  freshness_lag_days,
  hot_rows,
  unique_event_keys,
  missing_event_key,
  duplicate_key_count,
  same_key_different_content_hash,
  report_status,
  source_run_id,
  source_month,
  source_artifact,
  manifest_uri,
  manifest_sha256,
  source_generated_at_min,
  source_generated_at_max,
  source_coverage_max_date,
  route_decision,
  rebuild_strategy,
  replace_scope_weeks
)
select
  p.target_report_id,
  'REPORT_EXISTENCE_FIRST',
  'DETERMINISTIC_WINNER',
  'KPIONE2_ROUTE_B_LOCAL',
  'local:normalized_events.parquet',
  'ID',
  'GLOBAL',
  'cg_core.v_rr_frecuencia_base_resuelta_v2',
  2,
  7,
  p.loaded_min_date,
  p.loaded_max_date,
  date '2026-06-08',
  date '2026-06-01',
  (now()::date - p.loaded_max_date)::int,
  p.expected_mapped_canonical_rows,
  p.expected_mapped_canonical_rows,
  0,
  1043,
  0,
  'BUILT',
  p.target_run_id,
  p.source_month,
  p.source_artifact,
  p.manifest_uri,
  p.manifest_sha256,
  (select min(source_generated_at) from pg_temp.route_b_file_manifest_input),
  (select max(source_generated_at) from pg_temp.route_b_file_manifest_input),
  p.source_coverage_max_date,
  p.route_decision,
  'ROUTE_B_PARTIAL_JUNE_REBUILD',
  p.replace_scope_weeks
from pg_temp.route_b_apply_params p
on conflict (report_id) do update set
  hot_weeks = excluded.hot_weeks,
  loaded_min_date = excluded.loaded_min_date,
  loaded_max_date = excluded.loaded_max_date,
  max_loaded_week = excluded.max_loaded_week,
  hot_start_week = excluded.hot_start_week,
  freshness_lag_days = excluded.freshness_lag_days,
  hot_rows = excluded.hot_rows,
  unique_event_keys = excluded.unique_event_keys,
  missing_event_key = excluded.missing_event_key,
  duplicate_key_count = excluded.duplicate_key_count,
  same_key_different_content_hash = excluded.same_key_different_content_hash,
  source_run_id = excluded.source_run_id,
  source_month = excluded.source_month,
  source_artifact = excluded.source_artifact,
  manifest_uri = excluded.manifest_uri,
  manifest_sha256 = excluded.manifest_sha256,
  source_generated_at_min = excluded.source_generated_at_min,
  source_generated_at_max = excluded.source_generated_at_max,
  source_coverage_max_date = excluded.source_coverage_max_date,
  route_decision = excluded.route_decision,
  rebuild_strategy = excluded.rebuild_strategy,
  replace_scope_weeks = excluded.replace_scope_weeks,
  built_at = now();

-- Insert file/chunk ledger. filename_epoch_ms is used as deterministic batch_id.
insert into cg_reform.chunk_registry (
  chunk_id,
  report_id,
  batch_id,
  source_file,
  source_sheet,
  file_hash,
  storage_uri,
  row_count,
  hot_row_count,
  unique_event_keys_hot,
  min_fecha_visita,
  max_fecha_visita,
  source_generated_at,
  docprops_created_at,
  candidate_state,
  is_export_cap_split,
  is_split_complement,
  complements_date,
  complements_file_hashes
)
select
  'route-b-file-' || f.filename_epoch_ms::text,
  p.target_report_id,
  f.filename_epoch_ms,
  f.source_file,
  f.sheet,
  f.file_hash,
  null::text,
  f.photo_rows,
  f.photo_rows,
  f.distinct_event_keys,
  f.min_fecha,
  f.max_fecha,
  f.source_generated_at,
  f.docprops_created_at,
  f.candidate_state,
  f.is_export_cap_split,
  f.is_split_complement,
  f.complements_date,
  case
    when nullif(trim(f.complements_file_hashes), '') is null then null
    else string_to_array(f.complements_file_hashes, ',')
  end
from pg_temp.route_b_file_manifest_input f
cross join pg_temp.route_b_apply_params p
on conflict (chunk_id) do update set
  report_id = excluded.report_id,
  batch_id = excluded.batch_id,
  source_file = excluded.source_file,
  source_sheet = excluded.source_sheet,
  file_hash = excluded.file_hash,
  row_count = excluded.row_count,
  hot_row_count = excluded.hot_row_count,
  unique_event_keys_hot = excluded.unique_event_keys_hot,
  min_fecha_visita = excluded.min_fecha_visita,
  max_fecha_visita = excluded.max_fecha_visita,
  source_generated_at = excluded.source_generated_at,
  docprops_created_at = excluded.docprops_created_at,
  candidate_state = excluded.candidate_state,
  is_export_cap_split = excluded.is_export_cap_split,
  is_split_complement = excluded.is_split_complement,
  complements_date = excluded.complements_date,
  complements_file_hashes = excluded.complements_file_hashes,
  created_at = now();

-- Current-surface replace: June weeks only.
delete from cg_reform.report_existence_week e
using pg_temp.route_b_apply_params p
where e.week_start = any(p.replace_scope_weeks);

delete from cg_reform.quarantine_min q
using pg_temp.route_b_apply_params p
where q.report_id = p.target_report_id
   or q.source_run_id = p.target_run_id
   or q.week_start = any(p.replace_scope_weeks);

delete from cg_reform.canonical_hot_keys c
using pg_temp.route_b_apply_params p
where c.week_start = any(p.replace_scope_weeks);

-- Insert canonical event current surface.
insert into cg_reform.canonical_hot_keys (
  event_key,
  report_id,
  sp_item_id,
  content_hash,
  week_start,
  fecha_visita_min,
  fecha_visita_max,
  cod_rt,
  cliente_norm,
  holding_norm,
  marca_value,
  reponedor_norm,
  raw_row_count,
  conflict_flag,
  conflict_version_count,
  batch_id_min,
  batch_id_max,
  source_row_min,
  source_row_max,
  source_run_id,
  source_month,
  source_generated_at,
  source_file_hashes,
  source_file_count,
  n_fotos_calculado
)
select distinct
  e.event_key,
  p.target_report_id,
  e.sp_item_id,
  e.content_hash_identifier,
  e.week_start,
  e.fecha,
  e.fecha,
  e.cod_rt,
  e.cliente_norm,
  e.holding_norm,
  e.cliente_norm,
  e.reponedor_norm,
  e.n_fotos_calculado::bigint,
  false,
  1,
  null::bigint,
  null::bigint,
  e.min_source_row,
  e.max_source_row,
  p.target_run_id,
  p.source_month,
  e.source_generated_at,
  e.source_file_hashes,
  e.source_file_count,
  e.n_fotos_calculado
from pg_temp.route_b_events_built e
cross join pg_temp.route_b_apply_params p
join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
  on rr.effective_week_start::date = e.week_start
 and upper(trim(rr.cod_rt)) = e.cod_rt
 and upper(trim(rr.cliente_norm)) = e.cliente_norm
where e.week_start = any(p.replace_scope_weeks);

-- Quarantine true identifier conflicts. Expected: none for 862144.
insert into cg_reform.quarantine_min (
  quarantine_key,
  report_id,
  reason,
  severity,
  event_key,
  week_start,
  cod_rt,
  cliente_norm,
  raw_row_count,
  sample_batch_id,
  sample_source_row,
  conflicting_content_hashes,
  conflicting_chunk_ids,
  details,
  source_run_id,
  source_month,
  source_generated_at,
  source_file_hashes
)
select
  md5(concat_ws('|', p.target_report_id, 'ID_CONTENT_CONFLICT', c.event_key)),
  p.target_report_id,
  'ID_CONTENT_CONFLICT',
  'HIGH',
  c.event_key,
  null::date,
  null::text,
  null::text,
  1::bigint,
  null::bigint,
  null::integer,
  string_to_array(c.content_hash_events, ','),
  null,
  jsonb_build_object(
    'rule', 'identifier_content_conflict_after_corrected_collapse',
    'per_file_json', c.per_file_json,
    'note', '862144 cap-split photo-count artifact should not appear here after corrected hash'
  ),
  p.target_run_id,
  p.source_month,
  null::timestamptz,
  null
from pg_temp.route_b_conflicts_input c
cross join pg_temp.route_b_apply_params p
where c.content_hash_event_count > 1;

-- Quarantine apply-scope events without an exact denominator match.
insert into cg_reform.quarantine_min (
  quarantine_key,
  report_id,
  reason,
  severity,
  event_key,
  week_start,
  cod_rt,
  cliente_norm,
  raw_row_count,
  sample_batch_id,
  sample_source_row,
  conflicting_content_hashes,
  conflicting_chunk_ids,
  details,
  source_run_id,
  source_month,
  source_generated_at,
  source_file_hashes
)
select
  md5(concat_ws('|', p.target_report_id, 'UNRESOLVED_SCOPE', e.event_key)),
  p.target_report_id,
  'UNRESOLVED_SCOPE',
  'MEDIUM',
  e.event_key,
  e.week_start,
  e.cod_rt,
  e.cliente_norm,
  1::bigint,
  null::bigint,
  e.min_source_row,
  null,
  null,
  jsonb_build_object(
    'rule', 'source_event_without_exact_denominator_scope',
    'week_start', e.week_start,
    'fecha', e.fecha,
    'holding_norm', e.holding_norm,
    'reponedor_norm', e.reponedor_norm,
    'n_fotos_calculado', e.n_fotos_calculado,
    'source_file_count', e.source_file_count,
    'note', 'KPIONE2 evidence exists but denominator pair is not present in cg_core.v_rr_frecuencia_base_resuelta_v2'
  ),
  p.target_run_id,
  p.source_month,
  e.source_generated_at,
  e.source_file_hashes
from pg_temp.route_b_events_built e
cross join pg_temp.route_b_apply_params p
left join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
  on rr.effective_week_start::date = e.week_start
 and upper(trim(rr.cod_rt)) = e.cod_rt
 and upper(trim(rr.cliente_norm)) = e.cliente_norm
where e.week_start = any(p.replace_scope_weeks)
  and rr.cod_rt is null;

-- Recompute weekly existence with day-aware load status.
with params as (
  select * from pg_temp.route_b_apply_params
),
week_days as (
  select
    p.target_report_id,
    p.source_month,
    w.week_start,
    d::date as day
  from params p
  cross join unnest(p.replace_scope_weeks) as w(week_start)
  cross join lateral generate_series(w.week_start, w.week_start + interval '6 days', interval '1 day') d
  where d::date >= p.source_month
    and d::date < (p.source_month + interval '1 month')::date
),
coverage as (
  select
    wd.week_start,
    count(*)::integer as expected_days,
    count(*) filter (where wd.day <= p.loaded_max_date)::integer as loaded_days,
    count(*) filter (where wd.day > p.loaded_max_date)::integer as not_loaded_days
  from week_days wd
  cross join params p
  group by wd.week_start
),
denominator as (
  select
    p.target_report_id as report_id,
    rr.effective_week_start::date as week_start,
    upper(trim(rr.cod_rt)) as cod_rt,
    upper(trim(rr.cliente_norm)) as cliente_norm,
    min(rr.local_nombre) as local_nombre,
    min(rr.gestor) as gestor,
    min(rr.rutero) as rutero,
    min(rr.reponedor_scope) as reponedor_scope,
    min(rr.modalidad) as modalidad,
    max(rr.visitas_exigidas_semana) as visitas_exigidas_semana
  from cg_core.v_rr_frecuencia_base_resuelta_v2 rr
  cross join params p
  where rr.effective_week_start::date = any(p.replace_scope_weeks)
    and nullif(trim(rr.cod_rt), '') is not null
    and nullif(trim(rr.cliente_norm), '') is not null
  group by p.target_report_id, rr.effective_week_start::date, upper(trim(rr.cod_rt)), upper(trim(rr.cliente_norm))
),
canonical_events as (
  select
    c.report_id,
    c.week_start,
    upper(trim(c.cod_rt)) as cod_rt,
    upper(trim(c.cliente_norm)) as cliente_norm,
    count(*)::integer as canonical_event_count
  from cg_reform.canonical_hot_keys c
  join params p on p.target_report_id = c.report_id
  group by c.report_id, c.week_start, upper(trim(c.cod_rt)), upper(trim(c.cliente_norm))
),
scored as (
  select
    d.*,
    coalesce(c.canonical_event_count, 0)::integer as canonical_event_count,
    0::integer as quarantine_presence_count,
    coalesce(c.canonical_event_count, 0)::integer as presence_count,
    false as conflicted_present_flag,
    cov.loaded_days,
    cov.expected_days,
    cov.not_loaded_days,
    case
      when cov.loaded_days = cov.expected_days then 'FULL'
      when cov.loaded_days = 0 then 'NONE'
      else 'PARTIAL'
    end as week_load_status,
    case
      when cov.loaded_days < cov.expected_days then false
      else true
    end as report_loaded,
    case
      when cov.loaded_days < cov.expected_days then 'NOT_LOADED'
      when coalesce(c.canonical_event_count, 0) >= 1 then 'EXISTS'
      else 'MISSING'
    end as status,
    case
      when cov.loaded_days < cov.expected_days then 'week_partially_or_not_loaded_by_route_b_day_coverage'
      when coalesce(c.canonical_event_count, 0) >= 1 then 'event_key_exists_for_scope'
      else 'loaded_report_without_scope_event'
    end as status_reason
  from denominator d
  join coverage cov on cov.week_start = d.week_start
  left join canonical_events c
    on c.report_id = d.report_id
   and c.week_start = d.week_start
   and c.cod_rt = d.cod_rt
   and c.cliente_norm = d.cliente_norm
)
insert into cg_reform.report_existence_week (
  existence_key,
  report_id,
  week_start,
  cod_rt,
  cliente_norm,
  local_nombre,
  gestor,
  rutero,
  reponedor_scope,
  modalidad,
  visitas_exigidas_semana,
  event_count,
  canonical_event_count,
  quarantine_presence_count,
  presence_count,
  conflicted_present_flag,
  status,
  status_reason,
  report_loaded,
  loaded_days,
  expected_days,
  not_loaded_days,
  week_load_status
)
select
  md5(concat_ws('|', report_id, week_start::text, cod_rt, cliente_norm)),
  report_id,
  week_start,
  cod_rt,
  cliente_norm,
  local_nombre,
  gestor,
  rutero,
  reponedor_scope,
  modalidad,
  visitas_exigidas_semana,
  presence_count::bigint,
  canonical_event_count,
  quarantine_presence_count,
  presence_count,
  conflicted_present_flag,
  status,
  status_reason,
  report_loaded,
  loaded_days,
  expected_days,
  not_loaded_days,
  week_load_status
from scored;

-- In-transaction validation gates.
do $$
declare
  v_report_id text := (select target_report_id from pg_temp.route_b_apply_params);
  v_run_id text := (select target_run_id from pg_temp.route_b_apply_params);
  v_expected_mapped_canonical_rows bigint := (select expected_mapped_canonical_rows from pg_temp.route_b_apply_params);
  v_expected_unresolved_scope_rows bigint := (select expected_unresolved_scope_rows from pg_temp.route_b_apply_params);
  v_canonical_rows bigint;
  v_conflict_862144 bigint;
  v_quarantine_conflicts bigint;
  v_quarantine_unresolved bigint;
  v_partial_week_rows bigint;
  v_pending_week_rows bigint;
  v_false_missing bigint;
begin
  select count(*) into v_canonical_rows
  from cg_reform.canonical_hot_keys
  where report_id = v_report_id;

  if v_canonical_rows <> v_expected_mapped_canonical_rows then
    raise exception 'Route-B canonical row count mismatch: % expected=%', v_canonical_rows, v_expected_mapped_canonical_rows;
  end if;

  select count(*) into v_conflict_862144
  from cg_reform.canonical_hot_keys
  where report_id = v_report_id
    and event_key = '862144'
    and n_fotos_calculado = 4
    and conflict_flag = false;

  if v_conflict_862144 <> 1 then
    raise exception 'Route-B 862144 cap-split merge validation failed: %', v_conflict_862144;
  end if;

  select count(*) into v_quarantine_conflicts
  from cg_reform.quarantine_min
  where report_id = v_report_id
    and reason = 'ID_CONTENT_CONFLICT';

  if v_quarantine_conflicts <> 0 then
    raise exception 'Route-B expected zero true ID_CONTENT_CONFLICT rows, got %', v_quarantine_conflicts;
  end if;

  select count(*) into v_quarantine_unresolved
  from cg_reform.quarantine_min
  where report_id = v_report_id
    and reason = 'UNRESOLVED_SCOPE';

  if v_quarantine_unresolved <> v_expected_unresolved_scope_rows then
    raise exception
      'Route-B unresolved scope quarantine row count mismatch: % expected=%',
      v_quarantine_unresolved,
      v_expected_unresolved_scope_rows;
  end if;

  select count(*) into v_partial_week_rows
  from cg_reform.report_existence_week e
  cross join pg_temp.route_b_apply_params p
  where e.report_id = v_report_id
    and e.week_start = any(p.deferred_source_weeks);

  if v_partial_week_rows <> 0 then
    raise exception 'Route-B deferred source week should not be materialized without denominator: % rows', v_partial_week_rows;
  end if;

  select count(*) into v_pending_week_rows
  from (
    select 1
    from cg_reform.canonical_hot_keys
    where report_id = v_report_id
      and week_start = date '2026-06-22'
    union all
    select 1
    from cg_reform.quarantine_min
    where report_id = v_report_id
      and week_start = date '2026-06-22'
    union all
    select 1
    from cg_reform.report_existence_week
    where report_id = v_report_id
      and week_start = date '2026-06-22'
  ) pending_week;

  if v_pending_week_rows <> 0 then
    raise exception 'Route-B pending 2026-06-22 week must remain outside 009E: % rows', v_pending_week_rows;
  end if;

  select count(*) into v_false_missing
  from cg_reform.report_existence_week
  where report_id = v_report_id
    and report_loaded = false
    and status = 'MISSING';

  if v_false_missing <> 0 then
    raise exception 'Route-B NOT_LOADED>MISSING validation failed: % rows', v_false_missing;
  end if;
end $$;

-- No COMMIT here. Manual operator must COMMIT only after this file completes
-- without errors and any required in-transaction review is complete.
