-- FAST_REFORM_009E / 5_VALIDATION_READONLY
-- Issue: #18
-- Mode: SQL_PACKAGE_REVIEW_ONLY
--
-- Purpose:
--   Read-only post-apply validation for the Route-B current-surface replace.
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
--   expected_deferred_source_event_rows = 6997
--   expected_file_manifest_rows = 5
--   expected_day_coverage_rows = 19

begin read only;

select current_user;
show transaction_read_only;
show default_transaction_read_only;

with params as (
  select
    'fast_reform_009c_route_b_20260620_2345'::text as target_run_id,
    'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id,
    date '2026-06-01' as source_month
)
select
  r.report_id,
  r.source_run_id,
  r.source_month,
  r.source_relation,
  r.source_artifact,
  r.loaded_min_date,
  r.loaded_max_date,
  r.source_coverage_max_date,
  r.hot_rows,
  r.unique_event_keys,
  r.hot_weeks,
  r.rebuild_strategy,
  r.duplicate_key_count,
  r.same_key_different_content_hash,
  r.route_decision,
  r.replace_scope_weeks,
  r.built_at
from cg_reform.report_registry r
join params p on p.target_report_id = r.report_id;

-- Source visibility and scope decision for partial June Route-B rebuild.
with params as (
  select
    'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id,
    date '2026-06-19' as artifact_source_coverage_max_date,
    date '2026-06-19' as source_coverage_max_date,
    date '2026-06-24' as current_local_folder_source_coverage_max_date,
    'photo-excel-admin_1782440454408.xlsx'::text as new_file_deferred_to_next_phase,
    array[date '2026-06-01', date '2026-06-08'] as apply_scope_weeks,
    array[date '2026-06-15'] as deferred_source_weeks,
    array[date '2026-06-22'] as pending_source_weeks_009F,
    array[date '2026-06-29'] as future_not_loaded_weeks,
    22195::bigint as expected_artifact_event_rows,
    15198::bigint as expected_apply_scope_event_rows,
    6997::bigint as expected_deferred_source_event_rows
)
select
  artifact_source_coverage_max_date,
  source_coverage_max_date,
  current_local_folder_source_coverage_max_date,
  new_file_deferred_to_next_phase,
  apply_scope_weeks,
  deferred_source_weeks,
  pending_source_weeks_009F,
  future_not_loaded_weeks,
  expected_artifact_event_rows,
  expected_apply_scope_event_rows,
  expected_deferred_source_event_rows,
  'DEFERRED_SOURCE_WEEK_DENOMINATOR_MISSING' as deferred_source_reason,
  'FUTURE_WEEK_DENOMINATOR_MISSING_OR_NOT_READY' as future_week_reason
from params;

-- Report metadata must describe the materialized apply scope, not the full artifact.
with params as (
  select
    'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id,
    15145::bigint as expected_mapped_canonical_rows
)
select
  r.hot_rows,
  r.unique_event_keys,
  p.expected_mapped_canonical_rows,
  (r.hot_rows = p.expected_mapped_canonical_rows) as hot_rows_match_mapped_canonical,
  (r.unique_event_keys = p.expected_mapped_canonical_rows) as unique_event_keys_match_mapped_canonical
from cg_reform.report_registry r
join params p on p.target_report_id = r.report_id;

with params as (
  select 'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id
)
select 'canonical_hot_keys' as object_name, count(*)::bigint as rows
from cg_reform.canonical_hot_keys c join params p on p.target_report_id = c.report_id
union all
select 'chunk_registry', count(*)::bigint
from cg_reform.chunk_registry c join params p on p.target_report_id = c.report_id
union all
select 'quarantine_min', count(*)::bigint
from cg_reform.quarantine_min q join params p on p.target_report_id = q.report_id
union all
select 'report_existence_week', count(*)::bigint
from cg_reform.report_existence_week e join params p on p.target_report_id = e.report_id
order by object_name;

-- Canonical current surface must contain only apply-scope events.
with params as (
  select
    'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id,
    15145::bigint as expected_mapped_canonical_rows
)
select
  count(*)::bigint as canonical_rows,
  p.expected_mapped_canonical_rows,
  (count(*)::bigint = p.expected_mapped_canonical_rows) as canonical_rows_match_expected
from cg_reform.canonical_hot_keys c
join params p on p.target_report_id = c.report_id
group by p.expected_mapped_canonical_rows;

-- 862144 must be canonical, not quarantined.
with params as (
  select 'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id
)
select
  c.event_key,
  c.report_id,
  c.week_start,
  c.cod_rt,
  c.cliente_norm,
  c.content_hash,
  c.conflict_flag,
  c.conflict_version_count,
  c.n_fotos_calculado,
  c.source_file_count,
  c.source_generated_at,
  c.source_file_hashes
from cg_reform.canonical_hot_keys c
join params p on p.target_report_id = c.report_id
where c.event_key = '862144';

with params as (
  select 'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id
)
select
  count(*)::bigint as id_content_conflict_rows,
  count(*) filter (where event_key = '862144')::bigint as conflict_862144_rows
from cg_reform.quarantine_min q
join params p on p.target_report_id = q.report_id
where q.reason = 'ID_CONTENT_CONFLICT';

-- Unresolved apply-scope events must be quarantined, not canonicalized.
select
  count(*) filter (where reason = 'UNRESOLVED_SCOPE') as unresolved_scope_rows,
  (
    count(*) filter (where reason = 'UNRESOLVED_SCOPE') = 53
  ) as unresolved_scope_count_pass
from cg_reform.quarantine_min
where report_id = 'route_b_report_existence_first_202606_20260620_2345';

-- Weekly status and day-aware coverage.
with params as (
  select 'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id
)
select
  week_start,
  week_load_status,
  loaded_days,
  expected_days,
  not_loaded_days,
  status,
  status_reason,
  report_loaded,
  count(*)::bigint as rows,
  sum(canonical_event_count)::bigint as canonical_event_count,
  sum(presence_count)::bigint as presence_count
from cg_reform.report_existence_week e
join params p on p.target_report_id = e.report_id
group by
  week_start,
  week_load_status,
  loaded_days,
  expected_days,
  not_loaded_days,
  status,
  status_reason,
  report_loaded
order by week_start, status, status_reason;

-- NOT_LOADED must dominate MISSING for partial/unloaded weeks.
with params as (
  select
    'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id,
    array[date '2026-06-15'] as deferred_source_weeks
)
select
  count(*) filter (where report_loaded = false and status = 'MISSING')::bigint
    as not_loaded_missing_violations,
  count(*) filter (where week_start = any(p.deferred_source_weeks))::bigint
    as deferred_source_week_materialized_rows
from cg_reform.report_existence_week e
join params p on p.target_report_id = e.report_id;

-- File/chunk evidence.
with params as (
  select 'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id
)
select
  chunk_id,
  batch_id,
  source_file,
  file_hash,
  source_generated_at,
  docprops_created_at,
  row_count,
  unique_event_keys_hot,
  candidate_state,
  is_export_cap_split,
  is_split_complement,
  complements_date,
  complements_file_hashes
from cg_reform.chunk_registry c
join params p on p.target_report_id = c.report_id
order by source_generated_at, source_file;

-- Replace scope counts.
with params as (
  select
    'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id,
    array[
      date '2026-06-01',
      date '2026-06-08'
    ] as replace_scope_weeks
)
select
  w.week_start,
  (select count(*)::bigint from cg_reform.canonical_hot_keys c join params p on p.target_report_id = c.report_id where c.week_start = w.week_start) as canonical_rows,
  (select count(*)::bigint from cg_reform.report_existence_week e join params p on p.target_report_id = e.report_id where e.week_start = w.week_start) as existence_rows
from unnest((select replace_scope_weeks from params)) as w(week_start)
order by w.week_start;

-- Excluded weeks must remain outside the Route-B partial June target report.
with params as (
  select
    'route_b_report_existence_first_202606_20260620_2345'::text as target_report_id,
    array[date '2026-06-15'] as deferred_source_weeks,
    array[date '2026-06-22'] as pending_source_weeks_009F,
    array[date '2026-06-29'] as future_not_loaded_weeks
),
excluded_weeks as (
  select 'DEFERRED_SOURCE_WEEK_DENOMINATOR_MISSING' as exclusion_reason, unnest(deferred_source_weeks) as week_start from params
  union all
  select 'PENDING_SOURCE_WEEK_009F_DENOMINATOR_MISSING' as exclusion_reason, unnest(pending_source_weeks_009F) as week_start from params
  union all
  select 'FUTURE_WEEK_DENOMINATOR_MISSING_OR_NOT_READY' as exclusion_reason, unnest(future_not_loaded_weeks) as week_start from params
)
select
  x.exclusion_reason,
  x.week_start,
  (select count(*)::bigint from cg_reform.canonical_hot_keys c join params p on p.target_report_id = c.report_id where c.week_start = x.week_start) as canonical_rows,
  (select count(*)::bigint from cg_reform.report_existence_week e join params p on p.target_report_id = e.report_id where e.week_start = x.week_start) as existence_rows
from excluded_weeks x
order by x.week_start;

-- No persistent Route-B stage objects.
with forbidden_objects(obj_name) as (
  values
    ('cg_reform.route_b_events_stage'),
    ('cg_reform.route_b_file_manifest_stage'),
    ('cg_reform.route_b_day_coverage'),
    ('cg_reform.route_b_conflicts_stage')
)
select
  obj_name,
  to_regclass(obj_name) as existing_object,
  (to_regclass(obj_name) is null) as no_persistent_stage_pass
from forbidden_objects
order by obj_name;

-- First Route-B apply must not grant anon/authenticated access to cg_reform objects.
select
  n.nspname || '.' || c.relname as object_name,
  case c.relkind when 'r' then 'table' when 'v' then 'view' when 'm' then 'materialized_view' else c.relkind::text end as object_type,
  case when to_regrole('anon') is null then null else has_table_privilege('anon', c.oid, 'SELECT') end as anon_select,
  case when to_regrole('authenticated') is null then null else has_table_privilege('authenticated', c.oid, 'SELECT') end as authenticated_select
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'cg_reform'
  and c.relkind in ('r', 'v', 'm')
order by object_name;

rollback;
