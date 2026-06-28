-- FAST_REFORM_009E / 1_PRECHECK_READONLY
-- Issue: #18
-- Mode: SQL_PACKAGE_REVIEW_ONLY
--
-- Purpose:
--   Read-only precheck for the revised Route-B apply design after Claude gate.
--
-- Route decision:
--   route_decision = A_FREEZE_009E_TO_EXISTING_5_FILE_ARTIFACT
--   artifact_name = fast_reform_009c_route_b_20260620_2345
--   artifact_source_coverage_max_date = 2026-06-19
--   current_local_folder_source_coverage_max_date: 2026-06-24
--   new_file_deferred_to_next_phase = photo-excel-admin_1782440454408.xlsx
--   pending_source_weeks_009F = [2026-06-22]
--   future_not_loaded_weeks = [2026-06-29]
--   009E treats cg_reform.report_registry/canonical_hot_keys empty state as
--   FIRST_ROUTE_B_PARTIAL_BUILD, not replacement over an existing current report.
--
-- Guards:
--   - no DDL
--   - no DML
--   - no writes
--   - no persistent staging tables
--   - no cg_raw.kpione2_raw mutation
--   - no public.* / cg_mart.* changes
--   - no grants / app cutover / reclaim

begin read only;

select current_user;
show transaction_read_only;
show default_transaction_read_only;
show server_version_num;
show search_path;

select
  current_setting('transaction_read_only') = 'on' as transaction_is_read_only,
  current_setting('default_transaction_read_only') = 'on' as default_transaction_is_read_only,
  current_setting('server_version_num')::int >= 150000 as supports_security_invoker_view_option;

-- Confirm required current cg_reform objects exist.
with expected_objects(obj_name) as (
  values
    ('cg_reform.report_registry'),
    ('cg_reform.chunk_registry'),
    ('cg_reform.canonical_hot_keys'),
    ('cg_reform.quarantine_min'),
    ('cg_reform.report_existence_week'),
    ('cg_reform.v_report_existence_week'),
    ('cg_reform.v_report_existence_week_hot'),
    ('cg_reform.v_freshness_status'),
    ('cg_reform.v_quarantine_evidence'),
    ('cg_reform.v_chunk_inventory'),
    ('cg_reform.v_compare_weekly')
)
select
  obj_name,
  to_regclass(obj_name) as existing_object,
  (to_regclass(obj_name) is not null) as exists
from expected_objects
order by obj_name;

-- Persistent Route-B staging tables are explicitly disallowed.
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

-- Current authoritative cg_reform dictionary for collision review.
select
  table_schema,
  table_name,
  ordinal_position,
  column_name,
  data_type,
  udt_name,
  is_nullable
from information_schema.columns
where table_schema = 'cg_reform'
  and table_name in (
    'report_registry',
    'chunk_registry',
    'canonical_hot_keys',
    'quarantine_min',
    'report_existence_week'
  )
order by table_name, ordinal_position;

-- Check required additive columns before/after 2_ADDITIVE_DDL.
with expected_columns(table_name, column_name) as (
  values
    ('report_registry', 'source_run_id'),
    ('report_registry', 'source_month'),
    ('report_registry', 'source_artifact'),
    ('report_registry', 'manifest_uri'),
    ('report_registry', 'manifest_sha256'),
    ('report_registry', 'source_generated_at_min'),
    ('report_registry', 'source_generated_at_max'),
    -- In 009E this column stores artifact_source_coverage_max_date, not the
    -- current local folder source coverage.
    ('report_registry', 'source_coverage_max_date'),
    ('report_registry', 'route_decision'),
    ('report_registry', 'rebuild_strategy'),
    ('report_registry', 'replace_scope_weeks'),
    ('chunk_registry', 'source_generated_at'),
    ('chunk_registry', 'docprops_created_at'),
    ('chunk_registry', 'candidate_state'),
    ('chunk_registry', 'is_export_cap_split'),
    ('chunk_registry', 'is_split_complement'),
    ('chunk_registry', 'complements_date'),
    ('chunk_registry', 'complements_file_hashes'),
    ('canonical_hot_keys', 'source_run_id'),
    ('canonical_hot_keys', 'source_month'),
    ('canonical_hot_keys', 'source_generated_at'),
    ('canonical_hot_keys', 'source_file_hashes'),
    ('canonical_hot_keys', 'source_file_count'),
    ('canonical_hot_keys', 'n_fotos_calculado'),
    ('quarantine_min', 'source_run_id'),
    ('quarantine_min', 'source_month'),
    ('quarantine_min', 'source_generated_at'),
    ('quarantine_min', 'source_file_hashes'),
    ('report_existence_week', 'loaded_days'),
    ('report_existence_week', 'expected_days'),
    ('report_existence_week', 'not_loaded_days'),
    ('report_existence_week', 'week_load_status')
)
select
  e.table_name,
  e.column_name,
  (c.column_name is not null) as exists
from expected_columns e
left join information_schema.columns c
  on c.table_schema = 'cg_reform'
 and c.table_name = e.table_name
 and c.column_name = e.column_name
order by e.table_name, e.column_name;

-- Type reconciliation for the known 008B drift risk.
select
  table_schema,
  table_name,
  column_name,
  data_type,
  udt_name
from information_schema.columns
where table_schema = 'cg_reform'
  and table_name = 'quarantine_min'
  and column_name = 'conflicting_chunk_ids';

-- Latest/current report surface before the Route-B replace.
select
  report_id,
  model_name,
  conflict_policy,
  source_system,
  source_relation,
  loaded_min_date,
  loaded_max_date,
  max_loaded_week,
  hot_start_week,
  built_at
from cg_reform.report_registry
order by built_at desc, report_id desc
limit 5;

-- Read-only snapshot counts for rows that 009E may replace in current-surface tables.
with params as (
  select
    date '2026-06-01' as source_month,
    array[
      date '2026-06-01',
      date '2026-06-08',
      date '2026-06-15',
      date '2026-06-22',
      date '2026-06-29'
    ] as replace_scope_weeks
),
canonical_snapshot as (
  select
    c.week_start,
    count(*)::bigint as canonical_rows,
    count(*) filter (where c.conflict_flag)::bigint as conflict_rows
  from cg_reform.canonical_hot_keys c
  cross join params p
  where c.week_start = any(p.replace_scope_weeks)
  group by c.week_start
),
existence_snapshot as (
  select
    e.week_start,
    count(*)::bigint as existence_rows,
    count(*) filter (where e.status = 'EXISTS')::bigint as exists_rows,
    count(*) filter (where e.status = 'MISSING')::bigint as missing_rows,
    count(*) filter (where e.status = 'NOT_LOADED')::bigint as not_loaded_rows
  from cg_reform.report_existence_week e
  cross join params p
  where e.week_start = any(p.replace_scope_weeks)
  group by e.week_start
)
select
  w.week_start,
  coalesce(c.canonical_rows, 0) as canonical_rows,
  coalesce(c.conflict_rows, 0) as canonical_conflict_rows,
  coalesce(e.existence_rows, 0) as existence_rows,
  coalesce(e.exists_rows, 0) as exists_rows,
  coalesce(e.missing_rows, 0) as missing_rows,
  coalesce(e.not_loaded_rows, 0) as not_loaded_rows
from unnest((select replace_scope_weeks from params)) as w(week_start)
left join canonical_snapshot c using (week_start)
left join existence_snapshot e using (week_start)
order by w.week_start;

-- Remnant HOT weeks before June: these are not touched by the June-only replace.
select
  week_start,
  count(*)::bigint as canonical_rows_pre_june
from cg_reform.canonical_hot_keys
where week_start < date '2026-06-01'
group by week_start
order by week_start;

-- Future June weeks policy: month-complete rebuild must reconstruct them as NOT_LOADED.
select
  week_start,
  count(*)::bigint as canonical_rows_future_june
from cg_reform.canonical_hot_keys
where week_start in (date '2026-06-22', date '2026-06-29')
group by week_start
order by week_start;

-- Denominator availability for every June replace-scope week, including future weeks.
with params as (
  select array[
    date '2026-06-01',
    date '2026-06-08',
    date '2026-06-15',
    date '2026-06-22',
    date '2026-06-29'
  ] as replace_scope_weeks
)
select
  rr.effective_week_start::date as week_start,
  count(*)::bigint as denominator_rows,
  count(*) filter (where nullif(trim(rr.cod_rt), '') is null)::bigint as missing_cod_rt,
  count(*) filter (where nullif(trim(rr.cliente_norm), '') is null)::bigint as missing_cliente_norm
from cg_core.v_rr_frecuencia_base_resuelta_v2 rr
cross join params p
where rr.effective_week_start::date = any(p.replace_scope_weeks)
group by rr.effective_week_start::date
order by rr.effective_week_start::date;

-- Confirm app-facing/current mart objects remain observable only; 009E must not modify them.
select
  n.nspname || '.' || c.relname as object_name,
  case c.relkind
    when 'r' then 'table'
    when 'v' then 'view'
    when 'm' then 'materialized_view'
    else c.relkind::text
  end as object_type,
  c.relrowsecurity as rls_enabled,
  case when to_regrole('anon') is null then null else has_table_privilege('anon', c.oid, 'SELECT') end as anon_select,
  case when to_regrole('authenticated') is null then null else has_table_privilege('authenticated', c.oid, 'SELECT') end as authenticated_select
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname || '.' || c.relname in (
  'cg_raw.kpione2_raw',
  'cg_mart.fact_cg_out_weekly_v2',
  'cg_mart.fact_cg_visita_dia_resuelta_v2',
  'cg_mart.mv_cg_out_weekly_v2',
  'public.v_cg_cumplimiento_detalle_v2',
  'public.v_cg_cumplimiento_semana_scope_v2'
)
order by object_name;

rollback;
