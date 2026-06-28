-- FAST_REFORM_009E / 6_CLEANUP_ROLLBACK_AFTER_COMMIT
-- Issue: #18
-- Mode: SQL_PACKAGE_REVIEW_ONLY
--
-- Purpose:
--   Cleanup rollback after a committed Route-B June current-surface replace.
--   This is NOT a complete restoration rollback by itself.
--
-- Guards:
--   - execute only after explicit rollback authorization
--   - cg_reform only
--   - no cg_raw.* / public.* / cg_mart.* changes
--   - no grants / app cutover / reclaim
--
-- Important:
--   Rollback before commit = plain ROLLBACK of the still-open apply transaction.
--   Rollback after commit = cleanup + restore from snapshot or re-run 008B.
--
--   Because cg_reform.canonical_hot_keys is a current-surface table keyed by
--   event_key, rollback after commit needs either:
--     A) a pre-apply snapshot exported from 1_PRECHECK_READONLY, or
--     B) re-run 008B using the previous validated build.
--   This script removes the 009E target report/current rows and leaves the
--   system ready for that explicit restore step.
--
-- Route decision:
--   route_decision = A_FREEZE_009E_TO_EXISTING_5_FILE_ARTIFACT
--   artifact_name = fast_reform_009c_route_b_20260620_2345
--   artifact_source_coverage_max_date = 2026-06-19
--   current_local_folder_source_coverage_max_date: 2026-06-24
--   new_file_deferred_to_next_phase = photo-excel-admin_1782440454408.xlsx
--   pending_source_weeks_009F = [2026-06-22]
--   future_not_loaded_weeks = [2026-06-29]
--
-- Scope:
--   apply_scope_weeks = [2026-06-01, 2026-06-08]
--   deferred_source_weeks = [2026-06-15]
--     2026-06-15 = DEFERRED_SOURCE_WEEK_DENOMINATOR_MISSING
--   pending_source_weeks_009F = [2026-06-22]
--     2026-06-22 has partial source but is outside frozen 009E.
--   future_not_loaded_weeks = [2026-06-29]
--     2026-06-29 = FUTURE_WEEK_DENOMINATOR_MISSING_OR_NOT_READY

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

create temp table pg_temp.route_b_rollback_params (
  target_report_id text primary key,
  target_run_id text not null,
  source_month date not null,
  replace_scope_weeks date[] not null
) on commit drop;

insert into pg_temp.route_b_rollback_params values (
  'route_b_report_existence_first_202606_20260620_2345',
  'fast_reform_009c_route_b_20260620_2345',
  date '2026-06-01',
  array[
    date '2026-06-01',
    date '2026-06-08'
  ]
);

delete from cg_reform.report_existence_week e
using pg_temp.route_b_rollback_params p
where e.report_id = p.target_report_id
   or e.week_start = any(p.replace_scope_weeks);

delete from cg_reform.quarantine_min q
using pg_temp.route_b_rollback_params p
where q.report_id = p.target_report_id
   or q.source_run_id = p.target_run_id
   or q.source_month = p.source_month;

delete from cg_reform.canonical_hot_keys c
using pg_temp.route_b_rollback_params p
where c.report_id = p.target_report_id
   or c.source_run_id = p.target_run_id
   or c.source_month = p.source_month
   or c.week_start = any(p.replace_scope_weeks);

delete from cg_reform.chunk_registry c
using pg_temp.route_b_rollback_params p
where c.report_id = p.target_report_id;

delete from cg_reform.report_registry r
using pg_temp.route_b_rollback_params p
where r.report_id = p.target_report_id;

-- Validation after deletion.
do $$
declare
  v_report_id text := (select target_report_id from pg_temp.route_b_rollback_params);
  v_remaining bigint;
begin
  select
    (select count(*) from cg_reform.report_registry where report_id = v_report_id)
    + (select count(*) from cg_reform.chunk_registry where report_id = v_report_id)
    + (select count(*) from cg_reform.canonical_hot_keys where report_id = v_report_id)
    + (select count(*) from cg_reform.report_existence_week where report_id = v_report_id)
    + (select count(*) from cg_reform.quarantine_min where report_id = v_report_id)
  into v_remaining;

  if v_remaining <> 0 then
    raise exception 'Route-B rollback deletion incomplete: % target rows remain', v_remaining;
  end if;
end $$;

commit;

-- Required manual follow-up after commit:
--   Restore the previous current surface from exported snapshot or re-run 008B.
--   Then execute 5_VALIDATION_READONLY.sql and confirm latest cg_reform views point
--   to the intended report_id.
