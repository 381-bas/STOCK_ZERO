-- NO APPLY
-- REVIEW ROLLBACK ONLY
-- Requires separate Bastian authorization before any SQL execution.
-- Drops only Route B additive review objects.

begin;

drop view if exists cg_raw.v_kpione2_photo_day_presence_contract;
drop view if exists cg_raw.v_kpione2_photo_event_contract;
drop table if exists cg_raw.kpione2_photo_raw;

commit;
