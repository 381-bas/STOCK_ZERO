-- FAST_REFORM_009E / 2B_MICRO_DDL_QUARANTINE_REASON_EXPAND
-- Expande quarantine_min_reason_chk para incluir razones Route-B.
-- Idempotente: usa DO block con drop-if-exists.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'quarantine_min_reason_chk'
      and conrelid = 'cg_reform.quarantine_min'::regclass
  ) then
    alter table cg_reform.quarantine_min
      drop constraint quarantine_min_reason_chk;
  end if;

  alter table cg_reform.quarantine_min
    add constraint quarantine_min_reason_chk check (
      reason in (
        'missing_event_key',
        'same_key_different_content_hash',
        'missing_cod_rt',
        'missing_cliente',
        'missing_fecha_visita',
        'ID_CONTENT_CONFLICT',
        'UNRESOLVED_SCOPE'
      )
    );
end $$;

commit;
