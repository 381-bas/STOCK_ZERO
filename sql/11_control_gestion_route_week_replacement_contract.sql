-- NO APPLY
-- IMPLEMENTATION CONTRACT ONLY
-- REQUIRES SEPARATE BASTIAN AUTHORIZATION

-- Contract for guarded weekly replacement of public.ruta_rutero from a full
-- RUTA_RUTERO workbook snapshot. This file is review-only in CG005E and must
-- not be executed without a separate Bastian authorization.

begin;

-- Batch lifecycle compatibility required before apply:
--
-- alter table cg_core.ruta_rutero_load_batch
--   drop constraint if exists ruta_rutero_load_batch_status_check;
--
-- alter table cg_core.ruta_rutero_load_batch
--   add constraint ruta_rutero_load_batch_status_check
--   check (status in ('pending', 'failed', 'ok', 'cancelled', 'superseded'));

create table if not exists cg_core.ruta_rutero_week_assignment (
    assignment_id bigint generated always as identity primary key,
    effective_week_start date not null,
    route_policy_version text not null,
    ruta_batch_id bigint not null,
    assignment_status text not null,
    input_file_name text not null,
    input_file_sha256 text not null,
    schema_signature text not null,
    current_surface_hash text not null,
    resolved_surface_hash text not null,
    assigned_at timestamptz not null default now(),
    assigned_by text not null,
    replaces_ruta_batch_id bigint,
    rollback_of_assignment_id bigint,
    notes text,
    constraint ruta_rutero_week_assignment_monday_check
        check (extract(isodow from effective_week_start) = 1),
    constraint ruta_rutero_week_assignment_status_check
        check (assignment_status in ('PENDING', 'ACTIVE', 'SUPERSEDED', 'FAILED', 'ROLLED_BACK')),
    constraint ruta_rutero_week_assignment_policy_check
        check (route_policy_version <> ''),
    constraint ruta_rutero_week_assignment_current_hash_check
        check (current_surface_hash ~ '^[A-Fa-f0-9]{64}$'),
    constraint ruta_rutero_week_assignment_resolved_hash_check
        check (resolved_surface_hash ~ '^[A-Fa-f0-9]{64}$')
);

do $$
begin
    if to_regclass('cg_core.ruta_rutero_load_batch') is not null then
        alter table cg_core.ruta_rutero_week_assignment
            add constraint ruta_rutero_week_assignment_batch_fk
            foreign key (ruta_batch_id)
            references cg_core.ruta_rutero_load_batch(ruta_batch_id);
    end if;
exception
    when duplicate_object then null;
end $$;

do $$
begin
    alter table cg_core.ruta_rutero_week_assignment
        add constraint ruta_rutero_week_assignment_rollback_fk
        foreign key (rollback_of_assignment_id)
        references cg_core.ruta_rutero_week_assignment(assignment_id);
exception
    when duplicate_object then null;
end $$;

create index if not exists ix_ruta_rutero_week_assignment_week
    on cg_core.ruta_rutero_week_assignment (effective_week_start, route_policy_version, assigned_at desc);

create unique index if not exists ux_ruta_rutero_week_assignment_active
    on cg_core.ruta_rutero_week_assignment (effective_week_start, route_policy_version)
    where assignment_status = 'ACTIVE';

-- Week-batch resolution.
--
-- Explicit ACTIVE assignment is authoritative for its week. Legacy inference is
-- visible only for weeks with no ACTIVE assignment, so legacy batches cannot
-- backfill bajas from an explicitly assigned weekly snapshot.

create or replace view cg_core.v_ruta_rutero_load_batch_week_v2 as
with explicit_assignment as (
    select
        a.ruta_batch_id,
        a.effective_week_start,
        extract(week from a.effective_week_start)::integer as effective_week_iso,
        a.route_policy_version,
        'EXPLICIT_ASSIGNMENT'::text as route_week_source,
        a.assignment_id,
        a.assigned_at
    from cg_core.ruta_rutero_week_assignment a
    where a.assignment_status = 'ACTIVE'
), legacy_inferred as (
    select
        b.ruta_batch_id,
        date_trunc('week', b.loaded_at at time zone 'America/Santiago')::date as effective_week_start,
        extract(week from (b.loaded_at at time zone 'America/Santiago')::date)::integer as effective_week_iso,
        'LEGACY_LOADED_AT_INFERENCE'::text as route_policy_version,
        'LEGACY_INFERRED'::text as route_week_source,
        null::bigint as assignment_id,
        b.loaded_at as assigned_at
    from cg_core.ruta_rutero_load_batch b
    where b.status = 'ok'
      and not exists (
          select 1
          from cg_core.ruta_rutero_week_assignment a
          where a.assignment_status = 'ACTIVE'
            and a.effective_week_start =
                date_trunc('week', b.loaded_at at time zone 'America/Santiago')::date
      )
)
select *
from explicit_assignment
union all
select *
from legacy_inferred;

create or replace view cg_core.v_ruta_rutero_latest_week_batch_v2 as
with ranked as (
    select
        *,
        row_number() over (
            partition by effective_week_start
            order by
                case when route_week_source = 'EXPLICIT_ASSIGNMENT' then 0 else 1 end,
                assigned_at desc,
                ruta_batch_id desc
        ) as rn
    from cg_core.v_ruta_rutero_load_batch_week_v2
)
select
    ruta_batch_id,
    effective_week_start,
    effective_week_iso,
    route_policy_version,
    route_week_source,
    assignment_id
from ranked
where rn = 1;

-- Resolved weekly frequency surface.
--
-- The resolved view consumes only v_ruta_rutero_latest_week_batch_v2. For a
-- week with an explicit assignment, that single batch is the only source of
-- grains; history remains available for audit but never fills gaps.
--
-- Logical winner precedence is independent from source_row:
-- 1. EXPLICIT_ASSIGNMENT first;
-- 2. weekly obligation descending;
-- 3. day-plan sum descending;
-- 4. operational completeness descending;
-- 5. normalized operational tuple;
-- 6. row_hash ascending.

create or replace view cg_core.v_rr_frecuencia_base_resuelta_v2 as
with source_rows as (
    select
        wb.effective_week_start,
        wb.effective_week_iso,
        wb.route_policy_version,
        wb.route_week_source,
        r.ruta_batch_id,
        nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), '') as cod_rt_norm,
        nullif(trim(coalesce(r.cod_rt, r.cod_rt_norm)), '') as cod_rt,
        nullif(trim(coalesce(r.cod_b2b_norm, r.cod_b2b)), '') as cod_b2b,
        nullif(trim(r.local_nombre), '') as local_nombre,
        nullif(trim(r.direccion), '') as direccion,
        nullif(trim(r.cliente), '') as cliente,
        upper(trim(coalesce(nullif(trim(r.cliente_norm), ''), nullif(trim(r.cliente), ''), ''))) as cliente_norm,
        nullif(trim(r.gestores), '') as gestor_value,
        upper(trim(coalesce(nullif(trim(r.gestor_norm), ''), nullif(trim(r.gestores), ''), ''))) as gestor_norm_value,
        nullif(trim(r.supervisor), '') as supervisor_value,
        upper(trim(coalesce(nullif(trim(r.supervisor_norm), ''), nullif(trim(r.supervisor), ''), ''))) as supervisor_norm_value,
        nullif(trim(r.rutero), '') as rutero_value,
        upper(trim(coalesce(nullif(trim(r.rutero), ''), ''))) as rutero_norm_value,
        nullif(trim(r.reponedor), '') as reponedor_value,
        upper(trim(coalesce(nullif(trim(r.reponedor_norm), ''), nullif(trim(r.reponedor), ''), ''))) as reponedor_norm_value,
        nullif(trim(r.jefe_operaciones), '') as jefe_operaciones_value,
        upper(trim(coalesce(nullif(trim(r.jefe_operaciones), ''), ''))) as jefe_operaciones_norm_value,
        nullif(trim(r.modalidad), '') as modalidad_value,
        upper(trim(coalesce(nullif(trim(r.modalidad), ''), ''))) as modalidad_norm_value,
        coalesce(r.veces_por_semana, 0) as visitas_exigidas_semana,
        coalesce(r.lunes, 0) as lunes,
        coalesce(r.martes, 0) as martes,
        coalesce(r.miercoles, 0) as miercoles,
        coalesce(r.jueves, 0) as jueves,
        coalesce(r.viernes, 0) as viernes,
        coalesce(r.sabado, 0) as sabado,
        coalesce(r.domingo, 0) as domingo,
        r.row_hash,
        r.source_row,
        row_number() over (
            partition by r.ruta_batch_id, r.row_hash
            order by r.source_row
        ) as exact_dup_rank
    from cg_core.ruta_rutero_load_rows r
    join cg_core.v_ruta_rutero_latest_week_batch_v2 wb
      on wb.ruta_batch_id = r.ruta_batch_id
    where nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), '') is not null
      and nullif(trim(coalesce(r.cliente_norm, r.cliente)), '') is not null
), exact_deduped as (
    select *
    from source_rows
    where exact_dup_rank = 1
), scored as (
    select
        *,
        (case when modalidad_value is not null then 1 else 0 end
         + case when reponedor_value is not null then 1 else 0 end
         + case when gestor_value is not null then 1 else 0 end
         + case when supervisor_value is not null then 1 else 0 end
         + case when rutero_value is not null then 1 else 0 end) as operational_completeness,
        count(*) over (
            partition by effective_week_start, cod_rt_norm, cliente_norm
        ) as logical_rows
    from exact_deduped
), person_conflicts as (
    select
        effective_week_start,
        cod_rt_norm,
        cliente_norm,
        count(distinct (
            coalesce(reponedor_norm_value, '') || '|' ||
            coalesce(gestor_norm_value, '') || '|' ||
            coalesce(supervisor_norm_value, '') || '|' ||
            coalesce(rutero_norm_value, '')
        )) as route_person_versions
    from scored
    group by effective_week_start, cod_rt_norm, cliente_norm
), resolved as (
    select
        s.*,
        pc.route_person_versions,
        row_number() over (
            partition by s.effective_week_start, s.cod_rt_norm, s.cliente_norm
            order by
                case when s.route_week_source = 'EXPLICIT_ASSIGNMENT' then 0 else 1 end,
                s.visitas_exigidas_semana desc,
                (s.lunes + s.martes + s.miercoles + s.jueves + s.viernes + s.sabado + s.domingo) desc,
                s.operational_completeness desc,
                s.modalidad_norm_value asc,
                s.reponedor_norm_value asc,
                s.gestor_norm_value asc,
                s.supervisor_norm_value asc,
                s.rutero_norm_value asc,
                s.row_hash asc
        ) as logical_rank
    from scored s
    join person_conflicts pc
      on pc.effective_week_start = s.effective_week_start
     and pc.cod_rt_norm = s.cod_rt_norm
     and pc.cliente_norm = s.cliente_norm
)
select
    effective_week_start,
    effective_week_iso,
    route_policy_version,
    route_week_source,
    ruta_batch_id,
    cod_rt,
    cod_rt_norm,
    cod_b2b,
    local_nombre,
    direccion,
    cliente,
    cliente_norm,
    gestor_value as gestor,
    gestor_norm_value as gestor_norm,
    supervisor_value as supervisor,
    supervisor_norm_value as supervisor_norm,
    rutero_value as rutero,
    reponedor_value as reponedor_scope,
    reponedor_norm_value as reponedor_scope_norm,
    jefe_operaciones_value as jefe_operaciones,
    jefe_operaciones_norm_value as jefe_operaciones_norm,
    modalidad_value as modalidad,
    visitas_exigidas_semana::integer,
    lunes::integer,
    martes::integer,
    miercoles::integer,
    jueves::integer,
    viernes::integer,
    sabado::integer,
    domingo::integer,
    case when logical_rows > 1 then 1 else 0 end::integer as ruta_duplicada_flag,
    logical_rows::integer as ruta_duplicada_rows,
    case when route_person_versions > 1 then 1 else 0 end::integer as ruta_person_conflict_flag
from resolved
where logical_rank = 1;

-- Rollback contract. The guarded loader prepares an explicit rollback command
-- that validates the current hash, locks the weekly assignment scope, restores
-- public.ruta_rutero from the previous batch history excluding exact duplicate
-- row_hash excess, marks the failed assignment ROLLED_BACK, reactivates the
-- prior assignment, and postvalidates before commit.

rollback;
