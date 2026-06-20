-- NO APPLY
-- ROLLBACK DDL CONTRACT ONLY
-- REQUIRES SEPARATE BASTIAN AUTHORIZATION
-- CG005N-Q package correction: signature-preserving rollback
-- Derived exclusively from research/CG005N_PRESTATE_CATALOG.json
-- Rollback is logical signature-preserving: compatibility columns remain present.

begin;

set local search_path = cg_core, cg_mart, public, pg_catalog;

-- Rollback must be pre-data-apply. Productive assignments or route batch drift block rollback.
do $$
declare
    current_route_batch_count bigint;
    current_max_ruta_batch_id bigint;
begin
    if to_regclass('cg_core.ruta_rutero_week_assignment') is not null then
        if exists (select 1 from cg_core.ruta_rutero_week_assignment limit 1) then
            raise exception 'cg005n_rollback_blocked_assignment_rows_present';
        end if;
    end if;

    select count(*)::bigint, max(ruta_batch_id)::bigint
      into current_route_batch_count, current_max_ruta_batch_id
      from cg_core.ruta_rutero_load_batch;

    if current_route_batch_count is distinct from 19
       or current_max_ruta_batch_id is distinct from 19 then
        raise exception 'cg005n_rollback_blocked_route_batch_baseline_changed';
    end if;
end $$;

-- Restore pre-CG005N view definitions by replacement. Dependency order is resolved -> latest -> week.

create or replace view "cg_core"."v_rr_frecuencia_base_resuelta_v2" as
WITH normalized AS (
         SELECT wb.effective_week_start,
            wb.effective_week_iso,
            r.ruta_batch_id,
            wb.status,
            wb.loaded_at,
            NULLIF(TRIM(BOTH FROM COALESCE(r.cod_rt_norm, r.cod_rt)), ''::text) AS cod_rt,
            NULLIF(TRIM(BOTH FROM COALESCE(r.cod_b2b_norm, r.cod_b2b)), ''::text) AS cod_b2b,
            NULLIF(TRIM(BOTH FROM r.local_nombre), ''::text) AS local_nombre,
            NULLIF(TRIM(BOTH FROM r.direccion), ''::text) AS direccion,
            NULLIF(TRIM(BOTH FROM r.cliente), ''::text) AS cliente,
            upper(TRIM(BOTH FROM COALESCE(NULLIF(TRIM(BOTH FROM r.cliente_norm), ''::text), NULLIF(TRIM(BOTH FROM r.cliente), ''::text), ''::text))) AS cliente_norm,
            NULLIF(TRIM(BOTH FROM r.gestores), ''::text) AS gestor_value,
            upper(TRIM(BOTH FROM COALESCE(NULLIF(TRIM(BOTH FROM r.gestor_norm), ''::text), NULLIF(TRIM(BOTH FROM r.gestores), ''::text), ''::text))) AS gestor_norm_value,
            NULLIF(TRIM(BOTH FROM r.supervisor), ''::text) AS supervisor_value,
            upper(TRIM(BOTH FROM COALESCE(NULLIF(TRIM(BOTH FROM r.supervisor_norm), ''::text), NULLIF(TRIM(BOTH FROM r.supervisor), ''::text), ''::text))) AS supervisor_norm_value,
            NULLIF(TRIM(BOTH FROM r.rutero), ''::text) AS rutero_value,
            NULLIF(TRIM(BOTH FROM r.reponedor), ''::text) AS reponedor_value,
            upper(TRIM(BOTH FROM COALESCE(NULLIF(TRIM(BOTH FROM r.reponedor_norm), ''::text), NULLIF(TRIM(BOTH FROM r.reponedor), ''::text), ''::text))) AS reponedor_norm_value,
            NULLIF(TRIM(BOTH FROM r.jefe_operaciones), ''::text) AS jefe_operaciones_value,
            upper(TRIM(BOTH FROM COALESCE(NULLIF(TRIM(BOTH FROM r.jefe_operaciones), ''::text), ''::text))) AS jefe_operaciones_norm_value,
            NULLIF(TRIM(BOTH FROM r.modalidad), ''::text) AS modalidad_value,
            COALESCE(r.veces_por_semana, 0) AS visitas_exigidas_semana,
            COALESCE(r.lunes, 0) AS lunes,
            COALESCE(r.martes, 0) AS martes,
            COALESCE(r.miercoles, 0) AS miercoles,
            COALESCE(r.jueves, 0) AS jueves,
            COALESCE(r.viernes, 0) AS viernes,
            COALESCE(r.sabado, 0) AS sabado,
            COALESCE(r.domingo, 0) AS domingo,
            r.source_row
           FROM ruta_rutero_load_rows r
             JOIN v_ruta_rutero_load_batch_week_v2 wb ON wb.ruta_batch_id = r.ruta_batch_id
          WHERE NULLIF(TRIM(BOTH FROM COALESCE(r.cod_rt_norm, r.cod_rt)), ''::text) IS NOT NULL AND NULLIF(TRIM(BOTH FROM COALESCE(r.cliente_norm, r.cliente)), ''::text) IS NOT NULL
        ), winning_batch_by_grain AS (
         SELECT ranked.effective_week_start,
            ranked.cod_rt,
            ranked.cliente_norm,
            ranked.ruta_batch_id
           FROM ( SELECT normalized.effective_week_start,
                    normalized.cod_rt,
                    normalized.cliente_norm,
                    normalized.ruta_batch_id,
                    row_number() OVER (PARTITION BY normalized.effective_week_start, normalized.cod_rt, normalized.cliente_norm ORDER BY (
                        CASE
                            WHEN normalized.status = 'ok'::text THEN 0
                            ELSE 1
                        END), normalized.loaded_at DESC NULLS LAST, normalized.ruta_batch_id DESC) AS rn
                   FROM normalized
                  GROUP BY normalized.effective_week_start, normalized.cod_rt, normalized.cliente_norm, normalized.ruta_batch_id, normalized.status, normalized.loaded_at) ranked
          WHERE ranked.rn = 1
        ), normalized_dedup AS (
         SELECT n.effective_week_start,
            n.effective_week_iso,
            n.ruta_batch_id,
            n.status,
            n.loaded_at,
            n.cod_rt,
            n.cod_b2b,
            n.local_nombre,
            n.direccion,
            n.cliente,
            n.cliente_norm,
            n.gestor_value,
            n.gestor_norm_value,
            n.supervisor_value,
            n.supervisor_norm_value,
            n.rutero_value,
            n.reponedor_value,
            n.reponedor_norm_value,
            n.jefe_operaciones_value,
            n.jefe_operaciones_norm_value,
            n.modalidad_value,
            n.visitas_exigidas_semana,
            n.lunes,
            n.martes,
            n.miercoles,
            n.jueves,
            n.viernes,
            n.sabado,
            n.domingo,
            n.source_row
           FROM normalized n
             JOIN winning_batch_by_grain w ON w.effective_week_start = n.effective_week_start AND w.cod_rt = n.cod_rt AND w.cliente_norm = n.cliente_norm AND w.ruta_batch_id = n.ruta_batch_id
        )
 SELECT effective_week_start,
    effective_week_iso,
    ruta_batch_id,
    cod_rt,
    min(cod_b2b) FILTER (WHERE cod_b2b IS NOT NULL) AS cod_b2b,
    min(local_nombre) FILTER (WHERE local_nombre IS NOT NULL) AS local_nombre,
    min(direccion) FILTER (WHERE direccion IS NOT NULL) AS direccion,
    min(cliente) FILTER (WHERE cliente IS NOT NULL) AS cliente,
    cliente_norm,
    string_agg(DISTINCT gestor_value, ' | '::text ORDER BY gestor_value) FILTER (WHERE gestor_value IS NOT NULL) AS gestor,
    string_agg(DISTINCT gestor_norm_value, ' | '::text ORDER BY gestor_norm_value) FILTER (WHERE gestor_norm_value IS NOT NULL) AS gestor_norm,
    string_agg(DISTINCT supervisor_value, ' | '::text ORDER BY supervisor_value) FILTER (WHERE supervisor_value IS NOT NULL) AS supervisor,
    string_agg(DISTINCT supervisor_norm_value, ' | '::text ORDER BY supervisor_norm_value) FILTER (WHERE supervisor_norm_value IS NOT NULL) AS supervisor_norm,
    string_agg(DISTINCT rutero_value, ' | '::text ORDER BY rutero_value) FILTER (WHERE rutero_value IS NOT NULL) AS rutero,
    string_agg(DISTINCT reponedor_value, ' | '::text ORDER BY reponedor_value) FILTER (WHERE reponedor_value IS NOT NULL) AS reponedor_scope,
    string_agg(DISTINCT reponedor_norm_value, ' | '::text ORDER BY reponedor_norm_value) FILTER (WHERE reponedor_norm_value IS NOT NULL) AS reponedor_scope_norm,
    string_agg(DISTINCT jefe_operaciones_value, ' | '::text ORDER BY jefe_operaciones_value) FILTER (WHERE jefe_operaciones_value IS NOT NULL) AS jefe_operaciones,
    string_agg(DISTINCT jefe_operaciones_norm_value, ' | '::text ORDER BY jefe_operaciones_norm_value) FILTER (WHERE jefe_operaciones_norm_value IS NOT NULL) AS jefe_operaciones_norm,
    string_agg(DISTINCT modalidad_value, ' | '::text ORDER BY modalidad_value) FILTER (WHERE modalidad_value IS NOT NULL) AS modalidad,
    max(visitas_exigidas_semana) AS visitas_exigidas_semana,
    max(lunes) AS lunes,
    max(martes) AS martes,
    max(miercoles) AS miercoles,
    max(jueves) AS jueves,
    max(viernes) AS viernes,
    max(sabado) AS sabado,
    max(domingo) AS domingo,
        CASE
            WHEN count(*) > 1 THEN 1
            ELSE 0
        END AS ruta_duplicada_flag,
    count(*)::integer AS ruta_duplicada_rows,
    'LEGACY_LOADED_AT_INFERENCE'::text AS route_policy_version,
    'LEGACY_INFERRED'::text AS route_week_source,
    cod_rt::text AS cod_rt_norm,
    NULL::integer AS ruta_person_conflict_flag
   FROM normalized_dedup
  GROUP BY effective_week_start, effective_week_iso, ruta_batch_id, cod_rt, cliente_norm;
alter view "cg_core"."v_rr_frecuencia_base_resuelta_v2" owner to "postgres";
comment on view "cg_core"."v_rr_frecuencia_base_resuelta_v2" is null;
revoke all on "cg_core"."v_rr_frecuencia_base_resuelta_v2" from public;
revoke all on "cg_core"."v_rr_frecuencia_base_resuelta_v2" from "stock_zero_readonly";
grant select on "cg_core"."v_rr_frecuencia_base_resuelta_v2" to "stock_zero_readonly";

create or replace view "cg_core"."v_ruta_rutero_latest_week_batch_v2" as
SELECT ruta_batch_id,
    source_file,
    source_sheet,
    loader_name,
    loaded_rows,
    status,
    loaded_at,
    notes,
    effective_week_start,
    effective_week_iso,
    route_policy_version,
    route_week_source,
    assignment_id
   FROM v_ruta_rutero_load_batch_week_v2
  WHERE rn_week_ok = 1;
alter view "cg_core"."v_ruta_rutero_latest_week_batch_v2" owner to "postgres";
comment on view "cg_core"."v_ruta_rutero_latest_week_batch_v2" is null;
revoke all on "cg_core"."v_ruta_rutero_latest_week_batch_v2" from public;
revoke all on "cg_core"."v_ruta_rutero_latest_week_batch_v2" from "stock_zero_readonly";
grant select on "cg_core"."v_ruta_rutero_latest_week_batch_v2" to "stock_zero_readonly";

create or replace view "cg_core"."v_ruta_rutero_load_batch_week_v2" as
WITH base AS (
         SELECT b.ruta_batch_id,
            b.source_file,
            b.source_sheet,
            b.loader_name,
            b.loaded_rows,
            b.status,
            b.loaded_at,
            b.notes,
            date_trunc('week'::text, (b.loaded_at AT TIME ZONE 'America/Santiago'::text))::date AS effective_week_start,
            EXTRACT(week FROM (b.loaded_at AT TIME ZONE 'America/Santiago'::text)::date)::integer AS effective_week_iso
           FROM ruta_rutero_load_batch b
        )
 SELECT ruta_batch_id,
    source_file,
    source_sheet,
    loader_name,
    loaded_rows,
    status,
    loaded_at,
    notes,
    effective_week_start,
    effective_week_iso,
    row_number() OVER (PARTITION BY effective_week_start ORDER BY loaded_at DESC, ruta_batch_id DESC) AS rn_week_ok,
    'LEGACY_LOADED_AT_INFERENCE'::text AS route_policy_version,
    'LEGACY_INFERRED'::text AS route_week_source,
    NULL::bigint AS assignment_id,
    loaded_at::timestamptz AS assigned_at
   FROM base
  WHERE status = 'ok'::text;
alter view "cg_core"."v_ruta_rutero_load_batch_week_v2" owner to "postgres";
comment on view "cg_core"."v_ruta_rutero_load_batch_week_v2" is null;
revoke all on "cg_core"."v_ruta_rutero_load_batch_week_v2" from public;
revoke all on "cg_core"."v_ruta_rutero_load_batch_week_v2" from "stock_zero_readonly";
grant select on "cg_core"."v_ruta_rutero_load_batch_week_v2" to "stock_zero_readonly";

-- Restore original route batch status constraint using a validated temporary constraint.
alter table cg_core.ruta_rutero_load_batch
  drop constraint if exists ruta_rutero_load_batch_status_check_cg005n_rollback;

alter table cg_core.ruta_rutero_load_batch
  add constraint ruta_rutero_load_batch_status_check_cg005n_rollback
  CHECK (status = ANY (ARRAY['ok'::text, 'cancelled'::text, 'superseded'::text]))
  not valid;

alter table cg_core.ruta_rutero_load_batch
  validate constraint ruta_rutero_load_batch_status_check_cg005n_rollback;

alter table cg_core.ruta_rutero_load_batch
  drop constraint if exists ruta_rutero_load_batch_status_check;

alter table cg_core.ruta_rutero_load_batch
  rename constraint ruta_rutero_load_batch_status_check_cg005n_rollback
  to ruta_rutero_load_batch_status_check;
alter table "cg_core"."ruta_rutero_load_batch" owner to "postgres";
comment on table "cg_core"."ruta_rutero_load_batch" is null;
revoke all on "cg_core"."ruta_rutero_load_batch" from public;
revoke all on "cg_core"."ruta_rutero_load_batch" from "stock_zero_readonly";
grant select on "cg_core"."ruta_rutero_load_batch" to "stock_zero_readonly";

-- Restore absence of cg_core.ruta_rutero_week_assignment from prestate.
drop table if exists cg_core.ruta_rutero_week_assignment;

rollback;
