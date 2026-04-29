-- =========================================================
-- CONTROL_GESTION V2 CALIBRATION PATCH (DRAFT, NO EJECUTAR)
-- ---------------------------------------------------------
-- Objetivo:
-- 1. Resolver duplicados de ruta versionada por cod_rt + cliente_norm
-- 2. Exponer sobrecumplimiento sin destruir el valor raw
-- 3. Separar auditoria de FUERA_CRUCE real vs SIN_BATCH_RUTA_SEMANA
-- =========================================================

create or replace view cg_mart.v_cg_ruta_duplicados_auditoria_v2 as
with normalized as (
    select
        wb.effective_week_start,
        wb.effective_week_iso,
        r.ruta_batch_id,
        nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), '') as cod_rt_norm,
        upper(trim(coalesce(nullif(trim(r.cliente_norm), ''), nullif(trim(r.cliente), ''), ''))) as cliente_norm,
        nullif(trim(r.gestores), '') as gestor_value,
        nullif(trim(r.supervisor), '') as supervisor_value,
        nullif(trim(r.reponedor), '') as reponedor_value,
        nullif(trim(r.modalidad), '') as modalidad_value,
        coalesce(r.veces_por_semana, 0) as visitas_exigidas_semana,
        coalesce(r.lunes, 0) as lunes,
        coalesce(r.martes, 0) as martes,
        coalesce(r.miercoles, 0) as miercoles,
        coalesce(r.jueves, 0) as jueves,
        coalesce(r.viernes, 0) as viernes,
        coalesce(r.sabado, 0) as sabado,
        coalesce(r.domingo, 0) as domingo,
        r.source_row
    from cg_core.ruta_rutero_load_rows r
    join cg_core.v_ruta_rutero_load_batch_week_v2 wb
      on wb.ruta_batch_id = r.ruta_batch_id
    where nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), '') is not null
      and nullif(trim(coalesce(r.cliente_norm, r.cliente)), '') is not null
)
select
    ruta_batch_id,
    effective_week_start,
    effective_week_iso,
    cod_rt_norm,
    cliente_norm,
    count(*)::integer as rows,
    string_agg(distinct gestor_value, ' | ' order by gestor_value)
        filter (where gestor_value is not null) as gestores,
    string_agg(distinct supervisor_value, ' | ' order by supervisor_value)
        filter (where supervisor_value is not null) as supervisores,
    string_agg(distinct reponedor_value, ' | ' order by reponedor_value)
        filter (where reponedor_value is not null) as reponedores,
    string_agg(distinct modalidad_value, ' | ' order by modalidad_value)
        filter (where modalidad_value is not null) as modalidades,
    max(visitas_exigidas_semana)::integer as visitas_exigidas,
    (
        max(lunes)::integer +
        max(martes)::integer +
        max(miercoles)::integer +
        max(jueves)::integer +
        max(viernes)::integer +
        max(sabado)::integer +
        max(domingo)::integer
    )::integer as dias_planificados,
    string_agg(source_row::text, ', ' order by source_row) as source_rows
from normalized
group by
    ruta_batch_id,
    effective_week_start,
    effective_week_iso,
    cod_rt_norm,
    cliente_norm
having count(*) > 1;


create or replace view cg_core.v_rr_frecuencia_base_resuelta_v2 as
with normalized as (
    select
        wb.effective_week_start,
        wb.effective_week_iso,
        r.ruta_batch_id,
        nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), '') as cod_rt,
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
        nullif(trim(r.reponedor), '') as reponedor_value,
        upper(trim(coalesce(nullif(trim(r.reponedor_norm), ''), nullif(trim(r.reponedor), ''), ''))) as reponedor_norm_value,
        nullif(trim(r.jefe_operaciones), '') as jefe_operaciones_value,
        upper(trim(coalesce(nullif(trim(r.jefe_operaciones), ''), ''))) as jefe_operaciones_norm_value,
        nullif(trim(r.modalidad), '') as modalidad_value,
        coalesce(r.veces_por_semana, 0) as visitas_exigidas_semana,
        coalesce(r.lunes, 0) as lunes,
        coalesce(r.martes, 0) as martes,
        coalesce(r.miercoles, 0) as miercoles,
        coalesce(r.jueves, 0) as jueves,
        coalesce(r.viernes, 0) as viernes,
        coalesce(r.sabado, 0) as sabado,
        coalesce(r.domingo, 0) as domingo,
        r.source_row
    from cg_core.ruta_rutero_load_rows r
    join cg_core.v_ruta_rutero_load_batch_week_v2 wb
      on wb.ruta_batch_id = r.ruta_batch_id
    where nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), '') is not null
      and nullif(trim(coalesce(r.cliente_norm, r.cliente)), '') is not null
)
select
    effective_week_start,
    effective_week_iso,
    ruta_batch_id,
    cod_rt,
    min(cod_b2b) filter (where cod_b2b is not null) as cod_b2b,
    min(local_nombre) filter (where local_nombre is not null) as local_nombre,
    min(direccion) filter (where direccion is not null) as direccion,
    min(cliente) filter (where cliente is not null) as cliente,
    cliente_norm,
    string_agg(distinct gestor_value, ' | ' order by gestor_value)
        filter (where gestor_value is not null) as gestor,
    string_agg(distinct gestor_norm_value, ' | ' order by gestor_norm_value)
        filter (where gestor_norm_value is not null) as gestor_norm,
    string_agg(distinct supervisor_value, ' | ' order by supervisor_value)
        filter (where supervisor_value is not null) as supervisor,
    string_agg(distinct supervisor_norm_value, ' | ' order by supervisor_norm_value)
        filter (where supervisor_norm_value is not null) as supervisor_norm,
    string_agg(distinct rutero_value, ' | ' order by rutero_value)
        filter (where rutero_value is not null) as rutero,
    string_agg(distinct reponedor_value, ' | ' order by reponedor_value)
        filter (where reponedor_value is not null) as reponedor_scope,
    string_agg(distinct reponedor_norm_value, ' | ' order by reponedor_norm_value)
        filter (where reponedor_norm_value is not null) as reponedor_scope_norm,
    string_agg(distinct jefe_operaciones_value, ' | ' order by jefe_operaciones_value)
        filter (where jefe_operaciones_value is not null) as jefe_operaciones,
    string_agg(distinct jefe_operaciones_norm_value, ' | ' order by jefe_operaciones_norm_value)
        filter (where jefe_operaciones_norm_value is not null) as jefe_operaciones_norm,
    string_agg(distinct modalidad_value, ' | ' order by modalidad_value)
        filter (where modalidad_value is not null) as modalidad,
    max(visitas_exigidas_semana)::integer as visitas_exigidas_semana,
    max(lunes)::integer as lunes,
    max(martes)::integer as martes,
    max(miercoles)::integer as miercoles,
    max(jueves)::integer as jueves,
    max(viernes)::integer as viernes,
    max(sabado)::integer as sabado,
    max(domingo)::integer as domingo,
    case when count(*) > 1 then 1 else 0 end::integer as ruta_duplicada_flag,
    count(*)::integer as ruta_duplicada_rows
from normalized
group by
    effective_week_start,
    effective_week_iso,
    ruta_batch_id,
    cod_rt,
    cliente_norm;


create or replace view cg_core.v_rr_frecuencia_base_v2 as
select
    r.effective_week_start,
    r.effective_week_iso,
    r.ruta_batch_id,
    r.cod_rt,
    r.cod_b2b,
    r.local_nombre,
    r.cliente,
    r.cliente_norm,
    r.visitas_exigidas_semana,
    r.lunes,
    r.martes,
    r.miercoles,
    r.jueves,
    r.viernes,
    r.sabado,
    r.domingo,
    r.gestor,
    r.supervisor,
    r.rutero,
    r.reponedor_scope,
    r.modalidad
from cg_core.v_rr_frecuencia_base_resuelta_v2 r
join cg_core.v_ruta_rutero_latest_week_batch_v2 wb
  on wb.ruta_batch_id = r.ruta_batch_id;


create or replace view cg_core.v_rr_operativa_base_v2 as
select
    effective_week_start,
    effective_week_iso,
    ruta_batch_id,
    cod_rt,
    cod_b2b,
    local_nombre,
    direccion,
    cliente,
    cliente_norm,
    gestor,
    supervisor,
    rutero,
    reponedor_scope,
    jefe_operaciones,
    modalidad
from cg_core.v_rr_frecuencia_base_resuelta_v2;


create or replace view cg_core.v_cg_evento_scope_v2 as
with evidence as (
    select *
    from cg_core.v_cg_evidencia_unificada_v2
),
weeks_with_route as (
    select distinct
        effective_week_start
    from cg_core.v_ruta_rutero_latest_week_batch_v2
),
rr as (
    select *
    from cg_core.v_rr_frecuencia_base_resuelta_v2
),
match_candidates as (
    select
        e.*,
        case when w.effective_week_start is not null then 1 else 0 end as route_week_available,
        rr.ruta_batch_id,
        rr.cod_rt as scope_cod_rt,
        rr.cod_b2b as scope_cod_b2b,
        rr.local_nombre as scope_local_nombre,
        rr.direccion as scope_direccion,
        rr.cliente as scope_cliente,
        rr.cliente_norm as scope_cliente_norm,
        rr.gestor as scope_gestor,
        rr.gestor_norm as scope_gestor_norm,
        rr.supervisor as scope_supervisor,
        rr.supervisor_norm as scope_supervisor_norm,
        rr.rutero as scope_rutero,
        rr.reponedor_scope as scope_reponedor,
        rr.reponedor_scope_norm as scope_reponedor_norm,
        rr.jefe_operaciones as scope_jefe_operaciones,
        rr.jefe_operaciones_norm as scope_jefe_operaciones_norm,
        rr.modalidad as scope_modalidad,
        rr.ruta_duplicada_flag,
        rr.ruta_duplicada_rows,
        case
            when e.cod_rt_candidate is not null
             and rr.cod_rt = e.cod_rt_candidate
             and rr.cliente_norm = e.cliente_norm then 1
            when e.cod_b2b_candidate is not null
             and rr.cod_b2b = e.cod_b2b_candidate
             and rr.cliente_norm = e.cliente_norm then 2
            else 99
        end as match_rank
    from evidence e
    left join weeks_with_route w
      on w.effective_week_start = e.report_week_start
    left join rr
      on rr.effective_week_start = e.report_week_start
     and (
            (e.cod_rt_candidate is not null and rr.cod_rt = e.cod_rt_candidate and rr.cliente_norm = e.cliente_norm)
         or (e.cod_b2b_candidate is not null and rr.cod_b2b = e.cod_b2b_candidate and rr.cliente_norm = e.cliente_norm)
     )
),
ranked as (
    select
        m.*,
        row_number() over (
            partition by m.fuente, m.raw_id
            order by m.match_rank, m.scope_cod_rt nulls last, m.scope_cod_b2b nulls last
        ) as rn,
        count(*) filter (where m.scope_cod_rt is not null or m.scope_cod_b2b is not null)
            over (partition by m.fuente, m.raw_id) as scope_match_candidates
    from match_candidates m
)
select
    r.fuente,
    r.raw_id,
    r.batch_id,
    r.ruta_batch_id,
    r.source_file,
    r.source_sheet,
    r.source_row,
    r.ingested_at,
    r.payload_json,
    coalesce(r.scope_cod_rt, r.cod_rt_candidate) as cod_rt,
    coalesce(r.scope_cod_b2b, r.cod_b2b_candidate) as cod_b2b,
    coalesce(r.scope_local_nombre, r.local_nombre_candidate, r.local_ref_raw) as local_nombre,
    coalesce(r.scope_cliente, r.cliente_candidate) as cliente,
    coalesce(r.scope_cliente_norm, r.cliente_norm) as cliente_norm,
    r.persona_raw as reponedor,
    r.persona_norm,
    r.scope_gestor as gestor,
    coalesce(r.scope_gestor_norm, upper(trim(coalesce(nullif(r.scope_gestor, ''), '')))) as gestor_norm,
    r.scope_rutero as rutero,
    r.scope_reponedor as reponedor_scope,
    coalesce(r.scope_reponedor_norm, upper(trim(coalesce(nullif(r.scope_reponedor, ''), '')))) as reponedor_scope_norm,
    r.scope_supervisor as supervisor,
    r.scope_jefe_operaciones as jefe_operaciones,
    r.scope_modalidad as modalidad,
    r.fecha_visita,
    r.report_week_start as semana_inicio,
    r.report_week_iso as semana_iso,
    r.fecha_visita_key,
    r.visita_value,
    r.has_evidence,
    r.registro_fuera_cruce,
    r.cod_rt_candidate,
    r.cod_b2b_candidate,
    r.cliente_candidate,
    r.local_ref_raw,
    r.local_nombre_candidate,
    case
        when r.scope_cod_rt is not null then 'week_cod_rt_cliente'
        when r.scope_cod_b2b is not null then 'week_cod_b2b_cliente'
        else r.source_match_hint
    end as match_quality,
    case
        when r.scope_cod_rt is not null or r.scope_cod_b2b is not null then 'MATCH_OK'
        when r.route_week_available = 0 then 'SIN_BATCH_RUTA_SEMANA'
        when r.scope_match_candidates > 1 then 'MATCH_AMBIGUO'
        when upper(trim(coalesce(r.registro_fuera_cruce, ''))) = 'N/A' then 'FUERA_SCOPE'
        else 'SIN_MATCH'
    end as match_status,
    case
        when r.scope_cod_rt is not null or r.scope_cod_b2b is not null then 'CON_EVIDENCIA'
        when r.route_week_available = 0 then 'SIN_BATCH_RUTA_SEMANA'
        when r.scope_match_candidates > 1 then 'CONFLICTO_SCOPE'
        when upper(trim(coalesce(r.registro_fuera_cruce, ''))) = 'N/A' then 'FUERA_CRUCE'
        when nullif(trim(coalesce(r.cod_rt_candidate, r.cod_b2b_candidate, '')), '') is null then 'SIN_LLAVE_LOCAL'
        when nullif(trim(coalesce(r.cliente_candidate, '')), '') is null then 'SIN_CLIENTE'
        else 'SIN_MATCH'
    end as brecha_tipo,
    case
        when r.scope_reponedor is not null
         and r.persona_norm = upper(trim(coalesce(nullif(r.scope_reponedor, ''), ''))) then 1
        else 0
    end as persona_match_exacta,
    case
        when r.scope_cod_rt is not null or r.scope_cod_b2b is not null then null
        when r.route_week_available = 0 then 'SIN_BATCH_OK_RUTA_SEMANA'
        when r.scope_match_candidates > 1 then 'SCOPE_AMBIGUO'
        when upper(trim(coalesce(r.registro_fuera_cruce, ''))) = 'N/A' then 'REGISTRO_FUERA_CRUCE'
        when nullif(trim(coalesce(r.cod_rt_candidate, r.cod_b2b_candidate, '')), '') is null then 'SIN_LLAVE_LOCAL'
        when nullif(trim(coalesce(r.cliente_candidate, '')), '') is null then 'SIN_CLIENTE'
        else 'SIN_MATCH_RUTA_VERSIONADA'
    end as motivo_no_match,
    coalesce(r.ruta_duplicada_flag, 0)::integer as ruta_duplicada_flag,
    coalesce(r.ruta_duplicada_rows, 0)::integer as ruta_duplicada_rows
from ranked r
where r.rn = 1;


create or replace view cg_mart.v_cg_fuera_cruce_auditoria_v2 as
select
    fuente,
    batch_id,
    raw_id,
    source_file,
    source_sheet,
    source_row,
    cod_rt_candidate,
    cod_b2b_candidate,
    cliente_candidate,
    reponedor as persona_raw,
    fecha_visita,
    registro_fuera_cruce,
    match_status,
    brecha_tipo,
    motivo_no_match,
    payload_json,
    case
        when brecha_tipo = 'FUERA_CRUCE' then 'FUERA_CRUCE_REAL'
        when brecha_tipo = 'SIN_BATCH_RUTA_SEMANA' then 'SIN_BATCH_RUTA_SEMANA'
        else 'OTRO'
    end as audit_categoria,
    ruta_batch_id,
    ruta_duplicada_flag,
    ruta_duplicada_rows
from cg_core.v_cg_evento_scope_v2
where match_status <> 'MATCH_OK'
   or upper(trim(coalesce(registro_fuera_cruce, ''))) = 'N/A';


create or replace view cg_mart.v_cg_fuera_cruce_real_v2 as
select *
from cg_mart.v_cg_fuera_cruce_auditoria_v2
where audit_categoria = 'FUERA_CRUCE_REAL';


create or replace view cg_mart.v_cg_sin_batch_ruta_semana_v2 as
select *
from cg_mart.v_cg_fuera_cruce_auditoria_v2
where audit_categoria = 'SIN_BATCH_RUTA_SEMANA';


create or replace view cg_mart.v_cg_conflictos_scope_v2 as
select
    fuente,
    batch_id,
    raw_id,
    source_file,
    source_sheet,
    source_row,
    cod_rt_candidate,
    cod_b2b_candidate,
    cliente_candidate,
    local_ref_raw,
    local_nombre_candidate,
    reponedor as persona_raw,
    persona_norm,
    fecha_visita,
    semana_inicio,
    semana_iso,
    registro_fuera_cruce,
    match_quality,
    match_status,
    brecha_tipo,
    motivo_no_match,
    persona_match_exacta,
    payload_json,
    ruta_batch_id,
    ruta_duplicada_flag,
    ruta_duplicada_rows
from cg_core.v_cg_evento_scope_v2
where match_status in ('MATCH_AMBIGUO', 'SIN_MATCH', 'FUERA_SCOPE', 'SIN_BATCH_RUTA_SEMANA')
   or persona_match_exacta = 0
   or ruta_duplicada_flag = 1;


create or replace view cg_mart.v_cg_out_weekly_v2 as
with base as (
    select
        f.effective_week_start,
        f.effective_week_iso,
        f.ruta_batch_id,
        f.cod_rt,
        f.cod_b2b,
        f.local_nombre,
        f.cliente,
        f.cliente_norm,
        f.gestor,
        f.supervisor,
        f.rutero,
        f.reponedor_scope,
        f.modalidad,
        f.visitas_exigidas_semana,
        f.lunes,
        f.martes,
        f.miercoles,
        f.jueves,
        f.viernes,
        f.sabado,
        f.domingo,
        f.ruta_duplicada_flag,
        f.ruta_duplicada_rows
    from cg_core.v_rr_frecuencia_base_resuelta_v2 f
),
agg as (
    select
        b.effective_week_start,
        b.effective_week_iso,
        b.ruta_batch_id,
        b.cod_rt,
        b.cod_b2b,
        b.local_nombre,
        b.cliente,
        b.cliente_norm,
        b.gestor,
        b.supervisor,
        b.rutero,
        b.reponedor_scope,
        b.modalidad,
        max(case when extract(isodow from d.fecha_visita) = 1 then d.visita_valida_dia else 0 end)::integer as lunes_flag,
        max(case when extract(isodow from d.fecha_visita) = 2 then d.visita_valida_dia else 0 end)::integer as martes_flag,
        max(case when extract(isodow from d.fecha_visita) = 3 then d.visita_valida_dia else 0 end)::integer as miercoles_flag,
        max(case when extract(isodow from d.fecha_visita) = 4 then d.visita_valida_dia else 0 end)::integer as jueves_flag,
        max(case when extract(isodow from d.fecha_visita) = 5 then d.visita_valida_dia else 0 end)::integer as viernes_flag,
        max(case when extract(isodow from d.fecha_visita) = 6 then d.visita_valida_dia else 0 end)::integer as sabado_flag,
        max(case when extract(isodow from d.fecha_visita) = 7 then d.visita_valida_dia else 0 end)::integer as domingo_flag,
        max(b.lunes)::integer as lunes_plan,
        max(b.martes)::integer as martes_plan,
        max(b.miercoles)::integer as miercoles_plan,
        max(b.jueves)::integer as jueves_plan,
        max(b.viernes)::integer as viernes_plan,
        max(b.sabado)::integer as sabado_plan,
        max(b.domingo)::integer as domingo_plan,
        max(b.visitas_exigidas_semana)::integer as visita,
        sum(coalesce(d.visita_valida_dia, 0))::integer as visita_realizada_raw,
        least(sum(coalesce(d.visita_valida_dia, 0))::integer, max(b.visitas_exigidas_semana)::integer)::integer as visita_realizada_cap,
        greatest(sum(coalesce(d.visita_valida_dia, 0))::integer - max(b.visitas_exigidas_semana)::integer, 0)::integer as sobre_cumplimiento,
        sum(coalesce(d.kpione_mark, 0))::integer as dias_kpione,
        sum(coalesce(d.kpione2_mark, 0))::integer as dias_kpione2,
        sum(coalesce(d.power_app_mark, 0))::integer as dias_power_app,
        sum(coalesce(d.doble_marcaje_dia, 0))::integer as dias_doble_marcaje,
        sum(coalesce(d.triple_marcaje_dia, 0))::integer as dias_triple_marcaje,
        sum(coalesce(d.persona_conflicto_rows_dia, 0))::integer as persona_conflicto_rows,
        max(b.ruta_duplicada_flag)::integer as ruta_duplicada_flag,
        max(b.ruta_duplicada_rows)::integer as ruta_duplicada_rows,
        concat_ws(
            ' | ',
            case when max(case when coalesce(d.kpione_mark, 0) = 1 then 1 else 0 end) = 1 then 'KPIONE' end,
            case when max(case when coalesce(d.kpione2_mark, 0) = 1 then 1 else 0 end) = 1 then 'KPIONE2' end,
            case when max(case when coalesce(d.power_app_mark, 0) = 1 then 1 else 0 end) = 1 then 'POWER_APP' end
        ) as fuentes_reportadas_semana
    from base b
    left join cg_core.v_cg_visita_dia_resuelta_v2 d
      on d.cod_rt = b.cod_rt
     and d.cliente_norm = b.cliente_norm
     and d.semana_inicio = b.effective_week_start
    group by
        b.effective_week_start,
        b.effective_week_iso,
        b.ruta_batch_id,
        b.cod_rt,
        b.cod_b2b,
        b.local_nombre,
        b.cliente,
        b.cliente_norm,
        b.gestor,
        b.supervisor,
        b.rutero,
        b.reponedor_scope,
        b.modalidad
)
select
    cod_rt as "COD_RT",
    cod_b2b as "COD_B2B",
    local_nombre as "LOCAL",
    cliente as "CLIENTE",
    gestor as "GESTOR",
    rutero as "RUTERO",
    reponedor_scope as "REPONEDOR",
    supervisor as "SUPERVISOR",
    modalidad as "MODALIDAD",
    effective_week_start as "SEMANA_INICIO",
    effective_week_iso as "SEMANA_ISO",
    lunes_flag as "LUNES_FLAG",
    martes_flag as "MARTES_FLAG",
    miercoles_flag as "MIERCOLES_FLAG",
    jueves_flag as "JUEVES_FLAG",
    viernes_flag as "VIERNES_FLAG",
    sabado_flag as "SABADO_FLAG",
    domingo_flag as "DOMINGO_FLAG",
    lunes_plan as "LUNES_PLAN",
    martes_plan as "MARTES_PLAN",
    miercoles_plan as "MIERCOLES_PLAN",
    jueves_plan as "JUEVES_PLAN",
    viernes_plan as "VIERNES_PLAN",
    sabado_plan as "SABADO_PLAN",
    domingo_plan as "DOMINGO_PLAN",
    visita as "VISITA",
    visita_realizada_raw as "VISITA_REALIZADA",
    (visita_realizada_raw - visita)::integer as "DIFERENCIA",
    case when visita_realizada_raw >= visita then 'CUMPLE' else 'INCUMPLE' end as "ALERTA",
    dias_kpione as "DIAS_KPIONE",
    dias_kpione2 as "DIAS_KPIONE2",
    dias_power_app as "DIAS_POWER_APP",
    dias_doble_marcaje as "DIAS_DOBLE_MARCAJE",
    dias_triple_marcaje as "DIAS_TRIPLE_MARCAJE",
    fuentes_reportadas_semana as "FUENTES_REPORTADAS_SEMANA",
    persona_conflicto_rows as "PERSONA_CONFLICTO_ROWS",
    visita_realizada_raw as "VISITA_REALIZADA_RAW",
    visita_realizada_cap as "VISITA_REALIZADA_CAP",
    sobre_cumplimiento as "SOBRE_CUMPLIMIENTO",
    ruta_duplicada_flag as "RUTA_DUPLICADA_FLAG",
    ruta_duplicada_rows as "RUTA_DUPLICADA_ROWS"
from agg;


create or replace view public.v_cg_cumplimiento_semana_scope_v2 as
select
    "COD_RT",
    "COD_B2B",
    "LOCAL",
    "CLIENTE",
    "GESTOR",
    "RUTERO",
    "REPONEDOR",
    "SUPERVISOR",
    "MODALIDAD",
    "SEMANA_INICIO",
    "SEMANA_ISO",
    "LUNES_FLAG",
    "MARTES_FLAG",
    "MIERCOLES_FLAG",
    "JUEVES_FLAG",
    "VIERNES_FLAG",
    "SABADO_FLAG",
    "DOMINGO_FLAG",
    "LUNES_PLAN",
    "MARTES_PLAN",
    "MIERCOLES_PLAN",
    "JUEVES_PLAN",
    "VIERNES_PLAN",
    "SABADO_PLAN",
    "DOMINGO_PLAN",
    "VISITA",
    "VISITA_REALIZADA",
    "DIFERENCIA",
    "ALERTA",
    "DIAS_KPIONE",
    "DIAS_KPIONE2",
    "DIAS_POWER_APP",
    "DIAS_DOBLE_MARCAJE",
    "DIAS_TRIPLE_MARCAJE",
    "FUENTES_REPORTADAS_SEMANA",
    "PERSONA_CONFLICTO_ROWS",
    "VISITA_REALIZADA_RAW",
    "VISITA_REALIZADA_CAP",
    "SOBRE_CUMPLIMIENTO",
    "RUTA_DUPLICADA_FLAG",
    "RUTA_DUPLICADA_ROWS"
from cg_mart.v_cg_out_weekly_v2;


create or replace view public.v_cg_cumplimiento_detalle_v2 as
select
    fuente,
    gestor,
    gestor_norm,
    cliente,
    cliente_norm,
    cod_rt,
    local_nombre,
    reponedor_scope as reponedor,
    modalidad,
    fecha_visita,
    fecha_visita_key,
    semana_inicio,
    semana_iso,
    has_evidence,
    match_status,
    brecha_tipo,
    persona_norm,
    local_nombre_candidate,
    match_quality,
    persona_match_exacta,
    batch_id,
    ruta_batch_id,
    ruta_duplicada_flag,
    ruta_duplicada_rows
from cg_core.v_cg_evento_scope_v2;


-- =========================================================
-- SMOKE QUERIES PROPUESTAS (NO EJECUTAR EN ESTE DRAFT)
-- =========================================================
-- Duplicados de ruta antes/despues:
-- with last_batch as (
--   select ruta_batch_id
--   from cg_core.ruta_rutero_load_batch
--   where status = 'ok'
--   order by loaded_at desc, ruta_batch_id desc
--   limit 1
-- )
-- select count(*) as duplicated_pairs_before
-- from (
--   select cod_rt, cliente_norm
--   from cg_core.ruta_rutero_load_rows r
--   join last_batch b using (ruta_batch_id)
--   group by cod_rt, cliente_norm
--   having count(*) > 1
-- ) d;
--
-- select count(*) as duplicated_pairs_after
-- from cg_mart.v_cg_ruta_duplicados_auditoria_v2;
--
-- Filas resueltas de frecuencia:
-- select count(*) as rows_resueltas
-- from cg_core.v_rr_frecuencia_base_resuelta_v2;
--
-- Universo semanal v2:
-- select count(*) as scope_v2_rows
-- from public.v_cg_cumplimiento_semana_scope_v2;
-- Esperado inicial cercano: 3364
--
-- Sobrecumplimiento:
-- select
--   count(*) filter (where "VISITA_REALIZADA" > "VISITA") as rows_raw_over_plan,
--   sum("SOBRE_CUMPLIMIENTO") as sobre_cumplimiento_total,
--   count(*) filter (where "SOBRE_CUMPLIMIENTO" > 0) as rows_sobre_cumplimiento
-- from public.v_cg_cumplimiento_semana_scope_v2;
--
-- Fuera de cruce real:
-- select fuente, count(*)
-- from cg_mart.v_cg_fuera_cruce_real_v2
-- group by fuente
-- order by fuente;
--
-- Sin batch de ruta por semana:
-- select fuente, count(*)
-- from cg_mart.v_cg_sin_batch_ruta_semana_v2
-- group by fuente
-- order by fuente;
--
-- Auditoria unificada por categoria:
-- select audit_categoria, fuente, count(*)
-- from cg_mart.v_cg_fuera_cruce_auditoria_v2
-- group by audit_categoria, fuente
-- order by audit_categoria, fuente;
--
-- Doble y triple marcaje:
-- select
--   count(*) filter (where doble_marcaje_dia = 1) as doble_rows,
--   count(*) filter (where triple_marcaje_dia = 1) as triple_rows
-- from cg_mart.v_cg_marcaje_multifuente_dia_v2;
--
-- Comparacion semanal:
-- select
--   "SEMANA_INICIO",
--   count(*) as rows,
--   sum("VISITA") as visita_plan,
--   sum("VISITA_REALIZADA") as visita_realizada_raw,
--   sum("VISITA_REALIZADA_CAP") as visita_realizada_cap,
--   sum("SOBRE_CUMPLIMIENTO") as sobre_cumplimiento
-- from public.v_cg_cumplimiento_semana_scope_v2
-- group by "SEMANA_INICIO"
-- order by "SEMANA_INICIO" desc;
