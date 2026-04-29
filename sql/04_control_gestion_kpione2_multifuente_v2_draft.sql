-- CONTROL_GESTION KPIONE2 + MULTIFUENTE V2
-- Draft versionado. NO ejecutar sin revision previa.
--
-- Decisiones de diseno:
-- 1) KPIONE2.0 entra como fuente nueva en cg_raw.
-- 2) load_control_gestion_raw_v17.py sigue siendo el unico loader operativo.
-- 3) El cumplimiento semanal no se infla por reportes dobles o triples.
-- 4) La auditoria multifuente queda separada y filtrable.
-- 5) STOCK_ZERO sigue usando public.ruta_rutero viva.
-- 6) CONTROL_GESTION v2 usa RUTA_RUTERO versionada por carga semanal.

create schema if not exists cg_raw;
create schema if not exists cg_core;
create schema if not exists cg_mart;

-- =========================================================
-- RAW KPIONE2
-- =========================================================
create table if not exists cg_raw.kpione2_raw (
    id bigserial primary key,
    batch_id bigint not null references cg_audit.batch_registry(batch_id),
    source_file text not null,
    source_sheet text not null,
    source_row integer not null,
    ingested_at timestamptz not null default now(),
    payload_json jsonb not null,
    source_event_id text,
    sp_item_id text,
    holding_raw text,
    subcadena_raw text,
    codigo_local_raw text,
    marca_raw text,
    local_raw text,
    direccion_raw text,
    reponedor_raw text,
    fecha_raw text,
    hora_raw text,
    tipo_tarea_raw text,
    n_fotos_raw integer,
    comentarios_raw text,
    link_foto_raw text,
    visita_raw numeric,
    registro_fuera_cruce_raw text,
    semana_raw integer,
    holding_norm text,
    codigo_local_norm text,
    marca_norm text,
    reponedor_norm text,
    fecha_visita date,
    semana_iso integer,
    visita_value numeric,
    registro_fuera_cruce text,
    has_evidence boolean,
    unique (batch_id, source_row)
);

create index if not exists ix_kpione2_raw_batch_id
    on cg_raw.kpione2_raw (batch_id);

create index if not exists ix_kpione2_raw_fecha_visita
    on cg_raw.kpione2_raw (fecha_visita);

create index if not exists ix_kpione2_raw_codigo_local_norm
    on cg_raw.kpione2_raw (codigo_local_norm);

create index if not exists ix_kpione2_raw_marca_norm
    on cg_raw.kpione2_raw (marca_norm);

-- =========================================================
-- RUTA_RUTERO VERSIONADA PARA CONTROL_GESTION
-- =========================================================
-- IMPORTANTE:
-- public.ruta_rutero sigue siendo la tabla viva usada por STOCK_ZERO.
-- CONTROL_GESTION v2 NO debe depender de esa tabla viva para cumplimiento.
-- CONTROL_GESTION v2 debe depender de cargas versionadas por semana.

create table if not exists cg_core.ruta_rutero_load_batch (
    ruta_batch_id bigserial primary key,
    source_file text not null,
    source_sheet text not null default 'RUTA_RUTERO',
    loader_name text not null,
    loaded_rows integer not null default 0,
    status text not null check (status in ('ok', 'cancelled', 'superseded')),
    loaded_at timestamptz not null default now(),
    notes text
);

create index if not exists ix_ruta_rutero_load_batch_status_loaded_at
    on cg_core.ruta_rutero_load_batch (status, loaded_at desc, ruta_batch_id desc);

create table if not exists cg_core.ruta_rutero_load_rows (
    ruta_row_id bigserial primary key,
    ruta_batch_id bigint not null references cg_core.ruta_rutero_load_batch(ruta_batch_id),
    source_file text not null,
    source_sheet text not null default 'RUTA_RUTERO',
    source_row integer not null,
    ingested_at timestamptz not null default now(),
    payload_json jsonb,
    cadena text,
    formato text,
    region text,
    comuna text,
    cod_rt text,
    cod_b2b text,
    local_nombre text,
    direccion text,
    veces_por_semana integer,
    rutero text,
    jefe_operaciones text,
    gestores text,
    cliente text,
    supervisor text,
    reponedor text,
    lunes integer,
    martes integer,
    miercoles integer,
    jueves integer,
    viernes integer,
    sabado integer,
    domingo integer,
    visita_mensual integer,
    dif integer,
    obs text,
    aux text,
    gg integer,
    modalidad text,
    row_hash text,
    source text,
    source_ingested_at timestamptz,
    cod_rt_norm text,
    cod_b2b_norm text,
    cliente_norm text,
    gestor_norm text,
    supervisor_norm text,
    reponedor_norm text,
    unique (ruta_batch_id, source_row)
);

create index if not exists ix_ruta_rutero_load_rows_batch_id
    on cg_core.ruta_rutero_load_rows (ruta_batch_id);

create index if not exists ix_ruta_rutero_load_rows_batch_cod_rt_cliente
    on cg_core.ruta_rutero_load_rows (ruta_batch_id, cod_rt_norm, cliente_norm);

create index if not exists ix_ruta_rutero_load_rows_batch_cod_b2b_cliente
    on cg_core.ruta_rutero_load_rows (ruta_batch_id, cod_b2b_norm, cliente_norm);

create or replace view cg_core.v_ruta_rutero_load_batch_week_v2 as
with base as (
    select
        b.ruta_batch_id,
        b.source_file,
        b.source_sheet,
        b.loader_name,
        b.loaded_rows,
        b.status,
        b.loaded_at,
        b.notes,
        date_trunc('week', b.loaded_at at time zone 'America/Santiago')::date as effective_week_start,
        extract(week from (b.loaded_at at time zone 'America/Santiago')::date)::integer as effective_week_iso
    from cg_core.ruta_rutero_load_batch b
)
select
    ruta_batch_id,
    source_file,
    source_sheet,
    loader_name,
    loaded_rows,
    status,
    loaded_at,
    notes,
    effective_week_start,
    effective_week_iso,
    row_number() over (
        partition by effective_week_start
        order by loaded_at desc, ruta_batch_id desc
    ) as rn_week_ok
from base
where status = 'ok';

create or replace view cg_core.v_ruta_rutero_latest_week_batch_v2 as
select
    ruta_batch_id,
    source_file,
    source_sheet,
    loader_name,
    loaded_rows,
    status,
    loaded_at,
    notes,
    effective_week_start,
    effective_week_iso
from cg_core.v_ruta_rutero_load_batch_week_v2
where rn_week_ok = 1;

create or replace view cg_core.v_cg_latest_batch_by_source as
with ranked as (
    select
        source_sheet,
        case
            when source_sheet = 'DB (KPIONE)' then 'KPIONE'
            when source_sheet = 'DB (KPIONE2.0)' then 'KPIONE2'
            when source_sheet = 'DB (POWER_APP)' then 'POWER_APP'
            else source_sheet
        end as fuente,
        batch_id,
        source_file,
        loader_name,
        loaded_rows,
        started_at,
        finished_at,
        notes,
        row_number() over (
            partition by source_sheet
            order by batch_id desc
        ) as rn
    from cg_audit.batch_registry
    where status = 'ok'
      and source_sheet in ('DB (KPIONE)', 'DB (KPIONE2.0)', 'DB (POWER_APP)')
)
select
    fuente,
    source_sheet,
    batch_id,
    source_file,
    loader_name,
    loaded_rows,
    started_at,
    finished_at,
    notes
from ranked
where rn = 1;

create or replace view cg_core.v_rr_operativa_base_v2 as
select
    wb.effective_week_start,
    wb.effective_week_iso,
    r.ruta_batch_id,
    nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), '') as cod_rt,
    nullif(trim(coalesce(r.cod_b2b_norm, r.cod_b2b)), '') as cod_b2b,
    max(nullif(trim(r.local_nombre), '')) as local_nombre,
    max(nullif(trim(r.direccion), '')) as direccion,
    nullif(trim(r.cliente), '') as cliente,
    upper(trim(coalesce(nullif(trim(r.cliente_norm), ''), nullif(trim(r.cliente), ''), ''))) as cliente_norm,
    string_agg(distinct nullif(trim(r.gestores), ''), ' | ' order by nullif(trim(r.gestores), ''))
        filter (where nullif(trim(r.gestores), '') is not null) as gestor,
    string_agg(distinct nullif(trim(r.supervisor), ''), ' | ' order by nullif(trim(r.supervisor), ''))
        filter (where nullif(trim(r.supervisor), '') is not null) as supervisor,
    string_agg(distinct nullif(trim(r.rutero), ''), ' | ' order by nullif(trim(r.rutero), ''))
        filter (where nullif(trim(r.rutero), '') is not null) as rutero,
    string_agg(distinct nullif(trim(r.reponedor), ''), ' | ' order by nullif(trim(r.reponedor), ''))
        filter (where nullif(trim(r.reponedor), '') is not null) as reponedor_scope,
    string_agg(distinct nullif(trim(r.jefe_operaciones), ''), ' | ' order by nullif(trim(r.jefe_operaciones), ''))
        filter (where nullif(trim(r.jefe_operaciones), '') is not null) as jefe_operaciones,
    string_agg(distinct nullif(trim(r.modalidad), ''), ' | ' order by nullif(trim(r.modalidad), ''))
        filter (where nullif(trim(r.modalidad), '') is not null) as modalidad
from cg_core.ruta_rutero_load_rows r
join cg_core.v_ruta_rutero_latest_week_batch_v2 wb
  on wb.ruta_batch_id = r.ruta_batch_id
where nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), '') is not null
  and nullif(trim(coalesce(r.cliente_norm, r.cliente)), '') is not null
group by
    wb.effective_week_start,
    wb.effective_week_iso,
    r.ruta_batch_id,
    nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), ''),
    nullif(trim(coalesce(r.cod_b2b_norm, r.cod_b2b)), ''),
    nullif(trim(r.cliente), ''),
    upper(trim(coalesce(nullif(trim(r.cliente_norm), ''), nullif(trim(r.cliente), ''), '')));

create or replace view cg_core.v_rr_frecuencia_base_v2 as
select
    wb.effective_week_start,
    wb.effective_week_iso,
    r.ruta_batch_id,
    nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), '') as cod_rt,
    nullif(trim(coalesce(r.cod_b2b_norm, r.cod_b2b)), '') as cod_b2b,
    max(nullif(trim(r.local_nombre), '')) as local_nombre,
    nullif(trim(r.cliente), '') as cliente,
    upper(trim(coalesce(nullif(trim(r.cliente_norm), ''), nullif(trim(r.cliente), ''), ''))) as cliente_norm,
    max(coalesce(r.veces_por_semana, 0))::integer as visitas_exigidas_semana,
    max(coalesce(r.lunes, 0))::integer as lunes,
    max(coalesce(r.martes, 0))::integer as martes,
    max(coalesce(r.miercoles, 0))::integer as miercoles,
    max(coalesce(r.jueves, 0))::integer as jueves,
    max(coalesce(r.viernes, 0))::integer as viernes,
    max(coalesce(r.sabado, 0))::integer as sabado,
    max(coalesce(r.domingo, 0))::integer as domingo,
    string_agg(distinct nullif(trim(r.gestores), ''), ' | ' order by nullif(trim(r.gestores), ''))
        filter (where nullif(trim(r.gestores), '') is not null) as gestor,
    string_agg(distinct nullif(trim(r.supervisor), ''), ' | ' order by nullif(trim(r.supervisor), ''))
        filter (where nullif(trim(r.supervisor), '') is not null) as supervisor,
    string_agg(distinct nullif(trim(r.rutero), ''), ' | ' order by nullif(trim(r.rutero), ''))
        filter (where nullif(trim(r.rutero), '') is not null) as rutero,
    string_agg(distinct nullif(trim(r.reponedor), ''), ' | ' order by nullif(trim(r.reponedor), ''))
        filter (where nullif(trim(r.reponedor), '') is not null) as reponedor_scope,
    string_agg(distinct nullif(trim(r.modalidad), ''), ' | ' order by nullif(trim(r.modalidad), ''))
        filter (where nullif(trim(r.modalidad), '') is not null) as modalidad
from cg_core.ruta_rutero_load_rows r
join cg_core.v_ruta_rutero_latest_week_batch_v2 wb
  on wb.ruta_batch_id = r.ruta_batch_id
where nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), '') is not null
  and nullif(trim(coalesce(r.cliente_norm, r.cliente)), '') is not null
group by
    wb.effective_week_start,
    wb.effective_week_iso,
    r.ruta_batch_id,
    nullif(trim(coalesce(r.cod_rt_norm, r.cod_rt)), ''),
    nullif(trim(coalesce(r.cod_b2b_norm, r.cod_b2b)), ''),
    nullif(trim(r.cliente), ''),
    upper(trim(coalesce(nullif(trim(r.cliente_norm), ''), nullif(trim(r.cliente), ''), '')));

create or replace view cg_core.v_cg_evidencia_unificada_v2 as
with latest as (
    select fuente, source_sheet, batch_id
    from cg_core.v_cg_latest_batch_by_source
),
kpione as (
    select
        'KPIONE'::text as fuente,
        r.id as raw_id,
        r.batch_id,
        r.source_file,
        r.source_sheet,
        r.source_row,
        r.ingested_at,
        r.payload_json,
        nullif(trim(coalesce(r.payload_json->>'holding', '')), '') as holding_raw,
        nullif(trim(coalesce(r.payload_json->>'subcadena', '')), '') as subcadena_raw,
        nullif(trim(coalesce(r.payload_json->>'COD_RT', '')), '') as cod_rt_candidate,
        nullif(trim(coalesce(r.payload_json->>'codlocal', '')), '') as cod_b2b_candidate,
        nullif(trim(coalesce(r.payload_json->>'marca', '')), '') as cliente_candidate,
        upper(trim(coalesce(nullif(r.payload_json->>'marca', ''), ''))) as cliente_norm,
        nullif(trim(coalesce(r.payload_json->>'trabajador', '')), '') as persona_raw,
        upper(trim(coalesce(nullif(r.payload_json->>'trabajador', ''), ''))) as persona_norm,
        nullif(trim(coalesce(r.payload_json->>'nombre_local', '')), '') as local_ref_raw,
        nullif(trim(coalesce(r.payload_json->>'nombre_local', '')), '') as local_nombre_candidate,
        coalesce(
            case
                when coalesce(r.payload_json->>'FECHA', '') ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                    then to_date(r.payload_json->>'FECHA', 'DD-MM-YYYY')
            end,
            case
                when coalesce(r.payload_json->>'Fecha_reg', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                    then left(r.payload_json->>'Fecha_reg', 10)::date
            end
        ) as fecha_visita,
        coalesce(
            case
                when coalesce(r.payload_json->>'SEMANA', '') ~ '^[0-9]+$'
                    then (r.payload_json->>'SEMANA')::integer
            end,
            case
                when coalesce(r.payload_json->>'FECHA', '') ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                    then extract(week from to_date(r.payload_json->>'FECHA', 'DD-MM-YYYY'))::integer
            end
        ) as semana_iso,
        case
            when coalesce(r.payload_json->>'VISITA', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                then (r.payload_json->>'VISITA')::numeric
            else null
        end as visita_value,
        case
            when nullif(trim(coalesce(r.payload_json->>'foto_inicial', '')), '') is not null then true
            when nullif(trim(coalesce(r.payload_json->>'foto_final', '')), '') is not null then true
            when nullif(trim(coalesce(r.payload_json->>'foto_bodega', '')), '') is not null then true
            when nullif(trim(coalesce(r.payload_json->>'foto_panoramico', '')), '') is not null then true
            when nullif(trim(coalesce(r.evidencia_raw, '')), '') is not null then true
            else false
        end as has_evidence,
        coalesce(nullif(trim(coalesce(r.payload_json->>'REGISTRO_FUERA_CRUCE', '')), ''), 'N/A') as registro_fuera_cruce,
        case
            when nullif(trim(coalesce(r.payload_json->>'COD_RT', '')), '') is not null then 'payload_cod_rt'
            when nullif(trim(coalesce(r.payload_json->>'codlocal', '')), '') is not null then 'payload_cod_b2b'
            else 'sin_llave'
        end as source_match_hint
    from cg_raw.kpione_raw r
    join latest l
      on l.fuente = 'KPIONE'
     and l.batch_id = r.batch_id
),
kpione2 as (
    select
        'KPIONE2'::text as fuente,
        r.id as raw_id,
        r.batch_id,
        r.source_file,
        r.source_sheet,
        r.source_row,
        r.ingested_at,
        r.payload_json,
        nullif(trim(r.holding_raw), '') as holding_raw,
        nullif(trim(r.subcadena_raw), '') as subcadena_raw,
        nullif(trim(coalesce(r.codigo_local_norm, r.codigo_local_raw)), '') as cod_rt_candidate,
        null::text as cod_b2b_candidate,
        nullif(trim(r.marca_raw), '') as cliente_candidate,
        upper(trim(coalesce(nullif(r.marca_norm, ''), nullif(r.marca_raw, ''), ''))) as cliente_norm,
        nullif(trim(r.reponedor_raw), '') as persona_raw,
        upper(trim(coalesce(nullif(r.reponedor_norm, ''), nullif(r.reponedor_raw, ''), ''))) as persona_norm,
        nullif(trim(r.local_raw), '') as local_ref_raw,
        nullif(trim(r.local_raw), '') as local_nombre_candidate,
        coalesce(
            r.fecha_visita,
            case
                when coalesce(r.fecha_raw, '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                    then r.fecha_raw::date
            end
        ) as fecha_visita,
        coalesce(
            r.semana_iso,
            r.semana_raw,
            case
                when coalesce(r.fecha_raw, '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                    then extract(week from r.fecha_raw::date)::integer
            end
        ) as semana_iso,
        coalesce(
            r.visita_value,
            r.visita_raw,
            case
                when coalesce(r.payload_json->>'VISITA', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    then (r.payload_json->>'VISITA')::numeric
            end
        ) as visita_value,
        coalesce(
            r.has_evidence,
            case
                when coalesce(r.n_fotos_raw, 0) > 0 then true
                when nullif(trim(coalesce(r.link_foto_raw, '')), '') is not null then true
                else false
            end
        ) as has_evidence,
        coalesce(nullif(trim(coalesce(r.registro_fuera_cruce, r.registro_fuera_cruce_raw, '')), ''), 'N/A') as registro_fuera_cruce,
        case
            when nullif(trim(coalesce(r.codigo_local_norm, r.codigo_local_raw, '')), '') is not null then 'payload_cod_rt'
            else 'sin_llave'
        end as source_match_hint
    from cg_raw.kpione2_raw r
    join latest l
      on l.fuente = 'KPIONE2'
     and l.batch_id = r.batch_id
),
power_app as (
    select
        'POWER_APP'::text as fuente,
        r.id as raw_id,
        r.batch_id,
        r.source_file,
        r.source_sheet,
        r.source_row,
        r.ingested_at,
        r.payload_json,
        null::text as holding_raw,
        null::text as subcadena_raw,
        nullif(trim(coalesce(r.title_local_raw, r.payload_json->>'Local: Title', '')), '') as cod_rt_candidate,
        null::text as cod_b2b_candidate,
        nullif(trim(coalesce(r.titulo_marca_raw, r.payload_json->>'Marca: Título', r.payload_json->>'Marca: TÃ­tulo', '')), '') as cliente_candidate,
        upper(trim(coalesce(nullif(coalesce(r.titulo_marca_raw, r.payload_json->>'Marca: Título', r.payload_json->>'Marca: TÃ­tulo', ''), ''), ''))) as cliente_norm,
        nullif(trim(coalesce(r.persona_raw, r.payload_json->>'Creado por', '')), '') as persona_raw,
        upper(trim(coalesce(nullif(coalesce(r.persona_raw, r.payload_json->>'Creado por', ''), ''), ''))) as persona_norm,
        nullif(trim(coalesce(r.payload_json->>'Local: LOCAL', r.title_local_raw, '')), '') as local_ref_raw,
        nullif(trim(coalesce(r.payload_json->>'Local: LOCAL', r.title_local_raw, '')), '') as local_nombre_candidate,
        coalesce(
            case
                when coalesce(r.payload_json->>'FECHA', '') ~ '^[0-9]{2}-[0-9]{2}-[0-9]{2}$'
                    then to_date(r.payload_json->>'FECHA', 'DD-MM-YY')
            end,
            case
                when coalesce(r.payload_json->>'Creado', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                    then left(r.payload_json->>'Creado', 10)::date
            end
        ) as fecha_visita,
        coalesce(
            case
                when coalesce(r.payload_json->>'SEM', '') ~ '^[0-9]+$'
                    then (r.payload_json->>'SEM')::integer
            end,
            case
                when coalesce(r.payload_json->>'FECHA', '') ~ '^[0-9]{2}-[0-9]{2}-[0-9]{2}$'
                    then extract(week from to_date(r.payload_json->>'FECHA', 'DD-MM-YY'))::integer
            end
        ) as semana_iso,
        case
            when coalesce(r.payload_json->>'CONTAR', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                then (r.payload_json->>'CONTAR')::numeric
            else 1::numeric
        end as visita_value,
        case
            when coalesce(r.payload_json->>'CONTAR', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                then (r.payload_json->>'CONTAR')::numeric > 0
            else true
        end as has_evidence,
        coalesce(nullif(trim(coalesce(r.payload_json->>'REGISTRO_FUERA_CRUCE', '')), ''), 'N/A') as registro_fuera_cruce,
        case
            when nullif(trim(coalesce(r.title_local_raw, r.payload_json->>'Local: Title', '')), '') is not null then 'title_local_cod_rt'
            else 'sin_llave'
        end as source_match_hint
    from cg_raw.power_app_raw r
    join latest l
      on l.fuente = 'POWER_APP'
     and l.batch_id = r.batch_id
)
select
    fuente,
    raw_id,
    batch_id,
    source_file,
    source_sheet,
    source_row,
    ingested_at,
    payload_json,
    holding_raw,
    subcadena_raw,
    cod_rt_candidate,
    cod_b2b_candidate,
    cliente_candidate,
    cliente_norm,
    persona_raw,
    persona_norm,
    local_ref_raw,
    local_nombre_candidate,
    fecha_visita,
    case when fecha_visita is not null then date_trunc('week', fecha_visita)::date end as report_week_start,
    case when fecha_visita is not null then extract(week from fecha_visita)::integer end as report_week_iso,
    case when fecha_visita is not null then to_char(fecha_visita, 'YYYY-MM-DD') else null end as fecha_visita_key,
    semana_iso,
    visita_value,
    has_evidence,
    registro_fuera_cruce,
    source_match_hint
from kpione
union all
select
    fuente,
    raw_id,
    batch_id,
    source_file,
    source_sheet,
    source_row,
    ingested_at,
    payload_json,
    holding_raw,
    subcadena_raw,
    cod_rt_candidate,
    cod_b2b_candidate,
    cliente_candidate,
    cliente_norm,
    persona_raw,
    persona_norm,
    local_ref_raw,
    local_nombre_candidate,
    fecha_visita,
    case when fecha_visita is not null then date_trunc('week', fecha_visita)::date end,
    case when fecha_visita is not null then extract(week from fecha_visita)::integer end,
    case when fecha_visita is not null then to_char(fecha_visita, 'YYYY-MM-DD') else null end,
    semana_iso,
    visita_value,
    has_evidence,
    registro_fuera_cruce,
    source_match_hint
from kpione2
union all
select
    fuente,
    raw_id,
    batch_id,
    source_file,
    source_sheet,
    source_row,
    ingested_at,
    payload_json,
    holding_raw,
    subcadena_raw,
    cod_rt_candidate,
    cod_b2b_candidate,
    cliente_candidate,
    cliente_norm,
    persona_raw,
    persona_norm,
    local_ref_raw,
    local_nombre_candidate,
    fecha_visita,
    case when fecha_visita is not null then date_trunc('week', fecha_visita)::date end,
    case when fecha_visita is not null then extract(week from fecha_visita)::integer end,
    case when fecha_visita is not null then to_char(fecha_visita, 'YYYY-MM-DD') else null end,
    semana_iso,
    visita_value,
    has_evidence,
    registro_fuera_cruce,
    source_match_hint
from power_app;

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
    from cg_core.v_rr_operativa_base_v2
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
        rr.supervisor as scope_supervisor,
        rr.rutero as scope_rutero,
        rr.reponedor_scope as scope_reponedor,
        rr.jefe_operaciones as scope_jefe_operaciones,
        rr.modalidad as scope_modalidad,
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
    upper(trim(coalesce(nullif(r.scope_gestor, ''), ''))) as gestor_norm,
    r.scope_rutero as rutero,
    r.scope_reponedor as reponedor_scope,
    upper(trim(coalesce(nullif(r.scope_reponedor, ''), ''))) as reponedor_scope_norm,
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
    end as motivo_no_match
from ranked r
where r.rn = 1;

create or replace view cg_core.v_cg_visita_dia_resuelta_v2 as
with daily as (
    select
        cod_rt,
        cod_b2b,
        cliente,
        cliente_norm,
        local_nombre,
        gestor,
        gestor_norm,
        rutero,
        reponedor_scope,
        reponedor_scope_norm,
        supervisor,
        jefe_operaciones,
        modalidad,
        fecha_visita,
        semana_inicio,
        semana_iso,
        max(case when fuente = 'KPIONE' then 1 else 0 end)::integer as kpione_mark,
        max(case when fuente = 'KPIONE2' then 1 else 0 end)::integer as kpione2_mark,
        max(case when fuente = 'POWER_APP' then 1 else 0 end)::integer as power_app_mark,
        count(*) filter (where fuente = 'KPIONE')::integer as kpione_rows_dia,
        count(*) filter (where fuente = 'KPIONE2')::integer as kpione2_rows_dia,
        count(*) filter (where fuente = 'POWER_APP')::integer as power_app_rows_dia,
        sum(case when persona_match_exacta = 0 and nullif(trim(coalesce(persona_norm, '')), '') is not null then 1 else 0 end)::integer as persona_conflicto_rows_dia
    from cg_core.v_cg_evento_scope_v2
    where fecha_visita is not null
      and match_status = 'MATCH_OK'
    group by
        cod_rt, cod_b2b, cliente, cliente_norm, local_nombre, gestor, gestor_norm, rutero,
        reponedor_scope, reponedor_scope_norm, supervisor, jefe_operaciones, modalidad,
        fecha_visita, semana_inicio, semana_iso
)
select
    d.cod_rt,
    d.cod_b2b,
    d.cliente,
    d.cliente_norm,
    d.local_nombre,
    d.gestor,
    d.gestor_norm,
    d.rutero,
    d.reponedor_scope,
    d.reponedor_scope_norm,
    d.supervisor,
    d.jefe_operaciones,
    d.modalidad,
    d.fecha_visita,
    d.semana_inicio,
    d.semana_iso,
    1::integer as visita_valida_dia,
    d.kpione_mark,
    d.kpione2_mark,
    d.power_app_mark,
    (d.kpione_mark + d.kpione2_mark + d.power_app_mark)::integer as fuentes_reportadas_count,
    concat_ws(
        ' | ',
        case when d.kpione_mark = 1 then 'KPIONE' end,
        case when d.kpione2_mark = 1 then 'KPIONE2' end,
        case when d.power_app_mark = 1 then 'POWER_APP' end
    ) as fuentes_reportadas_label,
    case when (d.kpione_mark + d.kpione2_mark + d.power_app_mark) = 2 then 1 else 0 end::integer as doble_marcaje_dia,
    case when (d.kpione_mark + d.kpione2_mark + d.power_app_mark) = 3 then 1 else 0 end::integer as triple_marcaje_dia,
    d.kpione_rows_dia,
    d.kpione2_rows_dia,
    d.power_app_rows_dia,
    d.persona_conflicto_rows_dia
from daily d;

create or replace view cg_mart.v_cg_marcaje_multifuente_dia_v2 as
select
    cod_rt,
    cod_b2b,
    cliente,
    cliente_norm,
    local_nombre as local,
    gestor,
    rutero,
    reponedor_scope as reponedor,
    supervisor,
    modalidad,
    fecha_visita,
    semana_inicio,
    semana_iso,
    kpione_mark,
    kpione2_mark,
    power_app_mark,
    fuentes_reportadas_count,
    fuentes_reportadas_label,
    doble_marcaje_dia,
    triple_marcaje_dia,
    kpione_rows_dia,
    kpione2_rows_dia,
    power_app_rows_dia,
    persona_conflicto_rows_dia
from cg_core.v_cg_visita_dia_resuelta_v2
where fuentes_reportadas_count >= 2;

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
    payload_json
from cg_core.v_cg_evento_scope_v2
where match_status <> 'MATCH_OK'
   or upper(trim(coalesce(registro_fuera_cruce, ''))) = 'N/A';

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
    payload_json
from cg_core.v_cg_evento_scope_v2
where match_status in ('MATCH_AMBIGUO', 'SIN_MATCH', 'FUERA_SCOPE', 'SIN_BATCH_RUTA_SEMANA')
   or persona_match_exacta = 0;

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
        f.domingo
    from cg_core.v_rr_frecuencia_base_v2 f
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
        sum(coalesce(d.visita_valida_dia, 0))::integer as visita_realizada,
        sum(coalesce(d.kpione_mark, 0))::integer as dias_kpione,
        sum(coalesce(d.kpione2_mark, 0))::integer as dias_kpione2,
        sum(coalesce(d.power_app_mark, 0))::integer as dias_power_app,
        sum(coalesce(d.doble_marcaje_dia, 0))::integer as dias_doble_marcaje,
        sum(coalesce(d.triple_marcaje_dia, 0))::integer as dias_triple_marcaje,
        sum(coalesce(d.persona_conflicto_rows_dia, 0))::integer as persona_conflicto_rows,
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
    visita_realizada as "VISITA_REALIZADA",
    (visita_realizada - visita)::integer as "DIFERENCIA",
    case when (visita_realizada - visita) < 0 then 'INCUMPLE' else 'CUMPLE' end as "ALERTA",
    dias_kpione as "DIAS_KPIONE",
    dias_kpione2 as "DIAS_KPIONE2",
    dias_power_app as "DIAS_POWER_APP",
    dias_doble_marcaje as "DIAS_DOBLE_MARCAJE",
    dias_triple_marcaje as "DIAS_TRIPLE_MARCAJE",
    fuentes_reportadas_semana as "FUENTES_REPORTADAS_SEMANA",
    persona_conflicto_rows as "PERSONA_CONFLICTO_ROWS"
from agg;

create or replace view public.v_cg_cumplimiento_semana_scope_v2 as
select *
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
    ruta_batch_id
from cg_core.v_cg_evento_scope_v2;

-- =========================================================
-- SMOKE QUERIES PROPUESTAS (NO EJECUTAR EN ESTE DRAFT)
-- =========================================================
-- Objetos v2 creados:
-- select table_schema, table_name
-- from information_schema.tables
-- where (table_schema, table_name) in (
--   ('cg_raw', 'kpione2_raw'),
--   ('cg_core', 'ruta_rutero_load_batch'),
--   ('cg_core', 'ruta_rutero_load_rows')
-- )
-- order by table_schema, table_name;
--
-- select table_schema, table_name
-- from information_schema.views
-- where (table_schema, table_name) in (
--   ('cg_core', 'v_ruta_rutero_load_batch_week_v2'),
--   ('cg_core', 'v_ruta_rutero_latest_week_batch_v2'),
--   ('cg_core', 'v_cg_evidencia_unificada_v2'),
--   ('cg_core', 'v_cg_evento_scope_v2'),
--   ('cg_core', 'v_cg_visita_dia_resuelta_v2'),
--   ('cg_mart', 'v_cg_out_weekly_v2'),
--   ('cg_mart', 'v_cg_fuera_cruce_auditoria_v2'),
--   ('cg_mart', 'v_cg_marcaje_multifuente_dia_v2'),
--   ('cg_mart', 'v_cg_conflictos_scope_v2'),
--   ('public', 'v_cg_cumplimiento_semana_scope_v2'),
--   ('public', 'v_cg_cumplimiento_detalle_v2')
-- )
-- order by table_schema, table_name;
--
-- KPIONE2 ultimo batch:
-- select l.batch_id, count(*)
-- from cg_raw.kpione2_raw r
-- join cg_core.v_cg_latest_batch_by_source l
--   on l.fuente = 'KPIONE2'
--  and l.batch_id = r.batch_id
-- group by l.batch_id;
-- Esperado: ~8414
--
-- Fuera de cruce KPIONE2:
-- select registro_fuera_cruce, count(*)
-- from cg_raw.kpione2_raw r
-- join cg_core.v_cg_latest_batch_by_source l
--   on l.fuente = 'KPIONE2'
--  and l.batch_id = r.batch_id
-- group by registro_fuera_cruce
-- order by count(*) desc;
-- Esperado: ~282 fuera de cruce
--
-- Ruta versionada: ultimo batch OK por semana:
-- select effective_week_start, effective_week_iso, ruta_batch_id, loaded_rows, loaded_at
-- from cg_core.v_ruta_rutero_latest_week_batch_v2
-- order by effective_week_start desc;
--
-- Semanas con evidencia sin batch de ruta:
-- select e.report_week_start, e.report_week_iso, count(*) as evidencias
-- from cg_core.v_cg_evidencia_unificada_v2 e
-- left join cg_core.v_ruta_rutero_latest_week_batch_v2 b
--   on b.effective_week_start = e.report_week_start
-- where e.report_week_start is not null
--   and b.ruta_batch_id is null
-- group by e.report_week_start, e.report_week_iso
-- order by e.report_week_start desc;
--
-- Duplicados cod_rt + cliente por batch de ruta:
-- select
--   ruta_batch_id,
--   nullif(trim(coalesce(cod_rt_norm, cod_rt)), '') as cod_rt_key,
--   upper(trim(coalesce(nullif(cliente_norm, ''), nullif(cliente, ''), ''))) as cliente_norm_key,
--   count(*) as rows_per_key
-- from cg_core.ruta_rutero_load_rows
-- where nullif(trim(coalesce(cod_rt_norm, cod_rt)), '') is not null
--   and nullif(trim(coalesce(cliente_norm, cliente)), '') is not null
-- group by
--   ruta_batch_id,
--   nullif(trim(coalesce(cod_rt_norm, cod_rt)), ''),
--   upper(trim(coalesce(nullif(cliente_norm, ''), nullif(cliente, ''), '')))
-- having count(*) > 1
-- order by ruta_batch_id desc, rows_per_key desc;
--
-- Duplicados diarios resueltos:
-- select cod_rt, cliente_norm, fecha_visita, count(*)
-- from cg_core.v_cg_visita_dia_resuelta_v2
-- group by cod_rt, cliente_norm, fecha_visita
-- having count(*) > 1;
--
-- Doble y triple marcaje:
-- select
--   sum(doble_marcaje_dia) as dias_doble_marcaje,
--   sum(triple_marcaje_dia) as dias_triple_marcaje
-- from cg_core.v_cg_visita_dia_resuelta_v2;
--
-- Fuera de cruce por fuente:
-- select
--   fuente,
--   match_status,
--   brecha_tipo,
--   motivo_no_match,
--   count(*)
-- from cg_mart.v_cg_fuera_cruce_auditoria_v2
-- group by fuente, match_status, brecha_tipo, motivo_no_match
-- order by fuente, count(*) desc;
--
-- Resumen semanal v2:
-- select
--   "SEMANA_ISO",
--   count(*) as filas,
--   sum("VISITA") as visita_plan,
--   sum("VISITA_REALIZADA") as visita_realizada,
--   sum("DIFERENCIA") as diferencia,
--   sum("DIAS_KPIONE") as dias_kpione,
--   sum("DIAS_KPIONE2") as dias_kpione2,
--   sum("DIAS_POWER_APP") as dias_power_app,
--   sum("DIAS_DOBLE_MARCAJE") as dias_doble_marcaje,
--   sum("DIAS_TRIPLE_MARCAJE") as dias_triple_marcaje
-- from public.v_cg_cumplimiento_semana_scope_v2
-- group by "SEMANA_ISO"
-- order by "SEMANA_ISO" desc;
