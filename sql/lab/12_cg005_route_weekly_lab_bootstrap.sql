-- LAB ONLY
-- Minimal local PostgreSQL bootstrap for CG005I-M route weekly replacement.
-- Not productive DDL. Do not apply to Supabase or any remote host.
-- Derived from scripts/load_ruta_rutero_from_excel.py and SQL 11 contract.

create schema if not exists cg_core;

create table if not exists public.ruta_rutero (
    cadena text not null default '',
    formato text not null default '',
    region text not null default '',
    comuna text not null default '',
    cod_rt text not null default '',
    cod_b2b text not null default '',
    local_nombre text not null default '',
    direccion text not null default '',
    veces_por_semana integer not null default 0,
    rutero text not null default '',
    jefe_operaciones text not null default '',
    gestores text not null default '',
    cliente text not null default '',
    supervisor text not null default '',
    reponedor text not null default '',
    lunes integer not null default 0,
    martes integer not null default 0,
    miercoles integer not null default 0,
    jueves integer not null default 0,
    viernes integer not null default 0,
    sabado integer not null default 0,
    domingo integer not null default 0,
    visita_mensual integer not null default 0,
    dif integer not null default 0,
    obs text not null default '',
    aux text not null default '',
    gg integer not null default 0,
    modalidad text not null default '',
    row_hash text not null,
    source text not null,
    source_row integer not null,
    cod_rt_norm text generated always as (nullif(btrim(cod_rt), '')) stored,
    cod_b2b_norm text generated always as (nullif(btrim(cod_b2b), '')) stored,
    cliente_norm text generated always as (upper(btrim(coalesce(nullif(cliente, ''), '')))) stored,
    gestor_norm text generated always as (upper(btrim(coalesce(nullif(gestores, ''), '')))) stored,
    supervisor_norm text generated always as (upper(btrim(coalesce(nullif(supervisor, ''), '')))) stored,
    reponedor_norm text generated always as (upper(btrim(coalesce(nullif(reponedor, ''), '')))) stored,
    constraint ruta_rutero_row_hash_check check (row_hash ~ '^[A-Fa-f0-9]{32}$'),
    constraint ruta_rutero_days_check check (
        lunes in (0,1) and martes in (0,1) and miercoles in (0,1) and
        jueves in (0,1) and viernes in (0,1) and sabado in (0,1) and domingo in (0,1)
    )
);

create index if not exists ix_lab_ruta_rutero_source
    on public.ruta_rutero (source);

create index if not exists ix_lab_ruta_rutero_row_hash
    on public.ruta_rutero (row_hash);

create table if not exists cg_core.ruta_rutero_load_batch (
    ruta_batch_id bigint generated always as identity primary key,
    source_file text not null,
    source_sheet text not null,
    loader_name text not null,
    loaded_rows integer not null default 0,
    status text not null,
    loaded_at timestamptz not null,
    notes text,
    constraint ruta_rutero_load_batch_status_check
        check (status in ('pending', 'failed', 'ok', 'cancelled', 'superseded'))
);

create table if not exists cg_core.ruta_rutero_load_rows (
    ruta_row_id bigint generated always as identity primary key,
    ruta_batch_id bigint not null references cg_core.ruta_rutero_load_batch(ruta_batch_id),
    source_file text not null,
    source_sheet text not null,
    source_row integer not null,
    payload_json jsonb not null,
    cadena text not null default '',
    formato text not null default '',
    region text not null default '',
    comuna text not null default '',
    cod_rt text not null default '',
    cod_b2b text not null default '',
    local_nombre text not null default '',
    direccion text not null default '',
    veces_por_semana integer not null default 0,
    rutero text not null default '',
    jefe_operaciones text not null default '',
    gestores text not null default '',
    cliente text not null default '',
    supervisor text not null default '',
    reponedor text not null default '',
    lunes integer not null default 0,
    martes integer not null default 0,
    miercoles integer not null default 0,
    jueves integer not null default 0,
    viernes integer not null default 0,
    sabado integer not null default 0,
    domingo integer not null default 0,
    visita_mensual integer not null default 0,
    dif integer not null default 0,
    obs text not null default '',
    aux text not null default '',
    gg integer not null default 0,
    modalidad text not null default '',
    row_hash text not null,
    source text not null,
    source_ingested_at timestamptz not null,
    cod_rt_norm text,
    cod_b2b_norm text,
    cliente_norm text,
    gestor_norm text,
    supervisor_norm text,
    reponedor_norm text,
    constraint ruta_rutero_load_rows_row_hash_check check (row_hash ~ '^[A-Fa-f0-9]{32}$'),
    constraint ruta_rutero_load_rows_days_check check (
        lunes in (0,1) and martes in (0,1) and miercoles in (0,1) and
        jueves in (0,1) and viernes in (0,1) and sabado in (0,1) and domingo in (0,1)
    )
);

create index if not exists ix_lab_ruta_rutero_load_rows_batch
    on cg_core.ruta_rutero_load_rows (ruta_batch_id);

create index if not exists ix_lab_ruta_rutero_load_rows_source_hash
    on cg_core.ruta_rutero_load_rows (source, row_hash);
