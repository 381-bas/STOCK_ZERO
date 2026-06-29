-- NO APPLY
-- REVIEW DDL ONLY
-- Requires separate Bastian authorization before any SQL execution.
-- Route B contract: photo_row -> event_row -> day_presence.
-- Forbidden assumption: one_excel_row_equals_one_visit.

begin;

create schema if not exists cg_raw;

create table if not exists cg_raw.kpione2_photo_raw (
    kpione2_photo_raw_id bigint generated always as identity primary key,
    source_file_name text not null,
    source_file_sha256 text not null,
    source_sheet text not null default 'Fotos',
    source_row_number bigint not null,
    event_id text not null,
    sp_item_id text,
    holding text,
    subcadena text,
    cod_rt text not null,
    cliente_norm text not null,
    local_nombre text,
    direccion text,
    reponedor text,
    fecha date not null,
    fecha_subida timestamptz,
    hora text,
    tipo_de_tarea text,
    foto_num_total text,
    n_fotos_calculado integer not null,
    comentarios text,
    link_foto text,
    event_stable_hash text not null,
    photo_row_hash text not null,
    loaded_at timestamptz not null default now(),
    constraint kpione2_photo_raw_source_sha_check
        check (source_file_sha256 ~ '^[A-Fa-f0-9]{64}$'),
    constraint kpione2_photo_raw_source_row_check
        check (source_row_number > 0),
    constraint kpione2_photo_raw_n_fotos_check
        check (n_fotos_calculado > 0),
    constraint kpione2_photo_raw_event_hash_check
        check (event_stable_hash ~ '^[A-Fa-f0-9]{64}$'),
    constraint kpione2_photo_raw_photo_hash_check
        check (photo_row_hash ~ '^[A-Fa-f0-9]{64}$'),
    constraint kpione2_photo_raw_source_row_unique
        unique (source_file_sha256, source_sheet, source_row_number)
);

create index if not exists ix_kpione2_photo_raw_event
    on cg_raw.kpione2_photo_raw (event_id, sp_item_id);

create index if not exists ix_kpione2_photo_raw_day_presence
    on cg_raw.kpione2_photo_raw (fecha, cod_rt, cliente_norm);

create index if not exists ix_kpione2_photo_raw_source_file
    on cg_raw.kpione2_photo_raw (source_file_sha256, source_sheet);

create or replace view cg_raw.v_kpione2_photo_event_contract as
select
    event_id,
    min(sp_item_id) as sp_item_id,
    min(fecha) as fecha,
    min(cod_rt) as cod_rt,
    min(cliente_norm) as cliente_norm,
    count(*)::integer as photo_rows,
    max(n_fotos_calculado)::integer as n_fotos_calculado,
    min(hora) as hora_primera_foto,
    min(event_stable_hash) as event_stable_hash
from cg_raw.kpione2_photo_raw
group by event_id;

create or replace view cg_raw.v_kpione2_photo_day_presence_contract as
select
    fecha,
    cod_rt,
    cliente_norm,
    1::integer as day_presence
from cg_raw.v_kpione2_photo_event_contract
group by fecha, cod_rt, cliente_norm;

revoke all on table cg_raw.kpione2_photo_raw from public;
revoke all on cg_raw.v_kpione2_photo_event_contract from public;
revoke all on cg_raw.v_kpione2_photo_day_presence_contract from public;

commit;
