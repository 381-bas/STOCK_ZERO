-- =========================================================
-- CONTROL_GESTION V2 DAILY INTERMEDIATE MART DRAFT
-- FASE_9C5M_C_CONTROL_GESTION_DAILY_FACT_SQL_DRAFT_HARDENING
-- =========================================================
-- Manual draft only. Do not execute from Codex.
-- No app switch in this phase.
-- No business-rule changes:
--   KPIONE2 first, POWER_APP fallback, KPIONE1 audit-only.
--
-- Purpose:
--   Materialize daily resolved evidence by affected dates before the weekly
--   aggregation, so incremental refresh does not rebuild from
--   cg_mart.v_cg_out_weekly_v2.
--
-- Current runtime remains:
--   CG_V2_OUT_WEEKLY_VIEW=cg_mart.mv_cg_out_weekly_v2

-- =========================================================
-- 1) DAILY PHYSICAL TABLE
-- =========================================================
-- Grain candidate:
--   fecha_visita + cod_rt + cliente_norm
--
-- Keep the unique constraint optional until shared-management and cod_b2b
-- edge cases are validated on production data.
CREATE TABLE IF NOT EXISTS cg_mart.fact_cg_visita_dia_resuelta_v2 (
    semana_inicio date NOT NULL,
    fecha_visita date NOT NULL,
    cod_rt text NOT NULL,
    cod_b2b text,
    cliente text,
    cliente_norm text NOT NULL,
    local_nombre text,
    gestor text,
    gestor_norm text,
    rutero text,
    reponedor_scope text,
    reponedor_scope_norm text,
    supervisor text,
    jefe_operaciones text,
    modalidad text,
    semana_iso integer,
    fuente_ganadora text,
    fuentes_presentes text,
    tiene_kpione2 integer,
    tiene_power_app integer,
    tiene_kpione1 integer,
    power_app_fallback integer,
    kpione1_audit_only integer,
    useful_day integer,
    raw_evidence_count integer,
    same_source_multimark integer,
    multisource_overlap integer,
    kpione_rows_dia integer,
    kpione2_rows_dia integer,
    power_app_rows_dia integer,
    persona_conflicto_rows_dia integer,
    match_quality text,
    registro_fuera_cruce text,
    mart_loaded_at timestamptz NOT NULL DEFAULT now()
);

-- =========================================================
-- 2) DAILY INDEXES
-- =========================================================
CREATE INDEX IF NOT EXISTS ix_fact_cg_visita_dia_semana
ON cg_mart.fact_cg_visita_dia_resuelta_v2 (semana_inicio);

CREATE INDEX IF NOT EXISTS ix_fact_cg_visita_dia_fecha
ON cg_mart.fact_cg_visita_dia_resuelta_v2 (fecha_visita);

CREATE INDEX IF NOT EXISTS ix_fact_cg_visita_dia_semana_cod_cliente
ON cg_mart.fact_cg_visita_dia_resuelta_v2 (semana_inicio, cod_rt, cliente_norm);

CREATE INDEX IF NOT EXISTS ix_fact_cg_visita_dia_cod_cliente_fecha
ON cg_mart.fact_cg_visita_dia_resuelta_v2 (cod_rt, cliente_norm, fecha_visita);

CREATE INDEX IF NOT EXISTS ix_fact_cg_visita_dia_semana_fuente
ON cg_mart.fact_cg_visita_dia_resuelta_v2 (semana_inicio, fuente_ganadora);

-- Optional post-validation only. Do not execute until the grain is proven
-- safe for shared-management and cod_b2b fallback cases.
-- CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_cg_visita_dia_grain
-- ON cg_mart.fact_cg_visita_dia_resuelta_v2 (fecha_visita, cod_rt, cliente_norm);

-- =========================================================
-- 3) DAILY INCREMENTAL REBUILD FOR AFFECTED DATES
-- =========================================================
-- Manual affected dates example:
--   ARRAY[DATE '2026-05-13']::date[]
--
-- Future script placeholder example:
--   :affected_dates::date[]
--
-- Operational rule:
--   Delete and rebuild affected dates only in the daily mart.
--   Do not delete whole weeks in this layer.
--
-- The stage starts from cg_core.v_cg_evidencia_unificada_v2 filtered by
-- affected_dates. It intentionally does not use cg_mart.v_cg_out_weekly_v2.
BEGIN;

CREATE TEMP TABLE _cg_affected_dates (
    fecha_visita date PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _cg_affected_dates (fecha_visita)
SELECT DISTINCT fecha_visita
FROM unnest(ARRAY[DATE '2026-05-13']::date[]) AS dates(fecha_visita)
WHERE fecha_visita IS NOT NULL;

-- Placeholder-driven alternative for a future script:
-- INSERT INTO _cg_affected_dates (fecha_visita)
-- SELECT DISTINCT fecha_visita
-- FROM unnest(:affected_dates::date[]) AS dates(fecha_visita)
-- WHERE fecha_visita IS NOT NULL;

CREATE TEMP TABLE _cg_daily_stage
ON COMMIT DROP AS
WITH affected_weeks AS (
    SELECT DISTINCT
        date_trunc('week', ad.fecha_visita)::date AS semana_inicio
    FROM _cg_affected_dates ad
),
evidence AS MATERIALIZED (
    SELECT
        e.fuente,
        e.raw_id,
        e.batch_id,
        e.source_file,
        e.source_sheet,
        e.source_row,
        e.ingested_at,
        e.payload_json,
        e.holding_raw,
        e.subcadena_raw,
        e.cod_rt_candidate,
        e.cod_b2b_candidate,
        e.cliente_candidate,
        e.cliente_norm,
        e.persona_raw,
        e.persona_norm,
        e.local_ref_raw,
        e.local_nombre_candidate,
        e.fecha_visita,
        e.report_week_start,
        e.report_week_iso,
        e.fecha_visita_key,
        e.semana_iso,
        e.visita_value,
        e.has_evidence,
        e.registro_fuera_cruce,
        e.source_match_hint
    FROM cg_core.v_cg_evidencia_unificada_v2 e
    JOIN _cg_affected_dates ad
      ON ad.fecha_visita = e.fecha_visita
    WHERE e.fuente IN ('KPIONE', 'KPIONE2', 'POWER_APP')
),
weeks_with_route AS MATERIALIZED (
    SELECT DISTINCT
        w.effective_week_start
    FROM cg_core.v_ruta_rutero_latest_week_batch_v2 w
    JOIN affected_weeks aw
      ON aw.semana_inicio = w.effective_week_start
),
rr AS MATERIALIZED (
    SELECT
        r.effective_week_start,
        r.effective_week_iso,
        r.ruta_batch_id,
        r.cod_rt,
        r.cod_b2b,
        r.local_nombre,
        r.cliente,
        r.cliente_norm,
        r.gestor,
        r.gestor_norm,
        r.supervisor,
        r.rutero,
        r.reponedor_scope,
        r.reponedor_scope_norm,
        r.jefe_operaciones,
        r.modalidad,
        r.ruta_duplicada_flag,
        r.ruta_duplicada_rows
    FROM cg_core.v_rr_frecuencia_base_resuelta_v2 r
    JOIN affected_weeks aw
      ON aw.semana_inicio = r.effective_week_start
),
match_candidates AS MATERIALIZED (
    SELECT
        e.fuente,
        e.raw_id,
        e.batch_id,
        e.source_file,
        e.source_sheet,
        e.source_row,
        e.ingested_at,
        e.payload_json,
        e.holding_raw,
        e.subcadena_raw,
        e.cod_rt_candidate,
        e.cod_b2b_candidate,
        e.cliente_candidate,
        e.cliente_norm,
        e.persona_raw,
        e.persona_norm,
        e.local_ref_raw,
        e.local_nombre_candidate,
        e.fecha_visita,
        e.report_week_start,
        e.report_week_iso,
        e.fecha_visita_key,
        e.semana_iso,
        e.visita_value,
        e.has_evidence,
        e.registro_fuera_cruce,
        e.source_match_hint,
        CASE WHEN w.effective_week_start IS NOT NULL THEN 1 ELSE 0 END AS route_week_available,
        rr.ruta_batch_id,
        rr.cod_rt AS scope_cod_rt,
        rr.cod_b2b AS scope_cod_b2b,
        rr.local_nombre AS scope_local_nombre,
        rr.cliente AS scope_cliente,
        rr.cliente_norm AS scope_cliente_norm,
        rr.gestor AS scope_gestor,
        rr.gestor_norm AS scope_gestor_norm,
        rr.rutero AS scope_rutero,
        rr.reponedor_scope AS scope_reponedor,
        rr.reponedor_scope_norm AS scope_reponedor_norm,
        rr.supervisor AS scope_supervisor,
        rr.jefe_operaciones AS scope_jefe_operaciones,
        rr.modalidad AS scope_modalidad,
        rr.ruta_duplicada_flag AS scope_ruta_duplicada_flag,
        rr.ruta_duplicada_rows AS scope_ruta_duplicada_rows,
        CASE
            WHEN e.cod_rt_candidate IS NOT NULL
             AND rr.cod_rt = e.cod_rt_candidate
             AND rr.cliente_norm = e.cliente_norm THEN 1
            WHEN e.cod_b2b_candidate IS NOT NULL
             AND rr.cod_b2b = e.cod_b2b_candidate
             AND rr.cliente_norm = e.cliente_norm THEN 2
            ELSE 99
        END AS match_rank
    FROM evidence e
    LEFT JOIN weeks_with_route w
      ON w.effective_week_start = e.report_week_start
    LEFT JOIN rr
      ON rr.effective_week_start = e.report_week_start
     AND (
            (
                e.cod_rt_candidate IS NOT NULL
                AND rr.cod_rt = e.cod_rt_candidate
                AND rr.cliente_norm = e.cliente_norm
            )
         OR (
                e.cod_b2b_candidate IS NOT NULL
                AND rr.cod_b2b = e.cod_b2b_candidate
                AND rr.cliente_norm = e.cliente_norm
            )
     )
),
ranked AS MATERIALIZED (
    SELECT
        m.fuente,
        m.raw_id,
        m.batch_id,
        m.source_file,
        m.source_sheet,
        m.source_row,
        m.ingested_at,
        m.payload_json,
        m.holding_raw,
        m.subcadena_raw,
        m.cod_rt_candidate,
        m.cod_b2b_candidate,
        m.cliente_candidate,
        m.cliente_norm,
        m.persona_raw,
        m.persona_norm,
        m.local_ref_raw,
        m.local_nombre_candidate,
        m.fecha_visita,
        m.report_week_start,
        m.report_week_iso,
        m.fecha_visita_key,
        m.semana_iso,
        m.visita_value,
        m.has_evidence,
        m.registro_fuera_cruce,
        m.source_match_hint,
        m.route_week_available,
        m.ruta_batch_id,
        m.scope_cod_rt,
        m.scope_cod_b2b,
        m.scope_local_nombre,
        m.scope_cliente,
        m.scope_cliente_norm,
        m.scope_gestor,
        m.scope_gestor_norm,
        m.scope_rutero,
        m.scope_reponedor,
        m.scope_reponedor_norm,
        m.scope_supervisor,
        m.scope_jefe_operaciones,
        m.scope_modalidad,
        m.scope_ruta_duplicada_flag,
        m.scope_ruta_duplicada_rows,
        m.match_rank,
        row_number() OVER (
            PARTITION BY m.fuente, m.raw_id
            ORDER BY m.match_rank, m.scope_cod_rt NULLS LAST, m.scope_cod_b2b NULLS LAST
        ) AS rn,
        count(*) FILTER (
            WHERE m.scope_cod_rt IS NOT NULL OR m.scope_cod_b2b IS NOT NULL
        ) OVER (PARTITION BY m.fuente, m.raw_id) AS scope_match_candidates
    FROM match_candidates m
),
event_scope AS MATERIALIZED (
    SELECT
        r.fuente,
        r.raw_id,
        r.batch_id,
        r.ruta_batch_id,
        r.source_file,
        r.source_sheet,
        r.source_row,
        r.ingested_at,
        r.payload_json,
        COALESCE(r.scope_cod_rt, r.cod_rt_candidate) AS cod_rt,
        COALESCE(r.scope_cod_b2b, r.cod_b2b_candidate) AS cod_b2b,
        COALESCE(r.scope_local_nombre, r.local_nombre_candidate, r.local_ref_raw) AS local_nombre,
        COALESCE(r.scope_cliente, r.cliente_candidate) AS cliente,
        COALESCE(r.scope_cliente_norm, r.cliente_norm) AS cliente_norm,
        r.persona_raw AS reponedor,
        r.persona_norm,
        r.scope_gestor AS gestor,
        COALESCE(
            r.scope_gestor_norm,
            UPPER(TRIM(COALESCE(NULLIF(r.scope_gestor, ''), '')))
        ) AS gestor_norm,
        r.scope_rutero AS rutero,
        r.scope_reponedor AS reponedor_scope,
        COALESCE(
            r.scope_reponedor_norm,
            UPPER(TRIM(COALESCE(NULLIF(r.scope_reponedor, ''), '')))
        ) AS reponedor_scope_norm,
        r.scope_supervisor AS supervisor,
        r.scope_jefe_operaciones AS jefe_operaciones,
        r.scope_modalidad AS modalidad,
        r.fecha_visita,
        r.report_week_start AS semana_inicio,
        r.report_week_iso AS semana_iso,
        r.fecha_visita_key,
        r.visita_value,
        r.has_evidence,
        r.registro_fuera_cruce,
        r.cod_rt_candidate,
        r.cod_b2b_candidate,
        r.cliente_candidate,
        r.local_ref_raw,
        r.local_nombre_candidate,
        COALESCE(r.scope_ruta_duplicada_flag, 0)::integer AS ruta_duplicada_flag,
        COALESCE(r.scope_ruta_duplicada_rows, 0)::integer AS ruta_duplicada_rows,
        CASE
            WHEN r.scope_cod_rt IS NOT NULL THEN 'week_cod_rt_cliente'
            WHEN r.scope_cod_b2b IS NOT NULL THEN 'week_cod_b2b_cliente'
            ELSE r.source_match_hint
        END AS match_quality,
        CASE
            WHEN r.scope_cod_rt IS NOT NULL OR r.scope_cod_b2b IS NOT NULL THEN 'MATCH_OK'
            WHEN r.route_week_available = 0 THEN 'SIN_BATCH_RUTA_SEMANA'
            WHEN r.scope_match_candidates > 1 THEN 'MATCH_AMBIGUO'
            WHEN UPPER(TRIM(COALESCE(r.registro_fuera_cruce, ''))) = 'N/A' THEN 'FUERA_SCOPE'
            ELSE 'SIN_MATCH'
        END AS match_status,
        CASE
            WHEN r.scope_reponedor IS NOT NULL
             AND r.persona_norm = UPPER(TRIM(COALESCE(NULLIF(r.scope_reponedor, ''), ''))) THEN 1
            ELSE 0
        END::integer AS persona_match_exacta
    FROM ranked r
    WHERE r.rn = 1
),
daily AS MATERIALIZED (
    SELECT
        es.cod_rt,
        es.cod_b2b,
        es.cliente,
        es.cliente_norm,
        es.local_nombre,
        es.gestor,
        es.gestor_norm,
        es.rutero,
        es.reponedor_scope,
        es.reponedor_scope_norm,
        es.supervisor,
        es.jefe_operaciones,
        es.modalidad,
        es.fecha_visita,
        es.semana_inicio,
        es.semana_iso,
        max(CASE WHEN es.fuente = 'KPIONE' THEN 1 ELSE 0 END)::integer AS kpione_mark,
        max(CASE WHEN es.fuente = 'KPIONE2' THEN 1 ELSE 0 END)::integer AS kpione2_mark,
        max(CASE WHEN es.fuente = 'POWER_APP' THEN 1 ELSE 0 END)::integer AS power_app_mark,
        count(*) FILTER (WHERE es.fuente = 'KPIONE')::integer AS kpione_rows_dia,
        count(*) FILTER (WHERE es.fuente = 'KPIONE2')::integer AS kpione2_rows_dia,
        count(*) FILTER (WHERE es.fuente = 'POWER_APP')::integer AS power_app_rows_dia,
        sum(
            CASE
                WHEN es.persona_match_exacta = 0
                 AND NULLIF(TRIM(COALESCE(es.persona_norm, '')), '') IS NOT NULL THEN 1
                ELSE 0
            END
        )::integer AS persona_conflicto_rows_dia,
        min(es.match_quality) AS match_quality,
        min(es.registro_fuera_cruce) AS registro_fuera_cruce
    FROM event_scope es
    WHERE es.fecha_visita IS NOT NULL
      AND es.match_status = 'MATCH_OK'
    GROUP BY
        es.cod_rt,
        es.cod_b2b,
        es.cliente,
        es.cliente_norm,
        es.local_nombre,
        es.gestor,
        es.gestor_norm,
        es.rutero,
        es.reponedor_scope,
        es.reponedor_scope_norm,
        es.supervisor,
        es.jefe_operaciones,
        es.modalidad,
        es.fecha_visita,
        es.semana_inicio,
        es.semana_iso
),
precedence AS MATERIALIZED (
    SELECT
        d.semana_inicio,
        d.fecha_visita,
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
        d.semana_iso,
        CASE
            WHEN d.kpione2_mark = 1 THEN 'KPIONE2'
            WHEN d.kpione2_mark = 0 AND d.power_app_mark = 1 THEN 'POWER_APP'
            ELSE NULL
        END AS fuente_ganadora,
        concat_ws(
            ' | ',
            CASE WHEN d.kpione_mark = 1 THEN 'KPIONE' END,
            CASE WHEN d.kpione2_mark = 1 THEN 'KPIONE2' END,
            CASE WHEN d.power_app_mark = 1 THEN 'POWER_APP' END
        ) AS fuentes_presentes,
        d.kpione2_mark AS tiene_kpione2,
        d.power_app_mark AS tiene_power_app,
        d.kpione_mark AS tiene_kpione1,
        CASE
            WHEN d.kpione2_mark = 0 AND d.power_app_mark = 1 THEN 1
            ELSE 0
        END::integer AS power_app_fallback,
        CASE WHEN d.kpione_mark = 1 THEN 1 ELSE 0 END::integer AS kpione1_audit_only,
        CASE
            WHEN d.kpione2_mark = 1 THEN 1
            WHEN d.kpione2_mark = 0 AND d.power_app_mark = 1 THEN 1
            ELSE 0
        END::integer AS useful_day,
        (d.kpione_rows_dia + d.kpione2_rows_dia + d.power_app_rows_dia)::integer AS raw_evidence_count,
        CASE
            WHEN d.kpione_rows_dia > 1 OR d.kpione2_rows_dia > 1 OR d.power_app_rows_dia > 1 THEN 1
            ELSE 0
        END::integer AS same_source_multimark,
        CASE
            WHEN (d.kpione_mark + d.kpione2_mark + d.power_app_mark) > 1 THEN 1
            ELSE 0
        END::integer AS multisource_overlap,
        d.kpione_rows_dia,
        d.kpione2_rows_dia,
        d.power_app_rows_dia,
        d.persona_conflicto_rows_dia,
        d.match_quality,
        d.registro_fuera_cruce
    FROM daily d
)
SELECT
    p.semana_inicio,
    p.fecha_visita,
    p.cod_rt,
    p.cod_b2b,
    p.cliente,
    p.cliente_norm,
    p.local_nombre,
    p.gestor,
    p.gestor_norm,
    p.rutero,
    p.reponedor_scope,
    p.reponedor_scope_norm,
    p.supervisor,
    p.jefe_operaciones,
    p.modalidad,
    p.semana_iso,
    p.fuente_ganadora,
    p.fuentes_presentes,
    p.tiene_kpione2,
    p.tiene_power_app,
    p.tiene_kpione1,
    p.power_app_fallback,
    p.kpione1_audit_only,
    p.useful_day,
    p.raw_evidence_count,
    p.same_source_multimark,
    p.multisource_overlap,
    p.kpione_rows_dia,
    p.kpione2_rows_dia,
    p.power_app_rows_dia,
    p.persona_conflicto_rows_dia,
    p.match_quality,
    p.registro_fuera_cruce,
    now()::timestamptz AS mart_loaded_at
FROM precedence p;

-- Required stage precheck. If stage_rows = 0, stop and ROLLBACK; do not
-- replace affected dates with an empty result.
SELECT
    COUNT(*)::bigint AS stage_rows,
    COUNT(DISTINCT fecha_visita)::bigint AS stage_dates,
    MIN(fecha_visita) AS min_stage_date,
    MAX(fecha_visita) AS max_stage_date,
    COALESCE(SUM(useful_day), 0)::bigint AS useful_days,
    COALESCE(SUM(raw_evidence_count), 0)::bigint AS raw_evidence_count
FROM _cg_daily_stage;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM _cg_daily_stage LIMIT 1) THEN
        RAISE EXCEPTION 'BLOCK_EMPTY_DAILY_STAGE: no source rows for affected dates';
    END IF;
END $$;

-- Optional pre-replace validation. All diff columns should be 0 before the
-- DELETE/INSERT is allowed to proceed.
WITH src AS (
    SELECT
        v.fecha_visita,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(v.useful_day), 0)::bigint AS useful_day,
        COALESCE(SUM(v.tiene_kpione2), 0)::bigint AS tiene_kpione2,
        COALESCE(SUM(v.tiene_power_app), 0)::bigint AS tiene_power_app,
        COALESCE(SUM(v.kpione1_audit_only), 0)::bigint AS kpione1_audit_only,
        COALESCE(SUM(v.power_app_fallback), 0)::bigint AS power_app_fallback,
        COALESCE(SUM(v.raw_evidence_count), 0)::bigint AS raw_evidence_count,
        COUNT(*) FILTER (WHERE v.fuente_ganadora = 'KPIONE2')::bigint AS kpione2_winner_rows,
        COUNT(*) FILTER (WHERE v.fuente_ganadora = 'POWER_APP')::bigint AS power_app_winner_rows,
        COUNT(*) FILTER (WHERE v.fuente_ganadora IS NULL)::bigint AS no_winner_rows
    FROM cg_core.v_cg_visita_dia_precedencia_v2 v
    JOIN _cg_affected_dates ad
      ON ad.fecha_visita = v.fecha_visita
    GROUP BY v.fecha_visita
),
stage AS (
    SELECT
        s.fecha_visita,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(s.useful_day), 0)::bigint AS useful_day,
        COALESCE(SUM(s.tiene_kpione2), 0)::bigint AS tiene_kpione2,
        COALESCE(SUM(s.tiene_power_app), 0)::bigint AS tiene_power_app,
        COALESCE(SUM(s.kpione1_audit_only), 0)::bigint AS kpione1_audit_only,
        COALESCE(SUM(s.power_app_fallback), 0)::bigint AS power_app_fallback,
        COALESCE(SUM(s.raw_evidence_count), 0)::bigint AS raw_evidence_count,
        COUNT(*) FILTER (WHERE s.fuente_ganadora = 'KPIONE2')::bigint AS kpione2_winner_rows,
        COUNT(*) FILTER (WHERE s.fuente_ganadora = 'POWER_APP')::bigint AS power_app_winner_rows,
        COUNT(*) FILTER (WHERE s.fuente_ganadora IS NULL)::bigint AS no_winner_rows
    FROM _cg_daily_stage s
    GROUP BY s.fecha_visita
)
SELECT
    COALESCE(stage.fecha_visita, src.fecha_visita) AS fecha_visita,
    COALESCE(stage.rows, 0) - COALESCE(src.rows, 0) AS rows_diff,
    COALESCE(stage.useful_day, 0) - COALESCE(src.useful_day, 0) AS useful_day_diff,
    COALESCE(stage.tiene_kpione2, 0) - COALESCE(src.tiene_kpione2, 0) AS tiene_kpione2_diff,
    COALESCE(stage.tiene_power_app, 0) - COALESCE(src.tiene_power_app, 0) AS tiene_power_app_diff,
    COALESCE(stage.kpione1_audit_only, 0) - COALESCE(src.kpione1_audit_only, 0) AS kpione1_audit_only_diff,
    COALESCE(stage.power_app_fallback, 0) - COALESCE(src.power_app_fallback, 0) AS power_app_fallback_diff,
    COALESCE(stage.raw_evidence_count, 0) - COALESCE(src.raw_evidence_count, 0) AS raw_evidence_count_diff,
    COALESCE(stage.kpione2_winner_rows, 0) - COALESCE(src.kpione2_winner_rows, 0) AS kpione2_winner_rows_diff,
    COALESCE(stage.power_app_winner_rows, 0) - COALESCE(src.power_app_winner_rows, 0) AS power_app_winner_rows_diff,
    COALESCE(stage.no_winner_rows, 0) - COALESCE(src.no_winner_rows, 0) AS no_winner_rows_diff
FROM stage
FULL JOIN src USING (fecha_visita)
ORDER BY fecha_visita;

DELETE FROM cg_mart.fact_cg_visita_dia_resuelta_v2 f
USING _cg_affected_dates ad
WHERE f.fecha_visita = ad.fecha_visita;

INSERT INTO cg_mart.fact_cg_visita_dia_resuelta_v2 (
    semana_inicio,
    fecha_visita,
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
    semana_iso,
    fuente_ganadora,
    fuentes_presentes,
    tiene_kpione2,
    tiene_power_app,
    tiene_kpione1,
    power_app_fallback,
    kpione1_audit_only,
    useful_day,
    raw_evidence_count,
    same_source_multimark,
    multisource_overlap,
    kpione_rows_dia,
    kpione2_rows_dia,
    power_app_rows_dia,
    persona_conflicto_rows_dia,
    match_quality,
    registro_fuera_cruce,
    mart_loaded_at
)
SELECT
    s.semana_inicio,
    s.fecha_visita,
    s.cod_rt,
    s.cod_b2b,
    s.cliente,
    s.cliente_norm,
    s.local_nombre,
    s.gestor,
    s.gestor_norm,
    s.rutero,
    s.reponedor_scope,
    s.reponedor_scope_norm,
    s.supervisor,
    s.jefe_operaciones,
    s.modalidad,
    s.semana_iso,
    s.fuente_ganadora,
    s.fuentes_presentes,
    s.tiene_kpione2,
    s.tiene_power_app,
    s.tiene_kpione1,
    s.power_app_fallback,
    s.kpione1_audit_only,
    s.useful_day,
    s.raw_evidence_count,
    s.same_source_multimark,
    s.multisource_overlap,
    s.kpione_rows_dia,
    s.kpione2_rows_dia,
    s.power_app_rows_dia,
    s.persona_conflicto_rows_dia,
    s.match_quality,
    s.registro_fuera_cruce,
    s.mart_loaded_at
FROM _cg_daily_stage s;

COMMIT;

ANALYZE cg_mart.fact_cg_visita_dia_resuelta_v2;

-- =========================================================
-- 4) DAILY FACT VALIDATION AFTER APPLY
-- =========================================================
-- Manual validation example. Replace the ARRAY before running.
-- Future script placeholder:
--   SELECT unnest(:affected_dates::date[]) AS fecha_visita
WITH affected_dates AS (
    SELECT DISTINCT fecha_visita
    FROM unnest(ARRAY[DATE '2026-05-13']::date[]) AS dates(fecha_visita)
    WHERE fecha_visita IS NOT NULL
),
src AS (
    SELECT
        v.fecha_visita,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(v.useful_day), 0)::bigint AS useful_day,
        COALESCE(SUM(v.tiene_kpione2), 0)::bigint AS tiene_kpione2,
        COALESCE(SUM(v.tiene_power_app), 0)::bigint AS tiene_power_app,
        COALESCE(SUM(v.kpione1_audit_only), 0)::bigint AS kpione1_audit_only,
        COALESCE(SUM(v.power_app_fallback), 0)::bigint AS power_app_fallback,
        COALESCE(SUM(v.raw_evidence_count), 0)::bigint AS raw_evidence_count,
        COUNT(*) FILTER (WHERE v.fuente_ganadora = 'KPIONE2')::bigint AS kpione2_winner_rows,
        COUNT(*) FILTER (WHERE v.fuente_ganadora = 'POWER_APP')::bigint AS power_app_winner_rows,
        COUNT(*) FILTER (WHERE v.fuente_ganadora IS NULL)::bigint AS no_winner_rows
    FROM cg_core.v_cg_visita_dia_precedencia_v2 v
    JOIN affected_dates ad
      ON ad.fecha_visita = v.fecha_visita
    GROUP BY v.fecha_visita
),
fact AS (
    SELECT
        f.fecha_visita,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(f.useful_day), 0)::bigint AS useful_day,
        COALESCE(SUM(f.tiene_kpione2), 0)::bigint AS tiene_kpione2,
        COALESCE(SUM(f.tiene_power_app), 0)::bigint AS tiene_power_app,
        COALESCE(SUM(f.kpione1_audit_only), 0)::bigint AS kpione1_audit_only,
        COALESCE(SUM(f.power_app_fallback), 0)::bigint AS power_app_fallback,
        COALESCE(SUM(f.raw_evidence_count), 0)::bigint AS raw_evidence_count,
        COUNT(*) FILTER (WHERE f.fuente_ganadora = 'KPIONE2')::bigint AS kpione2_winner_rows,
        COUNT(*) FILTER (WHERE f.fuente_ganadora = 'POWER_APP')::bigint AS power_app_winner_rows,
        COUNT(*) FILTER (WHERE f.fuente_ganadora IS NULL)::bigint AS no_winner_rows
    FROM cg_mart.fact_cg_visita_dia_resuelta_v2 f
    JOIN affected_dates ad
      ON ad.fecha_visita = f.fecha_visita
    GROUP BY f.fecha_visita
)
SELECT
    COALESCE(fact.fecha_visita, src.fecha_visita) AS fecha_visita,
    COALESCE(fact.rows, 0) - COALESCE(src.rows, 0) AS rows_diff,
    COALESCE(fact.useful_day, 0) - COALESCE(src.useful_day, 0) AS useful_day_diff,
    COALESCE(fact.tiene_kpione2, 0) - COALESCE(src.tiene_kpione2, 0) AS tiene_kpione2_diff,
    COALESCE(fact.tiene_power_app, 0) - COALESCE(src.tiene_power_app, 0) AS tiene_power_app_diff,
    COALESCE(fact.kpione1_audit_only, 0) - COALESCE(src.kpione1_audit_only, 0) AS kpione1_audit_only_diff,
    COALESCE(fact.power_app_fallback, 0) - COALESCE(src.power_app_fallback, 0) AS power_app_fallback_diff,
    COALESCE(fact.raw_evidence_count, 0) - COALESCE(src.raw_evidence_count, 0) AS raw_evidence_count_diff,
    COALESCE(fact.kpione2_winner_rows, 0) - COALESCE(src.kpione2_winner_rows, 0) AS kpione2_winner_rows_diff,
    COALESCE(fact.power_app_winner_rows, 0) - COALESCE(src.power_app_winner_rows, 0) AS power_app_winner_rows_diff,
    COALESCE(fact.no_winner_rows, 0) - COALESCE(src.no_winner_rows, 0) AS no_winner_rows_diff
FROM fact
FULL JOIN src USING (fecha_visita)
ORDER BY fecha_visita;

-- =========================================================
-- 5) FUTURE WEEKLY VIEW FROM DAILY FACT
-- =========================================================
-- Draft only. Do not apply yet.
-- Do not replace cg_mart.v_cg_out_weekly_v2 in this phase.
-- Do not change CG_V2_OUT_WEEKLY_VIEW in this phase.
CREATE OR REPLACE VIEW cg_mart.v_cg_out_weekly_from_daily_fact_v2 AS
WITH base AS (
    SELECT
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
    FROM cg_core.v_rr_frecuencia_base_resuelta_v2 f
),
agg AS (
    SELECT
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
        max(CASE WHEN extract(isodow FROM d.fecha_visita) = 1 THEN d.useful_day ELSE 0 END)::integer AS lunes_flag,
        max(CASE WHEN extract(isodow FROM d.fecha_visita) = 2 THEN d.useful_day ELSE 0 END)::integer AS martes_flag,
        max(CASE WHEN extract(isodow FROM d.fecha_visita) = 3 THEN d.useful_day ELSE 0 END)::integer AS miercoles_flag,
        max(CASE WHEN extract(isodow FROM d.fecha_visita) = 4 THEN d.useful_day ELSE 0 END)::integer AS jueves_flag,
        max(CASE WHEN extract(isodow FROM d.fecha_visita) = 5 THEN d.useful_day ELSE 0 END)::integer AS viernes_flag,
        max(CASE WHEN extract(isodow FROM d.fecha_visita) = 6 THEN d.useful_day ELSE 0 END)::integer AS sabado_flag,
        max(CASE WHEN extract(isodow FROM d.fecha_visita) = 7 THEN d.useful_day ELSE 0 END)::integer AS domingo_flag,
        max(b.lunes)::integer AS lunes_plan,
        max(b.martes)::integer AS martes_plan,
        max(b.miercoles)::integer AS miercoles_plan,
        max(b.jueves)::integer AS jueves_plan,
        max(b.viernes)::integer AS viernes_plan,
        max(b.sabado)::integer AS sabado_plan,
        max(b.domingo)::integer AS domingo_plan,
        max(b.visitas_exigidas_semana)::integer AS visita,
        sum(COALESCE(d.useful_day, 0))::integer AS visita_realizada_raw,
        least(
            sum(COALESCE(d.useful_day, 0))::integer,
            max(b.visitas_exigidas_semana)::integer
        )::integer AS visita_realizada_cap,
        greatest(
            sum(COALESCE(d.useful_day, 0))::integer - max(b.visitas_exigidas_semana)::integer,
            0
        )::integer AS sobre_cumplimiento,
        sum(COALESCE(d.tiene_kpione1, 0))::integer AS dias_kpione,
        sum(COALESCE(d.tiene_kpione2, 0))::integer AS dias_kpione2,
        sum(COALESCE(d.tiene_power_app, 0))::integer AS dias_power_app,
        sum(
            CASE
                WHEN (
                    COALESCE(d.tiene_kpione1, 0)
                  + COALESCE(d.tiene_kpione2, 0)
                  + COALESCE(d.tiene_power_app, 0)
                ) = 2 THEN 1
                ELSE 0
            END
        )::integer AS dias_doble_marcaje,
        sum(
            CASE
                WHEN (
                    COALESCE(d.tiene_kpione1, 0)
                  + COALESCE(d.tiene_kpione2, 0)
                  + COALESCE(d.tiene_power_app, 0)
                ) = 3 THEN 1
                ELSE 0
            END
        )::integer AS dias_triple_marcaje,
        sum(COALESCE(d.persona_conflicto_rows_dia, 0))::integer AS persona_conflicto_rows,
        max(b.ruta_duplicada_flag)::integer AS ruta_duplicada_flag,
        max(b.ruta_duplicada_rows)::integer AS ruta_duplicada_rows,
        concat_ws(
            ' | ',
            CASE WHEN max(CASE WHEN COALESCE(d.tiene_kpione1, 0) = 1 THEN 1 ELSE 0 END) = 1 THEN 'KPIONE' END,
            CASE WHEN max(CASE WHEN COALESCE(d.tiene_kpione2, 0) = 1 THEN 1 ELSE 0 END) = 1 THEN 'KPIONE2' END,
            CASE WHEN max(CASE WHEN COALESCE(d.tiene_power_app, 0) = 1 THEN 1 ELSE 0 END) = 1 THEN 'POWER_APP' END
        ) AS fuentes_reportadas_semana
    FROM base b
    LEFT JOIN cg_mart.fact_cg_visita_dia_resuelta_v2 d
      ON d.cod_rt = b.cod_rt
     AND d.cliente_norm = b.cliente_norm
     AND d.semana_inicio = b.effective_week_start
    GROUP BY
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
SELECT
    cod_rt AS "COD_RT",
    cod_b2b AS "COD_B2B",
    local_nombre AS "LOCAL",
    cliente AS "CLIENTE",
    gestor AS "GESTOR",
    rutero AS "RUTERO",
    reponedor_scope AS "REPONEDOR",
    supervisor AS "SUPERVISOR",
    modalidad AS "MODALIDAD",
    effective_week_start AS "SEMANA_INICIO",
    effective_week_iso AS "SEMANA_ISO",
    lunes_flag AS "LUNES_FLAG",
    martes_flag AS "MARTES_FLAG",
    miercoles_flag AS "MIERCOLES_FLAG",
    jueves_flag AS "JUEVES_FLAG",
    viernes_flag AS "VIERNES_FLAG",
    sabado_flag AS "SABADO_FLAG",
    domingo_flag AS "DOMINGO_FLAG",
    lunes_plan AS "LUNES_PLAN",
    martes_plan AS "MARTES_PLAN",
    miercoles_plan AS "MIERCOLES_PLAN",
    jueves_plan AS "JUEVES_PLAN",
    viernes_plan AS "VIERNES_PLAN",
    sabado_plan AS "SABADO_PLAN",
    domingo_plan AS "DOMINGO_PLAN",
    visita AS "VISITA",
    visita_realizada_raw AS "VISITA_REALIZADA",
    (visita_realizada_raw - visita)::integer AS "DIFERENCIA",
    CASE WHEN visita_realizada_raw >= visita THEN 'CUMPLE' ELSE 'INCUMPLE' END AS "ALERTA",
    dias_kpione AS "DIAS_KPIONE",
    dias_kpione2 AS "DIAS_KPIONE2",
    dias_power_app AS "DIAS_POWER_APP",
    dias_doble_marcaje AS "DIAS_DOBLE_MARCAJE",
    dias_triple_marcaje AS "DIAS_TRIPLE_MARCAJE",
    fuentes_reportadas_semana AS "FUENTES_REPORTADAS_SEMANA",
    persona_conflicto_rows AS "PERSONA_CONFLICTO_ROWS",
    visita_realizada_raw AS "VISITA_REALIZADA_RAW",
    visita_realizada_cap AS "VISITA_REALIZADA_CAP",
    sobre_cumplimiento AS "SOBRE_CUMPLIMIENTO",
    ruta_duplicada_flag AS "RUTA_DUPLICADA_FLAG",
    ruta_duplicada_rows AS "RUTA_DUPLICADA_ROWS"
FROM agg;

-- =========================================================
-- 6) FUTURE WEEKLY VALIDATION
-- =========================================================
-- Draft only. Run only after the daily fact and weekly-from-daily view are
-- approved and applied manually.
WITH affected_weeks AS (
    SELECT DISTINCT semana_inicio
    FROM unnest(ARRAY[DATE '2026-05-11']::date[]) AS weeks(semana_inicio)
    WHERE semana_inicio IS NOT NULL
),
mv AS (
    SELECT
        m."SEMANA_INICIO"::date AS semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(COALESCE(m."VISITA", 0)), 0)::bigint AS visita,
        COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
        COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
        COALESCE(SUM(GREATEST(COALESCE(m."VISITA", 0) - COALESCE(m."VISITA_REALIZADA_CAP", 0), 0)), 0)::bigint AS visitas_pendientes_calc,
        COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(CAST(m."ALERTA" AS text), ''))) = 'CUMPLE')::bigint AS cumple_rows,
        COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(CAST(m."ALERTA" AS text), ''))) = 'INCUMPLE')::bigint AS incumple_rows,
        COALESCE(SUM(CASE
            WHEN COALESCE(m."RUTA_DUPLICADA_FLAG", 0) = 1
              OR COALESCE(m."RUTA_DUPLICADA_ROWS", 0) > 1
              OR CAST(m."GESTOR" AS text) LIKE '%|%'
              OR CAST(m."RUTERO" AS text) LIKE '%|%'
            THEN 1 ELSE 0 END), 0)::bigint AS gestion_compartida_rows
    FROM cg_mart.mv_cg_out_weekly_v2 m
    JOIN affected_weeks aw
      ON aw.semana_inicio = m."SEMANA_INICIO"
    GROUP BY m."SEMANA_INICIO"::date
),
candidate AS (
    SELECT
        c."SEMANA_INICIO"::date AS semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(COALESCE(c."VISITA", 0)), 0)::bigint AS visita,
        COALESCE(SUM(COALESCE(c."VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
        COALESCE(SUM(COALESCE(c."VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
        COALESCE(SUM(GREATEST(COALESCE(c."VISITA", 0) - COALESCE(c."VISITA_REALIZADA_CAP", 0), 0)), 0)::bigint AS visitas_pendientes_calc,
        COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(CAST(c."ALERTA" AS text), ''))) = 'CUMPLE')::bigint AS cumple_rows,
        COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(CAST(c."ALERTA" AS text), ''))) = 'INCUMPLE')::bigint AS incumple_rows,
        COALESCE(SUM(CASE
            WHEN COALESCE(c."RUTA_DUPLICADA_FLAG", 0) = 1
              OR COALESCE(c."RUTA_DUPLICADA_ROWS", 0) > 1
              OR CAST(c."GESTOR" AS text) LIKE '%|%'
              OR CAST(c."RUTERO" AS text) LIKE '%|%'
            THEN 1 ELSE 0 END), 0)::bigint AS gestion_compartida_rows
    FROM cg_mart.v_cg_out_weekly_from_daily_fact_v2 c
    JOIN affected_weeks aw
      ON aw.semana_inicio = c."SEMANA_INICIO"
    GROUP BY c."SEMANA_INICIO"::date
)
SELECT
    COALESCE(candidate.semana_inicio, mv.semana_inicio) AS semana_inicio,
    COALESCE(candidate.rows, 0) - COALESCE(mv.rows, 0) AS rows_diff,
    COALESCE(candidate.visita, 0) - COALESCE(mv.visita, 0) AS visita_diff,
    COALESCE(candidate.visita_realizada_raw, 0) - COALESCE(mv.visita_realizada_raw, 0) AS visita_realizada_raw_diff,
    COALESCE(candidate.visita_realizada_cap, 0) - COALESCE(mv.visita_realizada_cap, 0) AS visita_realizada_cap_diff,
    COALESCE(candidate.visitas_pendientes_calc, 0) - COALESCE(mv.visitas_pendientes_calc, 0) AS visitas_pendientes_calc_diff,
    COALESCE(candidate.cumple_rows, 0) - COALESCE(mv.cumple_rows, 0) AS cumple_rows_diff,
    COALESCE(candidate.incumple_rows, 0) - COALESCE(mv.incumple_rows, 0) AS incumple_rows_diff,
    COALESCE(candidate.gestion_compartida_rows, 0) - COALESCE(mv.gestion_compartida_rows, 0) AS gestion_compartida_rows_diff
FROM candidate
FULL JOIN mv USING (semana_inicio)
ORDER BY semana_inicio DESC;

-- =========================================================
-- 7) FALLBACK AND APPLY ORDER
-- =========================================================
-- Apply order for human review:
--   1. Create daily fact table and non-unique indexes.
--   2. Run daily stage for one affected date.
--   3. Confirm stage vs daily precedence validation returns only 0 diffs.
--   4. Apply daily DELETE/INSERT for affected dates.
--   5. Confirm fact vs daily precedence validation returns only 0 diffs.
--   6. Only then review weekly-from-daily view in isolation.
--   7. Do not change CG_V2_OUT_WEEKLY_VIEW until weekly validation is clean.
--
-- Runtime rollback remains:
--   CG_V2_OUT_WEEKLY_VIEW=cg_mart.mv_cg_out_weekly_v2
