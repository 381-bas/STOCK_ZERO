-- =========================================================
-- CONTROL_GESTION V2 INCREMENTAL MART DRAFT
-- FASE_9C5K_B_CONTROL_GESTION_INCREMENTAL_MART_SQL_DRAFT_AND_BENCHMARK
-- =========================================================
-- Manual draft only. Do not execute from Codex.
-- No business-rule changes:
--   KPIONE2 first, POWER_APP fallback, KPIONE1 audit-only.
--
-- Purpose:
--   Replace daily full MV refresh with a physical mart that can be
--   rebuilt only for affected operational weeks.
--
-- Runtime switch, only after human review and validation:
--   CG_V2_OUT_WEEKLY_VIEW=cg_mart.fact_cg_out_weekly_v2

-- =========================================================
-- 1) PHYSICAL TABLE, SAME PUBLIC SIGNATURE AS CURRENT MV
-- =========================================================
CREATE TABLE IF NOT EXISTS cg_mart.fact_cg_out_weekly_v2 (
    "COD_RT" text,
    "COD_B2B" text,
    "LOCAL" text,
    "CLIENTE" text,
    "GESTOR" text,
    "RUTERO" text,
    "REPONEDOR" text,
    "SUPERVISOR" text,
    "MODALIDAD" text,
    "SEMANA_INICIO" date,
    "SEMANA_ISO" integer,
    "LUNES_FLAG" integer,
    "MARTES_FLAG" integer,
    "MIERCOLES_FLAG" integer,
    "JUEVES_FLAG" integer,
    "VIERNES_FLAG" integer,
    "SABADO_FLAG" integer,
    "DOMINGO_FLAG" integer,
    "LUNES_PLAN" integer,
    "MARTES_PLAN" integer,
    "MIERCOLES_PLAN" integer,
    "JUEVES_PLAN" integer,
    "VIERNES_PLAN" integer,
    "SABADO_PLAN" integer,
    "DOMINGO_PLAN" integer,
    "VISITA" integer,
    "VISITA_REALIZADA" integer,
    "DIFERENCIA" integer,
    "ALERTA" text,
    "DIAS_KPIONE" integer,
    "DIAS_KPIONE2" integer,
    "DIAS_POWER_APP" integer,
    "DIAS_DOBLE_MARCAJE" integer,
    "DIAS_TRIPLE_MARCAJE" integer,
    "FUENTES_REPORTADAS_SEMANA" text,
    "PERSONA_CONFLICTO_ROWS" integer,
    "VISITA_REALIZADA_RAW" integer,
    "VISITA_REALIZADA_CAP" integer,
    "SOBRE_CUMPLIMIENTO" integer,
    "RUTA_DUPLICADA_FLAG" integer,
    "RUTA_DUPLICADA_ROWS" integer,
    "SEMANA_INICIO_KEY" date,
    "GESTOR_NORM_FILTER" text,
    "RUTERO_NORM_FILTER" text,
    "LOCAL_NORM_FILTER" text,
    "CLIENTE_NORM_FILTER" text,
    "ALERTA_NORM_FILTER" text,
    "GESTION_COMPARTIDA_FLAG_CALC" integer,
    "VISITAS_PENDIENTES_CALC" integer
);

-- =========================================================
-- 2) INDEXES
-- =========================================================
CREATE INDEX IF NOT EXISTS ix_fact_cg_out_weekly_v2_semana
ON cg_mart.fact_cg_out_weekly_v2 ("SEMANA_INICIO");

CREATE INDEX IF NOT EXISTS ix_fact_cg_out_weekly_v2_semana_gestor
ON cg_mart.fact_cg_out_weekly_v2 ("SEMANA_INICIO", "GESTOR_NORM_FILTER");

CREATE INDEX IF NOT EXISTS ix_fact_cg_out_weekly_v2_semana_rutero
ON cg_mart.fact_cg_out_weekly_v2 ("SEMANA_INICIO", "RUTERO_NORM_FILTER");

CREATE INDEX IF NOT EXISTS ix_fact_cg_out_weekly_v2_semana_cliente
ON cg_mart.fact_cg_out_weekly_v2 ("SEMANA_INICIO", "CLIENTE_NORM_FILTER");

CREATE INDEX IF NOT EXISTS ix_fact_cg_out_weekly_v2_semana_alerta
ON cg_mart.fact_cg_out_weekly_v2 ("SEMANA_INICIO", "ALERTA_NORM_FILTER");

CREATE INDEX IF NOT EXISTS ix_fact_cg_out_weekly_v2_matrix_filters
ON cg_mart.fact_cg_out_weekly_v2 (
    "SEMANA_INICIO",
    "GESTOR_NORM_FILTER",
    "RUTERO_NORM_FILTER",
    "LOCAL_NORM_FILTER",
    "CLIENTE_NORM_FILTER"
);

-- Candidate validated on current MV: 0 duplicates, 0 null/blank key inputs.
-- Optional post-validation only. Keep this as a review point. If business
-- later allows shared duplicated route/customer weekly rows, do not force it.
-- CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_cg_out_weekly_v2_semana_codrt_cliente
-- ON cg_mart.fact_cg_out_weekly_v2 ("SEMANA_INICIO", "COD_RT", "CLIENTE_NORM_FILTER");

-- =========================================================
-- 3) INITIAL BACKFILL FROM VALIDATED MV
-- =========================================================
-- Protected manual step:
--   1. Run the precheck below.
--   2. Run the INSERT only when fact_rows_before = 0.
--   3. The NOT EXISTS guard makes the INSERT idempotent/no-op if the table
--      already contains rows.
--
-- Do not use this block to repair partial incremental failures. For that,
-- use the affected-week rebuild block below or rebuild from the validated MV
-- in a separate authorized runbook.
SELECT
    COUNT(*)::bigint AS fact_rows_before
FROM cg_mart.fact_cg_out_weekly_v2;

INSERT INTO cg_mart.fact_cg_out_weekly_v2 (
    "COD_RT", "COD_B2B", "LOCAL", "CLIENTE", "GESTOR", "RUTERO",
    "REPONEDOR", "SUPERVISOR", "MODALIDAD", "SEMANA_INICIO",
    "SEMANA_ISO", "LUNES_FLAG", "MARTES_FLAG", "MIERCOLES_FLAG",
    "JUEVES_FLAG", "VIERNES_FLAG", "SABADO_FLAG", "DOMINGO_FLAG",
    "LUNES_PLAN", "MARTES_PLAN", "MIERCOLES_PLAN", "JUEVES_PLAN",
    "VIERNES_PLAN", "SABADO_PLAN", "DOMINGO_PLAN", "VISITA",
    "VISITA_REALIZADA", "DIFERENCIA", "ALERTA", "DIAS_KPIONE",
    "DIAS_KPIONE2", "DIAS_POWER_APP", "DIAS_DOBLE_MARCAJE",
    "DIAS_TRIPLE_MARCAJE", "FUENTES_REPORTADAS_SEMANA",
    "PERSONA_CONFLICTO_ROWS", "VISITA_REALIZADA_RAW",
    "VISITA_REALIZADA_CAP", "SOBRE_CUMPLIMIENTO", "RUTA_DUPLICADA_FLAG",
    "RUTA_DUPLICADA_ROWS", "SEMANA_INICIO_KEY", "GESTOR_NORM_FILTER",
    "RUTERO_NORM_FILTER", "LOCAL_NORM_FILTER", "CLIENTE_NORM_FILTER",
    "ALERTA_NORM_FILTER", "GESTION_COMPARTIDA_FLAG_CALC",
    "VISITAS_PENDIENTES_CALC"
)
SELECT
    "COD_RT", "COD_B2B", "LOCAL", "CLIENTE", "GESTOR", "RUTERO",
    "REPONEDOR", "SUPERVISOR", "MODALIDAD", "SEMANA_INICIO",
    "SEMANA_ISO", "LUNES_FLAG", "MARTES_FLAG", "MIERCOLES_FLAG",
    "JUEVES_FLAG", "VIERNES_FLAG", "SABADO_FLAG", "DOMINGO_FLAG",
    "LUNES_PLAN", "MARTES_PLAN", "MIERCOLES_PLAN", "JUEVES_PLAN",
    "VIERNES_PLAN", "SABADO_PLAN", "DOMINGO_PLAN", "VISITA",
    "VISITA_REALIZADA", "DIFERENCIA", "ALERTA", "DIAS_KPIONE",
    "DIAS_KPIONE2", "DIAS_POWER_APP", "DIAS_DOBLE_MARCAJE",
    "DIAS_TRIPLE_MARCAJE", "FUENTES_REPORTADAS_SEMANA",
    "PERSONA_CONFLICTO_ROWS", "VISITA_REALIZADA_RAW",
    "VISITA_REALIZADA_CAP", "SOBRE_CUMPLIMIENTO", "RUTA_DUPLICADA_FLAG",
    "RUTA_DUPLICADA_ROWS", "SEMANA_INICIO_KEY", "GESTOR_NORM_FILTER",
    "RUTERO_NORM_FILTER", "LOCAL_NORM_FILTER", "CLIENTE_NORM_FILTER",
    "ALERTA_NORM_FILTER", "GESTION_COMPARTIDA_FLAG_CALC",
    "VISITAS_PENDIENTES_CALC"
FROM cg_mart.mv_cg_out_weekly_v2
WHERE NOT EXISTS (
    SELECT 1
    FROM cg_mart.fact_cg_out_weekly_v2
    LIMIT 1
);

ANALYZE cg_mart.fact_cg_out_weekly_v2;

-- =========================================================
-- 4) INCREMENTAL REBUILD FOR AFFECTED WEEKS
-- =========================================================
-- affected_weeks must be provided by the caller as date[].
--
-- Manual review example:
--   ARRAY[DATE '2026-05-11', DATE '2026-05-04']::date[]
--
-- Driver/script placeholder example:
--   :affected_weeks::date[]
--
-- Rollback: this block is transactional. Any error should ROLLBACK and keep
-- the previous fact rows for weeks not yet committed.
BEGIN;

-- Manual affected weeks. Replace the ARRAY before running.
CREATE TEMP TABLE _cg_affected_weeks (
    semana_inicio date PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _cg_affected_weeks (semana_inicio)
SELECT DISTINCT semana_inicio
FROM unnest(ARRAY[DATE '2026-05-11', DATE '2026-05-04']::date[]) AS weeks(semana_inicio)
WHERE semana_inicio IS NOT NULL;

-- Placeholder-driven alternative for a future script:
-- INSERT INTO _cg_affected_weeks (semana_inicio)
-- SELECT DISTINCT semana_inicio
-- FROM unnest(:affected_weeks::date[]) AS weeks(semana_inicio)
-- WHERE semana_inicio IS NOT NULL;

-- Stage the replacement rows first. Do not delete current fact rows until the
-- stage has been populated and checked.
CREATE TEMP TABLE _cg_out_weekly_v2_stage
ON COMMIT DROP AS
SELECT
    v."COD_RT",
    v."COD_B2B",
    v."LOCAL",
    v."CLIENTE",
    v."GESTOR",
    v."RUTERO",
    v."REPONEDOR",
    v."SUPERVISOR",
    v."MODALIDAD",
    v."SEMANA_INICIO",
    v."SEMANA_ISO",
    v."LUNES_FLAG",
    v."MARTES_FLAG",
    v."MIERCOLES_FLAG",
    v."JUEVES_FLAG",
    v."VIERNES_FLAG",
    v."SABADO_FLAG",
    v."DOMINGO_FLAG",
    v."LUNES_PLAN",
    v."MARTES_PLAN",
    v."MIERCOLES_PLAN",
    v."JUEVES_PLAN",
    v."VIERNES_PLAN",
    v."SABADO_PLAN",
    v."DOMINGO_PLAN",
    v."VISITA",
    v."VISITA_REALIZADA",
    v."DIFERENCIA",
    v."ALERTA",
    v."DIAS_KPIONE",
    v."DIAS_KPIONE2",
    v."DIAS_POWER_APP",
    v."DIAS_DOBLE_MARCAJE",
    v."DIAS_TRIPLE_MARCAJE",
    v."FUENTES_REPORTADAS_SEMANA",
    v."PERSONA_CONFLICTO_ROWS",
    v."VISITA_REALIZADA_RAW",
    v."VISITA_REALIZADA_CAP",
    v."SOBRE_CUMPLIMIENTO",
    v."RUTA_DUPLICADA_FLAG",
    v."RUTA_DUPLICADA_ROWS",
    CAST(v."SEMANA_INICIO" AS date) AS "SEMANA_INICIO_KEY",
    UPPER(TRIM(COALESCE(CAST(v."GESTOR" AS text), ''))) AS "GESTOR_NORM_FILTER",
    UPPER(TRIM(COALESCE(CAST(v."RUTERO" AS text), ''))) AS "RUTERO_NORM_FILTER",
    UPPER(TRIM(COALESCE(CAST(v."LOCAL" AS text), ''))) AS "LOCAL_NORM_FILTER",
    UPPER(TRIM(COALESCE(CAST(v."CLIENTE" AS text), ''))) AS "CLIENTE_NORM_FILTER",
    UPPER(TRIM(COALESCE(CAST(v."ALERTA" AS text), ''))) AS "ALERTA_NORM_FILTER",
    CASE
        WHEN COALESCE(v."RUTA_DUPLICADA_FLAG", 0) = 1
          OR COALESCE(v."RUTA_DUPLICADA_ROWS", 0) > 1
          OR CAST(v."GESTOR" AS text) LIKE '%|%'
          OR CAST(v."RUTERO" AS text) LIKE '%|%'
        THEN 1
        ELSE 0
    END::integer AS "GESTION_COMPARTIDA_FLAG_CALC",
    GREATEST(COALESCE(v."VISITA", 0) - COALESCE(v."VISITA_REALIZADA_CAP", 0), 0)::integer AS "VISITAS_PENDIENTES_CALC"
FROM cg_mart.v_cg_out_weekly_v2 v
JOIN _cg_affected_weeks aw
  ON aw.semana_inicio = v."SEMANA_INICIO";

-- Required precheck. If stage_rows = 0, stop and ROLLBACK; do not replace
-- weeks with an empty result.
SELECT
    COUNT(*)::bigint AS stage_rows,
    COUNT(DISTINCT "SEMANA_INICIO")::bigint AS stage_weeks,
    MIN("SEMANA_INICIO") AS min_stage_week,
    MAX("SEMANA_INICIO") AS max_stage_week
FROM _cg_out_weekly_v2_stage;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM _cg_out_weekly_v2_stage LIMIT 1) THEN
        RAISE EXCEPTION 'BLOCK_EMPTY_INCREMENTAL_STAGE: no source rows for affected weeks';
    END IF;
END $$;

DELETE FROM cg_mart.fact_cg_out_weekly_v2 f
USING _cg_affected_weeks aw
WHERE f."SEMANA_INICIO" = aw.semana_inicio;

INSERT INTO cg_mart.fact_cg_out_weekly_v2 (
    "COD_RT", "COD_B2B", "LOCAL", "CLIENTE", "GESTOR", "RUTERO",
    "REPONEDOR", "SUPERVISOR", "MODALIDAD", "SEMANA_INICIO",
    "SEMANA_ISO", "LUNES_FLAG", "MARTES_FLAG", "MIERCOLES_FLAG",
    "JUEVES_FLAG", "VIERNES_FLAG", "SABADO_FLAG", "DOMINGO_FLAG",
    "LUNES_PLAN", "MARTES_PLAN", "MIERCOLES_PLAN", "JUEVES_PLAN",
    "VIERNES_PLAN", "SABADO_PLAN", "DOMINGO_PLAN", "VISITA",
    "VISITA_REALIZADA", "DIFERENCIA", "ALERTA", "DIAS_KPIONE",
    "DIAS_KPIONE2", "DIAS_POWER_APP", "DIAS_DOBLE_MARCAJE",
    "DIAS_TRIPLE_MARCAJE", "FUENTES_REPORTADAS_SEMANA",
    "PERSONA_CONFLICTO_ROWS", "VISITA_REALIZADA_RAW",
    "VISITA_REALIZADA_CAP", "SOBRE_CUMPLIMIENTO", "RUTA_DUPLICADA_FLAG",
    "RUTA_DUPLICADA_ROWS", "SEMANA_INICIO_KEY", "GESTOR_NORM_FILTER",
    "RUTERO_NORM_FILTER", "LOCAL_NORM_FILTER", "CLIENTE_NORM_FILTER",
    "ALERTA_NORM_FILTER", "GESTION_COMPARTIDA_FLAG_CALC",
    "VISITAS_PENDIENTES_CALC"
)
SELECT
    s."COD_RT",
    s."COD_B2B",
    s."LOCAL",
    s."CLIENTE",
    s."GESTOR",
    s."RUTERO",
    s."REPONEDOR",
    s."SUPERVISOR",
    s."MODALIDAD",
    s."SEMANA_INICIO",
    s."SEMANA_ISO",
    s."LUNES_FLAG",
    s."MARTES_FLAG",
    s."MIERCOLES_FLAG",
    s."JUEVES_FLAG",
    s."VIERNES_FLAG",
    s."SABADO_FLAG",
    s."DOMINGO_FLAG",
    s."LUNES_PLAN",
    s."MARTES_PLAN",
    s."MIERCOLES_PLAN",
    s."JUEVES_PLAN",
    s."VIERNES_PLAN",
    s."SABADO_PLAN",
    s."DOMINGO_PLAN",
    s."VISITA",
    s."VISITA_REALIZADA",
    s."DIFERENCIA",
    s."ALERTA",
    s."DIAS_KPIONE",
    s."DIAS_KPIONE2",
    s."DIAS_POWER_APP",
    s."DIAS_DOBLE_MARCAJE",
    s."DIAS_TRIPLE_MARCAJE",
    s."FUENTES_REPORTADAS_SEMANA",
    s."PERSONA_CONFLICTO_ROWS",
    s."VISITA_REALIZADA_RAW",
    s."VISITA_REALIZADA_CAP",
    s."SOBRE_CUMPLIMIENTO",
    s."RUTA_DUPLICADA_FLAG",
    s."RUTA_DUPLICADA_ROWS",
    s."SEMANA_INICIO_KEY",
    s."GESTOR_NORM_FILTER",
    s."RUTERO_NORM_FILTER",
    s."LOCAL_NORM_FILTER",
    s."CLIENTE_NORM_FILTER",
    s."ALERTA_NORM_FILTER",
    s."GESTION_COMPARTIDA_FLAG_CALC",
    s."VISITAS_PENDIENTES_CALC"
FROM _cg_out_weekly_v2_stage s;

COMMIT;

ANALYZE cg_mart.fact_cg_out_weekly_v2;

-- =========================================================
-- 5) VALIDATE AFFECTED WEEKS ONLY
-- =========================================================
-- Manual validation example. Replace the ARRAY before running.
-- Future script placeholder:
--   SELECT unnest(:affected_weeks::date[]) AS semana_inicio
WITH affected_weeks AS (
    SELECT DISTINCT semana_inicio
    FROM unnest(ARRAY[DATE '2026-05-11', DATE '2026-05-04']::date[]) AS weeks(semana_inicio)
    WHERE semana_inicio IS NOT NULL
),
src AS (
    SELECT
        v."SEMANA_INICIO"::date AS semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(COALESCE(v."VISITA", 0)), 0)::bigint AS visita,
        COALESCE(SUM(COALESCE(v."VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
        COALESCE(SUM(COALESCE(v."VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
        COALESCE(SUM(GREATEST(COALESCE(v."VISITA", 0) - COALESCE(v."VISITA_REALIZADA_CAP", 0), 0)), 0)::bigint AS visitas_pendientes_calc,
        COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(CAST(v."ALERTA" AS text), ''))) = 'CUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS cumple_rows,
        COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(CAST(v."ALERTA" AS text), ''))) = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS incumple_rows,
        COALESCE(SUM(CASE
            WHEN COALESCE(v."RUTA_DUPLICADA_FLAG", 0) = 1
              OR COALESCE(v."RUTA_DUPLICADA_ROWS", 0) > 1
              OR CAST(v."GESTOR" AS text) LIKE '%|%'
              OR CAST(v."RUTERO" AS text) LIKE '%|%'
            THEN 1 ELSE 0 END), 0)::bigint AS gestion_compartida_rows
    FROM cg_mart.v_cg_out_weekly_v2 v
    JOIN affected_weeks aw
      ON aw.semana_inicio = v."SEMANA_INICIO"
    GROUP BY v."SEMANA_INICIO"::date
),
fact AS (
    SELECT
        f."SEMANA_INICIO"::date AS semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(COALESCE(f."VISITA", 0)), 0)::bigint AS visita,
        COALESCE(SUM(COALESCE(f."VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
        COALESCE(SUM(COALESCE(f."VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
        COALESCE(SUM(COALESCE(f."VISITAS_PENDIENTES_CALC", 0)), 0)::bigint AS visitas_pendientes_calc,
        COALESCE(SUM(CASE WHEN COALESCE(f."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(f."ALERTA" AS text), '')))) = 'CUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS cumple_rows,
        COALESCE(SUM(CASE WHEN COALESCE(f."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(f."ALERTA" AS text), '')))) = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS incumple_rows,
        COALESCE(SUM(COALESCE(f."GESTION_COMPARTIDA_FLAG_CALC", 0)), 0)::bigint AS gestion_compartida_rows
    FROM cg_mart.fact_cg_out_weekly_v2 f
    JOIN affected_weeks aw
      ON aw.semana_inicio = f."SEMANA_INICIO"
    GROUP BY f."SEMANA_INICIO"::date
)
SELECT
    COALESCE(src.semana_inicio, fact.semana_inicio) AS semana_inicio,
    COALESCE(fact.rows, 0) - COALESCE(src.rows, 0) AS rows_diff,
    COALESCE(fact.visita, 0) - COALESCE(src.visita, 0) AS visita_diff,
    COALESCE(fact.visita_realizada_raw, 0) - COALESCE(src.visita_realizada_raw, 0) AS visita_realizada_raw_diff,
    COALESCE(fact.visita_realizada_cap, 0) - COALESCE(src.visita_realizada_cap, 0) AS visita_realizada_cap_diff,
    COALESCE(fact.visitas_pendientes_calc, 0) - COALESCE(src.visitas_pendientes_calc, 0) AS visitas_pendientes_calc_diff,
    COALESCE(fact.cumple_rows, 0) - COALESCE(src.cumple_rows, 0) AS cumple_diff,
    COALESCE(fact.incumple_rows, 0) - COALESCE(src.incumple_rows, 0) AS incumple_diff,
    COALESCE(fact.gestion_compartida_rows, 0) - COALESCE(src.gestion_compartida_rows, 0) AS gestion_compartida_diff
FROM src
FULL JOIN fact USING (semana_inicio)
ORDER BY semana_inicio DESC;

-- =========================================================
-- 6) FULL FALLBACK, IF INCREMENTAL FAILS
-- =========================================================
-- Keep the existing full-refresh flow available:
--   python scripts/refresh_control_gestion_v2_mv.py --statement-timeout-seconds 1800 --validate --skip-analyze
--
-- If this fact table becomes the runtime source and incremental fails:
--   1. Do not reload Excel unless source load failed.
--   2. Rerun incremental for affected weeks.
--   3. If still failing, rebuild fact from validated MV or temporarily point
--      CG_V2_OUT_WEEKLY_VIEW back to cg_mart.mv_cg_out_weekly_v2.
