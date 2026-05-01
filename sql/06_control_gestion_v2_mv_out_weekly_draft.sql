-- =========================================================
-- CONTROL_GESTION V2 MV OUT WEEKLY DRAFT
-- =========================================================
-- Manual draft only.
-- Do not auto-execute from app or Codex.
-- Execute manually by Basti with explicit authorization.
-- Non-destructive.
-- Does not replace cg_mart.v_cg_out_weekly_v2.
-- Source of truth for this draft: cg_mart.v_cg_out_weekly_v2.
--
-- Purpose:
-- Materialize the weekly output used by CONTROL_GESTION v2 so that:
--   1. get_cg_v2_scope_kpis can read a precomputed weekly dataset.
--   2. get_cg_v2_daily_matrix_full/page can optionally read the same object later.
--   3. future exports can reuse the same weekly grain without recomputing the full view chain.
--
-- Runbook:
--   1. Execute CREATE MATERIALIZED VIEW ... WITH NO DATA.
--   2. Execute REFRESH MATERIALIZED VIEW cg_mart.mv_cg_out_weekly_v2;
--   3. Execute ANALYZE cg_mart.mv_cg_out_weekly_v2;
--   4. Execute CREATE INDEX IF NOT EXISTS statements below.
--   5. Execute ANALYZE again.
--   6. Execute validation queries below. Differences should stay at zero.
--   7. Execute EXPLAIN queries below.
--   8. Only in a later phase, if all checks pass, change env:
--        CG_V2_OUT_WEEKLY_VIEW=cg_mart.mv_cg_out_weekly_v2
--   9. Smoke app with the new env.
--  10. If anything fails, keep env pointing to:
--        CG_V2_OUT_WEEKLY_VIEW=cg_mart.v_cg_out_weekly_v2
--
-- Notes:
--   * REFRESH MATERIALIZED VIEW CONCURRENTLY requires a validated unique index.
--   * This draft does not create a unique index yet.
--   * The MV can remain created and unused while the app keeps reading the current view.

-- =========================================================
-- 1) CREATE MATERIALIZED VIEW
-- =========================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS cg_mart.mv_cg_out_weekly_v2 AS
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
    CAST(v."SEMANA_INICIO" AS DATE) AS "SEMANA_INICIO_KEY",
    UPPER(TRIM(COALESCE(CAST(v."GESTOR" AS TEXT), ''))) AS "GESTOR_NORM_FILTER",
    UPPER(TRIM(COALESCE(CAST(v."RUTERO" AS TEXT), ''))) AS "RUTERO_NORM_FILTER",
    UPPER(TRIM(COALESCE(CAST(v."LOCAL" AS TEXT), ''))) AS "LOCAL_NORM_FILTER",
    UPPER(TRIM(COALESCE(CAST(v."CLIENTE" AS TEXT), ''))) AS "CLIENTE_NORM_FILTER",
    UPPER(TRIM(COALESCE(CAST(v."ALERTA" AS TEXT), ''))) AS "ALERTA_NORM_FILTER",
    CASE
        WHEN COALESCE(v."RUTA_DUPLICADA_FLAG", 0) = 1
          OR COALESCE(v."RUTA_DUPLICADA_ROWS", 0) > 1
          OR CAST(v."GESTOR" AS TEXT) LIKE '%|%'
          OR CAST(v."RUTERO" AS TEXT) LIKE '%|%'
        THEN 1
        ELSE 0
    END::integer AS "GESTION_COMPARTIDA_FLAG_CALC",
    GREATEST(COALESCE(v."VISITA", 0) - COALESCE(v."VISITA_REALIZADA_CAP", 0), 0)::integer AS "VISITAS_PENDIENTES_CALC"
FROM cg_mart.v_cg_out_weekly_v2 v
WITH NO DATA;

-- =========================================================
-- 2) INITIAL REFRESH AND ANALYZE
-- =========================================================
REFRESH MATERIALIZED VIEW cg_mart.mv_cg_out_weekly_v2;

ANALYZE cg_mart.mv_cg_out_weekly_v2;

-- =========================================================
-- 3) INDEXES OVER THE MV
-- =========================================================
CREATE INDEX IF NOT EXISTS ix_mv_cg_out_weekly_v2_semana
ON cg_mart.mv_cg_out_weekly_v2 ("SEMANA_INICIO");

CREATE INDEX IF NOT EXISTS ix_mv_cg_out_weekly_v2_semana_gestor
ON cg_mart.mv_cg_out_weekly_v2 ("SEMANA_INICIO", "GESTOR_NORM_FILTER");

CREATE INDEX IF NOT EXISTS ix_mv_cg_out_weekly_v2_semana_gestor_rutero
ON cg_mart.mv_cg_out_weekly_v2 ("SEMANA_INICIO", "GESTOR_NORM_FILTER", "RUTERO_NORM_FILTER");

CREATE INDEX IF NOT EXISTS ix_mv_cg_out_weekly_v2_semana_gestor_local
ON cg_mart.mv_cg_out_weekly_v2 ("SEMANA_INICIO", "GESTOR_NORM_FILTER", "LOCAL_NORM_FILTER");

CREATE INDEX IF NOT EXISTS ix_mv_cg_out_weekly_v2_semana_gestor_cliente
ON cg_mart.mv_cg_out_weekly_v2 ("SEMANA_INICIO", "GESTOR_NORM_FILTER", "CLIENTE_NORM_FILTER");

CREATE INDEX IF NOT EXISTS ix_mv_cg_out_weekly_v2_semana_alerta
ON cg_mart.mv_cg_out_weekly_v2 ("SEMANA_INICIO", "ALERTA_NORM_FILTER");

CREATE INDEX IF NOT EXISTS ix_mv_cg_out_weekly_v2_matrix_filters
ON cg_mart.mv_cg_out_weekly_v2 (
    "SEMANA_INICIO",
    "GESTOR_NORM_FILTER",
    "RUTERO_NORM_FILTER",
    "LOCAL_NORM_FILTER",
    "CLIENTE_NORM_FILTER"
);

ANALYZE cg_mart.mv_cg_out_weekly_v2;

-- Optional future block:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY cg_mart.mv_cg_out_weekly_v2;
-- This stays disabled by default because it requires a validated unique index.

-- =========================================================
-- 4) POST-REFRESH VALIDATIONS
-- Differences should stay at zero.
-- =========================================================

-- 4.1 Totals: source vs MV
WITH src AS (
    SELECT
        COUNT(*)::bigint AS rows_total,
        COALESCE(SUM(COALESCE("VISITA", 0)), 0)::bigint AS visita_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap_total,
        COALESCE(SUM(COALESCE("SOBRE_CUMPLIMIENTO", 0)), 0)::bigint AS sobre_cumplimiento_total,
        COALESCE(SUM(GREATEST(COALESCE("VISITA", 0) - COALESCE("VISITA_REALIZADA_CAP", 0), 0)), 0)::bigint AS visitas_pendientes_total,
        COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(CAST("ALERTA" AS TEXT), ''))) = 'CUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS cumple_rows,
        COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(CAST("ALERTA" AS TEXT), ''))) = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS incumple_rows,
        COALESCE(SUM(CASE
            WHEN COALESCE("RUTA_DUPLICADA_FLAG", 0) = 1
              OR COALESCE("RUTA_DUPLICADA_ROWS", 0) > 1
              OR CAST("GESTOR" AS TEXT) LIKE '%|%'
              OR CAST("RUTERO" AS TEXT) LIKE '%|%'
            THEN 1 ELSE 0 END), 0)::bigint AS gestion_compartida_rows
    FROM cg_mart.v_cg_out_weekly_v2
),
mv AS (
    SELECT
        COUNT(*)::bigint AS rows_total,
        COALESCE(SUM(COALESCE("VISITA", 0)), 0)::bigint AS visita_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap_total,
        COALESCE(SUM(COALESCE("SOBRE_CUMPLIMIENTO", 0)), 0)::bigint AS sobre_cumplimiento_total,
        COALESCE(SUM(COALESCE("VISITAS_PENDIENTES_CALC", 0)), 0)::bigint AS visitas_pendientes_total,
        COALESCE(SUM(CASE WHEN "ALERTA_NORM_FILTER" = 'CUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS cumple_rows,
        COALESCE(SUM(CASE WHEN "ALERTA_NORM_FILTER" = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS incumple_rows,
        COALESCE(SUM(COALESCE("GESTION_COMPARTIDA_FLAG_CALC", 0)), 0)::bigint AS gestion_compartida_rows
    FROM cg_mart.mv_cg_out_weekly_v2
)
SELECT
    src.rows_total AS src_rows_total,
    mv.rows_total AS mv_rows_total,
    mv.rows_total - src.rows_total AS diff_rows_total,
    src.visita_total AS src_visita_total,
    mv.visita_total AS mv_visita_total,
    mv.visita_total - src.visita_total AS diff_visita_total,
    src.visita_realizada_raw_total AS src_visita_realizada_raw_total,
    mv.visita_realizada_raw_total AS mv_visita_realizada_raw_total,
    mv.visita_realizada_raw_total - src.visita_realizada_raw_total AS diff_visita_realizada_raw_total,
    src.visita_realizada_cap_total AS src_visita_realizada_cap_total,
    mv.visita_realizada_cap_total AS mv_visita_realizada_cap_total,
    mv.visita_realizada_cap_total - src.visita_realizada_cap_total AS diff_visita_realizada_cap_total,
    src.sobre_cumplimiento_total AS src_sobre_cumplimiento_total,
    mv.sobre_cumplimiento_total AS mv_sobre_cumplimiento_total,
    mv.sobre_cumplimiento_total - src.sobre_cumplimiento_total AS diff_sobre_cumplimiento_total,
    src.visitas_pendientes_total AS src_visitas_pendientes_total,
    mv.visitas_pendientes_total AS mv_visitas_pendientes_total,
    mv.visitas_pendientes_total - src.visitas_pendientes_total AS diff_visitas_pendientes_total,
    src.cumple_rows AS src_cumple_rows,
    mv.cumple_rows AS mv_cumple_rows,
    mv.cumple_rows - src.cumple_rows AS diff_cumple_rows,
    src.incumple_rows AS src_incumple_rows,
    mv.incumple_rows AS mv_incumple_rows,
    mv.incumple_rows - src.incumple_rows AS diff_incumple_rows,
    src.gestion_compartida_rows AS src_gestion_compartida_rows,
    mv.gestion_compartida_rows AS mv_gestion_compartida_rows,
    mv.gestion_compartida_rows - src.gestion_compartida_rows AS diff_gestion_compartida_rows
FROM src
CROSS JOIN mv;

-- 4.2 Totals by week: source vs MV
WITH src AS (
    SELECT
        "SEMANA_INICIO",
        COUNT(*)::bigint AS rows_total,
        COALESCE(SUM(COALESCE("VISITA", 0)), 0)::bigint AS visita_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap_total,
        COALESCE(SUM(COALESCE("SOBRE_CUMPLIMIENTO", 0)), 0)::bigint AS sobre_cumplimiento_total,
        COALESCE(SUM(GREATEST(COALESCE("VISITA", 0) - COALESCE("VISITA_REALIZADA_CAP", 0), 0)), 0)::bigint AS visitas_pendientes_total,
        COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(CAST("ALERTA" AS TEXT), ''))) = 'CUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS cumple_rows,
        COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(CAST("ALERTA" AS TEXT), ''))) = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS incumple_rows,
        COALESCE(SUM(CASE
            WHEN COALESCE("RUTA_DUPLICADA_FLAG", 0) = 1
              OR COALESCE("RUTA_DUPLICADA_ROWS", 0) > 1
              OR CAST("GESTOR" AS TEXT) LIKE '%|%'
              OR CAST("RUTERO" AS TEXT) LIKE '%|%'
            THEN 1 ELSE 0 END), 0)::bigint AS gestion_compartida_rows
    FROM cg_mart.v_cg_out_weekly_v2
    GROUP BY "SEMANA_INICIO"
),
mv AS (
    SELECT
        "SEMANA_INICIO",
        COUNT(*)::bigint AS rows_total,
        COALESCE(SUM(COALESCE("VISITA", 0)), 0)::bigint AS visita_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap_total,
        COALESCE(SUM(COALESCE("SOBRE_CUMPLIMIENTO", 0)), 0)::bigint AS sobre_cumplimiento_total,
        COALESCE(SUM(COALESCE("VISITAS_PENDIENTES_CALC", 0)), 0)::bigint AS visitas_pendientes_total,
        COALESCE(SUM(CASE WHEN "ALERTA_NORM_FILTER" = 'CUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS cumple_rows,
        COALESCE(SUM(CASE WHEN "ALERTA_NORM_FILTER" = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS incumple_rows,
        COALESCE(SUM(COALESCE("GESTION_COMPARTIDA_FLAG_CALC", 0)), 0)::bigint AS gestion_compartida_rows
    FROM cg_mart.mv_cg_out_weekly_v2
    GROUP BY "SEMANA_INICIO"
)
SELECT
    COALESCE(src."SEMANA_INICIO", mv."SEMANA_INICIO") AS "SEMANA_INICIO",
    COALESCE(src.rows_total, 0) AS src_rows_total,
    COALESCE(mv.rows_total, 0) AS mv_rows_total,
    COALESCE(mv.rows_total, 0) - COALESCE(src.rows_total, 0) AS diff_rows_total,
    COALESCE(src.visita_total, 0) AS src_visita_total,
    COALESCE(mv.visita_total, 0) AS mv_visita_total,
    COALESCE(mv.visita_total, 0) - COALESCE(src.visita_total, 0) AS diff_visita_total,
    COALESCE(src.visita_realizada_raw_total, 0) AS src_visita_realizada_raw_total,
    COALESCE(mv.visita_realizada_raw_total, 0) AS mv_visita_realizada_raw_total,
    COALESCE(mv.visita_realizada_raw_total, 0) - COALESCE(src.visita_realizada_raw_total, 0) AS diff_visita_realizada_raw_total,
    COALESCE(src.visita_realizada_cap_total, 0) AS src_visita_realizada_cap_total,
    COALESCE(mv.visita_realizada_cap_total, 0) AS mv_visita_realizada_cap_total,
    COALESCE(mv.visita_realizada_cap_total, 0) - COALESCE(src.visita_realizada_cap_total, 0) AS diff_visita_realizada_cap_total,
    COALESCE(src.sobre_cumplimiento_total, 0) AS src_sobre_cumplimiento_total,
    COALESCE(mv.sobre_cumplimiento_total, 0) AS mv_sobre_cumplimiento_total,
    COALESCE(mv.sobre_cumplimiento_total, 0) - COALESCE(src.sobre_cumplimiento_total, 0) AS diff_sobre_cumplimiento_total,
    COALESCE(src.visitas_pendientes_total, 0) AS src_visitas_pendientes_total,
    COALESCE(mv.visitas_pendientes_total, 0) AS mv_visitas_pendientes_total,
    COALESCE(mv.visitas_pendientes_total, 0) - COALESCE(src.visitas_pendientes_total, 0) AS diff_visitas_pendientes_total,
    COALESCE(src.cumple_rows, 0) AS src_cumple_rows,
    COALESCE(mv.cumple_rows, 0) AS mv_cumple_rows,
    COALESCE(mv.cumple_rows, 0) - COALESCE(src.cumple_rows, 0) AS diff_cumple_rows,
    COALESCE(src.incumple_rows, 0) AS src_incumple_rows,
    COALESCE(mv.incumple_rows, 0) AS mv_incumple_rows,
    COALESCE(mv.incumple_rows, 0) - COALESCE(src.incumple_rows, 0) AS diff_incumple_rows,
    COALESCE(src.gestion_compartida_rows, 0) AS src_gestion_compartida_rows,
    COALESCE(mv.gestion_compartida_rows, 0) AS mv_gestion_compartida_rows,
    COALESCE(mv.gestion_compartida_rows, 0) - COALESCE(src.gestion_compartida_rows, 0) AS diff_gestion_compartida_rows
FROM src
FULL OUTER JOIN mv
  ON mv."SEMANA_INICIO" = src."SEMANA_INICIO"
ORDER BY "SEMANA_INICIO" DESC;

-- 4.3 Current week comparison
WITH current_week AS (
    SELECT MAX("SEMANA_INICIO") AS semana_inicio
    FROM cg_mart.v_cg_out_weekly_v2
),
src AS (
    SELECT
        COUNT(*)::bigint AS rows_total,
        COALESCE(SUM(COALESCE("VISITA", 0)), 0)::bigint AS visita_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap_total,
        COALESCE(SUM(COALESCE("SOBRE_CUMPLIMIENTO", 0)), 0)::bigint AS sobre_cumplimiento_total,
        COALESCE(SUM(GREATEST(COALESCE("VISITA", 0) - COALESCE("VISITA_REALIZADA_CAP", 0), 0)), 0)::bigint AS visitas_pendientes_total
    FROM cg_mart.v_cg_out_weekly_v2
    WHERE "SEMANA_INICIO" = (SELECT semana_inicio FROM current_week)
),
mv AS (
    SELECT
        COUNT(*)::bigint AS rows_total,
        COALESCE(SUM(COALESCE("VISITA", 0)), 0)::bigint AS visita_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw_total,
        COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap_total,
        COALESCE(SUM(COALESCE("SOBRE_CUMPLIMIENTO", 0)), 0)::bigint AS sobre_cumplimiento_total,
        COALESCE(SUM(COALESCE("VISITAS_PENDIENTES_CALC", 0)), 0)::bigint AS visitas_pendientes_total
    FROM cg_mart.mv_cg_out_weekly_v2
    WHERE "SEMANA_INICIO" = (SELECT semana_inicio FROM current_week)
)
SELECT
    src.rows_total AS src_rows_total,
    mv.rows_total AS mv_rows_total,
    mv.rows_total - src.rows_total AS diff_rows_total,
    src.visita_total AS src_visita_total,
    mv.visita_total AS mv_visita_total,
    mv.visita_total - src.visita_total AS diff_visita_total,
    src.visita_realizada_raw_total AS src_visita_realizada_raw_total,
    mv.visita_realizada_raw_total AS mv_visita_realizada_raw_total,
    mv.visita_realizada_raw_total - src.visita_realizada_raw_total AS diff_visita_realizada_raw_total,
    src.visita_realizada_cap_total AS src_visita_realizada_cap_total,
    mv.visita_realizada_cap_total AS mv_visita_realizada_cap_total,
    mv.visita_realizada_cap_total - src.visita_realizada_cap_total AS diff_visita_realizada_cap_total,
    src.sobre_cumplimiento_total AS src_sobre_cumplimiento_total,
    mv.sobre_cumplimiento_total AS mv_sobre_cumplimiento_total,
    mv.sobre_cumplimiento_total - src.sobre_cumplimiento_total AS diff_sobre_cumplimiento_total,
    src.visitas_pendientes_total AS src_visitas_pendientes_total,
    mv.visitas_pendientes_total AS mv_visitas_pendientes_total,
    mv.visitas_pendientes_total - src.visitas_pendientes_total AS diff_visitas_pendientes_total
FROM src
CROSS JOIN mv;

-- =========================================================
-- 5) GRAIN VALIDATION
-- Do not create a unique index until this query stays empty.
-- =========================================================
SELECT
    "SEMANA_INICIO",
    "COD_RT",
    "CLIENTE",
    "GESTOR",
    "RUTERO",
    "REPONEDOR",
    "MODALIDAD",
    COUNT(*) AS rows_in_grain
FROM cg_mart.mv_cg_out_weekly_v2
GROUP BY
    "SEMANA_INICIO",
    "COD_RT",
    "CLIENTE",
    "GESTOR",
    "RUTERO",
    "REPONEDOR",
    "MODALIDAD"
HAVING COUNT(*) > 1
ORDER BY rows_in_grain DESC, "SEMANA_INICIO" DESC, "GESTOR", "RUTERO", "COD_RT", "CLIENTE";

-- =========================================================
-- 6) EXPLAIN QUERIES AGAINST THE MV
-- Replace sample values as needed before running.
-- =========================================================

-- 6.1 KPI global for current week
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    COUNT(*)::int AS total_rows,
    COALESCE(SUM(COALESCE("VISITA", 0)), 0)::int AS visita_plan,
    COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::int AS visita_realizada_raw,
    COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::int AS visita_realizada_cap,
    COALESCE(SUM(COALESCE("VISITAS_PENDIENTES_CALC", 0)), 0)::int AS visitas_pendientes,
    COALESCE(SUM(COALESCE("SOBRE_CUMPLIMIENTO", 0)), 0)::int AS sobre_cumplimiento,
    COALESCE(SUM(CASE WHEN "ALERTA_NORM_FILTER" = 'CUMPLE' THEN 1 ELSE 0 END), 0)::int AS cumple_rows,
    COALESCE(SUM(CASE WHEN "ALERTA_NORM_FILTER" = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::int AS incumple_rows,
    COALESCE(SUM(COALESCE("GESTION_COMPARTIDA_FLAG_CALC", 0)), 0)::int AS gestion_compartida_rows
FROM cg_mart.mv_cg_out_weekly_v2
WHERE "SEMANA_INICIO" = (
    SELECT MAX("SEMANA_INICIO")
    FROM cg_mart.mv_cg_out_weekly_v2
);

-- 6.2 KPI by gestor
WITH params AS (
    SELECT
        (SELECT MAX("SEMANA_INICIO") FROM cg_mart.mv_cg_out_weekly_v2) AS semana_inicio,
        UPPER(TRIM('EYRIMAR VARGAS')) AS gestor_norm
)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    COUNT(*)::int AS total_rows,
    COALESCE(SUM(COALESCE(m."VISITA", 0)), 0)::int AS visita_plan,
    COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_RAW", 0)), 0)::int AS visita_realizada_raw,
    COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_CAP", 0)), 0)::int AS visita_realizada_cap,
    COALESCE(SUM(COALESCE(m."VISITAS_PENDIENTES_CALC", 0)), 0)::int AS visitas_pendientes,
    COALESCE(SUM(COALESCE(m."SOBRE_CUMPLIMIENTO", 0)), 0)::int AS sobre_cumplimiento,
    COALESCE(SUM(CASE WHEN m."ALERTA_NORM_FILTER" = 'CUMPLE' THEN 1 ELSE 0 END), 0)::int AS cumple_rows,
    COALESCE(SUM(CASE WHEN m."ALERTA_NORM_FILTER" = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::int AS incumple_rows,
    COALESCE(SUM(COALESCE(m."GESTION_COMPARTIDA_FLAG_CALC", 0)), 0)::int AS gestion_compartida_rows
FROM cg_mart.mv_cg_out_weekly_v2 m
CROSS JOIN params p
WHERE m."SEMANA_INICIO" = p.semana_inicio
  AND m."GESTOR_NORM_FILTER" = p.gestor_norm;

-- 6.3 KPI by gestor + rutero
WITH params AS (
    SELECT
        (SELECT MAX("SEMANA_INICIO") FROM cg_mart.mv_cg_out_weekly_v2) AS semana_inicio,
        UPPER(TRIM('EYRIMAR VARGAS')) AS gestor_norm,
        UPPER(TRIM('CMU-2')) AS rutero_norm
)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    COUNT(*)::int AS total_rows,
    COALESCE(SUM(COALESCE(m."VISITA", 0)), 0)::int AS visita_plan,
    COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_RAW", 0)), 0)::int AS visita_realizada_raw,
    COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_CAP", 0)), 0)::int AS visita_realizada_cap,
    COALESCE(SUM(COALESCE(m."VISITAS_PENDIENTES_CALC", 0)), 0)::int AS visitas_pendientes,
    COALESCE(SUM(COALESCE(m."SOBRE_CUMPLIMIENTO", 0)), 0)::int AS sobre_cumplimiento,
    COALESCE(SUM(CASE WHEN m."ALERTA_NORM_FILTER" = 'CUMPLE' THEN 1 ELSE 0 END), 0)::int AS cumple_rows,
    COALESCE(SUM(CASE WHEN m."ALERTA_NORM_FILTER" = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::int AS incumple_rows,
    COALESCE(SUM(COALESCE(m."GESTION_COMPARTIDA_FLAG_CALC", 0)), 0)::int AS gestion_compartida_rows
FROM cg_mart.mv_cg_out_weekly_v2 m
CROSS JOIN params p
WHERE m."SEMANA_INICIO" = p.semana_inicio
  AND m."GESTOR_NORM_FILTER" = p.gestor_norm
  AND m."RUTERO_NORM_FILTER" = p.rutero_norm;

-- 6.4 KPI by gestor + local
WITH params AS (
    SELECT
        (SELECT MAX("SEMANA_INICIO") FROM cg_mart.mv_cg_out_weekly_v2) AS semana_inicio,
        UPPER(TRIM('EYRIMAR VARGAS')) AS gestor_norm,
        UPPER(TRIM('DARKSTORE VINA DEL MAR')) AS local_norm
)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    COUNT(*)::int AS total_rows,
    COALESCE(SUM(COALESCE(m."VISITA", 0)), 0)::int AS visita_plan,
    COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_RAW", 0)), 0)::int AS visita_realizada_raw,
    COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_CAP", 0)), 0)::int AS visita_realizada_cap,
    COALESCE(SUM(COALESCE(m."VISITAS_PENDIENTES_CALC", 0)), 0)::int AS visitas_pendientes,
    COALESCE(SUM(COALESCE(m."SOBRE_CUMPLIMIENTO", 0)), 0)::int AS sobre_cumplimiento,
    COALESCE(SUM(CASE WHEN m."ALERTA_NORM_FILTER" = 'CUMPLE' THEN 1 ELSE 0 END), 0)::int AS cumple_rows,
    COALESCE(SUM(CASE WHEN m."ALERTA_NORM_FILTER" = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::int AS incumple_rows,
    COALESCE(SUM(COALESCE(m."GESTION_COMPARTIDA_FLAG_CALC", 0)), 0)::int AS gestion_compartida_rows
FROM cg_mart.mv_cg_out_weekly_v2 m
CROSS JOIN params p
WHERE m."SEMANA_INICIO" = p.semana_inicio
  AND m."GESTOR_NORM_FILTER" = p.gestor_norm
  AND m."LOCAL_NORM_FILTER" = p.local_norm;

-- 6.5 KPI by gestor + cliente
WITH params AS (
    SELECT
        (SELECT MAX("SEMANA_INICIO") FROM cg_mart.mv_cg_out_weekly_v2) AS semana_inicio,
        UPPER(TRIM('EYRIMAR VARGAS')) AS gestor_norm,
        UPPER(TRIM('CASO Y CIA')) AS cliente_norm
)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    COUNT(*)::int AS total_rows,
    COALESCE(SUM(COALESCE(m."VISITA", 0)), 0)::int AS visita_plan,
    COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_RAW", 0)), 0)::int AS visita_realizada_raw,
    COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_CAP", 0)), 0)::int AS visita_realizada_cap,
    COALESCE(SUM(COALESCE(m."VISITAS_PENDIENTES_CALC", 0)), 0)::int AS visitas_pendientes,
    COALESCE(SUM(COALESCE(m."SOBRE_CUMPLIMIENTO", 0)), 0)::int AS sobre_cumplimiento,
    COALESCE(SUM(CASE WHEN m."ALERTA_NORM_FILTER" = 'CUMPLE' THEN 1 ELSE 0 END), 0)::int AS cumple_rows,
    COALESCE(SUM(CASE WHEN m."ALERTA_NORM_FILTER" = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::int AS incumple_rows,
    COALESCE(SUM(COALESCE(m."GESTION_COMPARTIDA_FLAG_CALC", 0)), 0)::int AS gestion_compartida_rows
FROM cg_mart.mv_cg_out_weekly_v2 m
CROSS JOIN params p
WHERE m."SEMANA_INICIO" = p.semana_inicio
  AND m."GESTOR_NORM_FILTER" = p.gestor_norm
  AND m."CLIENTE_NORM_FILTER" = p.cliente_norm;
