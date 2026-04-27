-- STOCK_ZERO
-- CLIENTE scope materialized views
-- DDL documental/versionable alineado con las MVs creadas en Supabase.

-- =========================================================
-- 1) BASE ENRICHED INVENTORY
-- =========================================================
CREATE MATERIALIZED VIEW public.mv_cliente_scope_inventory_enriched AS
WITH base AS (
    SELECT
        b.fecha,
        b.cod_rt,
        b.local_nombre_rr,
        COALESCE(NULLIF(TRIM(b.marca), ''), '') AS cliente,
        UPPER(TRIM(COALESCE(NULLIF(b.marca_norm, ''), NULLIF(b.marca, ''), ''))) AS cliente_norm,
        COALESCE(NULLIF(TRIM(b.marca), ''), '') AS marca,
        UPPER(TRIM(COALESCE(NULLIF(b.marca_norm, ''), NULLIF(b.marca, ''), ''))) AS marca_norm,
        CAST(b.sku AS TEXT) AS sku,
        COALESCE(b.producto, '') AS producto,
        COALESCE(b.stock, 0)::int AS stock,
        COALESCE(b.venta_7, 0)::int AS venta_7,
        COALESCE(b.negativo::text, '') AS negativo,
        COALESCE(b.riesgo_quiebre::text, '') AS riesgo_quiebre,
        COALESCE(b.otros::text, '') AS otros
    FROM public.mv_scope_fact_latest_cliente b
),
rr_ctx AS (
    SELECT
        rr.cod_rt,
        rr.cliente_norm,
        STRING_AGG(DISTINCT rr.gestor, ' | ' ORDER BY rr.gestor)
            FILTER (WHERE NULLIF(TRIM(COALESCE(rr.gestor, '')), '') IS NOT NULL) AS gestores,
        STRING_AGG(DISTINCT rr.supervisor, ' | ' ORDER BY rr.supervisor)
            FILTER (WHERE NULLIF(TRIM(COALESCE(rr.supervisor, '')), '') IS NOT NULL) AS supervisores,
        STRING_AGG(DISTINCT rr.rutero, ' | ' ORDER BY rr.rutero)
            FILTER (WHERE NULLIF(TRIM(COALESCE(rr.rutero, '')), '') IS NOT NULL) AS ruteros,
        STRING_AGG(DISTINCT rr.reponedor, ' | ' ORDER BY rr.reponedor)
            FILTER (WHERE NULLIF(TRIM(COALESCE(rr.reponedor, '')), '') IS NOT NULL) AS reponedores,
        STRING_AGG(DISTINCT rr.modalidad, ' | ' ORDER BY rr.modalidad)
            FILTER (WHERE NULLIF(TRIM(COALESCE(rr.modalidad, '')), '') IS NOT NULL) AS modalidades
    FROM (
        SELECT DISTINCT
            r.cod_rt,
            UPPER(TRIM(COALESCE(r.cliente, ''))) AS cliente_norm,
            NULLIF(TRIM(r.gestores), '') AS gestor,
            NULLIF(TRIM(r.supervisor), '') AS supervisor,
            NULLIF(TRIM(r.rutero), '') AS rutero,
            NULLIF(TRIM(r.reponedor), '') AS reponedor,
            NULLIF(TRIM(r.modalidad), '') AS modalidad
        FROM public.ruta_rutero r
        WHERE NULLIF(TRIM(COALESCE(r.cod_rt, '')), '') IS NOT NULL
          AND NULLIF(TRIM(COALESCE(r.cliente, '')), '') IS NOT NULL
    ) rr
    GROUP BY rr.cod_rt, rr.cliente_norm
)
SELECT
    base.fecha,
    base.cod_rt,
    base.local_nombre_rr,
    base.cliente,
    base.cliente_norm,
    base.marca,
    base.marca_norm,
    base.sku,
    base.producto,
    base.stock,
    base.venta_7,
    base.negativo,
    base.riesgo_quiebre,
    base.otros,
    COALESCE(NULLIF(rr_ctx.gestores, ''), 'SIN ASIGNAR') AS gestores,
    COALESCE(NULLIF(rr_ctx.supervisores, ''), 'SIN ASIGNAR') AS supervisores,
    COALESCE(NULLIF(rr_ctx.ruteros, ''), 'SIN ASIGNAR') AS ruteros,
    COALESCE(NULLIF(rr_ctx.reponedores, ''), 'SIN ASIGNAR') AS reponedores,
    COALESCE(NULLIF(rr_ctx.modalidades, ''), 'SIN ASIGNAR') AS modalidades,
    CASE WHEN COALESCE(base.venta_7, 0) = 0 THEN 1 ELSE 0 END AS venta_0_flag,
    CASE WHEN UPPER(TRIM(COALESCE(base.negativo, ''))) = 'SI' THEN 1 ELSE 0 END AS negativo_flag,
    CASE WHEN UPPER(TRIM(COALESCE(base.riesgo_quiebre, ''))) = 'SI' THEN 1 ELSE 0 END AS quiebre_flag,
    CASE WHEN NULLIF(TRIM(COALESCE(base.otros, '')), '') IS NOT NULL THEN 1 ELSE 0 END AS otros_flag,
    CASE
        WHEN COALESCE(base.venta_7, 0) = 0
          OR UPPER(TRIM(COALESCE(base.negativo, ''))) = 'SI'
          OR UPPER(TRIM(COALESCE(base.riesgo_quiebre, ''))) = 'SI'
          OR NULLIF(TRIM(COALESCE(base.otros, '')), '') IS NOT NULL
        THEN 1 ELSE 0
    END AS skus_en_foco_flag
FROM base
LEFT JOIN rr_ctx
  ON rr_ctx.cod_rt = base.cod_rt
 AND rr_ctx.cliente_norm = base.cliente_norm;

CREATE UNIQUE INDEX ux_mv_cliente_scope_inventory_enriched_inv
ON public.mv_cliente_scope_inventory_enriched (cod_rt, cliente_norm, sku);

CREATE INDEX ix_mv_cliente_scope_inventory_enriched_cliente
ON public.mv_cliente_scope_inventory_enriched (cliente_norm);

CREATE INDEX ix_mv_cliente_scope_inventory_enriched_cliente_codrt
ON public.mv_cliente_scope_inventory_enriched (cliente_norm, cod_rt);


-- =========================================================
-- 2) RANKING POR CLIENTE
-- =========================================================
CREATE MATERIALIZED VIEW public.mv_cliente_scope_ranking_cliente AS
SELECT
    MAX(cliente) AS cliente,
    cliente_norm,
    COUNT(DISTINCT cod_rt)::int AS locales,
    COUNT(*)::int AS total_skus,
    SUM(venta_0_flag)::int AS venta_0,
    SUM(negativo_flag)::int AS negativos,
    SUM(quiebre_flag)::int AS quiebres,
    SUM(otros_flag)::int AS otros,
    SUM(skus_en_foco_flag)::int AS skus_en_foco,
    MIN(fecha) AS fecha_min,
    MAX(fecha) AS fecha_max
FROM public.mv_cliente_scope_inventory_enriched
GROUP BY cliente_norm;

CREATE UNIQUE INDEX ux_mv_cliente_scope_ranking_cliente
ON public.mv_cliente_scope_ranking_cliente (cliente_norm);

CREATE INDEX ix_mv_cliente_scope_ranking_cliente_order
ON public.mv_cliente_scope_ranking_cliente (
    skus_en_foco DESC,
    quiebres DESC,
    venta_0 DESC,
    total_skus DESC,
    cliente_norm
);


-- =========================================================
-- 3) RANKING POR RESPONSABLE
-- =========================================================
CREATE MATERIALIZED VIEW public.mv_cliente_scope_ranking_responsable AS
WITH rr_pairs AS (
    SELECT DISTINCT
        e.cod_rt,
        e.cliente_norm,
        e.sku,
        'GESTOR'::text AS responsable_tipo,
        COALESCE(NULLIF(TRIM(r.gestores), ''), '') AS responsable,
        UPPER(TRIM(COALESCE(r.gestores, ''))) AS responsable_norm
    FROM public.mv_cliente_scope_inventory_enriched e
    JOIN public.ruta_rutero r
      ON r.cod_rt = e.cod_rt
     AND UPPER(TRIM(COALESCE(r.cliente, ''))) = e.cliente_norm
    WHERE NULLIF(TRIM(COALESCE(r.gestores, '')), '') IS NOT NULL

    UNION

    SELECT DISTINCT
        e.cod_rt,
        e.cliente_norm,
        e.sku,
        'SUPERVISOR'::text AS responsable_tipo,
        COALESCE(NULLIF(TRIM(r.supervisor), ''), '') AS responsable,
        UPPER(TRIM(COALESCE(r.supervisor, ''))) AS responsable_norm
    FROM public.mv_cliente_scope_inventory_enriched e
    JOIN public.ruta_rutero r
      ON r.cod_rt = e.cod_rt
     AND UPPER(TRIM(COALESCE(r.cliente, ''))) = e.cliente_norm
    WHERE NULLIF(TRIM(COALESCE(r.supervisor, '')), '') IS NOT NULL
),
dedup AS (
    SELECT DISTINCT
        rr_pairs.responsable_tipo,
        rr_pairs.responsable,
        rr_pairs.responsable_norm,
        rr_pairs.cod_rt,
        rr_pairs.cliente_norm,
        rr_pairs.sku
    FROM rr_pairs
)
SELECT
    d.responsable_tipo,
    MAX(d.responsable) AS responsable,
    d.responsable_norm,
    COUNT(DISTINCT d.cliente_norm)::int AS clientes,
    COUNT(DISTINCT d.cod_rt)::int AS locales,
    COUNT(*)::int AS total_skus,
    SUM(e.venta_0_flag)::int AS venta_0,
    SUM(e.negativo_flag)::int AS negativos,
    SUM(e.quiebre_flag)::int AS quiebres,
    SUM(e.otros_flag)::int AS otros,
    SUM(e.skus_en_foco_flag)::int AS skus_en_foco
FROM dedup d
JOIN public.mv_cliente_scope_inventory_enriched e
  ON e.cod_rt = d.cod_rt
 AND e.cliente_norm = d.cliente_norm
 AND e.sku = d.sku
GROUP BY
    d.responsable_tipo,
    d.responsable_norm;

CREATE UNIQUE INDEX ux_mv_cliente_scope_ranking_responsable
ON public.mv_cliente_scope_ranking_responsable (responsable_tipo, responsable_norm);

CREATE INDEX ix_mv_cliente_scope_ranking_responsable_order
ON public.mv_cliente_scope_ranking_responsable (
    responsable_tipo,
    skus_en_foco DESC,
    negativos DESC,
    venta_0 DESC,
    quiebres DESC,
    responsable_norm
);


-- =========================================================
-- VALIDACIONES ESPERADAS
-- =========================================================
-- 1) Base enriched inventory
-- SELECT COUNT(*) FROM public.mv_cliente_scope_inventory_enriched;
-- Esperado: 22956
--
-- SELECT
--   COUNT(*) AS filas,
--   COUNT(DISTINCT cod_rt || '|' || cliente_norm || '|' || sku) AS claves
-- FROM public.mv_cliente_scope_inventory_enriched;
-- Esperado: filas = claves = 22956
--
-- SELECT COUNT(*)
-- FROM public.mv_cliente_scope_inventory_enriched
-- WHERE gestores = 'SIN ASIGNAR'
--   AND supervisores = 'SIN ASIGNAR'
--   AND ruteros = 'SIN ASIGNAR'
--   AND reponedores = 'SIN ASIGNAR';
-- Esperado: 53
--
-- SELECT
--   SUM(venta_0_flag) AS venta_0,
--   SUM(negativo_flag) AS negativos,
--   SUM(quiebre_flag) AS quiebres,
--   SUM(otros_flag) AS otros,
--   SUM(skus_en_foco_flag) AS skus_en_foco
-- FROM public.mv_cliente_scope_inventory_enriched;
-- Esperado:
--   venta_0 = 9011
--   negativos = 263
--   quiebres = 1368
--   otros = 0
--   skus_en_foco = 10540
--
-- 2) Ranking cliente
-- SELECT COUNT(*) FROM public.mv_cliente_scope_ranking_cliente;
-- Esperado: 40
--
-- SELECT
--   SUM(total_skus) AS total_skus,
--   SUM(venta_0) AS venta_0,
--   SUM(negativos) AS negativos,
--   SUM(quiebres) AS quiebres,
--   SUM(otros) AS otros,
--   SUM(skus_en_foco) AS skus_en_foco
-- FROM public.mv_cliente_scope_ranking_cliente;
-- Esperado:
--   total_skus = 22956
--   venta_0 = 9011
--   negativos = 263
--   quiebres = 1368
--   otros = 0
--   skus_en_foco = 10540
--
-- 3) Ranking responsable
-- SELECT COUNT(*)
-- FROM public.mv_cliente_scope_ranking_responsable;
-- Esperado: 21
--
-- SELECT responsable_tipo, COUNT(*)
-- FROM public.mv_cliente_scope_ranking_responsable
-- GROUP BY responsable_tipo
-- ORDER BY responsable_tipo;
-- Esperado:
--   GESTOR = 8
--   SUPERVISOR = 13
--
-- Casos conocidos:
-- GESTOR / CARLOS GARAY
--   total_skus = 4388
--   venta_0 = 1748
--   negativos = 54
--   quiebres = 283
--   otros = 0
--   skus_en_foco = 2070
--
-- SUPERVISOR / ALEXIS INATTI
--   total_skus = 2251
--   venta_0 = 840
--   negativos = 28
--   quiebres = 138
--   otros = 0
--   skus_en_foco = 996


-- =========================================================
-- ORDEN DE REFRESH RECOMENDADO
-- =========================================================
-- 1. REFRESH MATERIALIZED VIEW public.mv_scope_fact_latest_cliente;
-- 2. REFRESH MATERIALIZED VIEW public.mv_cliente_scope_inventory_enriched;
-- 3. REFRESH MATERIALIZED VIEW public.mv_cliente_scope_ranking_cliente;
-- 4. REFRESH MATERIALIZED VIEW public.mv_cliente_scope_ranking_responsable;
--
-- Si se quiere usar CONCURRENTLY:
-- - mantener primero los índices únicos
-- - refrescar cada MV por separado
