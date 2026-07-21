-- CONTROL_GESTION Route B -> daily precedence bridge.
-- Route B is KPIONE2 authority at day/location/client grain.
CREATE OR REPLACE VIEW cg_core.v_cg_visita_dia_precedencia_route_b_v1 AS
WITH route_b_candidates AS MATERIALIZED (
    SELECT
        d.fecha,
        d.location_key,
        d.cliente_norm,
        d.event_count,
        rr.effective_week_start,
        rr.effective_week_iso,
        rr.cod_rt,
        rr.cod_b2b,
        rr.local_nombre,
        rr.cliente,
        rr.gestor,
        rr.gestor_norm,
        rr.rutero,
        rr.reponedor_scope,
        rr.reponedor_scope_norm,
        rr.supervisor,
        rr.jefe_operaciones,
        rr.modalidad,
        row_number() OVER (
            PARTITION BY d.fecha, d.location_key, d.cliente_norm
            ORDER BY
                CASE WHEN rr.cod_rt = d.location_key THEN 1 ELSE 2 END,
                rr.cod_rt,
                rr.cod_b2b NULLS LAST
        ) AS match_rank
    FROM cg_core.kpione_day_presence_v1 d
    JOIN cg_core.v_rr_frecuencia_base_resuelta_v2 rr
      ON rr.effective_week_start = date_trunc('week', d.fecha)::date
     AND rr.cliente_norm = d.cliente_norm
     AND (
            rr.cod_rt = d.location_key
         OR upper(trim(rr.local_nombre)) = d.location_key
     )
),
route_b AS MATERIALIZED (
    SELECT *
    FROM route_b_candidates
    WHERE match_rank = 1
),
legacy AS MATERIALIZED (
    SELECT *
    FROM cg_core.v_cg_visita_dia_precedencia_v2
)
SELECT
    COALESCE(l.semana_inicio, r.effective_week_start)::date AS semana_inicio,
    COALESCE(l.fecha_visita, r.fecha)::date AS fecha_visita,
    COALESCE(l.cod_rt, r.cod_rt)::text AS cod_rt,
    COALESCE(l.cod_b2b, r.cod_b2b)::text AS cod_b2b,
    COALESCE(l.cliente, r.cliente)::text AS cliente,
    COALESCE(l.cliente_norm, r.cliente_norm)::text AS cliente_norm,
    COALESCE(l.local_nombre, r.local_nombre)::text AS local_nombre,
    COALESCE(l.gestor, r.gestor)::text AS gestor,
    COALESCE(l.gestor_norm, r.gestor_norm)::text AS gestor_norm,
    COALESCE(l.rutero, r.rutero)::text AS rutero,
    COALESCE(l.reponedor_scope, r.reponedor_scope)::text AS reponedor_scope,
    COALESCE(l.reponedor_scope_norm, r.reponedor_scope_norm)::text AS reponedor_scope_norm,
    COALESCE(l.supervisor, r.supervisor)::text AS supervisor,
    COALESCE(l.jefe_operaciones, r.jefe_operaciones)::text AS jefe_operaciones,
    COALESCE(l.modalidad, r.modalidad)::text AS modalidad,
    COALESCE(l.semana_iso, r.effective_week_iso)::integer AS semana_iso,
    CASE
        WHEN r.fecha IS NOT NULL OR COALESCE(l.tiene_kpione2, 0) = 1 THEN 'KPIONE2'
        ELSE l.fuente_ganadora
    END::text AS fuente_ganadora,
    concat_ws(
        ' | ',
        CASE WHEN COALESCE(l.tiene_kpione1, 0) = 1 THEN 'KPIONE' END,
        CASE WHEN r.fecha IS NOT NULL OR COALESCE(l.tiene_kpione2, 0) = 1 THEN 'KPIONE2' END,
        CASE WHEN COALESCE(l.tiene_power_app, 0) = 1 THEN 'POWER_APP' END
    )::text AS fuentes_presentes,
    CASE WHEN r.fecha IS NOT NULL OR COALESCE(l.tiene_kpione2, 0) = 1 THEN 1 ELSE 0 END::integer AS tiene_kpione2,
    COALESCE(l.tiene_power_app, 0)::integer AS tiene_power_app,
    COALESCE(l.tiene_kpione1, 0)::integer AS tiene_kpione1,
    CASE
        WHEN r.fecha IS NOT NULL OR COALESCE(l.tiene_kpione2, 0) = 1 THEN 0
        ELSE COALESCE(l.power_app_fallback, 0)
    END::integer AS power_app_fallback,
    COALESCE(l.kpione1_audit_only, 0)::integer AS kpione1_audit_only,
    CASE
        WHEN r.fecha IS NOT NULL OR COALESCE(l.useful_day, 0) = 1 THEN 1 ELSE 0
    END::integer AS useful_day,
    -- Route B replaces the legacy KPIONE2 contribution on the exact overlap
    -- key; audit evidence from KPIONE1/POWER_APP remains visible.
    CASE
        WHEN r.fecha IS NOT NULL THEN
            GREATEST(
                COALESCE(l.raw_evidence_count, 0) - COALESCE(l.kpione2_rows_dia, 0),
                0
            ) + COALESCE(r.event_count, 0)
        ELSE COALESCE(l.raw_evidence_count, 0)
    END::integer AS raw_evidence_count,
    CASE
        WHEN r.fecha IS NOT NULL THEN
            CASE WHEN COALESCE(r.event_count, 0) > 1 THEN 1 ELSE 0 END
        ELSE COALESCE(l.same_source_multimark, 0)
    END::integer AS same_source_multimark,
    CASE
        WHEN r.fecha IS NOT NULL
         AND (COALESCE(l.tiene_power_app, 0) = 1 OR COALESCE(l.tiene_kpione1, 0) = 1)
            THEN 1
        ELSE COALESCE(l.multisource_overlap, 0)
    END::integer AS multisource_overlap,
    COALESCE(l.kpione_rows_dia, 0)::integer AS kpione_rows_dia,
    CASE
        WHEN r.fecha IS NOT NULL THEN COALESCE(r.event_count, 0)
        ELSE COALESCE(l.kpione2_rows_dia, 0)
    END::integer AS kpione2_rows_dia,
    COALESCE(l.power_app_rows_dia, 0)::integer AS power_app_rows_dia,
    COALESCE(l.persona_conflicto_rows_dia, 0)::integer AS persona_conflicto_rows_dia,
    COALESCE(l.match_quality, 'route_b_week_location_cliente')::text AS match_quality,
    COALESCE(l.registro_fuera_cruce, '')::text AS registro_fuera_cruce
FROM legacy l
FULL OUTER JOIN route_b r
  ON r.fecha = l.fecha_visita
 AND r.cod_rt = l.cod_rt
 AND r.cliente_norm = l.cliente_norm;

DO $grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stock_zero_codex_ro') THEN
        GRANT USAGE ON SCHEMA cg_core TO stock_zero_codex_ro;
        GRANT SELECT ON cg_core.v_cg_visita_dia_precedencia_route_b_v1 TO stock_zero_codex_ro;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stock_zero_app_ro') THEN
        GRANT USAGE ON SCHEMA cg_core TO stock_zero_app_ro;
        GRANT SELECT ON cg_core.v_cg_visita_dia_precedencia_route_b_v1 TO stock_zero_app_ro;
    END IF;
END
$grants$;
