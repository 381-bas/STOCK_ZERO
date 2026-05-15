# Contratos de Datos, SQL y Backend

## Entidades minimas

| entidad/campo | descripcion | dominio | obligatorio | observacion |
|---|---|---|---|---|
| `cod_rt` | identificador local/ruta | todos | si | clave principal operativa |
| `local_nombre_rr` / `local` | nombre local | todos | si | visible en tablas/export |
| `cliente` | cliente/marca visible | reposicion, cliente, CG | si | en stock UX aparece como `MARCA` |
| `cliente_norm` | cliente normalizado | cliente, CG | si | clave anti duplicidad |
| `marca` | marca visible | stock | si | filtro/export |
| `marca_norm` | marca normalizada | scope | si | cruce consistente |
| `sku` | SKU | reposicion, cliente | si | ordenar numerico/texto |
| `producto` | descripcion SKU | reposicion, cliente | si | visible/export |
| `stock` | inventario | reposicion, cliente | si | base de tabla |
| `venta_7` | venta ultimos 7 dias | reposicion, cliente | si | Venta 0 |
| `fecha` | fecha stock/visita | todos | si | freshness |
| `rutero` | ruta/persona | reposicion, CG | si | filtro operativo |
| `reponedor` | ejecutor | reposicion, CG | si | contexto y export |
| `gestor` | responsable gestion | cliente, CG | si | ranking/filtro |
| `supervisor` | responsable supervision | cliente | si | ranking/filtro |
| `modalidad` | modalidad operativa | reposicion, CG | si | filtro |
| `semana_inicio` | inicio semana | CG | si | filtro central |
| `semana_iso` | semana ISO | CG | recomendado | auditoria |
| `VISITA` | visitas exigidas | CG | si | plan semanal |
| `VISITA_REALIZADA_RAW` | evidencia reportada | CG v2 | si | auditoria |
| `VISITA_REALIZADA_CAP` | evidencia valida capada | CG v2 | si | KPI cumplimiento |
| `VISITAS_PENDIENTES` | plan pendiente | CG v2 | si | alerta |
| `fuente_evidencia` | KPIONE2/POWER_APP/KPIONE1 | CG | si | trazabilidad |
| `ALERTA` | cumple/incumple/estado | CG | si | filtros y KPI |
| flags diarios `LUN..DOM` | plan/evidencia diaria | CG v2 | si | matriz diaria |
| flags conflicto | doble/triple/fuera cruce | CG audit | si | evitar inflacion |

## Matriz objeto actual a alternativa

| objeto_actual | proposito | dominio | criticidad | alternativa para informatica |
|---|---|---|---|---|
| `public.v_stock_local_cliente_ux` | stock UX, KPIs, tabla SKU | reposicion | alta | endpoint/mart stock filtrable |
| `public.ruta_rutero` | ruta, local, modalidad, RR | reposicion | alta | tabla maestra ruta versionada |
| `public.v_locales_home` | selector local | LOCAL | media | endpoint locales |
| `public.v_selector_modalidad` | selector modalidad | MERCADERISTA | media | catalogo modalidad |
| `public.v_selector_rutero_reponedor_modalidad` | selector RR por modalidad | MERCADERISTA | media | endpoint RR |
| `public.v_locales_por_modalidad_rutero` | locales por RR | MERCADERISTA | alta | endpoint locales ruta |
| `public.mv_scope_fact_latest_cliente` | foto latest cliente | Cliente | alta | mart latest cliente |
| `public.v_scope_cliente_responsable_fact_bridge` | puente responsable-stock | Cliente | alta | vista normalizada |
| `public.v_scope_cliente_responsable_rr_distinct` | RR/responsables distintos | Cliente | alta | dimension responsable-ruta |
| `public.v_scope_cliente_responsable_summary` | agregados scope | Cliente | alta | summary materializado |
| `public.mv_cliente_scope_inventory_enriched` | inventario enriquecido | Cliente | alta | mart inventario |
| `public.mv_cliente_scope_ranking_cliente` | ranking cliente | Cliente | media | query agregada |
| `public.mv_cliente_scope_ranking_responsable` | ranking responsable | Cliente | media | query agregada |
| `cg_mart.v_cg_out_weekly_v2` / `mv_cg_out_weekly_v2` | weekly mart principal | CG v2 | critica | mart semanal equivalente |
| `cg_core.v_cg_visita_dia_resuelta_v2` | evidencia diaria resuelta | CG v2 | critica | endpoint daily evidence |
| `cg_core.v_rr_frecuencia_base_resuelta_v2` | frecuencia planificada | CG v2 | alta | modelo frecuencia |
| audit views `cg_mart.v_cg_*` | duplicados, multifuente, fuera cruce | CG audit | alta | audit marts |

## Endpoints sugeridos no obligatorios

- `GET /stock/selectores/locales`
- `GET /stock/kpis`
- `GET /stock/items`
- `GET /stock/export/inventario`
- `GET /cliente/scope/kpis`
- `GET /cliente/scope/rankings`
- `GET /cliente/scope/detalle-sku`
- `GET /cg/v2/weeks`
- `GET /cg/v2/kpis`
- `GET /cg/v2/daily-matrix`
- `GET /cg/v2/audit`
- `GET /cg/v2/export`

No se entrega SQL inventado. Los objetos sin definicion completa quedan listados en `09_SQL_PENDIENTE_DE_EVIDENCIA.md`.
