# SQL Pendiente de Evidencia

Estos objetos son requeridos por la app activa o por el contrato funcional, pero no tienen definicion completa disponible en el evidence packet incluido. No se debe inventar DDL. La accion requerida es obtener evidencia SQL o definir equivalentes con pruebas.

| objeto | dominio | por que importa | criticidad | evidencia actual | accion requerida |
|---|---|---|---|---|---|
| `public.v_stock_local_cliente_ux` | Reposicion | fuente de stock, venta, flags y tabla SKU | alta | referenciado en `app/db.py` como `RESULT_VIEW` | obtener definicion o contrato backend equivalente |
| `public.ruta_rutero` | Reposicion/Cliente | maestro ruta, local, cliente, RR, modalidad | alta | referenciado como `RUTA_TABLE` | obtener definicion/estructura/versionado |
| `public.v_locales_home` | LOCAL | selector locales | media | referenciado en `get_locales_home` | evidenciar columnas |
| `public.v_selector_modalidad` | MERCADERISTA | selector modalidad | media | referenciado en `get_modalidades_home` | evidenciar columnas |
| `public.v_selector_rutero_reponedor_modalidad` | MERCADERISTA | selector rutero-reponedor | media | referenciado en `get_rutero_reponedor_por_modalidad` | evidenciar columnas |
| `public.v_locales_por_modalidad_rutero` | MERCADERISTA | locales por modalidad/RR | alta | referenciado en `get_locales_por_modalidad_rr` | evidenciar columnas |
| `public.mv_cliente_scope_inventory_enriched` | Cliente | inventario enriquecido/ranking/export | alta | referenciado en `app/db.py` | obtener definicion |
| `public.mv_cliente_scope_ranking_cliente` | Cliente | ranking cliente | media | referenciado en `app/db.py` | obtener definicion |
| `public.mv_cliente_scope_ranking_responsable` | Cliente | ranking responsable | media | referenciado en `app/db.py` | obtener definicion |
| `cg_mart.v_cg_out_weekly_v2` | CG v2 | weekly mart principal | critica | referenciado como `CG_V2_OUT_WEEKLY_VIEW` | obtener definicion o MV equivalente |
| `cg_mart.mv_cg_out_weekly_v2` | CG v2 | alternativa materializada weekly mart | critica | detectado por funcion que admite MV | confirmar objeto vigente |
| `cg_core.v_cg_visita_dia_resuelta_v2` | CG v2 | evidencia diaria resuelta | critica | referenciado en daily evidence | obtener definicion |
| `cg_core.v_rr_frecuencia_base_resuelta_v2` | CG v2 | frecuencia base resuelta | alta | referenciado en contrato v2 | obtener definicion |
| `cg_mart.v_cg_marcaje_multifuente_dia_v2` | CG audit | doble/triple/multifuente | alta | referenciado en audit summary | obtener definicion |
| `cg_mart.v_cg_ruta_duplicados_auditoria_v2` | CG audit | duplicados ruta | alta | referenciado en audit summary | obtener definicion |
| `cg_mart.v_cg_fuera_cruce_real_v2` | CG audit | evidencia fuera cruce | alta | referenciado en audit summary | obtener definicion |
| `cg_mart.v_cg_sin_batch_ruta_semana_v2` | CG audit | contexto historico sin batch | media | referenciado en audit summary | obtener definicion |

## Objetos con evidencia parcial en packet

El packet incluye inventario/columnas/definiciones para:

- `public.mv_scope_fact_latest_cliente`.
- `public.v_scope_cliente_responsable_fact_bridge`.
- `public.v_scope_cliente_responsable_rr_distinct`.
- `public.v_scope_cliente_responsable_summary`.

El packet descubre objetos CG public, pero no entrega definiciones completas de todos ellos en este paquete.
