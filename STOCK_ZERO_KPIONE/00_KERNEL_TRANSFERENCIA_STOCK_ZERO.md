# Kernel de Transferencia STOCK_ZERO

## Proposito

Este paquete prepara la transferencia tecnico-funcional de STOCK_ZERO / GESTIONZERO al equipo de informatica. La entrega sirve para entender y replicar la logica funcional en una aplicacion y arquitectura propia.

## Alcance

Esta entrega no es un clon obligatorio de Streamlit, no impone Supabase como arquitectura obligatoria y no reemplaza el diseno tecnico de informatica. La carpeta `reference_app/app_active_only` es evidencia tecnica de UX, filtros, consultas, consumo de datos y exportaciones.

Informatica debe preservar reglas, contratos y flujos. Puede adaptar interfaz, framework, backend, base de datos, jobs y servicios, siempre que los resultados funcionales se mantengan consistentes.

## Capas

| capa | responsabilidad | puede adaptarse | debe preservarse |
|---|---|---|---|
| UX | navegacion, filtros, tablas, paginacion, botones | si | nombres funcionales, secuencia de filtros, estados esperados |
| Backend/API | endpoints, controladores, validacion de parametros | si | contratos de entrada/salida y semantica de filtros |
| SQL/modelo | reglas criticas, normalizacion, agregaciones, precedencia de fuentes | si, con evidencia | calculos, claves, no duplicidad, trazabilidad |
| Export | archivos Excel/PDF y hojas | si | columnas minimas, scope filtrado/global, advertencias |

## Principios duros

- La logica critica no debe vivir en el frontend.
- Normalizar claves como `cod_rt`, `cliente_norm`, `marca_norm`, responsable normalizado y fuente.
- Evitar inflar KPIs por duplicidades, doble/triple marcaje o multiples fuentes.
- Mantener trazabilidad de evidencia: fuente, fecha, semana y regla aplicada.
- Mostrar freshness de datos: fecha stock, `fecha_datos`, `ingested_at` o equivalente.
- Las exportaciones deben ser consistentes con filtros visibles o declarar cuando son globales.
- Las brechas SQL sin definicion completa deben cerrarse con evidencia antes de implementar productivo.

## Fuentes incluidas

- Codigo activo vigente: `reference_app/app_active_only`.
- Codigo historico opcional: `reference_app/app_historical_optional/v_i`.
- Evidence packet SQL: `evidence/STOCK_ZERO_SUPABASE_EVIDENCE_PACKET_V3_1_B3_CONTROL_GESTION_SQL_20260514_1605.json`.

El contenido historico/checkpoint no es contrato vigente.
