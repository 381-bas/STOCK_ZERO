# 014B — KPIONE Raw Ingestion Direction Synthesis

## Estado

014B revisó adversarialmente el modelo de ingesta de Control Gestión para reemplazar progresivamente el maestro manual `CUMPLIMIENTO_FRECUENCIA.xlsx` por exports raw KPIONE `photo-excel-admin_*.xlsx`.

Commit base de research:

`cba28ac docs(research): review 014B KPIONE raw ingestion model`

## Veredicto

`PROMOTE_RAW_EXPORTS_AS_NEW_INPUT`

Este veredicto aplica como dirección de modelo local/no-apply. No autoriza writes productivos, SQL apply, DDL, data movement, refresh productivo, cambios UX ni modificación de loaders productivos.

## Hallazgos que cambian el plan

### FACT 1 — El maestro manual es raw KPIONE + derivadas

`CUMPLIMIENTO_FRECUENCIA.xlsx`, hoja `DB (KPIONE2.0)`, no debe tratarse como una fuente distinta. Es el resultado manual de consolidar exports KPIONE raw y agregar columnas derivadas.

En el solapamiento de 2026-06-01, 1.220 de 1.221 IDs del maestro aparecen en raw export, aproximadamente 99,7%.

Decisión:

- El maestro queda como artefacto legacy/read-only.
- Los raw exports pasan a ser la nueva dirección de input.
- El modelo futuro debe reemplazar el paste manual por validación y normalización testeable.

### FACT 2 — VISITA es semántica Excel, no dato KPIONE

`VISITA` no viene desde KPIONE como campo fuente. Es una fórmula del maestro manual:

`VISITA = IFERROR(1/COUNTIFS(Codigo Local, Fecha, Marca), 1/COUNTIFS(Local, Fecha, Marca))`

Esto define presencia diaria fraccional por local/día/marca, con fallback por nombre de local cuando `Codigo Local` falla.

Decisión:

- La semántica de `VISITA` debe replicarse en código testeable antes de tocar DB/mart.
- El fallback por `Local` debe tratarse como decisión explícita, no accidente.
- 014C debe incluir test golden para `VISITA`, incluyendo caso con `Codigo Local` vacío.

### FACT 3 — Los exports raw no son batches equivalentes

Los 6 archivos `photo-excel-admin_*.xlsx` no equivalen a 6 batches limpios. La evidencia sugiere:

- ventanas manuales;
- posible truncamiento por archivo cercano a 50.000 filas;
- parches de días específicos;
- cero solapamiento de IDs en la muestra actual;
- cobertura real junio solo hasta 2026-06-24.

Decisión:

- `source_file_id` se extrae desde el nombre del archivo, pero no debe usarse como `batch_id`.
- El batch futuro debe declararse como ventana + set de source_file_ids + sha256s.
- 014C debe implementar gate de truncamiento para filas >= 50.000.
- Junio no puede declararse completo hasta conseguir o documentar 2026-06-25..2026-06-30.

## Modelo operativo aprobado para avanzar

### Mantener

- App actual intacta.
- Supabase intacto.
- Loader v17 intacto.
- `CUMPLIMIENTO_FRECUENCIA.xlsx` intacto como evidencia legacy/read-only.
- Stash UX CG V2 Risk Digest sin aplicar.

### Promover como dirección

- `photo-excel-admin_*.xlsx` como input raw KPIONE candidato.
- Validadores locales no-apply.
- Manifiestos ligeros versionables.
- Evidencia pesada fuera de Git, salvo resumen/manifest pequeño.

### Romper progresivamente

- Dependencia manual del maestro `CUMPLIMIENTO_FRECUENCIA.xlsx`.
- Supuesto `1 descarga = 1 semana completa`.
- Fórmula Excel como dueño oculto de semántica `VISITA`.
- Hojas fuera de contrato como parte del modelo futuro.

### No tocar todavía

- Supabase.
- SQL apply.
- DDL.
- `scripts/load_control_gestion_raw_v17.py`.
- `app/db.py`.
- UX activa.
- Mart/vistas activas.
- Movimiento de archivos.
- Limpieza/corrección del maestro legacy.

## Preguntas operativas pendientes

### P1 — Origen del nivel evento

Por lo explicado por Bastián, el maestro manual fue construido juntando archivos descargados desde KPIONE. La hipótesis vigente es:

`CUMPLIMIENTO_FRECUENCIA.xlsx = consolidación manual de raw exports + columnas derivadas`

014C debe validar si existe diferencia real entre nivel foto y nivel evento, sin asumir que KPIONE entregaba un export distinto.

### P2 — Límite 50.000 filas

El archivo con 50.001 filas sugiere cap/truncamiento de KPIONE. Hasta confirmarlo, 014C debe tratarlo como riesgo bloqueante local:

`TRUNCATION_SUSPECT`

## 014C — Contrato de implementación

Nombre:

`014C_KPIONE_RAW_EXPORT_VALIDATOR_NO_APPLY`

Objetivo:

Construir y ejecutar un validador local/no-apply que demuestre si los exports raw KPIONE pueden transformarse en evento canónico y reemplazar progresivamente el maestro manual.

Debe producir:

1. Manifest por archivo:
   - source_file_id
   - source_file_name
   - sha256
   - row_count
   - distinct_id_count
   - fecha_min
   - fecha_max
   - truncation_suspect
2. Matriz de solapamiento de IDs entre archivos.
3. Clasificación de duplicados:
   - same_id_same_hash = dedupe silencioso
   - same_id_diff_hash = conflicto bloqueante
4. Unión canónica local.
5. Campos mínimos:
   - event_id
   - sp_item_id
   - cod_rt
   - local_nombre
   - cliente_norm
   - fecha
   - week_start
   - reponedor
   - tipo_tarea
   - link_foto
   - event_stable_hash
   - source_file_id
   - source_file_sha256
   - source_row_number
6. Réplica local de `VISITA`.
7. Test golden de `VISITA`, incluyendo fallback por local_nombre.
8. Paridad 2026-06-01 contra maestro:
   - IDs raw vs maestro
   - tasa de match esperada >= 99%
   - deltas explicados
9. Cobertura junio:
   - días cubiertos
   - días faltantes
   - semanas operativas afectadas
10. Veredicto:
   - RAW_EXPORTS_READY_FOR_DRY_RUN_LOADER
   - RAW_EXPORTS_PARTIAL_NEEDS_MORE_EXPORTS
   - RAW_EXPORTS_BLOCKED_BY_TRUNCATION_OR_CONFLICT
   - RAW_EXPORTS_BLOCKED_BY_SCHEMA

## Criterio de cierre 014C

014C cierra OK si:

- no toca Supabase;
- no toca loader productivo;
- no mueve archivos;
- no aplica stash;
- genera JSON/MD ligero versionable en `research/014C_KPIONE_RAW_EXPORT_VALIDATOR_NO_APPLY/`;
- genera evidencia pesada solo en `evidence/` si aplica;
- muestra si raw exports pueden pasar a loader dry-run en 014D/014E.

## Decisión sobre evidencia

La evidencia pesada no se versiona por defecto.

Sí se puede versionar:

- manifest ligero;
- resumen de cobertura;
- matriz de solapamiento agregada;
- reporte de paridad resumido;
- test golden pequeño.

No se versiona:

- dumps completos;
- comparaciones fila a fila;
- Excel raw;
- payloads históricos pesados.

## Cierre

014B no autoriza ejecución productiva. Autoriza avanzar a 014C con Codex para crear un validador no-apply sobre raw exports KPIONE.

La dirección de modelo cambia:

`legacy manual master -> raw KPIONE exports -> canonical local events -> future dry-run loader -> future authorized DB path`
