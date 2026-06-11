# CLAUDE_C003_PRODUCT_EVOLUTION_REV2
**Agente:** Claude (claude-opus-4-8)
**Tipo:** research_synthesis (no mueve ni implementa código)
**Fecha:** 2026-06-11
**Baseline runtime:** `2c135a7be1d813a2c914f43b26b0579504a7f8a5`
**Entrada decisiva:** `research/C002_B0_PARITY_ROOTCAUSE.json` (G0 ABIERTO, root cause CONFIRMADO)
**Salidas estructuradas:** `research/AI_PROJECT_HORIZON_PROPOSAL.json`, `research/AI_CAPABILITY_MAP_PROPOSAL.json`
**Autorización:** ninguna propuesta autoriza implementación. `db_access: none`.

---

## 0. El hallazgo que reescribe la estrategia (B0)

C002 B0 demostró que **Control Gestión no es reproducible desde "último raw" + "última ruta"**. El excedente
diario (13,470 claves extra sobre 2026-04-27..2026-05-10) vino de reconstruir desde snapshots raw acumulativos
sin declarar la ventana productiva (`fecha_visita >= 2026-05-11`). Filtrar por esa ventana eliminó las 13,470
claves extra (de 34,817 a 21,347 filas).

Conclusión de producto: **cualquier capacidad reutilizable de STOCK_ZERO debe tratar el tiempo como un contrato
explícito**, no como un default. Esto convierte seis declaraciones temporales en capacidades de primera clase:
`affected_date_window`, `raw_batch_set`, `route_snapshot_id/version`, `source_precedence_version`,
`build_version`, y `daily_fact_staged` para construir el weekly.

Este informe propone el horizonte vivo (H0-H5) y el mapa de capacidades sobre esa base, respetando los 10
principios y el principio B0. No se propone segundo repositorio ni big-bang.

---

## 1. Qué permanece en STOCK_ZERO legacy estable

STOCK_ZERO **no se elimina** y **deja de crecer horizontalmente** (principios 1 y 2). Permanece como
consumidor estable:

- **`presentation_streamlit`** (`app/Home.py`, `app/screens/*`, `streamlit_app.py`, `app/exports.py`) — `KEEP_LEGACY`.
- **`inventory_ingestion`** (`scripts/load_fact_from_excel.py`) — `KEEP_LEGACY`, dominio independiente.
- **`route_master_ingestion`** (`scripts/load_ruta_rutero_from_excel.py`) — `KEEP_LEGACY`.
- **`readonly_extraction`** (`scripts/cg_readonly_extract.py`) y **`laboratory_reproducibility`** — `KEEP_LEGACY` (bus de conocimiento + lab).

El B3 fallback **permanece** y solo se hace observable; no se elimina en la primera etapa.

---

## 2. Qué capacidades se extraen

Marcadas `EXTRACT_DOMAIN` / `EXTRACT_ADAPTER` en el mapa, todas detrás de contratos temporales:

| Capacidad | Dominio | target_state | Reuso |
|---|---|---|---|
| `temporal_window_resolution` | TEMPORAL | EXTRACT_DOMAIN | HIGH |
| `raw_batch_selection` | CG | EXTRACT_DOMAIN | HIGH |
| `route_snapshot_versioning` | CG | EXTRACT_DOMAIN | HIGH |
| `source_precedence` | CG | EXTRACT_DOMAIN | HIGH |
| `parity_and_reconstruction` | TEMPORAL | EXTRACT_DOMAIN | HIGH |
| `daily_fact_builder` | CG | EXTRACT_DOMAIN | MEDIUM |
| `weekly_fact_builder` | CG | EXTRACT_DOMAIN | MEDIUM |
| `cg_raw_ingestion_kpione2_adapter` | CG | EXTRACT_ADAPTER | MEDIUM |
| `inventory_read_api` / `cg_read_api` | INV / CG | EXTRACT_DOMAIN | MEDIUM |

`inventory_derivation_offload` queda `REWRITE_LATER` (tras la paridad COD_RT de C002 Programa A).

---

## 3. Qué contratos temporales deben preceder la extracción

**Ninguna capacidad CG se extrae antes de que estos seis contratos sean inputs versionados (H1):**

1. **affected_date_window** — la ventana que el build cubre (p.ej. `>= 2026-05-11`); nunca inferida del último raw.
2. **raw_batch_set por fuente** — KPIONE 19, KPIONE2 38, POWER_APP 26 son acumulativos/mixtos; el batch no acota fechas, la ventana sí.
3. **route_snapshot_id/version** — para 2026-06-01 el baseline corresponde a `ruta_batch 16/17`, no a `18`.
4. **source_precedence_version** — KPIONE2 > POWER_APP > KPIONE1 (audit-only), llave de día `[semana_inicio, fecha_visita, cod_rt, cliente_norm]`.
5. **build_version** — identificador de la receta de reconstrucción.
6. **daily_fact_staged** — el weekly se construye desde el daily staged, **no** recomputando event_scope desde raw (causa del timeout de 900s).

---

## 4. Cómo separar Inventario y Control Gestión

Son **dominios independientes** (principio 3) y ya tienen loaders separados. La separación es a nivel de
**interfaz de capacidad**, dentro del mismo repo (sin repo nuevo):

- **Inventario:** `fact_stock_venta`, `ruta_rutero` (selectores), MVs cliente; key `(fecha, cod_rt, sku, marca)`; contrato temporal mínimo (`fecha` + `v_data_version`).
- **Control Gestión:** `cg_raw`/`cg_core`/`cg_mart`; contrato temporal completo (los seis).
- **Punto de entanglement:** ambos viven hoy dentro de `app/db.py` (5,348 líneas) y comparten `ruta_rutero`. La separación (H2) exige primero el runtime smoke de C002 Programa C para confirmar funciones static-only antes de mover nada.
- `route_master_ingestion` es compartido (selectores de Inventario + resolución de ruta CG): se mantiene legacy y se expone como dependencia, no se duplica.

---

## 5. Qué capacidades se pueden comercializar

Las capacidades **genéricas de contrato temporal** son las de mayor potencial de reuso fuera de Retail Trust
(reuse_potential HIGH): `temporal_window_resolution`, `raw_batch_selection`, `route_snapshot_versioning`,
`source_precedence`, `parity_and_reconstruction`, más el patrón `laboratory_reproducibility`/`readonly_extraction`.

En conjunto forman un **motor de reconstrucción determinística sobre fuentes acumulativas** — un problema común
en pipelines retail/operacionales. La decisión de productizar se difiere a **H5** (decisión de Bastián), sin
proponer repo nuevo ni big-bang.

---

## 6. Qué es específico de Retail Trust

`KEEP_LEGACY` o específico de dominio, no generalizable tal cual:

- Las **reglas de precedencia concretas** y las **etiquetas de fuente** (KPIONE / KPIONE2 / POWER_APP).
- El **esquema del maestro de ruta** (`ruta_rutero`) y la semántica `COD KPI ONE`.
- Los **layouts de workbook** (`DB_GLOBAL_INVENTARIO.xlsx`, `CUMPLIMIENTO_FRECUENCIA.xlsx`), flags `FOCO_CLIENTE`, columna `duplicados`.
- La **UI Streamlit** y las pantallas (CLIENTE / CONTROL GESTION / LOCAL / MERCADERISTA).
- El campo **SP Item ID** payload-only de KPIONE2 (detalle de fuente).

La línea divisoria: el *motor temporal* es genérico; las *reglas, esquemas y UI* son de Retail Trust.

---

## 7. Cómo migrar sin big-bang

Migración incremental, horizonte por horizonte, cada paso reversible y gated:

1. **H0** cierra la paridad y declara los contratos (solo Codex/Bastián, sin tocar producto).
2. **H1** promueve los contratos a config versionada + catálogo DB autoritativo.
3. **H2** establece la costura de dominio (interfaces, no movimiento de código) tras el runtime smoke.
4. **H3** formaliza KPIONE2 y hermanos como adapters detrás de `source_precedence` (payload_json se conserva).
5. **H4** expone una capa de capacidad headless; Streamlit pasa a ser **un** consumidor (principio 7).
6. **H5** punto de decisión de productización (sin repo nuevo, sin big-bang).

Cada capacidad solo se extrae cuando sus `extraction_prerequisites` y `tests_required` están satisfechos.

---

## 8. Qué papel cumple Docker durante H0-H2

Docker/`local_pg` (`stock_zero_cg_parity_pg`, PostgreSQL 17.10, `127.0.0.1:55432`) es **laboratorio
reproducible, no respaldo** (principio 5):

- **H0:** entorno donde se cierra la paridad (missing key, 56 diferencias, ruta, weekly desde daily staged).
- **H1:** valida que los contratos declarados reproducen la paridad cerrada y soporta el catálogo DB.
- **H2:** valida las costuras de dominio contra la paridad cerrada **sin** escribir en Supabase.

En todo momento el lab se reconstruye desde git + manifiestos de evidencia (9C7A3/9C7A4, 0 hash mismatches),
nunca se usa como fuente de verdad persistente.

---

## 9. Cuándo se puede eliminar local_pg

Criterio propuesto (H0.5, requiere decisión de Bastián): `local_pg` puede destruirse **solo** cuando:

1. **G0 está cerrado** y la paridad es reproducible desde una receta versionada en git (con `build_version`).
2. Los **seis contratos temporales** están declarados como data/config bajo control de versiones.
3. Los **manifiestos de evidencia** (9C7A3/9C7A4) siguen siendo la fuente reconstruible, de modo que el contenedor se **regenera on-demand** en vez de preservarse.

Estimación: viable hacia el final de **H4**, cuando reproducibilidad = git + evidencia, no contenedor vivo.
Mientras esos tres no se cumplan, destruir `local_pg` perdería el único entorno donde la paridad es verificable.

---

## 10. Qué debe validar Codex

1. **H0.1** — causa de la única clave KPIONE2 faltante en 2026-05-15 (precedencia / normalización / borde de ventana).
2. **H0.2/H0.3** — las 56 diferencias de valor en la semana 2026-06-01 y la **política autoritativa de route_snapshot** (16/17 vs 18).
3. **H0.4** — reconstrucción del weekly desde daily staged bajo cota de tiempo (sin recomputar desde raw).
4. **H1** — catálogo DB autoritativo con consumidores de `payload_json`/`row_hash` a nivel columna; clasificación raw necesario/reconstruible/historico.
5. **Mapa de capacidades** — verificar `current_files`, `temporal_contract` y `target_state` de cada capacidad contra el código y el grafo de dependencias real.
6. Confirmar que `payload_json` sigue siendo superficie canónica (no removible) y que `ruta_rutero_load_rows` es dependencia viva.

---

## 11. Qué decisiones debe tomar Bastián

1. **Política de route_snapshot** para semanas con divergencia (16/17 vs 18) — bloquea H0.3.
2. **Criterio de destrucción de Docker/local_pg** (H0.5).
3. **Separar Inventario y Control Gestión** como dominios independientes a nivel de interfaz (H2) — sí/no y alcance.
4. **Fuente histórica autoritativa** de stock/ventas (heredado de C002; `fact_stock_venta` no cubre el rango de `DB_GLOBAL_HISTORICO`).
5. **Hojas CG manuales** (`DESARROLLO_CONTROL_GESTION`, `OUT`) y columna `duplicados`: ¿uso operacional vigente? (heredado de C002).
6. **Productización** (H5): ¿se exploran las capacidades genéricas como producto? Sin repo nuevo ni big-bang.

---

## 12. Veredicto

**READY_FOR_CODEX_VALIDATION.** Seis horizontes (H0-H5) y 16 capacidades mapeadas, todas ancladas en evidencia
validada (C001 Codex matrix, C002 portfolio, C002 B0 root cause). La estrategia respeta los 10 principios y el
principio B0: el tiempo es contrato explícito; STOCK_ZERO permanece estable y deja de crecer horizontalmente;
Inventario y Control Gestión se separan a nivel de capacidad; KPIONE2 es un adapter; Docker es lab; Git es el
bus; nada depende obligatoriamente de Streamlit; sin repo nuevo; sin big-bang; sin implementación.

*Este informe es síntesis de investigación. No autoriza cambios de runtime, loader, SQL, DB, Docker, datos ni evidencia.*
