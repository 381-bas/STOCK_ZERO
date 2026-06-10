# CLAUDE_C001_SYSTEM_UNDERSTANDING
**Agente:** Claude (claude-sonnet-4-6)  
**Fase:** CLAUDE_C001_SYSTEM_UNDERSTANDING  
**Fecha:** 2026-06-09  
**Baseline commit:** `2c135a7be1d813a2c914f43b26b0579504a7f8a5`  
**Hallazgos nuevos:** 30 PROPOSED en `research/AI_FINDINGS_LEDGER.jsonl`  
**Autorización:** research_write — solo `AI_FINDINGS_LEDGER.jsonl` y este informe

---

## A. Modelo de extremo a extremo

### Cadena de entrada
```
streamlit_app.py
  └── app/Home.py → main()
        ├── imports diferidos (db, exports, screens) — dentro de main(), no a nivel módulo
        ├── app/screens/control_gestion.py  →  app/db.py  →  Supabase (cg_mart.*, public.*)
        ├── app/screens/cliente.py          →  app/db.py  →  Supabase (public.*)
        └── app/screens/reposicion.py       →  app/services/stock.py  →  app/db.py  →  Supabase
```

### Rutas de escritura (loaders)
| Script | Origen | Destino DB |
|---|---|---|
| `load_fact_from_excel.py` | `DB_GLOBAL_INVENTARIO.xlsx` → hoja BASE | `public.fact_stock_venta` |
| `load_ruta_rutero_from_excel.py` | `DB_GLOBAL_INVENTARIO.xlsx` → hoja RUTA_RUTERO | `public.ruta_rutero`, `cg_core.ruta_rutero_load_rows`, `cg_core.ruta_rutero_load_batch` |
| `load_control_gestion_raw_v17.py` | `CUMPLIMIENTO_FRECUENCIA.xlsx` → 3 hojas | `cg_raw.kpione_raw`, `cg_raw.kpione2_raw`, `cg_raw.power_app_raw`, `cg_audit.batch_registry` |
| `refresh_control_gestion_v2_incremental.py` | `cg_core.v_cg_visita_dia_precedencia_v2` | `cg_mart.fact_cg_visita_dia_resuelta_v2`, `cg_mart.fact_cg_out_weekly_v2`, `cg_mart.mv_cg_out_weekly_v2` |

### Capas de caché
- `st.cache_resource`: motor SQLAlchemy (singleton de proceso)
- `st.cache_data(TTL=180s)`: consultas y selectores
- `st.cache_data(TTL=600s)`: selectores RUTA_RUTERO
- `st.cache_data(TTL=300s)`: resolución URL activa de DB
- `st.cache_data(TTL=60s)`: versión de datos (DV)
- Invalidación manual (`_invalidate_runtime_cache`): solo `st.session_state` — **no** limpia `st.cache_data/resource`

### Esquemas Supabase (inferidos del código)
```
public       → fact_stock_venta, ruta_rutero, ~8 views/MVs de stock y cliente
cg_raw       → kpione_raw, kpione2_raw, power_app_raw  (write-only desde la app)
cg_core      → vistas de ruteo y historial de cargas  (leídas por refresh script)
cg_mart      → fact_cg_visita_dia_resuelta_v2, fact_cg_out_weekly_v2, mv_cg_out_weekly_v2  (leídas por app)
cg_audit     → batch_registry  (write-only desde la app)
```

---

## B. Linaje de las 3 fuentes de datos limpias

### B1. DB_GLOBAL_INVENTARIO.xlsx — fuente de inventario

```
Reporte externo retail (datos fuente)
  └── paste manual cols A-H  →  BASE[CADENA, FECHA, MARCA, SKU, DESC, N_LOCAL, VTA, INV]
RUTA_RUTERO (3,568 filas, Power Query)
  └── XLOOKUP triple  →  BASE[COD_RT]   ← ~155M comparaciones por recálculo
  └── VLOOKUP simple  →  BASE[NOMBRE_LOCAL_RR]
FOCO_CLIENTE (848 filas, manual)
  └── XLOOKUP cuádruple  →  BASE[OTROS]  ← ~2% hit rate
COUNTIFS auto-referencia  →  BASE[duplicados]  ← REDUNDANTE: el loader ya deduplica
  ↓
Excel recalcula + guarda  (riesgo: loader lee cache, no fórmulas en vivo)
  ↓
load_fact_from_excel.py → public.fact_stock_venta
```

**Columnas fórmula vs directas:**
- 8 columnas SOURCE_DIRECT (pegadas del informe externo)
- 4 columnas FORMULA_DERIVED (COD_RT, NOMBRE_LOCAL_RR, OTROS, duplicados)
- 174,540 celdas fórmula × recálculo completo en cada apertura del workbook

**Riesgo crítico:** Si el workbook se guarda sin recálculo actualizado, el loader ingesta valores de período anterior sin advertencia. No existe validación pre-carga de frescura de fórmulas.

### B2. CUMPLIMIENTO_FRECUENCIA.xlsx — fuente de Control Gestión

```
DB (KPIONE)      → 75,124 rows × 22 cols  ← cargado a cg_raw.kpione_raw
DB (KPIONE2.0)   → 45,736 rows × 18 cols  ← cargado a cg_raw.kpione2_raw
DB (POWER_APP)   →  5,497 rows × 24 cols  ← cargado a cg_raw.power_app_raw
DESARROLLO_CONTROL_GESTION → 3,439 × 71  ← NO consumido por loader
OUT              → 3,439 × 34             ← NO consumido por loader (probable precursor manual)
Tabla1           → 3,469 rows             ← copia de RUTA_RUTERO (no consumida)
gg               → 1 row
```

**Protección contra recargas:** SHA256 del sheet completo almacenado en `cg_audit.batch_registry.notes`. Si el hash coincide con la carga anterior, el sheet se omite completo. Mecanismo anti-bloat efectivo.

**Precedencia de fuentes (KPIONE2 > POWER_APP > KPIONE1):** KPIONE2 es la fuente primaria activa; KPIONE1 tiene rol de auditoría únicamente.

### B3. DB_GLOBAL_INVENTARIO.xlsx — hoja RUTA_RUTERO

```
RUTA_RUTERO (3,568 filas, 30 columnas)
  ↓
load_ruta_rutero_from_excel.py
  ├── UPSERT → public.ruta_rutero            (maestro activo)
  ├── INSERT → cg_core.ruta_rutero_load_rows (historial completo + payload_json)
  └── INSERT → cg_core.ruta_rutero_load_batch (metadata de batch)
```

**row_hash:** MD5 de 29 campos de negocio almacenado en public.ruta_rutero y ruta_rutero_load_rows. El propio código lo documenta como "fingerprint negocio: útil para análisis; NO es llave de UPSERT". No se encontró ningún consumidor en app/db.py, screens, ni services.

---

## C. Pasos manuales y fórmulas automatizables

### Pasos manuales actuales (carga semanal de inventario)
1. Obtener reporte semanal de cadena retail (externo)
2. Pegar filas A-H en BASE
3. Actualizar RUTA_RUTERO si hay locales nuevos
4. Actualizar FOCO_CLIENTE si hay nuevos flags de foco
5. Esperar recálculo de fórmulas Excel (10-60 seg para COD_RT XLOOKUP × 43,635 filas)
6. Guardar workbook
7. Ejecutar loader

**Pasos 3-6 pueden introducir errores silenciosos si se omiten.** El loader no tiene validación de frescura entre el paso 6 y el paso 7.

### Candidatos para offload a Python

| Columna | Prioridad | Tabla de referencia | Bloqueadores antes de implementar |
|---|---|---|---|
| `COD_RT` | ALTA | `public.ruta_rutero` (ya en DB) | Compatibilidad de tipos N_LOCAL int vs COD_B2B; manejo de duplicados COD_KPI_ONE |
| `NOMBRE_LOCAL_RR` | MEDIA | `public.ruta_rutero` | Depende de COD_RT resuelto; bajo riesgo independiente |
| `OTROS` | BAJA | FOCO_CLIENTE (848 filas) | Necesita tabla FOCO_CLIENTE en DB; ~2% hit rate |
| `duplicados` | NINGUNA | — (redundante) | Solo eliminar del workbook; no requiere cambio en loader |

**Escenario de mayor ahorro / menor riesgo:** Offload COD_RT + NOMBRE_LOCAL_RR juntos (comparten la misma tabla de referencia). Elimina el riesgo de stale-formula-values para los campos más críticos del modelo CG.

**Metodología de paridad recomendada:**
1. Ejecutar loader actual (Excel-formula path) sobre período histórico conocido → CSV referencia
2. Implementar Python-compute en script separado (no en el loader productivo)
3. Comparar con `pd.testing.assert_frame_equal` sobre clave lógica `[fecha, cod_rt, sku, marca]`
4. Verificar: distribución COD_RT idéntica, NO_RT count igual, OTROS no-vacíos iguales
5. Solo después de paridad confirmada: proponer modificación al loader productivo

---

## D. Complejidad de loaders y DB

### Tamaño de scripts
| Script | Líneas | Funciones | Rol |
|---|---|---|---|
| `load_control_gestion_raw_v17.py` | 2,023 | 54 | CG raw → cg_raw (3 fuentes) |
| `refresh_control_gestion_v2_incremental.py` | 1,672 | 45 | cg_core → cg_mart (4 etapas) |
| `app/db.py` | 5,347 | 149 | Capa de acceso DB (todo el sistema) |
| `load_ruta_rutero_from_excel.py` | 720 | 18 | RUTA_RUTERO → 3 tablas DB |
| `load_fact_from_excel.py` | 702 | 19 | BASE → fact_stock_venta |

### Acumulación en tablas de historial
Dos patrones de acumulación sin retención definida:
1. **`cg_core.ruta_rutero_load_rows`:** +3,568 filas por carga de RUTA_RUTERO (cada vez que se ejecuta el loader), con `payload_json` completo por fila
2. **`cg_raw.*`:** acumula por carga de CG; el hash-skip evita duplicados pero no limpia cargas antiguas

### payload_json duplicación
- Presente en: `cg_raw.kpione_raw`, `cg_raw.kpione2_raw`, `cg_raw.power_app_raw`, `cg_core.ruta_rutero_load_rows`
- Impacto: ~2× el tamaño de escritura por fila (todos los campos en JSON + columnas individuales)
- Consumidores en app/db.py: **cero** (confirmado por grep)
- El mecanismo hash-skip en batch_registry ya provee auditoría a nivel de batch

### Funciones db.py sin callers estáticos
10 funciones confirmadas sin callers en ningún archivo Python del repo:

| Función | Grupo probable |
|---|---|
| `get_scope_clientes_rr` | Coverage audit (validado en shared memory) |
| `get_rr_stock_en_local` | Coverage audit (validado en shared memory) |
| `get_cobertura_local` | Coverage audit (validado en shared memory) |
| `get_clientes_sin_match_rr_stock` | Coverage audit (validado en shared memory) |
| `get_cg_v2_contract_smoke` | CG v2 introspection (nuevo) |
| `get_cg_v2_filter_options` | CG v2 introspection (nuevo) |
| `get_result_view_contract` | Feature stub abandonado (nuevo) |
| `get_mercaderistas_home` | Feature stub abandonado (nuevo) |
| `get_locales_por_mercaderista` | Feature stub abandonado (nuevo) |
| `get_tabla_ux_paginada` | Feature stub abandonado (nuevo) |

---

## E. JOINs y hashes que requieren justificación

### row_hash en RUTA_RUTERO
- **Qué es:** MD5 de 29 campos de negocio por fila
- **Dónde se almacena:** `public.ruta_rutero.row_hash` y `cg_core.ruta_rutero_load_rows.row_hash`
- **Consumidores confirmados en runtime:** ninguno (grep en app/ da cero resultados)
- **Documentación del propio código:** "fingerprint negocio: útil para análisis; NO es llave de UPSERT"
- **Pregunta para Codex:** ¿Existe algún query, stored procedure, o reporte externo que lea `row_hash`?

### JOINs en db.py (35 cláusulas)
Los 35 JOINs se distribuyen en funciones activas (mayormente CG v2 y stock/cliente). No se identificaron JOINs claramente muertos. Los JOINs en funciones zero-caller (grupo coverage-audit y stubs) son candidatos a eliminación si las funciones se confirman como inactivas.

### CTEs (15 patrones WITH)
Las 15 CTEs están en funciones activas del CG v2 stack (ranking gestores, ruteros, clientes comparten estructura `_cg_v2_global_ranking_parts`). Esta duplicación estructural fue registrada como PARTIAL debt en shared memory (CODEX-RUNTIME-0002-VALIDATION-006).

### XLOOKUP en Excel (no DB)
- `COD_RT`: triple-condición sobre 3,568 filas × 43,635 filas = ~155M comparaciones
- `OTROS`: cuádruple-condición sobre 848 filas × 43,635 filas = ~37M comparaciones
- Estos no son JOINs en DB pero tienen el mismo problema de escala y son precomputaciones que podrían moverse a Python.

---

## F. Portfolio de simplificaciones priorizado

### Prioridad 1 — Alta confianza, bajo riesgo, impacto inmediato

**R01: Eliminar columna `duplicados` de BASE**
- Acción: Eliminar la columna del workbook Excel (no del loader)
- Impacto: Elimina 43,635 fórmulas COUNTIFS redundantes; elimina warning del loader
- Requiere: Solo cambio manual en Excel; cero riesgo de datos
- Evidencia: `CLAUDE-C001-INVDATA-FACT-003`

**R02: Documentar flujo de 7 pasos con advertencia de frescura**
- Acción: Agregar pre-flight en `load_fact_from_excel.py` que compare mtime del workbook con ventana semanal esperada
- Impacto: Previene ingestión silenciosa de valores COD_RT stale
- Requiere: ~10 líneas en loader; autorización de Codex
- Evidencia: `CLAUDE-C001-INVDATA-FACT-004`, `CLAUDE-C001-MANUAL-FACT-001`

**R03: Documentar roles de hojas en CUMPLIMIENTO_FRECUENCIA.xlsx**
- Acción: Agregar README en `data/` listando qué hojas consume el loader y cuáles son análisis manual
- Impacto: Previene eliminación accidental de hojas fuente; clarifica mantenimiento
- Requiere: Solo documentación; cero riesgo
- Evidencia: `CLAUDE-C001-CGDATA-FACT-001`, `CLAUDE-C001-CGDATA-FACT-002`

### Prioridad 2 — Media confianza, requiere validación Codex

**R04: Establecer retención para `cg_core.ruta_rutero_load_rows`**
- Acción: Agregar policy de retención (ej. últimas 52 cargas)
- Impacto: Controla crecimiento indefinido de tabla de historial; reduce storage
- Requiere: Codex confirme conteo actual de filas y ausencia de consumidor
- Evidencia: `CLAUDE-C001-RUTA-FACT-002`, `CLAUDE-C001-RUTA-INF-001`

**R05: Establecer retención para `cg_raw.*`**
- Acción: Agregar policy de retención (ej. últimas 12 semanas) en kpione_raw, kpione2_raw, power_app_raw
- Impacto: Controla crecimiento del esquema de staging; no afecta app
- Requiere: Codex confirme conteo actual y ventana de acceso
- Evidencia: `CLAUDE-C001-SUPABASE-INF-001`

**R06: Agregar startup probe para CG_V2_* env vars**
- Acción: En `app/db.py` o llamado desde `Home.py main()`: verificar cada CG_V2_* resuelve a un objeto existente en `pg_catalog` antes del primer request
- Impacto: Detecta misconfiguraciones de env antes de que lleguen al usuario
- Requiere: ~20 líneas; autorización Codex
- Evidencia: `CLAUDE-C001-SUPABASE-FACT-002`

### Prioridad 3 — Requiere paridad test o validación profunda de Codex

**R07: Offload COD_RT + NOMBRE_LOCAL_RR a Python en loader**
- Acción: Calcular COD_RT en Python usando merge contra `public.ruta_rutero` al momento de carga
- Impacto: Elimina dependencia de Excel para los campos más críticos; elimina riesgo de stale-formula
- Requiere: Codex build parity test primero; verificar compatibilidad N_LOCAL vs COD_B2B
- Evidencia: `CLAUDE-C001-INVDATA-HYP-001`

**R08: Evaluar retiro de `payload_json` en cg_raw y ruta_rutero_load_rows**
- Acción: Eliminar columna payload_json si Codex confirma cero consumidores en DB
- Impacto: ~40-60% reducción de tamaño de escritura por fila en loaders CG y ruta
- Requiere: Codex confirme zero consumers en procedures, views, y reportes externos
- Evidencia: `CLAUDE-C001-LOADER-FACT-003`, `CLAUDE-C001-COMPLEXITY-HYP-002`

**R09: Confirmar y limpiar 5 funciones db.py nuevas zero-caller**
- Acción: Codex confirma ausencia de callers dinámicos; si confirmado, eliminar 5 nuevas funciones
- Impacto: Elimina ~250-350 líneas de código SQL muerto y sus dependencias de vistas
- Requiere: Codex valide cada una (reflection, __all__, getattr pattern)
- Evidencia: `CLAUDE-C001-DBPY-FACT-001`, `CLAUDE-C001-DBPY-INF-001`

**R10: Definir governance para DB_GLOBAL_HISTORICO.xlsx y DB_HISTORICA interna**
- Acción: Codex confirma cobertura de `public.fact_stock_venta`; si completa, proponer retiro de ambos archivos Excel de historial
- Impacto: Elimina 3-layer history problem; establece fact_stock_venta como única fuente de verdad histórica
- Requiere: Query de Codex: `SELECT MIN(fecha), MAX(fecha), COUNT(*) FROM public.fact_stock_venta`
- Evidencia: `CLAUDE-C001-COMPLEXITY-HYP-001`, `CLAUDE-C001-COMPLEXITY-INF-001`

---

## G. Preguntas directas para Codex

Las siguientes preguntas requieren acceso DB de lectura (DB_URL_CODEX) o inspección dinámica de callers. Claude no puede responderlas sin acceso a runtime.

### G1. Cobertura histórica de fact_stock_venta
```sql
SELECT MIN(fecha), MAX(fecha), COUNT(*) FROM public.fact_stock_venta;
```
**Contexto:** Determina si DB_GLOBAL_HISTORICO.xlsx y DB_HISTORICA interna son redundantes con el DB. Necesario para R10.

### G2. Crecimiento de cg_raw tables
```sql
SELECT 'kpione_raw' AS t, COUNT(*) FROM cg_raw.kpione_raw
UNION ALL SELECT 'kpione2_raw', COUNT(*) FROM cg_raw.kpione2_raw
UNION ALL SELECT 'power_app_raw', COUNT(*) FROM cg_raw.power_app_raw;
```
**Contexto:** Determina si se necesita una política de retención. Necesario para R05.

### G3. Crecimiento de ruta_rutero_load_rows
```sql
SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM cg_core.ruta_rutero_load_rows;
```
**Contexto:** Determina si la acumulación por carga es un problema real. Necesario para R04.

### G4. Consumidores de payload_json en cg_raw
Inspeccionar `pg_catalog` / `information_schema` para procedures y views que referencien las columnas `payload_json` en `cg_raw.*` y `cg_core.ruta_rutero_load_rows`. Necesario para R08.

### G5. Consumidores de row_hash en ruta_rutero
Buscar en `pg_catalog`, views, y reports cualquier referencia a `ruta_rutero.row_hash` o `ruta_rutero_load_rows.row_hash`. Necesario para `CLAUDE-C001-RUTA-INF-001`.

### G6. Callers dinámicos de las 10 funciones db.py zero-caller
Confirmar que ninguna de las siguientes funciones tiene callers vía `getattr`, `__all__`, o reflection en ningún módulo Python del repo: `get_result_view_contract`, `get_mercaderistas_home`, `get_locales_por_mercaderista`, `get_tabla_ux_paginada`, `get_cg_v2_contract_smoke`, `get_cg_v2_filter_options`. Necesario para R09.

### G7. Objetos DB exclusivamente referenciados por funciones zero-caller
Para cada función del grupo coverage-audit y CG v2 introspection, identificar qué vistas o tablas DB son referenciadas únicamente por esas funciones y en ninguna otra parte del codebase. Necesario para estimar el alcance de la limpieza de R09.

### G8. Scope de transacciones en refresh_control_gestion_v2_incremental.py
Confirmar si cada etapa del pipeline (stage → daily → weekly → MV) está dentro de una transacción única o si las etapas son commits independientes. Si son independientes, una falla en etapa 3 deja fact_cg_visita_dia_resuelta_v2 con datos que no se reflejan en fact_cg_out_weekly_v2. Necesario para documentar modo de fallo y eventual protección.

---

## Resumen ejecutivo

**Complejidad esencial vs histórica:**

El sistema tiene complejidad esencial legítima: múltiples fuentes de CG con precedencia declarada, pipeline de 4 etapas para transformar raw a mart, capas de caché configuradas por TTL. Esta complejidad existe por razones de negocio.

La complejidad histórica acumulada es:
1. **3 capas de historial Excel + DB** sin fuente de verdad única para datos históricos de stock
2. **4 columnas fórmula en Excel** para derivar datos que ya están en el DB (COD_RT se podría computar con RUTA_RUTERO en DB)
3. **payload_json en 4 tablas** duplicando columnas ya almacenadas individualmente
4. **10 funciones db.py sin callers** (5 validadas previamente + 5 nuevas)
5. **Acumulación indefinida** en cg_core.ruta_rutero_load_rows y cg_raw sin retención definida
6. **Hojas de análisis manual** (DESARROLLO_CONTROL_GESTION, OUT) que posiblemente replican lo que ya muestra el DB en pantalla

**El cambio de mayor impacto con menor riesgo** es R02 (pre-flight de frescura de workbook) combinado con R01 (eliminar columna duplicados). Estos no requieren cambios en DB ni en lógica de carga, y previenen la clase más común de error silencioso.

**El cambio de mayor impacto a mediano plazo** es R07 (offload COD_RT a Python), pero requiere un test de paridad riguroso antes de cualquier modificación al loader productivo.
