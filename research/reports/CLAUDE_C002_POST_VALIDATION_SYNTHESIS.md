# CLAUDE_C002_POST_VALIDATION_SYNTHESIS
**Agente:** Claude (claude-opus-4-8)
**Tipo:** research_synthesis (sin re-investigar, sin implementar)
**Fecha:** 2026-06-11
**Baseline runtime:** `2c135a7be1d813a2c914f43b26b0579504a7f8a5`
**Base de validación:** `research/C001_CODEX_VALIDATION_MATRIX.json` (Codex: 11 VALIDATED, 13 PARTIAL, 5 DISPUTED, 1 REJECTED)
**Salida estructurada:** `research/C002_IMPLEMENTATION_PORTFOLIO.json`
**Autorización:** ningún hallazgo autoriza implementación. `db_access: none`.

---

## 0. Punto de partida tras la validación de Codex

C001 propuso 30 hallazgos y 10 recomendaciones. Codex los validó de forma independiente con acceso DB
read-only. Lo que sobrevivió como **verdad accionable** y lo que quedó **bloqueado** define este portafolio:

**Validado y utilizable como base:**
- BASE tiene 4 columnas fórmula; el loader lee valores guardados, no fórmulas vivas (`INVDATA-FACT-002`).
- `duplicados` es redundante para el loader (`INVDATA-FACT-003`) — pero su remoción es decisión operacional.
- COD_RT depende de un XLOOKUP sobre RUTA_RUTERO; el valor stale se ingesta en silencio (`INVDATA-FACT-004`).
- El loader CG consume solo 3 hojas; DESARROLLO/OUT no tienen consumidor (`CGDATA-FACT-001/002`).
- `row_hash` es huella de análisis, no llave de UPSERT (`RUTA-FACT-001`).
- 10 funciones de `app/db.py` son static-only sin dispatch dinámico (`DBPY-FACT-001`).
- `Home.py` difiere los imports dentro de `main()` (`DBPY-FACT-003`).

**Bloqueado / corregido por Codex (lo respetamos como restricción dura):**
- `payload_json` **tiene consumidores DB** (cg_core/cg_mart) → **no se puede retirar** (R08 REJECTED).
- `cg_core.ruta_rutero_load_rows` **es dependencia viva** de `v_rr_frecuencia_base_resuelta_v2` (RUTA-INF-001 PARTIAL).
- **Paridad funcional 9C7A4 NO cerrada**: diario 34,817 vs 21,348; semanal por timeout.
- **DB_GLOBAL_HISTORICO no se puede retirar**: `fact_stock_venta` cubre 2026-05-02..2026-06-07; Hoja1 cubre 2026-03-03..2026-04-06 (R10 REJECTED_FOR_NOW).
- La lista de constantes `CG_V2_*` de C001 era incorrecta; la real tiene 9 nombres distintos (R06 REFORMULATE).
- `DB_HISTORICA` interna **no** comparte el esquema de BASE (10 columnas, no 12) — el "3-layer schema idéntico" era falso.
- El refresh **commitea los datos antes** de un ANALYZE post-commit best-effort → no hay riesgo de "estado parcial" como se sobreafirmó.

Este informe convierte ese estado en **tres programas** con orden estricto y gates explícitos.

---

## 1. PROGRAMA A — DATA PREPARATION OFFLOAD

**Objetivo:** Reemplazar progresivamente las columnas fórmula de Excel por cómputo Python, **probado por
paridad read-only antes de tocar el loader productivo**. Primero COD_RT, luego NOMBRE_LOCAL_RR, después
OTROS; `duplicados` como decisión operacional separada.

**Restricción clave (Codex R07):** el primer prototipo **debe usar la hoja RUTA_RUTERO del mismo workbook**,
no `public.ruta_rutero` viva, para evitar desalineación temporal (la DB puede estar más fresca que el período
del workbook).

### Diseño del prototipo COD_RT

| Elemento | Definición |
|---|---|
| **Entradas** | BASE (43,635 filas: CADENA, FECHA, MARCA, SKU, N_LOCAL, VTA, INV) + RUTA_RUTERO del mismo workbook (3,568 filas: CADENA, COD B2B, CLIENTE, COD KPI ONE, LOCAL) |
| **Outputs** | COD_RT computado en Python por fila; NOMBRE_LOCAL_RR computado; reporte de paridad (CSV/JSON) — nunca escrito en el pipeline productivo |
| **Llaves** | COD_RT: `CADENA + N_LOCAL(=COD B2B) + MARCA(=CLIENTE) → COD KPI ONE`. NOMBRE_LOCAL_RR: `COD_RT → LOCAL`. Alineación de filas: `(fecha, cod_rt, sku, marca)` (llave UPSERT del fact) |
| **Normalizaciones** | trim + upper en claves; cast explícito N_LOCAL int vs COD B2B; SKU sin `.0`; cod_rt upper — espejo de la coerción implícita de Excel |
| **Conflictos** | COD KPI ONE duplicado en RUTA_RUTERO (VLOOKUP toma la primera ocurrencia; Python debe replicar first-match); filas NO_RT (el loader las descarta en silencio; la paridad debe contarlas igual) |
| **Comparación fila a fila** | join Python vs Excel-cache sobre la llave lógica; clasificar MATCH / MISMATCH_VALUE / MISSING_PYTHON / EXTRA_PYTHON; `assert_frame_equal` tras alineación |
| **Métricas de paridad** | exact_match_rate de COD_RT (meta 100% en semana ya cerrada); igualdad de conteo NO_RT; igualdad de distribución de COD_RT; igualdad de conteo de filas; match rate de NOMBRE_LOCAL_RR condicional a COD_RT |
| **Rollback** | el prototipo vive en un script nuevo (p.ej. `research/labs/`), **nunca** edita `scripts/load_fact_from_excel.py`. Rollback = borrar el script de laboratorio |
| **Criterio para modificar el loader productivo** | (1) paridad exacta 100% de COD_RT en ≥1 semana histórica cerrada usando RUTA_RUTERO del workbook; (2) métricas NO_RT y distribución iguales; (3) Codex reproduce el resultado; (4) aprobación explícita de Bastian; recién entonces PR con feature-flag y fallback a valores Excel-cache |

**Reversibilidad:** ALTA · **Riesgo:** MEDIO (coerción/duplicados) · **Costo:** BAJO-MEDIO · **Prioridad:** **P1**.

**Criterio de abandono:** si la paridad no alcanza ~100% por semántica irreducible de Excel, abandonar el
offload y, en su lugar, mantener COD_RT en Excel pero añadir un guard de frescura (R02 reformulado:
frescura de fórmula/caché + flujo del operador, no solo mtime).

---

## 2. PROGRAMA B — SUPABASE STORAGE AND PARITY

**Objetivo:** Controlar el almacenamiento Supabase (hoy ~1.43 GB vs target 512 MB) **sin romper** el mart v2
que lee raw vía vistas DB. Orden estricto por gates; nunca retirar `payload_json` sin superficie equivalente.

### Orden estricto de gates

| Gate | Nombre | Criterio de salida | Dueño | Estado |
|---|---|---|---|---|
| **G0** | Paridad funcional diaria/semanal | Recompute local de `fact_cg_visita_dia_resuelta_v2` y `v_cg_out_weekly_v2` igual al baseline (cerrar el +13,469 diario y el timeout semanal) | Codex | **ABIERTO/BLOQUEADO** |
| **G1** | Catálogo DB autoritativo | Catálogo completo de objetos + dependencias con consumidores de `payload_json` y `row_hash` a nivel columna (supera la estimación parcial de ~28 objetos) | Codex | depende de G0 |
| **G2** | Clasificación de raw | Cada fila raw clasificada como `raw_necesario` / `raw_reconstruible` / `raw_historico`, con el selector de ventana `v_cg_latest_batch_by_source` como frontera | Codex | depende de G1 |
| **G3** | Diseño de retención/archivo (sin ejecutar) | Plan de archivo con reconstructibilidad probada y rollback ensayado; `payload_json` retenido o reemplazado por superficie equivalente probada | Codex + Bastian | depende de G2 |

### Taxonomía de raw (diferenciación exigida)
- **raw_necesario:** batches seleccionados hoy por `v_cg_latest_batch_by_source` y consumidos por el mart v2 → permanecen online.
- **raw_reconstruible:** batches antiguos reproducibles desde los parquet validados de 9C7A3 (71 archivos, hashes OK) → candidatos a archivo frío solo tras G0+G2.
- **raw_historico:** batches sin necesidad de reconstrucción y fuera de toda ventana de reporte → candidatos a archivo tras política de Bastian.

**Nunca:** retirar `payload_json` sin superficie equivalente probada (SP Item ID existe **solo** en payload para KPIONE2);
aplicar ventanas arbitrarias (12 semanas / 52 cargas) antes de medir en G2; borrar raw antes de que G0 pruebe que el mart es reproducible.

**Reversibilidad:** MEDIA-BAJA (archivo) / ALTA (medición) · **Riesgo:** ALTO si se intenta antes de G0 · **Costo:** ALTO · **Prioridad:** **P3 (debe esperar)**.

---

## 3. PROGRAMA C — CODE SIMPLIFICATION

**Objetivo:** Reducir el riesgo de `app/db.py` (5,348 líneas) mediante observabilidad y confirmación de
código muerto, **sin gran reescritura** y **sin eliminar B3 en la primera etapa**.

| Etapa | Acción | ¿Elimina código? |
|---|---|---|
| **C1** | Runtime smoke que ejercita cada pantalla (CLIENTE, CONTROL GESTION v2, LOCAL, MERCADERISTA) y registra qué `db.*` se ejecutan realmente | No |
| **C2** | Cruzar las 10 funciones static-only validadas (+`get_rutero_reponedor`) contra el trace; marcar runtime-dead solo si nunca se invocan | No |
| **C3** | Hacer **visible** el fallback B3 (log/métrica estructurada cuando se alcanza por excepción o `USE_CG_V2!=1`) — **no** eliminar B3 | No |
| **C4** | Plan de separación por costuras de `app/db.py` (infra / selectors / CG-v2 / coverage-audit) — solo diseño, sin mover código | No |
| **C5** | Solo tras C2 + aprobación de Bastian: PRs de borrado de funciones runtime-dead, cada uno con smoke antes/después | candidato, gated |

**Reversibilidad:** ALTA (C1-C4) · **Riesgo:** BAJO (C1-C4), MEDIO (C5 gated) · **Costo:** MEDIO · **Prioridad:** **P2 (paralelo con A)**.

**Criterio de abandono:** si una función "static-only" aparece en el trace de runtime, reclasificarla como
viva de inmediato y detener cualquier consideración de limpieza sobre ella.

---

## 4. Comparación de los tres programas

| Eje | PROGRAMA A (Offload) | PROGRAMA B (Storage/Parity) | PROGRAMA C (Code) |
|---|---|---|---|
| **Objetivo** | Eliminar riesgo de COD_RT stale vía Python probado | Controlar almacenamiento sin romper el mart | Reducir riesgo de db.py + visibilizar B3 |
| **Benef. operacional** | Menos error silencioso en atribución de ruta | Evita límite de plan / costo; protege CG | B3 observable; menos fallos latentes |
| **Benef. técnico** | Linaje COD_RT explícito y testeable | Catálogo DB autoritativo + contrato de reconstrucción | Lista de muerto verificada + decomposición segura |
| **Riesgo** | MEDIO (coerción/duplicados) | ALTO si antes de G0 | BAJO (C1-C4) / MEDIO (C5) |
| **Costo** | BAJO-MEDIO | ALTO | MEDIO |
| **Reversibilidad** | ALTA (lab) | MEDIA-BAJA (archivo) | ALTA (C1-C4) |
| **Evidencia disponible** | INVDATA-FACT-002/004 VALIDATED; R07 | 9C7A2/9C7A3/9C7A4 forenses y DDL | DBPY-FACT-001/003 VALIDATED; CG-001 |
| **Evidencia faltante** | duplicados COD KPI ONE; tipo N_LOCAL; semana cerrada | causa del +13,469; semanal; MIN(fecha) | trace de runtime que pruebe dead-code |
| **Primer experimento** | harness de paridad COD_RT (read-only) | (Codex) root-cause del surplus diario | levantar runtime smoke (C1) |
| **Criterio de éxito** | 100% paridad COD_RT reproducida | G0: paridad diaria+semanal exacta | smoke cubre 4 pantallas; static-only ausente |
| **Criterio de abandono** | paridad irreducible → guard de frescura | parity local irreproducible → live snapshot | función static-only aparece viva → reclasificar |
| **Dependencias** | semana cerrada; indep. de DB/B | G0 bloquea todo; indep. de A | indep. de A/B; C5 tras C2+Bastian |
| **Prioridad** | **P1** | **P3** | **P2** |

---

## 5. Recomendación

1. **Empezar primero — PROGRAMA A.** Es la mayor evidencia validada, read-only y aislada en laboratorio,
   usa RUTA_RUTERO del mismo workbook (sin DB, sin desalineación temporal), totalmente reversible, y ataca
   el riesgo de error silencioso de mayor valor (COD_RT stale). Coincide con el orden recomendado por Codex
   (ítem 2: "Add read-only parity tests for COD_RT").

2. **En paralelo — PROGRAMA C.** Independiente de A y de la DB; C1-C4 son aditivos/observabilidad de bajo
   riesgo. El runtime smoke y la confirmación del set static-only validado avanzan sin depender de la paridad.

3. **Debe esperar — PROGRAMA B.** El gate G0 (paridad funcional diaria/semanal) está abierto y **bloqueado**
   por el fallo de 9C7A4. Ninguna retención/archivo ni trabajo sobre `payload_json` es seguro hasta cerrar G0,
   y la remoción de `payload_json` está REJECTED de plano.

### 4. Acciones que explícitamente NO deben ejecutarse
- **NO** retirar `payload_json` de ninguna tabla cg_raw/cg_core (REJECTED; SP Item ID es payload-only en KPIONE2).
- **NO** retirar `DB_GLOBAL_HISTORICO.xlsx` ni la hoja interna `DB_HISTORICA` (REJECTED_FOR_NOW; la DB no cubre 2026-03-03..2026-04-06).
- **NO** borrar ninguna función static-only de `app/db.py` todavía (requiere smoke de runtime + aprobación de Bastian).
- **NO** eliminar ni evitar el fallback B3 en la primera etapa.
- **NO** modificar `scripts/load_fact_from_excel.py` ni ningún loader productivo antes de probar y aprobar la paridad de COD_RT.
- **NO** aplicar ventanas de retención arbitrarias (12 semanas / 52 cargas) a cg_raw o al historial de ruta antes de medir en G2.
- **NO** usar `public.ruta_rutero` viva para la primera prueba de paridad de COD_RT (desalineación temporal); usar la hoja RUTA_RUTERO del workbook.
- **NO** remover la columna `duplicados` del workbook sin confirmación operacional de Bastian.
- **NO** construir una tabla DB FOCO_CLIENTE para OTROS con la evidencia actual (hit-rate no probado).
- **NO** borrar ni aplicar los drafts `sql/09` y `sql/10` basándose solo en el nombre de archivo o el git status.

### 5. Máximo cinco fases futuras para Codex
1. **C002_A1_CODRT_PARITY_HARNESS** — harness read-only de paridad COD_RT desde RUTA_RUTERO del workbook (produce la evidencia que exige R07).
2. **C002_B0_FUNCTIONAL_PARITY_ROOTCAUSE** — causa raíz y cierre de la paridad funcional diaria/semanal 9C7A4 (Gate G0; desbloquea todo el trabajo de almacenamiento).
3. **C002_C1_RUNTIME_SMOKE** — runtime smoke + confirmar que las funciones static-only están muertas en runtime.
4. **C002_B1_DB_CATALOG** — catálogo DB autoritativo de objetos y dependencias con consumidores de `payload_json`/`row_hash` a nivel columna (Gate G1; depende de G0).
5. **C002_GOV_BASTIAN_DECISION_PACK** — paquete de decisión para Bastian: remoción de `duplicados`, fuente histórica autoritativa, hojas CG manuales (DESARROLLO/OUT), política de retención.

---

## 6. Veredicto

**READY_FOR_CHAT_BASTIAN_DECISION.** Los tres programas están diseñados, comparados y priorizados sobre la
evidencia validada por Codex. Ninguno autoriza implementación: A comienza como laboratorio read-only de
paridad, C avanza en paralelo como observabilidad/confirmación, y B espera detrás del gate de paridad G0.
Las decisiones de gobernanza (duplicados, fuente histórica, hojas manuales, retención) requieren a Bastian.

*Este informe sintetiza investigación validada en un plan priorizado. No autoriza cambios de runtime, loader,
SQL, DB, datos ni evidencia.*
