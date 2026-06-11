# CLAUDE_C004_G0_IMPLEMENTATION_DOSSIER
**Agente:** Claude (claude-fable-5)
**Tipo:** implementation_design — dossier ejecutable para Codex; nada se ejecuta aquí
**Fecha:** 2026-06-11
**Baseline runtime:** `2c135a7be1d813a2c914f43b26b0579504a7f8a5`
**Maestro estructurado:** `research/C004_G0_IMPLEMENTATION_DOSSIER.json`
**Destino:** cerrar G0 en el laboratorio local (`stock_zero_cg_parity_pg`), sin código productivo ni cambios DB.
**Backlog servido:** CG-002 (VALIDATED), CG-003 (PARTIAL), CG-004 (PARTIAL). `implementation_not_authorized = true`.

---

## 0. Punto de partida (hechos cerrados, no re-investigados)

Daily local: 21,347 filas con **1 clave faltante** (KPIONE2, 2026-05-15) y **56 diferencias de valor**
(concentradas en la semana 2026-06-01). Weekly: timeout (900s vía vista; 180s una semana). La causa del
excedente original (13,469) está cerrada: ventana productiva omitida. payload_json no se elimina. Sin
autorización de limpieza Supabase.

**Hallazgo técnico nuevo que funda este dossier** (extraído del DDL y del refresh productivo, lectura
read-only): la atribución de ruta hoy es *"último batch ok cargado por semana, evaluado al momento del
query"* (`winning_batch_by_grain`: rank por `status='ok'` → `loaded_at DESC` → `ruta_batch_id DESC`, con
`effective_week_start` = semana ISO del `loaded_at` en America/Santiago). Eso significa que **un batch
cargado más tarde dentro de la misma semana reescribe retroactivamente al ganador**. Los facts congelaron
al ganador vigente en el momento de materializar — pero ese "pin" nunca se registró. Esa es exactamente la
divergencia 16/17 (baseline) vs 18 (lab) de la semana 2026-06-01.

Segundo hallazgo estructural: **la receta productiva del weekly ya es "desde daily staged"**
(`_cg_weekly_stage` en `refresh_control_gestion_v2_incremental.py`: base de frecuencia de ruta LEFT JOIN
`fact_cg_visita_dia_resuelta_v2` sobre `(cod_rt, cliente_norm, semana_inicio)`). El timeout del lab vino de
usar la **vista** `v_cg_out_weekly_v2`, que recomputa desde raw con `string_agg(DISTINCT)`/WindowAgg. La
paridad weekly debe replicar la receta staged, no la vista.

---

## BLOQUE 1 — Clave faltante (10 pasos de diagnóstico)

Diseño outside-in en `c004_g0.missing_key_trace`: (1) forense de la fila baseline (su `fuente_ganadora`,
conteos raw y `mart_loaded_at` acotan qué batches pudieron producirla); (2) **barrido de TODOS los batches
KPIONE2, no solo el 38** — los snapshots acumulativos pueden *eliminar* filas antiguas, hipótesis líder;
(3) divergencia payload vs columnas físicas; (4) traza de normalización de `cod_rt`/`cliente` byte a byte
(mojibake UTF-8 incluido); (5) replay de precedencia; (6) **test de inclusión en route-scope** — si el grano
`(cod_rt, cliente_norm)` existe bajo el batch antiguo pero no bajo el 18, la clave faltante es una baja por
snapshot de ruta y el pin de BLOQUE 3 también la recupera; (7) auditoría de borde de ventana
(tipo/zona horaria/parsing de fecha); (8) diff etapa-por-etapa pre-materialización; (9) confirmación de
ausencia real vs re-keying; (10) veredicto de **fila histórica no reconstruible** → dos salidas admisibles,
ambas decisión de Bastián (BD3): excepción documentada y versionada en `expected_parity`, o exigencia de
fuente archival declarada. Por defecto: paridad estricta.

Todo expresado como queries/pseudocódigo read-only; nada ejecutado en esta fase.

## BLOQUE 2 — Matriz de las 56 diferencias

Tabla `c004_g0.value_diff_matrix`: una fila por `(clave_diaria, columna)` con valor baseline, valor local,
y los valores esperados bajo las superficies de ruta materializadas por batch 16, 17 y 18. Dimensiones:
gestor, rutero, reponedor, cliente, local, modalidad, frecuencia, estado de visita, fuente, batch de ruta.

Reglas de clasificación ordenadas: **SNAPSHOT_DIFF** (baseline coincide con 16/17 y local con 18 en columna
atribuida por ruta) → **ROUTE_DUPLICATE_DIFF** (grano duplicado dentro del mismo batch, `ruta_duplicada_flag`)
→ **NORMALIZATION_DIFF** (iguales tras UPPER/TRIM) → **PRECEDENCE_DIFF** (columnas de atribución de fuente)
→ **TIMESTAMP_MATERIALIZATION_DIFF** (derivadas de build-time; se documentan y excluyen) → **OTHER** (debe
llegar a cero por investigación, nunca por pin). El PASS exige que las 56 tengan exactamente una clase.

## BLOQUE 3 — Política de ruta (3 opciones + modelo combinado)

| | A_WEEK_FROZEN | B_EFFECTIVE_DATED | C_BUILD_PINNED |
|---|---|---|---|
| Significado | La ruta vigente al inicio de semana gobierna toda la semana | valid_from/valid_to aplicado por fecha_visita | Cada build registra el snapshot usado |
| Reproducibilidad | ALTA (con registro) | ALTA solo si los intervalos son datos de primera clase (hoy no existen) | TOTAL por construcción |
| Complejidad | BAJA-MEDIA (cambia el rank del ganador) | ALTA (dimensión versionada + cambio de todos los joins) | BAJA (metadata + disciplina) |
| Riesgo | Correcciones intra-semana no aplican hasta la semana siguiente | Backfill de intervalos = inferencia que puede reescribir historia | Incompleta sola: no dice qué pinear a futuro |
| Compatibilidad app | ALTA | MEDIA (B3 también lee ruta) | TOTAL |
| Corrección retroactiva | No (por diseño); rebuild explícito | Fuerte | Nuevo build con nuevo pin |
| Almacenamiento | Sin cambio | Moderado (tabla de intervalos) | Despreciable |

**Modelo recomendado: COMBINADO** — POLICY_C siempre activa (capa técnica de reproducibilidad, Codex) +
**una** política operacional elegida por Bastián (A o B) que decide qué pinea cada build futuro. Recomendación
de corto plazo: **A**, porque el grano weekly ya es por `effective_week_start`, no exige tablas nuevas, y la
evidencia B0 sugiere que las recargas intra-semana son correcciones de carga más que cambios efectivos de ruta.
B queda como upgrade si Bastián confirma que los cambios de ruta a mitad de semana son requisito real
(evidencia que Codex debe levantar: frecuencia con que A y B discreparían en la práctica).

**Los IDs de batch (16/17/18) son linaje técnico, jamás decisión de negocio** (C003-HORIZON-CORR-002/004).
Y crítico: **la paridad G0 cierra contra el baseline existente** pineando su linaje técnico; la política
operacional gobierna builds futuros — si Bastián quisiera re-expresar la historia bajo la nueva política, eso
es un build nuevo con nuevo `build_version`, no paridad.

## BLOQUE 4 — Weekly desde daily staged

Flujo: `build context → daily staged exacto → weekly staged → comparación con baseline semanal`, en esquema
local nuevo `c004_g0` (aislado de `c002_b0`; rollback = `DROP SCHEMA c004_g0 CASCADE`).

- **Superficies:** `build_context`, `daily_staged` (21,348 filas, hash-paridad diaria previa),
  `route_freq_pinned` (frecuencia de ruta con ganador FORZADO al pin por semana; assert: exactamente 1 batch
  por semana — `ruta_batch_id` está en el GROUP BY productivo y dos batches partirían filas), `weekly_staged`,
  `weekly_diff`.
- **Llaves:** diaria `(fecha_visita, cod_rt, cliente_norm)` (única, validada en B0); join weekly productivo
  `(cod_rt, cliente_norm, semana_inicio=effective_week_start)`; llave semanal candidata
  `(SEMANA_INICIO, COD_RT, CLIENTE_NORM_FILTER)` — **Codex debe verificar unicidad en baseline primero** y
  extender el grano si los duplicados de ruta la rompen, registrando la llave verificada en el build context.
- **Agregaciones:** espejo exacto de `_cg_weekly_stage` — flags por día (`max(case isodow)`), planes
  (`max(coalesce(día,0))`), `VISITA`/`VISITA_REALIZADA_RAW`/`CAP`/`SOBRE_CUMPLIMIENTO`, días por fuente,
  doble/triple marcaje, derivadas (`DIFERENCIA`, `ALERTA`, `*_NORM_FILTER`, `GESTION_COMPARTIDA_FLAG_CALC`,
  `VISITAS_PENDIENTES_CALC`).
- **string_agg DISTINCT:** la receta staged **no lo usa** — `FUENTES_REPORTADAS_SEMANA` es `concat_ws` con
  orden fijo de fuentes (determinístico). El costo de `string_agg(DISTINCT)` vive solo en la vista, que este
  diseño evita por completo. Regla: toda agregación de strings del lab debe ser orden-determinística para que
  los hashes de fila sean estables.
- **Índices locales (solo lab):** `daily_staged(semana_inicio, cod_rt, cliente_norm)`,
  `route_freq_pinned(effective_week_start, cod_rt, cliente_norm)`, baseline weekly por su llave verificada.
- **Métrica de tiempo:** objetivo ≤120s, límite duro 300s por ventana declarada (vs 900s/180s del camino por
  vista); es una cota de reproducibilidad del lab re-baselineable con evidencia de hardware (BD4), no un SLA
  productivo. **No se propone SQL productivo todavía.**

## BLOQUE 5 — Build context (CG_BUILD_CONTEXT_V1)

Contrato JSON que permite reconstruir exactamente un build histórico. Campos: `build_id`,
`affected_date_window{start,end}`, `raw_batch_set{KPIONE[],KPIONE2[],POWER_APP[]}` (puede incluir batches
antiguos cuando un snapshot posterior eliminó filas), `route_policy_version`, `route_snapshot_by_period[]`
(`{period_week_start, ruta_batch_id, pin_reason}`), `source_precedence_version`, `daily_builder_version` y
`weekly_builder_version` (id de receta + hash de contenido SQL), `input_hashes`, `expected_parity` (con
`documented_exceptions` vacío salvo aprobación de Bastián) y `output_hashes`.

**Regla de validación:** un build context es INVÁLIDO si cualquier campo es null, vacío o expresa "latest".
Almacenamiento: archivos JSON versionados en git durante H0-H1; registro en DB es decisión H1+.

## BLOQUE 6 — Secuencia de 8 experimentos para Codex

Incrementales y aislados; cada uno con superficie propia en `c004_g0` y rollback por DROP:

1. **X1_MISSING_KEY_FORENSICS** — localizar dónde desaparece la clave (pasos 1-10 del Bloque 1). PASS = clase de causa asignada con evidencia.
2. **X2_DAILY_KEY_CLOSURE** — rebuild diario con el fix de X1; PASS = 21,348 filas, 0/0/0 en claves.
3. **X3_VALUE_DIFF_MATRIX** — clasificar el 100% de las diferencias; PASS = OTHER=0.
4. **X4_ROUTE_PIN_DAILY_VALUE_CLOSURE** — `route_freq_pinned` + fixes de X3; PASS = value_differences=0 y full_row_hash_parity=true (exclusiones build-time documentadas).
5. **X5_BUILD_CONTEXT_REGISTRATION** — congelar el cierre como CG_BUILD_CONTEXT_V1 válido; PASS = cero campos null/"latest".
6. **X6_WEEKLY_STAGED_BUILD** — construir weekly desde staged; PASS = bajo el límite de tiempo con conteo plausible.
7. **X7_WEEKLY_PARITY** — verificar unicidad de llave semanal y paridad exacta llave+valor; PASS = todo en cero.
8. **X8_CLEAN_ROOM_REPRODUCIBILITY** — rebuild desde cero en `c004_g0_rerun` solo con receta + build context; PASS = hashes de salida idénticos. Es la **precondición dura** para someter a Bastián la destrucción del lab.

FAIL de cualquiera devuelve al bloque que lo alimenta con el diferencial nuevo; ningún experimento toca
`c002_b0`, los baselines, los parquet ni el contenedor.

## BLOQUE 7 — Criterios de cierre G0

**Daily:** extra=0, missing=0, duplicates=0, value_differences=0, full_row_hash_parity=true (exclusiones
build-time solo vía lista versionada; excepciones históricas solo con firma de Bastián, default estricto).
**Weekly:** paridad exacta de llave y valor, duplicados=0, tiempo reproducible dentro del límite propuesto,
construcción exclusivamente desde daily staged (cualquier camino que relea raw o la vista es no conforme).
**Reproducibilidad:** build context versionado, receta reconstruible por hash de contenido, hashes de inputs
y outputs registrados, laboratorio recreable (X8 hash-idéntico).
**G0 = CLOSED** solo cuando las tres familias pasan **en el mismo build context registrado**.

---

## Decisiones de Bastián (5)

1. **BD1** — Semántica operacional de ruta: ¿A (week-frozen) o B (effective-dated)? (C es técnica y siempre activa.)
2. **BD2** — Confirmar el objetivo de paridad: baseline existente tal como fue materializado (recomendado).
3. **BD3** — Si X1 paso 10 dispara: ¿excepción histórica documentada o fuente archival obligatoria?
4. **BD4** — Aceptar o re-baselinar la cota de tiempo weekly (120s/300s) del lab.
5. **BD5** — Aprobar la destrucción de Docker/local_pg **solo después** de la evidencia X8 (BDP-LOCAL-PG-DESTRUCTION).

## Riesgos principales

R1 clave históricamente no reconstruible (→ ruta BD3); R2 llave semanal no única en baseline (→ X7 verifica
primero y extiende el grano); R3 fuga de dos batches en una semana parte filas del GROUP BY (→ assert en
`route_freq_pinned`); R4 el pin enmascara una causa menor (→ X3 clasifica ANTES de pinear); R6 la decisión de
política no bloquea G0 (la paridad pinea el linaje del baseline; la política gobierna el futuro); R7 prohibido
destruir el lab antes de X8.

*Este dossier diseña; no ejecuta. Ningún experimento corre sin su propia fase Codex explícitamente autorizada.
Sin DB, sin Docker, sin loaders, sin SQL productivo, sin limpieza Supabase.*
