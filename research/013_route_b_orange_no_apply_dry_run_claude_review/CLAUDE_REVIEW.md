Voy a leer el packet completo y la evidencia asociada antes de emitir el veredicto.



Leí el primer 42% del packet. Sigo con el resto antes de emitir el veredicto.



He leído el packet completo (2710 líneas: locks de gobernanza, gates 012, contrato activo, ambos test suites, el lab runner y el loader dry-run de Route B). Entrego la revisión adversarial en el formato obligatorio del documento.



---



# Claude Adversarial Review — 013 ROUTE_B_ORANGE_NO_APPLY_DRY_RUN



## Verdict



**APPROVE_WITH_WARNINGS**



El scope 013 preserva las fronteras no-apply (Q1: sí — el loader dry-run no importa clientes DB, `--apply` está bloqueado con `LoaderUsageError`, y el phase lock prohíbe explícitamente los caminos de escritura). Pero hay tres debilidades de evidencia que, si no se corrigen en esta fase, harían que el "PASS" del dry-run valga menos de lo que aparenta.



## Top risks



Ordenados por severidad; cada uno con clasificación epistémica.



1. **\[FACT] La evidencia de "day_presence es binaria" es tautológica.** En `analyze_photo_dataframe`, `day_presence\["presence"] = 1` se asigna como constante y luego el flag `day_presence_is_binary` verifica `(presence == 1).all()`. Ese flag **no puede fallar nunca**, y el test `test_day_presence_is_binary_not_event_count` valida la misma tautología. La única señal real es `max_events_per_day_presence` (que sí distingue evento≠presencia), pero no es bloqueante. Hoy el invariante `event_row -> day_presence` está *afirmado*, no *probado*.



2. **\[FACT] El invariante anti "1 fila = 1 visita" se verifica con un proxy agregado débil.** `forbidden_assumption_rejected = photo_rows != distinct_event_ids`. Dos problemas: (a) un archivo legítimo donde cada evento tiene exactamente 1 foto haría fallar el flag (falso WARN) — el proxy confunde "el dato tiene multiplicidad" con "el código respeta el grano"; (b) no prueba estructuralmente que el mapeo sea N-fotos→1-evento; solo compara dos conteos. Un bug de agrupación que colapse o duplique eventos podría pasar si los agregados coinciden por coincidencia.



3. **\[FACT] No hay guardas sobre las claves del denominador.** El contrato mapea `Codigo Local → cod_rt` y `Marca → cliente_norm`, pero no existe flag `no_null_cod_rt_rows` ni `no_null_cliente_norm_rows`. `day_presence` agrupa con `dropna=False`, así que filas con Codigo Local vacío colapsan silenciosamente en un solo bucket de presencia con clave `""` — distorsión del numerador indetectable con el set de flags actual.



4. **\[INFERENCE] La vía más probable de alteración accidental del denominador (Q2) es el join, no el conteo.** EXIGIDAS nace del lado ruta (frecuencias de `ruta_rutero`); Route B es evidencia (numerador). El denominador cambia accidentalmente si: (a) la normalización de claves foto (`normalize_key`: NFKD + upper) no es idéntica a la normalización del lado ruta, generando pares (cod_rt, cliente) huérfanos que tienten a una implementación futura a ensanchar el join (full outer / union de granos = EXIGIDAS alterado); (b) la semántica de semana difiere (`week_start_monday` desde Fecha vs `effective_week_start` del contrato semanal de ruta), corriendo visitas entre semanas y desalineando numerador/denominador semanal; (c) el fallo de grano infla VISITA_REALIZADA_RAW y el CAP lo enmascara — la reconciliación debe comparar RAW y CAP por separado. No existe hoy ningún test de paridad de normalización entre `load_kpione2_photo_from_excel.normalize_key` y la normalización del loader de ruta.



5. **\[FACT — gap] La "denominator reconciliation evidence" es close_condition pero no tiene definición operativa.** Ningún artefacto del packet define cómo se computan localmente EXIGIDAS/VISITA/VISITA_REALIZADA_RAW/CAP/PENDIENTE/ALERTA, ni qué es el "before" en un contexto donde las vistas productivas viven en Supabase (no autorizado). Sin definición explícita, Codex puede (a) improvisar una semántica de compliance no contractual, o (b) sufrir scope creep hacia lecturas DB. Esto debe fijarse *antes* de implementar.



6. **\[FACT] El validador CLI está atado al workbook 009F.** `expected_from_contract` toma los valores esperados del `009F_evidence` del contrato; cualquier sample controlado distinto dará `WARN_REVIEW_REQUIRED` vía CLI. Eso genera presión para editar `load_kpione2_photo_from_excel.py` — que está en la lista forbidden. La salida correcta es un script nuevo que importe `analyze_photo_dataframe` con `expected` por parámetro, no tocar el loader.



7. **\[FACT] `pytest` sin acotar puede ejecutar writes al lab loopback.** `LocalPostgresIntegrationTests` en `test_cg_route_weekly_local_lab.py` hace inserts reales si hay PostgreSQL en 127.0.0.1:55433 (con guard `parse_loopback_dsn` sólido — no es riesgo Supabase). Pero la evidencia 013 debería nacer de comandos nombrados y acotados a los archivos de test relevantes, para que "no data movement" sea auditable sin discusión.



8. **\[HYPOTHESIS] Riesgo latente de parseo de fechas.** `pd.to_datetime(df\[col_fecha], errors="coerce")` sin `dayfirst`: si un export futuro entrega Fecha como texto `dd/mm/aaaa`, un swap día/mes parseable produce semanas corridas sin disparar `null_fecha_rows`. No es defecto del sample actual; es fragilidad forward-only.



## Required tests (Q3, Q4)



**Evidencia local mínima obligatoria antes de aceptar la implementación de Codex (Q3):**

1. Log de `python -m pytest tests/test_kpione2_photo_grain.py -q` (y el archivo de tests nuevo) con exit 0.

2. JSON dry-run del workbook real con `verdict=PASS_ROUTE_B_DRY_RUN` y las métricas 009F (37908 / 5892 / 2026-06-20 / 2026-06-24).

3. JSON/MD de reconciliación sobre samples controlados: delta = 0 en los 7 outputs protegidos por (semana_inicio, cod_rt, cliente_norm), con valores golden calculados a mano en el fixture.

4. `git diff --name-only` mostrando solo rutas permitidas (tests nuevos/extendidos, script validador nuevo, artefactos research/013).

5. Declaración `db_access.used=false` en el reporte estructurado de Codex.



**Tests faltantes (Q4):**

- **Presencia binaria real (no tautológica):** fixture con 3 eventos el mismo día/mismo (cod_rt, cliente) donde se afirme `day_presence_rows == 1` y que un cómputo de cumplimiento derivado use 1, no 3. Reemplazar o complementar el flag tautológico.

- **Todos-los-eventos-con-1-foto:** fixture legítimo donde `photo_rows == distinct_event_ids`; decidir y documentar el comportamiento esperado del proxy (hoy: falso WARN). El invariante debe verificarse estructuralmente (cardinalidad del groupby), no por desigualdad de agregados.

- **Claves de denominador nulas:** filas con Codigo Local o Marca vacíos → nuevos flags bloqueantes + test.

- **Paridad de normalización foto↔ruta:** mismo set de strings adversariales (acentos, ñ, mayúsculas, espacios dobles, ceros a la izquierda) pasado por la normalización de ambos loaders; afirmar identidad o documentar el mapeo. Este es el test que protege el join del denominador.

- **Reconciliación golden:** sample sintético ruta+fotos con EXIGIDAS/VISITA/RAW/CAP/PENDIENTE/ALERTA calculados a mano; before (sin evidencia Route B) vs after (con day_presence Route B) — delta cero en denominador, delta explicable solo en numerador.

- **Anomalías de `Foto Nº/Total`:** "0/0", total ausente, "3/2" (filas > total declarado), fila de foto duplicada exacta.

- **Evento cruzando semana** (domingo/lunes) disparando `no_event_ids_multi_week`.

- **Guard de aislamiento para el script nuevo:** replicar `test_new_loader_does_not_import_productive_loader_or_db_clients` sobre el validador nuevo (sin psycopg/sqlalchemy/DB_URL/import del loader v17) y afirmar `db_apply=false, sql_apply=false, writes_executed=false` en su payload.



## Scope violations if any



**Ninguna violación activa en el packet.** Dos ambigüedades que deben cerrarse como reglas antes de implementar:

1. El forbidden dice "modify `load_kpione2_photo_from_excel.py` **as productive implementation**" — ese calificador es explotable. Regla para 013: **cualquier** edición a ese archivo se trata como forbidden; el trabajo nuevo va en script/tests aparte que lo importan.

2. "Add or modify local tests" no debe interpretarse como licencia para tocar `test_cg_route_weekly_local_lab.py` (dominio ruta semanal, fuera de Route B foto). Restringir a `test_kpione2_photo_grain.py` + archivos nuevos.



## Suggested implementation path (Q5, Q8)



**Q5: ambos** — extender tests y crear un validador nuevo; ninguna de las dos cosas sola cierra los gaps.



Dentro de scope 013 (ORANGE_NO_APPLY):

1. Nuevo script (p. ej. `scripts/validate_route_b_denominator_dry_run.py`) que **importe** `analyze_photo_dataframe` del loader existente, acepte `--expected-json` por sample, compute los 7 outputs protegidos localmente desde fixtures (ruta sintética + day_presence foto) y emita JSON de reconciliación before/after con delta por (semana_inicio, cod_rt, cliente_norm). Sin imports DB, con los mismos flags de seguridad en el payload.

2. Extender `tests/test_kpione2_photo_grain.py` con los fixtures adversariales listados arriba.

3. Registrar evidencia en `research/013_route_b_orange_no_apply_dry_run_claude_review/` (permitido por el lock).

4. Documentar en el artefacto de reconciliación la definición exacta de cada métrica usada localmente, marcada como **semántica local-sample, no productiva**.



Requiere fase futura RED/apply (fuera de 013): reconciliación before/after contra datos productivos reales; DDL apply de `sql/15`/`sql/16`; cualquier cambio a vistas de compliance; generalización del CLI del loader; backfill; cutover y rollback productivo.



## What not to do



- No editar `scripts/load_control_gestion_raw_v17.py`, `scripts/load_kpione2_photo_from_excel.py` (ni "solo un flag"), el contrato `kpione2_photo_export_contract_v1.json`, ni SQL productivo.

- No ejecutar lecturas a Supabase (ni RO) para "conseguir el before" de la reconciliación — la reconciliación 013 es local-sample por definición.

- No correr la suite completa de pytest como evidencia; solo comandos nombrados sobre los archivos de test del scope.

- No mapear filas de foto a visitas en ningún artefacto intermedio, ni siquiera "temporalmente".

- No `git add .`; commit solo de los archivos nombrados.

- No tratar un PASS del validador actual como evidencia del invariante de presencia binaria (riesgo #1) hasta que el flag deje de ser tautológico.



**Qué convertiría esto en BLOCK (Q7):** cualquier write/SQL apply a Supabase; edición de los dos loaders protegidos o de la semántica del contrato; una reconciliación local que muestre delta en EXIGIDAS y se proponga "ajustar el denominador" en vez de detenerse; implementación que derive visitas de filas de foto; o cierre de fase sin definición operativa de la reconciliación (close_condition insatisfecha con evidencia decorativa).



## Files that must remain untouched (Q6)



`scripts/load_control_gestion_raw_v17.py` · `scripts/load_kpione2_photo_from_excel.py` · `contracts/control_gestion/kpione2_photo_export_contract_v1.json` · `sql/15_kpione2_photo_raw_ddl.sql` y `sql/16_..._rollback.sql` (permanecen review-only) · `scripts/refresh_control_gestion_v2_incremental.py` / `_mv.py` · `scripts/load_fact_from_excel.py` · `scripts/load_ruta_rutero_from_excel.py` · `scripts/cliente_mvs.py` · `scripts/cg_route_weekly_local_lab.py` · `tests/test_cg_route_weekly_local_lab.py` · vistas de compliance · `research/AI_SHARED_MEMORY.json` / `AI_BACKLOG.json` · `data/photo-excel-admin_*.xlsx` · locks de gobernanza salvo campos de closeout autorizados.



## Final recommendation for ChatGPT/Bastian/Codex



**Proceder a implementación Codex bajo APPROVE_WITH_WARNINGS, condicionado a:** (1) ChatGPT fija por escrito la definición local de las 7 métricas de reconciliación antes de que Codex escriba código (cierra riesgo #5); (2) Codex implementa el validador nuevo + tests adversariales sin tocar el loader (cierra #6 y la ambigüedad de scope); (3) el flag tautológico de presencia binaria y la ausencia de guardas de claves nulas (#1, #3) se resuelven en los tests/validador nuevos — son los dos hallazgos que más devalúan la evidencia actual; (4) el test de paridad de normalización foto↔ruta (#4) se trata como bloqueante para el futuro gate RED, porque es el mecanismo real por el que Route B rompería el denominador en producción. Con esas cuatro condiciones, 013 puede cerrar con evidencia auditable y el verdicto final GO/NO-GO queda bien fundado para Bastián.
