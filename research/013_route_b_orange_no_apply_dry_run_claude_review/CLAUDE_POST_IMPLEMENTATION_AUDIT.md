Procedo con la auditoría post-implementación: verifico el diff real contra la base, leo los 4 archivos nuevos y reproduzco la evidencia de pytest de forma independiente.



El diff contra la base confirma el scope. Ahora reviso el test suite y los dos documentos de evidencia.



Falta el documento de evidencia de integración y la reproducción independiente de pytest.



Auditoría completada con verificación independiente: leí los 4 archivos nuevos completos, verifiqué el diff real contra la base `e8421b9` y reproduje pytest con el venv del repo (`22 passed, 8 subtests passed in 12.28s` — coincide con la evidencia declarada; mi primer intento con `python` global falló porque no es el intérprete del repo, no por los tests).



---



# Claude Post-Implementation Audit — 013 ROUTE_B_ORANGE_NO_APPLY_DRY_RUN



## Verdict



**APPROVE_WITH_WARNINGS**



## Scope compliance



**[FACT — verificado, no solo declarado]**



- `git status --porcelain` muestra exactamente los 4 archivos nuevos (`A`), sin modificaciones a archivos existentes.

- `git diff --name-only e8421b9` contra la base de fase lista solo los 4 archivos nuevos más los artefactos de gobernanza/review ya commiteados en la apertura autorizada de fase (772e27f, 37d4b1b, 22f380a). **Ningún archivo prohibido aparece**: los dos loaders protegidos, el contrato activo, SQL, runtime y lógica DB están intactos.

- El validador es read-only por diseño: un solo `--input-json` local, salida por stdout, sin `write_text`, sin imports DB. El test `test_validator_imports_no_db_clients_or_productive_loaders` lo verifica estáticamente vía AST (más robusto que el grep de strings del test 010).

- Todos los payloads (PASS, BLOCK y error) declaran `mode=ORANGE_NO_APPLY_DRY_RUN`, `db_access.used=false`, `sql_apply=false`, `writes_executed=false`, verificado por test.

- El único import de Route B es `load_kpione2_photo_from_excel` **sin modificarlo** — exactamente el patrón que exigí en la revisión previa.



**Respuestas Q1 y Q2: SÍ** en ambas.



## Findings incorporated



Contraste de mis 4 condiciones previas contra el código real (no contra la tabla del evidence doc):



1. **Tautología de presencia binaria — CORREGIDA (Q4: sí).** La prueba ya no es `presence==1`. El validador verifica cardinalidad estructural: cada evento resuelve a exactamente una tupla (sp_item, fecha, semana, cod_rt, cliente) y `day_presence` es el agrupado de tuplas distintas. El test golden afirma `photo_rows=3, event_rows=3, day_presence_rows=1, max_events_per_day_presence=3` — falsificable. El payload declara explícitamente `day_presence_constant_check_used_as_proof=false`.

2. **Proxy débil `photo_rows != distinct_event_ids` — CORREGIDO (Q5: sí).** `legacy_aggregate_inequality_used_as_proof=false`; el fixture legítimo de 1-foto-por-evento pasa aunque el flag legacy del loader importado dé false (se degrada a advisory, no bloqueante — correcto). La prueba real es la partición: cada fila pertenece a exactamente un evento y la suma de filas por evento iguala el total.

3. **Guardas de claves nulas — CORREGIDAS (Q6: sí).** `no_blank_cod_rt` / `no_blank_cliente_norm` bloquean en ambos lados (foto y ruta), con 4 subtests cubriendo `""`, `" "`, `"   "` y `None`. Además, un evento con clave vacía falla `event_row_to_day_presence_structural_pass`.

4. **Definición local de las 7 métricas — HECHA (Q7: sí, con la limitación correcta).** `METRICS_DEFINITION` en el payload + [DENOMINATOR_RECONCILIATION.md](research/013_route_b_orange_no_apply_dry_run_claude_review/DENOMINATOR_RECONCILIATION.md) definen EXIGIDAS/VISITA (ruta-autoritativas), RAW (fechas de presencia distintas), CAP (`min(RAW, VISITA)`), PENDIENTE (`max(VISITA-CAP, 0)`) y ALERTA, etiquetadas honestamente como `controlled_local_sample_only_not_productive_semantics`. El test golden afirma valores por grano calculados a mano, no solo agregados.



Cobertura adicional que pedí y está presente: anomalías `0/0`, total ausente, `3/2`, duplicado exacto de fila; evento cruzando domingo/lunes bloquea por multi-semana; semana_inicio debe ser lunes; presencia foto sin grano de ruta correspondiente **bloquea en vez de crear denominador** (Q8: no — no puede crear grains nuevos: `no_unmatched_photo_day_presence` es bloqueante, y los granos huérfanos se reportan, no se agregan).



**Q3: sí** — el invariante `photo_row -> event_row -> day_presence` está probado estructuralmente, no por proxies.



## Remaining risks



1. **[FACT] Tres flags de reconciliación son verdaderos por construcción, no falsificables:** `route_row_count_unchanged` está **hardcodeado** `True` ([validate_route_b_denominator_dry_run.py:502](scripts/validate_route_b_denominator_dry_run.py:502)); `denominator_delta_zero` y `only_numerator_metrics_changed` no pueden fallar porque `_metrics` recibe el mismo `EXIGIDAS` en before y after. Esto **no es evidencia decorativa engañosa** — el modelo local *define* la ruta como única autoridad del denominador y la protección real y falsificable es `no_unmatched_photo_day_presence` más los deltas golden por grano. Pero el scorecard no debe leer "denominator_delta_zero=true" como prueba empírica de que una implementación productiva no alteraría el denominador: prueba la definición del modelo, no una implementación con libertad de fallar.

2. **[INFERENCE] Paridad de normalización foto↔ruta real: solo parcialmente cubierta.** El validador aplica el `normalize_key` del loader de fotos a *ambos* lados del fixture — consistencia interna del harness, sí; pero no compara contra la normalización del pipeline de ruta real (`load_ruta_rutero_from_excel` / SQL `upper(trim(...))`, que **no** hace strip de acentos, mientras `normalize_key` sí). "MÁRCA" reconcilia aquí y podría no reconciliar en producción. Esta era mi condición (4) previa y la marqué como bloqueante del gate RED — el diferimiento es consistente, pero ningún documento 013 lo registra explícitamente como blocker RED.

3. **[HYPOTHESIS, heredado] Parseo de fechas sin `dayfirst`** en el analizador importado y en `_timestamp` — fragilidad forward-only si un export futuro entrega fechas texto dd/mm.

4. **[FACT, heredado] El loader 010 sigue emitiendo `PASS_ROUTE_B_DRY_RUN` apoyado en sus flags débiles** (intocable en 013, correcto). Futuras fases deben tratar el validador nuevo como la evidencia de grano autoritativa, no el verdict del loader.



## Required fixes before commit, if any



**Ninguno bloqueante.** Dos endurecimientos opcionales (no condicionan el commit; pueden ir en un commit posterior de la misma fase si ChatGPT lo prefiere):

- Computar `route_row_count_unchanged` desde `route_rows_before == route_rows_after` en vez de hardcodearlo (una línea, elimina el único flag literal-True).

- Registrar en el closeout/scorecard de 013 la paridad de normalización cross-loader como **blocker explícito del gate RED** (es documentación research/013, dentro de scope permitido).



## Deferred risks



Correctamente diferidos a fase RED/apply y así declarados en ambos documentos de evidencia: reconciliación before/after contra datos productivos reales; paridad de normalización contra el pipeline de ruta real y las vistas SQL; semántica productiva de VISITA (aquí alias forzado de EXIGIDAS, con `route_visita_matches_exigidas` bloqueante — supuesto del modelo local que producción debe revalidar); DDL apply de sql/15-16; rollback/cutover; backfill.



## Final recommendation for ChatGPT/Bastián/Codex



**Q9/Q10: Autorizar el commit de implementación 013 (APPROVE_WITH_WARNINGS), limitado exactamente a los 4 archivos nuevos nombrados**, con autorización explícita de Bastián como exige la gobernanza (esta auditoría no ejecuta ni autoriza git por sí misma). Las 4 condiciones de mi revisión previa están incorporadas o diferidas de forma consistente con esa revisión; la evidencia de pytest es reproducible (verificada por mí con el venv del repo); el scope no-apply se mantiene verificado, no solo declarado. Condiciones para el cierre de fase (no para el commit): (a) el scorecard debe registrar que `denominator_delta_zero` es verdad-por-construcción del modelo local y que la protección falsificable es el bloqueo de presencia huérfana + los deltas golden; (b) registrar la paridad de normalización foto↔ruta real como blocker nominal del próximo gate RED; (c) el verdict final GO/NO-GO/GO_WITH_LIMITS de 013 debe citar el validador nuevo, no el verdict del loader 010, como evidencia de grano.
