# CLAUDE_CG005F_CORRECTION_REVALIDATION
**Agente:** Claude (revalidación independiente, sin DB)
**Tarea:** CLAUDE_CG005F_BLOCKER_CORRECTION_REVALIDATION_NO_DB
**Fecha:** 2026-06-13
**Commit de corrección:** `9637e08cad2085b0a3ba1de63a333a2edd2a96ba`
**Hallazgos previos:** `ee26d0fc64c3ee3129d2102365ea307bcee80984` (validados sobre `074ee5b`)
**HEAD en revalidación:** `7cb5942` — el working tree es idéntico a `9637e08` para los 3 archivos validados.
**Veredicto:** **VALIDATED_WITH_CORRECTIONS**

> Sin DB, sin DDL, sin Docker, sin `--apply`, sin rollback ejecutado. Solo `py_compile` + suite de tests.
> Esta revalidación no autoriza implementación, DDL, apply ni el laboratorio local.

---

## Veredicto

Los **tres blockers originales de borde-DDL** (F-01 backfill legacy, F-02 contrato `cod_rt_norm`, F-03
supersede de assignment) están **corregidos**, junto con F-05, F-06, F-07, F-09, F-10 y F-11. La unidad ya
**no** está `BLOCKED_BEFORE_DDL`: puede avanzar a un ensayo aislado en PostgreSQL local para aplicar el DDL,
correr dry-run y probar conductualmente F-01/F-02/F-03/F-05/F-06/F-07.

No es un pase limpio. Quedan **dos correcciones** antes de confiar en apply/rollback:

- **F-08 (firma de advisory lock inválida):** `pg_advisory_xact_lock(hashtextextended(...), hashtextextended(...))`
  pasa **dos `bigint`**. PostgreSQL solo provee `pg_advisory_xact_lock(bigint)` y
  `pg_advisory_xact_lock(integer, integer)`; `int8` no castea implícitamente a `int4`, así que **no hay
  overload que matchee** y la llamada **lanzará en tiempo de apply/rollback**. Los tests mockean el cursor
  (`FakeCursor`, test_56) y solo verifican el substring → **no detectan el error**.
- **F-04 (rollback sin postcheck completo):** el rollback ahora **sí restaura** `public.ruta_rutero` desde el
  batch anterior (`distinct on (row_hash)`), reactiva el assignment previo y marca el fallido `ROLLED_BACK`,
  pero **hace `commit` sin** afirmar que el hash restaurado iguala al hash almacenado del assignment previo
  ni revalidar las vistas semanal/resuelta ni el conjunto de granos. Per criterio del contrato, *no se acepta
  como completo un rollback que solo restaura filas y actualiza estados*.

---

## Validación 1 — Integridad (PASS)

- `python -m py_compile scripts/load_ruta_rutero_from_excel.py` → **COMPILE_OK**.
- `python -m unittest ... test_load_ruta_rutero_weekly_replace.py` → **70/70 OK**, reproducible.
- SQL conserva cabecera `-- NO APPLY` / `IMPLEMENTATION CONTRACT ONLY` / `REQUIRES SEPARATE BASTIAN AUTHORIZATION`; archivo envuelto en `begin; ... rollback;`.
- Default no-write (no-apply ⇒ dry-run; `connect_db` no se llama en dry-run/source-check). Sin DB real, sin `shell=True`, sin SQL libre por CLI, DSN redactado.

## Matriz F-01 … F-11

| F | Estado | Núcleo de evidencia | ¿Bloquea lab? | ¿Bloquea Supabase? |
|---|---|---|---|---|
| F-01 | **CORRECTED** | view resuelto consume solo `v_ruta_rutero_latest_week_batch_v2`; `legacy_inferred` excluido por semana ACTIVE; postcheck de grain-set | no | sí |
| F-02 | **CORRECTED** | view expone `cod_rt` y `cod_rt_norm`; loader/postcheck/refresh compatibles | no | sí |
| F-03 | **CORRECTED** | `supersede_active_assignment` + `replaces_ruta_batch_id` + lock + FOR UPDATE + idempotencia | no | sí |
| F-04 | **PARTIAL** | restauración + reactivación + ROLLED_BACK presentes; **falta postcheck completo pre-commit** | no | sí |
| F-05 | **CORRECTED** | postcheck completo: hashes, grano duplicado (cod_rt_norm), grain-set vs batch, no-stale | no | sí |
| F-06 | **CORRECTED** | un único `canonical_current_surface_hash` (SHA-256 source_row\|row_hash) en Python y sobre filas DB; comparado | no | sí |
| F-07 | **CORRECTED** | `logical_rank` sin `source_row`; campos normalizados + `row_hash`; BD-CG005B-001 APROBADO | no | sí |
| F-08 | **FAILED** | `pg_advisory_xact_lock(bigint,bigint)` no existe; firma inválida; tests la ocultan | no | sí |
| F-09 | **CORRECTED** | apply rechaza `--skip-source-check` (test_65) | no | sí |
| F-10 | **CORRECTED** | warning `workbook_has_no_intrinsic_effective_week` (loader 711); semana obligatoria/lunes | no | sí |
| F-11 | **CORRECTED** | sin `cur.execute('BEGIN')` redundante (test_66) | no | sí |

Conteo: **9 CORRECTED · 1 PARTIAL (F-04) · 1 FAILED (F-08)**.

## Detalle de las dos correcciones pendientes

### F-08 — firma del advisory lock (INVALID_SIGNATURE)
`acquire_week_assignment_lock` (loader 1104-1113):
```sql
select pg_advisory_xact_lock(hashtextextended(%s, 0), hashtextextended(%s, 0))
```
`hashtextextended(text, bigint) → bigint`. Overloads disponibles: `pg_advisory_xact_lock(bigint)` y
`pg_advisory_xact_lock(integer, integer)`. Dos `bigint` no matchean `(int4,int4)` porque `int8→int4` es cast
de asignación, no implícito → **error en ejecución** (`function pg_advisory_xact_lock(bigint, bigint) does not
exist`). Falla cerrado (antes de cualquier escritura). **Corrección:** llave única bigint
`pg_advisory_xact_lock(hashtextextended(%s || '|' || %s, 0))`, o forma de dos `int4`
`pg_advisory_xact_lock(hashtext(%s), hashtext(%s))`; añadir un test contra PostgreSQL real.

### F-04 — postcheck de rollback
`run_weekly_replacement_rollback` (loader 1581-1693) restaura y actualiza estados, pero en 1682 hace `commit`
sin: (a) afirmar `restored current_surface_hash == previous assignment.current_surface_hash`; (b) revalidar
`v_ruta_rutero_load_batch_week_v2` (ahora EXPLICIT apunta al batch reactivado); (c) revalidar el view resuelto
y el grain-set. **Corrección:** ejecutar un postcheck completo antes del commit del rollback.

## Validación 10 — fortaleza de tests

70 tests, **ninguno** corre contra PostgreSQL real. Estilos: *pure unit* (transformaciones, hashing, sort
keys, idempotencia), *SQL/substring o texto fuente* (contrato SQL, `POSTCHECK_CONTRACT`, source text),
*mocked transaction* (`FakeCursor`/`connect_db` mockeado), *semantic fixture* (CLI/guards + workbook fixture).
Varios verdes afirman **estructura, no comportamiento**.

**Afirmaciones que no pueden probarse sin DB real (local_db_gaps):** exclusión real de backfill legacy y de
las 70 bajas (F-01); `verify_db_contract` contra el view desplegado con `cod_rt_norm` (F-02); índice ACTIVE
único + ciclo supersede→insert bajo constraints reales (F-03); restauración + postcheck completo del rollback
(F-04); ejecución de los queries de postcheck contra vistas reales (F-05); igualdad del hash recomputado en DB
vs plan sobre una superficie real (F-06); order-independence del `ORDER BY` a nivel DB (F-07); validez de la
firma del advisory lock (F-08, alto-confianza INVÁLIDA).

## Recomendación

Avanzar a un **laboratorio local PostgreSQL autorizado** para aplicar el DDL (`sql/11`), correr dry-run y
**probar conductualmente** F-01/F-02/F-03/F-05/F-06/F-07. **Exigir** las correcciones de F-08 (firma del lock)
y F-04 (postcheck del rollback) **antes** de cualquier apply o rollback. Sin Supabase, sin retención, sin
limpieza. Decisión de fase: ChatGPT/Bastián.
