# STOCK_ZERO — Registro correctivo multiagente Fase 022

**Estado del documento:** DRAFT_PRE_020B_EXECUTION  
**Fase:** `022_CONTROL_GESTION_ROUTE_B_PRODUCTIVE_APPLY_AND_APP_VALIDATION`  
**Rama:** `codex/022-cg-route-b-june-apply`  
**HEAD técnico vigente:** `75ba94fbb3bba9a0d90565c93a951a66892487db`  
**Gate productivo:** `CLOSED`  
**Apply productivo:** `NOT_STARTED`  
**Merge:** `NOT_AUTHORIZED`

> Este registro consolida la auditoría de Claude, la adjudicación de ChatGPT y la implementación verificada por Codex. No reemplaza Git, el plan 018, la evidencia máquina ni el estado real de base de datos.

---

## 1. Orden de autoridad

1. Git HEAD y archivos tracked.
2. Plan productivo 018.
3. Evidencia máquina validada.
4. Estado real verificado de base de datos.
5. Kernels vigentes.
6. Auditorías y conversaciones de agentes.

Cuando dos fuentes discrepan, se documenta la contradicción y prevalece la fuente superior. Ningún agente puede crear una tercera interpretación por conveniencia.

---

## 2. Evidencia de Claude

### Veredicto

`AMBER`

La arquitectura general de Route B es válida y permanece fail-closed. El bloqueo fue clasificado como defecto local del validador, no como fallo estructural del diseño.

### Hallazgos aceptados

1. El precheck mezclaba dos autoridades de catálogo:
   - objetos y tipos mediante `pg_class`;
   - columnas mediante `information_schema.columns`.

2. La visibilidad de `information_schema.columns` depende de privilegios, por lo que un rol auditor sin `SELECT` podía producir un falso:
   - `route_b_object_column_signature_mismatch`.

3. El plan 018 conserva deliberadamente el estado de preparación hasta completar la evidencia. Su aparente desfase respecto de la DB no debe corregirse antes de cerrar la transición.

4. La estructura física y los permisos del observer debían validarse por separado.

### Hipótesis que Claude dejó sin probar

- **H1:** `stock_zero_codex_ro` carece de `SELECT` sobre uno o más objetos Route B.
- **H2:** las firmas físicas reales coinciden exactamente con `physical_contract.object_signatures`.

### Sugerencias rechazadas

- rediseño general de Route B;
- actualización anticipada del plan 018;
- reestructuración global de credenciales;
- nuevos frameworks de gobernanza;
- refactors de elegancia sin impacto directo en 022;
- automatización adicional del grant fuera del flujo gobernado;
- cambios a `cg_raw.kpione2_raw`.

---

## 3. Adjudicación ChatGPT/Bastián

### Decisiones

- Se aceptó un único parche técnico para separar firma física y privilegios.
- Se rechazó cualquier cambio de plan, SQL, kernels o manifest antes de cerrar la evidencia.
- Se prohibió ejecutar DB desde Codex.
- Se mantuvo la ejecución productiva y administrativa bajo control manual de Bastián.
- Claude queda como auditor puntual, no como gate permanente.
- ChatGPT mantiene la función de contraste, decisión y generación del prompt de implementación.
- Codex implementa, prueba y versiona únicamente el alcance autorizado.

### Regla operativa multiagente

| Actor | Responsabilidad | No puede |
|---|---|---|
| Bastián | Autorizar y ejecutar operaciones sensibles | Delegar decisiones productivas implícitamente |
| ChatGPT | Contrastar evidencia, decidir alcance y emitir prompts | Ejecutar DB o ampliar alcance sin autorización |
| Claude | Auditar contradicciones y consecuencias actuales | Implementar, ejecutar DB o diseñar arquitectura futura |
| Codex | Implementar, probar y hacer commit del alcance aprobado | Acceder a secretos, abrir gate, aplicar producción o mergear |
| Git | Fuente técnica autoritativa | Ser sustituido por conversaciones |

---

## 4. Implementación Codex

### Commit

`75ba94fbb3bba9a0d90565c93a951a66892487db`

Título:

`fix(route-b): reconcile readonly observer access`

### Archivos modificados

- `scripts/precheck_kpione_route_b_018_read_only.py`
- `scripts/kpione_route_b_v1.py`
- `scripts/reconcile_route_b_readonly_observer.py`
- `scripts/invoke_stock_zero_db_operation.ps1`
- `tests/test_018_productive_readiness.py`
- `tests/test_019_database_credential_architecture.py`
- `tests/test_020B_operational_evidence_tooling.py`
- `tests/test_022_readonly_observer_reconciliation.py`

### Correcciones implementadas

- firmas físicas Route B mediante `pg_catalog.pg_attribute`;
- validación de permisos separada de la firma física;
- diferencias estructurales reportadas mediante `mismatched_objects`;
- runner productivo alineado con la misma fuente de catálogo;
- operación administrativa idempotente:
  - `admin-reconcile-route-b-readonly-observer`;
- grant permitido únicamente para:
  - `USAGE` en `cg_raw` y `cg_core`;
  - `SELECT` sobre los cinco objetos Route B;
- rechazo explícito de:
  - DML adicional;
  - `CREATE`;
  - privilegios de secuencia;
  - cambios a PUBLIC;
  - cambios a legacy;
  - cambios a atributos del rol.

### Tests reportados

- `py_compile`: PASS
- parse PowerShell wrapper: PASS
- `test_022_readonly_observer_reconciliation`: 14 PASS
- tests 022 bridge/provisioner: 13 PASS
- tests 019: 16 PASS, 1 skip opt-in
- tests 020B: 16 PASS, 1 skip opt-in
- tests 018: 43 PASS
- preflight generic: WARN, blockers `[]`

### Invariantes

- plan 018 unchanged;
- SQL 17 unchanged;
- manifest unchanged;
- credenciales unchanged;
- evidencia administrativa fuente unchanged;
- gate closed;
- productive apply false;
- push false;
- merge false.

---

## 5. Estado previo a la ejecución 020B v4

### Script manual

`C:\\Users\\basti\\AppData\\Local\\Temp\\stock_zero_022_complete_020b_v4.ps1`

SHA256:

`4cbe706e1bf3b5d8ccd724c5b15ed66bd03ec8922608c31ea3f2af664878d434`

### Run 020B

`a554f464-8213-442c-bf0b-d06b7acc26ca`

### Evidencia administrativa fuente

`evidence/runtime/020B/27dd51bc-fec8-4ce2-9fb6-5f863ac57d26/02_admin_provisioning.json`

### Evidencias nuevas esperadas

1. `01_readonly_baseline.json`
2. `02_admin_provisioning.json`
3. `03_productive_role_verification.json`
4. `04_readonly_postcheck.json`
5. `05_infrastructure_bundle.json`

### Flujo esperado

1. inspección administrativa de firmas físicas;
2. grant mínimo sólo si H2 pasa y H1 confirma el gap;
3. baseline read-only;
4. reconciliación de provisioning comprometido;
5. verificación del rol productivo con rollback;
6. postcheck read-only;
7. bundle;
8. `validate-existing`.

### Stop conditions

- firma física distinta del plan;
- cualquier privilegio adicional al grant mínimo;
- cambio en legacy;
- cambio en ACL de PUBLIC;
- batch activo inesperado;
- evidencia con SHA, run_id o fingerprint inconsistente;
- cualquier intento de abrir gate o ejecutar apply.

---

## 6. Estado pendiente de completar tras ejecutar v4

> Esta sección debe actualizarse sólo con salida real y sanitizada.

```json
{
  "v4_verdict": "PASS_020B_INFRASTRUCTURE_BUNDLE",
  "maintenance_run_id": "757664d1-09bc-48f3-b4a8-25623d8a3b5c",
  "maintenance_evidence_sha256": "700f4c69db418233a6b0cd188c9226db6ae51245f28b0272b26e171f8cc1f510",
  "hypothesis_H1": "PROVEN_PARTIAL_SELECT_GAP_ON_THREE_CG_RAW_TABLES",
  "hypothesis_H2": "PROVEN_PHYSICAL_SIGNATURES_MATCH",
  "readonly_observer_result": "PASS_READONLY_OBSERVER_GRANTS_RECONCILED",
  "evidence_01_05": "PASSED",
  "bundle_sha256": "0cf15c1e39db58e550aed6758748591ce2589bd568ffcaed02435c7840c2c272",
  "active_batches": 0,
  "productive_apply": false,
  "gate": "CLOSED"
}
```

---

## 7. Política de cierre de la fase

No avanzar a apply productivo hasta que:

- H2 sea probada;
- observer access quede compliant;
- las evidencias 01–05 existan;
- `05_infrastructure_bundle.json` tenga `status=PASSED`;
- `validate-existing` pase;
- active batches sea 0;
- legacy y PUBLIC ACL permanezcan invariantes;
- ChatGPT y Bastián autoricen explícitamente la transición del plan.

No actualizar kernels ni plan antes del cierre operativo real.

---

## 8. Commit strategy

Este archivo no debe cambiar el HEAD antes de ejecutar el script v4, porque el script y la evidencia están fijados al commit:

`75ba94fbb3bba9a0d90565c93a951a66892487db`

Después de completar v4:

1. actualizar la sección 6 con resultados reales;
2. revisar evidencia 020B;
3. añadir este único documento al repositorio;
4. hacer un commit documental separado;
5. mantener plan y kernels sin cambios salvo autorización posterior explícita.

Commit documental sugerido:

`docs(route-b): record 022 multi-agent corrective review`

---

## 9. Closeout provisional

```text
AUDIT_CLAUDE          = AMBER, LOCAL_DEFECTS_ONLY
ADJUDICATION          = SINGLE_TECHNICAL_PATCH_ACCEPTED
CODEX_COMMIT          = 75ba94fbb3bba9a0d90565c93a951a66892487db
PLAN_018              = UNCHANGED
SQL_17                = UNCHANGED
OBSERVER_RECONCILE    = IMPLEMENTED_NOT_EXECUTED
020B_EVIDENCE         = PENDING
PRODUCTIVE_APPLY      = FALSE
GATE                  = CLOSED
PUSH                  = FALSE
MERGE                 = FALSE
```
