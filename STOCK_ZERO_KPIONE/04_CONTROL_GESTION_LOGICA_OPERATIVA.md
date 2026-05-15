# Control Gestion - Logica Operativa

## Prioridad

Control Gestion v2 es el nucleo operativo principal. Legacy queda como compatibilidad, fallback y referencia historica.

## Objetivo

Medir cumplimiento semanal y diario de visitas planificadas contra evidencia real, evitando inflar resultados por multiples fuentes, doble/triple marcaje o datos historicos no gobernantes.

## Fuentes y precedencia

- KPIONE2-first: fuente principal cuando existe evidencia valida.
- POWER_APP fallback: solo se usa como respaldo por dia unico cuando no gobierna KPIONE2.
- KPIONE1 audit-only: historico/auditoria, no KPI principal.

Esta precedencia esta documentada en `app/exports.py` mediante `CG_V2_GLOBAL_PROTOTYPE_WARNING` y reflejada por el consumo de weekly/daily/audit views en `app/db.py`.

## Componentes requeridos

| componente | rol | objeto actual | brecha |
|---|---|---|---|
| Weekly mart | universo semanal resuelto y KPIs | `cg_mart.v_cg_out_weekly_v2` o MV equivalente | definicion no incluida |
| Daily evidence | evidencia por dia resuelta | `cg_core.v_cg_visita_dia_resuelta_v2` | definicion no incluida |
| Frecuencia ruta | plan/frecuencia base | `cg_core.v_rr_frecuencia_base_resuelta_v2` | definicion no incluida |
| Audit multifuente | doble/triple y fuentes | `cg_mart.v_cg_marcaje_multifuente_dia_v2` | definicion no incluida |
| Audit ruta duplicada | ruta duplicada | `cg_mart.v_cg_ruta_duplicados_auditoria_v2` | definicion no incluida |
| Audit fuera cruce | evidencia fuera de cruce real | `cg_mart.v_cg_fuera_cruce_real_v2` | definicion no incluida |
| Audit historico | sin batch ruta semana | `cg_mart.v_cg_sin_batch_ruta_semana_v2` | definicion no incluida |

## KPIs v2

- `% cumplimiento`: `VISITA_REALIZADA_CAP / VISITA` cuando `VISITA > 0`.
- Visitas exigidas: plan semanal.
- Visitas validas: evidencia capada al plan.
- Visitas reportadas: evidencia raw para auditoria.
- Visitas pendientes: plan no cubierto.
- Rutas cumplen/incumplen: conteo por alerta normalizada.
- Sobrecumplimiento: evidencia por sobre plan, no debe inflar cumplimiento base.
- Gestion compartida / ruta compartida: asociaciones de mas de una persona/ruta.

## Filtros v2

- Semana operativa.
- Gestor.
- Vista de analisis: rutero, local o cliente.
- Foco seleccionado segun vista.
- Alerta.

## Matriz diaria

Debe mostrar dias planificados y evidencia registrada por dia. La UX actual representa `1 = dia planificado` y check para evidencia. Informatica puede cambiar iconografia, pero no la semantica.

## Exportaciones

- Export global semanal: resumen, rankings gestores/ruteros/clientes, detalle calendario, auditoria asociados.
- Export filtrado v2: contexto, resumen y detalle para semana/filtros.

## Validaciones minimas

- Ninguna visita debe contarse dos veces por multiples fuentes.
- `VISITA_REALIZADA_CAP` no debe superar `VISITA` para cumplimiento base.
- `SOBRE_CUMPLIMIENTO` debe mantenerse separado.
- KPIONE1 no debe gobernar KPI principal.
- POWER_APP no debe reemplazar KPIONE2 cuando KPIONE2 existe.
- Alertas `CUMPLE` / `INCUMPLE` deben coincidir con plan, evidencia valida y pendientes.

## Legacy

Legacy consume objetos `public.v_cg_*` para compatibilidad: cumplimiento semanal scope, parity, alertas, inicio jefe/gestor y detalle. Puede mantenerse como referencia hasta que v2 sea validado por informatica.
