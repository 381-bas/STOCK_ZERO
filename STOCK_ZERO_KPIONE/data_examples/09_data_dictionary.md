# Data Dictionary

| campo | tipo sugerido | ejemplo | obligatorio | dominio | regla asociada | riesgo si falta |
|---|---|---|---|---|---|---|
| `cod_rt` | text | `RTF001` | si | todos | clave local/ruta | cruces rotos |
| `local_nombre` | text | `LOCAL_FICTICIO_NORTE` | si | todos | contexto local | tablas/export incompletos |
| `cliente` | text | `CLIENTE_UNO` | si | todos | filtro cliente | scope incorrecto |
| `cliente_norm` | text | `CLIENTE_UNO` | si | cliente/CG | normalizacion | duplicidad |
| `marca` | text | `CLIENTE_UNO` | si | stock/scope | filtro marca | cruce incompleto |
| `marca_norm` | text | `CLIENTE_UNO` | si | stock/scope | normalizacion | falsos faltantes |
| `sku` | text | `FSKU001` | si | stock/scope | detalle SKU | perdida de detalle |
| `producto` | text | `PRODUCTO_FICTICIO_001` | si | stock/scope | descripcion visible | export incompleto |
| `stock` | integer | `-2` | si | reposicion | negativo | KPI falso |
| `venta_7` | integer | `0` | si | reposicion | Venta 0/quiebre | foco errado |
| `negativo` | text/bool | `SI` | si | reposicion | negativo | ajuste omitido |
| `riesgo_quiebre` | text/bool | `SI` | si | reposicion | quiebre | alerta omitida |
| `otros` | text | `Pedido parcial` | no | reposicion | otros valido | ruido o perdida de observacion |
| `fecha` | date | `2026-05-12` | si | freshness | fecha stock | datos vencidos |
| `rutero` | text | `RUTERO_A` | si | reposicion/CG | ruta | scope errado |
| `reponedor` | text | `REPONEDOR_A` | si | reposicion/CG | ejecutor | contexto incompleto |
| `gestor` | text | `GESTOR_A` | si | cliente/CG | responsable | ranking errado |
| `supervisor` | text | `SUPERVISOR_A` | si | cliente | responsable | scope incompleto |
| `modalidad` | text | `MERCADERISTA` | si | reposicion/CG | selector | filtros rotos |
| `responsable_tipo` | text | `GESTOR` | si | cliente | ranking | mezcla de niveles |
| `responsable` | text | `GESTOR_A` | si | cliente | ranking | asignacion errada |
| `responsable_norm` | text | `GESTOR_A` | si | cliente | normalizacion | duplicidad |
| `semana_inicio` | date | `2026-05-11` | si | CG | semana operativa | agrupacion rota |
| `semana_iso` | text | `2026-W20` | recomendado | CG | calendario | auditoria incompleta |
| `visita` | integer | `3` | si | CG | plan semanal | cumplimiento invalido |
| `visita_realizada_raw_operativa` | integer | `4` | si | CG v2 | evidencia operativa raw | sin auditoria |
| `visita_realizada_cap` | integer | `3` | si | CG v2 | cap de visitas validas | inflacion |
| `sobrecumplimiento` | integer | `1` | si | CG v2 | exceso separado | KPI inflado |
| `visitas_pendientes` | integer | `0` | si | CG v2 | pendiente | alerta erronea |
| `alerta` | text | `CUMPLE` | si | CG | cumple/incumple | decision erronea |
| `fecha_visita` | date | `2026-05-11` | si | CG daily | matriz diaria | daily no reconciliable |
| `kpione2_mark` | integer/bool | `1` | si | CG daily | KPIONE2-first | fuente incorrecta |
| `power_app_mark` | integer/bool | `1` | si | CG daily | fallback | duplicidad |
| `kpione1_mark` | integer/bool | `1` | si | CG daily | audit-only | historico infla KPI |
| `fuente_resuelta` | text | `KPIONE2` | si | CG daily | precedencia | resultado ambiguo |
| `visita_valida` | integer/bool | `1` | si | CG daily | cumplimiento | inflacion |
| `audit_only_flag` | integer/bool | `1` | si | CG audit | KPIONE1 audit-only | auditoria perdida |
| `doble_marcaje_dia` | integer/bool | `1` | si | CG audit | doble marcaje | doble conteo |
| `triple_marcaje_dia` | integer/bool | `1` | si | CG audit | multifuente | inflacion |
| `motivo_resolucion` | text | `KPIONE2 prevalece` | recomendado | CG audit | trazabilidad | auditoria pobre |
