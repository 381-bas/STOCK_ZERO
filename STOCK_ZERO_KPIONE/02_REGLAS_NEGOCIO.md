# Reglas de Negocio

| regla | definicion | dominio | capa recomendada | evidencia | criticidad | riesgo si se implementa mal |
|---|---|---|---|---|---|---|
| Venta 0 | SKU con `Venta(+7)=0` o venta equivalente semanal en cero | Reposicion, Cliente | SQL/modelo | `app/db.py:get_kpis_local*`, `app/exports.py:_venta_0_flag` | alta | sub/sobre estimar urgencias de reposicion |
| Negativo | flag `NEGATIVO='SI'` | Reposicion, Cliente | SQL/modelo | `app/db.py:get_kpis_local*` | alta | omitir ajustes de inventario |
| Riesgo de quiebre | flag `RIESGO DE QUIEBRE='SI'` | Reposicion, Cliente | SQL/modelo | `app/db.py:get_kpis_local*` | alta | perder quiebres accionables |
| Otros | observacion no vacia y distinta de `NO`, `N/A`, `NA`, `-` | Reposicion, Cliente | SQL/modelo + export | `app/db.py:get_kpis_local*`, `app/exports.py:_clean_otros` | media | ruido operativo o perdida de observaciones |
| Fecha/data freshness | fecha maxima de stock o `v_data_version` equivalente | Todos | Backend/SQL | `app/db.py:get_data_version*` | alta | decisiones con datos vencidos |
| Cliente scope | universo agregable por cliente/responsable/local/SKU | Cliente | SQL/modelo | `get_kpis_scope_cliente`, `get_tabla_scope_*` | alta | rankings inconsistentes |
| `cod_rt + cliente_norm/marca_norm` | combinacion normalizada para cruzar local, cliente/marca y stock | Cliente, Reposicion | SQL/modelo | evidence Q02, scope views | alta | duplicidades y falsos faltantes |
| Rutero/Reponedor/Modalidad | filtros que restringen universo MERCADERISTA | Reposicion | Backend/API + SQL | `get_locales_por_modalidad_rr`, `_rr_scope_exists` | alta | mostrar locales fuera de ruta |
| Gestor/Supervisor | responsables para ranking y scope | Cliente, CG | SQL/modelo | `get_responsables_home_scope`, CG v2 | alta | asignacion operativa equivocada |
| Cumplimiento semanal | compara visita planificada contra realizada por semana | CG | SQL/modelo | `get_cg_scope_kpis`, `get_cg_v2_scope_kpis` | critica | KPI de cumplimiento falso |
| Visita planificada vs realizada | separar exigida/plan de evidencia efectiva | CG v2 | SQL/modelo | `VISITA`, `VISITA_REALIZADA_*` | critica | inflar o castigar cumplimiento |
| Cap de visita valida | visitas validas no deben exceder plan para cumplimiento base | CG v2 | SQL/modelo | `VISITA_REALIZADA_CAP`, `SOBRE_CUMPLIMIENTO` | critica | sobrecumplimiento infla cumplimiento |
| Doble/triple marcaje | misma visita detectada en mas de una fuente/dia | CG audit | SQL/modelo | `v_cg_marcaje_multifuente_dia_v2`, audit cards | alta | doble conteo de visitas |
| Multifuente | evidencia cruzada entre KPIONE/POWER_APP | CG | SQL/modelo | `DIAS_KPIONE`, `DIAS_KPIONE2`, `DIAS_POWER_APP` | alta | fuente incorrecta gobierna resultado |
| KPIONE2-first | KPIONE2 gobierna cuando existe evidencia valida | CG v2 | SQL/modelo | `CG_V2_GLOBAL_PROTOTYPE_WARNING` | critica | precedencia operacional incorrecta |
| POWER_APP fallback | POWER_APP solo como fallback por dia unico | CG v2 | SQL/modelo | warning export | critica | duplicar evidencia ya cubierta |
| KPIONE1 audit-only | KPIONE1 queda historico/auditoria, no KPI principal | CG v2 | SQL/modelo/audit | warning export | alta | mezclar historico con operativo |
| Cumple/Incumple | alerta derivada de plan, realizado y pendientes | CG | SQL/modelo | `_cg_v2_status_case`, KPIs | alta | alertas erroneas |
| Export filtrado/global | export filtrado respeta filtros; global cubre semana completa | Export | Backend/export | `build_control_gestion_*`, `get_cg_v2_export_*` | media | entrega datos fuera de scope sin aviso |

## Nota de autoridad

Si una regla se deriva de nombres de columnas o funciones pero falta definicion SQL completa en el evidence packet, queda como regla funcional evidenciada por app y debe cerrarse con evidencia de modelo antes de productivo.
