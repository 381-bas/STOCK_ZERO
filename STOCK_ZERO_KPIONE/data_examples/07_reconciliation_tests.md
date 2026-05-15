# Reconciliation Tests

Estas pruebas son conceptuales y pueden implementarse en el framework que informatica prefiera.

## Runner local

El archivo `10_validate_reconciliation.py` automatiza las pruebas principales de este documento usando solo CSV locales. Desde `STOCK_ZERO_KPIONE`, ejecutar:

```powershell
python data_examples\10_validate_reconciliation.py
```

El runner recalcula `expected_stock_kpis.csv`, `expected_cliente_scope_summary.csv`, `expected_cliente_scope_responsable_totals.csv`, `expected_cg_daily_resolution.csv` y `expected_cg_weekly_result.csv`; tambien valida caps, precedencia KPIONE2/POWER_APP/KPIONE1, doble/triple sin inflacion y ausencia de credenciales/URLs obvias.

## Stock

| prueba | entrada | salida esperada | criterio |
|---|---|---|---|
| KPI total vs detalle | `01_stock_ux_sample.csv` | `expected_stock_kpis.csv` GLOBAL | exact match |
| Local RTF001 | filtrar `cod_rt=RTF001` | 6 SKUs, 2 Venta 0, 1 negativo, 2 quiebres, 1 otros | exact match |
| Local + cliente | filtrar `cod_rt=RTF001`, `cliente=CLIENTE_UNO` | 3 SKUs, 1 Venta 0, 1 negativo, 1 quiebre | exact match |
| Detalle vs export filtrado | detalle stock filtrado | mismas filas en export | exact match |

## Cliente scope

| prueba | entrada | salida esperada | criterio |
|---|---|---|---|
| Ranking gestor | `03_cliente_scope_sample.csv` | grupos GESTOR en `expected_cliente_scope_summary.csv` | exact match |
| Ranking supervisor | `03_cliente_scope_sample.csv` | grupos SUPERVISOR en expected | exact match |
| Ranking total por responsable | `03_cliente_scope_sample.csv` | `expected_cliente_scope_responsable_totals.csv` | exact match |
| Normalizacion | `Cliente Uno` y `CLIENTE_UNO` | una clave `CLIENTE_UNO` | exact match |
| No duplicidad interna | grupo responsable/cliente/cod_rt | SKU unico por grupo | sin duplicados no justificados |

## Control Gestion v2

| prueba | entrada | salida esperada | criterio |
|---|---|---|---|
| Weekly vs daily | `04_cg_weekly_sample.csv` + `05_cg_daily_evidence_sample.csv` | `expected_cg_weekly_result.csv` | exact match por `semana_inicio,cod_rt,cliente` |
| Fuente resuelta vs raw | daily evidence | `expected_cg_daily_resolution.csv` | exact match |
| Cap visitas validas | weekly sample | `visita_realizada_cap <= visita` | siempre verdadero |
| Sobrecumplimiento separado | RTF003 y RTF009 | cap no supera plan, sobrecumplimiento = 1 | exact match |
| Doble/triple sin inflacion | filas con flags audit | `visita_valida` max 1 por dia | siempre verdadero |
| KPIONE1 audit-only | filas solo KPIONE1 | `visita_valida=0` | siempre verdadero |
| POWER_APP fallback | POWER_APP sin KPIONE2 | `fuente_resuelta=POWER_APP`, `visita_valida=1` | exact match |
| KPIONE2 gana | KPIONE2 con otra fuente | `fuente_resuelta=KPIONE2`, `visita_valida=1` | exact match |

## Export global vs filtrado

- Export global debe cuadrar con consulta global del fixture.
- Export filtrado debe cuadrar con el mismo filtro aplicado al detalle.
- Si se suman grupos, deben ser grupos disjuntos.
- No sumar ranking gestor + ranking supervisor como universo unico salvo que el contrato lo indique.
