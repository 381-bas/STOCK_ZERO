# API Response Examples

Estos endpoints son ejemplos conceptuales; informatica puede adaptarlos a su arquitectura.

## `GET /stock/kpis?cod_rt=RTF001`

```json
{
  "scope": "LOCAL",
  "cod_rt": "RTF001",
  "cliente": "ALL",
  "total_skus": 6,
  "venta_0": 2,
  "negativos": 1,
  "quiebres": 2,
  "otros_validos": 1,
  "fecha_stock": "2026-05-12"
}
```

## `GET /stock/items?cod_rt=RTF001&cliente=CLIENTE_UNO`

```json
{
  "rows": [
    {
      "cod_rt": "RTF001",
      "cliente": "CLIENTE_UNO",
      "sku": "FSKU001",
      "producto": "PRODUCTO_FICTICIO_001",
      "stock": 10,
      "venta_7": 0,
      "indicadores": ["VENTA_0"]
    }
  ],
  "total_rows": 3
}
```

## `GET /cliente/scope-summary?responsable_tipo=GESTOR`

```json
{
  "rows": [
    {
      "responsable_tipo": "GESTOR",
      "responsable": "GESTOR_A",
      "cliente": "CLIENTE_UNO",
      "cod_rt": "RTF001",
      "total_skus": 2,
      "venta_0": 1,
      "negativos": 1,
      "quiebres": 0,
      "otros_validos": 0
    }
  ]
}
```

## `GET /cg/weekly?semana_inicio=2026-05-11`

```json
{
  "semana_inicio": "2026-05-11",
  "rows": [
    {
      "cod_rt": "RTF003",
      "cliente": "CLIENTE_DOS",
      "visita": 2,
      "visita_realizada_raw_operativa": 3,
      "visita_realizada_cap": 2,
      "sobrecumplimiento": 1,
      "visitas_pendientes": 0,
      "alerta": "CUMPLE"
    }
  ]
}
```

## `GET /cg/daily-resolution?cod_rt=RTF003`

```json
{
  "rows": [
    {
      "fecha_visita": "2026-05-11",
      "fuente_resuelta": "KPIONE2",
      "visita_valida": 1,
      "doble_marcaje_dia": 1,
      "triple_marcaje_dia": 1,
      "motivo_resolucion": "Triple fuente auditada y una sola visita valida"
    }
  ]
}
```

## `GET /cg/export-summary?semana_inicio=2026-05-11`

```json
{
  "weekly_rows": 10,
  "visita_total": 20,
  "visita_realizada_raw_operativa_total": 16,
  "visita_realizada_cap_total": 14,
  "sobrecumplimiento_total": 2,
  "visitas_pendientes_total": 6,
  "warning": "KPIONE2 primero; POWER_APP fallback sin KPIONE2; KPIONE1 audit-only."
}
```
