# Expected Export Shapes

## Inventario local / mercaderista / cliente

Columnas minimas:

- Fecha stock
- COD_RT
- LOCAL
- CLIENTE
- Sku
- Descripcion del Producto
- Stock
- VENTA 0
- NEGATIVO
- RIESGO DE QUIEBRE
- OTROS
- ACCION SUGERIDA
- GESTOR
- SUPERVISOR
- RUTERO
- REPONEDOR
- MODALIDAD

Validacion de filas:

- Export filtrado por `cod_rt=RTF001` debe contener 6 filas desde `01_stock_ux_sample.csv`.
- Export filtrado por `cod_rt=RTF001` y `cliente=CLIENTE_UNO` debe contener 3 filas.
- Export global de fixture stock debe contener 12 filas.

## Control Gestion v2 filtrado

Columnas minimas:

- SEMANA
- GESTOR
- RUTERO
- COD_RT
- LOCAL
- CLIENTE
- MODALIDAD
- EXIGIDAS SEM.
- LUN
- MAR
- MIE
- JUE
- VIE
- SAB
- DOM
- PENDIENTE
- ALERTA
- GESTION COMPARTIDA
- RUTA COMPARTIDA

Validacion:

- Export filtrado `cod_rt=RTF003` debe mostrar `VISITA=2`, `VISITA_REALIZADA_CAP=2`, `SOBRECUMPLIMIENTO=1`.
- La matriz diaria de `RTF003` debe incluir una fila triple fuente auditada con una sola visita valida.

## Control Gestion v2 global

Hojas esperadas:

- RESUMEN_EJECUTIVO
- RANKING_GESTORES
- RANKING_RUTEROS
- RANKING_CLIENTES
- DETALLE_CALENDARIO
- AUDITORIA_ASOCIADOS

Validacion global:

- Total weekly fixture: 10 filas.
- Suma de `visita` global: 20.
- Suma de `visita_realizada_raw_operativa`: 16.
- Suma de `visita_realizada_cap`: 14.
- Suma de `sobrecumplimiento`: 2.
- Suma de `visitas_pendientes`: 6.

Advertencia obligatoria:

Cumplimiento calculado desde weekly mart v2; KPIONE2 primero; POWER_APP solo fallback cuando KPIONE2 no marca el dia; KPIONE1 audit-only.
