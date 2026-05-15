# Exports - Contrato de Salida

## Principio

Las exportaciones son contrato funcional de entrega. El formato visual puede adaptarse, pero columnas minimas, scope y advertencias deben conservarse.

## Inventario LOCAL / MERCADERISTA / CLIENTE

Columnas minimas esperadas:

- Fecha stock.
- COD_RT.
- LOCAL.
- CLIENTE.
- Sku.
- Descripcion del Producto.
- Stock.
- VENTA 0.
- NEGATIVO.
- RIESGO DE QUIEBRE.
- OTROS.
- ACCION SUGERIDA.
- GESTOR.
- SUPERVISOR.
- RUTERO.
- REPONEDOR.
- MODALIDAD.

## Export foco reposicion

Columnas minimas:

- Fecha stock.
- COD_RT.
- LOCAL.
- CLIENTE.
- RUTERO.
- REPONEDOR.
- MARCA.
- Sku.
- Descripcion del Producto.
- Stock.
- FOCO PRINCIPAL.
- ACCION SUGERIDA.
- VENTA 0.
- NEGATIVO.
- RIESGO DE QUIEBRE.
- OTROS.

## CG v2 filtrado

Columnas minimas de detalle:

- SEMANA.
- GESTOR.
- RUTERO.
- COD_RT.
- LOCAL.
- CLIENTE.
- MODALIDAD.
- EXIGIDAS SEM.
- LUN, MAR, MIE, JUE, VIE, SAB, DOM.
- PENDIENTE.
- ALERTA.
- GESTION COMPARTIDA.
- RUTA COMPARTIDA.

Debe incluir contexto: semana, gestor, vista, foco, alerta, fuente weekly y fecha de generacion.

## CG v2 global

Hojas esperadas:

- `RESUMEN_EJECUTIVO`.
- `RANKING_GESTORES`.
- `RANKING_RUTEROS`.
- `RANKING_CLIENTES`.
- `DETALLE_CALENDARIO`.
- `AUDITORIA_ASOCIADOS`.

Debe incluir advertencia operativa: cumplimiento calculado desde weekly mart v2; precedencia KPIONE2 primero; POWER_APP fallback por dia unico; KPIONE1 audit-only.

## Reglas de formato

- Congelar encabezados cuando aplique.
- Autoajustar columnas o entregar ancho legible.
- Mantener tipos numericos para conteos.
- Evitar ocultar columnas criticas.
- Sanitizar nombres de archivo sin perder `cod_rt`, semana o fecha.

## Filtrado vs global

Export filtrado debe reflejar filtros visibles. Export global debe declarar que cubre la semana completa. No mezclar ambos sin etiqueta.
