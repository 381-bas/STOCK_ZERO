# Reposicion LOCAL y MERCADERISTA

## Objetivo

Entregar al usuario operativo una lectura accionable de stock por local, cliente y ruta, priorizando Venta 0, Negativo, Riesgo de quiebre y Otros.

## LOCAL

Flujo actual:

1. Seleccionar local `cod_rt`.
2. Seleccionar cliente o `Todos`.
3. Cargar contexto de local: mercaderistas/modalidades asociadas.
4. Mostrar KPIs de stock.
5. Mostrar tabla SKU paginada.
6. Descargar inventario.

## MERCADERISTA

Flujo actual:

1. Seleccionar modalidad.
2. Seleccionar rutero-reponedor.
3. Seleccionar local `cod_rt` dentro de esa ruta.
4. Seleccionar cliente o `Todos`.
5. Mostrar KPIs, tabla SKU y export.

## Selectores requeridos

| selector | fuente actual | regla |
|---|---|---|
| local | `public.v_locales_home` | solo locales disponibles |
| cliente local | `public.ruta_rutero` | clientes asociados a local |
| modalidad | `public.v_selector_modalidad` | modalidades disponibles |
| rutero-reponedor | `public.v_selector_rutero_reponedor_modalidad` | condicionado por modalidad |
| local por ruta | `public.v_locales_por_modalidad_rutero` | condicionado por modalidad + RR |

## KPIs

- `total_skus`.
- `venta_0`.
- `negativos`.
- `quiebres`.
- `otros`.
- `fecha_stock`.

Los calculos deben quedar en backend/modelo, no en la tabla visual.

## Tabla SKU

Columnas visibles minimas:

- Fecha stock.
- Cliente cuando no hay cliente unico seleccionado.
- SKU.
- Producto.
- Stock.
- Indicadores.

Indicadores UX concatena acciones derivadas de flags ya calculados.

## Focos

Aunque la app conserva funciones para filtro de foco, la pantalla activa LOCAL/MERCADERISTA prioriza inventario por local/cliente. Si informatica reintroduce filtros de foco, deben usar las mismas reglas de Venta 0, Negativo, Riesgo de quiebre y Otros.

## Export

- Inventario LOCAL.
- Inventario MERCADERISTA.
- Export foco/local en funciones historicas/compatibles.

## Dependencias

- `public.ruta_rutero` para contexto y scope.
- `public.v_stock_local_cliente_ux` para stock, ventas y flags.

## Validaciones

- Local seleccionado debe pertenecer a modalidad/RR en MERCADERISTA.
- Cliente debe pertenecer al local/scope.
- Fecha stock visible debe corresponder al universo consultado.
- Totales KPI deben reconciliar con tabla/export.
