# Cliente Scope - Logica Operativa

## Objetivo

Permitir lectura de focos operativos por cliente y responsables, con rankings por gestor/supervisor, cliente, local y detalle SKU.

## Flujo de filtros

1. Cliente: todos o cliente especifico.
2. Responsable tipo: `Todos`, `GESTOR`, `SUPERVISOR`.
3. Responsable: lista condicionada por tipo y cliente.
4. Scope level: define si se muestra vista global, cliente, responsable o detalle.

## Contrato de responsables

El modelo debe distinguir `responsable_tipo`, `responsable`, `responsable_norm`, `gestor`, `supervisor`, `rutero`, `reponedor` y `modalidad` cuando aplique.

## Normalizacion

`cod_rt`, `cliente_norm` y `marca_norm` son claves para cruzar stock, ruta y scope sin duplicidad. No debe hacerse matching por texto visible sin normalizacion.

## Vistas/objetos actuales

| objeto | uso |
|---|---|
| `public.mv_scope_fact_latest_cliente` | base latest por cliente |
| `public.v_scope_cliente_responsable_fact_bridge` | puente responsable-stock |
| `public.v_scope_cliente_responsable_rr_distinct` | responsables/ruta distintos |
| `public.v_scope_cliente_responsable_summary` | agregados por scope |
| `public.mv_cliente_scope_inventory_enriched` | inventario enriquecido para export/ranking |
| `public.mv_cliente_scope_ranking_cliente` | ranking cliente |
| `public.mv_cliente_scope_ranking_responsable` | ranking responsable |

## Tablas esperadas

- Ranking gestores.
- Ranking supervisores.
- Ranking focos por cliente.
- Locales del cliente/responsable.
- Locales accionables.
- Detalle SKU.

## KPIs

- Total SKUs o SKUs scope.
- Venta 0.
- Negativos.
- Quiebres.
- Otros.
- Locales scope.
- Clientes scope.
- Responsables scope.

## Export inventario cliente

Debe incluir fecha stock, COD_RT, local, cliente, SKU, producto, stock, flags de foco, accion sugerida, gestor, supervisor, rutero, reponedor y modalidad cuando existan.

## Validaciones

- Totales por ranking deben reconciliar con detalle SKU.
- Un mismo SKU/local/cliente no debe duplicarse por multiples responsables sin una regla explicita.
- Los filtros de responsable no deben ocultar stock del cliente fuera del scope seleccionado sin indicarlo.
- Scope sin cruce con stock debe mostrarse como estado valido, no error tecnico.
