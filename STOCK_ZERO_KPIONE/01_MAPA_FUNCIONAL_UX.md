# Mapa Funcional UX

## Resumen de modos

| modo/pantalla | objetivo usuario | filtros | KPIs visibles | tablas/datos | acciones | adaptable por informatica |
|---|---|---|---|---|---|---|
| Home / navegacion | elegir consulta y validar token | `t`, `modo`, `q`, `foco`, `cod_rt`, `modalidad`, `rutero`, `reponedor` | no aplica | no aplica | entrar a modo | framework, layout, autenticacion |
| LOCAL | revisar stock accionable de un local | local `cod_rt`, cliente | Venta 0, Negativo, Quiebres, Otros, total SKUs, fecha stock | SKU, producto, stock, indicadores | descargar inventario | UX adaptable; comportamiento funcional obligatorio; reglas criticas fuera de UX |
| MERCADERISTA | revisar stock por modalidad, rutero, reponedor y local | modalidad, rutero-reponedor, local, cliente | mismos KPIs de reposicion | SKU, producto, stock, indicadores | descargar inventario/foco/local | UX adaptable; comportamiento funcional obligatorio; reglas criticas fuera de UX |
| CLIENTE scope | ver universo por cliente y responsables | cliente, responsable tipo, responsable | focos, locales, clientes, responsables, SKUs | rankings, locales, detalle SKU | export inventario cliente | UX adaptable; comportamiento funcional obligatorio; reglas criticas fuera de UX |
| Control Gestion v2 | validar cumplimiento semanal/diario | semana, gestor, vista, rutero/local/cliente, alerta | cumplimiento, visitas, pendientes, sobrecumplimiento, gestion compartida | resumen competitivo, matriz diaria, auditoria | export global y filtrado | UX adaptable; comportamiento funcional obligatorio; reglas criticas fuera de UX |
| Control Gestion legacy | fallback/compatibilidad | semana, gestor, cliente, alerta | cumple, incumple, doble marcaje, locales | scope, alertas, detalle evidencia, parity | consulta operativa | puede reemplazarse por v2 validado |
| Exports | entregar evidencia operativa | scope visible o semana global | resumen segun export | hojas/columnas definidas | descargar Excel/PDF | formato visual adaptable |

## Home / navegacion

La app activa usa `app/Home.py` como entrada. Aplica token gate mediante `APP_TOKEN`, inicializa estado, lee query params y deriva a `render_reposicion`, `render_cliente` o `render_control_gestion`.

Informatica puede reemplazar Streamlit por su autenticacion y router, manteniendo los modos funcionales.

## LOCAL

Flujo: seleccionar local, seleccionar cliente opcional, cargar contexto, mostrar KPIs de stock, mostrar tabla paginada y descargar inventario. En LOCAL los filtros de marca/foco/busqueda quedan neutralizados en la pantalla activa para priorizar inventario por local/cliente.

## MERCADERISTA

Flujo: seleccionar modalidad, rutero-reponedor, local y cliente opcional. El universo queda restringido por ruta y modalidad. La UX debe impedir cargar tablas si falta un selector operativo.

## CLIENTE scope

Flujo: seleccionar cliente, tipo responsable (`Todos`, `GESTOR`, `SUPERVISOR`) y responsable. La app refleja/consume el nivel de scope definido por backend/SQL y muestra ranking por responsable, cliente, local o detalle SKU. El frontend no calcula reglas criticas de scope ni debe recalcular agregados criticos.

## Control Gestion v2

Flujo principal: seleccionar semana reciente, gestor, vista de analisis (`RUTERO`, `LOCAL`, `CLIENTE`), foco y alerta. Muestra KPIs, resumen competitivo, export global, matriz diaria y export filtrado. Si v2 falla, la app actual cae a legacy, pero la transferencia debe priorizar v2.

## Control Gestion legacy

Se conserva como compatibilidad/referencia: semana, gestor, cliente, alerta; cumplimiento semanal, alertas, detalle evidencia y parity.

## Que puede adaptar informatica

- Framework, componentes, estilos, paginacion y autenticacion.
- Nombres de endpoints y arquitectura backend.
- Motor SQL o modelo persistente.

Debe preservar filtros funcionales, resultados, normalizaciones, no duplicidad, freshness y contratos de exportacion.
