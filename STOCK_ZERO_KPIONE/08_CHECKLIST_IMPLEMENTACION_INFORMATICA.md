# Checklist de Implementacion Informatica

## Datos requeridos

- [ ] Stock latest por SKU/local/cliente.
- [ ] Ruta/local/modalidad/rutero/reponedor versionable.
- [ ] Cliente/marca normalizados.
- [ ] Gestor/supervisor/responsable normalizados.
- [ ] Plan semanal y diario de visitas.
- [ ] Evidencia diaria por fuente.
- [ ] Flags de doble/triple marcaje y multifuente.
- [ ] Freshness: fecha datos e ingesta.

## Backend/API

- [ ] Endpoints de selectores.
- [ ] Endpoints KPI stock.
- [ ] Endpoint tabla SKU paginada.
- [ ] Endpoints cliente scope.
- [ ] Endpoints CG v2 weekly/daily/audit.
- [ ] Endpoints de export.
- [ ] Validacion de parametros y scopes.

## SQL/modelo

- [ ] Calcular reglas criticas fuera del frontend.
- [ ] Normalizar claves.
- [ ] Resolver precedencia KPIONE2/POWER_APP/KPIONE1.
- [ ] Separar raw, cap y sobrecumplimiento.
- [ ] Generar audit views o equivalentes.
- [ ] Reconciliar totales con detalle.

## UX

- [ ] Mantener modos LOCAL, MERCADERISTA, CLIENTE y CG v2.
- [ ] Mantener secuencia de filtros.
- [ ] Mostrar freshness.
- [ ] Mostrar estados sin datos como estados operativos.
- [ ] Evitar calculos criticos en componentes visuales.

## Export

- [ ] Inventario local.
- [ ] Inventario mercaderista.
- [ ] Inventario cliente.
- [ ] CG v2 filtrado.
- [ ] CG v2 global.
- [ ] Advertencias de fuente y precedencia.

## Seguridad

- [ ] No exponer DSN ni secretos.
- [ ] Roles de lectura para validacion.
- [ ] Separar ambientes.
- [ ] Auditar fecha/fuente de datos.

## Pruebas minimas

- [ ] Caso LOCAL con cliente todos y cliente unico.
- [ ] Caso MERCADERISTA con modalidad/RR/local.
- [ ] Caso CLIENTE L0-L3.
- [ ] Caso CG v2 cumple.
- [ ] Caso CG v2 incumple.
- [ ] Caso doble/triple marcaje.
- [ ] Caso POWER_APP fallback.
- [ ] Export global y filtrado abren correctamente.

## Pruebas de reconciliacion numerica minima

- [ ] KPI total debe cuadrar con detalle filtrado.
- [ ] Detalle filtrado debe cuadrar con export filtrado.
- [ ] Export global debe cuadrar con consulta global.
- [ ] Cumplimiento semanal debe cuadrar con matriz diaria.
- [ ] Visitas validas no deben inflarse por doble/triple marcaje.
- [ ] KPIONE2 debe prevalecer; POWER_APP solo fallback; KPIONE1 audit-only.

## Responsables sugeridos por frente

- Datos: contratos de campos, normalizaciones, freshness y evidencia SQL faltante.
- Backend: endpoints, validacion de parametros, paginacion, export services y reglas de acceso.
- UX: flujos, filtros, estados sin datos, tablas y acciones de descarga.
- QA: reconciliacion numerica, casos de borde, regresiones y validacion de export.
- Negocio: criterios de aceptacion, precedencia de fuentes y aprobacion operativa.

## Criterios de aceptacion

- [ ] KPIs reconciliados contra fuente validada.
- [ ] Sin inflacion por duplicidad.
- [ ] Freshness visible.
- [ ] Export respeta scope.
- [ ] Brechas SQL cerradas con evidencia.
