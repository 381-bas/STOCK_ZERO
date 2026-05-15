# STOCK_ZERO_KPIONE

Paquete de handoff tecnico-funcional para transferir la logica de STOCK_ZERO / GESTIONZERO al equipo de informatica.

## Que contiene

- Documentacion funcional y tecnica para replicar reglas, flujos y contratos.
- Codigo activo como referencia tecnica en `reference_app/app_active_only`.
- Evidence packet SQL en `evidence`.

`reference_app/app_active_only` contiene una copia de referencia de la app vigente. Esta carpeta es respaldo tecnico para entender UX, consultas, consumo de datos y exportaciones; no representa una arquitectura obligatoria ni un clon a implementar.

## Como leerlo

1. `00_KERNEL_TRANSFERENCIA_STOCK_ZERO.md` para principios y alcance.
2. `01_MAPA_FUNCIONAL_UX.md` para pantallas y filtros.
3. `02_REGLAS_NEGOCIO.md` para reglas criticas.
4. `03_CONTRATOS_DATOS_SQL_BACKEND.md` para datos y endpoints equivalentes.
5. `04_CONTROL_GESTION_LOGICA_OPERATIVA.md` para CG v2.
6. `05_CLIENTE_SCOPE_LOGICA_OPERATIVA.md` y `06_REPOSICION_LOCAL_MERCADERISTA.md` para dominios operativos.
7. `07_EXPORTS_CONTRATO_SALIDA.md` para salidas.
8. `08_CHECKLIST_IMPLEMENTACION_INFORMATICA.md` para aceptacion.
9. `09_SQL_PENDIENTE_DE_EVIDENCIA.md` para brechas.
10. `10_MANIFEST_ARCHIVOS_REFERENCIA.md` para inventario del paquete.

## Que debe hacer informatica

Replicar la logica funcional en su propia arquitectura, preservando reglas, normalizaciones, filtros, KPIs, trazabilidad, freshness y exportaciones. Puede elegir framework, backend, motor SQL y despliegue.

## Que no debe hacer

- No tratar `app_active_only` como clon obligatorio de Streamlit.
- No imponer Supabase por este paquete.
- No mover reglas criticas al frontend.
- No inventar SQL faltante sin evidencia y pruebas.
