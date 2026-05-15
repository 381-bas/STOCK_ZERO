# Data Examples STOCK_ZERO_KPIONE

## Proposito

Este data pack entrega fixtures pequenos, ficticios y autocontenidos para que informatica pueda implementar y validar la logica funcional de STOCK_ZERO en su propia aplicacion y arquitectura.

## Alcance

- No es un dump real.
- No contiene datos productivos.
- No contiene nombres reales de personas, clientes, locales ni SKU.
- No impone Streamlit, Supabase ni un diseno de API obligatorio.
- No incluye DDL SQL.

## Como leer los fixtures

1. Usar `01_stock_ux_sample.csv` y `02_ruta_rutero_sample.csv` para reposicion LOCAL/MERCADERISTA.
2. Usar `03_cliente_scope_sample.csv` para scope cliente, responsables y rankings.
3. Usar `04_cg_weekly_sample.csv` y `05_cg_daily_evidence_sample.csv` para Control Gestion v2.
4. Comparar resultados contra `06_expected_outputs/`.
5. Usar `07_reconciliation_tests.md` como suite conceptual de pruebas.
6. Usar `08_api_response_examples.md` solo como ejemplos de forma de respuesta, no como arquitectura obligatoria.
7. Usar `09_data_dictionary.md` para campos, tipos y riesgos.

## Validacion local del data pack

Desde la carpeta `STOCK_ZERO_KPIONE`, ejecutar:

```powershell
python data_examples\10_validate_reconciliation.py
```

El runner no ejecuta SQL, no conecta a DB y solo valida CSV locales. Sirve como referencia reproducible para QA e informatica: recalcula expected outputs, revisa reglas de precedencia CG v2, valida caps de visitas y busca patrones obvios de credenciales o URLs.

## Reglas cubiertas

- Venta 0.
- Negativo.
- Riesgo de quiebre.
- Otros valido/no valido.
- Filtro por local y cliente/marca.
- Cruce `cod_rt + cliente_norm/marca_norm`.
- Gestor, supervisor y rankings.
- Visita planificada vs realizada.
- `VISITA_REALIZADA_CAP <= VISITA`.
- Sobrecumplimiento separado.
- KPIONE2 primero.
- POWER_APP solo fallback cuando KPIONE2 no marca el dia.
- KPIONE1 audit-only cuando es unica fuente.
- Doble/triple/multifuente auditado sin inflar visitas.
- Export filtrado/global y reconciliacion.

## Que no cubre

- Datos productivos.
- Volumen o performance.
- Seguridad de ambientes.
- DDL real de vistas/tablas.
- Casuistica completa de todos los clientes o rutas.
