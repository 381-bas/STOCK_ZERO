# app/Home.py
import os
import streamlit as st
from datetime import datetime

from app import db
from app.exports import build_export_df, export_excel_one_sheet, export_pdf_table

st.set_page_config(page_title="STOCK_ZERO", layout="wide")

# -----------------------------
# Helpers query params (compat)
# -----------------------------
def _qp_get(key: str, default: str = "") -> str:
    try:
        qp = st.query_params  # Streamlit moderno
        v = qp.get(key, default)
        if isinstance(v, list):
            return v[0] if v else default
        return v if v is not None else default
    except Exception:
        qp = st.experimental_get_query_params()
        v = qp.get(key, [default])
        return v[0] if isinstance(v, list) and v else default

def _as_bool(s: str) -> bool:
    s = (s or "").strip().lower()
    return s in {"1", "true", "t", "si", "sí", "y", "yes"}

# -----------------------------
# Access by token in link (?t=)
# -----------------------------
APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
t_in = _qp_get("t", "").strip()

st.title("STOCK_ZERO")
st.caption("Lectura operativa · filtro + búsqueda · export por filtro actual")

if APP_TOKEN and t_in != APP_TOKEN:
    st.error("Link no válido o expirado. Solicita un link actualizado.")
    st.stop()

# -----------------------------
# Init defaults from query params
if "init_done" not in st.session_state:
    st.session_state.init_done = True
    st.session_state.f_search = _qp_get("q", "")
    st.session_state.f_foco = _qp_get("foco", "Todo")  # Todo | Negativos | Riesgo | Negativos + Riesgo
# -----------------------------
# DB read: show real error (debug)
# -----------------------------
try:
    rr = db.qdf("""
        SELECT rutero, reponedor
        FROM public.v_selector_rutero_reponedor
        ORDER BY rutero, reponedor
    """)
except Exception as e:
    st.error("No pude leer datos desde la DB. Revisa DB_URL/Secrets y vistas.")
    with st.expander("Detalles técnicos"):
        st.code(repr(e))
    st.stop()

if rr.empty:
    st.warning("No hay datos para mostrar.")
    st.stop()

rr = rr.copy()
rr["label"] = rr["rutero"].astype(str) + " — " + rr["reponedor"].astype(str)

# Preselección por query params si vienen
qp_rutero = _qp_get("rutero", "")
qp_reponedor = _qp_get("reponedor", "")
default_rr_label = ""
if qp_rutero and qp_reponedor:
    hit = rr[(rr["rutero"].astype(str) == qp_rutero) & (rr["reponedor"].astype(str) == qp_reponedor)]
    if not hit.empty:
        default_rr_label = hit.iloc[0]["label"]

if "sel_rr_label" not in st.session_state:
    st.session_state.sel_rr_label = default_rr_label or rr.iloc[0]["label"]


# -----------------------------
# Filters (auto-update, sin botón)
# -----------------------------
def _reset_on_rr_change():
    # Cambió RUTERO/REPONEDOR => reiniciar dependientes
    st.session_state.sel_local_label = ""   # fuerza recalcular local válido
    st.session_state.sel_marcas = []        # vuelve a "Todas"
    st.session_state.f_search = ""          # (opcional) limpia búsqueda
    st.session_state.f_foco = "Todo"        # (opcional) vuelve a foco base
    

def _reset_on_local_change():
    # Cambió LOCAL => reiniciar marcas (porque cambian las disponibles)
    st.session_state.sel_marcas = []

top1, top2 = st.columns([2, 3], gap="small")

with top1:
    rr_label = st.selectbox(
        "RUTERO — REPONEDOR",
        rr["label"].tolist(),
        key="sel_rr_label",
        on_change=_reset_on_rr_change,
    )

# RR seleccionado (ya está en session_state)
sel_rr = rr[rr["label"] == st.session_state.sel_rr_label].iloc[0]
rutero = sel_rr["rutero"]
reponedor = sel_rr["reponedor"]

# locales (dependen de rr)
try:
    locs = db.qdf("""
        SELECT cod_rt, nombre_local_rr
        FROM public.v_locales_por_ruta
        WHERE rutero=:rutero AND reponedor=:reponedor
        ORDER BY cod_rt
    """, {"rutero": rutero, "reponedor": reponedor})
except Exception as e:
    st.error("No pude leer locales (v_locales_por_ruta).")
    with st.expander("Detalles técnicos"):
        st.code(repr(e))
    st.stop()

if locs.empty:
    st.warning("No hay locales para este RUTERO—REPONEDOR.")
    st.stop()

locs = locs.copy()
locs["label"] = locs["cod_rt"].astype(str) + " — " + locs["nombre_local_rr"].astype(str)
loc_labels = locs["label"].tolist()

# Garantiza que el local actual exista en las opciones (evita “rebote”)
if st.session_state.get("sel_local_label", "") not in loc_labels:
    qp_cod_rt = _qp_get("cod_rt", "").strip()
    if qp_cod_rt:
        hit = locs[locs["cod_rt"].astype(str) == qp_cod_rt]
        st.session_state.sel_local_label = hit.iloc[0]["label"] if not hit.empty else loc_labels[0]
    else:
        st.session_state.sel_local_label = loc_labels[0]

with top2:
    local_label = st.selectbox(
        "LOCAL (COD_RT)",
        loc_labels,
        key="sel_local_label",
        on_change=_reset_on_local_change,
    )

# LOCAL seleccionado (ya está en session_state)
row_loc = locs[locs["label"] == st.session_state.sel_local_label].iloc[0]
cod_rt = row_loc["cod_rt"]
nombre_local_rr = row_loc["nombre_local_rr"]

# marcas (dependen de rr + local)
marcas_disponibles: list[str] = []
try:
    mdf = db.qdf("""
        SELECT DISTINCT marca
        FROM public.v_home_latest
        WHERE rutero=:rutero AND reponedor=:reponedor AND cod_rt=:cod_rt
        ORDER BY marca
    """, {"rutero": rutero, "reponedor": reponedor, "cod_rt": cod_rt})
    marcas_disponibles = mdf["marca"].astype(str).tolist() if not mdf.empty else []
except Exception:
    marcas_disponibles = []

# init de marcas desde query params SOLO 1 vez (primera carga)
if "sel_marcas" not in st.session_state:
    qp_marcas = _qp_get("marcas", "")
    default_marcas = [m.strip() for m in qp_marcas.split(",") if m.strip()]
    st.session_state.sel_marcas = [m for m in default_marcas if m in set(marcas_disponibles)]

# sanitiza selección (evita valores fuera de options)
st.session_state.sel_marcas = [m for m in (st.session_state.sel_marcas or []) if m in set(marcas_disponibles)]

mid1, mid2 = st.columns([3, 2], gap="small")

with mid1:
    st.multiselect(
        "MARCA (opcional)",
        options=marcas_disponibles,
        key="sel_marcas",
        placeholder="Todas",
    )

with mid2:
    foco_opts = ["Todo", "Negativos", "Riesgo", "Negativos + Riesgo"]
    foco_default = st.session_state.f_foco if st.session_state.get("f_foco") in foco_opts else st.session_state.get("f_foco", "Todo")
    # por compat: si viene f_foco de init, úsalo
    if "f_foco" not in st.session_state:
        st.session_state.f_foco = _qp_get("foco", "Todo")
    st.selectbox(
        "Foco operativo",
        foco_opts,
        key="f_foco",
        index=foco_opts.index(st.session_state.f_foco) if st.session_state.f_foco in foco_opts else 0,
    )

st.text_input(
    "Búsqueda (SKU o descripción)",
    key="f_search",
    placeholder="Ej: 779... / galleta / snack...",
)

# Variables finales para el resto del script (mantiene tu estructura actual)
marcas = st.session_state.get("sel_marcas", [])

# -----------------------------
# KPIs + fecha (robusto)
# -----------------------------
marca_filter = ""
kpi_params = {"rutero": rutero, "reponedor": reponedor, "cod_rt": cod_rt}
if marcas:
    marca_filter = "AND marca = ANY(:marcas)"
    kpi_params["marcas"] = marcas

kpis = db.qdf(f"""
    SELECT
      COUNT(*) AS total_skus,
      SUM(CASE WHEN stock < 0 THEN 1 ELSE 0 END) AS negativos,
      SUM(CASE WHEN venta_7 > 0 AND stock > 0 AND stock < venta_7 THEN 1 ELSE 0 END) AS riesgo_quiebre,
      SUM(venta_7) AS venta_total_7,
      SUM(stock) AS stock_total,
      MAX(fecha) AS fecha_datos
    FROM public.v_home_latest
    WHERE rutero=:rutero AND reponedor=:reponedor AND cod_rt=:cod_rt
    {marca_filter}
""", kpi_params)

file_stamp = datetime.now().date().isoformat()
if not kpis.empty and kpis.iloc[0].get("fecha_datos") is not None:
    file_stamp = str(kpis.iloc[0]["fecha_datos"])

c1, c2, c3, c4, c5 = st.columns(5)
if not kpis.empty:
    row = kpis.iloc[0]
    c1.metric("Total SKUs", int(row["total_skus"] or 0))
    c2.metric("Negativos", int(row["negativos"] or 0))
    c3.metric("Riesgo quiebre", int(row["riesgo_quiebre"] or 0))
    c4.metric("Venta(+7) total", int(row["venta_total_7"] or 0))
    c5.metric("Stock total", int(row["stock_total"] or 0))

st.caption(f"Datos al: {file_stamp} · Local: {cod_rt} · {nombre_local_rr}")

# -----------------------------
# Build WHERE filters for v_local_skus_ux
# -----------------------------
only_neg = st.session_state.f_foco in {"Negativos", "Negativos + Riesgo"}
only_risk = st.session_state.f_foco in {"Riesgo", "Negativos + Riesgo"}
search = (st.session_state.f_search or "").strip()

def _where_sql_and_params():
    params = {"rutero": rutero, "reponedor": reponedor, "cod_rt": cod_rt}
    extra = []
    if marcas:
        extra.append('"MARCA" = ANY(:marcas)')
        params["marcas"] = marcas
    if only_neg:
        extra.append("UPPER(COALESCE(\"NEGATIVO\",'NO'))='SI'")
    if only_risk:
        extra.append("UPPER(COALESCE(\"RIESGO DE QUIEBRE\",'NO'))='SI'")
    if search:
        extra.append("(\"Sku\" ILIKE :q OR \"Descripción del Producto\" ILIKE :q)")
        params["q"] = f"%{search}%"
    extra_sql = (" AND " + " AND ".join(extra)) if extra else ""
    return extra_sql, params

extra_sql, base_params = _where_sql_and_params()

# -----------------------------
# Query FULL rows (sin paginación)
# -----------------------------
sql_full = f"""
SELECT
    "MARCA","Sku","Descripción del Producto",
    "Stock","Venta(+7)","NEGATIVO","RIESGO DE QUIEBRE","OTROS"
FROM public.v_local_skus_ux
WHERE rutero=:rutero AND reponedor=:reponedor AND cod_rt=:cod_rt
{extra_sql}
ORDER BY
    "MARCA" ASC,
    CASE WHEN "Sku" ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
    CASE WHEN "Sku" ~ '^[0-9]+$' THEN ("Sku")::bigint END ASC,
    "Sku" ASC,
    "Descripción del Producto" ASC
"""

df = db.qdf(sql_full, base_params)

st.caption(f"Filas: {len(df)}")

df_show = build_export_df(df) if not df.empty else df

# Tabla “base”
st.dataframe(
    df_show[["MARCA","Sku","Descripción del Producto","Stock","Venta(+7)","NEGATIVO","RIESGO DE QUIEBRE","OTROS"]]
    if not df_show.empty else df_show,
    use_container_width=True,
    hide_index=True,
)

# -----------------------------
# Export (por filtro actual) — usa lo ya consultado
# -----------------------------
with st.expander("EXPORTAR (por filtro actual)", expanded=False):
    st.write("Exporta exactamente lo que estás viendo (filtros + búsqueda + foco).")
    if df_show.empty:
        st.info("No hay filas para exportar con el filtro actual.")
    else:
        prep = st.toggle("Preparar export ahora", value=False)
        if prep:
            df_export = build_export_df(df)  # asegura columnas/formatos

            fname_base = f"STOCK_ZERO_{cod_rt}_{file_stamp}"
            excel_bytes = export_excel_one_sheet(cod_rt, df_export)

            pdf_lines = [
                f"STOCK_ZERO · {cod_rt} · {nombre_local_rr}",
                f"RUTERO: {rutero}  |  REPONEDOR: {reponedor}",
                f"Datos al: {file_stamp}  |  Foco: {st.session_state.f_foco}",
                f"Marcas: {', '.join(marcas) if marcas else 'Todas'}  |  Búsqueda: {search if search else '-'}",
            ]
            pdf_bytes = export_pdf_table(pdf_lines, df_export)

            dA, dB = st.columns(2)
            with dA:
                st.download_button(
                    "Descargar Excel (filtrado)",
                    data=excel_bytes,
                    file_name=f"{fname_base}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            with dB:
                st.download_button(
                    "Descargar PDF (filtrado)",
                    data=pdf_bytes,
                    file_name=f"{fname_base}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )