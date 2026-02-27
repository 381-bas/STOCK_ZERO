# app/Home.py
import os
import time
import math
import logging
import traceback
from datetime import datetime

import streamlit as st

logger = logging.getLogger("stock_zero")

# DEBUG se define en runtime (dentro de main)
DEBUG = False


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


def _dbg(msg: str, **kv) -> None:
    global DEBUG
    ts = datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]
    extra = " ".join([f"{k}={v}" for k, v in kv.items()]) if kv else ""
    line = f"{ts} | {msg}" + (f" | {extra}" if extra else "")

    try:
        logger.info("DBG %s", line)
    except Exception:
        pass

    if not DEBUG:
        return

    st.session_state.setdefault("_dbg_lines", [])
    st.session_state["_dbg_lines"].append(line)

    if len(st.session_state["_dbg_lines"]) > 250:
        st.session_state["_dbg_lines"] = st.session_state["_dbg_lines"][-250:]

    st.session_state["_dbg_last"] = msg


def _dbg_block() -> None:
    global DEBUG
    if not DEBUG:
        return
    st.sidebar.markdown("### 🛠 DEBUG")
    st.sidebar.caption(f"Último paso: {st.session_state.get('_dbg_last', '-')}")
    with st.sidebar.expander("Trace (últimos 250)", expanded=False):
        st.sidebar.code("\n".join(st.session_state.get("_dbg_lines", [])))


def _timed(label: str):
    class _T:
        def __enter__(self_):
            self_.t0 = time.perf_counter()
            _dbg(f"START {label}")
            return self_

        def __exit__(self_, exc_type, exc, tb):
            ms = int((time.perf_counter() - self_.t0) * 1000)
            if exc_type is None:
                _dbg(f"OK {label}", ms=ms)
            else:
                _dbg(f"ERR {label}", ms=ms, exc=str(exc_type.__name__))
            return False

    return _T()


def main():
    global DEBUG

    st.set_page_config(page_title="STOCK_ZERO", layout="wide")

    from app import db
    from app.exports import build_export_df, export_excel_one_sheet, export_pdf_table

    DEBUG = _as_bool(_qp_get("debug", "")) or _as_bool(os.getenv("DEBUG_UI", ""))
    _dbg("BOOT Home.py", py=str(getattr(__import__("sys"), "version", "na")).split()[0])
    _dbg_block()

    # Token gate
    APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
    t_in = _qp_get("t", "").strip()

    st.title("STOCK_ZERO")
    st.caption("Lectura operativa · marcha blanca UX · tabla compacta · export bajo demanda")

    _dbg("TOKEN gate", token_set=bool(APP_TOKEN), t_present=bool(t_in))
    _dbg_block()

    if APP_TOKEN and t_in != APP_TOKEN:
        st.error("Link no válido o expirado. Solicita un link actualizado.")
        st.stop()

    # Init defaults desde query params (solo una vez)
    if "init_done" not in st.session_state:
        st.session_state.init_done = True
        st.session_state.f_search = _qp_get("q", "")
        st.session_state.f_foco = _qp_get("foco", "Todo")
        _dbg("INIT from query params", q=bool(st.session_state.f_search), foco=st.session_state.f_foco)
        _dbg_block()

    # Helpers reset/invalidate
    def _invalidate_runtime_cache():
        st.session_state.pop("_kpis_key", None)
        st.session_state.pop("_kpis_row", None)
        st.session_state.pop("_total_key", None)
        st.session_state.pop("_total_rows", None)
        st.session_state.pop("_export_key", None)
        st.session_state.pop("_export_excel", None)
        st.session_state.pop("_export_pdf", None)

    def _reset_filters_defaults(reason: str):
        # pending (UI)
        st.session_state["sel_marcas"] = []
        st.session_state["f_search"] = ""
        st.session_state["f_foco"] = "Todo"
        # applied (queries)
        st.session_state["applied_marcas"] = []
        st.session_state["applied_search"] = ""
        st.session_state["applied_foco"] = "Todo"
        st.session_state["page"] = 1
        _invalidate_runtime_cache()
        _dbg(f"RESET filters ({reason})")
        _dbg_block()

    def _reset_on_rr_change():
        st.session_state["sel_local_label"] = ""
        _reset_filters_defaults("rr_change")

    def _reset_on_local_change():
        _reset_filters_defaults("local_change")

    # --------------------------------
    # RR selector
    # --------------------------------
    try:
        with _timed("QUERY rr"):
            rr = db.get_rutero_reponedor()
            _dbg("RR loaded", rows=len(rr))
            _dbg_block()
    except Exception as e:
        _dbg("FAIL rr", err=repr(e))
        st.error("No pude leer datos desde la DB. Revisa DB_URL/Secrets y vistas.")
        with st.expander("Detalles técnicos"):
            st.code(repr(e))
            if DEBUG:
                st.code(traceback.format_exc())
        st.stop()

    if rr.empty:
        st.warning("No hay datos para mostrar.")
        st.stop()

    rr = rr.copy()
    rr["label"] = rr["rutero"].astype(str) + " — " + rr["reponedor"].astype(str)

    qp_rutero = _qp_get("rutero", "")
    qp_reponedor = _qp_get("reponedor", "")
    default_rr_label = ""
    if qp_rutero and qp_reponedor:
        hit = rr[(rr["rutero"].astype(str) == qp_rutero) & (rr["reponedor"].astype(str) == qp_reponedor)]
        if not hit.empty:
            default_rr_label = hit.iloc[0]["label"]

    RR_PLACEHOLDER = "— Selecciona —"
    rr_opts = [RR_PLACEHOLDER] + rr["label"].tolist()

    if "sel_rr_label" not in st.session_state:
        st.session_state["sel_rr_label"] = default_rr_label or RR_PLACEHOLDER

    top1, top2 = st.columns([2, 3], gap="small")

    with top1:
        st.selectbox(
            "RUTERO — REPONEDOR",
            rr_opts,
            key="sel_rr_label",
            on_change=_reset_on_rr_change,
        )

    if st.session_state["sel_rr_label"] == RR_PLACEHOLDER:
        st.info("Selecciona un RUTERO—REPONEDOR para cargar locales, KPIs y tabla.")
        st.stop()

    hit_rr = rr.loc[rr["label"] == st.session_state["sel_rr_label"]]
    if hit_rr.empty:
        _dbg("ERR rr selection invalid", sel=st.session_state["sel_rr_label"])
        st.error("Selección de RUTERO—REPONEDOR inválida (rerun). Vuelve a seleccionar.")
        st.stop()

    sel_rr = hit_rr.iloc[0]
    rutero = sel_rr["rutero"]
    reponedor = sel_rr["reponedor"]
    _dbg("RR selected", rutero=rutero, reponedor=reponedor)
    _dbg_block()

    # --------------------------------
    # Locales (depende de rr)
    # --------------------------------
    try:
        with _timed("QUERY locs"):
            locs = db.get_locales(rutero, reponedor)
            _dbg("LOCS loaded", rows=len(locs))
            _dbg_block()
    except Exception as e:
        _dbg("FAIL locs", err=repr(e))
        st.error("No pude leer locales (v_locales_por_ruta).")
        with st.expander("Detalles técnicos"):
            st.code(repr(e))
            if DEBUG:
                st.code(traceback.format_exc())
        st.stop()

    if locs.empty:
        st.warning("No hay locales para este RUTERO—REPONEDOR.")
        st.stop()

    locs = locs.copy()
    locs["label"] = locs["cod_rt"].astype(str) + " — " + locs["nombre_local_rr"].astype(str)
    loc_labels = locs["label"].tolist()

    if st.session_state.get("sel_local_label", "") not in loc_labels:
        qp_cod_rt = _qp_get("cod_rt", "").strip()
        if qp_cod_rt:
            hit = locs.loc[locs["cod_rt"].astype(str) == qp_cod_rt, "label"]
            st.session_state["sel_local_label"] = hit.iloc[0] if not hit.empty else loc_labels[0]
        else:
            st.session_state["sel_local_label"] = loc_labels[0]

    with top2:
        st.selectbox(
            "LOCAL (COD_RT)",
            loc_labels,
            key="sel_local_label",
            on_change=_reset_on_local_change,
        )

    hit_loc = locs.loc[locs["label"] == st.session_state["sel_local_label"]]
    if hit_loc.empty:
        _dbg("ERR local selection invalid", sel=st.session_state["sel_local_label"])
        st.error("El local seleccionado ya no está disponible (rerun). Vuelve a seleccionar.")
        st.stop()

    row_loc = hit_loc.iloc[0]
    cod_rt = row_loc["cod_rt"]
    nombre_local_rr = row_loc["nombre_local_rr"]
    _dbg("LOCAL selected", cod_rt=cod_rt)
    _dbg_block()

    # Estado 2: al cambiar RR/Local, setea defaults aplicados
    sel_key = f"{rutero}|{reponedor}|{cod_rt}"
    if st.session_state.get("_sel_key") != sel_key:
        st.session_state["_sel_key"] = sel_key
        _reset_filters_defaults("rr_local_ready")

    # Datos al (global)
    file_stamp = datetime.now().date().isoformat()
    try:
        with _timed("QUERY data_version_info"):
            dv_info = db.get_data_version_info()
            if dv_info and dv_info.get("fecha_datos") is not None:
                file_stamp = str(dv_info["fecha_datos"])
    except Exception:
        pass

    # Marcas disponibles
    try:
        with _timed("QUERY marcas"):
            marcas_disponibles = db.get_marcas(rutero, reponedor, cod_rt)
            _dbg("MARCAS loaded", n=len(marcas_disponibles))
            _dbg_block()
    except Exception as e:
        _dbg("FAIL marcas", err=repr(e))
        marcas_disponibles = []

    st.session_state.setdefault("sel_marcas", [])
    mset = set(marcas_disponibles)
    st.session_state["sel_marcas"] = [m for m in (st.session_state.get("sel_marcas") or []) if m in mset]

    # Snapshot applied (Estado 2 = defaults; Estado 3 = button)
    marcas = list(st.session_state.get("applied_marcas", []) or [])
    foco_ap = st.session_state.get("applied_foco", "Todo")
    search_ap = (st.session_state.get("applied_search", "") or "").strip()

    # DV (para invalidar caches de home)
    dv = db.get_data_version()

    # --------------------------------
    # KPIs (4) — post-local
    # --------------------------------
    kpis_key = (dv, rutero, reponedor, cod_rt, tuple(marcas))
    kpis_row = st.session_state.get("_kpis_row")

    if st.session_state.get("_kpis_key") != kpis_key or kpis_row is None:
        try:
            with _timed("QUERY kpis"):
                kpis = db.get_kpis_local(rutero, reponedor, cod_rt, marcas)
                _dbg("KPIS loaded", rows=0 if kpis is None else len(kpis))
                _dbg_block()
            kpis_row = dict(kpis.iloc[0]) if (kpis is not None and not kpis.empty) else None
            st.session_state["_kpis_key"] = kpis_key
            st.session_state["_kpis_row"] = kpis_row
        except Exception as e:
            _dbg("FAIL kpis", err=repr(e))
            st.session_state["_kpis_key"] = kpis_key
            st.session_state["_kpis_row"] = None
            kpis_row = None

    k1, k2, k3, k4 = st.columns(4, gap="small")
    if kpis_row:
        k1.metric("Venta 0", int(kpis_row.get("venta_0") or 0))
        k1.caption("Productos sin rotación (Prioridad Alta).")

        k2.metric("Negativo", int(kpis_row.get("negativos") or 0))
        k2.caption("Realizar ajuste de inventario.")

        k3.metric("Quiebres", int(kpis_row.get("quiebres") or 0))
        k3.caption("Solicitar empuje.")

        k4.metric("Otros", int(kpis_row.get("otros") or 0))
        k4.caption("Observaciones cliente.")

    # --------------------------------
    # Filtros secundarios (debajo KPIs)
    # --------------------------------
    def _apply_filters():
        st.session_state["applied_marcas"] = list(st.session_state.get("sel_marcas", []) or [])
        st.session_state["applied_foco"] = st.session_state.get("f_foco", "Todo")
        st.session_state["applied_search"] = (st.session_state.get("f_search", "") or "").strip()
        st.session_state["page"] = 1
        _invalidate_runtime_cache()
        _dbg(
            "APPLY filters",
            marcas=len(st.session_state["applied_marcas"]),
            foco=st.session_state["applied_foco"],
            q=bool(st.session_state["applied_search"]),
        )
        _dbg_block()

    with st.form("filters_form", clear_on_submit=False):
        r1c1, r1c2 = st.columns([3, 2], gap="small")
        with r1c1:
            st.multiselect(
                "MARCA (opcional)",
                options=marcas_disponibles,
                key="sel_marcas",
                placeholder="Todas",
            )
        with r1c2:
            foco_opts = ["Todo", "Venta 0", "Negativo", "Quiebres", "Otros"]
            current = st.session_state.get("f_foco", "Todo")
            st.selectbox(
                "Foco operativo",
                foco_opts,
                key="f_foco",
                index=foco_opts.index(current) if current in foco_opts else 0,
            )

        r2c1, r2c2 = st.columns([4, 1], gap="small")
        with r2c1:
            st.text_input(
                "Búsqueda (SKU o descripción)",
                key="f_search",
                placeholder="Ej: 779... / galleta / snack... (mín 2 caracteres)",
            )
        with r2c2:
            st.form_submit_button("Aplicar", on_click=_apply_filters)

    # Snapshot applied (luego de aplicar)
    marcas = list(st.session_state.get("applied_marcas", []) or [])
    foco_ap = st.session_state.get("applied_foco", "Todo")
    search_ap = (st.session_state.get("applied_search", "") or "").strip()

    # --------------------------------
    # TABLA paginada (25)
    # --------------------------------
    page_size = 25
    st.session_state.setdefault("page", 1)

    # total rápido desde KPIs cuando corresponde
    max_m = int(os.getenv("MAX_MARCA_FILTER", "50"))
    can_use_kpi_total = (
        len((search_ap or "").strip()) < 2
        and (not marcas or len(marcas) <= max_m)
        and bool(kpis_row)
    )
    kpi_total_rows = None
    if can_use_kpi_total:
        if foco_ap == "Todo":
            kpi_total_rows = int(kpis_row.get("total_skus") or 0)
        elif foco_ap == "Venta 0":
            kpi_total_rows = int(kpis_row.get("venta_0") or 0)
        elif foco_ap == "Negativo":
            kpi_total_rows = int(kpis_row.get("negativos") or 0)
        elif foco_ap == "Quiebres":
            kpi_total_rows = int(kpis_row.get("quiebres") or 0)
        elif foco_ap == "Otros":
            kpi_total_rows = int(kpis_row.get("otros") or 0)

    total_key = (dv, rutero, reponedor, cod_rt, tuple(marcas), foco_ap, search_ap)
    if st.session_state.get("_total_key") != total_key or st.session_state.get("_total_rows") is None:
        try:
            if kpi_total_rows is not None:
                st.session_state["_total_key"] = total_key
                st.session_state["_total_rows"] = int(kpi_total_rows)
                _dbg("TOTAL from KPIs", total=st.session_state["_total_rows"])
                _dbg_block()
            else:
                with _timed("QUERY total_rows"):
                    total_rows = db.get_tabla_ux_total(
                        rutero=rutero,
                        reponedor=reponedor,
                        cod_rt=cod_rt,
                        marcas=marcas,
                        foco=foco_ap,
                        search=search_ap,
                    )
                st.session_state["_total_key"] = total_key
                st.session_state["_total_rows"] = int(total_rows or 0)
                _dbg("TOTAL ready", total=st.session_state["_total_rows"])
                _dbg_block()
        except Exception as e:
            _dbg("FAIL total_rows", err=repr(e))
            st.session_state["_total_key"] = total_key
            st.session_state["_total_rows"] = 0

    total_rows = int(st.session_state.get("_total_rows") or 0)
    total_pages = max(1, int(math.ceil(total_rows / page_size))) if total_rows > 0 else 1

    # clamp page ANTES de widget (evita error Streamlit)
    if st.session_state["page"] > total_pages:
        st.session_state["page"] = total_pages
    if st.session_state["page"] < 1:
        st.session_state["page"] = 1

    def _page_prev():
        st.session_state["page"] = max(1, int(st.session_state["page"]) - 1)

    def _page_next():
        st.session_state["page"] = min(total_pages, int(st.session_state["page"]) + 1)

    p1, p2, p3, p4 = st.columns([0.6, 1.2, 0.6, 6], gap="small")
    with p1:
        st.button("◀", disabled=(st.session_state["page"] <= 1), on_click=_page_prev)
    with p2:
        st.number_input(
            "Página",
            min_value=1,
            max_value=total_pages,
            step=1,
            key="page",
            label_visibility="collapsed",
        )
    with p3:
        st.button("▶", disabled=(st.session_state["page"] >= total_pages), on_click=_page_next)
    with p4:
        if total_rows:
            start = (int(st.session_state["page"]) - 1) * page_size + 1
            end = min(int(st.session_state["page"]) * page_size, total_rows)
            st.caption(f"Página {int(st.session_state['page'])} / {total_pages} · {start}-{end} de {total_rows}")
        else:
            st.caption("Sin filas para el filtro aplicado.")

    # Query page
    try:
        with _timed("QUERY tabla page"):
            df_page = db.get_tabla_ux_page(
                rutero=rutero,
                reponedor=reponedor,
                cod_rt=cod_rt,
                marcas=marcas,
                page=int(st.session_state["page"]),
                page_size=page_size,
                foco=foco_ap,
                search=search_ap,
            )
            _dbg("TABLA page loaded", rows=0 if df_page is None else len(df_page), page=st.session_state["page"])
            _dbg_block()
    except Exception as e:
        _dbg("FAIL tabla page", err=repr(e))
        st.error("No pude leer la tabla (v_local_skus_ux).")
        with st.expander("Detalles técnicos"):
            st.code(repr(e))
            if DEBUG:
                st.code(traceback.format_exc())
        st.stop()

    df_raw = df_page  # UI usa datos raw (incluye Venta(+7) solo para calcular indicadores)
    _dbg("DF_RAW ready", rows=0 if df_raw is None else len(df_raw))
    _dbg_block()

    # Tabla compacta: MARCA, SKU, PRODUCTO, STOCK, INDICADORES
    def _row_indicadores(r) -> str:
        parts = []

        # Venta 0 (derivada de Venta(+7) == 0)
        try:
            v = int(r.get("Venta(+7)", 0) or 0)
        except Exception:
            v = 0
        if v == 0:
            parts.append("Venta 0: Productos sin rotación (Prioridad Alta).")

        # Negativo
        if str(r.get("NEGATIVO", "")).strip().upper() == "SI":
            parts.append("Negativo: Realizar ajuste de inventario.")

        # Quiebres
        if str(r.get("RIESGO DE QUIEBRE", "")).strip().upper() == "SI":
            parts.append("Quiebres: Solicitar empuje.")

        # Otros (solo si es un texto útil)
        ot = str(r.get("OTROS", "") or "").strip()
        if ot and ot.upper() not in {"NO", "N/A", "NA", "-"}:
            parts.append("Otros: Observaciones cliente.")

        return " · ".join(parts)

    if df_raw is not None and not df_raw.empty:
        df_tbl = df_raw.copy()
        df_tbl["INDICADORES"] = df_tbl.apply(_row_indicadores, axis=1)
        df_tbl = df_tbl.rename(columns={"Sku": "SKU", "Descripción del Producto": "PRODUCTO"})
        df_tbl = df_tbl[["MARCA", "SKU", "PRODUCTO", "Stock", "INDICADORES"]]
    else:
        df_tbl = df_raw


    st.dataframe(df_tbl, width="stretch", hide_index=True)

    # --------------------------------
    # EXPORT — bajo demanda
    # --------------------------------
    with st.expander("EXPORTAR (por filtro aplicado)", expanded=False):
        st.write("Exporta exactamente lo aplicado (marcas + foco + búsqueda).")

        export_key = (dv, rutero, reponedor, cod_rt, tuple(marcas), foco_ap, search_ap)

        cA, cB = st.columns(2)
        do_excel = cA.button("Preparar Excel", use_container_width=True)
        do_pdf = cB.button("Preparar PDF", use_container_width=True)

        if do_excel or do_pdf:
            if st.session_state.get("_export_key") != export_key:
                st.session_state["_export_key"] = export_key
                st.session_state.pop("_export_excel", None)
                st.session_state.pop("_export_pdf", None)

            try:
                with _timed("EXPORT query (db.get_tabla_ux_export)"):
                    df_export_raw = db.get_tabla_ux_export(
                        rutero=rutero,
                        reponedor=reponedor,
                        cod_rt=cod_rt,
                        marcas=marcas,
                        foco=foco_ap,
                        search=search_ap,
                    )
                    _dbg("EXPORT df loaded", rows=0 if df_export_raw is None else len(df_export_raw))
                    _dbg_block()
            except Exception as e:
                st.error("No pude preparar export (query).")
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
                st.stop()

            if df_export_raw is None or df_export_raw.empty:
                st.info("No hay filas para exportar con el filtro aplicado.")
            else:
                with _timed("EXPORT build_df"):
                    df_export = build_export_df(df_export_raw)

                if do_excel and st.session_state.get("_export_excel") is None:
                    with _timed("EXPORT excel_bytes"):
                        st.session_state["_export_excel"] = export_excel_one_sheet(cod_rt, df_export)

                if do_pdf and st.session_state.get("_export_pdf") is None:
                    pdf_lines = [
                        f"STOCK_ZERO · {cod_rt} · {nombre_local_rr}",
                        f"RUTERO: {rutero}  |  REPONEDOR: {reponedor}",
                        f"Datos al: {file_stamp}  |  Foco: {foco_ap}",
                        f"Marcas: {', '.join(marcas) if marcas else 'Todas'}  |  Búsqueda: {search_ap if search_ap else '-'}",
                    ]
                    with _timed("EXPORT pdf_bytes"):
                        st.session_state["_export_pdf"] = export_pdf_table(pdf_lines, df_export)

        dA, dB = st.columns(2)
        if st.session_state.get("_export_excel") is not None:
            dA.download_button(
                "Descargar Excel (filtrado)",
                data=st.session_state["_export_excel"],
                file_name=f"STOCK_ZERO_{cod_rt}_{file_stamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        if st.session_state.get("_export_pdf") is not None:
            dB.download_button(
                "Descargar PDF (filtrado)",
                data=st.session_state["_export_pdf"],
                file_name=f"STOCK_ZERO_{cod_rt}_{file_stamp}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )


if __name__ == "__main__":
    main()