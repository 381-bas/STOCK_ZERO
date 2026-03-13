# app/Home.py
import os
import time
import math
import logging
import traceback
import pandas as pd
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


def _infer_runtime_env() -> str:
    raw = (
        os.getenv("STOCKZERO_RUNTIME_ENV", "")
        or os.getenv("APP_ENV", "")
        or os.getenv("ENV", "")
    ).strip().lower()
    if raw in {"local", "public"}:
        return raw
    if any(os.getenv(k) for k in ("IS_STREAMLIT_CLOUD", "STREAMLIT_RUNTIME", "STREAMLIT_CLOUD")):
        return "public"
    return "local"


def _ensure_run_context() -> None:
    seq = int(st.session_state.get("_run_seq", 0)) + 1
    stamp = int(time.time() * 1000) % 1000000
    st.session_state["_run_seq"] = seq
    st.session_state["_run_id"] = f"RUN{seq:04d}-{stamp:06d}"
    st.session_state["_runtime_env"] = _infer_runtime_env()
    st.session_state["_run_path"] = "cold" if seq == 1 else "warm"


def _dbg(msg: str, tag: str = "UI", **kv) -> None:
    global DEBUG
    ts = datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]
    ctx = {
        "run_id": st.session_state.get("_run_id", "-"),
        "env": st.session_state.get("_runtime_env", "-"),
        "path": st.session_state.get("_run_path", "-"),
        "mode": st.session_state.get("home_mode", "-"),
    }
    payload = {**ctx, **kv}
    extra = " ".join([f"{k}={v}" for k, v in payload.items()]) if payload else ""
    line = f"{ts} | {tag} | {msg}" + (f" | {extra}" if extra else "")

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

    st.session_state["_dbg_last"] = f"{tag} | {msg}"


def _dbg_block() -> None:
    global DEBUG
    if not DEBUG:
        return
    st.sidebar.markdown("### 🛠 DEBUG")
    st.sidebar.caption(f"Último paso: {st.session_state.get('_dbg_last', '-')}")
    with st.sidebar.expander("Trace (últimos 250)", expanded=False):
        st.sidebar.code("\n".join(st.session_state.get("_dbg_lines", [])))


def _timed(label: str, tag: str = "UI"):
    class _T:
        def __enter__(self_):
            self_.t0 = time.perf_counter()
            _dbg(f"START {label}", tag=tag)
            return self_

        def __exit__(self_, exc_type, exc, tb):
            ms = round((time.perf_counter() - self_.t0) * 1000.0, 3)
            if exc_type is None:
                _dbg(f"OK {label}", tag=tag, ms=ms)
            else:
                _dbg(f"ERR {label}", tag=tag, ms=ms, exc=str(exc_type.__name__))
            return False

    return _T()


def main():
    global DEBUG

    st.set_page_config(page_title="STOCK_ZERO", layout="wide")

    from app import db
    from app.exports import build_export_df, export_excel_one_sheet, export_pdf_table

    DEBUG = _as_bool(_qp_get("debug", "")) or _as_bool(os.getenv("DEBUG_UI", ""))
    _ensure_run_context()
    _dbg("BOOT Home.py", py=str(getattr(__import__("sys"), "version", "na")).split()[0], run_seq=st.session_state.get("_run_seq"))
    _dbg_block()

    APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
    t_in = _qp_get("t", "").strip()

    st.title("STOCK_ZERO")
    st.caption("Lectura operativa · marcha blanca UX · tabla compacta · export bajo demanda")

    _dbg("RUN context ready", env=st.session_state.get("_runtime_env"), path=st.session_state.get("_run_path"))
    _dbg("TOKEN gate", token_set=bool(APP_TOKEN), t_present=bool(t_in))
    _dbg_block()

    if APP_TOKEN and t_in != APP_TOKEN:
        st.error("Link no válido o expirado. Solicita un link actualizado.")
        st.stop()

    if "init_done" not in st.session_state:
        st.session_state.init_done = True
        st.session_state.f_search = _qp_get("q", "")
        st.session_state.f_foco = _qp_get("foco", "Todo")

        qp_mode = (_qp_get("modo", "LOCAL") or "LOCAL").strip().upper()
        if qp_mode not in {"LOCAL", "MERCADERISTA"}:
            qp_mode = "LOCAL"
        st.session_state.home_mode = qp_mode

        _dbg(
            "INIT from query params",
            q=bool(st.session_state.f_search),
            foco=st.session_state.f_foco,
            mode=st.session_state.home_mode,
        )
        _dbg_block()

    def _invalidate_runtime_cache():
        st.session_state.pop("_kpis_key", None)
        st.session_state.pop("_kpis_row", None)
        st.session_state.pop("_total_key", None)
        st.session_state.pop("_total_rows", None)

        st.session_state.pop("_export_key", None)
        st.session_state.pop("_export_df_key", None)
        st.session_state.pop("_export_df", None)
        st.session_state.pop("_export_excel", None)
        st.session_state.pop("_export_pdf", None)
        st.session_state.pop("_export_request", None)
        st.session_state.pop("_export_busy", None)

    def _reset_filters_defaults(reason: str):
        st.session_state["sel_marcas"] = []
        st.session_state["f_search"] = ""
        st.session_state["f_foco"] = "Todo"
        st.session_state["applied_marcas"] = []
        st.session_state["applied_search"] = ""
        st.session_state["applied_foco"] = "Todo"
        st.session_state["page"] = 1
        _invalidate_runtime_cache()
        _dbg(f"RESET filters ({reason})")
        _dbg_block()

    def _reset_on_mode_change():
        st.session_state["sel_modalidad"] = ""
        st.session_state["sel_rr_label"] = ""
        st.session_state["sel_local_label"] = ""
        _reset_filters_defaults("mode_change")

    def _reset_on_modalidad_change():
        st.session_state["sel_rr_label"] = ""
        st.session_state["sel_local_label"] = ""
        _reset_filters_defaults("modalidad_change")

    def _reset_on_rr_change():
        st.session_state["sel_local_label"] = ""
        _reset_filters_defaults("rr_change")

    def _reset_on_local_change():
        _reset_filters_defaults("local_change")

    LOCAL_PLACEHOLDER = "— Selecciona local —"
    MODALIDAD_PLACEHOLDER = "— Selecciona modalidad —"
    RR_PLACEHOLDER = "— Selecciona rutero—reponedor —"

    qp_cod_rt = _qp_get("cod_rt", "").strip()
    qp_modalidad = _qp_get("modalidad", "").strip()
    qp_rutero = _qp_get("rutero", "").strip()
    qp_reponedor = _qp_get("reponedor", "").strip()

    modo = ""
    modalidad_sel = ""
    rutero = ""
    reponedor = ""
    cod_rt = ""
    nombre_local_rr = ""
    panel_mercaderista = "-"
    panel_modalidad = "-"

    # --------------------------------
    # Filtros superiores
    # --------------------------------
    top1, top2, top3 = st.columns([1.3, 1.7, 2.6], gap="small")

    mode_opts = ["LOCAL", "MERCADERISTA"]
    st.session_state.setdefault("home_mode", "LOCAL")

    with top1:
        st.selectbox(
            "CONSULTA",
            mode_opts,
            key="home_mode",
            on_change=_reset_on_mode_change,
        )

    modo = st.session_state["home_mode"]

    # ==============================================
    # MODO LOCAL
    # ==============================================
    if modo == "LOCAL":
        try:
            with _timed("QUERY locales_home", tag="QUERY"):
                locs = db.get_locales_home()
                _dbg("LOCS HOME loaded", rows=len(locs))
                _dbg_block()
        except Exception as e:
            _dbg("FAIL locales_home", err=repr(e))
            st.error("No pude leer locales desde ruta_rutero.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        if locs.empty:
            st.warning("No hay locales para mostrar.")
            st.stop()

        locs = locs.copy()
        locs["label"] = locs["cod_rt"].astype(str) + " — " + locs["nombre_local"].astype(str)
        loc_labels = [LOCAL_PLACEHOLDER] + locs["label"].tolist()

        if st.session_state.get("sel_local_label", "") not in loc_labels:
            if qp_cod_rt:
                hit = locs.loc[locs["cod_rt"].astype(str) == qp_cod_rt, "label"]
                st.session_state["sel_local_label"] = hit.iloc[0] if not hit.empty else LOCAL_PLACEHOLDER
            else:
                st.session_state["sel_local_label"] = LOCAL_PLACEHOLDER

        with top2:
            st.selectbox(
                "LOCAL (COD_RT)",
                loc_labels,
                key="sel_local_label",
                on_change=_reset_on_local_change,
            )

        if st.session_state["sel_local_label"] == LOCAL_PLACEHOLDER:
            st.info("Selecciona un LOCAL para cargar indicadores, clientes y tabla.")
            st.stop()

        hit_loc = locs.loc[locs["label"] == st.session_state["sel_local_label"]]
        if hit_loc.empty:
            _dbg("ERR local selection invalid", sel=st.session_state["sel_local_label"])
            st.error("El local seleccionado ya no está disponible. Vuelve a seleccionar.")
            st.stop()

        row_loc = hit_loc.iloc[0]
        cod_rt = str(row_loc["cod_rt"])
        _dbg("LOCAL selected", modo=modo, cod_rt=cod_rt)
        _dbg_block()

    # ==============================================
    # MODO MERCADERISTA
    # ==============================================
    else:
        try:
            with _timed("QUERY modalidades_home", tag="QUERY"):
                modalidades = db.get_modalidades_home()
                _dbg("MODALIDADES loaded", rows=len(modalidades))
                _dbg_block()
        except Exception as e:
            _dbg("FAIL modalidades_home", err=repr(e))
            st.error("No pude leer modalidades desde ruta_rutero.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        modalidad_opts = [MODALIDAD_PLACEHOLDER] + modalidades

        if st.session_state.get("sel_modalidad", "") not in modalidad_opts:
            if qp_modalidad and qp_modalidad in modalidades:
                st.session_state["sel_modalidad"] = qp_modalidad
            else:
                st.session_state["sel_modalidad"] = MODALIDAD_PLACEHOLDER

        with top2:
            st.selectbox(
                "MODALIDAD",
                modalidad_opts,
                key="sel_modalidad",
                on_change=_reset_on_modalidad_change,
            )

        modalidad_sel = st.session_state["sel_modalidad"]

        rr_df = None
        rr_opts = [RR_PLACEHOLDER]

        if modalidad_sel != MODALIDAD_PLACEHOLDER:
            try:
                with _timed("QUERY rr_por_modalidad", tag="QUERY"):
                    rr_df = db.get_rutero_reponedor_por_modalidad(modalidad_sel)
                    _dbg("RR by modalidad loaded", rows=0 if rr_df is None else len(rr_df))
                    _dbg_block()
            except Exception as e:
                _dbg("FAIL rr_por_modalidad", err=repr(e))
                st.error("No pude leer rutero—reponedor para la modalidad seleccionada.")
                with st.expander("Detalles técnicos"):
                    st.code(repr(e))
                    if DEBUG:
                        st.code(traceback.format_exc())
                st.stop()

            if rr_df is not None and not rr_df.empty:
                rr_df = rr_df.copy()
                rr_df["label"] = rr_df["rutero"].astype(str) + " — " + rr_df["reponedor"].astype(str)
                rr_opts = [RR_PLACEHOLDER] + rr_df["label"].tolist()

        if st.session_state.get("sel_rr_label", "") not in rr_opts:
            if rr_df is not None and not rr_df.empty and qp_rutero and qp_reponedor:
                hit_rr = rr_df[
                    (rr_df["rutero"].astype(str) == qp_rutero)
                    & (rr_df["reponedor"].astype(str) == qp_reponedor)
                ]
                st.session_state["sel_rr_label"] = hit_rr.iloc[0]["label"] if not hit_rr.empty else RR_PLACEHOLDER
            else:
                st.session_state["sel_rr_label"] = RR_PLACEHOLDER

        with top3:
            st.selectbox(
                "RUTERO — REPONEDOR",
                rr_opts,
                key="sel_rr_label",
                on_change=_reset_on_rr_change,
            )

        if modalidad_sel == MODALIDAD_PLACEHOLDER:
            st.info("Selecciona una MODALIDAD para habilitar el filtro RUTERO—REPONEDOR.")
            st.stop()

        if st.session_state["sel_rr_label"] == RR_PLACEHOLDER:
            st.info("Selecciona un RUTERO—REPONEDOR para cargar locales.")
            st.stop()

        hit_rr = rr_df.loc[rr_df["label"] == st.session_state["sel_rr_label"]] if rr_df is not None else None
        if hit_rr is None or hit_rr.empty:
            _dbg("ERR rr selection invalid", sel=st.session_state["sel_rr_label"])
            st.error("La selección de RUTERO—REPONEDOR ya no está disponible. Vuelve a seleccionar.")
            st.stop()

        row_rr = hit_rr.iloc[0]
        rutero = str(row_rr["rutero"])
        reponedor = str(row_rr["reponedor"])
        _dbg("RR selected", modalidad=modalidad_sel, rutero=rutero, reponedor=reponedor)
        _dbg_block()

        try:
            with _timed("QUERY locales_por_modalidad_rr", tag="QUERY"):
                locs = db.get_locales_por_modalidad_rr(modalidad_sel, rutero, reponedor)
                _dbg("LOCS by modalidad_rr loaded", rows=len(locs))
                _dbg_block()
        except Exception as e:
            _dbg("FAIL locales_por_modalidad_rr", err=repr(e))
            st.error("No pude leer locales para la combinación seleccionada.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        if locs.empty:
            st.warning("No hay locales para la combinación Modalidad + Rutero—Reponedor.")
            st.stop()

        locs = locs.copy()
        locs["label"] = locs["cod_rt"].astype(str) + " — " + locs["nombre_local"].astype(str)
        loc_labels = [LOCAL_PLACEHOLDER] + locs["label"].tolist()

        if st.session_state.get("sel_local_label", "") not in loc_labels:
            if qp_cod_rt:
                hit = locs.loc[locs["cod_rt"].astype(str) == qp_cod_rt, "label"]
                st.session_state["sel_local_label"] = hit.iloc[0] if not hit.empty else LOCAL_PLACEHOLDER
            else:
                st.session_state["sel_local_label"] = LOCAL_PLACEHOLDER

        st.selectbox(
            "LOCAL (COD_RT)",
            loc_labels,
            key="sel_local_label",
            on_change=_reset_on_local_change,
        )

        if st.session_state["sel_local_label"] == LOCAL_PLACEHOLDER:
            st.info("Selecciona un LOCAL para cargar clientes, indicadores y tabla.")
            st.stop()

        hit_loc = locs.loc[locs["label"] == st.session_state["sel_local_label"]]
        if hit_loc.empty:
            _dbg("ERR local selection invalid", sel=st.session_state["sel_local_label"])
            st.error("El local seleccionado ya no está disponible. Vuelve a seleccionar.")
            st.stop()

        row_loc = hit_loc.iloc[0]
        cod_rt = str(row_loc["cod_rt"])
        _dbg("LOCAL selected", modo=modo, modalidad=modalidad_sel, rutero=rutero, reponedor=reponedor, cod_rt=cod_rt)
        _dbg_block()

    # --------------------------------
    # Contexto del local
    # --------------------------------
    try:
        if modo == "LOCAL":
            with _timed("QUERY contexto_local_home", tag="QUERY"):
                ctx = db.get_contexto_local_home(cod_rt)
        else:
            with _timed("QUERY contexto_local_rr", tag="QUERY"):
                ctx = db.get_contexto_local(
                    rutero=rutero,
                    reponedor=reponedor,
                    cod_rt=cod_rt,
                    modalidad=modalidad_sel,
                )

        _dbg("CTX loaded", rows=0 if ctx is None else len(ctx))
        _dbg_block()
    except Exception as e:
        _dbg("FAIL contexto_local", err=repr(e))
        ctx = None

    if ctx is not None and not ctx.empty:
        row_ctx = ctx.iloc[0]
        nombre_local_rr = str(
            row_ctx.get("local_nombre")
            or row_ctx.get("nombre_local_rr")
            or cod_rt
        )

        if modo == "LOCAL":
            panel_mercaderista = str(row_ctx.get("mercaderistas") or "-")
            panel_modalidad = str(row_ctx.get("modalidades") or "-")
        else:
            panel_mercaderista = reponedor or "-"
            panel_modalidad = modalidad_sel or "-"
    else:
        nombre_local_rr = str(cod_rt)
        if modo == "MERCADERISTA":
            panel_mercaderista = reponedor or "-"
            panel_modalidad = modalidad_sel or "-"

    if modo == "LOCAL":
        i1, i2 = st.columns([2, 1], gap="small")
        i1.caption(f"Mercaderista: {panel_mercaderista}")
        i2.caption(f"Modalidad: {panel_modalidad}")
    else:
        i1, i2, i3 = st.columns([1.4, 1.8, 1.4], gap="small")
        i1.caption(f"Rutero: {rutero}")
        i2.caption(f"Reponedor: {reponedor}")
        i3.caption(f"Modalidad: {panel_modalidad}")

    # Estado: al cambiar selección real, resetea filtros aplicados
    if modo == "LOCAL":
        sel_key = f"{modo}|{cod_rt}"
    else:
        sel_key = f"{modo}|{modalidad_sel}|{rutero}|{reponedor}|{cod_rt}"

    if st.session_state.get("_sel_key") != sel_key:
        st.session_state["_sel_key"] = sel_key
        _reset_filters_defaults("selection_ready")

    # Fecha stock: se resolverá desde la misma query de KPIs
    file_stamp = "Sin stock"

    # --------------------------------
    # Filtros / consultas según modo
    # --------------------------------
    try:
        if modo == "LOCAL":
            with _timed("QUERY marcas_local", tag="QUERY"):
                marcas_disponibles = db.get_marcas_local(cod_rt)
                _dbg("MARCAS loaded", n=len(marcas_disponibles))
                _dbg_block()
        else:
            with _timed("QUERY marcas_rr", tag="QUERY"):
                marcas_disponibles = db.get_marcas(
                    rutero=rutero,
                    reponedor=reponedor,
                    cod_rt=cod_rt,
                    modalidad=modalidad_sel,
                )
                _dbg("MARCAS loaded", n=len(marcas_disponibles))
                _dbg_block()
    except Exception as e:
        _dbg("FAIL marcas", err=repr(e))
        marcas_disponibles = []

    st.session_state.setdefault("sel_marcas", [])
    mset = set(marcas_disponibles)
    st.session_state["sel_marcas"] = [m for m in (st.session_state.get("sel_marcas") or []) if m in mset]

    marcas = list(st.session_state.get("applied_marcas", []) or [])
    foco_ap = st.session_state.get("applied_foco", "Todo")
    search_ap = (st.session_state.get("applied_search", "") or "").strip()

    dv = db.get_data_version()

    # --------------------------------
    # KPIs
    # --------------------------------
    if modo == "LOCAL":
        kpis_key = (dv, modo, cod_rt, tuple(marcas))
    else:
        kpis_key = (dv, modo, modalidad_sel, rutero, reponedor, cod_rt, tuple(marcas))

    kpis_row = st.session_state.get("_kpis_row")

    if st.session_state.get("_kpis_key") != kpis_key or kpis_row is None:
        try:
            if modo == "LOCAL":
                with _timed("QUERY kpis_home", tag="QUERY"):
                    kpis = db.get_kpis_local_home(cod_rt, marcas)
            else:
                with _timed("QUERY kpis_rr", tag="QUERY"):
                    kpis = db.get_kpis_local(
                        rutero=rutero,
                        reponedor=reponedor,
                        cod_rt=cod_rt,
                        marcas=marcas,
                        modalidad=modalidad_sel,
                    )

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

    total_skus_kpi = int((kpis_row or {}).get("total_skus") or 0)
    fecha_stock_raw = (kpis_row or {}).get("fecha_stock")
    fecha_stock_dt = pd.to_datetime(fecha_stock_raw, errors="coerce")
    file_stamp = fecha_stock_dt.strftime("%Y-%m-%d") if pd.notna(fecha_stock_dt) else "Sin stock"

    s1, s2 = st.columns([1.4, 4], gap="small")
    s1.caption(f"Fecha stock: {file_stamp}")
    if total_skus_kpi == 0:
        s2.caption("Estado: Sin stock para la combinación seleccionada.")

    # --------------------------------
    # Filtros secundarios
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
                "CLIENTE (opcional)",
                options=marcas_disponibles,
                key="sel_marcas",
                placeholder="Todos",
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

    marcas = list(st.session_state.get("applied_marcas", []) or [])
    foco_ap = st.session_state.get("applied_foco", "Todo")
    search_ap = (st.session_state.get("applied_search", "") or "").strip()

    # --------------------------------
    # TABLA paginada
    # --------------------------------
    page_size = 25
    st.session_state.setdefault("page", 1)

    max_m = int(os.getenv("MAX_MARCA_FILTER", "50"))
    can_use_kpi_total = (
        len((search_ap or "").strip()) < 2
        and (not marcas or len(marcas) <= max_m)
        and bool(kpis_row)
    )

    kpi_total_rows = None

    # Optimización: si no hay stock base, no hace falta consultar total
    if total_skus_kpi == 0:
        kpi_total_rows = 0
    elif can_use_kpi_total:
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

    if modo == "LOCAL":
        total_key = (dv, modo, cod_rt, tuple(marcas), foco_ap, search_ap)
    else:
        total_key = (dv, modo, modalidad_sel, rutero, reponedor, cod_rt, tuple(marcas), foco_ap, search_ap)

    if st.session_state.get("_total_key") != total_key or st.session_state.get("_total_rows") is None:
        try:
            if kpi_total_rows is not None:
                st.session_state["_total_key"] = total_key
                st.session_state["_total_rows"] = int(kpi_total_rows)
                _dbg("TOTAL from KPIs", tag="CACHE", total=st.session_state["_total_rows"])
                _dbg_block()
            else:
                if modo == "LOCAL":
                    with _timed("QUERY total_rows_home", tag="QUERY"):
                        total_rows = db.get_tabla_ux_total_home(
                            cod_rt=cod_rt,
                            marcas=marcas,
                            foco=foco_ap,
                            search=search_ap,
                        )
                else:
                    with _timed("QUERY total_rows_rr", tag="QUERY"):
                        total_rows = db.get_tabla_ux_total(
                            rutero=rutero,
                            reponedor=reponedor,
                            cod_rt=cod_rt,
                            marcas=marcas,
                            foco=foco_ap,
                            search=search_ap,
                            modalidad=modalidad_sel,
                        )

                st.session_state["_total_key"] = total_key
                st.session_state["_total_rows"] = int(total_rows or 0)
                _dbg("TOTAL ready", tag="CACHE", total=st.session_state["_total_rows"])
                _dbg_block()
        except Exception as e:
            _dbg("FAIL total_rows", err=repr(e))
            st.session_state["_total_key"] = total_key
            st.session_state["_total_rows"] = 0

    total_rows = int(st.session_state.get("_total_rows") or 0)
    total_pages = max(1, int(math.ceil(total_rows / page_size))) if total_rows > 0 else 1

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

    if total_rows == 0:
        df_page = pd.DataFrame(columns=[
            "MARCA", "Sku", "Descripción del Producto",
            "Stock", "Venta(+7)", "NEGATIVO", "RIESGO DE QUIEBRE", "OTROS"
        ])
        _dbg("TABLA page skipped", reason="sin_filas")
        _dbg_block()
    else:
        try:
            if modo == "LOCAL":
                with _timed("QUERY tabla_page_home", tag="QUERY"):
                    df_page = db.get_tabla_ux_page_home(
                        cod_rt=cod_rt,
                        marcas=marcas,
                        page=int(st.session_state["page"]),
                        page_size=page_size,
                        foco=foco_ap,
                        search=search_ap,
                    )
            else:
                with _timed("QUERY tabla_page_rr", tag="QUERY"):
                    df_page = db.get_tabla_ux_page(
                        rutero=rutero,
                        reponedor=reponedor,
                        cod_rt=cod_rt,
                        marcas=marcas,
                        page=int(st.session_state["page"]),
                        page_size=page_size,
                        foco=foco_ap,
                        search=search_ap,
                        modalidad=modalidad_sel,
                    )

            _dbg(
                "TABLA page loaded",
                rows=0 if df_page is None else len(df_page),
                page=st.session_state["page"],
            )
            _dbg_block()

        except Exception as e:
            _dbg("FAIL tabla page", err=repr(e))
            st.error("No pude leer la tabla de stock.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

    df_raw = df_page
    _dbg("DF_RAW ready", rows=0 if df_raw is None else len(df_raw))
    _dbg_block()

    def _row_indicadores(r) -> str:
        parts = []

        try:
            v = int(r.get("Venta(+7)", 0) or 0)
        except Exception:
            v = 0
        if v == 0:
            parts.append("Venta 0: Productos sin rotación (Prioridad Alta).")

        if str(r.get("NEGATIVO", "")).strip().upper() == "SI":
            parts.append("Negativo: Realizar ajuste de inventario.")

        if str(r.get("RIESGO DE QUIEBRE", "")).strip().upper() == "SI":
            parts.append("Quiebres: Solicitar empuje.")

        ot = str(r.get("OTROS", "") or "").strip()
        if ot and ot.upper() not in {"NO", "N/A", "NA", "-"}:
            parts.append("Otros: Observaciones cliente.")

        return " · ".join(parts)

    if df_raw is not None and not df_raw.empty:
        df_tbl = df_raw.copy()

        if "fecha" in df_tbl.columns:
            df_tbl["FECHA STOCK"] = pd.to_datetime(df_tbl["fecha"], errors="coerce").dt.strftime("%Y-%m-%d")
            df_tbl["FECHA STOCK"] = df_tbl["FECHA STOCK"].fillna("")
        else:
            df_tbl["FECHA STOCK"] = ""

        df_tbl["INDICADORES"] = df_tbl.apply(_row_indicadores, axis=1)
        df_tbl = df_tbl.rename(columns={
            "MARCA": "CLIENTE",
            "Sku": "SKU",
            "Descripción del Producto": "PRODUCTO",
        })
        df_tbl = df_tbl[["FECHA STOCK", "CLIENTE", "SKU", "PRODUCTO", "Stock", "INDICADORES"]]
    else:
        df_tbl = df_raw

    st.dataframe(df_tbl, width="stretch", hide_index=True)

    def _request_export(kind: str):
        st.session_state["_export_request"] = kind

    # --------------------------------
    # EXPORT
    # --------------------------------
    with st.expander("EXPORTAR (por filtro aplicado)", expanded=False):
        st.write("Exporta exactamente lo aplicado (cliente + foco + búsqueda).")

        if total_rows == 0:
            if total_skus_kpi == 0:
                st.info(f"Sin stock para exportar. Fecha stock: {file_stamp}.")
            else:
                st.info("No hay filas para exportar con el filtro aplicado.")

        if modo == "LOCAL":
            export_key = (dv, modo, cod_rt, tuple(marcas), foco_ap, search_ap)
        else:
            export_key = (dv, modo, modalidad_sel, rutero, reponedor, cod_rt, tuple(marcas), foco_ap, search_ap)

        if st.session_state.get("_export_key") != export_key:
            st.session_state["_export_key"] = export_key
            st.session_state.pop("_export_df_key", None)
            st.session_state.pop("_export_df", None)
            st.session_state.pop("_export_excel", None)
            st.session_state.pop("_export_pdf", None)
            st.session_state.pop("_export_request", None)
            st.session_state.pop("_export_busy", None)

        cA, cB = st.columns(2)
        cA.button(
            "Preparar Excel",
            key="btn_prepare_excel",
            use_container_width=True,
            disabled=(total_rows == 0 or st.session_state.get("_export_busy", False)),
            on_click=_request_export,
            args=("excel",),
        )
        cB.button(
            "Preparar PDF",
            key="btn_prepare_pdf",
            use_container_width=True,
            disabled=(total_rows == 0 or st.session_state.get("_export_busy", False)),
            on_click=_request_export,
            args=("pdf",),
        )

        export_request = st.session_state.get("_export_request")
        export_busy = bool(st.session_state.get("_export_busy", False))

        if export_busy:
            st.caption("Preparando archivo...")

        if export_request in {"excel", "pdf"} and not export_busy and total_rows > 0:
            need_excel = export_request == "excel" and st.session_state.get("_export_excel") is None
            need_pdf = export_request == "pdf" and st.session_state.get("_export_pdf") is None

            if need_excel or need_pdf:
                st.session_state["_export_busy"] = True
                try:
                    df_export = st.session_state.get("_export_df")

                    if st.session_state.get("_export_df_key") != export_key or df_export is None:
                        try:
                            if modo == "LOCAL":
                                with _timed("EXPORT query (db.get_tabla_ux_export_home)", tag="CACHE"):
                                    df_export_raw = db.get_tabla_ux_export_home(
                                        cod_rt=cod_rt,
                                        marcas=marcas,
                                        foco=foco_ap,
                                        search=search_ap,
                                    )
                            else:
                                with _timed("EXPORT query (db.get_tabla_ux_export)", tag="CACHE"):
                                    df_export_raw = db.get_tabla_ux_export(
                                        rutero=rutero,
                                        reponedor=reponedor,
                                        cod_rt=cod_rt,
                                        marcas=marcas,
                                        foco=foco_ap,
                                        search=search_ap,
                                        modalidad=modalidad_sel,
                                    )

                            _dbg("EXPORT df loaded", rows=0 if df_export_raw is None else len(df_export_raw))
                            _dbg_block()
                        except Exception as e:
                            _dbg("FAIL export query", err=repr(e))
                            st.error("No pude preparar export (query).")
                            st.code(repr(e))
                            if DEBUG:
                                st.code(traceback.format_exc())
                            st.stop()

                        if df_export_raw is None or df_export_raw.empty:
                            st.info("No hay filas para exportar con el filtro aplicado.")
                            st.session_state["_export_df_key"] = export_key
                            st.session_state["_export_df"] = None
                        else:
                            with _timed("EXPORT build_df", tag="CACHE"):
                                df_export = build_export_df(df_export_raw)

                            st.session_state["_export_df_key"] = export_key
                            st.session_state["_export_df"] = df_export
                    else:
                        _dbg("EXPORT build_df reused from cache", tag="CACHE")
                        _dbg_block()

                    df_export = st.session_state.get("_export_df")

                    if df_export is not None:
                        if need_excel:
                            with _timed("EXPORT excel_bytes", tag="UI"):
                                st.session_state["_export_excel"] = export_excel_one_sheet(cod_rt, df_export)

                        if need_pdf:
                            if modo == "LOCAL":
                                pdf_lines = [
                                    f"STOCK_ZERO · {cod_rt} · {nombre_local_rr}",
                                    f"Gestión: {panel_mercaderista}  |  Modalidad: {panel_modalidad}",
                                    f"Fecha stock: {file_stamp}  |  Foco: {foco_ap}",
                                    f"Clientes: {', '.join(marcas) if marcas else 'Todos'}  |  Búsqueda: {search_ap if search_ap else '-'}",
                                ]
                            else:
                                pdf_lines = [
                                    f"STOCK_ZERO · {cod_rt} · {nombre_local_rr}",
                                    f"Modalidad: {modalidad_sel}",
                                    f"Rutero: {rutero}  |  Reponedor: {reponedor}",
                                    f"Fecha stock: {file_stamp}  |  Foco: {foco_ap}",
                                    f"Clientes: {', '.join(marcas) if marcas else 'Todos'}  |  Búsqueda: {search_ap if search_ap else '-'}",
                                ]
                            with _timed("EXPORT pdf_bytes", tag="UI"):
                                st.session_state["_export_pdf"] = export_pdf_table(pdf_lines, df_export)

                finally:
                    st.session_state["_export_busy"] = False
                    st.session_state["_export_request"] = None

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
