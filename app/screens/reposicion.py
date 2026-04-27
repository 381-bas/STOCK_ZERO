import math
import os
import traceback

import pandas as pd
import streamlit as st

from app.exports import (
    build_export_df,
    build_focus_export_df,
    export_excel_one_sheet,
    export_excel_generic,
    export_pdf_focus_table,
    export_pdf_table,
)
from app.services import stock as stock_service


def _qp_get(key: str, default: str = "") -> str:
    try:
        qp = st.query_params
        v = qp.get(key, default)
        if isinstance(v, list):
            return v[0] if v else default
        return v if v is not None else default
    except Exception:
        qp = st.experimental_get_query_params()
        v = qp.get(key, [default])
        return v[0] if isinstance(v, list) and v else default


def render_reposicion(
    *,
    modo,
    db,
    DEBUG,
    top2,
    top3,
    LOCAL_PLACEHOLDER,
    FOCO_OPTIONS,
    qp_cod_rt,
    _dbg,
    _dbg_block,
    _timed,
    _normalize_focos_ui,
    _foco_label,
    _render_kpi_cards,
    _invalidate_runtime_cache,
    _reset_on_local_change,
    _reset_on_modalidad_change,
    _reset_on_rr_change,
    _df_total_rows,
    _drop_total_rows,
    _rename_and_pick,
):
    MODALIDAD_PLACEHOLDER = "— Selecciona modalidad —"
    RR_PLACEHOLDER = "— Selecciona rutero—reponedor —"

    qp_modalidad = _qp_get("modalidad", "").strip()
    qp_rutero = _qp_get("rutero", "").strip()
    qp_reponedor = _qp_get("reponedor", "").strip()

    modalidad_sel = ""
    rutero = ""
    reponedor = ""
    cod_rt = ""
    nombre_local_rr = ""
    panel_mercaderista = "-"
    panel_modalidad = "-"

    def _reset_filters_defaults(reason: str):
        st.session_state["sel_marcas"] = []
        st.session_state["sel_marca_home_cliente"] = "Todas"
        st.session_state["f_search"] = ""
        st.session_state["f_foco"] = []
        st.session_state["applied_marcas"] = []
        st.session_state["applied_scope_marca"] = None
        st.session_state["applied_search"] = ""
        st.session_state["applied_foco"] = []
        st.session_state["page"] = 1
        st.session_state["page_input_ui"] = 1
        st.session_state.pop("page_select_ui", None)
        _invalidate_runtime_cache()
        _dbg(f"RESET filters ({reason})")
        _dbg_block()

    if modo == "LOCAL":
        try:
            with _timed("SELECTOR locales_home", tag="SELECTOR"):
                locs = stock_service.get_local_selector_data()
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

    elif modo == "MERCADERISTA":
        try:
            with _timed("SELECTOR modalidades_home", tag="SELECTOR"):
                modalidades = stock_service.get_mercaderista_modalidades()
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
                with _timed("SELECTOR rr_por_modalidad", tag="SELECTOR"):
                    rr_df = stock_service.get_mercaderista_selector_data(modalidad_sel)
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
            with _timed("SELECTOR locales_por_modalidad_rr", tag="SELECTOR"):
                locs = stock_service.get_locales_por_modalidad_rr(modalidad_sel, rutero, reponedor)
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

    else:
        st.error("Modo de consulta no soportado.")
        st.stop()

    try:
        with _timed("SELECTOR contexto_local", tag="SELECTOR"):
            ctx = stock_service.get_local_context(
                modo=modo,
                cod_rt=cod_rt,
                rutero=rutero,
                reponedor=reponedor,
                modalidad=modalidad_sel,
            )

        _dbg("CTX loaded", rows=0 if ctx is None else len(ctx))
        _dbg_block()
    except Exception as e:
        _dbg("FAIL contexto_local", err=repr(e))
        ctx = None

    if ctx is not None and not ctx.empty:
        row_ctx = ctx.iloc[0]
        nombre_local_rr = str(row_ctx.get("local_nombre") or row_ctx.get("nombre_local_rr") or cod_rt)

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

    if modo == "LOCAL":
        sel_key = f"{modo}|{cod_rt}"
    else:
        sel_key = f"{modo}|{modalidad_sel}|{rutero}|{reponedor}|{cod_rt}"

    if st.session_state.get("_sel_key") != sel_key:
        st.session_state["_sel_key"] = sel_key
        _reset_filters_defaults("selection_ready")

    file_stamp = "Sin stock"

    try:
        with _timed("PAGE marcas", tag="PAGE"):
            marcas_disponibles = stock_service.get_brand_options(
                modo=modo,
                cod_rt=cod_rt,
                rutero=rutero,
                reponedor=reponedor,
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
    foco_ap = _normalize_focos_ui(st.session_state.get("applied_foco", []))
    search_ap = (st.session_state.get("applied_search", "") or "").strip()

    dv = stock_service.get_data_version()

    if modo == "LOCAL":
        kpis_key = (dv, modo, cod_rt, tuple(marcas))
    else:
        kpis_key = (dv, modo, modalidad_sel, rutero, reponedor, cod_rt, tuple(marcas))

    kpis_row = st.session_state.get("_kpis_row")

    if st.session_state.get("_kpis_key") != kpis_key or kpis_row is None:
        try:
            with _timed("PAGE kpis", tag="PAGE"):
                kpis = stock_service.get_kpis(
                    modo=modo,
                    cod_rt=cod_rt,
                    marcas=marcas,
                    rutero=rutero,
                    reponedor=reponedor,
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

    _render_kpi_cards(kpis_row)

    total_skus_kpi = int((kpis_row or {}).get("total_skus") or 0)
    fecha_stock_raw = (kpis_row or {}).get("fecha_stock")
    fecha_stock_dt = pd.to_datetime(fecha_stock_raw, errors="coerce")
    file_stamp = fecha_stock_dt.strftime("%Y-%m-%d") if pd.notna(fecha_stock_dt) else "Sin stock"

    if total_skus_kpi == 0:
        st.caption("Estado: Sin stock para la combinación seleccionada.")

    def _apply_filters():
        st.session_state["applied_marcas"] = list(st.session_state.get("sel_marcas", []) or [])
        st.session_state["applied_foco"] = _normalize_focos_ui(st.session_state.get("f_foco", []))
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

    filters_expanded = bool(
        st.session_state.get("applied_marcas")
        or _normalize_focos_ui(st.session_state.get("applied_foco", []))
        or (st.session_state.get("applied_search", "") or "").strip()
    )

    with st.expander("FILTROS (opcional)", expanded=filters_expanded):
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
                st.multiselect(
                    "Foco operativo",
                    options=FOCO_OPTIONS,
                    key="f_foco",
                    placeholder="Todo",
                )

            st.text_input(
                "Búsqueda (SKU o descripción)",
                key="f_search",
                placeholder="Ej: 779... / galleta / snack... (mín 2 caracteres)",
            )

            st.form_submit_button(
                "Aplicar",
                on_click=_apply_filters,
                use_container_width=True,
            )

    marcas = list(st.session_state.get("applied_marcas", []) or [])
    foco_ap = _normalize_focos_ui(st.session_state.get("applied_foco", []))
    search_ap = (st.session_state.get("applied_search", "") or "").strip()

    page_size = 25
    st.session_state.setdefault("page", 1)

    max_m = int(os.getenv("MAX_MARCA_FILTER", "50"))
    can_use_kpi_total = (
        len((search_ap or "").strip()) < 2
        and (not marcas or len(marcas) <= max_m)
        and bool(kpis_row)
        and len(foco_ap) <= 1
    )

    kpi_total_rows = None
    single_foco = foco_ap[0] if len(foco_ap) == 1 else None

    if total_skus_kpi == 0:
        kpi_total_rows = 0
    elif can_use_kpi_total and len(foco_ap) <= 1:
        if not foco_ap:
            kpi_total_rows = int(kpis_row.get("total_skus") or 0)
        elif single_foco == "Venta 0":
            kpi_total_rows = int(kpis_row.get("venta_0") or 0)
        elif single_foco == "Negativo":
            kpi_total_rows = int(kpis_row.get("negativos") or 0)
        elif single_foco == "Quiebres":
            kpi_total_rows = int(kpis_row.get("quiebres") or 0)
        elif single_foco == "Otros":
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
                with _timed("PAGE total_rows", tag="PAGE"):
                    total_rows = stock_service.get_total_rows(
                        modo=modo,
                        cod_rt=cod_rt,
                        marcas=marcas,
                        foco=foco_ap,
                        search=search_ap,
                        rutero=rutero,
                        reponedor=reponedor,
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
        st.session_state["page"] = max(1, int(st.session_state.get("page", 1)) - 1)
        st.session_state["page_input_ui"] = st.session_state["page"]

    def _page_next():
        st.session_state["page"] = min(total_pages, int(st.session_state.get("page", 1)) + 1)
        st.session_state["page_input_ui"] = st.session_state["page"]

    def _page_from_input():
        try:
            new_page = int(st.session_state.get("page_input_ui", 1))
        except Exception:
            new_page = 1

        new_page = max(1, min(total_pages, new_page))
        st.session_state["page"] = new_page
        st.session_state["page_input_ui"] = new_page

    current_page = int(st.session_state.get("page", 1))
    if current_page < 1:
        current_page = 1
        st.session_state["page"] = 1
    if current_page > total_pages:
        current_page = total_pages
        st.session_state["page"] = total_pages

    if int(st.session_state.get("page_input_ui", current_page)) != current_page:
        st.session_state["page_input_ui"] = current_page

    if total_rows:
        start = (current_page - 1) * page_size + 1
        end = min(current_page * page_size, total_rows)
        pager_text = f"{start}-{end} de {total_rows} registros"
    else:
        pager_text = "Sin filas para el filtro aplicado."

    p1, p2, p3, p4 = st.columns([0.9, 1.3, 0.9, 2.2], gap="small")

    with p1:
        st.button(
            "◀",
            key="page_prev_btn",
            disabled=(current_page <= 1),
            on_click=_page_prev,
            use_container_width=True,
        )

    with p2:
        st.number_input(
            "Página",
            min_value=1,
            max_value=max(1, total_pages),
            step=1,
            key="page_input_ui",
            on_change=_page_from_input,
            label_visibility="collapsed",
        )

    with p3:
        st.button(
            "▶",
            key="page_next_btn",
            disabled=(current_page >= total_pages),
            on_click=_page_next,
            use_container_width=True,
        )

    with p4:
        st.caption(pager_text)

    if total_rows == 0:
        df_page = pd.DataFrame(columns=[
            "MARCA", "Sku", "Descripción del Producto",
            "Stock", "Venta(+7)", "NEGATIVO", "RIESGO DE QUIEBRE", "OTROS"
        ])
        _dbg("TABLA page skipped", reason="sin_filas")
        _dbg_block()
    else:
        try:
            with _timed("PAGE tabla_page", tag="PAGE"):
                df_page = stock_service.get_page(
                    modo=modo,
                    cod_rt=cod_rt,
                    marcas=marcas,
                    foco=foco_ap,
                    search=search_ap,
                    page=int(st.session_state["page"]),
                    page_size=page_size,
                    rutero=rutero,
                    reponedor=reponedor,
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

    if modo == "LOCAL":
        export_key = (dv, modo, cod_rt, tuple(marcas), foco_ap, search_ap)
    else:
        export_key = (dv, modo, modalidad_sel, rutero, reponedor, cod_rt, tuple(marcas), foco_ap, search_ap)

    if st.session_state.get("_export_key") != export_key:
        st.session_state["_export_key"] = export_key
        st.session_state.pop("_export_raw", None)
        st.session_state.pop("_export_df_key", None)
        st.session_state.pop("_export_df", None)
        st.session_state.pop("_export_excel_key", None)
        st.session_state.pop("_export_excel", None)
        st.session_state.pop("_export_pdf_key", None)
        st.session_state.pop("_export_pdf", None)
        st.session_state.pop("_focus_export_key", None)
        st.session_state.pop("_focus_export_df", None)
        st.session_state.pop("_focus_export_excel_key", None)
        st.session_state.pop("_focus_export_excel", None)
        st.session_state.pop("_focus_export_pdf_key", None)
        st.session_state.pop("_focus_export_pdf", None)
        st.session_state.pop("_export_scope", None)

    def _prepare_export(scope: str, fmt: str):
        if total_rows <= 0:
            return

        fmt = str(fmt or "").strip().lower()
        if fmt not in {"excel", "pdf"}:
            return

        df_export_raw = st.session_state.get("_export_raw")

        if df_export_raw is None:
            try:
                with _timed("EXPORT query", tag="CACHE"):
                    df_export_raw = stock_service.get_export_raw(
                        modo=modo,
                        cod_rt=cod_rt,
                        marcas=marcas,
                        foco=foco_ap,
                        search=search_ap,
                        rutero=rutero,
                        reponedor=reponedor,
                        modalidad=modalidad_sel,
                    )

                _dbg("EXPORT raw loaded", rows=0 if df_export_raw is None else len(df_export_raw))
                _dbg_block()
                st.session_state["_export_raw"] = df_export_raw

            except Exception as e:
                _dbg("FAIL export query", err=repr(e))
                st.error("No pude preparar export.")
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
                st.stop()

        if df_export_raw is None or df_export_raw.empty:
            st.session_state["_export_scope"] = scope
            return

        if scope == "local":
            df_export = st.session_state.get("_export_df")

            if st.session_state.get("_export_df_key") != export_key or df_export is None:
                with _timed("EXPORT build_df", tag="CACHE"):
                    df_export = build_export_df(df_export_raw)
                st.session_state["_export_df_key"] = export_key
                st.session_state["_export_df"] = df_export

            if fmt == "excel":
                if st.session_state.get("_export_excel_key") != export_key:
                    with _timed("EXPORT excel_bytes", tag="UI"):
                        st.session_state["_export_excel"] = export_excel_one_sheet(cod_rt, df_export)
                    st.session_state["_export_excel_key"] = export_key

            elif fmt == "pdf":
                if st.session_state.get("_export_pdf_key") != export_key:
                    if modo == "LOCAL":
                        pdf_lines = [
                            f"STOCK_ZERO · {cod_rt} · {nombre_local_rr}",
                            "GESTIÓN REPOSICIÓN",
                            f"Gestión: {panel_mercaderista}  |  Modalidad: {panel_modalidad}",
                            f"Fecha stock: {file_stamp}  |  Foco: {_foco_label(foco_ap)}",
                            f"Clientes: {', '.join(marcas) if marcas else 'Todos'}  |  Búsqueda: {search_ap if search_ap else '-'}",
                        ]
                    else:
                        pdf_lines = [
                            f"STOCK_ZERO · {cod_rt} · {nombre_local_rr}",
                            "GESTIÓN REPOSICIÓN",
                            f"Modalidad: {modalidad_sel}",
                            f"Rutero: {rutero}  |  Reponedor: {reponedor}",
                            f"Fecha stock: {file_stamp}  |  Foco: {_foco_label(foco_ap)}",
                            f"Clientes: {', '.join(marcas) if marcas else 'Todos'}  |  Búsqueda: {search_ap if search_ap else '-'}",
                        ]

                    with _timed("EXPORT pdf_bytes", tag="UI"):
                        st.session_state["_export_pdf"] = export_pdf_table(pdf_lines, df_export)
                    st.session_state["_export_pdf_key"] = export_key

        elif scope == "foco":
            focus_key = (export_key, foco_ap)
            df_focus = st.session_state.get("_focus_export_df")

            if st.session_state.get("_focus_export_key") != focus_key or df_focus is None:
                with _timed("EXPORT build_focus_df", tag="CACHE"):
                    df_focus = build_focus_export_df(df_export_raw, foco=foco_ap)
                st.session_state["_focus_export_key"] = focus_key
                st.session_state["_focus_export_df"] = df_focus

            if df_focus is not None and not df_focus.empty:
                if fmt == "excel":
                    if st.session_state.get("_focus_export_excel_key") != focus_key:
                        with _timed("EXPORT focus_excel_bytes", tag="UI"):
                            st.session_state["_focus_export_excel"] = export_excel_generic(f"{cod_rt}_FOCO", df_focus)
                        st.session_state["_focus_export_excel_key"] = focus_key

                elif fmt == "pdf":
                    if st.session_state.get("_focus_export_pdf_key") != focus_key:
                        if modo == "LOCAL":
                            pdf_focus_lines = [
                                f"STOCK_ZERO · {cod_rt} · {nombre_local_rr}",
                                "GESTIÓN DE INDICADORES",
                                f"Gestión: {panel_mercaderista}  |  Modalidad: {panel_modalidad}",
                                f"Fecha stock: {file_stamp}  |  Foco: {_foco_label(foco_ap)}",
                                f"Clientes: {', '.join(marcas) if marcas else 'Todos'}  |  Búsqueda: {search_ap if search_ap else '-'}",
                            ]
                        else:
                            pdf_focus_lines = [
                                f"STOCK_ZERO · {cod_rt} · {nombre_local_rr}",
                                "GESTIÓN DE INDICADORES",
                                f"Modalidad: {modalidad_sel}",
                                f"Rutero: {rutero}  |  Reponedor: {reponedor}",
                                f"Fecha stock: {file_stamp}  |  Foco: {_foco_label(foco_ap)}",
                                f"Clientes: {', '.join(marcas) if marcas else 'Todos'}  |  Búsqueda: {search_ap if search_ap else '-'}",
                            ]

                        with _timed("EXPORT focus_pdf_bytes", tag="UI"):
                            st.session_state["_focus_export_pdf"] = export_pdf_focus_table(pdf_focus_lines, df_focus)
                        st.session_state["_focus_export_pdf_key"] = focus_key

        st.session_state["_export_scope"] = scope

    st.markdown("---")

    if total_rows == 0:
        if total_skus_kpi == 0:
            st.caption("Sin stock para exportar.")
        else:
            st.caption("No hay filas para exportar con el filtro aplicado.")
    else:
        with st.expander("EXPORTAR FOCO: Gestión de indicadores", expanded=False):
            _prepare_export("foco", "excel")
            _prepare_export("foco", "pdf")

            if st.session_state.get("_focus_export_excel") is not None:
                st.download_button(
                    "Descargar Excel",
                    data=st.session_state["_focus_export_excel"],
                    file_name=f"STOCK_ZERO_FOCO_{cod_rt}_{file_stamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    key="download_focus_excel",
                )
            else:
                st.caption("Sin Excel disponible")

            if st.session_state.get("_focus_export_pdf") is not None:
                st.download_button(
                    "Descargar PDF",
                    data=st.session_state["_focus_export_pdf"],
                    file_name=f"STOCK_ZERO_FOCO_{cod_rt}_{file_stamp}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="download_focus_pdf",
                )
            else:
                st.caption("Sin PDF disponible")

        with st.expander("EXPORTAR LOCAL: Gestión reposición.", expanded=False):
            _prepare_export("local", "excel")
            _prepare_export("local", "pdf")

            if st.session_state.get("_export_excel") is not None:
                st.download_button(
                    "Descargar Excel",
                    data=st.session_state["_export_excel"],
                    file_name=f"STOCK_ZERO_LOCAL_{cod_rt}_{file_stamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    key="download_local_excel",
                )
            else:
                st.caption("Sin Excel disponible")

            if st.session_state.get("_export_pdf") is not None:
                st.download_button(
                    "Descargar PDF",
                    data=st.session_state["_export_pdf"],
                    file_name=f"STOCK_ZERO_LOCAL_{cod_rt}_{file_stamp}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="download_local_pdf",
                )
            else:
                st.caption("Sin PDF disponible")

