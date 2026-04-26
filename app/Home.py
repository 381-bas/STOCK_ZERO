# app/Home.py
import os
import time
import math
import logging
import traceback
import pandas as pd
from datetime import datetime, timezone

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
    if os.name == "nt":
        return "local"
    if any(os.path.exists(p) for p in ("/mount/src", "/home/appuser", "/home/adminuser")):
        return "public"
    if (os.getenv("USER", "") or os.getenv("USERNAME", "")).strip().lower() in {"appuser", "adminuser"}:
        return "public"
    return "public" if os.name != "nt" else "local"


def _ensure_run_context() -> None:
    seq = int(st.session_state.get("_run_seq", 0)) + 1
    stamp = int(time.time() * 1000) % 1000000
    st.session_state["_run_seq"] = seq
    st.session_state["_run_id"] = f"RUN{seq:04d}-{stamp:06d}"
    st.session_state["_runtime_env"] = _infer_runtime_env()
    st.session_state["_run_path"] = "cold" if seq == 1 else "warm"


def _dbg(msg: str, tag: str = "UI", **kv) -> None:
    global DEBUG
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
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


FOCO_OPTIONS = ["Venta 0", "Negativo", "Quiebres", "Otros"]


def _normalize_focos_ui(value) -> list[str]:
    if value is None:
        raw = []
    elif isinstance(value, str):
        raw = [x.strip() for x in value.replace("|", ",").split(",") if x.strip()]
    else:
        raw = [str(x).strip() for x in value if str(x).strip()]

    out = []
    for x in raw:
        if x in FOCO_OPTIONS and x not in out:
            out.append(x)
    return out


def _foco_label(value) -> str:
    focos = _normalize_focos_ui(value)
    return "Todo" if not focos else " | ".join(focos)


def _render_kpi_cards(kpis_row: dict | None) -> None:
    values = {
        "Venta 0": int((kpis_row or {}).get("venta_0") or 0),
        "Negativo": int((kpis_row or {}).get("negativos") or 0),
        "Quiebres": int((kpis_row or {}).get("quiebres") or 0),
        "Otros": int((kpis_row or {}).get("otros") or 0),
    }

    cards = [
        ("Venta 0", values["Venta 0"], "Productos sin rotación"),
        ("Negativo", values["Negativo"], "Ajustar inventario"),
        ("Quiebres", values["Quiebres"], "Solicitar empuje"),
        ("Otros", values["Otros"], "Observación cliente"),
    ]

    st.markdown(
        """
<style>
.sz-kpi-grid{
    display:grid;
    grid-template-columns:repeat(auto-fit,minmax(220px,1fr));
    gap:.75rem;
    margin:.35rem 0 .35rem 0;
}
.sz-kpi-card{
    border:1px solid rgba(128,128,128,.22);
    border-radius:12px;
    padding:.85rem .95rem;
    background:rgba(250,250,250,.02);
}
.sz-kpi-title{
    font-size:.95rem;
    font-weight:600;
    margin-bottom:.5rem;
}
.sz-kpi-body{
    display:flex;
    align-items:center;
    gap:.7rem;
}
.sz-kpi-value{
    font-size:2.1rem;
    line-height:1;
    font-weight:700;
    min-width:2.2rem;
}
.sz-kpi-desc{
    font-size:.98rem;
    line-height:1.2;
    opacity:.86;
}
</style>
        """,
        unsafe_allow_html=True,
    )

    cards_html = "".join(
        f'<div class="sz-kpi-card">'
        f'<div class="sz-kpi-title">{title}</div>'
        f'<div class="sz-kpi-body">'
        f'<div class="sz-kpi-value">{value}</div>'
        f'<div class="sz-kpi-desc">{desc}</div>'
        f'</div>'
        f'</div>'
        for title, value, desc in cards
    )

    st.markdown(f'<div class="sz-kpi-grid">{cards_html}</div>', unsafe_allow_html=True)
    

def _scope_value_or_none(value: str | None, all_token: str = "Todos") -> str | None:
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    if v.upper() == str(all_token).strip().upper():
        return None
    if v.upper() == "— SELECCIONA TIPO —".upper():
        return None
    return v


def _df_total_rows(df: pd.DataFrame | None) -> int:
    if df is None or df.empty or "total_rows" not in df.columns:
        return 0
    try:
        return int(df["total_rows"].iloc[0] or 0)
    except Exception:
        return 0


def _drop_total_rows(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    return df.drop(columns=["total_rows"], errors="ignore").copy()


def _rename_and_pick(
    df: pd.DataFrame | None,
    rename_map: dict[str, str],
    ordered_cols: list[str] | None = None,
) -> pd.DataFrame:
    view = _drop_total_rows(df).rename(columns=rename_map)
    if ordered_cols:
        for col in ordered_cols:
            if col not in view.columns:
                view[col] = ""
        view = view[ordered_cols]
    return view


def main():
    global DEBUG

    st.set_page_config(page_title="STOCK_ZERO", layout="wide")

    from app import db
    from app.exports import (
        build_export_df,
        build_focus_export_df,
        build_inventory_cliente_export_df,
        export_excel_one_sheet,
        export_excel_generic,
        export_pdf_table,
        export_pdf_generic,
        export_pdf_focus_table,
    )
    from app.screens.control_gestion import render_control_gestion

    DEBUG = _as_bool(_qp_get("debug", "")) or _as_bool(os.getenv("DEBUG_UI", ""))
    _ensure_run_context()
    _dbg("BOOT Home.py", py=str(getattr(__import__("sys"), "version", "na")).split()[0], run_seq=st.session_state.get("_run_seq"))
    _dbg_block()

    APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
    t_in = _qp_get("t", "").strip()

    st.title("STOCK ZERO")

    _dbg("RUN context ready", env=st.session_state.get("_runtime_env"), path=st.session_state.get("_run_path"))
    _dbg("TOKEN gate", token_set=bool(APP_TOKEN), t_present=bool(t_in))
    _dbg_block()

    if APP_TOKEN and t_in != APP_TOKEN:
        st.error("Link no válido o expirado. Solicita un link actualizado.")
        st.stop()

    if "init_done" not in st.session_state:
        st.session_state.init_done = True
        st.session_state.f_search = _qp_get("q", "")
        st.session_state.f_foco = _normalize_focos_ui(_qp_get("foco", ""))

        qp_mode = (_qp_get("modo", "") or "").strip().upper()
        st.session_state.home_mode = qp_mode if qp_mode in {"LOCAL", "MERCADERISTA", "CLIENTE", "CONTROL GESTION"} else "-Seleccionar Tipo-"
        
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
        st.session_state.pop("_export_raw", None)
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

    def _reset_filters_defaults(reason: str):
        st.session_state["sel_marcas"] = []
        st.session_state["sel_marca_home_cliente"] = MARCA_SCOPE_PLACEHOLDER
        st.session_state["f_search"] = ""
        st.session_state["f_foco"] = []
        st.session_state["applied_marcas"] = []
        st.session_state["applied_scope_marca"] = None
        st.session_state["applied_search"] = ""
        st.session_state["applied_foco"] = []
        st.session_state["page"] = 1

        # nuevo
        st.session_state["page_input_ui"] = 1
        st.session_state.pop("page_select_ui", None)

        _invalidate_runtime_cache()
        _dbg(f"RESET filters ({reason})")
        _dbg_block()

    def _reset_on_mode_change():
        st.session_state["sel_modalidad"] = ""
        st.session_state["sel_rr_label"] = ""
        st.session_state["sel_local_label"] = ""

        _reset_cliente_state(
            "mode_change",
            reset_marca=True,
            reset_cliente=True,
            reset_tipo=True,
            reset_responsable=True,
        )
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

    def _reset_cliente_state(
        reason: str,
        *,
        reset_marca: bool = False,
        reset_cliente: bool = True,
        reset_tipo: bool = True,
        reset_responsable: bool = True,
    ):
        if reset_marca:
            st.session_state["sel_marca_home_cliente"] = MARCA_SCOPE_PLACEHOLDER
        if reset_cliente:
            st.session_state["sel_cliente_home_scope"] = CLIENTE_SCOPE_PLACEHOLDER
        if reset_tipo:
            st.session_state["sel_responsable_tipo"] = RESP_TIPO_PLACEHOLDER
        if reset_responsable:
            st.session_state["sel_responsable_lista"] = RESP_LISTA_PLACEHOLDER

        st.session_state["scope_level_cliente_mode"] = "L0"
        st.session_state["page_cliente_scope"] = 1
        st.session_state["page_cliente_scope_ui"] = 1
        st.session_state["page_cliente_detalle"] = 1
        st.session_state["page_cliente_detalle_ui"] = 1
        _dbg(f"RESET cliente_state ({reason})")
        _dbg_block()


    def _reset_on_cliente_marca_change():
        _reset_filters_defaults("cliente_marca_change")


    def _reset_on_cliente_change():
        _reset_cliente_state(
            "cliente_change",
            reset_marca=False,
            reset_cliente=False,
            reset_tipo=True,
            reset_responsable=True,
        )
        _reset_filters_defaults("cliente_change")


    def _reset_on_responsable_tipo_change():
        _reset_cliente_state(
            "responsable_tipo_change",
            reset_marca=False,
            reset_cliente=False,
            reset_tipo=False,
            reset_responsable=True,
        )
        _reset_filters_defaults("responsable_tipo_change")


    def _reset_on_responsable_lista_change():
        _reset_cliente_state(
            "responsable_lista_change",
            reset_marca=False,
            reset_cliente=False,
            reset_tipo=False,
            reset_responsable=False,
        )
        _reset_filters_defaults("responsable_lista_change")


    TYPE_PLACEHOLDER = "-Seleccionar Tipo-"
    LOCAL_PLACEHOLDER = "— Selecciona local —"
    MODALIDAD_PLACEHOLDER = "— Selecciona modalidad —"
    RR_PLACEHOLDER = "— Selecciona rutero—reponedor —"

    MARCA_SCOPE_PLACEHOLDER = "Todas"
    CLIENTE_SCOPE_PLACEHOLDER = "Todos"
    RESP_TIPO_PLACEHOLDER = "— Selecciona tipo —"
    RESP_TIPO_ALL = "Todos"
    RESP_LISTA_PLACEHOLDER = "Todos"

    st.session_state.setdefault("sel_marca_home_cliente", MARCA_SCOPE_PLACEHOLDER)
    st.session_state.setdefault("applied_scope_marca", None)
    st.session_state.setdefault("sel_cliente_home_scope", CLIENTE_SCOPE_PLACEHOLDER)
    st.session_state.setdefault("sel_responsable_tipo", RESP_TIPO_PLACEHOLDER)
    st.session_state.setdefault("sel_responsable_lista", RESP_LISTA_PLACEHOLDER)
    st.session_state.setdefault("scope_level_cliente_mode", "L0")

    st.session_state.setdefault("page_cliente_scope", 1)
    st.session_state.setdefault("page_cliente_scope_ui", 1)
    st.session_state.setdefault("page_cliente_detalle", 1)
    st.session_state.setdefault("page_cliente_detalle_ui", 1)

    st.session_state.setdefault("sel_cg_role", "JEFE OPERACIONES")
    st.session_state.setdefault("sel_cg_module", "Inicio")


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

    mode_opts = [TYPE_PLACEHOLDER, "LOCAL", "MERCADERISTA", "CLIENTE", "CONTROL GESTION"]
    st.session_state.setdefault("home_mode", TYPE_PLACEHOLDER)

    with top1:
        st.selectbox(
            "CONSULTA",
            mode_opts,
            key="home_mode",
            on_change=_reset_on_mode_change,
        )

    modo = st.session_state["home_mode"]

    if modo not in {"LOCAL", "MERCADERISTA", "CLIENTE", "CONTROL GESTION"}:
        st.info("Selecciona tipo de consulta para habilitar el flujo operativo.")
        st.stop()

    # ==============================================
    # MODO CLIENTE
    # ==============================================

    if modo == "CLIENTE":
        try:
            with _timed("SELECTOR marcas_home_global", tag="SELECTOR"):
                marcas_scope = db.get_marcas_home_global()
                _dbg("CLIENTE marcas loaded", rows=len(marcas_scope))
                _dbg_block()
        except Exception as e:
            _dbg("FAIL marcas_home_global", err=repr(e))
            st.error("No pude leer marcas para CLIENTE.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        marca_opts = [MARCA_SCOPE_PLACEHOLDER] + list(marcas_scope or [])
        if st.session_state.get("sel_marca_home_cliente", MARCA_SCOPE_PLACEHOLDER) not in marca_opts:
            st.session_state["sel_marca_home_cliente"] = MARCA_SCOPE_PLACEHOLDER

        try:
            with _timed("SELECTOR clientes_home_scope", tag="SELECTOR"):
                clientes_scope = db.get_clientes_home_scope()
                _dbg("CLIENTE clientes loaded", rows=len(clientes_scope))
                _dbg_block()
        except Exception as e:
            _dbg("FAIL clientes_home_scope", err=repr(e))
            st.error("No pude leer clientes para el scope seleccionado.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        cliente_opts = [CLIENTE_SCOPE_PLACEHOLDER] + list(clientes_scope or [])
        if st.session_state.get("sel_cliente_home_scope", CLIENTE_SCOPE_PLACEHOLDER) not in cliente_opts:
            st.session_state["sel_cliente_home_scope"] = CLIENTE_SCOPE_PLACEHOLDER

        with top2:
            st.selectbox(
                "CLIENTE",
                cliente_opts,
                key="sel_cliente_home_scope",
                on_change=_reset_on_cliente_change,
            )

        cliente_sel = _scope_value_or_none(
            st.session_state.get("sel_cliente_home_scope"),
            CLIENTE_SCOPE_PLACEHOLDER,
        )

        with top3:
            st.selectbox(
                "RESPONSABLE TIPO",
                [RESP_TIPO_PLACEHOLDER, RESP_TIPO_ALL, "GESTOR", "SUPERVISOR"],
                key="sel_responsable_tipo",
                on_change=_reset_on_responsable_tipo_change,
            )

        responsable_tipo_raw = str(st.session_state.get("sel_responsable_tipo") or "").strip()
        responsable_tipo_all = responsable_tipo_raw.upper() == RESP_TIPO_ALL.upper()
        responsable_tipo_sel = (
            None
            if responsable_tipo_raw in {"", RESP_TIPO_PLACEHOLDER, RESP_TIPO_ALL}
            else responsable_tipo_raw
        )
        kpi_only_tipo_scope = responsable_tipo_raw in {"", RESP_TIPO_PLACEHOLDER, RESP_TIPO_ALL}

        if responsable_tipo_sel:
            try:
                with _timed("SELECTOR responsables_home_scope", tag="SELECTOR"):
                    responsables_scope = db.get_responsables_home_scope(
                        tipo=responsable_tipo_sel,
                        cliente=cliente_sel,
                    )
                    _dbg(
                        "CLIENTE responsables loaded",
                        rows=len(responsables_scope),
                        tipo=responsable_tipo_sel,
                        cliente=cliente_sel or "Todos",
                    )
                    _dbg_block()
            except Exception as e:
                _dbg("FAIL responsables_home_scope", err=repr(e))
                st.error("No pude leer responsables para el scope seleccionado.")
                with st.expander("Detalles técnicos"):
                    st.code(repr(e))
                    if DEBUG:
                        st.code(traceback.format_exc())
                st.stop()
        else:
            responsables_scope = []

        responsable_opts = [RESP_LISTA_PLACEHOLDER] + list(responsables_scope or [])
        if st.session_state.get("sel_responsable_lista", RESP_LISTA_PLACEHOLDER) not in responsable_opts:
            st.session_state["sel_responsable_lista"] = RESP_LISTA_PLACEHOLDER

        row2c1, row2c2 = st.columns([2.0, 1.0], gap="small")

        with row2c1:
            if responsable_tipo_sel:
                st.selectbox(
                    "RESPONSABLE",
                    responsable_opts,
                    key="sel_responsable_lista",
                    on_change=_reset_on_responsable_lista_change,
                )
            else:
                st.session_state["sel_responsable_lista"] = RESP_LISTA_PLACEHOLDER

        responsable_sel = _scope_value_or_none(
            st.session_state.get("sel_responsable_lista"),
            RESP_LISTA_PLACEHOLDER,
        )

        if not clientes_scope:
            st.warning("No hay clientes operativos disponibles en el scope actual.")
        if responsable_tipo_sel and not responsables_scope:
            st.warning("No hay responsables para el tipo y scope seleccionados.")

        scope_level = db.get_scope_level(
            cliente=cliente_sel,
            responsable=responsable_sel,
        )
        scope_label = scope_level if responsable_tipo_all else ("KPI" if kpi_only_tipo_scope else scope_level)
        st.session_state["scope_level_cliente_mode"] = scope_level

        def _apply_scope_filters():
            st.session_state["applied_scope_marca"] = _scope_value_or_none(
                st.session_state.get("sel_marca_home_cliente"),
                MARCA_SCOPE_PLACEHOLDER,
            )
            st.session_state["applied_foco"] = _normalize_focos_ui(st.session_state.get("f_foco", []))
            st.session_state["applied_search"] = (st.session_state.get("f_search", "") or "").strip()

            st.session_state["page_cliente_scope"] = 1
            st.session_state["page_cliente_scope_ui"] = 1
            st.session_state["page_cliente_detalle"] = 1
            st.session_state["page_cliente_detalle_ui"] = 1

            _dbg(
                "APPLY filters CLIENTE",
                marca=st.session_state.get("applied_scope_marca") or "Todas",
                foco=st.session_state["applied_foco"],
                q=bool(st.session_state["applied_search"]),
                scope=scope_level,
            )
            _dbg_block()

        filters_expanded_cliente = bool(
            st.session_state.get("applied_scope_marca")
            or _normalize_focos_ui(st.session_state.get("applied_foco", []))
            or (st.session_state.get("applied_search", "") or "").strip()
        )

        def _render_scope_filters_panel():
            with st.expander("FILTROS (opcional)", expanded=filters_expanded_cliente):
                with st.form("filters_form_cliente", clear_on_submit=False):
                    f0, f1, f2 = st.columns([1.2, 1.1, 2.0], gap="small")

                    with f0:
                        st.selectbox(
                            "MARCA",
                            marca_opts,
                            key="sel_marca_home_cliente",
                        )

                    with f1:
                        st.multiselect(
                            "Foco operativo",
                            options=FOCO_OPTIONS,
                            key="f_foco",
                            placeholder="Todo",
                        )

                    with f2:
                        st.text_input(
                            "Búsqueda (SKU o descripción)",
                            key="f_search",
                            placeholder="Ej: 779... / galleta / snack... (mín 2 caracteres)",
                        )

                    st.form_submit_button(
                        "Aplicar",
                        on_click=_apply_scope_filters,
                        use_container_width=True,
                    )

        marca_ap = _scope_value_or_none(
            st.session_state.get("applied_scope_marca"),
            MARCA_SCOPE_PLACEHOLDER,
        )
        foco_ap = _normalize_focos_ui(st.session_state.get("applied_foco", []))
        search_ap = (st.session_state.get("applied_search", "") or "").strip()

        meta1, meta2, meta3, meta4, meta5 = st.columns([1.0, 1.3, 1.3, 1.2, 1.2], gap="small")
        meta1.caption(f"Scope: {scope_label}")
        meta2.caption(f"Cliente: {cliente_sel or 'Todos'}")
        meta3.caption(f"Resp. tipo: {responsable_tipo_raw or '-'}")
        meta4.caption(f"Responsable: {responsable_sel or 'Todos'}")
        meta5.caption(f"Marca filtro: {marca_ap or 'Todas'}")

        try:
            with _timed("PAGE kpis_scope_cliente", tag="PAGE"):
                kpis_scope = db.get_kpis_scope_cliente(
                    marca=marca_ap,
                    cliente=cliente_sel,
                    responsable_tipo=responsable_tipo_sel,
                    responsable=responsable_sel,
                    focos=foco_ap,
                    search=search_ap,
                )
                _dbg("CLIENTE KPIS loaded", rows=0 if kpis_scope is None else len(kpis_scope))
                _dbg_block()
        except Exception as e:
            _dbg("FAIL kpis_scope_cliente", err=repr(e))
            st.error("No pude leer KPIs del scope CLIENTE.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        kpi_scope_row = dict(kpis_scope.iloc[0]) if (kpis_scope is not None and not kpis_scope.empty) else {}
        _render_kpi_cards(kpi_scope_row)

        fecha_stock_raw = (kpi_scope_row or {}).get("fecha_stock")
        fecha_stock_dt = pd.to_datetime(fecha_stock_raw, errors="coerce")
        file_stamp = fecha_stock_dt.strftime("%Y-%m-%d") if pd.notna(fecha_stock_dt) else "Sin stock"

        c1, c2, c3, c4, c5 = st.columns([1.1, 1.1, 1.1, 1.1, 1.1], gap="small")
        c1.caption(f"Fecha stock: {file_stamp}")
        c2.caption(f"Locales: {int((kpi_scope_row or {}).get('locales_scope') or 0)}")
        c3.caption(f"Clientes: {int((kpi_scope_row or {}).get('clientes_scope') or 0)}")
        c4.caption(f"Responsables: {int((kpi_scope_row or {}).get('responsables_scope') or 0)}")
        skus_scope_total = int(
            (kpi_scope_row or {}).get("total_skus")
            or (kpi_scope_row or {}).get("skus_scope")
            or 0
        )
        c5.caption(f"SKUs scope: {skus_scope_total}")
        _render_scope_filters_panel()
        total_skus_scope = skus_scope_total
        total_skus_scope = int((kpi_scope_row or {}).get("total_skus") or 0)
        if total_skus_scope == 0:
            st.warning("El scope operativo existe, pero no cruza con stock/fact para la selección actual.")

        if kpi_only_tipo_scope and not responsable_tipo_all:
            st.caption("Selecciona Todos, GESTOR o SUPERVISOR para habilitar la lectura del scope.")
            st.stop()

        def _get_page_state(page_key: str, page_size: int = 25) -> tuple[int, int]:
            input_key = f"{page_key}_ui"

            st.session_state.setdefault(page_key, 1)
            st.session_state.setdefault(input_key, 1)

            try:
                current_page = int(st.session_state.get(page_key, 1))
            except Exception:
                current_page = 1

            current_page = max(1, current_page)
            st.session_state[page_key] = current_page

            if int(st.session_state.get(input_key, current_page)) != current_page:
                st.session_state[input_key] = current_page

            return current_page, (current_page - 1) * page_size

        def _render_scope_pager(page_key: str, total_rows: int, page_size: int = 25):
            input_key = f"{page_key}_ui"
            total_pages = max(1, int(math.ceil(total_rows / page_size))) if total_rows > 0 else 1

            if int(st.session_state.get(page_key, 1)) > total_pages:
                st.session_state[page_key] = total_pages
            if int(st.session_state.get(page_key, 1)) < 1:
                st.session_state[page_key] = 1

            current_page = int(st.session_state.get(page_key, 1))
            if int(st.session_state.get(input_key, current_page)) != current_page:
                st.session_state[input_key] = current_page

            def _prev():
                st.session_state[page_key] = max(1, int(st.session_state.get(page_key, 1)) - 1)
                st.session_state[input_key] = st.session_state[page_key]

            def _next():
                st.session_state[page_key] = min(total_pages, int(st.session_state.get(page_key, 1)) + 1)
                st.session_state[input_key] = st.session_state[page_key]

            def _from_input():
                try:
                    new_page = int(st.session_state.get(input_key, 1))
                except Exception:
                    new_page = 1
                new_page = max(1, min(total_pages, new_page))
                st.session_state[page_key] = new_page
                st.session_state[input_key] = new_page

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
                    key=f"{page_key}_prev_btn",
                    disabled=(current_page <= 1),
                    on_click=_prev,
                    use_container_width=True,
                )

            with p2:
                st.number_input(
                    "Página",
                    min_value=1,
                    max_value=max(1, total_pages),
                    step=1,
                    key=input_key,
                    on_change=_from_input,
                    label_visibility="collapsed",
                )

            with p3:
                st.button(
                    "▶",
                    key=f"{page_key}_next_btn",
                    disabled=(current_page >= total_pages),
                    on_click=_next,
                    use_container_width=True,
                )

            with p4:
                st.caption(pager_text)

        def _render_rr_people_context():
            if scope_level not in {"L2", "L3"}:
                return
            if not responsable_tipo_sel or not responsable_sel:
                return

            try:
                with _timed("PAGE rr_people_scope", tag="PAGE"):
                    rr_people = db.get_rr_people_scope(
                        tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        marca=marca_ap,
                        cliente=cliente_sel,
                    )
                    _dbg("CLIENTE rr_people loaded", rows=0 if rr_people is None else len(rr_people))
                    _dbg_block()
            except Exception as e:
                _dbg("FAIL rr_people_scope", err=repr(e))
                st.warning("No pude cargar MERCADERISTA A CARGO para este scope.")
                return

            if rr_people is None or rr_people.empty:
                return

            rr_view = rr_people.rename(columns={
                "rutero": "RUTERO",
                "reponedor": "REPONEDOR",
                "responsable": "RESPONSABLE",
                "cliente": "CLIENTE",
                "locales_relacionados": "LOCALES RELACIONADOS",
            })

            with st.expander(f"MERCADERISTA A CARGO ({len(rr_view)} filas)", expanded=False):
                st.dataframe(rr_view, width="stretch", hide_index=True)

        def _scope_file_token() -> str:
            raw = responsable_sel or cliente_sel or "scope"
            token = "".join(ch if str(ch).isalnum() else "_" for ch in str(raw))
            token = token.strip("_")
            return token[:60] or "scope"

        def _render_cliente_exports_l1_global() -> None:
            if scope_level != "L1":
                return
            if not cliente_sel or not responsable_tipo_all or responsable_sel:
                return

            try:
                with _timed("EXPORT cliente_l1_global_query", tag="CACHE"):
                    df_cliente_raw = db.get_export_inventario_cliente(cliente=cliente_sel)
                _dbg("CLIENTE L1 export raw loaded", rows=0 if df_cliente_raw is None else len(df_cliente_raw))
                _dbg_block()
            except Exception as e:
                _dbg("FAIL cliente_l1_global_export", err=repr(e))
                st.warning("No pude preparar los descargables globales del cliente.")
                return

            if df_cliente_raw is None or df_cliente_raw.empty:
                st.caption("Sin filas para exportar en el cliente seleccionado.")
                return

            scope_token = _scope_file_token()
            df_inventory_cliente = build_inventory_cliente_export_df(df_cliente_raw)
            df_focus_cliente = build_focus_export_df(df_cliente_raw, foco="Todo")

            st.markdown("#### Descargables cliente")
            ex1, ex2 = st.columns(2, gap="small")

            with ex1:
                inventory_excel = export_excel_generic(f"CLIENTE_{scope_token}", df_inventory_cliente)
                st.download_button(
                    "Inventario cliente",
                    data=inventory_excel,
                    file_name=f"STOCK_ZERO_INVENTARIO_CLIENTE_{scope_token}_{file_stamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    key=f"cliente_l1_inventory_excel_{scope_token}",
                )

            with ex2:
                if df_focus_cliente is None or df_focus_cliente.empty:
                    st.caption("Sin focos activos para el cliente seleccionado.")
                else:
                    focus_excel = export_excel_generic(f"CLIENTE_{scope_token}_FOCO", df_focus_cliente)
                    st.download_button(
                        "Foco Cliente",
                        data=focus_excel,
                        file_name=f"STOCK_ZERO_FOCO_CLIENTE_{scope_token}_{file_stamp}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key=f"cliente_l1_focus_excel_{scope_token}",
                    )

        def _render_cliente_exports() -> None:
            if scope_level not in {"L2", "L3"}:
                return

            local_rename = {
                "cod_rt": "COD_RT",
                "local_nombre_rr": "LOCAL",
                "nombre_local_rr": "LOCAL",
                "cliente": "CLIENTE",
                "rutero": "RUTERO",
                "reponedor": "REPONEDOR",
                "skus_scope": "TOTAL SKUS",
                "total_skus": "TOTAL SKUS",
                "venta_0": "VENTA 0",
                "negativos": "NEGATIVOS",
                "quiebres": "QUIEBRES OBS.",
                "otros": "OTROS",
                "skus_en_foco": "SKUS EN FOCO",
            }
            local_cols = [
                "COD_RT", "LOCAL", "CLIENTE", "RUTERO", "REPONEDOR",
                "TOTAL SKUS", "VENTA 0", "NEGATIVOS", "OTROS", "QUIEBRES OBS.", "SKUS EN FOCO",
            ]

            def _split_pipe_values(value) -> list[str]:
                raw = str(value or "").strip()
                if not raw:
                    return []
                return [part.strip() for part in raw.split("|") if part.strip()]

            def _match_multi_value(value, target: str | None) -> bool:
                if not target:
                    return True
                return str(target).strip().upper() in {x.upper() for x in _split_pipe_values(value)}

            scope_token = _scope_file_token()
            title_base = [
                "STOCK_ZERO · CLIENTE",
                f"Cliente: {cliente_sel or 'Todos'} | Resp. tipo: {responsable_tipo_raw or '-'} | Responsable: {responsable_sel or 'Todos'}",
                f"Marca: {marca_ap or 'Todas'} | Foco: {_foco_label(foco_ap)} | Búsqueda: {search_ap if search_ap else '-'}",
                f"Fecha stock: {file_stamp}",
            ]

            try:
                with _timed("EXPORT cliente_scope_local_query", tag="CACHE"):
                    df_local_export_raw = db.get_tabla_scope_local_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=None,
                        offset=None,
                    )
                df_local_export = _rename_and_pick(df_local_export_raw, local_rename, local_cols)
            except Exception as e:
                _dbg("FAIL cliente_scope_local_export", err=repr(e))
                st.warning("No pude preparar la exportación del scope.")
                return

            if df_local_export is None or df_local_export.empty:
                st.caption("Sin filas de scope para exportar.")
                return

            local_labels = ["Todos"]
            local_map = {}
            for _, row in df_local_export[["COD_RT", "LOCAL"]].drop_duplicates().iterrows():
                label = f"{row['COD_RT']} · {row['LOCAL']}"
                local_labels.append(label)
                local_map[label] = str(row["COD_RT"])

            mercaderistas = []
            for value in df_local_export.get("REPONEDOR", pd.Series(dtype=str)):
                for item in _split_pipe_values(value):
                    if item not in mercaderistas:
                        mercaderistas.append(item)

            st.markdown("#### Exportar scope")
            fx1, fx2 = st.columns(2, gap="small")
            with fx1:
                local_export_sel = st.selectbox(
                    "LOCAL para descarga",
                    local_labels,
                    key=f"cliente_export_local_sel_{scope_token}",
                )
            with fx2:
                mercaderista_export_sel = st.selectbox(
                    "MERCADERISTA para descarga",
                    ["Todos"] + mercaderistas,
                    key=f"cliente_export_merca_sel_{scope_token}",
                )

            selected_cod_rt = local_map.get(local_export_sel)
            selected_mercaderista = None if mercaderista_export_sel == "Todos" else mercaderista_export_sel

            df_local_filtered = df_local_export.copy()
            if selected_cod_rt:
                df_local_filtered = df_local_filtered[df_local_filtered["COD_RT"].astype(str) == str(selected_cod_rt)].copy()
            if selected_mercaderista:
                df_local_filtered = df_local_filtered[
                    df_local_filtered["REPONEDOR"].apply(lambda v: _match_multi_value(v, selected_mercaderista))
                ].copy()

            selected_cod_rts = set(df_local_filtered["COD_RT"].astype(str).tolist())

            title_scope_extra = []
            if selected_cod_rt:
                title_scope_extra.append(f"Local: {local_export_sel}")
            if selected_mercaderista:
                title_scope_extra.append(f"Mercaderista: {selected_mercaderista}")
            title_lines = title_base + ([" | ".join(title_scope_extra)] if title_scope_extra else [])

            ex1, ex2 = st.columns(2, gap="small")

            with ex1:
                with st.expander("LISTADO GLOBAL FOCOS", expanded=False):
                    if df_local_filtered.empty:
                        st.caption("Sin filas para exportar con el filtro de descarga actual.")
                    else:
                        local_excel = export_excel_generic(f"CLIENTE_{scope_token}", df_local_filtered)
                        local_pdf = export_pdf_generic(
                            title_lines + ["LISTADO GLOBAL FOCOS"],
                            df_local_filtered,
                            list(df_local_filtered.columns),
                        )
                        st.download_button(
                            "Descargar Excel",
                            data=local_excel,
                            file_name=f"STOCK_ZERO_CLIENTE_LOCALES_{scope_token}_{file_stamp}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            key=f"cliente_scope_local_excel_{scope_level}",
                        )
                        st.download_button(
                            "Descargar PDF",
                            data=local_pdf,
                            file_name=f"STOCK_ZERO_CLIENTE_LOCALES_{scope_token}_{file_stamp}.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                            key=f"cliente_scope_local_pdf_{scope_level}",
                        )

            with ex2:
                with st.expander("EXPORTAR SALAS FOCOS", expanded=False):
                    try:
                        with _timed("EXPORT cliente_scope_focus_query", tag="CACHE"):
                            df_focus_raw = db.get_detalle_sku_scope_total_page(
                                marca=marca_ap,
                                cliente=cliente_sel,
                                responsable_tipo=responsable_tipo_sel,
                                responsable=responsable_sel,
                                focos=foco_ap,
                                search=search_ap,
                                limit=None,
                                offset=None,
                            )

                        df_focus_input = _drop_total_rows(df_focus_raw).rename(columns={
                            "fecha": "fecha",
                            "cod_rt": "COD_RT",
                            "nombre_local_rr": "LOCAL",
                            "cliente": "CLIENTE",
                            "marca": "MARCA",
                            "sku": "Sku",
                            "descripcion": "Descripción del Producto",
                            "stock": "Stock",
                            "venta_7": "Venta(+7)",
                            "negativo": "NEGATIVO",
                            "riesgo_quiebre": "RIESGO DE QUIEBRE",
                            "otros": "OTROS",
                        })

                        ctx_cols = ["COD_RT", "LOCAL", "CLIENTE", "RUTERO", "REPONEDOR"]
                        df_focus_ctx = df_local_filtered[ctx_cols].copy() if not df_local_filtered.empty else pd.DataFrame(columns=ctx_cols)
                        if not df_focus_ctx.empty:
                            df_focus_ctx = df_focus_ctx.drop_duplicates(subset=["COD_RT", "CLIENTE"], keep="first")
                            df_focus_input = df_focus_input.merge(
                                df_focus_ctx,
                                how="left",
                                on=["COD_RT", "CLIENTE"],
                                suffixes=("", "_CTX"),
                            )
                            for col in ["LOCAL", "RUTERO", "REPONEDOR"]:
                                ctx_col = f"{col}_CTX"
                                if ctx_col in df_focus_input.columns:
                                    if col not in df_focus_input.columns:
                                        df_focus_input[col] = ""
                                    df_focus_input[col] = df_focus_input[col].where(
                                        df_focus_input[col].astype(str).str.strip() != "",
                                        df_focus_input[ctx_col],
                                    )
                                    df_focus_input = df_focus_input.drop(columns=[ctx_col])
                    except Exception as e:
                        _dbg("FAIL cliente_scope_focus_export", err=repr(e))
                        st.warning("No pude preparar la exportación de focos del scope.")
                        df_focus_input = pd.DataFrame()

                    if df_focus_input is None or df_focus_input.empty:
                        st.caption("Sin focos exportables para este scope.")
                    else:
                        if selected_cod_rts:
                            df_focus_input = df_focus_input[
                                df_focus_input["COD_RT"].astype(str).isin(selected_cod_rts)
                            ].copy()
                        else:
                            df_focus_input = df_focus_input.iloc[0:0].copy()

                        df_focus_export = build_focus_export_df(df_focus_input, foco=foco_ap)

                        if df_focus_export is None or df_focus_export.empty:
                            st.caption("Sin focos exportables para el filtro de descarga actual.")
                        else:
                            focus_excel = export_excel_generic(f"CLIENTE_{scope_token}_FOCO", df_focus_export)
                            focus_pdf = export_pdf_focus_table(
                                title_lines + ["EXPORTAR SALAS FOCOS"],
                                df_focus_export,
                            )
                            st.download_button(
                                "Descargar Excel",
                                data=focus_excel,
                                file_name=f"STOCK_ZERO_CLIENTE_FOCOS_{scope_token}_{file_stamp}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True,
                                key=f"cliente_scope_focus_excel_{scope_level}",
                            )
                            st.download_button(
                                "Descargar PDF",
                                data=focus_pdf,
                                file_name=f"STOCK_ZERO_CLIENTE_FOCOS_{scope_token}_{file_stamp}.pdf",
                                mime="application/pdf",
                                use_container_width=True,
                                key=f"cliente_scope_focus_pdf_{scope_level}",
                            )

        # -----------------------------
        # L0
        # -----------------------------
        if scope_level == "L0":
            try:
                with _timed("PAGE tabla_scope_responsable_L0", tag="PAGE"):
                    df_resp = db.get_tabla_scope_responsable_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=500,
                        offset=0,
                    )

                with _timed("PAGE tabla_scope_cliente_L0", tag="PAGE"):
                    df_cli = db.get_tabla_scope_cliente_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=500,
                        offset=0,
                    )
            except Exception as e:
                _dbg("FAIL tablas_scope_L0", err=repr(e))
                st.error("No pude leer tablas agregadas del scope global.")
                with st.expander("Detalles técnicos"):
                    st.code(repr(e))
                    if DEBUG:
                        st.code(traceback.format_exc())
                st.stop()

            st.markdown("#### Responsables en scope")
            df_resp_view = _rename_and_pick(
                df_resp,
                {
                    "responsable_tipo": "RESP. TIPO",
                    "responsable": "RESPONSABLE",
                    "clientes": "CLIENTES",
                    "locales": "LOCALES",
                    "skus_scope": "TOTAL SKUS",
                    "total_skus": "TOTAL SKUS",
                    "venta_0": "VENTA 0",
                    "negativos": "NEGATIVOS",
                    "quiebres": "QUIEBRES OBS.",
                    "otros": "OTROS",
                    "skus_en_foco": "SKUS EN FOCO",
                },
                [
                    "RESP. TIPO",
                    "RESPONSABLE",
                    "CLIENTES",
                    "LOCALES",
                    "TOTAL SKUS",
                    "VENTA 0",
                    "NEGATIVOS",
                    "OTROS",
                    "QUIEBRES OBS.",
                    "SKUS EN FOCO",
                ],
            )
            st.dataframe(df_resp_view, width="stretch", hide_index=True)

            st.markdown("#### Clientes en scope")
            df_cli_view = _rename_and_pick(
                df_cli,
                {
                    "cliente": "CLIENTE",
                    "responsables": "RESPONSABLES",
                    "locales": "LOCALES",
                    "skus_scope": "TOTAL SKUS",
                    "total_skus": "TOTAL SKUS",
                    "venta_0": "VENTA 0",
                    "negativos": "NEGATIVOS",
                    "quiebres": "QUIEBRES OBS.",
                    "otros": "OTROS",
                    "skus_en_foco": "SKUS EN FOCO",
                },
                [
                    "CLIENTE",
                    "RESPONSABLES",
                    "LOCALES",
                    "TOTAL SKUS",
                    "VENTA 0",
                    "NEGATIVOS",
                    "OTROS",
                    "QUIEBRES OBS.",
                    "SKUS EN FOCO",
                ],
            )
            st.dataframe(df_cli_view, width="stretch", hide_index=True)

        # -----------------------------
        # L1
        # -----------------------------
        elif scope_level == "L1":
            try:
                with _timed("PAGE tabla_scope_responsable_L1", tag="PAGE"):
                    df_resp = db.get_tabla_scope_responsable_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=500,
                        offset=0,
                    )

                current_page, offset = _get_page_state("page_cliente_scope", page_size=25)
                with _timed("PAGE tabla_scope_local_L1", tag="PAGE"):
                    df_local = db.get_tabla_scope_local_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=25,
                        offset=offset,
                    )
            except Exception as e:
                _dbg("FAIL tablas_scope_L1", err=repr(e))
                st.error("No pude leer la vista por cliente.")
                with st.expander("Detalles técnicos"):
                    st.code(repr(e))
                    if DEBUG:
                        st.code(traceback.format_exc())
                st.stop()

            st.markdown("#### Responsables del cliente")
            df_resp_view = _rename_and_pick(
                df_resp,
                {
                    "responsable_tipo": "RESP. TIPO",
                    "responsable": "RESPONSABLE",
                    "clientes": "CLIENTES",
                    "locales": "LOCALES",
                    "skus_scope": "TOTAL SKUS",
                    "total_skus": "TOTAL SKUS",
                    "venta_0": "VENTA 0",
                    "negativos": "NEGATIVOS",
                    "quiebres": "QUIEBRES OBS.",
                    "otros": "OTROS",
                    "skus_en_foco": "SKUS EN FOCO",
                },
                [
                    "RESP. TIPO",
                    "RESPONSABLE",
                    "CLIENTES",
                    "LOCALES",
                    "TOTAL SKUS",
                    "VENTA 0",
                    "NEGATIVOS",
                    "OTROS",
                    "QUIEBRES OBS.",
                    "SKUS EN FOCO",
                ],
            )
            st.dataframe(df_resp_view, width="stretch", hide_index=True)

            st.markdown("#### Locales del cliente")
            _render_scope_pager("page_cliente_scope", _df_total_rows(df_local), page_size=25)
            df_local_view = _rename_and_pick(
                df_local,
                {
                    "cod_rt": "COD_RT",
                    "local_nombre_rr": "LOCAL",
                    "nombre_local_rr": "LOCAL",
                    "cliente": "CLIENTE",
                    "responsable_tipo": "RESP. TIPO",
                    "responsable": "RESPONSABLE",
                    "skus_scope": "TOTAL SKUS",
                    "total_skus": "TOTAL SKUS",
                    "venta_0": "VENTA 0",
                    "negativos": "NEGATIVOS",
                    "quiebres": "QUIEBRES OBS.",
                    "otros": "OTROS",
                    "skus_en_foco": "SKUS EN FOCO",
                },
                [
                    "COD_RT",
                    "LOCAL",
                    "CLIENTE",
                    "RESP. TIPO",
                    "RESPONSABLE",
                    "TOTAL SKUS",
                    "VENTA 0",
                    "NEGATIVOS",
                    "OTROS",
                    "QUIEBRES OBS.",
                    "SKUS EN FOCO",
                ],
            )
            st.dataframe(df_local_view, width="stretch", hide_index=True)
            _render_cliente_exports_l1_global()
            
        # -----------------------------
        # L2
        # -----------------------------
        elif scope_level == "L2":
            try:
                with _timed("PAGE tabla_scope_cliente_L2", tag="PAGE"):
                    df_cli = db.get_tabla_scope_cliente_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=500,
                        offset=0,
                    )

                current_page, offset = _get_page_state("page_cliente_scope", page_size=25)
                with _timed("PAGE tabla_scope_local_L2", tag="PAGE"):
                    df_local = db.get_tabla_scope_local_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=25,
                        offset=offset,
                    )
            except Exception as e:
                _dbg("FAIL tablas_scope_L2", err=repr(e))
                st.error("No pude leer la vista por responsable.")
                with st.expander("Detalles técnicos"):
                    st.code(repr(e))
                    if DEBUG:
                        st.code(traceback.format_exc())
                st.stop()

            st.markdown("#### Clientes del responsable")
            df_cli_view = _rename_and_pick(
                df_cli,
                {
                    "cliente": "CLIENTE",
                    "locales": "LOCALES",
                    "skus_scope": "TOTAL SKUS",
                    "total_skus": "TOTAL SKUS",
                    "venta_0": "VENTA 0",
                    "negativos": "NEGATIVOS",
                    "quiebres": "QUIEBRES OBS.",
                    "otros": "OTROS",
                    "skus_en_foco": "SKUS EN FOCO",
                },
                [
                    "CLIENTE",
                    "LOCALES",
                    "TOTAL SKUS",
                    "VENTA 0",
                    "NEGATIVOS",
                    "OTROS",
                    "QUIEBRES OBS.",
                    "SKUS EN FOCO",
                ],
            )
            st.dataframe(df_cli_view, width="stretch", hide_index=True)

            st.markdown("#### Locales del responsable")
            _render_scope_pager("page_cliente_scope", _df_total_rows(df_local), page_size=25)
            df_local_view = _rename_and_pick(
                df_local,
                {
                    "cod_rt": "COD_RT",
                    "local_nombre_rr": "LOCAL",
                    "nombre_local_rr": "LOCAL",
                    "cliente": "CLIENTE",
                    "rutero": "RUTERO",
                    "reponedor": "REPONEDOR",
                    "skus_scope": "TOTAL SKUS",
                    "total_skus": "TOTAL SKUS",
                    "venta_0": "VENTA 0",
                    "negativos": "NEGATIVOS",
                    "quiebres": "QUIEBRES OBS.",
                    "otros": "OTROS",
                    "skus_en_foco": "SKUS EN FOCO",
                },
                [
                    "COD_RT",
                    "LOCAL",
                    "CLIENTE",
                    "RUTERO",
                    "REPONEDOR",
                    "TOTAL SKUS",
                    "VENTA 0",
                    "NEGATIVOS",
                    "OTROS",
                    "QUIEBRES OBS.",
                    "SKUS EN FOCO",
                ],
            )
            st.dataframe(df_local_view, width="stretch", hide_index=True)

            _render_cliente_exports()
            _render_rr_people_context()

        # -----------------------------
        # L3
        # -----------------------------
        else:
            try:
                current_page, offset = _get_page_state("page_cliente_scope", page_size=25)
                with _timed("PAGE tabla_scope_local_L3", tag="PAGE"):
                    df_local = db.get_tabla_scope_local_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=25,
                        offset=offset,
                    )

                current_page_det, offset_det = _get_page_state("page_cliente_detalle", page_size=25)
                with _timed("PAGE detalle_sku_scope_L3", tag="PAGE"):
                    df_det = db.get_detalle_sku_scope_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=25,
                        offset=offset_det,
                    )
            except Exception as e:
                _dbg("FAIL tablas_scope_L3", err=repr(e))
                st.error("No pude leer la vista detallada del scope.")
                with st.expander("Detalles técnicos"):
                    st.code(repr(e))
                    if DEBUG:
                        st.code(traceback.format_exc())
                st.stop()

            st.markdown("#### Locales accionables")
            _render_scope_pager("page_cliente_scope", _df_total_rows(df_local), page_size=25)
            df_local_view = _rename_and_pick(
                df_local,
                {
                    "cod_rt": "COD_RT",
                    "local_nombre_rr": "LOCAL",
                    "nombre_local_rr": "LOCAL",
                    "cliente": "CLIENTE",
                    "rutero": "RUTERO",
                    "reponedor": "REPONEDOR",
                    "skus_scope": "TOTAL SKUS",
                    "total_skus": "TOTAL SKUS",
                    "venta_0": "VENTA 0",
                    "negativos": "NEGATIVOS",
                    "quiebres": "QUIEBRES OBS.",
                    "otros": "OTROS",
                    "skus_en_foco": "SKUS EN FOCO",
                },
                [
                    "COD_RT",
                    "LOCAL",
                    "CLIENTE",
                    "RUTERO",
                    "REPONEDOR",
                    "TOTAL SKUS",
                    "VENTA 0",
                    "NEGATIVOS",
                    "OTROS",
                    "QUIEBRES OBS.",
                    "SKUS EN FOCO",
                ],
            )
            st.dataframe(df_local_view, width="stretch", hide_index=True)

            _render_cliente_exports()
            _render_rr_people_context()

            st.markdown("#### Detalle SKU")
            _render_scope_pager("page_cliente_detalle", _df_total_rows(df_det), page_size=25)
            df_det_view = _drop_total_rows(df_det).rename(columns={
                "fecha": "FECHA STOCK",
                "cod_rt": "COD_RT",
                "local_nombre_rr": "LOCAL",
                "nombre_local_rr": "LOCAL",
                "cliente": "CLIENTE",
                "responsable_tipo": "RESP. TIPO",
                "responsable": "RESPONSABLE",
                "marca": "MARCA",
                "sku": "SKU",
                "producto": "PRODUCTO",
                "descripcion": "PRODUCTO",
                "stock": "STOCK",
                "venta_7": "VENTA(+7)",
                "negativo": "NEGATIVO",
                "riesgo_quiebre": "RIESGO DE QUIEBRE",
                "otros": "OTROS",
            })
            st.dataframe(df_det_view, width="stretch", hide_index=True)
        st.stop()




    # ==============================================
    # MODO CONTROL GESTION
    # ==============================================
    if modo == "CONTROL GESTION":
        render_control_gestion(
            db=db,
            DEBUG=DEBUG,
            top2=top2,
            top3=top3,
            _dbg=_dbg,
            _timed=_timed,
            _df_total_rows=_df_total_rows,
            _rename_and_pick=_rename_and_pick,
        )
    # ==============================================
    # MODO LOCAL
    # ==============================================
    if modo == "LOCAL":
        try:
            with _timed("SELECTOR locales_home", tag="SELECTOR"):
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
    elif modo == "MERCADERISTA":
        try:
            with _timed("SELECTOR modalidades_home", tag="SELECTOR"):
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
                with _timed("SELECTOR rr_por_modalidad", tag="SELECTOR"):
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
            with _timed("SELECTOR locales_por_modalidad_rr", tag="SELECTOR"):
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
        
    else:
        st.error("Modo de consulta no soportado.")
        st.stop()

    # --------------------------------
    # Contexto del local
    # --------------------------------
    try:
        if modo == "LOCAL":
            with _timed("SELECTOR contexto_local_home", tag="SELECTOR"):
                ctx = db.get_contexto_local_home(cod_rt)
        else:
            with _timed("SELECTOR contexto_local_rr", tag="SELECTOR"):
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
            with _timed("PAGE marcas_local", tag="PAGE"):
                marcas_disponibles = db.get_marcas_local(cod_rt)
                _dbg("MARCAS loaded", n=len(marcas_disponibles))
                _dbg_block()
        else:
            with _timed("PAGE marcas_rr", tag="PAGE"):
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
    foco_ap = _normalize_focos_ui(st.session_state.get("applied_foco", []))
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
                with _timed("PAGE kpis_home", tag="PAGE"):
                    kpis = db.get_kpis_local_home(cod_rt, marcas)
            else:
                with _timed("PAGE kpis_rr", tag="PAGE"):
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

    _render_kpi_cards(kpis_row)

    total_skus_kpi = int((kpis_row or {}).get("total_skus") or 0)
    fecha_stock_raw = (kpis_row or {}).get("fecha_stock")
    fecha_stock_dt = pd.to_datetime(fecha_stock_raw, errors="coerce")
    file_stamp = fecha_stock_dt.strftime("%Y-%m-%d") if pd.notna(fecha_stock_dt) else "Sin stock"

    if total_skus_kpi == 0:
        st.caption("Estado: Sin stock para la combinación seleccionada.")
        
    # --------------------------------
    # Filtros secundarios
    # --------------------------------
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
        and len(foco_ap) <= 1
    )

    kpi_total_rows = None
    single_foco = foco_ap[0] if len(foco_ap) == 1 else None

    # Optimización: si no hay stock base, no hace falta consultar total
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
                if modo == "LOCAL":
                    with _timed("PAGE total_rows_home", tag="PAGE"):
                        total_rows = db.get_tabla_ux_total_home(
                            cod_rt=cod_rt,
                            marcas=marcas,
                            foco=foco_ap,
                            search=search_ap,
                        )
                else:
                    with _timed("PAGE total_rows_rr", tag="PAGE"):
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
            if modo == "LOCAL":
                with _timed("PAGE tabla_page_home", tag="PAGE"):
                    df_page = db.get_tabla_ux_page_home(
                        cod_rt=cod_rt,
                        marcas=marcas,
                        page=int(st.session_state["page"]),
                        page_size=page_size,
                        foco=foco_ap,
                        search=search_ap,
                    )
            else:
                with _timed("PAGE tabla_page_rr", tag="PAGE"):
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

    # --------------------------------
    # EXPORT
    # --------------------------------
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
        
if __name__ == "__main__":
    main()
