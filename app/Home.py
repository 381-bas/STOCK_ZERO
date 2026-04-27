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
    from app.screens.cliente import render_cliente
    from app.screens.control_gestion import render_control_gestion
    from app.screens.reposicion import render_reposicion

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
        st.session_state["f_search"] = ""
        st.session_state["f_foco"] = []
        st.session_state["applied_marcas"] = []
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
        st.session_state["sel_cliente_home_scope"] = CLIENTE_SCOPE_PLACEHOLDER
        st.session_state["sel_responsable_tipo"] = RESP_TIPO_PLACEHOLDER
        st.session_state["sel_responsable_lista"] = RESP_LISTA_PLACEHOLDER
        st.session_state["scope_level_cliente_mode"] = "L0"
        st.session_state["page_cliente_scope"] = 1
        st.session_state["page_cliente_scope_ui"] = 1
        st.session_state["page_cliente_detalle"] = 1
        st.session_state["page_cliente_detalle_ui"] = 1
        _dbg("RESET cliente_state (mode_change)")
        _dbg_block()
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

    TYPE_PLACEHOLDER = "-Seleccionar Tipo-"
    LOCAL_PLACEHOLDER = "— Selecciona local —"
    MODALIDAD_PLACEHOLDER = "— Selecciona modalidad —"
    RR_PLACEHOLDER = "— Selecciona rutero—reponedor —"

    CLIENTE_SCOPE_PLACEHOLDER = "Todos"
    RESP_TIPO_PLACEHOLDER = "— Selecciona tipo —"
    RESP_TIPO_ALL = "Todos"
    RESP_LISTA_PLACEHOLDER = "Todos"

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
        render_cliente(
            db=db,
            DEBUG=DEBUG,
            top2=top2,
            top3=top3,
            _dbg=_dbg,
            _dbg_block=_dbg_block,
            _timed=_timed,
            _render_kpi_cards=_render_kpi_cards,
            _invalidate_runtime_cache=_invalidate_runtime_cache,
            _reset_filters_defaults=_reset_filters_defaults,
            _df_total_rows=_df_total_rows,
            _drop_total_rows=_drop_total_rows,
            _rename_and_pick=_rename_and_pick,
            build_inventory_cliente_export_df=build_inventory_cliente_export_df,
            export_excel_generic=export_excel_generic,
        )

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
    if modo in {"LOCAL", "MERCADERISTA"}:
        render_reposicion(
            modo=modo,
            db=db,
            DEBUG=DEBUG,
            top2=top2,
            top3=top3,
            LOCAL_PLACEHOLDER=LOCAL_PLACEHOLDER,
            FOCO_OPTIONS=FOCO_OPTIONS,
            qp_cod_rt=qp_cod_rt,
            _dbg=_dbg,
            _dbg_block=_dbg_block,
            _timed=_timed,
            _normalize_focos_ui=_normalize_focos_ui,
            _foco_label=_foco_label,
            _render_kpi_cards=_render_kpi_cards,
            _invalidate_runtime_cache=_invalidate_runtime_cache,
            _reset_on_local_change=_reset_on_local_change,
            _reset_on_modalidad_change=_reset_on_modalidad_change,
            _reset_on_rr_change=_reset_on_rr_change,
            _df_total_rows=_df_total_rows,
            _drop_total_rows=_drop_total_rows,
            _rename_and_pick=_rename_and_pick,
        )
if __name__ == "__main__":
    main()
