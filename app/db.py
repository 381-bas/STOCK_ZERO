# app/db.py
from __future__ import annotations

from pathlib import Path
import os
import json
import time
import hashlib
import logging
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None  # noqa

logger = logging.getLogger("stock_zero")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

DV_TTL = int(os.getenv("DV_TTL", "60"))
QDF_TTL = int(os.getenv("QDF_TTL", "180"))
MAX_MARCA_FILTER = int(os.getenv("MAX_MARCA_FILTER", "50"))
ACTIVE_DB_URL_TTL = int(os.getenv("ACTIVE_DB_URL_TTL", "300"))

RUTA_TABLE = os.getenv("RUTA_TABLE", "public.ruta_rutero")

# RESULT_VIEW = puente transitorio activo para HOME_LOCAL/MERCADERISTA.
# No representa la verdad final del modelo; existe para desacoplar UX y capa fact/operativa.
RESULT_VIEW = os.getenv("RESULT_VIEW_STOCK_UX", "public.v_stock_local_cliente_ux")
RESULT_VIEW_ROLE = os.getenv("RESULT_VIEW_ROLE", "bridge_transition")

SELECTOR_TTL = int(os.getenv("SELECTOR_TTL", "600"))

LOCALES_HOME_VIEW = os.getenv("LOCALES_HOME_VIEW", "public.v_locales_home")
SELECTOR_MODALIDAD_VIEW = os.getenv("SELECTOR_MODALIDAD_VIEW", "public.v_selector_modalidad")
SELECTOR_MODALIDAD_RR_VIEW = os.getenv("SELECTOR_MODALIDAD_RR_VIEW", "public.v_selector_rutero_reponedor_modalidad")
LOCALES_MODALIDAD_RR_VIEW = os.getenv("LOCALES_MODALIDAD_RR_VIEW", "public.v_locales_por_modalidad_rutero")


SCOPE_RR_DISTINCT_VIEW = os.getenv(
    "SCOPE_RR_DISTINCT_VIEW",
    "public.v_scope_cliente_responsable_rr_distinct",
)
SCOPE_SUMMARY_VIEW = os.getenv(
    "SCOPE_SUMMARY_VIEW",
    "public.v_scope_cliente_responsable_summary",
)
SCOPE_FACT_BRIDGE_VIEW = os.getenv(
    "SCOPE_FACT_BRIDGE_VIEW",
    "public.v_scope_cliente_responsable_fact_bridge",
)
SCOPE_INVENTORY_BASE_VIEW = os.getenv(
    "SCOPE_INVENTORY_BASE_VIEW",
    "public.mv_scope_fact_latest_cliente",
)
SCOPE_CLIENTE_INVENTORY_ENRICHED_MV = os.getenv(
    "SCOPE_CLIENTE_INVENTORY_ENRICHED_MV",
    "public.mv_cliente_scope_inventory_enriched",
)
SCOPE_CLIENTE_RANKING_CLIENTE_MV = os.getenv(
    "SCOPE_CLIENTE_RANKING_CLIENTE_MV",
    "public.mv_cliente_scope_ranking_cliente",
)
SCOPE_CLIENTE_RANKING_RESPONSABLE_MV = os.getenv(
    "SCOPE_CLIENTE_RANKING_RESPONSABLE_MV",
    "public.mv_cliente_scope_ranking_responsable",
)


CG_PARITY_VIEW = os.getenv(
    "CG_PARITY_VIEW",
    "public.v_cg_cumplimiento_semana_local_parity",
)
CG_SCOPE_VIEW = os.getenv(
    "CG_SCOPE_VIEW",
    "public.v_cg_cumplimiento_semana_scope",
)
CG_ALERTAS_VIEW = os.getenv(
    "CG_ALERTAS_VIEW",
    "public.v_cg_alertas_control_gestion",
)
CG_INICIO_JEFE_VIEW = os.getenv(
    "CG_INICIO_JEFE_VIEW",
    "public.v_cg_inicio_jefe",
)
CG_INICIO_GESTOR_VIEW = os.getenv(
    "CG_INICIO_GESTOR_VIEW",
    "public.v_cg_inicio_gestor",
)
CG_DETALLE_VIEW = os.getenv(
    "CG_DETALLE_VIEW",
    "public.v_cg_cumplimiento_detalle",
)

CG_V2_SCOPE_VIEW = os.getenv(
    "CG_V2_SCOPE_VIEW",
    "public.v_cg_cumplimiento_semana_scope_v2",
)
CG_V2_DETALLE_VIEW = os.getenv(
    "CG_V2_DETALLE_VIEW",
    "public.v_cg_cumplimiento_detalle_v2",
)
CG_V2_OUT_WEEKLY_VIEW = os.getenv(
    "CG_V2_OUT_WEEKLY_VIEW",
    "cg_mart.v_cg_out_weekly_v2",
)
CG_V2_MULTI_MARCAJE_VIEW = os.getenv(
    "CG_V2_MULTI_MARCAJE_VIEW",
    "cg_mart.v_cg_marcaje_multifuente_dia_v2",
)
CG_V2_DAILY_EVIDENCE_VIEW = os.getenv(
    "CG_V2_DAILY_EVIDENCE_VIEW",
    "cg_core.v_cg_visita_dia_resuelta_v2",
)
CG_V2_ROUTE_FREQ_RESUELTA_VIEW = os.getenv(
    "CG_V2_ROUTE_FREQ_RESUELTA_VIEW",
    "cg_core.v_rr_frecuencia_base_resuelta_v2",
)
CG_V2_RUTA_DUP_VIEW = os.getenv(
    "CG_V2_RUTA_DUP_VIEW",
    "cg_mart.v_cg_ruta_duplicados_auditoria_v2",
)
CG_V2_FUERA_CRUCE_REAL_VIEW = os.getenv(
    "CG_V2_FUERA_CRUCE_REAL_VIEW",
    "cg_mart.v_cg_fuera_cruce_real_v2",
)
CG_V2_SIN_BATCH_RUTA_VIEW = os.getenv(
    "CG_V2_SIN_BATCH_RUTA_VIEW",
    "cg_mart.v_cg_sin_batch_ruta_semana_v2",
)

CG_COMPAT_ALERTAS_VIEW = os.getenv("CG_COMPAT_ALERTAS_VIEW", "public.v_alertas_control_gestion")
CG_COMPAT_INICIO_JEFE_VIEW = os.getenv("CG_COMPAT_INICIO_JEFE_VIEW", "public.v_inicio_jefe")
CG_COMPAT_INICIO_GESTOR_VIEW = os.getenv("CG_COMPAT_INICIO_GESTOR_VIEW", "public.v_inicio_gestor")
CG_COMPAT_DETALLE_VIEW = os.getenv("CG_COMPAT_DETALLE_VIEW", "public.v_cumplimiento_detalle")


def get_result_view_contract() -> dict[str, str]:
    return {
        "name": RESULT_VIEW,
        "role": RESULT_VIEW_ROLE,
        "status": "active_transition",
        "truth": "not_final_model_truth",
    }


@st.cache_data(ttl=ACTIVE_DB_URL_TTL, show_spinner=False)
def _get_active_db_url_cached() -> str:
    cache_sig = _sig("active_db_url")
    _set_mark("INFRA", "active_db_url", cache_sig)

    t0 = time.perf_counter()
    primary, fallback = _get_db_urls()
    selected = "none"

    if primary and _probe_pg(primary, target="primary"):
        selected = "primary"
        out = primary
    elif fallback and _probe_pg(fallback, target="fallback"):
        logger.warning("Usando DB_URL_FALLBACK (primary no responde).")
        selected = "fallback"
        out = fallback
    else:
        out = primary or (fallback or "")
        if primary:
            selected = "primary_unchecked"
        elif fallback:
            selected = "fallback_unchecked"

    _trace(
        "INFRA",
        "active_db_url_exec",
        active_db_url_ms=_fmt_ms(time.perf_counter() - t0),
        selected=selected,
        fallback_present=bool(fallback),
        ttl=ACTIVE_DB_URL_TTL,
    )
    return out


class AppError(RuntimeError):
    pass


TRACE_STATE_KEY = "_stock_zero_trace"


def _safe_session_state():
    try:
        return st.session_state
    except Exception:
        return None


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
    if any(Path(p).exists() for p in ("/mount/src", "/home/appuser", "/home/adminuser")):
        return "public"
    if (os.getenv("USER", "") or os.getenv("USERNAME", "")).strip().lower() in {"appuser", "adminuser"}:
        return "public"
    return "public" if os.name != "nt" else "local"


def _trace_ctx() -> dict[str, Any]:
    ss = _safe_session_state()
    return {
        "run_id": (ss.get("_run_id") if ss is not None else None) or "-",
        "env": (ss.get("_runtime_env") if ss is not None else None) or _infer_runtime_env(),
        "path": (ss.get("_run_path") if ss is not None else None) or "-",
        "mode": (ss.get("home_mode") if ss is not None else None) or "-",
    }


def _fmt_ms(seconds: float) -> float:
    return round(seconds * 1000.0, 3)


def _trace(tag: str, event: str, **kv) -> None:
    payload = {**_trace_ctx(), **kv}
    extra = " ".join(f"{k}={v}" for k, v in payload.items())
    try:
        logger.info("TRACE %s | %s | %s", tag, event, extra)
    except Exception:
        pass


def _sig(*parts: Any) -> str:
    try:
        raw = json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        raw = repr(parts)
    return hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()[:12]


def _trace_bucket() -> dict[str, Any]:
    ss = _safe_session_state()
    if ss is None:
        return {}
    bucket = ss.get(TRACE_STATE_KEY)
    if not isinstance(bucket, dict):
        bucket = {"marks": {}}
        ss[TRACE_STATE_KEY] = bucket
    if "marks" not in bucket or not isinstance(bucket["marks"], dict):
        bucket["marks"] = {}
    return bucket


def _get_mark(scope: str, name: str, sig: str) -> str | None:
    bucket = _trace_bucket()
    return bucket.get("marks", {}).get(f"{scope}:{name}:{sig}")


def _set_mark(scope: str, name: str, sig: str) -> str:
    bucket = _trace_bucket()
    stamp = str(time.time_ns())
    bucket.setdefault("marks", {})[f"{scope}:{name}:{sig}"] = stamp
    return stamp


# =========================================================
# 1) INFRA
# =========================================================
def _get_db_urls() -> tuple[str, str | None]:
    primary = (os.getenv("DB_URL_APP", "") or os.getenv("DB_URL", "")).strip()
    fallback = os.getenv("DB_URL_FALLBACK", "").strip() or None
    if not primary and not fallback:
        raise AppError(
            "Configuración incompleta: falta DB_URL_APP/DB_URL.\n"
            "Solución: agrega en tu .env una línea:\n"
            "DB_URL_APP=postgresql://USER:PASS@HOST:PORT/DB"
        )
    return primary, fallback


def _probe_pg(url: str, target: str = "primary") -> bool:
    t0 = time.perf_counter()
    if psycopg2 is None:
        _trace(
            "INFRA",
            "probe_pg",
            target=target,
            probe_pg_ms=_fmt_ms(time.perf_counter() - t0),
            ok=True,
            driver="psycopg2_missing",
        )
        return True

    timeout = int(os.getenv("CONNECT_TIMEOUT", "3"))
    try:
        with psycopg2.connect(url, connect_timeout=timeout) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
        ok = True
    except Exception as e:
        ok = False
        _trace(
            "INFRA",
            "probe_pg_err",
            target=target,
            probe_pg_ms=_fmt_ms(time.perf_counter() - t0),
            ok=ok,
            err=type(e).__name__,
        )
        return ok

    _trace("INFRA", "probe_pg", target=target, probe_pg_ms=_fmt_ms(time.perf_counter() - t0), ok=ok)
    return ok


def get_active_db_url() -> str:
    cache_sig = _sig("active_db_url")
    before = _get_mark("INFRA", "active_db_url", cache_sig)
    t0 = time.perf_counter()
    out = _get_active_db_url_cached()
    cache_state = "miss" if _get_mark("INFRA", "active_db_url", cache_sig) != before else "hit"
    _trace("INFRA", "get_active_db_url", active_db_url_ms=_fmt_ms(time.perf_counter() - t0), cache_state=cache_state)
    return out


@st.cache_resource(show_spinner=False)
def _engine_cached(db_url: str) -> Engine:
    cache_sig = _sig(db_url)
    _set_mark("INFRA", "engine", cache_sig)

    t0 = time.perf_counter()
    pool_size = int(os.getenv("POOL_SIZE", "15"))
    max_overflow = int(os.getenv("MAX_OVERFLOW", "30"))
    pool_timeout = int(os.getenv("POOL_TIMEOUT", "30"))
    pool_recycle = int(os.getenv("POOL_RECYCLE", "1800"))

    connect_timeout = int(os.getenv("CONNECT_TIMEOUT", "3"))
    stmt_timeout_ms = int(os.getenv("STATEMENT_TIMEOUT_MS", "15000"))

    connect_args: dict[str, Any] = {"connect_timeout": connect_timeout}
    if stmt_timeout_ms > 0:
        connect_args["options"] = f"-c statement_timeout={stmt_timeout_ms}"

    eng = create_engine(
        db_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
    )
    _trace(
        "INFRA",
        "engine_exec",
        engine_ms=_fmt_ms(time.perf_counter() - t0),
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        stmt_timeout_ms=stmt_timeout_ms,
    )
    return eng


def get_engine() -> Engine:
    db_url = get_active_db_url()
    cache_sig = _sig(db_url)
    before = _get_mark("INFRA", "engine", cache_sig)
    t0 = time.perf_counter()
    eng = _engine_cached(db_url)
    cache_state = "miss" if _get_mark("INFRA", "engine", cache_sig) != before else "hit"
    _trace("INFRA", "get_engine", engine_ms=_fmt_ms(time.perf_counter() - t0), cache_state=cache_state)
    return eng


@st.cache_data(ttl=DV_TTL, show_spinner=False)
def _get_data_version_info_cached() -> dict[str, Any]:
    cache_sig = _sig("data_version_info")
    _set_mark("DV", "data_version_info", cache_sig)

    eng = get_engine()
    total_t0 = time.perf_counter()
    with eng.connect() as conn:
        try:
            t_sql = time.perf_counter()
            df = pd.read_sql(text("SELECT fecha_datos, ingested_at FROM public.v_data_version;"), conn)
            read_sql_ms = _fmt_ms(time.perf_counter() - t_sql)
            _trace("DV", "data_version_info_candidate", source="public.v_data_version", read_sql_ms=read_sql_ms, rows=len(df))
            if not df.empty:
                out = {
                    "fecha_datos": df.iloc[0].get("fecha_datos"),
                    "ingested_at": df.iloc[0].get("ingested_at"),
                }
                _trace("DV", "get_data_version_info_exec", data_version_ms=_fmt_ms(time.perf_counter() - total_t0), source="public.v_data_version")
                return out
        except Exception as e:
            logger.warning("No pude leer v_data_version: %s", e)
            _trace("DV", "data_version_info_candidate_err", source="public.v_data_version", err=type(e).__name__)

        try:
            t_sql = time.perf_counter()
            df2 = pd.read_sql(text(f"SELECT MAX(fecha) AS fecha_datos FROM {RESULT_VIEW};"), conn)
            read_sql_ms = _fmt_ms(time.perf_counter() - t_sql)
            _trace("DV", "data_version_info_candidate", source=RESULT_VIEW, read_sql_ms=read_sql_ms, rows=len(df2))
            fd = df2.iloc[0].get("fecha_datos") if not df2.empty else None
            _trace("DV", "get_data_version_info_exec", data_version_ms=_fmt_ms(time.perf_counter() - total_t0), source=RESULT_VIEW)
            return {"fecha_datos": fd, "ingested_at": None}
        except Exception as e:
            logger.warning("No pude leer %s: %s", RESULT_VIEW, e)
            _trace("DV", "data_version_info_candidate_err", source=RESULT_VIEW, err=type(e).__name__)

    _trace("DV", "get_data_version_info_exec", data_version_ms=_fmt_ms(time.perf_counter() - total_t0), source="none")
    return {"fecha_datos": None, "ingested_at": None}


def get_data_version_info() -> dict[str, Any]:
    cache_sig = _sig("data_version_info")
    before = _get_mark("DV", "data_version_info", cache_sig)
    t0 = time.perf_counter()
    out = _get_data_version_info_cached()
    cache_state = "miss" if _get_mark("DV", "data_version_info", cache_sig) != before else "hit"
    _trace("DV", "get_data_version_info", data_version_ms=_fmt_ms(time.perf_counter() - t0), cache_state=cache_state)
    return out


@st.cache_data(ttl=300, show_spinner=False)
def _get_data_version_cached() -> str:
    cache_sig = _sig("data_version")
    _set_mark("DV", "data_version", cache_sig)

    eng = get_engine()
    candidates = [
        ("public.fact_stock_venta.ingested_at", "SELECT MAX(ingested_at) AS dv FROM public.fact_stock_venta;"),
        (RESULT_VIEW, f"SELECT MAX(fecha) AS dv FROM {RESULT_VIEW};"),
        ("public.v_data_version", "SELECT MAX(fecha_datos) AS dv FROM public.v_data_version;"),
    ]
    total_t0 = time.perf_counter()
    with eng.connect() as conn:
        for source, sql in candidates:
            try:
                t_sql = time.perf_counter()
                df = pd.read_sql(text(sql), conn)
                read_sql_ms = _fmt_ms(time.perf_counter() - t_sql)
                dv = df.iloc[0]["dv"]
                _trace("DV", "data_version_candidate", source=source, read_sql_ms=read_sql_ms, dv_found=dv is not None)
                if dv is not None:
                    out = str(dv)
                    _trace("DV", "get_data_version_exec", data_version_ms=_fmt_ms(time.perf_counter() - total_t0), source=source, dv=out)
                    return out
            except Exception as e:
                _trace("DV", "data_version_candidate_err", source=source, err=type(e).__name__)
                continue

    _trace("DV", "get_data_version_exec", data_version_ms=_fmt_ms(time.perf_counter() - total_t0), source="none", dv="NA")
    return "NA"


def _get_data_version_with_state() -> tuple[str, str]:
    cache_sig = _sig("data_version")
    before = _get_mark("DV", "data_version", cache_sig)
    t0 = time.perf_counter()
    out = _get_data_version_cached()
    cache_state = "miss" if _get_mark("DV", "data_version", cache_sig) != before else "hit"
    _trace("DV", "get_data_version", data_version_ms=_fmt_ms(time.perf_counter() - t0), cache_state=cache_state, dv=out)
    return out, cache_state


def get_data_version() -> str:
    out, _ = _get_data_version_with_state()
    return out


@st.cache_data(ttl=QDF_TTL, show_spinner=False)
def _qdf_cached(data_version: str, sql: str, params: dict[str, Any] | None) -> pd.DataFrame:
    cache_sig = _sig(data_version, sql, params or {})
    _set_mark("QUERY", "qdf", cache_sig)

    eng = get_engine()
    total_t0 = time.perf_counter()
    with eng.connect() as conn:
        t_sql = time.perf_counter()
        df = pd.read_sql(text(sql), conn, params=params)
        read_sql_ms = _fmt_ms(time.perf_counter() - t_sql)

    total_ms = _fmt_ms(time.perf_counter() - total_t0)
    _trace(
        "QUERY",
        "qdf_exec",
        dv_sig=_sig(data_version),
        sql_sig=_sig(sql),
        params_sig=_sig(params or {}),
        rows=len(df),
        read_sql_ms=read_sql_ms,
        qdf_total_ms=total_ms,
        page_read_sql_ms=read_sql_ms,
        page_total_ms=total_ms,
    )
    _trace(
        "PAGE",
        "page_exec",
        dv_sig=_sig(data_version),
        sql_sig=_sig(sql),
        params_sig=_sig(params or {}),
        rows=len(df),
        page_read_sql_ms=read_sql_ms,
        page_total_ms=total_ms,
    )
    return df


def qdf(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    total_t0 = time.perf_counter()
    dv, dv_state = _get_data_version_with_state()
    cache_sig = _sig(dv, sql, params or {})
    before = _get_mark("QUERY", "qdf", cache_sig)
    df = _qdf_cached(dv, sql, params)
    cache_state = "miss" if _get_mark("QUERY", "qdf", cache_sig) != before else "hit"
    total_ms = _fmt_ms(time.perf_counter() - total_t0)
    _trace(
        "QUERY",
        "qdf",
        dv_sig=_sig(dv),
        sql_sig=_sig(sql),
        params_sig=_sig(params or {}),
        rows=0 if df is None else len(df),
        qdf_total_ms=total_ms,
        page_total_ms=total_ms,
        cache_state=cache_state,
        dv_state=dv_state,
    )
    _trace(
        "PAGE",
        "page",
        dv_sig=_sig(dv),
        sql_sig=_sig(sql),
        params_sig=_sig(params or {}),
        rows=0 if df is None else len(df),
        page_total_ms=total_ms,
        cache_state=cache_state,
        dv_state=dv_state,
    )
    return df


@st.cache_data(ttl=SELECTOR_TTL, show_spinner=False)
def _selector_df_cached(name: str, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    cache_sig = _sig(name, sql, params or {})
    _set_mark("SELECTOR", name, cache_sig)

    eng = get_engine()
    total_t0 = time.perf_counter()
    with eng.connect() as conn:
        t_sql = time.perf_counter()
        df = pd.read_sql(text(sql), conn, params=params)
        read_sql_ms = _fmt_ms(time.perf_counter() - t_sql)

    total_ms = _fmt_ms(time.perf_counter() - total_t0)
    _trace(
        "SELECTOR",
        "selector_exec",
        selector=name,
        sql_sig=_sig(sql),
        params_sig=_sig(params or {}),
        rows=len(df),
        selector_read_sql_ms=read_sql_ms,
        selector_total_ms=total_ms,
        dv_state="not_used",
    )
    return df


def _selector_df(name: str, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    total_t0 = time.perf_counter()
    cache_sig = _sig(name, sql, params or {})
    before = _get_mark("SELECTOR", name, cache_sig)
    df = _selector_df_cached(name, sql, params)
    cache_state = "miss" if _get_mark("SELECTOR", name, cache_sig) != before else "hit"
    _trace(
        "SELECTOR",
        name,
        selector=name,
        sql_sig=_sig(sql),
        params_sig=_sig(params or {}),
        rows=0 if df is None else len(df),
        selector_total_ms=_fmt_ms(time.perf_counter() - total_t0),
        cache_state=cache_state,
        dv_state="not_used",
    )
    return df


# =========================================================
# 2) HELPERS DE SCOPE / FILTROS
# =========================================================
def _modalidad_clause(modalidad: str | None, field: str = "rr.modalidad") -> tuple[str, dict[str, Any]]:
    if modalidad and str(modalidad).strip():
        return (
            f" AND UPPER(TRIM(COALESCE({field}, ''))) = UPPER(TRIM(COALESCE(:modalidad, '')))",
            {"modalidad": str(modalidad).strip()},
        )
    return "", {}


def _build_result_filters(
    marcas: list[str] | None,
    search: str,
    foco: str | list[str] | tuple[str, ...] | None,
    alias: str = "",
) -> tuple[str, dict[str, Any]]:
    pfx = f"{alias}." if alias else ""
    params: dict[str, Any] = {}
    filters: list[str] = []

    if marcas:
        filters.append(f'AND {pfx}"MARCA" = ANY(:marcas)')
        params["marcas"] = marcas

    valid_focos = ["Venta 0", "Negativo", "Quiebres", "Otros"]

    if isinstance(foco, str):
        raw_focos = [x.strip() for x in foco.replace("|", ",").split(",") if x.strip()]
    elif foco is None:
        raw_focos = []
    else:
        raw_focos = [str(x).strip() for x in foco if str(x).strip()]

    focos = []
    for item in raw_focos:
        if item in valid_focos and item not in focos:
            focos.append(item)

    if focos:
        foco_clauses: list[str] = []

        if "Venta 0" in focos:
            foco_clauses.append(f'COALESCE({pfx}"Venta(+7)", 0) = 0')

        if "Negativo" in focos:
            foco_clauses.append(f"UPPER(TRIM(COALESCE({pfx}\"NEGATIVO\", ''))) = 'SI'")

        if "Quiebres" in focos:
            foco_clauses.append(f"UPPER(TRIM(COALESCE({pfx}\"RIESGO DE QUIEBRE\", ''))) = 'SI'")

        if "Otros" in focos:
            foco_clauses.append(
                f"""
                (
                    NULLIF(TRIM(COALESCE({pfx}"OTROS", '')), '') IS NOT NULL
                    AND UPPER(TRIM(COALESCE({pfx}"OTROS", ''))) NOT IN ('NO', 'N/A', 'NA', '-')
                )
                """
            )

        if foco_clauses:
            filters.append("AND (" + " OR ".join(f"({c})" for c in foco_clauses) + ")")

    s = (search or "").strip()
    if len(s) >= 2:
        filters.append(
            f"""
            AND (
                CAST({pfx}"Sku" AS TEXT) ILIKE :q
                OR COALESCE({pfx}"Descripción del Producto", '') ILIKE :q
                OR COALESCE({pfx}"MARCA", '') ILIKE :q
            )
            """
        )
        params["q"] = f"%{s}%"

    return "\n".join(filters), params


def _build_local_cliente_filter(
    cliente: str | None,
    alias: str = "v",
) -> tuple[str, dict[str, Any]]:
    cliente_sel = str(cliente or "").strip()
    if not cliente_sel or cliente_sel.upper() == "TODOS":
        return "", {}

    pfx = f"{alias}." if alias else ""
    return (
        f'AND UPPER(TRIM(COALESCE({pfx}"MARCA", \'\'))) = UPPER(TRIM(COALESCE(:cliente, \'\')))',
        {"cliente": cliente_sel},
    )


def _rr_scope_exists(alias: str = "v", modalidad: str | None = None) -> tuple[str, dict[str, Any]]:
    modalidad_sql, modalidad_params = _modalidad_clause(modalidad, "rr.modalidad")
    sql = f"""
        EXISTS (
            SELECT 1
            FROM {RUTA_TABLE} rr
            WHERE rr.cod_rt = {alias}.cod_rt
              AND UPPER(TRIM(COALESCE(rr.rutero, ''))) = UPPER(TRIM(COALESCE(:rutero, '')))
              AND UPPER(TRIM(COALESCE(rr.reponedor, ''))) = UPPER(TRIM(COALESCE(:reponedor, '')))
              {modalidad_sql}
              AND UPPER(TRIM(COALESCE(rr.cliente, ''))) = UPPER(TRIM(COALESCE({alias}."MARCA", '')))
        )
    """
    return sql, modalidad_params


# =========================================================
# 3) SELECTORES / CONTEXTO DESDE RUTA_RUTERO
# =========================================================
def get_rutero_reponedor() -> pd.DataFrame:
    return qdf(f"""
        SELECT DISTINCT
            TRIM(rutero) AS rutero,
            TRIM(reponedor) AS reponedor
        FROM {RUTA_TABLE}
        WHERE NULLIF(TRIM(COALESCE(rutero, '')), '') IS NOT NULL
          AND NULLIF(TRIM(COALESCE(reponedor, '')), '') IS NOT NULL
        ORDER BY rutero, reponedor
    """)


def get_locales(rutero: str, reponedor: str) -> pd.DataFrame:
    return qdf(f"""
        SELECT DISTINCT
            cod_rt,
            COALESCE(NULLIF(TRIM(local_nombre), ''), cod_rt) AS nombre_local_rr
        FROM {RUTA_TABLE}
        WHERE UPPER(TRIM(COALESCE(rutero, ''))) = UPPER(TRIM(COALESCE(:rutero, '')))
          AND UPPER(TRIM(COALESCE(reponedor, ''))) = UPPER(TRIM(COALESCE(:reponedor, '')))
          AND NULLIF(TRIM(COALESCE(cod_rt, '')), '') IS NOT NULL
        ORDER BY cod_rt, nombre_local_rr
    """, {"rutero": rutero, "reponedor": reponedor})


def get_contexto_local(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    modalidad: str | None = None,
) -> pd.DataFrame:
    modalidad_sql, extra = _modalidad_clause(modalidad, "modalidad")
    sql = f"""
        SELECT
            cod_rt,
            MAX(COALESCE(NULLIF(TRIM(local_nombre), ''), cod_rt)) AS nombre_local_rr,
            STRING_AGG(DISTINCT NULLIF(TRIM(cliente), ''), ' | ' ORDER BY NULLIF(TRIM(cliente), '')) AS clientes,
            STRING_AGG(DISTINCT NULLIF(TRIM(modalidad), ''), ' | ' ORDER BY NULLIF(TRIM(modalidad), '')) AS modalidades,
            STRING_AGG(DISTINCT NULLIF(TRIM(reponedor), ''), ' | ' ORDER BY NULLIF(TRIM(reponedor), '')) AS mercaderistas
        FROM {RUTA_TABLE}
        WHERE cod_rt = :cod_rt
          AND UPPER(TRIM(COALESCE(rutero, ''))) = UPPER(TRIM(COALESCE(:rutero, '')))
          AND UPPER(TRIM(COALESCE(reponedor, ''))) = UPPER(TRIM(COALESCE(:reponedor, '')))
          {modalidad_sql}
        GROUP BY cod_rt
    """
    params = {
        "rutero": rutero,
        "reponedor": reponedor,
        "cod_rt": cod_rt,
        **extra,
    }
    return _selector_df("get_contexto_local", sql, params)


def get_modalidades_home() -> list[str]:
    sql = f"""
        SELECT modalidad
        FROM {SELECTOR_MODALIDAD_VIEW}
        ORDER BY modalidad
    """
    df = _selector_df("get_modalidades_home", sql)
    return df["modalidad"].astype(str).tolist() if df is not None and not df.empty else []


def get_rutero_reponedor_por_modalidad(modalidad: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            rutero,
            reponedor
        FROM {SELECTOR_MODALIDAD_RR_VIEW}
        WHERE UPPER(TRIM(COALESCE(modalidad, ''))) = UPPER(TRIM(COALESCE(:modalidad, '')))
        ORDER BY rutero, reponedor
    """
    return _selector_df("get_rutero_reponedor_por_modalidad", sql, {"modalidad": modalidad})


def get_locales_por_modalidad_rr(modalidad: str, rutero: str, reponedor: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            cod_rt,
            COALESCE(NULLIF(TRIM(nombre_local_rr), ''), cod_rt) AS nombre_local
        FROM {LOCALES_MODALIDAD_RR_VIEW}
        WHERE UPPER(TRIM(COALESCE(modalidad, ''))) = UPPER(TRIM(COALESCE(:modalidad, '')))
          AND UPPER(TRIM(COALESCE(rutero, ''))) = UPPER(TRIM(COALESCE(:rutero, '')))
          AND UPPER(TRIM(COALESCE(reponedor, ''))) = UPPER(TRIM(COALESCE(:reponedor, '')))
        ORDER BY cod_rt, nombre_local
    """
    return _selector_df(
        "get_locales_por_modalidad_rr",
        sql,
        {
            "modalidad": modalidad,
            "rutero": rutero,
            "reponedor": reponedor,
        },
    )


def get_locales_home() -> pd.DataFrame:
    sql = f"""
        SELECT
            cod_rt,
            nombre_local
        FROM {LOCALES_HOME_VIEW}
        ORDER BY cod_rt, nombre_local
    """
    return _selector_df("get_locales_home", sql)


def get_clientes_local_home(cod_rt: str) -> list[str]:
    df = qdf(f"""
        SELECT DISTINCT
            TRIM(cliente) AS cliente
        FROM {RUTA_TABLE}
        WHERE cod_rt = :cod_rt
          AND NULLIF(TRIM(COALESCE(cliente, '')), '') IS NOT NULL
        ORDER BY cliente
    """, {"cod_rt": cod_rt})
    return df["cliente"].astype(str).tolist() if df is not None and not df.empty else []


def get_clientes_local_mercaderista(
    cod_rt: str,
    modalidad: str,
    rutero: str,
    reponedor: str,
) -> list[str]:
    modalidad_sql, extra = _modalidad_clause(modalidad, "modalidad")
    df = qdf(f"""
        SELECT DISTINCT
            TRIM(cliente) AS cliente
        FROM {RUTA_TABLE}
        WHERE cod_rt = :cod_rt
          AND UPPER(TRIM(COALESCE(rutero, ''))) = UPPER(TRIM(COALESCE(:rutero, '')))
          AND UPPER(TRIM(COALESCE(reponedor, ''))) = UPPER(TRIM(COALESCE(:reponedor, '')))
          {modalidad_sql}
          AND NULLIF(TRIM(COALESCE(cliente, '')), '') IS NOT NULL
        ORDER BY cliente
    """, {"cod_rt": cod_rt, "rutero": rutero, "reponedor": reponedor, **extra})
    return df["cliente"].astype(str).tolist() if df is not None and not df.empty else []


def get_mercaderistas_home() -> pd.DataFrame:
    return qdf(f"""
        SELECT DISTINCT
            TRIM(reponedor) AS mercaderista
        FROM {RUTA_TABLE}
        WHERE NULLIF(TRIM(COALESCE(reponedor, '')), '') IS NOT NULL
        ORDER BY mercaderista
    """)


def get_locales_por_mercaderista(mercaderista: str) -> pd.DataFrame:
    return qdf(f"""
        SELECT DISTINCT
            cod_rt,
            COALESCE(NULLIF(TRIM(local_nombre), ''), cod_rt) AS nombre_local
        FROM {RUTA_TABLE}
        WHERE UPPER(TRIM(COALESCE(reponedor, ''))) = UPPER(TRIM(COALESCE(:mercaderista, '')))
          AND NULLIF(TRIM(COALESCE(cod_rt, '')), '') IS NOT NULL
        ORDER BY cod_rt, nombre_local
    """, {"mercaderista": mercaderista})


def get_contexto_local_home(cod_rt: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            cod_rt,
            MAX(COALESCE(NULLIF(TRIM(local_nombre), ''), cod_rt)) AS local_nombre,
            STRING_AGG(DISTINCT NULLIF(TRIM(cliente), ''), ' | '
                       ORDER BY NULLIF(TRIM(cliente), '')) AS clientes,
            STRING_AGG(DISTINCT NULLIF(TRIM(reponedor), ''), ' | '
                       ORDER BY NULLIF(TRIM(reponedor), '')) AS mercaderistas,
            STRING_AGG(DISTINCT NULLIF(TRIM(modalidad), ''), ' | '
                       ORDER BY NULLIF(TRIM(modalidad), '')) AS modalidades
        FROM {RUTA_TABLE}
        WHERE cod_rt = :cod_rt
        GROUP BY cod_rt
    """
    return _selector_df("get_contexto_local_home", sql, {"cod_rt": cod_rt})


# =========================================================
# 4) RESULTADOS LOCAL / MERCADERISTA SOBRE VISTA PUENTE
# =========================================================
def get_marcas_local(cod_rt: str) -> list[str]:
    df = qdf(f"""
        SELECT DISTINCT v."MARCA" AS marca
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
        ORDER BY marca
    """, {"cod_rt": cod_rt})
    return df["marca"].astype(str).tolist() if df is not None and not df.empty else []


def get_marcas(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    modalidad: str | None = None,
) -> list[str]:
    exists_sql, extra = _rr_scope_exists("v", modalidad=modalidad)
    df = qdf(f"""
        SELECT DISTINCT v."MARCA" AS marca
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
          AND {exists_sql}
        ORDER BY marca
    """, {
        "rutero": rutero,
        "reponedor": reponedor,
        "cod_rt": cod_rt,
        **extra,
    })
    return df["marca"].astype(str).tolist() if df is not None and not df.empty else []


def get_kpis_local_home(
    cod_rt: str,
    marcas: list[str] | None = None,
    cliente: str | None = None,
) -> pd.DataFrame:
    where_extra, p2 = _build_result_filters(marcas, search="", foco="Todo", alias="v")
    cliente_where, cliente_params = _build_local_cliente_filter(cliente, alias="v")
    return qdf(f"""
        SELECT
            MAX(v.fecha) AS fecha_stock,
            COUNT(*)::int AS total_skus,
            COALESCE(SUM(CASE WHEN COALESCE(v."Venta(+7)", 0) = 0 THEN 1 ELSE 0 END), 0)::int AS venta_0,
            COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(v."NEGATIVO", ''))) = 'SI' THEN 1 ELSE 0 END), 0)::int AS negativos,
            COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(v."RIESGO DE QUIEBRE", ''))) = 'SI' THEN 1 ELSE 0 END), 0)::int AS quiebres,
            COALESCE(SUM(CASE
                WHEN NULLIF(TRIM(COALESCE(v."OTROS", '')), '') IS NOT NULL
                 AND UPPER(TRIM(COALESCE(v."OTROS", ''))) NOT IN ('NO', 'N/A', 'NA', '-')
                THEN 1 ELSE 0
            END), 0)::int AS otros
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
        {where_extra}
        {cliente_where}
    """, {"cod_rt": cod_rt, **p2, **cliente_params})


def get_kpis_local(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str] | None = None,
    modalidad: str | None = None,
    cliente: str | None = None,
) -> pd.DataFrame:
    exists_sql, extra = _rr_scope_exists("v", modalidad=modalidad)
    where_extra, p2 = _build_result_filters(marcas, search="", foco="Todo", alias="v")
    cliente_where, cliente_params = _build_local_cliente_filter(cliente, alias="v")
    return qdf(f"""
        SELECT
            MAX(v.fecha) AS fecha_stock,
            COUNT(*)::int AS total_skus,
            COALESCE(SUM(CASE WHEN COALESCE(v."Venta(+7)", 0) = 0 THEN 1 ELSE 0 END), 0)::int AS venta_0,
            COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(v."NEGATIVO", ''))) = 'SI' THEN 1 ELSE 0 END), 0)::int AS negativos,
            COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(v."RIESGO DE QUIEBRE", ''))) = 'SI' THEN 1 ELSE 0 END), 0)::int AS quiebres,
            COALESCE(SUM(CASE
                WHEN NULLIF(TRIM(COALESCE(v."OTROS", '')), '') IS NOT NULL
                 AND UPPER(TRIM(COALESCE(v."OTROS", ''))) NOT IN ('NO', 'N/A', 'NA', '-')
                THEN 1 ELSE 0
            END), 0)::int AS otros
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
          AND {exists_sql}
          {where_extra}
          {cliente_where}
    """, {
        "rutero": rutero,
        "reponedor": reponedor,
        "cod_rt": cod_rt,
        **extra,
        **p2,
        **cliente_params,
    })


def get_tabla_ux_total_home(
    cod_rt: str,
    marcas: list[str] | None = None,
    foco: str = "Todo",
    search: str = "",
    cliente: str | None = None,
) -> int:
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    cliente_where, cliente_params = _build_local_cliente_filter(cliente, alias="v")
    df = qdf(f"""
        SELECT COUNT(*)::int AS total
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
        {where_extra}
        {cliente_where}
    """, {"cod_rt": cod_rt, **p2, **cliente_params})
    return int(df.iloc[0]["total"]) if df is not None and not df.empty else 0


def get_tabla_ux_page_home(
    cod_rt: str,
    marcas: list[str] | None = None,
    page: int = 1,
    page_size: int = 25,
    foco: str = "Todo",
    search: str = "",
    cliente: str | None = None,
) -> pd.DataFrame:
    page = max(int(page or 1), 1)
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    cliente_where, cliente_params = _build_local_cliente_filter(cliente, alias="v")
    return qdf(f"""
        SELECT
          v.fecha,
          v."MARCA", v."Sku", v."Descripción del Producto",
          v."Stock", v."Venta(+7)", v."NEGATIVO", v."RIESGO DE QUIEBRE", v."OTROS"
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
        {where_extra}
        {cliente_where}
        ORDER BY
          v."MARCA" ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN (v."Sku")::bigint END ASC NULLS LAST,
          v."Sku" ASC,
          v."Descripción del Producto" ASC
        LIMIT :limit OFFSET :offset
    """, {
        "cod_rt": cod_rt,
        "limit": int(page_size),
        "offset": (page - 1) * int(page_size),
        **p2,
        **cliente_params,
    })


@st.cache_data(ttl=QDF_TTL, show_spinner=False)
def get_tabla_ux_export_home(
    cod_rt: str,
    marcas: list[str] | None = None,
    foco: str = "Todo",
    search: str = "",
    cliente: str | None = None,
) -> pd.DataFrame:
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    cliente_where, cliente_params = _build_local_cliente_filter(cliente, alias="v")
    return qdf(f"""
        SELECT
          v.fecha,
          v."MARCA", v."Sku", v."Descripción del Producto",
          v."Stock", v."Venta(+7)", v."NEGATIVO", v."RIESGO DE QUIEBRE", v."OTROS"
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
        {where_extra}
        {cliente_where}
        ORDER BY
          v."MARCA" ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN (v."Sku")::bigint END ASC NULLS LAST,
          v."Sku" ASC,
          v."Descripción del Producto" ASC
    """, {"cod_rt": cod_rt, **p2, **cliente_params})


def get_tabla_ux_total(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str] | None = None,
    foco: str = "Todo",
    search: str = "",
    modalidad: str | None = None,
    cliente: str | None = None,
) -> int:
    exists_sql, extra = _rr_scope_exists("v", modalidad=modalidad)
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    cliente_where, cliente_params = _build_local_cliente_filter(cliente, alias="v")
    df = qdf(f"""
        SELECT COUNT(*)::int AS total
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
          AND {exists_sql}
          {where_extra}
          {cliente_where}
    """, {
        "rutero": rutero,
        "reponedor": reponedor,
        "cod_rt": cod_rt,
        **extra,
        **p2,
        **cliente_params,
    })
    return int(df.iloc[0]["total"]) if df is not None and not df.empty else 0


def get_tabla_ux_page(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str] | None = None,
    page: int = 1,
    page_size: int = 25,
    foco: str = "Todo",
    search: str = "",
    modalidad: str | None = None,
    cliente: str | None = None,
) -> pd.DataFrame:
    page = max(int(page or 1), 1)
    exists_sql, extra = _rr_scope_exists("v", modalidad=modalidad)
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    cliente_where, cliente_params = _build_local_cliente_filter(cliente, alias="v")
    return qdf(f"""
        SELECT
          v.fecha,
          v."MARCA", v."Sku", v."Descripción del Producto",
          v."Stock", v."Venta(+7)", v."NEGATIVO", v."RIESGO DE QUIEBRE", v."OTROS"
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
          AND {exists_sql}
          {where_extra}
          {cliente_where}
        ORDER BY
          v."MARCA" ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN (v."Sku")::bigint END ASC NULLS LAST,
          v."Sku" ASC,
          v."Descripción del Producto" ASC
        LIMIT :limit OFFSET :offset
    """, {
        "rutero": rutero,
        "reponedor": reponedor,
        "cod_rt": cod_rt,
        "limit": int(page_size),
        "offset": (page - 1) * int(page_size),
        **extra,
        **p2,
        **cliente_params,
    })


def get_tabla_ux_paginada(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str] | None = None,
    page: int = 1,
    page_size: int = 25,
    foco: str = "Todo",
    search: str = "",
    modalidad: str | None = None,
) -> tuple[pd.DataFrame, int]:
    total = get_tabla_ux_total(
        rutero=rutero,
        reponedor=reponedor,
        cod_rt=cod_rt,
        marcas=marcas,
        foco=foco,
        search=search,
        modalidad=modalidad,
    )
    df = get_tabla_ux_page(
        rutero=rutero,
        reponedor=reponedor,
        cod_rt=cod_rt,
        marcas=marcas,
        page=page,
        page_size=page_size,
        foco=foco,
        search=search,
        modalidad=modalidad,
    )
    return df, int(total or 0)


@st.cache_data(ttl=QDF_TTL, show_spinner=False)
def get_tabla_ux_export(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str] | None = None,
    foco: str = "Todo",
    search: str = "",
    modalidad: str | None = None,
    cliente: str | None = None,
) -> pd.DataFrame:
    exists_sql, extra = _rr_scope_exists("v", modalidad=modalidad)
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    cliente_where, cliente_params = _build_local_cliente_filter(cliente, alias="v")
    return qdf(f"""
        SELECT
          v.fecha,
          v."MARCA", v."Sku", v."Descripción del Producto",
          v."Stock", v."Venta(+7)", v."NEGATIVO", v."RIESGO DE QUIEBRE", v."OTROS"
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
          AND {exists_sql}
          {where_extra}
          {cliente_where}
        ORDER BY
          v."MARCA" ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN (v."Sku")::bigint END ASC NULLS LAST,
          v."Sku" ASC,
          v."Descripción del Producto" ASC
    """, {
        "rutero": rutero,
        "reponedor": reponedor,
        "cod_rt": cod_rt,
        **extra,
        **p2,
        **cliente_params,
    })


def _scope_tipo_norm(value: str | None) -> str | None:
    v = str(value or "").strip().upper()
    return v if v in {"GESTOR", "SUPERVISOR"} else None


def _scope_match_text_expr(
    alias: str,
    preferred_field: str,
    fallback_field: str | None = None,
) -> str:
    pfx = f"{alias}." if alias else ""
    parts = [f"NULLIF(TRIM(COALESCE({pfx}{preferred_field}, '')), '')"]
    if fallback_field and fallback_field != preferred_field:
        parts.append(f"NULLIF(TRIM(COALESCE({pfx}{fallback_field}, '')), '')")
    parts.append("''")
    return "COALESCE(" + ", ".join(parts) + ")"


# =========================================================
# 5) CLIENTE_SCOPE SOBRE VISTAS DEDICADAS
# =========================================================
def _scope_norm_expr(field: str) -> str:
    return f"UPPER(TRIM(COALESCE({field}, '')))"


def _scope_is_selected(value: str | None) -> bool:
    if value is None:
        return False
    v = str(value).strip()
    return bool(v) and v.upper() != "TODOS"


def _normalize_scope_focos(
    focos: str | list[str] | tuple[str, ...] | None,
) -> list[str]:
    valid_focos = ["Venta 0", "Negativo", "Quiebres", "Otros"]

    if isinstance(focos, str):
        raw = [x.strip() for x in focos.replace("|", ",").split(",") if x.strip()]
    elif focos is None:
        raw = []
    else:
        raw = [str(x).strip() for x in focos if str(x).strip()]

    out: list[str] = []
    for item in raw:
        if item in valid_focos and item not in out:
            out.append(item)
    return out


def _scope_focos_is_all_or_empty(
    focos: str | list[str] | tuple[str, ...] | None,
) -> bool:
    focos_norm = _normalize_scope_focos(focos)
    return not focos_norm or len(focos_norm) == 4


def _scope_search_is_effectively_empty(search: str = "") -> bool:
    return len((search or "").strip()) < 2


def _scope_page_clause(
    limit: int | None,
    offset: int | None,
    params: dict[str, Any],
) -> str:
    if limit is None:
        return ""
    params["limit"] = max(int(limit), 1)
    params["offset"] = max(int(offset or 0), 0)
    return " LIMIT :limit OFFSET :offset "


def _scope_cliente_filters(
    *,
    alias: str = "",
    marca: str | None = None,
    cliente: str | None = None,
    responsable_tipo: str | None = None,
    responsable: str | None = None,
    cliente_field: str = "cliente_norm",
    responsable_field: str = "responsable_norm",
    responsable_tipo_field: str = "responsable_tipo",
    marca_field: str = "marca",
    apply_marca_intersection: bool = False,
) -> tuple[str, dict[str, Any]]:
    pfx = f"{alias}." if alias else ""
    params: dict[str, Any] = {}
    filters: list[str] = []

    if _scope_is_selected(cliente):
        filters.append(
            f"AND {_scope_norm_expr(f'{pfx}{cliente_field}')} = {_scope_norm_expr(':cliente')}"
        )
        params["cliente"] = str(cliente).strip()

    tipo_norm = _scope_tipo_norm(responsable_tipo)

    if _scope_is_selected(responsable) and tipo_norm is None:
        filters.append("AND 1=0")

    if tipo_norm is not None:
        filters.append(
            f"AND {_scope_norm_expr(f'{pfx}{responsable_tipo_field}')} = {_scope_norm_expr(':responsable_tipo')}"
        )
        params["responsable_tipo"] = tipo_norm

    if _scope_is_selected(responsable):
        filters.append(
            f"AND {_scope_norm_expr(f'{pfx}{responsable_field}')} = {_scope_norm_expr(':responsable')}"
        )
        params["responsable"] = str(responsable).strip()

    if _scope_is_selected(marca):
        params["marca"] = str(marca).strip()
        if apply_marca_intersection:
            cliente_match_expr = _scope_match_text_expr(
                alias,
                cliente_field,
                "cliente" if cliente_field != "cliente" else None,
            )
            filters.append(
                f"""
                AND EXISTS (
                    SELECT 1
                    FROM {SCOPE_FACT_BRIDGE_VIEW} b
                    WHERE b.cod_rt = {pfx}cod_rt
                      AND {_scope_norm_expr('b.cliente')} = {_scope_norm_expr(cliente_match_expr)}
                      AND {_scope_norm_expr('b.marca')} = {_scope_norm_expr(':marca')}
                )
                """
            )
        else:
            filters.append(
                f"AND {_scope_norm_expr(f'{pfx}{marca_field}')} = {_scope_norm_expr(':marca')}"
            )

    return "\n".join(filters), params


def _scope_summary_filters(
    *,
    alias: str = "",
    marca: str | None = None,
    cliente: str | None = None,
    responsable_tipo: str | None = None,
    responsable: str | None = None,
    focos: str | list[str] | tuple[str, ...] | None = None,
    search: str = "",
) -> tuple[str, dict[str, Any]]:
    filters_sql, params = _scope_cliente_filters(
        alias=alias,
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        cliente_field="cliente_norm",
        responsable_field="responsable_norm",
        responsable_tipo_field="responsable_tipo",
        marca_field="marca",
        apply_marca_intersection=True,
    )

    pfx = f"{alias}." if alias else ""
    filters: list[str] = [filters_sql] if filters_sql else []

    focos_norm = _normalize_scope_focos(focos)
    if focos_norm:
        foco_clauses: list[str] = []
        if "Venta 0" in focos_norm:
            foco_clauses.append(f"COALESCE({pfx}venta_0, 0) > 0")
        if "Negativo" in focos_norm:
            foco_clauses.append(f"COALESCE({pfx}negativos, 0) > 0")
        if "Quiebres" in focos_norm:
            foco_clauses.append(f"COALESCE({pfx}quiebres, 0) > 0")
        if "Otros" in focos_norm:
            foco_clauses.append(f"COALESCE({pfx}otros, 0) > 0")
        if foco_clauses:
            filters.append("AND (" + " OR ".join(f"({c})" for c in foco_clauses) + ")")

    s = (search or "").strip()
    if len(s) >= 2:
        filters.append(
            f"""
            AND (
                CAST({pfx}cod_rt AS TEXT) ILIKE :q
                OR COALESCE({pfx}local_nombre_rr, '') ILIKE :q
                OR COALESCE({pfx}cliente, '') ILIKE :q
                OR COALESCE({pfx}responsable, '') ILIKE :q
            )
            """
        )
        params["q"] = f"%{s}%"

    return "\n".join(x for x in filters if x), params


def _scope_detail_filters(
    *,
    alias: str = "",
    marca: str | None = None,
    cliente: str | None = None,
    responsable_tipo: str | None = None,
    responsable: str | None = None,
    focos: str | list[str] | tuple[str, ...] | None = None,
    search: str = "",
) -> tuple[str, dict[str, Any]]:
    filters_sql, params = _scope_cliente_filters(
        alias=alias,
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        cliente_field="cliente",
        responsable_field="responsable",
        responsable_tipo_field="responsable_tipo",
        marca_field="marca",
        apply_marca_intersection=False,
    )

    pfx = f"{alias}." if alias else ""
    filters: list[str] = [filters_sql] if filters_sql else []

    focos_norm = _normalize_scope_focos(focos)
    if focos_norm:
        foco_clauses: list[str] = []
        if "Venta 0" in focos_norm:
            foco_clauses.append(f"COALESCE({pfx}venta_7, 0) = 0")
        if "Negativo" in focos_norm:
            foco_clauses.append(f"{_scope_norm_expr(f'{pfx}negativo')} = 'SI'")
        if "Quiebres" in focos_norm:
            foco_clauses.append(f"{_scope_norm_expr(f'{pfx}riesgo_quiebre')} = 'SI'")
        if "Otros" in focos_norm:
            foco_clauses.append(
                f"""
                (
                    NULLIF(TRIM(COALESCE({pfx}otros, '')), '') IS NOT NULL
                    AND {_scope_norm_expr(f'{pfx}otros')} NOT IN ('NO', 'N/A', 'NA', '-')
                )
                """
            )
        if foco_clauses:
            filters.append("AND (" + " OR ".join(f"({c})" for c in foco_clauses) + ")")

    s = (search or "").strip()
    if len(s) >= 2:
        filters.append(
            f"""
            AND (
                CAST({pfx}cod_rt AS TEXT) ILIKE :q
                OR COALESCE({pfx}local_nombre_rr, '') ILIKE :q
                OR COALESCE({pfx}cliente, '') ILIKE :q
                OR COALESCE({pfx}responsable, '') ILIKE :q
                OR COALESCE({pfx}marca, '') ILIKE :q
                OR CAST({pfx}sku AS TEXT) ILIKE :q
                OR COALESCE({pfx}producto, '') ILIKE :q
            )
            """
        )
        params["q"] = f"%{s}%"

    return "\n".join(x for x in filters if x), params


def _scope_inventory_base_filters(
    *,
    alias: str = "",
    marca: str | None = None,
    cliente: str | None = None,
    responsable_tipo: str | None = None,
    responsable: str | None = None,
    focos: str | list[str] | tuple[str, ...] | None = None,
    search: str = "",
) -> tuple[str, dict[str, Any]]:
    pfx = f"{alias}." if alias else ""
    params: dict[str, Any] = {}
    filters: list[str] = []

    base_cliente_expr = _scope_match_text_expr(alias, "marca_norm", "marca")
    bridge_cliente_expr = (
        "COALESCE("
        "NULLIF(TRIM(COALESCE(br.cliente_norm, '')), ''), "
        "NULLIF(TRIM(COALESCE(br.cliente, '')), ''), "
        "''"
        ")"
    )

    if _scope_is_selected(cliente):
        filters.append(
            f"AND {_scope_norm_expr(base_cliente_expr)} = {_scope_norm_expr(':cliente')}"
        )
        params["cliente"] = str(cliente).strip()

    if _scope_is_selected(marca):
        filters.append(
            f"AND {_scope_norm_expr(base_cliente_expr)} = {_scope_norm_expr(':marca')}"
        )
        params["marca"] = str(marca).strip()

    tipo_norm = _scope_tipo_norm(responsable_tipo)
    responsable_selected = _scope_is_selected(responsable)
    if responsable_selected and tipo_norm is None:
        filters.append("AND 1=0")
    elif tipo_norm is not None or responsable_selected:
        scope_filters: list[str] = []
        responsable_tipo_expr = "COALESCE(br.responsable_tipo, '')"
        responsable_norm_expr = _scope_match_text_expr("br", "responsable_norm", "responsable")
        if tipo_norm is not None:
            scope_filters.append(
                f"AND {_scope_norm_expr(responsable_tipo_expr)} = {_scope_norm_expr(':responsable_tipo')}"
            )
            params["responsable_tipo"] = tipo_norm
        if responsable_selected:
            scope_filters.append(
                f"AND {_scope_norm_expr(responsable_norm_expr)} = {_scope_norm_expr(':responsable')}"
            )
            params["responsable"] = str(responsable).strip()

        filters.append(
            f"""
            AND EXISTS (
                SELECT 1
                FROM {SCOPE_FACT_BRIDGE_VIEW} br
                WHERE br.cod_rt = {pfx}cod_rt
                  AND CAST(br.sku AS TEXT) = CAST({pfx}sku AS TEXT)
                  AND {_scope_norm_expr(bridge_cliente_expr)} = {_scope_norm_expr(base_cliente_expr)}
                  {' '.join(scope_filters)}
            )
            """
        )

    focos_norm = _normalize_scope_focos(focos)
    if focos_norm:
        foco_clauses: list[str] = []
        negativo_expr = f"COALESCE({pfx}negativo::text, '')"
        quiebre_expr = f"COALESCE({pfx}riesgo_quiebre::text, '')"
        if "Venta 0" in focos_norm:
            foco_clauses.append(f"COALESCE({pfx}venta_7, 0) = 0")
        if "Negativo" in focos_norm:
            foco_clauses.append(f"{_scope_norm_expr(negativo_expr)} = 'SI'")
        if "Quiebres" in focos_norm:
            foco_clauses.append(f"{_scope_norm_expr(quiebre_expr)} = 'SI'")
        if "Otros" in focos_norm:
            foco_clauses.append(
                f"NULLIF(TRIM(COALESCE({pfx}otros::text, '')), '') IS NOT NULL"
            )
        if foco_clauses:
            filters.append("AND (" + " OR ".join(f"({c})" for c in foco_clauses) + ")")

    s = (search or "").strip()
    if len(s) >= 2:
        filters.append(
            f"""
            AND (
                CAST({pfx}cod_rt AS TEXT) ILIKE :q
                OR COALESCE({pfx}local_nombre_rr, '') ILIKE :q
                OR COALESCE({pfx}marca, '') ILIKE :q
                OR CAST({pfx}sku AS TEXT) ILIKE :q
                OR COALESCE({pfx}producto, '') ILIKE :q
            )
            """
        )
        params["q"] = f"%{s}%"

    return "\n".join(x for x in filters if x), params


def get_marcas_home_global() -> list[str]:
    sql = f"""
        SELECT DISTINCT
            TRIM(marca) AS marca
        FROM {SCOPE_FACT_BRIDGE_VIEW}
        WHERE NULLIF(TRIM(COALESCE(marca, '')), '') IS NOT NULL
        ORDER BY marca
    """
    df = _selector_df("get_marcas_home_global", sql)
    return df["marca"].astype(str).tolist() if df is not None and not df.empty else []


def get_clientes_home_scope(marca: str | None = None) -> list[str]:
    where_extra, params = _scope_inventory_base_filters(
        alias="base",
        marca=marca,
        cliente=None,
        responsable_tipo=None,
        responsable=None,
        focos=None,
        search="",
    )
    sql = f"""
        SELECT DISTINCT
            COALESCE(NULLIF(TRIM(base.marca), ''), TRIM(base.marca_norm)) AS cliente
        FROM {SCOPE_INVENTORY_BASE_VIEW} base
        WHERE NULLIF(TRIM(COALESCE(base.marca_norm, base.marca, '')), '') IS NOT NULL
        {where_extra}
        ORDER BY cliente
    """
    df = _selector_df("get_clientes_home_scope", sql, params)
    return df["cliente"].astype(str).tolist() if df is not None and not df.empty else []


def get_responsables_home_scope(
    tipo: str,
    marca: str | None = None,
    cliente: str | None = None,
) -> list[str]:
    tipo_norm = str(tipo or "").strip().upper()
    if tipo_norm not in {"GESTOR", "SUPERVISOR"}:
        return []

    where_extra, params = _scope_cliente_filters(
        alias="rr",
        marca=marca,
        cliente=cliente,
        responsable_tipo=tipo_norm,
        apply_marca_intersection=True,
        cliente_field="cliente_norm",
        responsable_field="responsable_norm",
        responsable_tipo_field="responsable_tipo",
    )
    sql = f"""
        SELECT DISTINCT
            COALESCE(NULLIF(TRIM(rr.responsable), ''), TRIM(rr.responsable_norm)) AS responsable
        FROM {SCOPE_RR_DISTINCT_VIEW} rr
        WHERE NULLIF(TRIM(COALESCE(rr.responsable_norm, rr.responsable, '')), '') IS NOT NULL
        {where_extra}
        ORDER BY responsable
    """
    df = _selector_df("get_responsables_home_scope", sql, params)
    return df["responsable"].astype(str).tolist() if df is not None and not df.empty else []


def get_rr_people_scope(
    tipo: str,
    responsable: str | None = None,
    marca: str | None = None,
    cliente: str | None = None,
) -> pd.DataFrame:
    tipo_norm = str(tipo or "").strip().upper()
    if tipo_norm not in {"GESTOR", "SUPERVISOR"}:
        return pd.DataFrame(
            columns=[
                "rutero",
                "reponedor",
                "responsable",
                "cliente",
                "locales_relacionados",
            ]
        )

    where_extra, params = _scope_cliente_filters(
        alias="rr",
        marca=marca,
        cliente=cliente,
        responsable_tipo=tipo_norm,
        responsable=responsable,
        apply_marca_intersection=True,
        cliente_field="cliente_norm",
        responsable_field="responsable_norm",
        responsable_tipo_field="responsable_tipo",
    )
    sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(rr.rutero), ''), '-') AS rutero,
            COALESCE(NULLIF(TRIM(rr.reponedor), ''), '-') AS reponedor,
            COALESCE(NULLIF(TRIM(rr.responsable), ''), TRIM(rr.responsable_norm)) AS responsable,
            COALESCE(NULLIF(TRIM(rr.cliente), ''), TRIM(rr.cliente_norm)) AS cliente,
            COUNT(DISTINCT rr.cod_rt)::int AS locales_relacionados
        FROM {SCOPE_RR_DISTINCT_VIEW} rr
        WHERE 1=1
        {where_extra}
        GROUP BY 1, 2, 3, 4
        ORDER BY rutero, reponedor, cliente
    """
    return _selector_df("get_rr_people_scope", sql, params)


def get_scope_level(
    cliente: str | None = None,
    responsable: str | None = None,
) -> str:
    has_cliente = _scope_is_selected(cliente)
    has_responsable = _scope_is_selected(responsable)

    if not has_cliente and not has_responsable:
        return "L0"
    if has_cliente and not has_responsable:
        return "L1"
    if not has_cliente and has_responsable:
        return "L2"
    return "L3"


def get_kpis_scope_cliente(
    marca: str | None = None,
    cliente: str | None = None,
    responsable_tipo: str | None = None,
    responsable: str | None = None,
    focos: list[str] | None = None,
    search: str = "",
) -> pd.DataFrame:
    where_base, params = _scope_inventory_base_filters(
        alias="base",
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        focos=focos,
        search=search,
    )

    sql = f"""
        WITH base_filtered AS (
            SELECT
                base.fecha,
                base.cod_rt,
                COALESCE(NULLIF(TRIM(base.local_nombre_rr), ''), CAST(base.cod_rt AS TEXT)) AS local_nombre_rr,
                COALESCE(NULLIF(TRIM(base.marca), ''), '') AS marca,
                COALESCE(NULLIF(TRIM(base.marca_norm), ''), NULLIF(TRIM(base.marca), ''), '') AS marca_norm,
                CAST(base.sku AS TEXT) AS sku,
                COALESCE(base.producto, '') AS producto,
                COALESCE(base.stock, 0)::int AS stock,
                COALESCE(base.venta_7, 0)::int AS venta_7,
                COALESCE(base.negativo::text, '') AS negativo,
                COALESCE(base.riesgo_quiebre::text, '') AS riesgo_quiebre,
                COALESCE(base.otros::text, '') AS otros
            FROM {SCOPE_INVENTORY_BASE_VIEW} base
            WHERE 1=1
            {where_base}
        ),
        responsables AS (
            SELECT
                COUNT(
                    DISTINCT UPPER(
                        TRIM(
                            COALESCE(
                                NULLIF(TRIM(COALESCE(br.responsable_norm, '')), ''),
                                NULLIF(TRIM(COALESCE(br.responsable, '')), ''),
                                ''
                            )
                        )
                    )
                )::int AS responsables_scope
            FROM {SCOPE_FACT_BRIDGE_VIEW} br
            WHERE NULLIF(TRIM(COALESCE(br.responsable_norm, br.responsable, '')), '') IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM base_filtered bf
                  WHERE bf.cod_rt = br.cod_rt
                    AND CAST(bf.sku AS TEXT) = CAST(br.sku AS TEXT)
                    AND {_scope_norm_expr("COALESCE(NULLIF(TRIM(COALESCE(br.cliente_norm, '')), ''), NULLIF(TRIM(COALESCE(br.cliente, '')), ''), '')")} =
                        {_scope_norm_expr("COALESCE(NULLIF(TRIM(COALESCE(bf.marca_norm, '')), ''), NULLIF(TRIM(COALESCE(bf.marca, '')), ''), '')")}
              )
        )
        SELECT
            MAX(base_filtered.fecha) AS fecha_stock,
            COUNT(DISTINCT base_filtered.cod_rt)::int AS locales_scope,
            COUNT(
                DISTINCT COALESCE(
                    NULLIF(TRIM(base_filtered.marca_norm), ''),
                    NULLIF(TRIM(base_filtered.marca), '')
                )
            )::int AS clientes_scope,
            COALESCE((SELECT responsables_scope FROM responsables), 0)::int AS responsables_scope,
            COUNT(*)::int AS total_skus,
            COALESCE(SUM(CASE WHEN COALESCE(base_filtered.venta_7, 0) = 0 THEN 1 ELSE 0 END), 0)::int AS venta_0,
            COALESCE(SUM(CASE WHEN {_scope_norm_expr("base_filtered.negativo")} = 'SI' THEN 1 ELSE 0 END), 0)::int AS negativos,
            COALESCE(SUM(CASE WHEN {_scope_norm_expr("base_filtered.riesgo_quiebre")} = 'SI' THEN 1 ELSE 0 END), 0)::int AS quiebres,
            COALESCE(SUM(CASE WHEN NULLIF(TRIM(COALESCE(base_filtered.otros, '')), '') IS NOT NULL THEN 1 ELSE 0 END), 0)::int AS otros
        FROM base_filtered
    """
    return qdf(sql, params)


def get_tabla_scope_responsable_total_page(
    marca: str | None = None,
    cliente: str | None = None,
    responsable_tipo: str | None = None,
    responsable: str | None = None,
    focos: list[str] | None = None,
    search: str = "",
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    tipo_norm = _scope_tipo_norm(responsable_tipo)
    responsable_selected = _scope_is_selected(responsable)
    use_mv_scope = (
        not _scope_is_selected(cliente)
        and not _scope_is_selected(marca)
        and _scope_search_is_effectively_empty(search)
        and _scope_focos_is_all_or_empty(focos)
        and (tipo_norm is not None or not responsable_selected)
    )
    if use_mv_scope:
        params: dict[str, Any] = {}
        where_clauses: list[str] = []
        responsable_match_expr = (
            "COALESCE("
            "NULLIF(TRIM(r.responsable_norm), ''), "
            "NULLIF(TRIM(r.responsable), ''), "
            "''"
            ")"
        )
        if tipo_norm is not None:
            where_clauses.append(
                f"AND {_scope_norm_expr('r.responsable_tipo')} = {_scope_norm_expr(':responsable_tipo')}"
            )
            params["responsable_tipo"] = tipo_norm
        if responsable_selected:
            where_clauses.append(
                f"AND {_scope_norm_expr(responsable_match_expr)} = {_scope_norm_expr(':responsable')}"
            )
            params["responsable"] = str(responsable).strip()
        page_sql = _scope_page_clause(limit, offset, params)
        where_sql = "\n".join(where_clauses)
        sql = f"""
            SELECT
                r.responsable_tipo,
                r.responsable,
                r.clientes,
                r.locales,
                r.total_skus,
                r.venta_0,
                r.negativos,
                r.quiebres,
                r.otros,
                r.skus_en_foco,
                COUNT(*) OVER()::int AS total_rows
            FROM {SCOPE_CLIENTE_RANKING_RESPONSABLE_MV} r
            WHERE 1=1
            {where_sql}
            ORDER BY
                r.skus_en_foco DESC,
                r.negativos DESC,
                r.venta_0 DESC,
                r.otros DESC,
                r.quiebres DESC,
                r.responsable ASC
            {page_sql}
        """
        return qdf(sql, params)

    where_extra, params = _scope_detail_filters(
        alias="b",
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        focos=focos,
        search=search,
    )
    page_sql = _scope_page_clause(limit, offset, params)

    sql = f"""
        WITH filtered AS (
            SELECT
                COALESCE(NULLIF(TRIM(b.responsable_tipo), ''), '-') AS responsable_tipo,
                COALESCE(NULLIF(TRIM(b.responsable), ''), TRIM(b.responsable_norm)) AS responsable,
                UPPER(
                    TRIM(
                        COALESCE(
                            NULLIF(TRIM(COALESCE(b.cliente_norm, '')), ''),
                            NULLIF(TRIM(COALESCE(b.cliente, '')), ''),
                            ''
                        )
                    )
                ) AS cliente_key,
                COALESCE(NULLIF(TRIM(b.cliente), ''), TRIM(b.cliente_norm)) AS cliente_label,
                b.cod_rt,
                (
                    CAST(b.cod_rt AS TEXT)
                    || '|'
                    || UPPER(
                        TRIM(
                            COALESCE(
                                NULLIF(TRIM(COALESCE(b.cliente_norm, '')), ''),
                                NULLIF(TRIM(COALESCE(b.cliente, '')), ''),
                                ''
                            )
                        )
                    )
                    || '|'
                    || CAST(b.sku AS TEXT)
                ) AS inv_key,
                CASE WHEN COALESCE(b.venta_7, 0) = 0 THEN 1 ELSE 0 END AS venta_0_flag,
                CASE WHEN {_scope_norm_expr("COALESCE(b.negativo::text, '')")} = 'SI' THEN 1 ELSE 0 END AS negativo_flag,
                CASE WHEN {_scope_norm_expr("COALESCE(b.riesgo_quiebre::text, '')")} = 'SI' THEN 1 ELSE 0 END AS quiebre_flag,
                (
                    CASE
                        WHEN NULLIF(TRIM(COALESCE(b.otros::text, '')), '') IS NOT NULL THEN 1
                        ELSE 0
                    END
                ) AS otros_flag
            FROM {SCOPE_FACT_BRIDGE_VIEW} b
            WHERE 1=1
            {where_extra}
        ),
        inv_distinct AS (
            SELECT
                responsable_tipo,
                responsable,
                cliente_key,
                cliente_label,
                cod_rt,
                inv_key,
                MAX(venta_0_flag)::int AS venta_0_flag,
                MAX(negativo_flag)::int AS negativo_flag,
                MAX(quiebre_flag)::int AS quiebre_flag,
                MAX(otros_flag)::int AS otros_flag
            FROM filtered
            GROUP BY 1, 2, 3, 4, 5, 6
        ),
        base AS (
            SELECT
                responsable_tipo,
                responsable,
                COUNT(DISTINCT cliente_key)::int AS clientes,
                COUNT(DISTINCT cod_rt)::int AS locales,
                COUNT(*)::int AS total_skus,
                COALESCE(SUM(venta_0_flag), 0)::int AS venta_0,
                COALESCE(SUM(negativo_flag), 0)::int AS negativos,
                COALESCE(SUM(quiebre_flag), 0)::int AS quiebres,
                COALESCE(SUM(otros_flag), 0)::int AS otros,
                COALESCE(
                    SUM(
                        CASE
                            WHEN venta_0_flag = 1
                              OR negativo_flag = 1
                              OR quiebre_flag = 1
                              OR otros_flag = 1
                            THEN 1 ELSE 0
                        END
                    ),
                    0
                )::int AS skus_en_foco
            FROM inv_distinct
            GROUP BY 1, 2
        )
        SELECT
            base.*,
            COUNT(*) OVER()::int AS total_rows
        FROM base
        ORDER BY skus_en_foco DESC, negativos DESC, venta_0 DESC, otros DESC, quiebres DESC, responsable ASC
        {page_sql}
    """
    return qdf(sql, params)


def get_tabla_scope_cliente_total_page(
    marca: str | None = None,
    cliente: str | None = None,
    responsable_tipo: str | None = None,
    responsable: str | None = None,
    focos: list[str] | None = None,
    search: str = "",
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    tipo_norm = _scope_tipo_norm(responsable_tipo)
    responsable_selected = _scope_is_selected(responsable)
    use_mv_scope = (
        tipo_norm is None
        and not responsable_selected
        and _scope_search_is_effectively_empty(search)
        and _scope_focos_is_all_or_empty(focos)
    )
    if use_mv_scope:
        params: dict[str, Any] = {}
        where_clauses: list[str] = []
        cliente_match_expr = "COALESCE(NULLIF(TRIM(base.cliente_norm), ''), NULLIF(TRIM(base.cliente), ''), '')"
        if _scope_is_selected(cliente):
            where_clauses.append(
                f"AND {_scope_norm_expr(cliente_match_expr)} = {_scope_norm_expr(':cliente')}"
            )
            params["cliente"] = str(cliente).strip()
        if _scope_is_selected(marca):
            where_clauses.append(
                f"AND {_scope_norm_expr(cliente_match_expr)} = {_scope_norm_expr(':marca')}"
            )
            params["marca"] = str(marca).strip()
        page_sql = _scope_page_clause(limit, offset, params)
        where_sql = "\n".join(where_clauses)
        sql = f"""
            WITH base AS (
                SELECT
                    rc.cliente,
                    rc.cliente_norm,
                    rc.locales,
                    rc.total_skus,
                    rc.venta_0,
                    rc.negativos,
                    rc.quiebres,
                    rc.otros,
                    rc.skus_en_foco,
                    rc.fecha_min,
                    rc.fecha_max
                FROM {SCOPE_CLIENTE_RANKING_CLIENTE_MV} rc
                WHERE 1=1
                {where_sql}
            ),
            cliente_keys AS (
                SELECT DISTINCT
                    e.cod_rt,
                    e.cliente_norm
                FROM {SCOPE_CLIENTE_INVENTORY_ENRICHED_MV} e
                JOIN base
                  ON base.cliente_norm = e.cliente_norm
            ),
            responsables AS (
                SELECT
                    ck.cliente_norm,
                    COUNT(DISTINCT resp.responsable_norm)::int AS responsables
                FROM cliente_keys ck
                JOIN LATERAL (
                    SELECT UPPER(TRIM(COALESCE(r.gestores, ''))) AS responsable_norm
                    FROM {RUTA_TABLE} r
                    WHERE r.cod_rt = ck.cod_rt
                      AND UPPER(TRIM(COALESCE(r.cliente, ''))) = ck.cliente_norm
                      AND NULLIF(TRIM(COALESCE(r.gestores, '')), '') IS NOT NULL
                    UNION
                    SELECT UPPER(TRIM(COALESCE(r.supervisor, ''))) AS responsable_norm
                    FROM {RUTA_TABLE} r
                    WHERE r.cod_rt = ck.cod_rt
                      AND UPPER(TRIM(COALESCE(r.cliente, ''))) = ck.cliente_norm
                      AND NULLIF(TRIM(COALESCE(r.supervisor, '')), '') IS NOT NULL
                ) resp ON TRUE
                GROUP BY ck.cliente_norm
            )
            SELECT
                base.cliente,
                COALESCE(responsables.responsables, 0)::int AS responsables,
                base.locales,
                base.total_skus,
                base.venta_0,
                base.negativos,
                base.quiebres,
                base.otros,
                base.skus_en_foco,
                base.fecha_min,
                base.fecha_max,
                COUNT(*) OVER()::int AS total_rows
            FROM base
            LEFT JOIN responsables
              ON responsables.cliente_norm = base.cliente_norm
            ORDER BY
                base.skus_en_foco DESC,
                base.quiebres DESC,
                base.venta_0 DESC,
                base.total_skus DESC,
                base.cliente ASC
            {page_sql}
        """
        return qdf(sql, params)

    where_extra, params = _scope_inventory_base_filters(
        alias="base",
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        focos=focos,
        search=search,
    )
    page_sql = _scope_page_clause(limit, offset, params)

    sql = f"""
        WITH base_filtered AS (
            SELECT
                base.fecha,
                base.cod_rt,
                COALESCE(NULLIF(TRIM(base.local_nombre_rr), ''), CAST(base.cod_rt AS TEXT)) AS local_nombre_rr,
                COALESCE(NULLIF(TRIM(base.marca), ''), '') AS cliente,
                COALESCE(NULLIF(TRIM(base.marca_norm), ''), NULLIF(TRIM(base.marca), ''), '') AS cliente_norm,
                CAST(base.sku AS TEXT) AS sku,
                COALESCE(base.producto, '') AS producto,
                COALESCE(base.venta_7, 0)::int AS venta_7,
                COALESCE(base.negativo::text, '') AS negativo,
                COALESCE(base.riesgo_quiebre::text, '') AS riesgo_quiebre,
                COALESCE(base.otros::text, '') AS otros
            FROM {SCOPE_INVENTORY_BASE_VIEW} base
            WHERE 1=1
            {where_extra}
        ),
        base_agg AS (
            SELECT
                COALESCE(NULLIF(TRIM(cliente), ''), TRIM(cliente_norm)) AS cliente,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(cliente_norm), ''), NULLIF(TRIM(cliente), ''), ''))) AS cliente_norm_key,
                COUNT(DISTINCT cod_rt)::int AS locales,
                COUNT(*)::int AS total_skus,
                COALESCE(SUM(CASE WHEN COALESCE(venta_7, 0) = 0 THEN 1 ELSE 0 END), 0)::int AS venta_0,
                COALESCE(SUM(CASE WHEN {_scope_norm_expr('negativo')} = 'SI' THEN 1 ELSE 0 END), 0)::int AS negativos,
                COALESCE(SUM(CASE WHEN {_scope_norm_expr('riesgo_quiebre')} = 'SI' THEN 1 ELSE 0 END), 0)::int AS quiebres,
                COALESCE(SUM(CASE WHEN NULLIF(TRIM(COALESCE(otros, '')), '') IS NOT NULL THEN 1 ELSE 0 END), 0)::int AS otros,
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(venta_7, 0) = 0
                              OR {_scope_norm_expr('negativo')} = 'SI'
                              OR {_scope_norm_expr('riesgo_quiebre')} = 'SI'
                              OR NULLIF(TRIM(COALESCE(otros, '')), '') IS NOT NULL
                            THEN 1 ELSE 0
                        END
                    ),
                    0
                )::int AS skus_en_foco,
                MIN(fecha) AS fecha_min,
                MAX(fecha) AS fecha_max
            FROM base_filtered
            GROUP BY 1, 2
        ),
        responsables AS (
            SELECT
                bf.cliente_norm_key,
                COUNT(
                    DISTINCT UPPER(
                        TRIM(
                            COALESCE(
                                NULLIF(TRIM(COALESCE(br.responsable_norm, '')), ''),
                                NULLIF(TRIM(COALESCE(br.responsable, '')), ''),
                                ''
                            )
                        )
                    )
                )::int AS responsables
            FROM (
                SELECT DISTINCT
                    cod_rt,
                    sku,
                    UPPER(TRIM(COALESCE(NULLIF(TRIM(cliente_norm), ''), NULLIF(TRIM(cliente), ''), ''))) AS cliente_norm_key
                FROM base_filtered
            ) bf
            JOIN {SCOPE_FACT_BRIDGE_VIEW} br
              ON br.cod_rt = bf.cod_rt
             AND CAST(br.sku AS TEXT) = CAST(bf.sku AS TEXT)
             AND {_scope_norm_expr("COALESCE(NULLIF(TRIM(COALESCE(br.cliente_norm, '')), ''), NULLIF(TRIM(COALESCE(br.cliente, '')), ''), '')")} = bf.cliente_norm_key
            WHERE NULLIF(TRIM(COALESCE(br.responsable_norm, br.responsable, '')), '') IS NOT NULL
            GROUP BY 1
        )
        SELECT
            base_agg.cliente,
            COALESCE(responsables.responsables, 0)::int AS responsables,
            base_agg.locales,
            base_agg.total_skus,
            base_agg.venta_0,
            base_agg.negativos,
            base_agg.quiebres,
            base_agg.otros,
            base_agg.skus_en_foco,
            base_agg.fecha_min,
            base_agg.fecha_max,
            COUNT(*) OVER()::int AS total_rows
        FROM base_agg
        LEFT JOIN responsables
          ON responsables.cliente_norm_key = base_agg.cliente_norm_key
        ORDER BY
            base_agg.skus_en_foco DESC,
            base_agg.quiebres DESC,
            base_agg.venta_0 DESC,
            base_agg.total_skus DESC,
            base_agg.cliente ASC
        {page_sql}
    """
    return qdf(sql, params)


def get_tabla_scope_local_total_page(
    marca: str | None = None,
    cliente: str | None = None,
    responsable_tipo: str | None = None,
    responsable: str | None = None,
    focos: list[str] | None = None,
    search: str = "",
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    where_extra, params = _scope_summary_filters(
        alias="s",
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        focos=focos,
        search=search,
    )
    rr_where_extra, rr_params = _scope_cliente_filters(
        alias="rr",
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        cliente_field="cliente_norm",
        responsable_field="responsable_norm",
        responsable_tipo_field="responsable_tipo",
        apply_marca_intersection=True,
    )
    params = {**params, **rr_params}
    page_sql = _scope_page_clause(limit, offset, params)

    sql = f"""
        WITH rr_ctx AS (
            SELECT
                rr.cod_rt,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.cliente_norm), ''), NULLIF(TRIM(rr.cliente), ''), ''))) AS cliente_norm_match,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.responsable_tipo), ''), '-'))) AS responsable_tipo_match,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.responsable_norm), ''), NULLIF(TRIM(rr.responsable), ''), ''))) AS responsable_norm_match,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.rutero), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.rutero), ''), '-')
                ) AS rutero,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.reponedor), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.reponedor), ''), '-')
                ) AS reponedor
            FROM {SCOPE_RR_DISTINCT_VIEW} rr
            WHERE 1=1
            {rr_where_extra}
            GROUP BY 1, 2, 3, 4
        ),
        base AS (
            SELECT
                s.cod_rt,
                COALESCE(NULLIF(TRIM(s.local_nombre_rr), ''), CAST(s.cod_rt AS TEXT)) AS nombre_local_rr,
                COALESCE(NULLIF(TRIM(s.cliente), ''), TRIM(s.cliente_norm)) AS cliente,
                COALESCE(NULLIF(TRIM(s.responsable_tipo), ''), '-') AS responsable_tipo,
                COALESCE(NULLIF(TRIM(s.responsable), ''), TRIM(s.responsable_norm)) AS responsable,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(s.cliente_norm), ''), NULLIF(TRIM(s.cliente), ''), ''))) AS cliente_norm_match,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(s.responsable_tipo), ''), '-'))) AS responsable_tipo_match,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(s.responsable_norm), ''), NULLIF(TRIM(s.responsable), ''), ''))) AS responsable_norm_match,
                COALESCE(SUM(s.skus_scope), 0)::int AS total_skus,
                COALESCE(SUM(s.venta_0), 0)::int AS venta_0,
                COALESCE(SUM(s.negativos), 0)::int AS negativos,
                COALESCE(SUM(s.quiebres), 0)::int AS quiebres,
                COALESCE(SUM(s.otros), 0)::int AS otros,
                (
                    COALESCE(SUM(s.venta_0), 0)
                    + COALESCE(SUM(s.negativos), 0)
                    + COALESCE(SUM(s.quiebres), 0)
                    + COALESCE(SUM(s.otros), 0)
                )::int AS skus_en_foco
            FROM {SCOPE_SUMMARY_VIEW} s
            WHERE 1=1
            {where_extra}
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        )
        SELECT
            base.cod_rt,
            base.nombre_local_rr,
            base.cliente,
            base.responsable_tipo,
            base.responsable,
            COALESCE(rr_ctx.rutero, '-') AS rutero,
            COALESCE(rr_ctx.reponedor, '-') AS reponedor,
            base.total_skus,
            base.venta_0,
            base.negativos,
            base.quiebres,
            base.otros,
            base.skus_en_foco,
            COUNT(*) OVER()::int AS total_rows
        FROM base
        LEFT JOIN rr_ctx
            ON rr_ctx.cod_rt = base.cod_rt
           AND rr_ctx.cliente_norm_match = base.cliente_norm_match
           AND rr_ctx.responsable_tipo_match = base.responsable_tipo_match
           AND rr_ctx.responsable_norm_match = base.responsable_norm_match
        ORDER BY
            base.skus_en_foco DESC,
            base.negativos DESC,
            base.venta_0 DESC,
            base.otros DESC,
            base.quiebres DESC,
            base.nombre_local_rr ASC
        {page_sql}
    """
    return qdf(sql, params)


def get_detalle_sku_scope_total_page(
    marca: str | None = None,
    cliente: str | None = None,
    responsable_tipo: str | None = None,
    responsable: str | None = None,
    focos: list[str] | None = None,
    search: str = "",
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    level = get_scope_level(cliente=cliente, responsable=responsable)
    if level in {"L0", "L1"}:
        return pd.DataFrame(
            columns=[
                "fecha",
                "cod_rt",
                "nombre_local_rr",
                "cliente",
                "responsable_tipo",
                "responsable",
                "marca",
                "sku",
                "descripcion",
                "stock",
                "venta_7",
                "negativo",
                "riesgo_quiebre",
                "otros",
                "total_rows",
            ]
        )

    where_extra, params = _scope_detail_filters(
        alias="b",
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        focos=focos,
        search=search,
    )
    page_sql = _scope_page_clause(limit, offset, params)

    sql = f"""
        SELECT
            b.fecha,
            b.cod_rt,
            COALESCE(NULLIF(TRIM(b.local_nombre_rr), ''), CAST(b.cod_rt AS TEXT)) AS nombre_local_rr,
            COALESCE(NULLIF(TRIM(b.cliente), ''), '') AS cliente,
            COALESCE(NULLIF(TRIM(b.responsable_tipo), ''), '-') AS responsable_tipo,
            COALESCE(NULLIF(TRIM(b.responsable), ''), '') AS responsable,
            COALESCE(NULLIF(TRIM(b.marca), ''), '') AS marca,
            CAST(b.sku AS TEXT) AS sku,
            COALESCE(b.producto, '') AS descripcion,
            COALESCE(b.stock, 0)::int AS stock,
            COALESCE(b.venta_7, 0)::int AS venta_7,
            COALESCE(b.negativo, '') AS negativo,
            COALESCE(b.riesgo_quiebre, '') AS riesgo_quiebre,
            COALESCE(b.otros, '') AS otros,
            COUNT(*) OVER()::int AS total_rows
        FROM {SCOPE_FACT_BRIDGE_VIEW} b
        WHERE 1=1
        {where_extra}
        ORDER BY
            CASE WHEN {_scope_norm_expr('b.negativo')} = 'SI' THEN 0 ELSE 1 END ASC,
            CASE WHEN {_scope_norm_expr('b.riesgo_quiebre')} = 'SI' THEN 0 ELSE 1 END ASC,
            COALESCE(b.venta_7, 0) ASC,
            COALESCE(b.marca, '') ASC,
            CAST(b.sku AS TEXT) ASC
        {page_sql}
    """
    return qdf(sql, params)


def get_export_inventario_cliente(
    cliente: str | None = None,
    marca: str | None = None,
    responsable_tipo: str | None = None,
    responsable: str | None = None,
    focos: list[str] | None = None,
    search: str = "",
) -> pd.DataFrame:
    cliente_sel = str(cliente or "").strip()
    tipo_norm = _scope_tipo_norm(responsable_tipo)
    responsable_selected = _scope_is_selected(responsable)
    if tipo_norm is None and not responsable_selected:
        where_extra, params = _scope_inventory_base_filters(
            alias="b",
            marca=marca,
            cliente=cliente_sel or None,
            responsable_tipo=None,
            responsable=None,
            focos=focos,
            search=search,
        )
        sql = f"""
            SELECT
                b.fecha,
                CAST(b.cod_rt AS TEXT) AS "COD_RT",
                b.local_nombre_rr AS "LOCAL",
                b.cliente AS "CLIENTE",
                COALESCE(NULLIF(b.gestores, ''), 'SIN ASIGNAR') AS "GESTOR",
                COALESCE(NULLIF(b.supervisores, ''), 'SIN ASIGNAR') AS "SUPERVISOR",
                COALESCE(NULLIF(b.ruteros, ''), 'SIN ASIGNAR') AS "RUTERO",
                COALESCE(NULLIF(b.reponedores, ''), 'SIN ASIGNAR') AS "REPONEDOR",
                COALESCE(NULLIF(b.modalidades, ''), 'SIN ASIGNAR') AS "MODALIDAD",
                b.marca AS "MARCA",
                CAST(b.sku AS TEXT) AS "Sku",
                b.producto AS "Descripción del Producto",
                b.stock AS "Stock",
                b.venta_7 AS "Venta(+7)",
                b.negativo AS "NEGATIVO",
                b.riesgo_quiebre AS "RIESGO DE QUIEBRE",
                b.otros AS "OTROS"
            FROM {SCOPE_CLIENTE_INVENTORY_ENRICHED_MV} b
            WHERE 1=1
            {where_extra}
            ORDER BY
                b.fecha DESC,
                b.cod_rt ASC,
                b.cliente ASC,
                CASE WHEN CAST(b.sku AS TEXT) ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
                CASE WHEN CAST(b.sku AS TEXT) ~ '^[0-9]+$' THEN CAST(b.sku AS BIGINT) END ASC NULLS LAST,
                CAST(b.sku AS TEXT) ASC
        """
        return qdf(sql, params)

    where_extra, params = _scope_inventory_base_filters(
        alias="b",
        marca=marca,
        cliente=cliente_sel or None,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        focos=focos,
        search=search,
    )

    sql = f"""
        WITH base_filtered AS (
            SELECT
                b.fecha,
                b.cod_rt,
                COALESCE(NULLIF(TRIM(b.local_nombre_rr), ''), CAST(b.cod_rt AS TEXT)) AS local_nombre_rr,
                COALESCE(NULLIF(TRIM(b.marca), ''), '') AS cliente,
                COALESCE(NULLIF(TRIM(b.marca_norm), ''), NULLIF(TRIM(b.marca), ''), '') AS cliente_norm,
                COALESCE(NULLIF(TRIM(b.marca), ''), '') AS marca,
                CAST(b.sku AS TEXT) AS sku,
                COALESCE(b.producto, '') AS producto,
                COALESCE(b.stock, 0)::int AS stock,
                COALESCE(b.venta_7, 0)::int AS venta_7,
                COALESCE(b.negativo::text, '') AS negativo,
                COALESCE(b.riesgo_quiebre::text, '') AS riesgo_quiebre,
                COALESCE(b.otros::text, '') AS otros
            FROM {SCOPE_INVENTORY_BASE_VIEW} b
            WHERE 1=1
            {where_extra}
        ),
        rr_ctx AS (
            SELECT
                rr.cod_rt,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.cliente_norm), ''), NULLIF(TRIM(rr.cliente), ''), ''))) AS cliente_norm_match,
                STRING_AGG(
                    DISTINCT CASE
                        WHEN UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.responsable_tipo), ''), '-'))) = 'GESTOR'
                        THEN COALESCE(NULLIF(TRIM(rr.responsable), ''), TRIM(rr.responsable_norm))
                    END,
                    ' | '
                    ORDER BY CASE
                        WHEN UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.responsable_tipo), ''), '-'))) = 'GESTOR'
                        THEN COALESCE(NULLIF(TRIM(rr.responsable), ''), TRIM(rr.responsable_norm))
                    END
                ) FILTER (
                    WHERE UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.responsable_tipo), ''), '-'))) = 'GESTOR'
                      AND NULLIF(TRIM(COALESCE(rr.responsable, rr.responsable_norm, '')), '') IS NOT NULL
                ) AS gestor,
                STRING_AGG(
                    DISTINCT CASE
                        WHEN UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.responsable_tipo), ''), '-'))) = 'SUPERVISOR'
                        THEN COALESCE(NULLIF(TRIM(rr.responsable), ''), TRIM(rr.responsable_norm))
                    END,
                    ' | '
                    ORDER BY CASE
                        WHEN UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.responsable_tipo), ''), '-'))) = 'SUPERVISOR'
                        THEN COALESCE(NULLIF(TRIM(rr.responsable), ''), TRIM(rr.responsable_norm))
                    END
                ) FILTER (
                    WHERE UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.responsable_tipo), ''), '-'))) = 'SUPERVISOR'
                      AND NULLIF(TRIM(COALESCE(rr.responsable, rr.responsable_norm, '')), '') IS NOT NULL
                ) AS supervisor,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.rutero), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.rutero), ''), '-')
                ) AS rutero,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.reponedor), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.reponedor), ''), '-')
                ) AS reponedor,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.modalidad), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.modalidad), ''), '-')
                ) AS modalidad
            FROM {SCOPE_RR_DISTINCT_VIEW} rr
            WHERE EXISTS (
                SELECT 1
                FROM base_filtered bf
                WHERE bf.cod_rt = rr.cod_rt
                  AND UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.cliente_norm), ''), NULLIF(TRIM(rr.cliente), ''), ''))) =
                      UPPER(TRIM(COALESCE(NULLIF(TRIM(bf.cliente_norm), ''), NULLIF(TRIM(bf.cliente), ''), '')))
            )
            GROUP BY 1, 2
        )
        SELECT
            bf.fecha,
            CAST(bf.cod_rt AS TEXT) AS "COD_RT",
            bf.local_nombre_rr AS "LOCAL",
            bf.cliente AS "CLIENTE",
            COALESCE(NULLIF(rr_ctx.gestor, ''), 'SIN ASIGNAR') AS "GESTOR",
            COALESCE(NULLIF(rr_ctx.supervisor, ''), 'SIN ASIGNAR') AS "SUPERVISOR",
            COALESCE(NULLIF(rr_ctx.rutero, ''), 'SIN ASIGNAR') AS "RUTERO",
            COALESCE(NULLIF(rr_ctx.reponedor, ''), 'SIN ASIGNAR') AS "REPONEDOR",
            COALESCE(NULLIF(rr_ctx.modalidad, ''), 'SIN ASIGNAR') AS "MODALIDAD",
            bf.marca AS "MARCA",
            bf.sku AS "Sku",
            bf.producto AS "Descripción del Producto",
            bf.stock AS "Stock",
            bf.venta_7 AS "Venta(+7)",
            bf.negativo AS "NEGATIVO",
            bf.riesgo_quiebre AS "RIESGO DE QUIEBRE",
            bf.otros AS "OTROS"
        FROM base_filtered bf
        LEFT JOIN rr_ctx
          ON rr_ctx.cod_rt = bf.cod_rt
         AND rr_ctx.cliente_norm_match = UPPER(TRIM(COALESCE(NULLIF(TRIM(bf.cliente_norm), ''), NULLIF(TRIM(bf.cliente), ''), '')))
        ORDER BY
            bf.fecha DESC,
            bf.cod_rt ASC,
            bf.cliente ASC,
            CASE WHEN bf.sku ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
            CASE WHEN bf.sku ~ '^[0-9]+$' THEN CAST(bf.sku AS BIGINT) END ASC NULLS LAST,
            bf.sku ASC
    """
    return qdf(sql, params)


@st.cache_data(ttl=QDF_TTL, show_spinner=False)
def get_export_inventario_local(
    cod_rt: str,
    cliente: str | None = None,
) -> pd.DataFrame:
    cliente_where, cliente_params = _build_local_cliente_filter(cliente, alias="v")
    sql = f"""
        WITH rr_ctx AS (
            SELECT
                rr.cod_rt,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.cliente), ''), ''))) AS cliente_norm_match,
                MAX(COALESCE(NULLIF(TRIM(rr.local_nombre), ''), CAST(rr.cod_rt AS TEXT))) AS local_nombre,
                MAX(COALESCE(NULLIF(TRIM(rr.cliente), ''), '')) AS cliente_display,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.gestores), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.gestores), ''), '-')
                ) AS gestor,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.rutero), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.rutero), ''), '-')
                ) AS rutero,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.reponedor), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.reponedor), ''), '-')
                ) AS reponedor
            FROM {RUTA_TABLE} rr
            WHERE rr.cod_rt = :cod_rt
              AND NULLIF(TRIM(COALESCE(rr.cliente, '')), '') IS NOT NULL
            GROUP BY
                rr.cod_rt,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.cliente), ''), '')))
        )
        SELECT
            v.fecha,
            CAST(v.cod_rt AS TEXT) AS "COD_RT",
            COALESCE(rr_ctx.local_nombre, CAST(v.cod_rt AS TEXT)) AS "LOCAL",
            COALESCE(rr_ctx.cliente_display, COALESCE(NULLIF(TRIM(v."MARCA"), ''), '')) AS "CLIENTE",
            COALESCE(rr_ctx.gestor, '-') AS "GESTOR",
            COALESCE(rr_ctx.rutero, '-') AS "RUTERO",
            COALESCE(rr_ctx.reponedor, '-') AS "REPONEDOR",
            COALESCE(NULLIF(TRIM(v."MARCA"), ''), '') AS "MARCA",
            CAST(v."Sku" AS TEXT) AS "Sku",
            COALESCE(v."Descripción del Producto", '') AS "Descripción del Producto",
            COALESCE(v."Stock", 0)::int AS "Stock",
            COALESCE(v."Venta(+7)", 0)::int AS "Venta(+7)",
            COALESCE(v."NEGATIVO", '') AS "NEGATIVO",
            COALESCE(v."RIESGO DE QUIEBRE", '') AS "RIESGO DE QUIEBRE",
            COALESCE(v."OTROS", '') AS "OTROS"
        FROM {RESULT_VIEW} v
        LEFT JOIN rr_ctx
          ON rr_ctx.cod_rt = v.cod_rt
         AND rr_ctx.cliente_norm_match = UPPER(TRIM(COALESCE(v."MARCA", '')))
        WHERE v.cod_rt = :cod_rt
        {cliente_where}
        ORDER BY
          COALESCE(rr_ctx.local_nombre, CAST(v.cod_rt AS TEXT)) ASC,
          v."MARCA" ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN (v."Sku")::bigint END ASC NULLS LAST,
          v."Sku" ASC,
          v."Descripción del Producto" ASC
    """
    return qdf(sql, {"cod_rt": cod_rt, **cliente_params})


@st.cache_data(ttl=QDF_TTL, show_spinner=False)
def get_export_inventario_mercaderista_local(
    cod_rt: str,
    modalidad: str,
    rutero: str,
    reponedor: str,
    cliente: str | None = None,
) -> pd.DataFrame:
    cliente_where, cliente_params = _build_local_cliente_filter(cliente, alias="v")
    modalidad_sql, modalidad_params = _modalidad_clause(modalidad, "rr.modalidad")
    sql = f"""
        WITH rr_ctx AS (
            SELECT
                rr.cod_rt,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.cliente), ''), ''))) AS cliente_norm_match,
                MAX(COALESCE(NULLIF(TRIM(rr.local_nombre), ''), CAST(rr.cod_rt AS TEXT))) AS local_nombre,
                MAX(COALESCE(NULLIF(TRIM(rr.cliente), ''), '')) AS cliente_display,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.gestores), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.gestores), ''), '-')
                ) AS gestor,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.rutero), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.rutero), ''), '-')
                ) AS rutero,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(TRIM(rr.reponedor), ''), '-'),
                    ' | '
                    ORDER BY COALESCE(NULLIF(TRIM(rr.reponedor), ''), '-')
                ) AS reponedor
            FROM {RUTA_TABLE} rr
            WHERE rr.cod_rt = :cod_rt
              AND UPPER(TRIM(COALESCE(rr.rutero, ''))) = UPPER(TRIM(COALESCE(:rutero, '')))
              AND UPPER(TRIM(COALESCE(rr.reponedor, ''))) = UPPER(TRIM(COALESCE(:reponedor, '')))
              {modalidad_sql}
              AND NULLIF(TRIM(COALESCE(rr.cliente, '')), '') IS NOT NULL
            GROUP BY
                rr.cod_rt,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.cliente), ''), '')))
        )
        SELECT
            v.fecha,
            CAST(v.cod_rt AS TEXT) AS "COD_RT",
            COALESCE(rr_ctx.local_nombre, CAST(v.cod_rt AS TEXT)) AS "LOCAL",
            COALESCE(rr_ctx.cliente_display, COALESCE(NULLIF(TRIM(v."MARCA"), ''), '')) AS "CLIENTE",
            COALESCE(rr_ctx.gestor, '-') AS "GESTOR",
            COALESCE(rr_ctx.rutero, '-') AS "RUTERO",
            COALESCE(rr_ctx.reponedor, '-') AS "REPONEDOR",
            COALESCE(NULLIF(TRIM(v."MARCA"), ''), '') AS "MARCA",
            CAST(v."Sku" AS TEXT) AS "Sku",
            COALESCE(v."Descripción del Producto", '') AS "Descripción del Producto",
            COALESCE(v."Stock", 0)::int AS "Stock",
            COALESCE(v."Venta(+7)", 0)::int AS "Venta(+7)",
            COALESCE(v."NEGATIVO", '') AS "NEGATIVO",
            COALESCE(v."RIESGO DE QUIEBRE", '') AS "RIESGO DE QUIEBRE",
            COALESCE(v."OTROS", '') AS "OTROS"
        FROM {RESULT_VIEW} v
        LEFT JOIN rr_ctx
          ON rr_ctx.cod_rt = v.cod_rt
         AND rr_ctx.cliente_norm_match = UPPER(TRIM(COALESCE(v."MARCA", '')))
        WHERE v.cod_rt = :cod_rt
          AND rr_ctx.cod_rt IS NOT NULL
          {cliente_where}
        ORDER BY
          COALESCE(rr_ctx.local_nombre, CAST(v.cod_rt AS TEXT)) ASC,
          v."MARCA" ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN (v."Sku")::bigint END ASC NULLS LAST,
          v."Sku" ASC,
          v."Descripción del Producto" ASC
    """
    return qdf(sql, {
        "cod_rt": cod_rt,
        "rutero": rutero,
        "reponedor": reponedor,
        **modalidad_params,
        **cliente_params,
    })



# =========================================================
# 6) AUDITORÍA DE COBERTURA E INCONSISTENCIAS
# =========================================================
def get_scope_clientes_rr(
    cod_rt: str,
    rutero: str,
    reponedor: str,
    modalidad: str | None = None,
) -> list[str]:
    modalidad_sql, extra = _modalidad_clause(modalidad, "modalidad")
    df = qdf(f"""
        SELECT DISTINCT TRIM(cliente) AS cliente
        FROM {RUTA_TABLE}
        WHERE cod_rt = :cod_rt
          AND UPPER(TRIM(COALESCE(rutero, ''))) = UPPER(TRIM(COALESCE(:rutero, '')))
          AND UPPER(TRIM(COALESCE(reponedor, ''))) = UPPER(TRIM(COALESCE(:reponedor, '')))
          {modalidad_sql}
          AND NULLIF(TRIM(COALESCE(cliente, '')), '') IS NOT NULL
        ORDER BY cliente
    """, {"cod_rt": cod_rt, "rutero": rutero, "reponedor": reponedor, **extra})
    return df["cliente"].astype(str).tolist() if df is not None and not df.empty else []


def get_rr_stock_en_local(cod_rt: str) -> pd.DataFrame:
    return qdf(f"""
        SELECT
            rr.cod_rt,
            TRIM(rr.modalidad) AS modalidad,
            TRIM(rr.rutero) AS rutero,
            TRIM(rr.reponedor) AS reponedor,
            TRIM(rr.cliente) AS cliente_rr,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM {RESULT_VIEW} v
                    WHERE v.cod_rt = rr.cod_rt
                      AND UPPER(TRIM(COALESCE(v."MARCA", ''))) = UPPER(TRIM(COALESCE(rr.cliente, '')))
                ) THEN 'SI'
                ELSE 'NO'
            END AS tiene_stock_match
        FROM {RUTA_TABLE} rr
        WHERE rr.cod_rt = :cod_rt
        ORDER BY modalidad, rutero, reponedor, cliente_rr
    """, {"cod_rt": cod_rt})


def get_cobertura_local(cod_rt: str) -> pd.DataFrame:
    return qdf(f"""
        WITH rr AS (
            SELECT DISTINCT
                cod_rt,
                UPPER(TRIM(COALESCE(cliente, ''))) AS cliente_norm
            FROM {RUTA_TABLE}
            WHERE cod_rt = :cod_rt
              AND NULLIF(TRIM(COALESCE(cliente, '')), '') IS NOT NULL
        ),
        st AS (
            SELECT DISTINCT
                cod_rt,
                UPPER(TRIM(COALESCE("MARCA", ''))) AS marca_norm
            FROM {RESULT_VIEW}
            WHERE cod_rt = :cod_rt
              AND NULLIF(TRIM(COALESCE("MARCA", '')), '') IS NOT NULL
        )
        SELECT
            :cod_rt AS cod_rt,
            (SELECT COUNT(*) FROM rr)::int AS clientes_rr,
            (SELECT COUNT(*) FROM st)::int AS clientes_stock,
            (
                SELECT COUNT(*)::int
                FROM rr
                INNER JOIN st
                    ON rr.cod_rt = st.cod_rt
                   AND rr.cliente_norm = st.marca_norm
            ) AS clientes_match
    """, {"cod_rt": cod_rt})


def get_clientes_sin_match_rr_stock(
    cod_rt: str,
    rutero: str,
    reponedor: str,
    modalidad: str | None = None,
) -> pd.DataFrame:
    modalidad_sql, extra = _modalidad_clause(modalidad, "rr.modalidad")
    return qdf(f"""
        SELECT DISTINCT
            rr.cod_rt,
            TRIM(rr.modalidad) AS modalidad,
            TRIM(rr.rutero) AS rutero,
            TRIM(rr.reponedor) AS reponedor,
            TRIM(rr.cliente) AS cliente_rr
        FROM {RUTA_TABLE} rr
        WHERE rr.cod_rt = :cod_rt
          AND UPPER(TRIM(COALESCE(rr.rutero, ''))) = UPPER(TRIM(COALESCE(:rutero, '')))
          AND UPPER(TRIM(COALESCE(rr.reponedor, ''))) = UPPER(TRIM(COALESCE(:reponedor, '')))
          {modalidad_sql}
          AND NULLIF(TRIM(COALESCE(rr.cliente, '')), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {RESULT_VIEW} v
              WHERE v.cod_rt = rr.cod_rt
                AND UPPER(TRIM(COALESCE(v."MARCA", ''))) = UPPER(TRIM(COALESCE(rr.cliente, '')))
          )
        ORDER BY cliente_rr
    """, {"cod_rt": cod_rt, "rutero": rutero, "reponedor": reponedor, **extra})



# =========================================================
# 7) CONTROL GESTION B3 / CONTRATO PUBLICO CANONICO
# =========================================================
def get_cg_contract() -> dict[str, Any]:
    return {
        "status": "frozen",
        "branch": "B3_CONTROL_GESTION_SQL",
        "roles_publicos": ["JEFE_OPERACIONES", "GESTOR"],
        "views": {
            "parity": CG_PARITY_VIEW,
            "scope": CG_SCOPE_VIEW,
            "alertas": CG_ALERTAS_VIEW,
            "inicio_jefe": CG_INICIO_JEFE_VIEW,
            "inicio_gestor": CG_INICIO_GESTOR_VIEW,
            "detalle": CG_DETALLE_VIEW,
        },
        "compat_aliases": {
            "alertas": CG_COMPAT_ALERTAS_VIEW,
            "inicio_jefe": CG_COMPAT_INICIO_JEFE_VIEW,
            "inicio_gestor": CG_COMPAT_INICIO_GESTOR_VIEW,
            "detalle": CG_COMPAT_DETALLE_VIEW,
        },
        "rules": [
            "no_recalculo_negocio_en_python",
            "pair_global_truth_separada_de_scope_assigned_truth",
            "no_supervisor_no_reponedor_en_contrato_publico_actual",
        ],
    }


def _cg_page_clause(limit: int | None, offset: int | None, params: dict[str, Any]) -> str:
    clauses: list[str] = []
    if limit is not None:
        params["limit"] = max(1, int(limit))
        clauses.append("LIMIT :limit")
    if offset is not None:
        params["offset"] = max(0, int(offset))
        clauses.append("OFFSET :offset")
    return (" " + " ".join(clauses)) if clauses else ""


def _cg_text_norm_expr(expr: str) -> str:
    return f"UPPER(TRIM(COALESCE(CAST({expr} AS TEXT), '')))"


def _cg_v2_out_weekly_is_mv() -> bool:
    view_name = str(CG_V2_OUT_WEEKLY_VIEW or "").strip().lower()
    return view_name == "cg_mart.mv_cg_out_weekly_v2" or view_name.endswith(".mv_cg_out_weekly_v2")


def _cg_norm_filter_value(value: str | None) -> str | None:
    raw = str(value or "").strip()
    return raw.upper() if raw else None


def _cg_pipe_member_filter(
    *,
    column_expr: str,
    param_prefix: str,
    value: str | None,
) -> tuple[str, dict[str, Any]]:
    token = _cg_norm_filter_value(value)
    if not token or token == "TODOS":
        return "", {}
    return (
        f"""
        AND (
            {column_expr} = :{param_prefix}_exact
            OR {column_expr} LIKE :{param_prefix}_prefix_spaced
            OR {column_expr} LIKE :{param_prefix}_suffix_spaced
            OR {column_expr} LIKE :{param_prefix}_middle_spaced
            OR {column_expr} LIKE :{param_prefix}_prefix_pipe
            OR {column_expr} LIKE :{param_prefix}_suffix_pipe
            OR {column_expr} LIKE :{param_prefix}_middle_pipe
        )
        """,
        {
            f"{param_prefix}_exact": token,
            f"{param_prefix}_prefix_spaced": f"{token} | %",
            f"{param_prefix}_suffix_spaced": f"% | {token}",
            f"{param_prefix}_middle_spaced": f"% | {token} | %",
            f"{param_prefix}_prefix_pipe": f"{token}|%",
            f"{param_prefix}_suffix_pipe": f"%|{token}",
            f"{param_prefix}_middle_pipe": f"%|{token}|%",
        },
    )


def _cg_pipe_token_map(raw_values: list[object]) -> dict[str, str]:
    token_map: dict[str, str] = {}
    for raw_value in raw_values:
        raw_text = str(raw_value or "").strip()
        if not raw_text:
            continue
        for token_part in raw_text.split("|"):
            token_display = token_part.strip()
            token_norm = _cg_norm_filter_value(token_display)
            if token_display and token_norm and token_norm not in token_map:
                token_map[token_norm] = token_display
    return token_map


def _cg_pipe_token_values(
    raw_values: list[object],
    *,
    preferred_first: str | None = None,
) -> list[str]:
    token_map = _cg_pipe_token_map(raw_values)
    if not token_map:
        return []
    ordered_keys = sorted(token_map)
    preferred_norm = _cg_norm_filter_value(preferred_first)
    if preferred_norm and preferred_norm in token_map:
        ordered_keys = [preferred_norm] + [key for key in ordered_keys if key != preferred_norm]
    return [token_map[key] for key in ordered_keys]


def _cg_v2_route_shared_display(
    raw_value: object,
    *,
    selected_rutero: str | None = None,
) -> str:
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return "No"
    raw_values = [part.strip() for part in raw_text.split(" || ") if part.strip()]
    token_values = _cg_pipe_token_values(raw_values, preferred_first=selected_rutero)
    if len(token_values) <= 1:
        return "No"
    return " | ".join(token_values)


def _cg_v2_out_weekly_filters(
    *,
    alias: str = "v",
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
    cod_rt: str | None = None,
    rutero: str | None = None,
    local: str | None = None,
) -> tuple[str, dict[str, Any]]:
    pfx = f"{alias}." if alias else ""
    filters: list[str] = []
    params: dict[str, Any] = {}
    mv_mode = _cg_v2_out_weekly_is_mv()

    if semana_inicio and str(semana_inicio).strip():
        filters.append(f'AND {pfx}"SEMANA_INICIO" = :semana_inicio')
        params["semana_inicio"] = str(semana_inicio).strip()

    if gestor and str(gestor).strip() and str(gestor).strip().upper() != "TODOS":
        if mv_mode:
            gestor_filter_sql, gestor_params = _cg_pipe_member_filter(
                column_expr=f'{pfx}"GESTOR_NORM_FILTER"',
                param_prefix="gestor_norm_filter",
                value=gestor,
            )
        else:
            gestor_filter_sql, gestor_params = _cg_pipe_member_filter(
                column_expr=_cg_text_norm_expr(f'{pfx}"GESTOR"'),
                param_prefix="gestor_norm_filter",
                value=gestor,
            )
        filters.append(gestor_filter_sql)
        params.update(gestor_params)

    if cliente and str(cliente).strip() and str(cliente).strip().upper() != "TODOS":
        if mv_mode:
            filters.append(f'AND {pfx}"CLIENTE_NORM_FILTER" = :cliente_norm_filter')
        else:
            filters.append(f'AND {_cg_text_norm_expr(f"{pfx}" + "\"CLIENTE\"")} = :cliente_norm_filter')
        params["cliente_norm_filter"] = _cg_norm_filter_value(cliente)

    if alerta and str(alerta).strip() and str(alerta).strip().upper() != "TODAS":
        if mv_mode:
            filters.append(f'AND {pfx}"ALERTA_NORM_FILTER" = :alerta_norm_filter')
        else:
            filters.append(f'AND {_cg_text_norm_expr(f"{pfx}" + "\"ALERTA\"")} = :alerta_norm_filter')
        params["alerta_norm_filter"] = _cg_norm_filter_value(alerta)

    if cod_rt and str(cod_rt).strip():
        filters.append(f'AND CAST({pfx}"COD_RT" AS TEXT) = :cod_rt')
        params["cod_rt"] = str(cod_rt).strip()

    if rutero and str(rutero).strip() and str(rutero).strip().upper() != "TODOS":
        if mv_mode:
            rutero_filter_sql, rutero_params = _cg_pipe_member_filter(
                column_expr=f'{pfx}"RUTERO_NORM_FILTER"',
                param_prefix="rutero_norm_filter",
                value=rutero,
            )
        else:
            rutero_filter_sql, rutero_params = _cg_pipe_member_filter(
                column_expr=_cg_text_norm_expr(f'{pfx}"RUTERO"'),
                param_prefix="rutero_norm_filter",
                value=rutero,
            )
        filters.append(rutero_filter_sql)
        params.update(rutero_params)

    if local and str(local).strip() and str(local).strip().upper() != "TODOS":
        if mv_mode:
            filters.append(f'AND {pfx}"LOCAL_NORM_FILTER" = :local_norm_filter')
        else:
            filters.append(f'AND {_cg_text_norm_expr(f"{pfx}" + "\"LOCAL\"")} = :local_norm_filter')
        params["local_norm_filter"] = _cg_norm_filter_value(local)

    return "\n".join(filters), params


def _cg_scope_filters(
    *,
    alias: str = "v",
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
    cod_rt: str | None = None,
    rutero: str | None = None,
    local: str | None = None,
) -> tuple[str, dict[str, Any]]:
    pfx = f"{alias}." if alias else ""
    filters: list[str] = []
    params: dict[str, Any] = {}

    if semana_inicio and str(semana_inicio).strip():
        filters.append(f'AND {pfx}"SEMANA_INICIO" = :semana_inicio')
        params["semana_inicio"] = str(semana_inicio).strip()

    if gestor and str(gestor).strip() and str(gestor).strip().upper() != "TODOS":
        filters.append(
            f'AND {_cg_text_norm_expr(f"{pfx}" + "\"GESTOR\"")} = {_cg_text_norm_expr(":gestor")}'
        )
        params["gestor"] = str(gestor).strip()

    if cliente and str(cliente).strip() and str(cliente).strip().upper() != "TODOS":
        filters.append(
            f'AND {_cg_text_norm_expr(f"{pfx}" + "\"CLIENTE\"")} = {_cg_text_norm_expr(":cliente")}'
        )
        params["cliente"] = str(cliente).strip()

    if alerta and str(alerta).strip() and str(alerta).strip().upper() != "TODAS":
        filters.append(
            f'AND {_cg_text_norm_expr(f"{pfx}" + "\"ALERTA\"")} = {_cg_text_norm_expr(":alerta")}'
        )
        params["alerta"] = str(alerta).strip()

    if cod_rt and str(cod_rt).strip():
        filters.append(f'AND CAST({pfx}"COD_RT" AS TEXT) = :cod_rt')
        params["cod_rt"] = str(cod_rt).strip()

    if rutero and str(rutero).strip() and str(rutero).strip().upper() != "TODOS":
        filters.append(
            f'AND {_cg_text_norm_expr(f"{pfx}" + "\"RUTERO\"")} = {_cg_text_norm_expr(":rutero")}'
        )
        params["rutero"] = str(rutero).strip()

    if local and str(local).strip() and str(local).strip().upper() != "TODOS":
        filters.append(
            f'AND {_cg_text_norm_expr(f"{pfx}" + "\"LOCAL\"")} = {_cg_text_norm_expr(":local")}'
        )
        params["local"] = str(local).strip()

    return "\n".join(filters), params


def _cg_v2_scope_search_filters(
    *,
    alias: str = "v",
    search: str = "",
) -> tuple[str, dict[str, Any]]:
    pfx = f"{alias}." if alias else ""
    raw = str(search or "").strip()
    if not raw:
        return "", {}
    return (
        f"""
        AND (
            CAST({pfx}"COD_RT" AS TEXT) ILIKE :search
            OR CAST({pfx}"LOCAL" AS TEXT) ILIKE :search
            OR CAST({pfx}"CLIENTE" AS TEXT) ILIKE :search
            OR CAST({pfx}"GESTOR" AS TEXT) ILIKE :search
            OR CAST({pfx}"RUTERO" AS TEXT) ILIKE :search
            OR CAST({pfx}"REPONEDOR" AS TEXT) ILIKE :search
        )
        """,
        {"search": f"%{raw}%"},
    )


def _cg_v2_daily_evidence_filters(
    *,
    alias: str = "d",
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    rutero: str | None = None,
    cod_rt: str | None = None,
) -> tuple[str, dict[str, Any]]:
    pfx = f"{alias}." if alias else ""
    filters: list[str] = []
    params: dict[str, Any] = {}

    if semana_inicio and str(semana_inicio).strip():
        filters.append(f"AND {pfx}semana_inicio = CAST(:semana_inicio AS DATE)")
        params["semana_inicio"] = str(semana_inicio).strip()

    if gestor and str(gestor).strip() and str(gestor).strip().upper() != "TODOS":
        filters.append(f"AND {_cg_text_norm_expr(f'{pfx}gestor')} = {_cg_text_norm_expr(':gestor')}")
        params["gestor"] = str(gestor).strip()

    if cliente and str(cliente).strip() and str(cliente).strip().upper() != "TODOS":
        filters.append(f"AND {_cg_text_norm_expr(f'{pfx}cliente')} = {_cg_text_norm_expr(':cliente')}")
        params["cliente"] = str(cliente).strip()

    if rutero and str(rutero).strip() and str(rutero).strip().upper() != "TODOS":
        filters.append(f"AND {_cg_text_norm_expr(f'{pfx}rutero')} = {_cg_text_norm_expr(':rutero')}")
        params["rutero"] = str(rutero).strip()

    if cod_rt and str(cod_rt).strip():
        filters.append(f"AND CAST({pfx}cod_rt AS TEXT) = :cod_rt")
        params["cod_rt"] = str(cod_rt).strip()

    return "\n".join(filters), params


def _cg_v2_status_case(plan_col: str, flag_col: str) -> str:
    return f"""
        CASE
            WHEN COALESCE({plan_col}, 0) >= 1 AND COALESCE({flag_col}, 0) >= 1 THEN 'PLAN_OK'
            WHEN COALESCE({plan_col}, 0) >= 1 AND COALESCE({flag_col}, 0) = 0 THEN 'PLAN_PEND'
            WHEN COALESCE({plan_col}, 0) = 0 AND COALESCE({flag_col}, 0) >= 1 THEN 'OFFPLAN_OK'
            ELSE 'NONE'
        END
    """


def _cg_v2_checklist_case(plan_col: str, flag_col: str) -> str:
    return f"""
        CASE
            WHEN COALESCE({plan_col}, 0) >= 1 AND COALESCE({flag_col}, 0) >= 1 THEN 'REQ_OK'
            WHEN COALESCE({plan_col}, 0) >= 1 AND COALESCE({flag_col}, 0) = 0 THEN 'REQ'
            WHEN COALESCE({plan_col}, 0) = 0 AND COALESCE({flag_col}, 0) >= 1 THEN 'OK'
            ELSE ''
        END
    """


def _cg_v2_daily_matrix_order_sql(vista_key: str) -> str:
    order_sql_map = {
        "RUTERO": """
            ORDER BY
                "SEMANA_INICIO" DESC,
                "GESTOR" ASC,
                "RUTERO" ASC,
                "LOCAL" ASC,
                "CLIENTE" ASC,
                "REPONEDOR" ASC
        """,
        "LOCAL": """
            ORDER BY
                "SEMANA_INICIO" DESC,
                "GESTOR" ASC,
                "LOCAL" ASC,
                "CLIENTE" ASC,
                "RUTERO" ASC,
                "REPONEDOR" ASC
        """,
        "CLIENTE": """
            ORDER BY
                "SEMANA_INICIO" DESC,
                "GESTOR" ASC,
                "CLIENTE" ASC,
                "LOCAL" ASC,
                "RUTERO" ASC,
                "REPONEDOR" ASC
        """,
    }
    return order_sql_map[vista_key]


def _cg_alertas_filters(
    *,
    alias: str = "a",
    semana_inicio: str | None = None,
    cliente: str | None = None,
    persona: str | None = None,
    cod_rt: str | None = None,
) -> tuple[str, dict[str, Any]]:
    pfx = f"{alias}." if alias else ""
    filters: list[str] = []
    params: dict[str, Any] = {}

    if semana_inicio and str(semana_inicio).strip():
        filters.append(
            f"""
            AND {pfx}fecha_visita >= CAST(:semana_inicio AS DATE)
            AND {pfx}fecha_visita < (CAST(:semana_inicio AS DATE) + INTERVAL '7 day')
            """
        )
        params["semana_inicio"] = str(semana_inicio).strip()

    if cliente and str(cliente).strip() and str(cliente).strip().upper() != "TODOS":
        filters.append(f'AND {_cg_text_norm_expr(f"{pfx}cliente")} = {_cg_text_norm_expr(":cliente")}')
        params["cliente"] = str(cliente).strip()

    if persona and str(persona).strip() and str(persona).strip().upper() != "TODOS":
        filters.append(f'AND {_cg_text_norm_expr(f"{pfx}persona")} = {_cg_text_norm_expr(":persona")}')
        params["persona"] = str(persona).strip()

    if cod_rt and str(cod_rt).strip():
        filters.append(f"AND CAST({pfx}cod_rt AS TEXT) = :cod_rt")
        params["cod_rt"] = str(cod_rt).strip()

    return "\n".join(filters), params


def _cg_detalle_filters(
    *,
    alias: str = "d",
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    cod_rt: str | None = None,
) -> tuple[str, dict[str, Any]]:
    pfx = f"{alias}." if alias else ""
    filters: list[str] = []
    params: dict[str, Any] = {}

    if semana_inicio and str(semana_inicio).strip():
        filters.append(
            f"""
            AND {pfx}fecha_visita >= CAST(:semana_inicio AS DATE)
            AND {pfx}fecha_visita < (CAST(:semana_inicio AS DATE) + INTERVAL '7 day')
            """
        )
        params["semana_inicio"] = str(semana_inicio).strip()

    if gestor and str(gestor).strip() and str(gestor).strip().upper() != "TODOS":
        filters.append(f'AND {_cg_text_norm_expr(f"{pfx}gestor")} = {_cg_text_norm_expr(":gestor")}')
        params["gestor"] = str(gestor).strip()

    if cliente and str(cliente).strip() and str(cliente).strip().upper() != "TODOS":
        filters.append(f'AND {_cg_text_norm_expr(f"{pfx}cliente")} = {_cg_text_norm_expr(":cliente")}')
        params["cliente"] = str(cliente).strip()

    if cod_rt and str(cod_rt).strip():
        filters.append(f"AND CAST({pfx}cod_rt AS TEXT) = :cod_rt")
        params["cod_rt"] = str(cod_rt).strip()

    return "\n".join(filters), params


def _cg_select_page(
    *,
    selector_name: str,
    view_name: str,
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    params: dict[str, Any] = {}
    page_sql = _cg_page_clause(limit, offset, params)
    sql = f"""
        SELECT *
        FROM {view_name}
        {page_sql}
    """
    return _selector_df(selector_name, sql, params or None)


def _cg_select_page_filtered(
    *,
    selector_name: str,
    view_name: str,
    where_sql: str = "",
    order_sql: str = "",
    params: dict[str, Any] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    from_alias: str | None = None,
    select_sql: str = "*",
) -> pd.DataFrame:
    query_params = dict(params or {})
    page_sql = _cg_page_clause(limit, offset, query_params)
    alias_sql = f" {from_alias}" if from_alias else ""
    sql = f"""
        WITH base AS (
            SELECT {select_sql}
            FROM {view_name}{alias_sql}
            WHERE 1=1
            {where_sql}
        )
        SELECT
            base.*,
            COUNT(*) OVER()::int AS total_rows
        FROM base
        {order_sql}
        {page_sql}
    """
    return _selector_df(selector_name, sql, query_params or None)


def get_cg_cumplimiento_semana_local_parity(
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    return _cg_select_page(
        selector_name="get_cg_cumplimiento_semana_local_parity",
        view_name=CG_PARITY_VIEW,
        limit=limit,
        offset=offset,
    )


def get_cg_cumplimiento_semana_scope(
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    return _cg_select_page(
        selector_name="get_cg_cumplimiento_semana_scope",
        view_name=CG_SCOPE_VIEW,
        limit=limit,
        offset=offset,
    )


def get_cg_alertas_control_gestion(
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    params: dict[str, Any] = {}
    page_sql = _cg_page_clause(limit, offset, params)
    sql = f"""
        SELECT *
        FROM {CG_ALERTAS_VIEW} a
        ORDER BY
            COALESCE(prioridad, 0) DESC,
            fecha_visita DESC,
            cod_rt ASC,
            cliente ASC
        {page_sql}
    """
    return _selector_df("get_cg_alertas_control_gestion", sql, params or None)


def get_cg_inicio_jefe(
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    return _cg_select_page(
        selector_name="get_cg_inicio_jefe",
        view_name=CG_INICIO_JEFE_VIEW,
        limit=limit,
        offset=offset,
    )


def get_cg_inicio_gestor(
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    return _cg_select_page(
        selector_name="get_cg_inicio_gestor",
        view_name=CG_INICIO_GESTOR_VIEW,
        limit=limit,
        offset=offset,
    )


def get_cg_cumplimiento_detalle(
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    return _cg_select_page(
        selector_name="get_cg_cumplimiento_detalle",
        view_name=CG_DETALLE_VIEW,
        limit=limit,
        offset=offset,
    )


def get_cg_scope_semanas() -> list[Any]:
    df = _selector_df(
        "get_cg_scope_semanas",
        f"""
        SELECT DISTINCT "SEMANA_INICIO" AS semana_inicio
        FROM {CG_SCOPE_VIEW}
        WHERE "SEMANA_INICIO" IS NOT NULL
        ORDER BY semana_inicio DESC
        """,
    )
    return df["semana_inicio"].tolist() if df is not None and not df.empty else []


def get_cg_scope_gestores(
    semana_inicio: str | None = None,
) -> list[str]:
    where_sql, params = _cg_scope_filters(alias="v", semana_inicio=semana_inicio)
    df = _selector_df(
        "get_cg_scope_gestores",
        f"""
        SELECT DISTINCT CAST("GESTOR" AS TEXT) AS gestor
        FROM {CG_SCOPE_VIEW} v
        WHERE NULLIF(TRIM(COALESCE(CAST("GESTOR" AS TEXT), '')), '') IS NOT NULL
        {where_sql}
        ORDER BY gestor
        """,
        params or None,
    )
    return df["gestor"].astype(str).tolist() if df is not None and not df.empty else []


def get_cg_scope_clientes(
    semana_inicio: str | None = None,
    gestor: str | None = None,
) -> list[str]:
    where_sql, params = _cg_scope_filters(alias="v", semana_inicio=semana_inicio, gestor=gestor)
    df = _selector_df(
        "get_cg_scope_clientes",
        f"""
        SELECT DISTINCT CAST("CLIENTE" AS TEXT) AS cliente
        FROM {CG_SCOPE_VIEW} v
        WHERE NULLIF(TRIM(COALESCE(CAST("CLIENTE" AS TEXT), '')), '') IS NOT NULL
        {where_sql}
        ORDER BY cliente
        """,
        params or None,
    )
    return df["cliente"].astype(str).tolist() if df is not None and not df.empty else []


def get_cg_scope_alertas(
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
) -> list[str]:
    where_sql, params = _cg_scope_filters(
        alias="v",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
    )
    df = _selector_df(
        "get_cg_scope_alertas",
        f"""
        SELECT DISTINCT CAST("ALERTA" AS TEXT) AS alerta
        FROM {CG_SCOPE_VIEW} v
        WHERE NULLIF(TRIM(COALESCE(CAST("ALERTA" AS TEXT), '')), '') IS NOT NULL
        {where_sql}
        ORDER BY alerta
        """,
        params or None,
    )
    return df["alerta"].astype(str).tolist() if df is not None and not df.empty else []


def get_cg_scope_kpis(
    *,
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
    cod_rt: str | None = None,
) -> pd.DataFrame:
    where_sql, params = _cg_scope_filters(
        alias="v",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
        alerta=alerta,
        cod_rt=cod_rt,
    )
    sql = f"""
        SELECT
            COUNT(*)::int AS total_rows_scope,
            COUNT(DISTINCT CAST("COD_RT" AS TEXT))::int AS locales_scope,
            COUNT(DISTINCT CAST("CLIENTE" AS TEXT))::int AS clientes_scope,
            COALESCE(SUM(COALESCE("VISITA", 0)), 0)::int AS visitas_plan,
            COALESCE(SUM(COALESCE("VISITA_REALIZADA", 0)), 0)::int AS visitas_realizadas,
            COALESCE(SUM(CASE
                WHEN {_cg_text_norm_expr('"ALERTA"')} = 'CUMPLE' THEN 1
                ELSE 0
            END), 0)::int AS cumple,
            COALESCE(SUM(CASE
                WHEN {_cg_text_norm_expr('"ALERTA"')} LIKE '%DOBLE%'
                  OR COALESCE("DIAS_DOBLE_MARCAJE", 0) > 0
                THEN 1
                ELSE 0
            END), 0)::int AS cumple_con_doble_marcaje,
            COALESCE(SUM(CASE
                WHEN {_cg_text_norm_expr('"ALERTA"')} <> 'CUMPLE'
                 AND {_cg_text_norm_expr('"ALERTA"')} NOT LIKE '%DOBLE%'
                THEN 1
                ELSE 0
            END), 0)::int AS incumple
        FROM {CG_SCOPE_VIEW} v
        WHERE 1=1
        {where_sql}
    """
    return qdf(sql, params or None)


def get_cg_scope_page(
    *,
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
    cod_rt: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    where_sql, params = _cg_scope_filters(
        alias="v",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
        alerta=alerta,
        cod_rt=cod_rt,
    )
    order_sql = """
        ORDER BY
            "SEMANA_INICIO" DESC,
            COALESCE("DIFERENCIA", 0) DESC,
            COALESCE("DIAS_DOBLE_MARCAJE", 0) DESC,
            CAST("GESTOR" AS TEXT) ASC,
            CAST("CLIENTE" AS TEXT) ASC,
            CAST("LOCAL" AS TEXT) ASC
    """
    select_sql = """
        "SEMANA_INICIO",
        "COD_RT",
        "CLIENTE",
        "LOCAL",
        "GESTOR",
        "MODALIDAD",
        "REPONEDOR_SCOPE",
        "VISITA",
        "VISITA_REALIZADA",
        "DIFERENCIA",
        "DIAS_DOBLE_MARCAJE",
        "DIAS_KPIONE",
        "DIAS_POWER_APP",
        "ALERTA"
    """
    return _cg_select_page_filtered(
        selector_name="get_cg_scope_page",
        view_name=CG_SCOPE_VIEW,
        where_sql=where_sql,
        order_sql=order_sql,
        params=params,
        limit=limit,
        offset=offset,
        from_alias="v",
        select_sql=select_sql,
    )


def get_cg_parity_page(
    *,
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
    cod_rt: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    where_sql, params = _cg_scope_filters(
        alias="v",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
        alerta=alerta,
        cod_rt=cod_rt,
    )
    order_sql = """
        ORDER BY
            "SEMANA_INICIO" DESC,
            CAST("CLIENTE" AS TEXT) ASC,
            CAST("LOCAL" AS TEXT) ASC
    """
    select_sql = """
        "SEMANA_INICIO",
        "COD_RT",
        "CLIENTE",
        "LOCAL",
        "GESTOR",
        "VISITA",
        "VISITA_REALIZADA",
        "DIFERENCIA",
        "ALERTA"
    """
    return _cg_select_page_filtered(
        selector_name="get_cg_parity_page",
        view_name=CG_PARITY_VIEW,
        where_sql=where_sql,
        order_sql=order_sql,
        params=params,
        limit=limit,
        offset=offset,
        from_alias="v",
        select_sql=select_sql,
    )


def get_cg_alertas_page(
    *,
    semana_inicio: str | None = None,
    cliente: str | None = None,
    persona: str | None = None,
    cod_rt: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    where_sql, params = _cg_alertas_filters(
        alias="a",
        semana_inicio=semana_inicio,
        cliente=cliente,
        persona=persona,
        cod_rt=cod_rt,
    )
    order_sql = """
        ORDER BY
            COALESCE(prioridad, 0) DESC,
            fecha_visita DESC,
            cod_rt ASC,
            cliente ASC
    """
    return _cg_select_page_filtered(
        selector_name="get_cg_alertas_page",
        view_name=CG_ALERTAS_VIEW,
        where_sql=where_sql,
        order_sql=order_sql,
        params=params,
        limit=limit,
        offset=offset,
        from_alias="a",
    )


def get_cg_detalle_page(
    *,
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    cod_rt: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    where_sql, params = _cg_detalle_filters(
        alias="d",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
        cod_rt=cod_rt,
    )
    order_sql = """
        ORDER BY
            fecha_visita DESC,
            cod_rt ASC,
            cliente ASC,
            gestor ASC
    """
    return _cg_select_page_filtered(
        selector_name="get_cg_detalle_page",
        view_name=CG_DETALLE_VIEW,
        where_sql=where_sql,
        order_sql=order_sql,
        params=params,
        limit=limit,
        offset=offset,
        from_alias="d",
    )


def get_cg_contract_smoke() -> dict[str, Any]:
    checks = [
        ("parity", CG_PARITY_VIEW),
        ("scope", CG_SCOPE_VIEW),
        ("alertas", CG_ALERTAS_VIEW),
        ("inicio_jefe", CG_INICIO_JEFE_VIEW),
        ("inicio_gestor", CG_INICIO_GESTOR_VIEW),
        ("detalle", CG_DETALLE_VIEW),
    ]
    results: list[dict[str, Any]] = []
    failed_objects: list[str] = []
    empty_warns: list[str] = []

    for object_name, view_name in checks:
        try:
            df = _selector_df(
                f"smoke_{object_name}",
                f"SELECT EXISTS (SELECT 1 FROM {view_name} LIMIT 1) AS has_rows",
            )
            has_rows = bool(df.iloc[0]["has_rows"]) if df is not None and not df.empty else False
            status = "ok" if has_rows else "warn"
            if not has_rows:
                empty_warns.append(object_name)
            results.append({
                "object": object_name,
                "view": view_name,
                "has_rows": has_rows,
                "status": status,
            })
        except Exception as exc:
            failed_objects.append(object_name)
            results.append({
                "object": object_name,
                "view": view_name,
                "has_rows": None,
                "status": "fail",
                "error": f"{type(exc).__name__}: {exc}",
            })

    smoke_status = "fail" if failed_objects else ("warn" if empty_warns else "ok")
    return {
        "smoke_status": smoke_status,
        "branch": "B3_CONTROL_GESTION_SQL",
        "views_checked": len(checks),
        "failed_objects": failed_objects,
        "zero_row_objects": empty_warns,
        "results": results,
    }


def get_cg_v2_contract() -> dict[str, Any]:
    return {
        "status": "parallel_read_only",
        "legacy_unchanged": True,
        "views": {
            "scope": CG_V2_SCOPE_VIEW,
            "detalle": CG_V2_DETALLE_VIEW,
            "out_weekly": CG_V2_OUT_WEEKLY_VIEW,
            "marcaje_multifuente": CG_V2_MULTI_MARCAJE_VIEW,
            "ruta_duplicados": CG_V2_RUTA_DUP_VIEW,
            "fuera_cruce_real": CG_V2_FUERA_CRUCE_REAL_VIEW,
            "sin_batch_ruta_semana": CG_V2_SIN_BATCH_RUTA_VIEW,
        },
        "rules": [
            "ruta_rutero_versionada_gobierna_plan_frecuencia",
            "kpione_kpione2_power_app_gobiernan_evidencia",
            "un_reporte_o_mas_por_fecha_local_cliente_equivale_a_una_visita_valida",
            "multiplicidad_se_conserva_como_auditoria_no_como_visita_adicional",
            "no_recalculo_negocio_en_python",
        ],
    }


def get_cg_v2_contract_smoke() -> dict[str, Any]:
    checks = [
        ("scope", CG_V2_SCOPE_VIEW),
        ("detalle", CG_V2_DETALLE_VIEW),
        ("out_weekly", CG_V2_OUT_WEEKLY_VIEW),
        ("marcaje_multifuente", CG_V2_MULTI_MARCAJE_VIEW),
        ("ruta_duplicados", CG_V2_RUTA_DUP_VIEW),
        ("fuera_cruce_real", CG_V2_FUERA_CRUCE_REAL_VIEW),
        ("sin_batch_ruta_semana", CG_V2_SIN_BATCH_RUTA_VIEW),
    ]
    results: list[dict[str, Any]] = []
    failed_objects: list[str] = []
    zero_row_objects: list[str] = []

    for object_name, view_name in checks:
        try:
            df = _selector_df(
                f"smoke_v2_{object_name}",
                f"SELECT COUNT(*)::bigint AS rows FROM {view_name}",
            )
            rows = int(df.iloc[0]["rows"]) if df is not None and not df.empty else 0
            status = "ok" if rows > 0 else "warn"
            if rows <= 0:
                zero_row_objects.append(object_name)
            results.append({
                "object": object_name,
                "view": view_name,
                "rows": rows,
                "status": status,
            })
        except Exception as exc:
            failed_objects.append(object_name)
            results.append({
                "object": object_name,
                "view": view_name,
                "rows": None,
                "status": "fail",
                "error": f"{type(exc).__name__}: {exc}",
            })

    smoke_status = "fail" if failed_objects else ("warn" if zero_row_objects else "ok")
    return {
        "smoke_status": smoke_status,
        "views_checked": len(checks),
        "failed_objects": failed_objects,
        "zero_row_objects": zero_row_objects,
        "results": results,
    }


def get_cg_v2_scope_semanas() -> pd.DataFrame:
    return _selector_df(
        "get_cg_v2_scope_semanas",
        f"""
        SELECT DISTINCT "SEMANA_INICIO" AS semana_inicio
        FROM {CG_V2_SCOPE_VIEW}
        WHERE "SEMANA_INICIO" IS NOT NULL
        ORDER BY semana_inicio DESC
        """,
    )


def get_cg_v2_recent_weeks(limit: int = 3) -> list[str]:
    recent_limit = max(1, int(limit or 3))
    df = _selector_df(
        "get_cg_v2_recent_weeks",
        f"""
        SELECT CAST(effective_week_start AS TEXT) AS semana_inicio
        FROM {CG_V2_ROUTE_FREQ_RESUELTA_VIEW}
        WHERE effective_week_start IS NOT NULL
        GROUP BY effective_week_start
        ORDER BY effective_week_start DESC
        LIMIT :limit
        """,
        {"limit": recent_limit},
    )
    return df["semana_inicio"].astype(str).tolist() if df is not None and not df.empty else []


def _cg_v2_route_filters(
    *,
    alias: str = "v",
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    rutero: str | None = None,
    local: str | None = None,
) -> tuple[str, dict[str, Any]]:
    pfx = f"{alias}." if alias else ""
    filters: list[str] = []
    params: dict[str, Any] = {}

    if semana_inicio and str(semana_inicio).strip():
        filters.append(f"AND CAST({pfx}effective_week_start AS TEXT) = :semana_inicio")
        params["semana_inicio"] = str(semana_inicio).strip()

    if gestor and str(gestor).strip() and str(gestor).strip().upper() != "TODOS":
        gestor_filter_sql, gestor_params = _cg_pipe_member_filter(
            column_expr=_cg_text_norm_expr(f"{pfx}gestor"),
            param_prefix="gestor_norm_filter",
            value=gestor,
        )
        filters.append(gestor_filter_sql)
        params.update(gestor_params)

    if cliente and str(cliente).strip() and str(cliente).strip().upper() != "TODOS":
        filters.append(
            f"AND {_cg_text_norm_expr(f'{pfx}cliente')} = {_cg_text_norm_expr(':cliente')}"
        )
        params["cliente"] = str(cliente).strip()

    if rutero and str(rutero).strip() and str(rutero).strip().upper() != "TODOS":
        rutero_filter_sql, rutero_params = _cg_pipe_member_filter(
            column_expr=_cg_text_norm_expr(f"{pfx}rutero"),
            param_prefix="rutero_norm_filter",
            value=rutero,
        )
        filters.append(rutero_filter_sql)
        params.update(rutero_params)

    if local and str(local).strip() and str(local).strip().upper() != "TODOS":
        filters.append(
            f"AND {_cg_text_norm_expr(f'{pfx}local_nombre')} = {_cg_text_norm_expr(':local')}"
        )
        params["local"] = str(local).strip()

    return "\n".join(filters), params


def _cg_v2_selector_values(
    *,
    selector_name: str,
    column_expr: str,
    result_alias: str,
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
    rutero: str | None = None,
    local: str | None = None,
) -> list[str]:
    where_sql, params = _cg_v2_route_filters(
        alias="v",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
        rutero=rutero,
        local=local,
    )
    df = _selector_df(
        selector_name,
        f"""
        SELECT CAST({column_expr} AS TEXT) AS {result_alias}
        FROM {CG_V2_ROUTE_FREQ_RESUELTA_VIEW} v
        WHERE NULLIF(TRIM(COALESCE(CAST({column_expr} AS TEXT), '')), '') IS NOT NULL
        {where_sql}
        GROUP BY {column_expr}
        ORDER BY {result_alias}
        """,
        params or None,
    )
    return df[result_alias].astype(str).tolist() if df is not None and not df.empty else []


def get_cg_v2_gestores(
    semana_inicio: str | None = None,
) -> list[str]:
    where_sql, params = _cg_v2_route_filters(
        alias="v",
        semana_inicio=semana_inicio,
    )
    df = _selector_df(
        "get_cg_v2_gestores",
        f"""
        SELECT DISTINCT CAST(v.gestor AS TEXT) AS gestor
        FROM {CG_V2_ROUTE_FREQ_RESUELTA_VIEW} v
        WHERE NULLIF(TRIM(COALESCE(CAST(v.gestor AS TEXT), '')), '') IS NOT NULL
        {where_sql}
        ORDER BY gestor
        """,
        params or None,
    )
    if df is None or df.empty or "gestor" not in df.columns:
        return []
    return _cg_pipe_token_values(df["gestor"].dropna().astype(str).tolist())


def get_cg_v2_clientes(
    semana_inicio: str | None = None,
    gestor: str | None = None,
    rutero: str | None = None,
    local: str | None = None,
) -> list[str]:
    return _cg_v2_selector_values(
        selector_name="get_cg_v2_clientes",
        column_expr="cliente",
        result_alias="cliente",
        semana_inicio=semana_inicio,
        gestor=gestor,
        rutero=rutero,
        local=local,
    )


def get_cg_v2_alertas(
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    rutero: str | None = None,
    local: str | None = None,
) -> list[str]:
    # ALERTA en CONTROL_GESTION v2 es un dominio estable y no necesita roundtrip a DB.
    return ["CUMPLE", "INCUMPLE"]


def get_cg_v2_filter_options(
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    rutero: str | None = None,
    local: str | None = None,
) -> dict[str, list[str]]:
    return {
        "gestores": get_cg_v2_gestores(semana_inicio=semana_inicio),
        "clientes": get_cg_v2_clientes(
            semana_inicio=semana_inicio,
            gestor=gestor,
            rutero=rutero,
            local=local,
        ),
        "alertas": get_cg_v2_alertas(
            semana_inicio=semana_inicio,
            gestor=gestor,
            cliente=cliente,
            rutero=rutero,
            local=local,
        ),
    }


def get_cg_v2_ruteros(
    semana_inicio: str | None = None,
    gestor: str | None = None,
) -> list[str]:
    where_sql, params = _cg_v2_route_filters(
        alias="v",
        semana_inicio=semana_inicio,
        gestor=gestor,
    )
    df = _selector_df(
        "get_cg_v2_ruteros",
        f"""
        SELECT DISTINCT CAST(v.rutero AS TEXT) AS rutero
        FROM {CG_V2_ROUTE_FREQ_RESUELTA_VIEW} v
        WHERE NULLIF(TRIM(COALESCE(CAST(v.rutero AS TEXT), '')), '') IS NOT NULL
        {where_sql}
        ORDER BY rutero
        """,
        params or None,
    )
    if df is None or df.empty or "rutero" not in df.columns:
        return []
    raw_ruteros = df["rutero"].dropna().astype(str).tolist()
    anchor_ruteros = [
        raw_text.strip()
        for raw_text in raw_ruteros
        if raw_text and "|" not in raw_text
    ]
    if anchor_ruteros:
        return _cg_pipe_token_values(anchor_ruteros)
    # Fallback controlado: si el universo no expone ruteros standalone,
    # usamos atomización completa para no dejar el selector vacío.
    return _cg_pipe_token_values(raw_ruteros)


def get_cg_v2_locales(
    semana_inicio: str | None = None,
    gestor: str | None = None,
    rutero: str | None = None,
) -> list[str]:
    return _cg_v2_selector_values(
        selector_name="get_cg_v2_locales",
        column_expr="local_nombre",
        result_alias="local",
        semana_inicio=semana_inicio,
        gestor=gestor,
        rutero=rutero,
    )


def get_cg_v2_scope_kpis(
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
    rutero: str | None = None,
    local: str | None = None,
    search: str = "",
) -> pd.DataFrame:
    mv_mode = _cg_v2_out_weekly_is_mv()
    where_sql, params = _cg_v2_out_weekly_filters(
        alias="v",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
        alerta=alerta,
        rutero=rutero,
        local=local,
    )
    search_sql, search_params = _cg_v2_scope_search_filters(alias="v", search=search)
    query_params = {**params, **search_params}
    alerta_norm_expr = '"ALERTA_NORM_FILTER"' if mv_mode else _cg_text_norm_expr('"ALERTA"')
    visitas_pendientes_expr = (
        'COALESCE(SUM(COALESCE("VISITAS_PENDIENTES_CALC", 0)), 0)::int AS visitas_pendientes'
        if mv_mode
        else 'COALESCE(SUM(GREATEST(COALESCE("VISITA", 0) - COALESCE("VISITA_REALIZADA_CAP", 0), 0)), 0)::int AS visitas_pendientes'
    )
    gestion_compartida_expr = (
        'COALESCE(SUM(COALESCE("GESTION_COMPARTIDA_FLAG_CALC", 0)), 0)::int AS gestion_compartida_rows'
        if mv_mode
        else """
            COALESCE(SUM(CASE
                WHEN COALESCE("RUTA_DUPLICADA_FLAG", 0) = 1
                  OR COALESCE("RUTA_DUPLICADA_ROWS", 0) > 1
                  OR CAST("GESTOR" AS TEXT) LIKE '%|%'
                  OR CAST("RUTERO" AS TEXT) LIKE '%|%'
                THEN 1
                ELSE 0
            END), 0)::int AS gestion_compartida_rows
        """
    )
    sql = f"""
        SELECT
            COUNT(*)::int AS total_rows,
            COALESCE(SUM(COALESCE("VISITA", 0)), 0)::int AS visita_plan,
            COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::int AS visita_realizada_raw,
            COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::int AS visita_realizada_cap,
            {visitas_pendientes_expr},
            COALESCE(SUM(COALESCE("SOBRE_CUMPLIMIENTO", 0)), 0)::int AS sobre_cumplimiento,
            COALESCE(SUM(CASE
                WHEN {alerta_norm_expr} = 'CUMPLE' THEN 1
                ELSE 0
            END), 0)::int AS cumple_rows,
            COALESCE(SUM(CASE
                WHEN {alerta_norm_expr} = 'INCUMPLE' THEN 1
                ELSE 0
            END), 0)::int AS incumple_rows,
            {gestion_compartida_expr}
        FROM {CG_V2_OUT_WEEKLY_VIEW} v
        WHERE 1=1
        {where_sql}
        {search_sql}
    """
    return _selector_df("get_cg_v2_scope_kpis", sql, query_params or None)


def get_cg_v2_daily_matrix_page(
    semana_inicio: str | None = None,
    gestor: str | None = None,
    vista: str = "RUTERO",
    rutero: str | None = None,
    local: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
    search: str = "",
    page: int = 1,
    page_size: int = 50,
) -> pd.DataFrame:
    vista_key = str(vista or "RUTERO").strip().upper()
    if vista_key not in {"RUTERO", "LOCAL", "CLIENTE"}:
        vista_key = "RUTERO"

    mv_mode = _cg_v2_out_weekly_is_mv()
    where_sql, params = _cg_v2_out_weekly_filters(
        alias="v",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
        alerta=alerta,
        rutero=rutero,
        local=local,
    )
    search_sql, search_params = _cg_v2_scope_search_filters(alias="v", search=search)
    query_params = {**params, **search_params}
    visitas_pendientes_expr = (
        'COALESCE("VISITAS_PENDIENTES_CALC", 0)::int AS "VISITAS_PENDIENTES"'
        if mv_mode
        else 'GREATEST(COALESCE("VISITA", 0) - COALESCE("VISITA_REALIZADA_CAP", 0), 0)::int AS "VISITAS_PENDIENTES"'
    )
    gestion_compartida_expr = (
        """
        CASE
            WHEN COALESCE("GESTION_COMPARTIDA_FLAG_CALC", 0) = 1
            THEN 'Si | ' || COALESCE(NULLIF(CAST("GESTOR" AS TEXT), ''), 'Compartida')
            ELSE 'No'
        END AS "GESTION_COMPARTIDA"
        """
        if mv_mode
        else """
        CASE
            WHEN COALESCE("RUTA_DUPLICADA_FLAG", 0) = 1
              OR COALESCE("RUTA_DUPLICADA_ROWS", 0) > 1
              OR CAST("GESTOR" AS TEXT) LIKE '%|%'
              OR CAST("RUTERO" AS TEXT) LIKE '%|%'
            THEN 'Si | ' || COALESCE(NULLIF(CAST("GESTOR" AS TEXT), ''), 'Compartida')
            ELSE 'No'
        END AS "GESTION_COMPARTIDA"
        """
    )
    select_sql = f"""
        "SEMANA_INICIO",
        "SEMANA_ISO",
        "COD_RT",
        "COD_B2B",
        "LOCAL",
        "CLIENTE",
        "GESTOR",
        "RUTERO",
        "REPONEDOR",
        "SUPERVISOR",
        "MODALIDAD",
        "VISITA",
        "VISITA_REALIZADA",
        "VISITA_REALIZADA_RAW",
        "VISITA_REALIZADA_CAP",
        {visitas_pendientes_expr},
        "SOBRE_CUMPLIMIENTO",
        "ALERTA",
        "RUTA_DUPLICADA_FLAG",
        "RUTA_DUPLICADA_ROWS",
        {gestion_compartida_expr},
        "LUNES_PLAN",
        "MARTES_PLAN",
        "MIERCOLES_PLAN",
        "JUEVES_PLAN",
        "VIERNES_PLAN",
        "SABADO_PLAN",
        "DOMINGO_PLAN",
        "LUNES_FLAG",
        "MARTES_FLAG",
        "MIERCOLES_FLAG",
        "JUEVES_FLAG",
        "VIERNES_FLAG",
        "SABADO_FLAG",
        "DOMINGO_FLAG",
        {_cg_v2_status_case('"LUNES_PLAN"', '"LUNES_FLAG"')} AS "LUNES_STATUS",
        {_cg_v2_status_case('"MARTES_PLAN"', '"MARTES_FLAG"')} AS "MARTES_STATUS",
        {_cg_v2_status_case('"MIERCOLES_PLAN"', '"MIERCOLES_FLAG"')} AS "MIERCOLES_STATUS",
        {_cg_v2_status_case('"JUEVES_PLAN"', '"JUEVES_FLAG"')} AS "JUEVES_STATUS",
        {_cg_v2_status_case('"VIERNES_PLAN"', '"VIERNES_FLAG"')} AS "VIERNES_STATUS",
        {_cg_v2_status_case('"SABADO_PLAN"', '"SABADO_FLAG"')} AS "SABADO_STATUS",
        {_cg_v2_status_case('"DOMINGO_PLAN"', '"DOMINGO_FLAG"')} AS "DOMINGO_STATUS",
        "DIAS_KPIONE",
        "DIAS_KPIONE2",
        "DIAS_POWER_APP",
        "DIAS_DOBLE_MARCAJE",
        "DIAS_TRIPLE_MARCAJE",
        "FUENTES_REPORTADAS_SEMANA",
        "PERSONA_CONFLICTO_ROWS"
    """
    page_num = max(1, int(page or 1))
    size = max(1, int(page_size or 50))
    combined_where = "\n".join(part for part in (where_sql, search_sql) if part)
    return _cg_select_page_filtered(
        selector_name="get_cg_v2_daily_matrix_page",
        view_name=CG_V2_OUT_WEEKLY_VIEW,
        where_sql=combined_where,
        order_sql=_cg_v2_daily_matrix_order_sql(vista_key),
        params=query_params,
        limit=size,
        offset=(page_num - 1) * size,
        from_alias="v",
        select_sql=select_sql,
    )


def get_cg_v2_daily_matrix_full(
    semana_inicio: str | None = None,
    gestor: str | None = None,
    vista: str = "RUTERO",
    rutero: str | None = None,
    local: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
) -> pd.DataFrame:
    vista_key = str(vista or "RUTERO").strip().upper()
    if vista_key not in {"RUTERO", "LOCAL", "CLIENTE"}:
        vista_key = "RUTERO"

    mv_mode = _cg_v2_out_weekly_is_mv()
    where_sql, params = _cg_v2_out_weekly_filters(
        alias="v",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
        alerta=alerta,
        rutero=rutero,
        local=local,
    )
    base_derived_cols = """
                UPPER(TRIM(COALESCE("ALERTA", ''))) AS "ALERTA_NORM_FILTER",
                CASE
                    WHEN COALESCE("RUTA_DUPLICADA_FLAG", 0) = 1
                      OR COALESCE("RUTA_DUPLICADA_ROWS", 0) > 1
                      OR CAST("GESTOR" AS TEXT) LIKE '%|%'
                      OR CAST("RUTERO" AS TEXT) LIKE '%|%'
                    THEN 1
                    ELSE 0
                END::int AS "GESTION_COMPARTIDA_FLAG_CALC",
                GREATEST(COALESCE("VISITA", 0) - COALESCE("VISITA_REALIZADA_CAP", 0), 0)::int AS "VISITAS_PENDIENTES_CALC",
    """
    if mv_mode:
        base_derived_cols = """
                COALESCE("ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE("ALERTA", '')))) AS "ALERTA_NORM_FILTER",
                COALESCE("GESTION_COMPARTIDA_FLAG_CALC", 0)::int AS "GESTION_COMPARTIDA_FLAG_CALC",
                COALESCE("VISITAS_PENDIENTES_CALC", 0)::int AS "VISITAS_PENDIENTES_CALC",
        """
    df = _selector_df(
        "get_cg_v2_daily_matrix_full",
        f"""
        WITH base AS (
            SELECT
                "SEMANA_INICIO",
                "GESTOR",
                "RUTERO",
                "REPONEDOR",
                "COD_RT",
                "LOCAL",
                "CLIENTE",
                "MODALIDAD",
                "VISITA",
                "VISITA_REALIZADA_CAP",
                "ALERTA",
                "RUTA_DUPLICADA_FLAG",
                "RUTA_DUPLICADA_ROWS",
                {base_derived_cols}
                "LUNES_PLAN",
                "LUNES_FLAG",
                "MARTES_PLAN",
                "MARTES_FLAG",
                "MIERCOLES_PLAN",
                "MIERCOLES_FLAG",
                "JUEVES_PLAN",
                "JUEVES_FLAG",
                "VIERNES_PLAN",
                "VIERNES_FLAG",
                "SABADO_PLAN",
                "SABADO_FLAG",
                "DOMINGO_PLAN",
                "DOMINGO_FLAG"
            FROM {CG_V2_OUT_WEEKLY_VIEW} v
            WHERE 1=1
            {where_sql}
        )
        SELECT
            "SEMANA_INICIO",
            MAX(COALESCE("GESTOR", '')) AS "GESTOR",
            MAX(COALESCE("RUTERO", '')) AS "RUTERO",
            MAX(COALESCE("REPONEDOR", '')) AS "REPONEDOR",
            "COD_RT",
            "LOCAL",
            "CLIENTE",
            "MODALIDAD",
            MAX(COALESCE("VISITA", 0))::int AS "VISITA",
            MAX(COALESCE("VISITAS_PENDIENTES_CALC", 0))::int AS "VISITAS_PENDIENTES",
            CASE
                WHEN SUM(CASE WHEN "ALERTA_NORM_FILTER" = 'INCUMPLE' THEN 1 ELSE 0 END) > 0
                THEN 'INCUMPLE'
                ELSE 'CUMPLE'
            END AS "ALERTA",
            CASE
                WHEN MAX(COALESCE("GESTION_COMPARTIDA_FLAG_CALC", 0)) = 1
                THEN 'Si | ' || COALESCE(
                    NULLIF(MAX(CASE
                        WHEN POSITION('|' IN COALESCE("GESTOR", '')) > 0
                        THEN CAST("GESTOR" AS TEXT)
                        ELSE ''
                    END), ''),
                    COALESCE(NULLIF(MAX(CAST("GESTOR" AS TEXT)), ''), 'Compartida')
                )
                ELSE 'No'
            END AS "GESTION_COMPARTIDA",
            COALESCE(
                NULLIF(STRING_AGG(DISTINCT NULLIF(CAST("RUTERO" AS TEXT), ''), ' || '), ''),
                MAX(COALESCE("RUTERO", ''))
            ) AS "RUTERO_SHARED_RAW",
            {_cg_v2_checklist_case('MAX(COALESCE("LUNES_PLAN", 0))', 'MAX(COALESCE("LUNES_FLAG", 0))')} AS "LUN",
            {_cg_v2_checklist_case('MAX(COALESCE("MARTES_PLAN", 0))', 'MAX(COALESCE("MARTES_FLAG", 0))')} AS "MAR",
            {_cg_v2_checklist_case('MAX(COALESCE("MIERCOLES_PLAN", 0))', 'MAX(COALESCE("MIERCOLES_FLAG", 0))')} AS "MIE",
            {_cg_v2_checklist_case('MAX(COALESCE("JUEVES_PLAN", 0))', 'MAX(COALESCE("JUEVES_FLAG", 0))')} AS "JUE",
            {_cg_v2_checklist_case('MAX(COALESCE("VIERNES_PLAN", 0))', 'MAX(COALESCE("VIERNES_FLAG", 0))')} AS "VIE",
            {_cg_v2_checklist_case('MAX(COALESCE("SABADO_PLAN", 0))', 'MAX(COALESCE("SABADO_FLAG", 0))')} AS "SAB",
            {_cg_v2_checklist_case('MAX(COALESCE("DOMINGO_PLAN", 0))', 'MAX(COALESCE("DOMINGO_FLAG", 0))')} AS "DOM"
        FROM base
        GROUP BY
            "SEMANA_INICIO",
            "COD_RT",
            "LOCAL",
            "CLIENTE",
            "MODALIDAD"
        {_cg_v2_daily_matrix_order_sql(vista_key)}
        """,
        params or None,
    )
    if df is None or df.empty:
        return df
    if "RUTERO_SHARED_RAW" in df.columns:
        df["RUTA_COMPARTIDA"] = df["RUTERO_SHARED_RAW"].apply(
            lambda raw: _cg_v2_route_shared_display(raw, selected_rutero=rutero)
        )
        df.drop(columns=["RUTERO_SHARED_RAW"], inplace=True)
    else:
        df["RUTA_COMPARTIDA"] = "No"
    return df
def get_cg_v2_daily_evidence(
    semana_inicio: str | None = None,
    cod_rt: str | None = None,
    cliente: str | None = None,
    gestor: str | None = None,
    rutero: str | None = None,
) -> pd.DataFrame:
    where_sql, params = _cg_v2_daily_evidence_filters(
        alias="d",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
        rutero=rutero,
        cod_rt=cod_rt,
    )
    return _selector_df(
        "get_cg_v2_daily_evidence",
        f"""
        SELECT
            cod_rt,
            cod_b2b,
            cliente,
            cliente_norm,
            local_nombre,
            gestor,
            gestor_norm,
            rutero,
            reponedor_scope,
            reponedor_scope_norm,
            supervisor,
            jefe_operaciones,
            modalidad,
            fecha_visita,
            EXTRACT(ISODOW FROM fecha_visita)::int AS dia_semana_iso,
            semana_inicio,
            semana_iso,
            visita_valida_dia,
            kpione_mark,
            kpione2_mark,
            power_app_mark,
            fuentes_reportadas_count,
            fuentes_reportadas_label,
            doble_marcaje_dia,
            triple_marcaje_dia,
            kpione_rows_dia,
            kpione2_rows_dia,
            power_app_rows_dia,
            persona_conflicto_rows_dia
        FROM {CG_V2_DAILY_EVIDENCE_VIEW} d
        WHERE 1=1
        {where_sql}
        ORDER BY
            semana_inicio DESC,
            fecha_visita DESC,
            gestor ASC,
            rutero ASC,
            cliente ASC,
            cod_rt ASC
        """,
        params or None,
    )


def get_cg_v2_scope_page(
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
    search: str = "",
    page: int = 1,
    page_size: int = 50,
) -> pd.DataFrame:
    where_sql, params = _cg_scope_filters(
        alias="v",
        semana_inicio=semana_inicio,
        gestor=gestor,
        cliente=cliente,
        alerta=alerta,
    )
    search_sql, search_params = _cg_v2_scope_search_filters(alias="v", search=search)
    query_params = {**params, **search_params}
    order_sql = """
        ORDER BY
            "SEMANA_INICIO" DESC,
            CAST("LOCAL" AS TEXT) ASC,
            CAST("CLIENTE" AS TEXT) ASC,
            CAST("RUTERO" AS TEXT) ASC,
            CAST("REPONEDOR" AS TEXT) ASC
    """
    select_sql = """
        "SEMANA_INICIO",
        "COD_RT",
        "COD_B2B",
        "LOCAL",
        "CLIENTE",
        "GESTOR",
        "RUTERO",
        "REPONEDOR",
        "SUPERVISOR",
        "MODALIDAD",
        "VISITA",
        "VISITA_REALIZADA",
        "VISITA_REALIZADA_RAW",
        "VISITA_REALIZADA_CAP",
        "SOBRE_CUMPLIMIENTO",
        "DIFERENCIA",
        "ALERTA",
        "DIAS_KPIONE",
        "DIAS_KPIONE2",
        "DIAS_POWER_APP",
        "DIAS_DOBLE_MARCAJE",
        "DIAS_TRIPLE_MARCAJE",
        "FUENTES_REPORTADAS_SEMANA",
        "PERSONA_CONFLICTO_ROWS",
        "RUTA_DUPLICADA_FLAG",
        "RUTA_DUPLICADA_ROWS"
    """
    page_num = max(1, int(page or 1))
    size = max(1, int(page_size or 50))
    combined_where = "\n".join(part for part in (where_sql, search_sql) if part)
    return _cg_select_page_filtered(
        selector_name="get_cg_v2_scope_page",
        view_name=CG_V2_SCOPE_VIEW,
        where_sql=combined_where,
        order_sql=order_sql,
        params=query_params,
        limit=size,
        offset=(page_num - 1) * size,
        from_alias="v",
        select_sql=select_sql,
    )


def get_cg_v2_audit_summary() -> dict[str, Any]:
    df = qdf(
        f"""
        SELECT
            (SELECT COUNT(*)::int FROM {CG_V2_RUTA_DUP_VIEW}) AS ruta_duplicada_rows,
            (SELECT COALESCE(SUM(rows), 0)::int FROM {CG_V2_RUTA_DUP_VIEW}) AS ruta_duplicada_source_rows,
            (SELECT COUNT(*)::int FROM {CG_V2_FUERA_CRUCE_REAL_VIEW}) AS fuera_cruce_real_rows,
            (SELECT COUNT(*)::int FROM {CG_V2_SIN_BATCH_RUTA_VIEW}) AS sin_batch_ruta_semana_rows,
            (
                SELECT COUNT(*)::int
                FROM {CG_V2_MULTI_MARCAJE_VIEW}
                WHERE doble_marcaje_dia = 1 OR triple_marcaje_dia = 1
            ) AS doble_triple_rows,
            (
                SELECT COUNT(*)::int
                FROM {CG_V2_MULTI_MARCAJE_VIEW}
                WHERE doble_marcaje_dia = 1
            ) AS doble_rows,
            (
                SELECT COUNT(*)::int
                FROM {CG_V2_MULTI_MARCAJE_VIEW}
                WHERE triple_marcaje_dia = 1
            ) AS triple_rows
        """
    )
    if df is None or df.empty:
        return {}
    return df.iloc[0].to_dict()

