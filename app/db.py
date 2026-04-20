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


def get_kpis_local_home(cod_rt: str, marcas: list[str] | None = None) -> pd.DataFrame:
    where_extra, p2 = _build_result_filters(marcas, search="", foco="Todo", alias="v")
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
    """, {"cod_rt": cod_rt, **p2})


def get_kpis_local(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str] | None = None,
    modalidad: str | None = None,
) -> pd.DataFrame:
    exists_sql, extra = _rr_scope_exists("v", modalidad=modalidad)
    where_extra, p2 = _build_result_filters(marcas, search="", foco="Todo", alias="v")
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
    """, {
        "rutero": rutero,
        "reponedor": reponedor,
        "cod_rt": cod_rt,
        **extra,
        **p2,
    })


def get_tabla_ux_total_home(
    cod_rt: str,
    marcas: list[str] | None = None,
    foco: str = "Todo",
    search: str = "",
) -> int:
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    df = qdf(f"""
        SELECT COUNT(*)::int AS total
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
        {where_extra}
    """, {"cod_rt": cod_rt, **p2})
    return int(df.iloc[0]["total"]) if df is not None and not df.empty else 0


def get_tabla_ux_page_home(
    cod_rt: str,
    marcas: list[str] | None = None,
    page: int = 1,
    page_size: int = 25,
    foco: str = "Todo",
    search: str = "",
) -> pd.DataFrame:
    page = max(int(page or 1), 1)
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    return qdf(f"""
        SELECT
          v.fecha,
          v."MARCA", v."Sku", v."Descripción del Producto",
          v."Stock", v."Venta(+7)", v."NEGATIVO", v."RIESGO DE QUIEBRE", v."OTROS"
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
        {where_extra}
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
    })


@st.cache_data(ttl=QDF_TTL, show_spinner=False)
def get_tabla_ux_export_home(
    cod_rt: str,
    marcas: list[str] | None = None,
    foco: str = "Todo",
    search: str = "",
) -> pd.DataFrame:
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    return qdf(f"""
        SELECT
          v.fecha,
          v."MARCA", v."Sku", v."Descripción del Producto",
          v."Stock", v."Venta(+7)", v."NEGATIVO", v."RIESGO DE QUIEBRE", v."OTROS"
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
        {where_extra}
        ORDER BY
          v."MARCA" ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
          CASE WHEN v."Sku" ~ '^[0-9]+$' THEN (v."Sku")::bigint END ASC NULLS LAST,
          v."Sku" ASC,
          v."Descripción del Producto" ASC
    """, {"cod_rt": cod_rt, **p2})


def get_tabla_ux_total(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str] | None = None,
    foco: str = "Todo",
    search: str = "",
    modalidad: str | None = None,
) -> int:
    exists_sql, extra = _rr_scope_exists("v", modalidad=modalidad)
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    df = qdf(f"""
        SELECT COUNT(*)::int AS total
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
          AND {exists_sql}
          {where_extra}
    """, {
        "rutero": rutero,
        "reponedor": reponedor,
        "cod_rt": cod_rt,
        **extra,
        **p2,
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
) -> pd.DataFrame:
    page = max(int(page or 1), 1)
    exists_sql, extra = _rr_scope_exists("v", modalidad=modalidad)
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    return qdf(f"""
        SELECT
          v.fecha,
          v."MARCA", v."Sku", v."Descripción del Producto",
          v."Stock", v."Venta(+7)", v."NEGATIVO", v."RIESGO DE QUIEBRE", v."OTROS"
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
          AND {exists_sql}
          {where_extra}
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
) -> pd.DataFrame:
    exists_sql, extra = _rr_scope_exists("v", modalidad=modalidad)
    where_extra, p2 = _build_result_filters(marcas, search, foco, alias="v")
    return qdf(f"""
        SELECT
          v.fecha,
          v."MARCA", v."Sku", v."Descripción del Producto",
          v."Stock", v."Venta(+7)", v."NEGATIVO", v."RIESGO DE QUIEBRE", v."OTROS"
        FROM {RESULT_VIEW} v
        WHERE v.cod_rt = :cod_rt
          AND {exists_sql}
          {where_extra}
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
    where_extra, params = _scope_cliente_filters(
        alias="rr",
        marca=marca,
        apply_marca_intersection=True,
        cliente_field="cliente_norm",
        responsable_field="responsable_norm",
        responsable_tipo_field="responsable_tipo",
    )
    sql = f"""
        SELECT DISTINCT
            COALESCE(NULLIF(TRIM(rr.cliente), ''), TRIM(rr.cliente_norm)) AS cliente
        FROM {SCOPE_RR_DISTINCT_VIEW} rr
        WHERE NULLIF(TRIM(COALESCE(rr.cliente_norm, rr.cliente, '')), '') IS NOT NULL
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
    where_summary, params_summary = _scope_summary_filters(
        alias="s",
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        focos=focos,
        search=search,
    )

    where_detail, params_detail = _scope_detail_filters(
        alias="b",
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        focos=focos,
        search=search,
    )

    params = {**params_summary, **params_detail}

    sql = f"""
        WITH kpi AS (
            SELECT
                COUNT(DISTINCT s.cod_rt)::int AS locales_scope,
                COUNT(DISTINCT s.cliente_norm)::int AS clientes_scope,
                COUNT(DISTINCT s.responsable_norm)::int AS responsables_scope,
                COALESCE(SUM(s.skus_scope), 0)::int AS total_skus,
                COALESCE(SUM(s.venta_0), 0)::int AS venta_0,
                COALESCE(SUM(s.negativos), 0)::int AS negativos,
                COALESCE(SUM(s.quiebres), 0)::int AS quiebres,
                COALESCE(SUM(s.otros), 0)::int AS otros
            FROM {SCOPE_SUMMARY_VIEW} s
            WHERE 1=1
            {where_summary}
        ),
        fecha AS (
            SELECT
                MAX(b.fecha) AS fecha_stock
            FROM {SCOPE_FACT_BRIDGE_VIEW} b
            WHERE 1=1
            {where_detail}
        )
        SELECT
            fecha.fecha_stock,
            kpi.locales_scope,
            kpi.clientes_scope,
            kpi.responsables_scope,
            kpi.total_skus,
            kpi.venta_0,
            kpi.negativos,
            kpi.quiebres,
            kpi.otros
        FROM kpi
        CROSS JOIN fecha
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
    where_extra, params = _scope_summary_filters(
        alias="s",
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        focos=focos,
        search=search,
    )
    page_sql = _scope_page_clause(limit, offset, params)

    sql = f"""
        WITH base AS (
            SELECT
                COALESCE(NULLIF(TRIM(s.responsable_tipo), ''), '-') AS responsable_tipo,
                COALESCE(NULLIF(TRIM(s.responsable), ''), TRIM(s.responsable_norm)) AS responsable,
                COUNT(DISTINCT s.cliente_norm)::int AS clientes,
                COUNT(DISTINCT s.cod_rt)::int AS locales,
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
    where_extra, params = _scope_summary_filters(
        alias="s",
        marca=marca,
        cliente=cliente,
        responsable_tipo=responsable_tipo,
        responsable=responsable,
        focos=focos,
        search=search,
    )
    page_sql = _scope_page_clause(limit, offset, params)

    sql = f"""
        WITH base AS (
            SELECT
                COALESCE(NULLIF(TRIM(s.cliente), ''), TRIM(s.cliente_norm)) AS cliente,
                COUNT(DISTINCT s.responsable_norm)::int AS responsables,
                COUNT(DISTINCT s.cod_rt)::int AS locales,
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
            GROUP BY 1
        )
        SELECT
            base.*,
            COUNT(*) OVER()::int AS total_rows
        FROM base
        ORDER BY skus_en_foco DESC, negativos DESC, venta_0 DESC, otros DESC, quiebres DESC, cliente ASC
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
    cliente: str,
) -> pd.DataFrame:
    cliente_sel = str(cliente or "").strip()
    if not cliente_sel:
        return pd.DataFrame(
            columns=[
                "fecha",
                "COD_RT",
                "LOCAL",
                "CLIENTE",
                "RUTERO",
                "REPONEDOR",
                "MARCA",
                "Sku",
                "Descripción del Producto",
                "Stock",
                "Venta(+7)",
                "NEGATIVO",
                "RIESGO DE QUIEBRE",
                "OTROS",
            ]
        )

    sql = f"""
        WITH rr_ctx AS (
            SELECT
                rr.cod_rt,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(rr.cliente_norm), ''), NULLIF(TRIM(rr.cliente), ''), ''))) AS cliente_norm_match,
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
            WHERE {_scope_norm_expr("COALESCE(NULLIF(TRIM(rr.cliente_norm), ''), NULLIF(TRIM(rr.cliente), ''), '')")} = {_scope_norm_expr(':cliente')}
            GROUP BY 1, 2
        ),
        base_fact AS (
            SELECT
                b.fecha,
                b.cod_rt,
                COALESCE(NULLIF(TRIM(b.local_nombre_rr), ''), CAST(b.cod_rt AS TEXT)) AS local_nombre_rr,
                COALESCE(NULLIF(TRIM(b.cliente), ''), '') AS cliente,
                COALESCE(NULLIF(TRIM(b.marca), ''), '') AS marca,
                CAST(b.sku AS TEXT) AS sku,
                COALESCE(b.producto, '') AS producto,
                COALESCE(b.stock, 0)::int AS stock,
                COALESCE(b.venta_7, 0)::int AS venta_7,
                COALESCE(b.negativo, '') AS negativo,
                COALESCE(b.riesgo_quiebre, '') AS riesgo_quiebre,
                COALESCE(b.otros, '') AS otros,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        b.cod_rt,
                        {_scope_norm_expr('b.cliente')},
                        {_scope_norm_expr('b.marca')},
                        CAST(b.sku AS TEXT)
                    ORDER BY
                        b.fecha DESC,
                        COALESCE(NULLIF(TRIM(b.local_nombre_rr), ''), CAST(b.cod_rt AS TEXT)) ASC,
                        COALESCE(b.producto, '') ASC,
                        COALESCE(NULLIF(TRIM(b.responsable_tipo), ''), '-') ASC,
                        COALESCE(NULLIF(TRIM(b.responsable), ''), '') ASC
                ) AS rn_fact
            FROM {SCOPE_FACT_BRIDGE_VIEW} b
            WHERE {_scope_norm_expr('b.cliente')} = {_scope_norm_expr(':cliente')}
        ),
        fact_latest AS (
            SELECT
                fecha,
                cod_rt,
                local_nombre_rr,
                cliente,
                marca,
                sku,
                producto,
                stock,
                venta_7,
                negativo,
                riesgo_quiebre,
                otros
            FROM base_fact
            WHERE rn_fact = 1
        )
        SELECT
            f.fecha,
            CAST(f.cod_rt AS TEXT) AS "COD_RT",
            f.local_nombre_rr AS "LOCAL",
            f.cliente AS "CLIENTE",
            COALESCE(rr_ctx.rutero, '-') AS "RUTERO",
            COALESCE(rr_ctx.reponedor, '-') AS "REPONEDOR",
            f.marca AS "MARCA",
            f.sku AS "Sku",
            f.producto AS "Descripción del Producto",
            f.stock AS "Stock",
            f.venta_7 AS "Venta(+7)",
            f.negativo AS "NEGATIVO",
            f.riesgo_quiebre AS "RIESGO DE QUIEBRE",
            f.otros AS "OTROS"
        FROM fact_latest f
        LEFT JOIN rr_ctx
          ON rr_ctx.cod_rt = f.cod_rt
         AND rr_ctx.cliente_norm_match = {_scope_norm_expr('f.cliente')}
        ORDER BY
            f.local_nombre_rr ASC,
            f.marca ASC,
            CASE WHEN f.sku ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
            CASE WHEN f.sku ~ '^[0-9]+$' THEN CAST(f.sku AS BIGINT) END ASC NULLS LAST,
            f.sku ASC,
            f.producto ASC
    """
    return qdf(sql, {"cliente": cliente_sel})



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


def _cg_scope_filters(
    *,
    alias: str = "v",
    semana_inicio: str | None = None,
    gestor: str | None = None,
    cliente: str | None = None,
    alerta: str | None = None,
    cod_rt: str | None = None,
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

    return "\n".join(filters), params


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
