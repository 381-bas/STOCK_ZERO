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


# =========================================================
# 5) AUDITORÍA DE COBERTURA E INCONSISTENCIAS
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
