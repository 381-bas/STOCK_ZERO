# app/db.py
from __future__ import annotations

from pathlib import Path
import os
import logging
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# (probe liviano para elegir primary/fallback sin castigar cada query)
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


class AppError(RuntimeError):
    pass


def _get_db_urls() -> tuple[str, str | None]:
    """
    APP: usa DB_URL_APP si existe (ideal RO). Si no, cae a DB_URL.
    Fallback opcional: DB_URL_FALLBACK
    """
    primary = (os.getenv("DB_URL_APP", "") or os.getenv("DB_URL", "")).strip()
    fallback = os.getenv("DB_URL_FALLBACK", "").strip() or None
    if not primary and not fallback:
        raise AppError(
            "Configuración incompleta: falta DB_URL_APP/DB_URL.\n"
            "Solución: agrega en tu .env una línea:\n"
            "DB_URL_APP=postgresql://USER:PASS@HOST:PORT/DB"
        )
    return primary, fallback


def _probe_pg(url: str) -> bool:
    """
    Probe cada ~30s (cacheado) para elegir primary/fallback.
    Si psycopg2 no está disponible, asumimos primary.
    """
    if psycopg2 is None:
        return True
    timeout = int(os.getenv("CONNECT_TIMEOUT", "3"))
    try:
        with psycopg2.connect(url, connect_timeout=timeout) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
        return True
    except Exception:
        return False


@st.cache_data(ttl=30, show_spinner=False)
def get_active_db_url() -> str:
    primary, fallback = _get_db_urls()
    if primary and _probe_pg(primary):
        return primary
    if fallback and _probe_pg(fallback):
        logger.warning("Usando DB_URL_FALLBACK (primary no responde).")
        return fallback
    return primary or (fallback or "")


@st.cache_resource(show_spinner=False)
def _engine_cached(db_url: str) -> Engine:
    """
    Pool para concurrencia (30–50 users). Ajustable por .env
    """
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
    logger.info("Engine listo (pool_size=%s, max_overflow=%s)", pool_size, max_overflow)
    return eng


def get_engine() -> Engine:
    return _engine_cached(get_active_db_url())


@st.cache_data(ttl=DV_TTL, show_spinner=False)
def get_data_version_info() -> dict[str, Any]:
    """
    Devuelve:
      - fecha_datos (negocio)
      - ingested_at (carga real) => ideal para invalidar caché
    Requiere vista: public.v_data_version
    """
    eng = get_engine()
    with eng.connect() as conn:
        try:
            df = pd.read_sql(text("SELECT fecha_datos, ingested_at FROM public.v_data_version;"), conn)
            if not df.empty:
                return {
                    "fecha_datos": df.iloc[0].get("fecha_datos"),
                    "ingested_at": df.iloc[0].get("ingested_at"),
                }
        except Exception as e:
            logger.warning("No pude leer v_data_version: %s", e)

        try:
            df2 = pd.read_sql(text("SELECT MAX(fecha_datos) AS fecha_datos FROM public.v_local_context_latest;"), conn)
            fd = df2.iloc[0].get("fecha_datos") if not df2.empty else None
            return {"fecha_datos": fd, "ingested_at": None}
        except Exception as e:
            logger.warning("No pude leer v_local_context_latest: %s", e)

    return {"fecha_datos": None, "ingested_at": None}


@st.cache_data(ttl=300, show_spinner=False)
def get_data_version() -> str:
    """
    Versión de datos para invalidar caché.
    """
    eng = get_engine()
    candidates = [
        "SELECT MAX(ingested_at) AS dv FROM public.fact_stock_venta;",
        "SELECT MAX(fecha) AS dv FROM public.v_home_latest;",
        "SELECT MAX(fecha) AS dv FROM public.v_local_skus_ux;",
    ]
    with eng.connect() as conn:
        for sql in candidates:
            try:
                df = pd.read_sql(text(sql), conn)
                dv = df.iloc[0]["dv"]
                if dv is not None:
                    return str(dv)
            except Exception:
                continue
    return "NA"


@st.cache_data(ttl=QDF_TTL, show_spinner=False)
def _qdf_cached(data_version: str, sql: str, params: dict[str, Any] | None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def qdf(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    dv = get_data_version()
    return _qdf_cached(dv, sql, params)


def get_rutero_reponedor() -> pd.DataFrame:
    return qdf("""
        SELECT rutero, reponedor
        FROM public.v_selector_rutero_reponedor
        ORDER BY rutero, reponedor
    """)


def get_locales(rutero: str, reponedor: str) -> pd.DataFrame:
    return qdf("""
        SELECT cod_rt, nombre_local_rr
        FROM public.v_locales_por_ruta
        WHERE rutero = :rutero AND reponedor = :reponedor
        ORDER BY cod_rt
    """, {"rutero": rutero, "reponedor": reponedor})


def get_marcas(rutero: str, reponedor: str, cod_rt: str) -> list[str]:
    # Más liviano: v_local_skus_ux (ya está “aplanada” para UI)
    df = qdf("""
        SELECT DISTINCT "MARCA" AS marca
        FROM public.v_local_skus_ux
        WHERE rutero=:rutero AND reponedor=:reponedor AND cod_rt=:cod_rt
        ORDER BY marca
    """, {"rutero": rutero, "reponedor": reponedor, "cod_rt": cod_rt})
    return df["marca"].astype(str).tolist() if not df.empty else []


def get_contexto_local(rutero: str, reponedor: str, cod_rt: str) -> pd.DataFrame:
    return qdf("""
        SELECT rutero, reponedor, cod_rt, nombre_local_rr, fecha_datos
        FROM public.v_local_context_latest
        WHERE rutero=:rutero AND reponedor=:reponedor AND cod_rt=:cod_rt
        LIMIT 1
    """, {"rutero": rutero, "reponedor": reponedor, "cod_rt": cod_rt})


def get_kpis_local(rutero: str, reponedor: str, cod_rt: str, marcas: list[str]) -> pd.DataFrame:
    max_m = int(os.getenv("MAX_MARCA_FILTER", "50"))
    if marcas and len(marcas) <= max_m:
        return qdf("""
            SELECT
              COUNT(*) AS total_skus,
              SUM(CASE WHEN stock < 0 THEN 1 ELSE 0 END) AS negativos,
              SUM(CASE WHEN venta_7 > 0 AND stock > 0 AND stock < venta_7 THEN 1 ELSE 0 END) AS riesgo_quiebre,
              SUM(venta_7) AS venta_total_7,
              SUM(stock) AS stock_total
            FROM public.v_home_latest
            WHERE rutero=:rutero AND reponedor=:reponedor AND cod_rt=:cod_rt
              AND marca = ANY(:marcas)
        """, {"rutero": rutero, "reponedor": reponedor, "cod_rt": cod_rt, "marcas": marcas})

    return qdf("""
        SELECT total_skus, negativos, riesgo_quiebre, venta_total_7, stock_total
        FROM public.v_local_kpis
        WHERE rutero=:rutero AND reponedor=:reponedor AND cod_rt=:cod_rt
        LIMIT 1
    """, {"rutero": rutero, "reponedor": reponedor, "cod_rt": cod_rt})


def _build_ux_filters(marcas: list[str], search: str, only_negativos: bool, only_riesgo: bool) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {}
    filters: list[str] = []

    if marcas:
        filters.append('AND "MARCA" = ANY(:marcas)')
        params["marcas"] = marcas

    if only_negativos:
        filters.append('AND UPPER(COALESCE("NEGATIVO",\'NO\')) = \'SI\'')

    if only_riesgo:
        filters.append('AND UPPER(COALESCE("RIESGO DE QUIEBRE",\'NO\')) = \'SI\'')

    s = (search or "").strip()
    if len(s) >= 2:
        params["q"] = f"%{s}%"
        filters.append("""
            AND (
                CAST("Sku" AS TEXT) ILIKE :q
                OR "Descripción del Producto" ILIKE :q
                OR "MARCA" ILIKE :q
            )
        """)

    return "\n".join(filters), params


def get_tabla_ux_total(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str],
    search: str = "",
    only_negativos: bool = False,
    only_riesgo: bool = False,
) -> int:
    where_extra, p2 = _build_ux_filters(marcas, search, only_negativos, only_riesgo)
    params: dict[str, Any] = {"rutero": rutero, "reponedor": reponedor, "cod_rt": cod_rt, **p2}

    df = qdf(f"""
        SELECT COUNT(*)::int AS total
        FROM public.v_local_skus_ux
        WHERE rutero=:rutero AND reponedor=:reponedor AND cod_rt=:cod_rt
        {where_extra}
    """, params)

    return int(df.iloc[0]["total"]) if (df is not None and not df.empty) else 0


def get_tabla_ux_page(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str],
    page: int,
    page_size: int,
    search: str = "",
    only_negativos: bool = False,
    only_riesgo: bool = False,
) -> pd.DataFrame:
    page = int(page or 1)
    if page < 1:
        page = 1

    where_extra, p2 = _build_ux_filters(marcas, search, only_negativos, only_riesgo)
    params: dict[str, Any] = {
        "rutero": rutero,
        "reponedor": reponedor,
        "cod_rt": cod_rt,
        "limit": int(page_size),
        "offset": (page - 1) * int(page_size),
        **p2,
    }

    sql = f"""
    SELECT
      "MARCA","Sku","Descripción del Producto",
      "Stock","Venta(+7)","NEGATIVO","RIESGO DE QUIEBRE","OTROS"
    FROM public.v_local_skus_ux
    WHERE rutero=:rutero AND reponedor=:reponedor AND cod_rt=:cod_rt
    {where_extra}
    ORDER BY
      (UPPER(COALESCE("NEGATIVO",'NO'))='SI') DESC,
      (UPPER(COALESCE("RIESGO DE QUIEBRE",'NO'))='SI') DESC,
      "MARCA" ASC,
      CASE WHEN "Sku" ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
      CASE WHEN "Sku" ~ '^[0-9]+$' THEN ("Sku")::bigint END ASC NULLS LAST,
      "Sku" ASC,
      "Descripción del Producto" ASC
    LIMIT :limit OFFSET :offset;
    """
    return qdf(sql, params)


def get_tabla_ux_paginada(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str],
    page: int,
    page_size: int,
    search: str = "",
    only_negativos: bool = False,
    only_riesgo: bool = False,
) -> tuple[pd.DataFrame, int]:
    """
    Compat: devuelve (df_page, total_rows) usando total/page separados (sin COUNT OVER).
    """
    total = get_tabla_ux_total(rutero, reponedor, cod_rt, marcas, search, only_negativos, only_riesgo)
    df = get_tabla_ux_page(rutero, reponedor, cod_rt, marcas, page, page_size, search, only_negativos, only_riesgo)
    return df, int(total or 0)


@st.cache_data(ttl=QDF_TTL, show_spinner=False)
def get_tabla_ux_export(
    rutero: str,
    reponedor: str,
    cod_rt: str,
    marcas: list[str],
    search: str = "",
    only_negativos: bool = False,
    only_riesgo: bool = False,
) -> pd.DataFrame:
    """
    Dataset para export (on-demand desde UI).
    Ahora sin SELECT * (menos payload).
    """
    where_extra, p2 = _build_ux_filters(marcas, search, only_negativos, only_riesgo)
    params: dict[str, Any] = {"rutero": rutero, "reponedor": reponedor, "cod_rt": cod_rt, **p2}

    sql = f"""
    SELECT
      "MARCA","Sku","Descripción del Producto",
      "Stock","Venta(+7)","NEGATIVO","RIESGO DE QUIEBRE","OTROS"
    FROM public.v_local_skus_ux
    WHERE rutero=:rutero AND reponedor=:reponedor AND cod_rt=:cod_rt
    {where_extra}
    """
    return qdf(sql, params)