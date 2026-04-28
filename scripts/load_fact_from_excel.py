# scripts/load_fact_from_excel.py
# -*- coding: utf-8 -*-
"""
Loader STOCK_ZERO - fact_stock_venta

Modo recomendado:
    smart-replace-date

Objetivo:
    - Leer BASE desde DB_GLOBAL_INVENTARIO.xlsx.
    - Limpiar y deduplicar igual que el loader original.
    - Detectar fechas nuevas/incompletas antes de cargar.
    - Evitar recargar fechas que ya calzan por conteo.
    - Reemplazar por fecha afectada para evitar sobrantes.
    - Cargar en batches con retry para reducir cortes de conexión Supabase.
    - Refrescar MVs cliente solo si hubo cambios reales.

Notas:
    - Llave lógica: fecha + cod_rt + sku + marca.
    - No usar cliente como puerta de carga: cliente se valida después vía MVs/vistas.
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime
from typing import Iterable

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from cliente_mvs import run_cliente_mvs_refresh


# =========================================================
# Normalización
# =========================================================
def norm_col(c: str) -> str:
    return str(c).strip()


def to_int_series(s: pd.Series) -> pd.Series:
    s = s.fillna(0)
    return pd.to_numeric(s, errors="coerce").fillna(0).round(0).astype(int)


def to_text_series(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def to_sku_text(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    return s


def parse_force_dates(raw: str) -> set[date]:
    out: set[date] = set()
    if not raw:
        return out

    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        try:
            out.add(pd.to_datetime(value, errors="raise").date())
        except Exception as exc:
            raise SystemExit(f"Fecha inválida en --force-dates: {value}") from exc
    return out


def chunked(seq: list[tuple], size: int) -> Iterable[list[tuple]]:
    if size <= 0:
        raise ValueError("batch-size debe ser mayor a 0")
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# =========================================================
# DB helpers
# =========================================================
def connect(db_url: str, connect_timeout: int):
    return psycopg2.connect(
        db_url,
        connect_timeout=connect_timeout,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def fetch_db_counts(
    *,
    db_url: str,
    fechas: list[date],
    connect_timeout: int,
) -> dict[date, int]:
    if not fechas:
        return {}

    sql = """
        select fecha, count(*)::int as db_rows
        from public.fact_stock_venta
        where fecha = any(%s::date[])
        group by fecha
    """

    with connect(db_url, connect_timeout) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (fechas,))
            rows = cur.fetchall()

    return {r[0]: int(r[1]) for r in rows}


def delete_date(
    *,
    cur,
    fecha: date,
) -> int:
    cur.execute("delete from public.fact_stock_venta where fecha = %s", (fecha,))
    return int(cur.rowcount or 0)


def insert_rows_batch(
    *,
    cur,
    sql_insert: str,
    rows: list[tuple],
    page_size: int,
) -> None:
    execute_values(
        cur,
        sql_insert,
        rows,
        page_size=max(1, min(page_size, len(rows))),
    )


# =========================================================
# Transformación Excel -> payload limpio
# =========================================================
def build_clean_payload(
    *,
    excel_path: str,
    sheet: str,
    source: str,
) -> tuple[pd.DataFrame, dict[str, int]]:
    df = pd.read_excel(excel_path, sheet_name=sheet)
    df.columns = [norm_col(c) for c in df.columns]

    required = [
        "CADENA",
        "FECHA",
        "MARCA",
        "SKU",
        "DESCRIPCION_PRODUCTO",
        "N_LOCAL",
        "VTA(Un)",
        "INV(Un)",
        "COD_RT",
        "NOMBRE_LOCAL_RR",
        "OTROS",
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(
            f"Faltan columnas en BASE: {missing}\n"
            f"Columnas encontradas: {list(df.columns)}"
        )

    out = pd.DataFrame()
    out["cadena"] = to_text_series(df["CADENA"])
    out["fecha"] = pd.to_datetime(df["FECHA"], errors="coerce").dt.date
    out["marca"] = to_text_series(df["MARCA"])
    out["sku"] = to_sku_text(df["SKU"])
    out["descripcion_producto"] = to_text_series(df["DESCRIPCION_PRODUCTO"])
    out["n_local"] = to_text_series(df["N_LOCAL"])
    out["venta_u"] = to_int_series(df["VTA(Un)"])
    out["inv_u"] = to_int_series(df["INV(Un)"])
    out["cod_rt"] = to_text_series(df["COD_RT"]).str.upper()
    out["nombre_local_rr"] = to_text_series(df["NOMBRE_LOCAL_RR"])
    out["otros"] = to_text_series(df["OTROS"])
    out["source"] = source

    raw_rows = int(len(out))

    out = out.dropna(subset=["fecha", "cod_rt", "sku", "marca"])
    out = out[out["cod_rt"].astype(str).str.len() > 0]
    out = out[out["sku"].astype(str).str.len() > 0]
    out = out[out["marca"].astype(str).str.len() > 0]

    valid_before_no_rt = int(len(out))

    no_rt_count = int((out["cod_rt"] == "NO_RT").sum())
    out = out[out["cod_rt"] != "NO_RT"].copy()

    conflict_key = ["fecha", "cod_rt", "sku", "marca"]
    duplicate_payload_count = int(out.duplicated(subset=conflict_key, keep="last").sum())
    out = out.drop_duplicates(subset=conflict_key, keep="last").copy()

    out = out.sort_values(conflict_key).reset_index(drop=True)

    stats = {
        "raw_rows": raw_rows,
        "valid_before_no_rt": valid_before_no_rt,
        "omitidas_no_rt": no_rt_count,
        "omitidas_duplicate_payload": duplicate_payload_count,
        "final_rows": int(len(out)),
    }

    return out, stats


def rows_from_df(out: pd.DataFrame) -> list[tuple]:
    return list(
        out[
            [
                "fecha",
                "cadena",
                "marca",
                "sku",
                "descripcion_producto",
                "n_local",
                "venta_u",
                "inv_u",
                "cod_rt",
                "nombre_local_rr",
                "otros",
                "source",
            ]
        ].itertuples(index=False, name=None)
    )


# =========================================================
# Plan de carga
# =========================================================
def build_plan(
    *,
    out: pd.DataFrame,
    db_counts: dict[date, int],
    force_dates: set[date],
    mode: str,
) -> pd.DataFrame:
    expected = (
        out.groupby("fecha", dropna=True)
        .size()
        .reset_index(name="expected_rows")
        .sort_values("fecha")
        .reset_index(drop=True)
    )

    expected["db_rows"] = expected["fecha"].map(lambda d: int(db_counts.get(d, 0)))
    expected["force"] = expected["fecha"].map(lambda d: d in force_dates)

    if mode == "upsert-all":
        expected["action"] = "load"
        expected["reason"] = "mode_upsert_all"
        return expected

    expected["action"] = "skip"
    expected["reason"] = "count_match"

    missing_mask = expected["db_rows"] == 0
    mismatch_mask = expected["db_rows"] != expected["expected_rows"]
    force_mask = expected["force"]

    expected.loc[missing_mask, ["action", "reason"]] = ["load", "missing_date"]
    expected.loc[mismatch_mask, ["action", "reason"]] = ["load", "count_mismatch"]
    expected.loc[force_mask, ["action", "reason"]] = ["load", "forced"]

    return expected


# =========================================================
# Carga
# =========================================================
def load_replace_by_date(
    *,
    db_url: str,
    out: pd.DataFrame,
    dates_to_load: list[date],
    sql_insert: str,
    batch_size: int,
    retries: int,
    connect_timeout: int,
) -> dict:
    result = {
        "mode": "smart-replace-date",
        "dates_loaded": [],
        "rows_loaded": 0,
        "rows_deleted": 0,
        "batches": 0,
    }

    for fecha in dates_to_load:
        part = out[out["fecha"] == fecha].copy()
        rows = rows_from_df(part)

        if not rows:
            print(f"WARN: fecha {fecha} quedó sin filas limpias; se omite.")
            continue

        for attempt in range(1, retries + 1):
            try:
                with connect(db_url, connect_timeout) as conn:
                    with conn.cursor() as cur:
                        deleted = delete_date(cur=cur, fecha=fecha)

                        loaded = 0
                        batches = 0
                        for chunk in chunked(rows, batch_size):
                            insert_rows_batch(
                                cur=cur,
                                sql_insert=sql_insert,
                                rows=chunk,
                                page_size=batch_size,
                            )
                            loaded += len(chunk)
                            batches += 1

                    conn.commit()

                result["dates_loaded"].append(str(fecha))
                result["rows_loaded"] += loaded
                result["rows_deleted"] += deleted
                result["batches"] += batches

                print(
                    f"OK fecha={fecha} deleted={deleted} loaded={loaded} "
                    f"batches={batches}"
                )
                break

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                if attempt >= retries:
                    raise RuntimeError(
                        f"Fallo definitivo cargando fecha={fecha} "
                        f"intentos={retries}. La transacción de esa fecha fue revertida."
                    ) from exc

                wait_s = attempt * 3
                print(
                    f"WARN fecha={fecha}: conexión caída intento "
                    f"{attempt}/{retries}. Reintento en {wait_s}s..."
                )
                time.sleep(wait_s)

    return result


def load_upsert_all(
    *,
    db_url: str,
    rows: list[tuple],
    sql_insert: str,
    batch_size: int,
    retries: int,
    connect_timeout: int,
) -> dict:
    result = {
        "mode": "upsert-all",
        "rows_loaded": 0,
        "batches": 0,
    }

    for chunk_idx, chunk in enumerate(chunked(rows, batch_size), start=1):
        for attempt in range(1, retries + 1):
            try:
                with connect(db_url, connect_timeout) as conn:
                    with conn.cursor() as cur:
                        insert_rows_batch(
                            cur=cur,
                            sql_insert=sql_insert,
                            rows=chunk,
                            page_size=batch_size,
                        )
                    conn.commit()

                result["rows_loaded"] += len(chunk)
                result["batches"] += 1
                print(f"OK batch={chunk_idx} loaded={len(chunk)}")
                break

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                if attempt >= retries:
                    raise RuntimeError(
                        f"Fallo definitivo cargando batch={chunk_idx} "
                        f"intentos={retries}. Batches previos pueden haber quedado confirmados."
                    ) from exc

                wait_s = attempt * 3
                print(
                    f"WARN batch={chunk_idx}: conexión caída intento "
                    f"{attempt}/{retries}. Reintento en {wait_s}s..."
                )
                time.sleep(wait_s)

    return result


# =========================================================
# Main
# =========================================================
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default=r"data\DB_GLOBAL_INVENTARIO.xlsx")
    ap.add_argument("--sheet", default="BASE")
    ap.add_argument("--db_url", default=os.getenv("DB_URL_LOAD", "") or os.getenv("DB_URL", ""))
    ap.add_argument("--source", default="DB_GLOBAL_INVENTARIO.xlsx:BASE")
    ap.add_argument("--no-refresh-cliente-mvs", action="store_true")

    ap.add_argument(
        "--mode",
        choices=["smart-replace-date", "upsert-all"],
        default=os.getenv("FACT_LOAD_MODE", "smart-replace-date"),
        help="smart-replace-date detecta fechas afectadas y reemplaza solo esas fechas. upsert-all carga todo en batches.",
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--force-dates",
        default="",
        help="Fechas separadas por coma para recargar sí o sí. Ej: 2026-04-28,2026-04-29",
    )
    ap.add_argument("--batch-size", type=int, default=int(os.getenv("FACT_LOAD_BATCH_SIZE", "500")))
    ap.add_argument("--retries", type=int, default=int(os.getenv("FACT_LOAD_RETRIES", "3")))
    ap.add_argument("--connect-timeout", type=int, default=int(os.getenv("PG_CONNECT_TIMEOUT", "20")))

    args = ap.parse_args()

    if not args.db_url:
        raise SystemExit("Falta DB_URL. Setea DB_URL_LOAD/DB_URL en .env o pásalo con --db_url")

    force_dates = parse_force_dates(args.force_dates)

    t0 = time.perf_counter()
    out, stats = build_clean_payload(
        excel_path=args.excel,
        sheet=args.sheet,
        source=args.source,
    )

    if out.empty:
        print(
            "WARN: no hay filas válidas para cargar. "
            f"stats={stats}"
        )
        return

    fechas_excel = sorted(out["fecha"].dropna().unique().tolist())
    db_counts = fetch_db_counts(
        db_url=args.db_url,
        fechas=fechas_excel,
        connect_timeout=args.connect_timeout,
    )

    plan = build_plan(
        out=out,
        db_counts=db_counts,
        force_dates=force_dates,
        mode=args.mode,
    )

    print("=== PRECHECK FACT LOAD ===")
    print(f"excel={args.excel}")
    print(f"sheet={args.sheet}")
    print(f"mode={args.mode}")
    print(f"batch_size={args.batch_size}")
    print(f"retries={args.retries}")
    print(f"stats={stats}")
    print(plan.to_string(index=False))

    dates_to_load = plan.loc[plan["action"] == "load", "fecha"].tolist()

    if args.dry_run:
        print("DRY_RUN: no se cargó nada y no se refrescaron MVs.")
        return

    if not dates_to_load:
        print("SKIP: no hay fechas nuevas/incompletas/forzadas. No se carga nada.")
        print("SKIP: refresh de MVs omitido porque no hubo cambios.")
        return

    sql_insert = """
    INSERT INTO public.fact_stock_venta
    (
        fecha,
        cadena,
        marca,
        sku,
        descripcion_producto,
        n_local,
        venta_u,
        inv_u,
        cod_rt,
        nombre_local_rr,
        otros,
        source
    )
    VALUES %s
    ON CONFLICT (fecha, cod_rt, sku, marca)
    DO UPDATE SET
      cadena = EXCLUDED.cadena,
      descripcion_producto = EXCLUDED.descripcion_producto,
      n_local = EXCLUDED.n_local,
      venta_u = EXCLUDED.venta_u,
      inv_u = EXCLUDED.inv_u,
      nombre_local_rr = EXCLUDED.nombre_local_rr,
      otros = EXCLUDED.otros,
      source = EXCLUDED.source,
      ingested_at = NOW();
    """

    if args.mode == "smart-replace-date":
        load_result = load_replace_by_date(
            db_url=args.db_url,
            out=out,
            dates_to_load=dates_to_load,
            sql_insert=sql_insert,
            batch_size=args.batch_size,
            retries=args.retries,
            connect_timeout=args.connect_timeout,
        )
    else:
        rows = rows_from_df(out)
        load_result = load_upsert_all(
            db_url=args.db_url,
            rows=rows,
            sql_insert=sql_insert,
            batch_size=args.batch_size,
            retries=args.retries,
            connect_timeout=args.connect_timeout,
        )

    print(f"LOAD_RESULT: {load_result}")

    if args.no_refresh_cliente_mvs:
        print("SKIP: refresh post-carga de MVs CLIENTE omitido por --no-refresh-cliente-mvs")
        print(f"elapsed_ms={round((time.perf_counter() - t0) * 1000)}")
        return

    refresh_result = run_cliente_mvs_refresh(
        db_url=args.db_url,
        execute=True,
        run_smoke=True,
    )
    print(refresh_result)

    print(
        "OK: carga finalizada | "
        f"mode={args.mode} | "
        f"dates_loaded={dates_to_load} | "
        f"stats={stats} | "
        f"elapsed_ms={round((time.perf_counter() - t0) * 1000)}"
    )


if __name__ == "__main__":
    main()
