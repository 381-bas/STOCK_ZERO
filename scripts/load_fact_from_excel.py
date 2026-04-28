# scripts/load_fact_from_excel.py
# -*- coding: utf-8 -*-
import os
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from cliente_mvs import run_cliente_mvs_refresh


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default=r"data\DB_GLOBAL_INVENTARIO.xlsx")
    ap.add_argument("--sheet", default="BASE")
    ap.add_argument("--db_url", default=os.getenv("DB_URL_LOAD", "") or os.getenv("DB_URL", ""))
    ap.add_argument("--source", default="DB_GLOBAL_INVENTARIO.xlsx:BASE")
    ap.add_argument("--no-refresh-cliente-mvs", action="store_true")
    args = ap.parse_args()

    if not args.db_url:
        raise SystemExit("Falta DB_URL. Setea DB_URL en .env o pásalo con --db_url")

    df = pd.read_excel(args.excel, sheet_name=args.sheet)
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
    out["source"] = args.source

    out = out.dropna(subset=["fecha", "cod_rt", "sku", "marca"])
    out = out[out["cod_rt"].astype(str).str.len() > 0]
    out = out[out["sku"].astype(str).str.len() > 0]
    out = out[out["marca"].astype(str).str.len() > 0]

    # Excluir huérfanos operacionales NO_RT antes de insertar
    no_rt_count = int((out["cod_rt"] == "NO_RT").sum())
    out = out[out["cod_rt"] != "NO_RT"].copy()

    # Defensa extra contra colisiones dentro del mismo payload
    conflict_key = ["fecha", "cod_rt", "sku", "marca"]
    duplicate_payload_count = int(out.duplicated(subset=conflict_key, keep="last").sum())
    out = out.drop_duplicates(subset=conflict_key, keep="last").copy()

    rows = list(out[[
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
    ]].itertuples(index=False, name=None))

    if not rows:
        print(
            "WARN: no hay filas válidas para cargar. "
            f"omitidas_no_rt={no_rt_count} "
            f"omitidas_duplicate_payload={duplicate_payload_count}"
        )
        return

    sql = """
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

    with psycopg2.connect(args.db_url) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        conn.commit()

    print(
        f"OK: cargadas/upsert {len(rows)} filas desde {args.excel} [{args.sheet}] | "
        f"omitidas_no_rt={no_rt_count} | "
        f"omitidas_duplicate_payload={duplicate_payload_count}"
    )

    if args.no_refresh_cliente_mvs:
        print("SKIP: refresh post-carga de MVs CLIENTE omitido por --no-refresh-cliente-mvs")
        return

    refresh_result = run_cliente_mvs_refresh(
        db_url=args.db_url,
        execute=True,
        run_smoke=True,
    )
    print(refresh_result)


if __name__ == "__main__":
    main()
