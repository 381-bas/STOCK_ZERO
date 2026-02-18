# scripts/load_fact_from_excel.py
# -*- coding: utf-8 -*-
import os
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def norm_col(c: str) -> str:
    return str(c).strip()

def to_int_series(s: pd.Series) -> pd.Series:
    s = s.fillna(0)
    # Algunos vienen como float (7.0). Convertimos seguro a int.
    return pd.to_numeric(s, errors="coerce").fillna(0).round(0).astype(int)

def to_sku_text(s: pd.Series) -> pd.Series:
    # SKU puede venir como número. Lo guardamos como texto sin .0
    s = s.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default=r"data\DB_GLOBAL_INVENTARIO.xlsx")
    ap.add_argument("--sheet", default="BASE")
    ap.add_argument("--db_url", default=os.getenv("DB_URL_LOAD", "") or os.getenv("DB_URL", ""))
    ap.add_argument("--source", default="DB_GLOBAL_INVENTARIO.xlsx:BASE")
    args = ap.parse_args()

    if not args.db_url:
        raise SystemExit("Falta DB_URL. Setea DB_URL en .env o pásalo con --db_url")

    df = pd.read_excel(args.excel, sheet_name=args.sheet)

    # Normaliza nombres de columnas (quita espacios al inicio/fin)
    df.columns = [norm_col(c) for c in df.columns]

    # Mapeo desde Excel -> columnas internas
    # Nota: en Excel el SKU viene como 'Sku' pero en tu archivo es 'Sku' con espacio en algunos casos ('Sku' o 'Sku' con prefijo)
    # Ya lo dejamos normalizado con strip, así quedará "Sku" si venía " Sku".
    required = {
        "FECHA": "fecha",
        "REGIÓN": "region",
        "CADENA": "cadena",
        "MARCA": "marca",
        "Sku": "sku",
        "Descripción del Producto": "descripcion_producto",
        "COD_RT": "cod_rt",
        "Nombre_local_RR": "nombre_local_rr",
        "GESTORES": "gestores",
        "SUPERVISOR": "supervisor",
        "REPONEDOR": "reponedor",
        "RUTERO": "rutero",
        "Inventario en Locales(U)": "stock",
        "Venta(u)": "venta_7",
    }

    # Algunos archivos traen 'CADENA ' y 'SUPERVISOR ' con espacio; con strip quedan 'CADENA' y 'SUPERVISOR'
    # Verificamos presentes
    missing = [k for k in required.keys() if k not in df.columns]
    if missing:
        raise SystemExit(f"Faltan columnas en BASE: {missing}\nColumnas encontradas: {list(df.columns)}")

    out = pd.DataFrame()
    out["fecha"] = pd.to_datetime(df["FECHA"], errors="coerce").dt.date
    out["region"] = df["REGIÓN"].astype(str).str.strip()
    out["cadena"] = df["CADENA"].astype(str).str.strip()
    out["marca"] = df["MARCA"].astype(str).str.strip()

    out["sku"] = to_sku_text(df["Sku"])
    out["descripcion_producto"] = df["Descripción del Producto"].astype(str).str.strip()

    out["cod_rt"] = df["COD_RT"].astype(str).str.strip()
    out["nombre_local_rr"] = df["Nombre_local_RR"].astype(str).str.strip()

    out["gestores"] = df["GESTORES"].astype(str).str.strip()
    out["supervisor"] = df["SUPERVISOR"].astype(str).str.strip()

    out["reponedor"] = df["REPONEDOR"].astype(str).str.strip()
    out["rutero"] = df["RUTERO"].astype(str).str.strip()

    out["stock"] = to_int_series(df["Inventario en Locales(U)"])
    out["venta_7"] = to_int_series(df["Venta(u)"])

    out["otros"] = ""
    out["source"] = args.source

    # Limpieza mínima
    out = out.dropna(subset=["fecha", "cod_rt", "sku", "marca"])
    out = out[out["cod_rt"].astype(str).str.len() > 0]
    out = out[out["sku"].astype(str).str.len() > 0]

    rows = list(out[[
        "fecha","region","cadena","gestores","supervisor",
        "rutero","reponedor","cod_rt","nombre_local_rr",
        "marca","sku","descripcion_producto","stock","venta_7",
        "otros","source"
    ]].itertuples(index=False, name=None))

    sql = """
    INSERT INTO public.fact_stock_venta
    (fecha, region, cadena, gestores, supervisor,
     rutero, reponedor, cod_rt, nombre_local_rr,
     marca, sku, descripcion_producto, stock, venta_7,
     otros, source)
    VALUES %s
    ON CONFLICT (fecha, cod_rt, sku, marca)
    DO UPDATE SET
      region = EXCLUDED.region,
      cadena = EXCLUDED.cadena,
      gestores = EXCLUDED.gestores,
      supervisor = EXCLUDED.supervisor,
      rutero = EXCLUDED.rutero,
      reponedor = EXCLUDED.reponedor,
      nombre_local_rr = EXCLUDED.nombre_local_rr,
      descripcion_producto = EXCLUDED.descripcion_producto,
      stock = EXCLUDED.stock,
      venta_7 = EXCLUDED.venta_7,
      otros = EXCLUDED.otros,
      source = EXCLUDED.source,
      ingested_at = NOW();
    """

    with psycopg2.connect(args.db_url) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        conn.commit()

    print(f"OK: cargadas/upsert {len(rows)} filas desde {args.excel} [{args.sheet}]")

if __name__ == "__main__":
    main()