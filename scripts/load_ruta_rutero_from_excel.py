# scripts/load_ruta_rutero_from_excel.py
# -*- coding: utf-8 -*-
import os
import argparse
import hashlib
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# -----------------------------
# Helpers
# -----------------------------
def norm_col(c: str) -> str:
    return str(c).strip()


def clean_str(series: pd.Series) -> pd.Series:
    # Convierte NaN -> "" y limpia espacios
    s = series.where(series.notna(), "")
    s = s.astype(str)
    # pandas puede dejar "nan" como string cuando viene mezclado
    s = s.replace({"nan": "", "None": "", "NaT": ""})
    return s.str.strip()


def to_int01(s: pd.Series) -> pd.Series:
    # días vienen como 1.0 o NaN
    return pd.to_numeric(s, errors="coerce").fillna(0).round(0).astype(int)


def to_int(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0).round(0).astype(int)


def md5_row(values) -> str:
    raw = "|".join("" if v is None else str(v).strip() for v in values)
    return hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()


def ensure_sslmode(db_url: str) -> str:
    # Supabase normalmente requiere sslmode=require (seguro dejarlo puesto)
    if not db_url:
        return db_url
    if "sslmode=" in db_url:
        return db_url
    if "?" in db_url:
        return db_url + "&sslmode=require"
    return db_url + "?sslmode=require"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default=r"data\DB_GLOBAL_INVENTARIO.xlsx")
    ap.add_argument("--sheet", default="RUTA_RUTERO")
    ap.add_argument("--db_url", default=os.getenv("DB_URL_LOAD", "") or os.getenv("DB_URL", ""))
    ap.add_argument("--source", default="DB_GLOBAL_INVENTARIO.xlsx:RUTA_RUTERO")
    args = ap.parse_args()

    if not args.db_url:
        raise SystemExit("Falta DB_URL_LOAD/DB_URL. Setea DB_URL_LOAD o pasa --db_url")

    db_url = ensure_sslmode(args.db_url)

    df = pd.read_excel(args.excel, sheet_name=args.sheet)
    df.columns = [norm_col(c) for c in df.columns]

    # Permitimos variaciones típicas de nombres (por si acaso)
    col_map = {}
    if "DIRECCION" in df.columns and "DIRECCIÓN" not in df.columns:
        col_map["DIRECCION"] = "DIRECCIÓN"
    if col_map:
        df = df.rename(columns=col_map)

    required = [
        "CADENA", "FORMATO", "REGION", "COMUNA", "COD KPI ONE", "COD B2B", "LOCAL", "DIRECCIÓN",
        "VECES POR SEMANA", "RUTERO", "JEFE DE OPERACIONES", "GESTORES", "CLIENTE", "SUPERVISOR", "REPONEDOR",
        "LUNES", "MARTES", "MIERCOLES", "JUEVES", "VIERNES", "SABADO", "DOMINGO",
        "Visita mensual", "DIF", "OBS", "AUX", "GG", "MODALIDAD"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"Faltan columnas en {args.sheet}: {missing}\nColumnas: {list(df.columns)}")

    out = pd.DataFrame()

    out["cadena"] = clean_str(df["CADENA"])
    out["formato"] = clean_str(df["FORMATO"])
    out["region"] = clean_str(df["REGION"])
    out["comuna"] = clean_str(df["COMUNA"])

    out["cod_rt"] = clean_str(df["COD KPI ONE"])
    out["cod_b2b"] = clean_str(df["COD B2B"]).str.replace(r"\.0$", "", regex=True)

    out["local_nombre"] = clean_str(df["LOCAL"])
    out["direccion"] = clean_str(df["DIRECCIÓN"])

    out["veces_por_semana"] = to_int(df["VECES POR SEMANA"])

    out["rutero"] = clean_str(df["RUTERO"])
    out["jefe_operaciones"] = clean_str(df["JEFE DE OPERACIONES"])
    out["gestores"] = clean_str(df["GESTORES"])
    out["cliente"] = clean_str(df["CLIENTE"])
    out["supervisor"] = clean_str(df["SUPERVISOR"])
    out["reponedor"] = clean_str(df["REPONEDOR"])

    out["lunes"] = to_int01(df["LUNES"])
    out["martes"] = to_int01(df["MARTES"])
    out["miercoles"] = to_int01(df["MIERCOLES"])
    out["jueves"] = to_int01(df["JUEVES"])
    out["viernes"] = to_int01(df["VIERNES"])
    out["sabado"] = to_int01(df["SABADO"])
    out["domingo"] = to_int01(df["DOMINGO"])

    out["visita_mensual"] = to_int(df["Visita mensual"])
    out["dif"] = to_int(df["DIF"])
    out["obs"] = clean_str(df["OBS"])
    out["aux"] = clean_str(df["AUX"])
    out["gg"] = to_int(df["GG"])
    out["modalidad"] = clean_str(df["MODALIDAD"])

    out["source"] = args.source

    # source_row: identidad por fila del origen (preferir IdRow si existe)
    if "IdRow" in df.columns:
        out["source_row"] = to_int(df["IdRow"])
    else:
        out = out.reset_index(drop=True)
        # header=1, primera fila de data=2 (aprox, suficiente para identidad estable)
        out["source_row"] = out.index.astype(int) + 2

    # Limpieza mínima (NO botamos por reponedor/cliente duplicado; solo evitamos basura sin local/rutero)
    out = out[out["cod_rt"].astype(str).str.len() > 0]
    out = out[out["rutero"].astype(str).str.len() > 0]

    # row_hash (fingerprint negocio: útil para análisis; NO es llave de UPSERT)
    cols_for_hash = [
        "cadena", "formato", "region", "comuna", "cod_rt", "cod_b2b", "local_nombre", "direccion",
        "veces_por_semana", "rutero", "jefe_operaciones", "gestores", "cliente", "supervisor", "reponedor",
        "lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo",
        "visita_mensual", "dif", "obs", "aux", "gg", "modalidad"
    ]
    out["row_hash"] = out[cols_for_hash].apply(lambda r: md5_row(r.tolist()), axis=1)

    rows = list(out[[
        "cadena", "formato", "region", "comuna", "cod_rt", "cod_b2b", "local_nombre", "direccion",
        "veces_por_semana", "rutero", "jefe_operaciones", "gestores", "cliente", "supervisor", "reponedor",
        "lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo",
        "visita_mensual", "dif", "obs", "aux", "gg", "modalidad",
        "row_hash", "source", "source_row"
    ]].itertuples(index=False, name=None))

    sql = """
    INSERT INTO public.ruta_rutero
    (cadena,formato,region,comuna,cod_rt,cod_b2b,local_nombre,direccion,
     veces_por_semana,rutero,jefe_operaciones,gestores,cliente,supervisor,reponedor,
     lunes,martes,miercoles,jueves,viernes,sabado,domingo,
     visita_mensual,dif,obs,aux,gg,modalidad,row_hash,source,source_row)
    VALUES %s
    ON CONFLICT (source, source_row) DO UPDATE SET
      cadena=EXCLUDED.cadena,
      formato=EXCLUDED.formato,
      region=EXCLUDED.region,
      comuna=EXCLUDED.comuna,
      cod_rt=EXCLUDED.cod_rt,
      cod_b2b=EXCLUDED.cod_b2b,
      local_nombre=EXCLUDED.local_nombre,
      direccion=EXCLUDED.direccion,
      veces_por_semana=EXCLUDED.veces_por_semana,
      rutero=EXCLUDED.rutero,
      jefe_operaciones=EXCLUDED.jefe_operaciones,
      gestores=EXCLUDED.gestores,
      cliente=EXCLUDED.cliente,
      supervisor=EXCLUDED.supervisor,
      reponedor=EXCLUDED.reponedor,
      lunes=EXCLUDED.lunes,
      martes=EXCLUDED.martes,
      miercoles=EXCLUDED.miercoles,
      jueves=EXCLUDED.jueves,
      viernes=EXCLUDED.viernes,
      sabado=EXCLUDED.sabado,
      domingo=EXCLUDED.domingo,
      visita_mensual=EXCLUDED.visita_mensual,
      dif=EXCLUDED.dif,
      obs=EXCLUDED.obs,
      aux=EXCLUDED.aux,
      gg=EXCLUDED.gg,
      modalidad=EXCLUDED.modalidad,
      row_hash=EXCLUDED.row_hash,
      ingested_at=now();
    """

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        conn.commit()

    print(f"OK: upsert {len(rows)} filas en public.ruta_rutero desde {args.excel} [{args.sheet}]")
    print(f"DB_URL sslmode={'require' if 'sslmode=require' in db_url else 'n/a'} | source={args.source}")


if __name__ == "__main__":
    main()