# scripts/load_ruta_rutero_from_excel.py
# -*- coding: utf-8 -*-
import os
import argparse
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values

from cliente_mvs import run_cliente_mvs_refresh
from refresh_control_gestion_v2_mv import run_cg_v2_mv_refresh

LOADER_NAME = "load_ruta_rutero_from_excel"
LOCAL_TZ = ZoneInfo("America/Santiago")


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


def normalize_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text in {"nan", "None", "NaT"}:
        return ""
    return text


def normalize_key(value) -> str:
    return normalize_text(value).upper()


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


def get_effective_week_info(loaded_at: datetime) -> tuple[str, int]:
    local_dt = loaded_at.astimezone(LOCAL_TZ)
    week_start = (local_dt.date() - timedelta(days=local_dt.weekday())).isoformat()
    week_iso = int(local_dt.date().isocalendar().week)
    return week_start, week_iso


def register_cg_ruta_batch(
    cur,
    *,
    source_file: str,
    source_sheet: str,
    loaded_at: datetime,
    notes: str,
) -> int:
    cur.execute(
        """
        insert into cg_core.ruta_rutero_load_batch (
            source_file,
            source_sheet,
            loader_name,
            loaded_rows,
            status,
            loaded_at,
            notes
        )
        values (%s, %s, %s, 0, 'cancelled', %s, %s)
        returning ruta_batch_id
        """,
        (source_file, source_sheet, LOADER_NAME, loaded_at, notes),
    )
    return int(cur.fetchone()[0])


def finalize_cg_ruta_batch(
    cur,
    *,
    ruta_batch_id: int,
    loaded_rows: int,
    status: str,
    notes: str,
) -> None:
    cur.execute(
        """
        update cg_core.ruta_rutero_load_batch
           set loaded_rows = %s,
               status = %s,
               notes = %s
         where ruta_batch_id = %s
        """,
        (loaded_rows, status, notes, ruta_batch_id),
    )


def build_cg_history_rows(
    out: pd.DataFrame,
    *,
    ruta_batch_id: int,
    source_file: str,
    source_sheet: str,
    loaded_at: datetime,
) -> list[tuple]:
    records = []
    for rec in out.to_dict(orient="records"):
        payload = {
            "cadena": rec["cadena"],
            "formato": rec["formato"],
            "region": rec["region"],
            "comuna": rec["comuna"],
            "cod_rt": rec["cod_rt"],
            "cod_b2b": rec["cod_b2b"],
            "local_nombre": rec["local_nombre"],
            "direccion": rec["direccion"],
            "veces_por_semana": rec["veces_por_semana"],
            "rutero": rec["rutero"],
            "jefe_operaciones": rec["jefe_operaciones"],
            "gestores": rec["gestores"],
            "cliente": rec["cliente"],
            "supervisor": rec["supervisor"],
            "reponedor": rec["reponedor"],
            "lunes": rec["lunes"],
            "martes": rec["martes"],
            "miercoles": rec["miercoles"],
            "jueves": rec["jueves"],
            "viernes": rec["viernes"],
            "sabado": rec["sabado"],
            "domingo": rec["domingo"],
            "visita_mensual": rec["visita_mensual"],
            "dif": rec["dif"],
            "obs": rec["obs"],
            "aux": rec["aux"],
            "gg": rec["gg"],
            "modalidad": rec["modalidad"],
            "row_hash": rec["row_hash"],
            "source": rec["source"],
            "source_row": rec["source_row"],
        }
        records.append(
            (
                ruta_batch_id,
                source_file,
                source_sheet,
                int(rec["source_row"]),
                Json(payload, dumps=lambda x: json.dumps(x, ensure_ascii=False)),
                rec["cadena"],
                rec["formato"],
                rec["region"],
                rec["comuna"],
                rec["cod_rt"],
                rec["cod_b2b"],
                rec["local_nombre"],
                rec["direccion"],
                int(rec["veces_por_semana"]),
                rec["rutero"],
                rec["jefe_operaciones"],
                rec["gestores"],
                rec["cliente"],
                rec["supervisor"],
                rec["reponedor"],
                int(rec["lunes"]),
                int(rec["martes"]),
                int(rec["miercoles"]),
                int(rec["jueves"]),
                int(rec["viernes"]),
                int(rec["sabado"]),
                int(rec["domingo"]),
                int(rec["visita_mensual"]),
                int(rec["dif"]),
                rec["obs"],
                rec["aux"],
                int(rec["gg"]),
                rec["modalidad"],
                rec["row_hash"],
                rec["source"],
                loaded_at,
                normalize_text(rec["cod_rt"]),
                normalize_text(rec["cod_b2b"]),
                normalize_key(rec["cliente"]),
                normalize_key(rec["gestores"]),
                normalize_key(rec["supervisor"]),
                normalize_key(rec["reponedor"]),
            )
        )
    return records


def insert_cg_history_rows(cur, rows: list[tuple]) -> None:
    if not rows:
        return
    execute_values(
        cur,
        """
        insert into cg_core.ruta_rutero_load_rows (
            ruta_batch_id,
            source_file,
            source_sheet,
            source_row,
            payload_json,
            cadena,
            formato,
            region,
            comuna,
            cod_rt,
            cod_b2b,
            local_nombre,
            direccion,
            veces_por_semana,
            rutero,
            jefe_operaciones,
            gestores,
            cliente,
            supervisor,
            reponedor,
            lunes,
            martes,
            miercoles,
            jueves,
            viernes,
            sabado,
            domingo,
            visita_mensual,
            dif,
            obs,
            aux,
            gg,
            modalidad,
            row_hash,
            source,
            source_ingested_at,
            cod_rt_norm,
            cod_b2b_norm,
            cliente_norm,
            gestor_norm,
            supervisor_norm,
            reponedor_norm
        ) values %s
        on conflict (ruta_batch_id, source_row) do update set
            payload_json = excluded.payload_json,
            cadena = excluded.cadena,
            formato = excluded.formato,
            region = excluded.region,
            comuna = excluded.comuna,
            cod_rt = excluded.cod_rt,
            cod_b2b = excluded.cod_b2b,
            local_nombre = excluded.local_nombre,
            direccion = excluded.direccion,
            veces_por_semana = excluded.veces_por_semana,
            rutero = excluded.rutero,
            jefe_operaciones = excluded.jefe_operaciones,
            gestores = excluded.gestores,
            cliente = excluded.cliente,
            supervisor = excluded.supervisor,
            reponedor = excluded.reponedor,
            lunes = excluded.lunes,
            martes = excluded.martes,
            miercoles = excluded.miercoles,
            jueves = excluded.jueves,
            viernes = excluded.viernes,
            sabado = excluded.sabado,
            domingo = excluded.domingo,
            visita_mensual = excluded.visita_mensual,
            dif = excluded.dif,
            obs = excluded.obs,
            aux = excluded.aux,
            gg = excluded.gg,
            modalidad = excluded.modalidad,
            row_hash = excluded.row_hash,
            source = excluded.source,
            source_ingested_at = excluded.source_ingested_at,
            cod_rt_norm = excluded.cod_rt_norm,
            cod_b2b_norm = excluded.cod_b2b_norm,
            cliente_norm = excluded.cliente_norm,
            gestor_norm = excluded.gestor_norm,
            supervisor_norm = excluded.supervisor_norm,
            reponedor_norm = excluded.reponedor_norm,
            ingested_at = now()
        """,
        rows,
        page_size=5000,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default=r"data\DB_GLOBAL_INVENTARIO.xlsx")
    ap.add_argument("--sheet", default="RUTA_RUTERO")
    ap.add_argument("--db_url", default=os.getenv("DB_URL_LOAD", "") or os.getenv("DB_URL", ""))
    ap.add_argument("--source", default="DB_GLOBAL_INVENTARIO.xlsx:RUTA_RUTERO")
    ap.add_argument("--no-cg-ruta-history", action="store_true")
    ap.add_argument("--no-refresh-cliente-mvs", action="store_true")
    ap.add_argument("--refresh-cg-v2-mv", action="store_true")
    ap.add_argument("--validate-cg-v2-mv", action="store_true")
    args = ap.parse_args()

    if not args.db_url:
        raise SystemExit("Falta DB_URL_LOAD/DB_URL. Setea DB_URL_LOAD o pasa --db_url")
    if args.validate_cg_v2_mv and not args.refresh_cg_v2_mv:
        raise SystemExit("--validate-cg-v2-mv requiere --refresh-cg-v2-mv")

    db_url = ensure_sslmode(args.db_url)

    excel_path = Path(args.excel)
    df = pd.read_excel(excel_path, sheet_name=args.sheet)
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

    loaded_at = datetime.now(tz=LOCAL_TZ)
    effective_week_start, effective_week_iso = get_effective_week_info(loaded_at)
    ruta_batch_id = None
    rows_loaded_cg_history = 0

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        conn.commit()

    if not args.no_cg_ruta_history:
        history_rows = build_cg_history_rows(
            out,
            ruta_batch_id=0,
            source_file=excel_path.name,
            source_sheet=args.sheet,
            loaded_at=loaded_at,
        )
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                ruta_batch_id = register_cg_ruta_batch(
                    cur,
                    source_file=excel_path.name,
                    source_sheet=args.sheet,
                    loaded_at=loaded_at,
                    notes=f"source={args.source}",
                )
                conn.commit()

            for idx, row in enumerate(history_rows):
                history_rows[idx] = (ruta_batch_id,) + row[1:]

            try:
                with conn.cursor() as cur:
                    insert_cg_history_rows(cur, history_rows)
                    rows_loaded_cg_history = len(history_rows)
                    finalize_cg_ruta_batch(
                        cur,
                        ruta_batch_id=ruta_batch_id,
                        loaded_rows=rows_loaded_cg_history,
                        status="ok",
                        notes=(
                            f"source={args.source} | rows_public={len(rows)} | "
                            f"rows_cg_history={rows_loaded_cg_history}"
                        ),
                    )
                conn.commit()
            except Exception as exc:
                conn.rollback()
                with conn.cursor() as cur:
                    finalize_cg_ruta_batch(
                        cur,
                        ruta_batch_id=ruta_batch_id,
                        loaded_rows=0,
                        status="cancelled",
                        notes=f"error={exc}",
                    )
                conn.commit()
                raise

    result = {
        "loader": LOADER_NAME,
        "excel": str(excel_path),
        "sheet": args.sheet,
        "source": args.source,
        "rows_loaded_public_ruta": int(len(rows)),
        "rows_loaded_cg_history": int(rows_loaded_cg_history),
        "ruta_batch_id": ruta_batch_id,
        "effective_week_start": effective_week_start,
        "effective_week_iso": effective_week_iso,
        "status": "ok",
        "cg_ruta_history_enabled": not args.no_cg_ruta_history,
    }

    if args.no_refresh_cliente_mvs:
        result["cliente_mvs_refresh"] = {"status": "skipped", "reason": "--no-refresh-cliente-mvs"}
    else:
        refresh_result = run_cliente_mvs_refresh(
            db_url=args.db_url,
            execute=True,
            run_smoke=True,
        )
        result["cliente_mvs_refresh"] = refresh_result

    if args.refresh_cg_v2_mv:
        try:
            result["cg_v2_mv_refresh"] = run_cg_v2_mv_refresh(
                db_url=args.db_url,
                validate=args.validate_cg_v2_mv,
            )
        except Exception as exc:
            result["status"] = "error"
            result["cg_v2_mv_refresh"] = {
                "status": "error",
                "error": str(exc),
            }
            print(json.dumps(result, ensure_ascii=False, indent=2))
            raise SystemExit(1)

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
