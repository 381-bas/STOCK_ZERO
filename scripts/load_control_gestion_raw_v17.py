# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from datetime import date
import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values


DEFAULT_FILE = r"data\CUMPLIMIENTO_FRECUENCIA.xlsx"
LOADER_NAME = "load_control_gestion_raw_v17"


def ensure_sslmode(db_url: str) -> str:
    if not db_url:
        return db_url
    if "sslmode=" in db_url:
        return db_url
    return db_url + ("&sslmode=require" if "?" in db_url else "?sslmode=require")


def clean_text(v: Any) -> str:
    if pd.isna(v) or v is None:
        return ""
    return str(v).strip()


def clean_json_value(v: Any) -> Any:
    if pd.isna(v) or v is None:
        return None
    return str(v)


def to_json_payload(rec: dict[str, Any]) -> dict[str, Any]:
    return {str(k): clean_json_value(v) for k, v in rec.items()}


def read_excel_sheet(excel_path: Path, sheet: str, **kwargs: Any) -> pd.DataFrame:
    try:
        return pd.read_excel(excel_path, sheet_name=sheet, **kwargs)
    except ValueError as exc:
        raise ValueError(f"No existe la hoja requerida '{sheet}' en {excel_path.name}") from exc


def parse_date_value(v: Any) -> date | None:
    text = clean_text(v)
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def calc_week_iso(fecha_visita: date | None) -> int | None:
    if fecha_visita is None:
        return None
    return int(fecha_visita.isocalendar().week)


def parse_numeric_value(v: Any) -> float | None:
    text = clean_text(v)
    if not text:
        return None
    normalized = text.replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def bool_from_evidence(v: Any) -> bool:
    text = clean_text(v).upper()
    if not text:
        return False
    if text in {"0", "NO", "FALSE", "SIN", "N/A"}:
        return False
    return True


def register_batch(cur, source_file: str, source_sheet: str) -> int:
    cur.execute(
        """
        insert into cg_audit.batch_registry (
            source_file, source_sheet, loader_name, loaded_rows, status
        )
        values (%s, %s, %s, 0, 'started')
        returning batch_id
        """,
        (source_file, source_sheet, LOADER_NAME),
    )
    return int(cur.fetchone()[0])


def finalize_batch(cur, batch_id: int, loaded_rows: int, status: str, notes: str = "") -> None:
    cur.execute(
        """
        update cg_audit.batch_registry
           set loaded_rows = %s,
               status = %s,
               finished_at = now(),
               notes = %s
         where batch_id = %s
        """,
        (loaded_rows, status, notes, batch_id),
    )


def load_kpione(cur, excel_path: Path) -> dict[str, Any]:
    sheet = "DB (KPIONE)"
    df = read_excel_sheet(excel_path, sheet, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    required = ["nombre_local", "marca", "trabajador", "Fecha_reg", "estado_foto"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"KPIONE missing required columns: {missing}")

    batch_id = register_batch(cur, excel_path.name, sheet)

    rows = []
    for i, rec in enumerate(df.to_dict(orient="records"), start=2):
        rows.append((
            batch_id,
            excel_path.name,
            sheet,
            i,
            Json(to_json_payload(rec), dumps=lambda x: json.dumps(x, ensure_ascii=False)),
            clean_text(rec.get("nombre_local")),
            clean_text(rec.get("marca")),
            clean_text(rec.get("trabajador")),
            clean_text(rec.get("Fecha_reg")),
            clean_text(rec.get("estado_foto")),
        ))

    execute_values(
        cur,
        """
        insert into cg_raw.kpione_raw (
            batch_id,
            source_file,
            source_sheet,
            source_row,
            payload_json,
            local_raw,
            cliente_raw,
            persona_raw,
            fecha_visita_raw,
            evidencia_raw
        ) values %s
        on conflict (batch_id, source_row) do update set
            source_file = excluded.source_file,
            source_sheet = excluded.source_sheet,
            payload_json = excluded.payload_json,
            local_raw = excluded.local_raw,
            cliente_raw = excluded.cliente_raw,
            persona_raw = excluded.persona_raw,
            fecha_visita_raw = excluded.fecha_visita_raw,
            evidencia_raw = excluded.evidencia_raw,
            ingested_at = now()
        """,
        rows,
        page_size=5000,
    )

    finalize_batch(cur, batch_id, len(rows), "ok", f"rows={len(rows)}")
    return {
        "sheet": sheet,
        "batch_id": batch_id,
        "rows_read": int(len(df)),
        "rows_loaded": int(len(rows)),
    }


def load_kpione2(cur, excel_path: Path) -> dict[str, Any]:
    sheet = "DB (KPIONE2.0)"
    df = read_excel_sheet(excel_path, sheet, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    required = ["Codigo Local", "Marca", "Reponedor", "Fecha", "VISITA"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"KPIONE2 missing required columns: {missing}")

    batch_id = register_batch(cur, excel_path.name, sheet)

    try:
        rows = []
        for i, rec in enumerate(df.to_dict(orient="records"), start=2):
            fecha_visita = parse_date_value(rec.get("Fecha"))
            visita_numeric = parse_numeric_value(rec.get("VISITA"))
            has_evidence = bool_from_evidence(rec.get("Link Foto")) or bool_from_evidence(rec.get("VISITA"))
            visita_value = 1 if has_evidence or (visita_numeric is not None and visita_numeric > 0) else 0

            rows.append((
                batch_id,
                excel_path.name,
                sheet,
                i,
                Json(to_json_payload(rec), dumps=lambda x: json.dumps(x, ensure_ascii=False)),
                clean_text(rec.get("Codigo Local")),
                clean_text(rec.get("Marca")),
                clean_text(rec.get("Reponedor")),
                clean_text(rec.get("Fecha")),
                clean_text(rec.get("VISITA")),
                fecha_visita,
                calc_week_iso(fecha_visita),
                visita_value,
                clean_text(rec.get("REGISTRO_FUERA_CRUCE")),
                has_evidence,
            ))

        execute_values(
            cur,
            """
            insert into cg_raw.kpione2_raw (
                batch_id,
                source_file,
                source_sheet,
                source_row,
                payload_json,
                codigo_local_raw,
                marca_raw,
                reponedor_raw,
                fecha_raw,
                visita_raw,
                fecha_visita,
                semana_iso,
                visita_value,
                registro_fuera_cruce,
                has_evidence
            ) values %s
            on conflict (batch_id, source_row) do update set
                source_file = excluded.source_file,
                source_sheet = excluded.source_sheet,
                payload_json = excluded.payload_json,
                codigo_local_raw = excluded.codigo_local_raw,
                marca_raw = excluded.marca_raw,
                reponedor_raw = excluded.reponedor_raw,
                fecha_raw = excluded.fecha_raw,
                visita_raw = excluded.visita_raw,
                fecha_visita = excluded.fecha_visita,
                semana_iso = excluded.semana_iso,
                visita_value = excluded.visita_value,
                registro_fuera_cruce = excluded.registro_fuera_cruce,
                has_evidence = excluded.has_evidence,
                ingested_at = now()
            """,
            rows,
            page_size=5000,
        )
    except Exception as exc:
        finalize_batch(cur, batch_id, 0, "cancelled", f"error={exc}")
        raise

    finalize_batch(cur, batch_id, len(rows), "ok", f"rows={len(rows)}")
    return {
        "sheet": sheet,
        "batch_id": batch_id,
        "rows_read": int(len(df)),
        "rows_loaded": int(len(rows)),
    }


def safe_col_name(value: Any, pos: int) -> str:
    raw = clean_text(value)
    if not raw or raw.lower() == "null":
        return f"unnamed_{pos}"
    raw = re.sub(r"\s+", " ", raw)
    return raw


def make_unique(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in names:
        if name not in seen:
            seen[name] = 0
            out.append(name)
        else:
            seen[name] += 1
            out.append(f"{name}__dup{seen[name]}")
    return out


def read_power_app_sheet(excel_path: Path) -> pd.DataFrame:
    raw = read_excel_sheet(excel_path, "DB (POWER_APP)", header=None, dtype=str)

    header_row = raw.iloc[0].tolist()
    clean_cols = make_unique([safe_col_name(v, i) for i, v in enumerate(header_row)])

    df = raw.iloc[1:].copy().reset_index(drop=True)
    df.columns = clean_cols

    resolved: dict[str, str] = {}
    useful = [
        "Marca: Título",
        "Local: Title",
        "Local: LOCAL",
        "Local: DIRECCIÓN",
        "Creado",
        "Creado por",
        "CONTAR",
        "SEM",
        "FECHA",
        "REGISTRO_FUERA_CRUCE",
    ]

    for col in df.columns:
        base = col.split("__dup")[0]
        if base in useful and base not in resolved:
            resolved[base] = col

    df = df.rename(columns={v: k for k, v in resolved.items()})

    required = [
        "Marca: Título",
        "Local: Title",
        "Creado",
        "Creado por",
        "REGISTRO_FUERA_CRUCE",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"POWER_APP missing required sanitized columns: {missing}")

    return df


def load_power_app(cur, excel_path: Path) -> dict[str, Any]:
    sheet = "DB (POWER_APP)"
    df = read_power_app_sheet(excel_path)

    batch_id = register_batch(cur, excel_path.name, sheet)

    rows = []
    for i, rec in enumerate(df.to_dict(orient="records"), start=2):
        rows.append((
            batch_id,
            excel_path.name,
            sheet,
            i,
            Json(to_json_payload(rec), dumps=lambda x: json.dumps(x, ensure_ascii=False)),
            clean_text(rec.get("Local: Title")),
            clean_text(rec.get("Marca: Título")),
            clean_text(rec.get("Creado por")),
            clean_text(rec.get("Creado")),
            clean_text(rec.get("REGISTRO_FUERA_CRUCE")),
        ))

    execute_values(
        cur,
        """
        insert into cg_raw.power_app_raw (
            batch_id,
            source_file,
            source_sheet,
            source_row,
            payload_json,
            title_local_raw,
            titulo_marca_raw,
            persona_raw,
            fecha_visita_raw,
            evidencia_raw
        ) values %s
        on conflict (batch_id, source_row) do update set
            source_file = excluded.source_file,
            source_sheet = excluded.source_sheet,
            payload_json = excluded.payload_json,
            title_local_raw = excluded.title_local_raw,
            titulo_marca_raw = excluded.titulo_marca_raw,
            persona_raw = excluded.persona_raw,
            fecha_visita_raw = excluded.fecha_visita_raw,
            evidencia_raw = excluded.evidencia_raw,
            ingested_at = now()
        """,
        rows,
        page_size=5000,
    )

    finalize_batch(cur, batch_id, len(rows), "ok", f"rows={len(rows)} | header_sanitized=true")
    return {
        "sheet": sheet,
        "batch_id": batch_id,
        "rows_read": int(len(df)),
        "rows_loaded": int(len(rows)),
        "notes": "header_sanitized=true",
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default=DEFAULT_FILE)
    ap.add_argument("--db_url", default=os.getenv("DB_URL_LOAD", "") or os.getenv("DB_URL", ""))
    args = ap.parse_args()

    excel_path = Path(args.excel)
    if not excel_path.exists():
        raise SystemExit(f"No existe el archivo: {excel_path}")
    if not args.db_url:
        raise SystemExit("Falta DB_URL_LOAD/DB_URL")

    db_url = ensure_sslmode(args.db_url)

    result: dict[str, Any] = {
        "loader": LOADER_NAME,
        "source_file": excel_path.name,
        "status": "ok",
    }

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            kpione_info = load_kpione(cur, excel_path)
            kpione2_info = load_kpione2(cur, excel_path)
            power_info = load_power_app(cur, excel_path)
        conn.commit()

    result["kpione"] = kpione_info
    result["kpione2"] = kpione2_info
    result["power_app"] = power_info
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
