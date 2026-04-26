from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, unquote

import pandas as pd
import pg8000.dbapi as pg


DEFAULT_SHEET = "DB (KPIONE)"
DEFAULT_SOURCE_FILE = "CUMPLIMIENTO_FRECUENCIA.xlsx"
DEFAULT_LOADER_NAME = "load_cg_kpione_raw_v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load KPIONE raw sheet into cg_raw.kpione_raw")
    parser.add_argument("--excel", required=True, help="Path to Excel workbook")
    parser.add_argument("--db_url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--sheet", default=DEFAULT_SHEET, help="Sheet name")
    parser.add_argument("--source_file", default=DEFAULT_SOURCE_FILE, help="Logical source file name")
    return parser.parse_args()


def normalize_scalar(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def row_to_payload(row: pd.Series) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.items():
        payload[str(key)] = None if pd.isna(value) else str(value)
    return payload


def connect_pg8000_from_url(db_url: str) -> pg.Connection:
    parsed = urlparse(db_url)

    if parsed.scheme not in ("postgresql", "postgres"):
        raise ValueError(f"Unsupported db_url scheme: {parsed.scheme}")

    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or ""
    port = parsed.port or 5432
    database = (parsed.path or "/postgres").lstrip("/")

    if not user:
        raise ValueError("db_url missing username")
    if not password:
        raise ValueError("db_url missing password")
    if not host:
        raise ValueError("db_url missing host")
    if not database:
        raise ValueError("db_url missing database")

    return pg.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
        ssl_context=True,
    )


def insert_batch(conn: pg.Connection, source_file: str, source_sheet: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            insert into cg_audit.batch_registry (
                source_file, source_sheet, loader_name, loaded_rows, status
            )
            values (%s, %s, %s, 0, 'started')
            returning batch_id
            """,
            (source_file, source_sheet, DEFAULT_LOADER_NAME),
        )
        batch_id = cur.fetchone()[0]
        return int(batch_id)
    finally:
        cur.close()


def finalize_batch(
    conn: pg.Connection,
    batch_id: int,
    loaded_rows: int,
    status: str,
    notes: str = "",
) -> None:
    cur = conn.cursor()
    try:
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
    finally:
        cur.close()


def main() -> None:
    args = parse_args()
    excel_path = Path(args.excel)

    if not excel_path.exists():
        raise FileNotFoundError(f"Excel not found: {excel_path}")

    df = pd.read_excel(excel_path, sheet_name=args.sheet, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    required_cols = [
        "nombre_local",
        "marca",
        "trabajador",
        "Fecha_reg",
        "estado_foto",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"KPIONE missing required columns: {missing}")

    conn = connect_pg8000_from_url(args.db_url)
    conn.autocommit = False

    batch_id = None
    loaded_rows = 0

    try:
        batch_id = insert_batch(conn, args.source_file, args.sheet)

        cur = conn.cursor()
        try:
            for idx, row in df.iterrows():
                payload = row_to_payload(row)

                cur.execute(
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
                    )
                    values (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)
                    """,
                    (
                        batch_id,
                        args.source_file,
                        args.sheet,
                        int(idx) + 2,
                        json.dumps(payload, ensure_ascii=False),
                        normalize_scalar(row.get("nombre_local")),
                        normalize_scalar(row.get("marca")),
                        normalize_scalar(row.get("trabajador")),
                        normalize_scalar(row.get("Fecha_reg")),
                        normalize_scalar(row.get("estado_foto")),
                    ),
                )
                loaded_rows += 1
        finally:
            cur.close()

        finalize_batch(conn, batch_id, loaded_rows, "ok")
        conn.commit()

        print(
            json.dumps(
                {
                    "loader": DEFAULT_LOADER_NAME,
                    "source_file": args.source_file,
                    "source_sheet": args.sheet,
                    "batch_id": batch_id,
                    "rows_read": int(len(df)),
                    "rows_loaded": loaded_rows,
                    "status": "ok",
                    "notes": "",
                },
                ensure_ascii=False,
                indent=2,
            )
        )

    except Exception as exc:
        conn.rollback()
        if batch_id is not None:
            try:
                finalize_batch(conn, batch_id, loaded_rows, "error", str(exc)[:1000])
                conn.commit()
            except Exception:
                conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()