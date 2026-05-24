# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from datetime import date, timedelta
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Callable

import pandas as pd


DEFAULT_FILE = r"data\CUMPLIMIENTO_FRECUENCIA.xlsx"
LOADER_NAME = "load_control_gestion_raw_v17"
LOADER_VERSION = "v17_9C2A"
DEFAULT_CG_V2_REFRESH_TIMEOUT_SECONDS = 1800
SOURCE_ORDER = ["KPIONE", "KPIONE2", "POWER_APP"]
SOURCE_TO_SHEET = {
    "KPIONE": "DB (KPIONE)",
    "KPIONE2": "DB (KPIONE2.0)",
    "POWER_APP": "DB (POWER_APP)",
}
SOURCE_REQUIRED_COLUMNS = {
    "KPIONE": ["nombre_local", "marca", "trabajador", "Fecha_reg", "estado_foto"],
    "KPIONE2": ["Codigo Local", "Marca", "Reponedor", "Fecha", "VISITA"],
}
KPIONE2_NUMERIC_COLUMNS = ["VISITA"]


def ensure_sslmode(db_url: str) -> str:
    if not db_url:
        return db_url
    if "sslmode=" in db_url:
        return db_url
    return db_url + ("&sslmode=require" if "?" in db_url else "?sslmode=require")


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


def week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def sorted_iso(values: set[date] | list[date]) -> list[str]:
    return [value.isoformat() for value in sorted(values)]


def clean_text(v: Any) -> str:
    if pd.isna(v) or v is None:
        return ""
    return str(v).strip()


def is_empty_like(v: Any) -> bool:
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except (TypeError, ValueError):
        pass
    return str(v).strip() == ""


def clean_json_value(v: Any) -> Any:
    if pd.isna(v) or v is None:
        return None
    return str(v)


def to_json_payload(rec: dict[str, Any]) -> dict[str, Any]:
    return {str(k): clean_json_value(v) for k, v in rec.items()}


def get_enabled_sources(force_source: str) -> list[str]:
    if force_source == "all":
        return SOURCE_ORDER.copy()
    return [force_source]


def read_excel_sheet(excel_path: Path, sheet: str, **kwargs: Any) -> pd.DataFrame:
    try:
        return pd.read_excel(excel_path, sheet_name=sheet, **kwargs)
    except ValueError as exc:
        raise ValueError(f"No existe la hoja requerida '{sheet}' en {excel_path.name}") from exc


def normalize_hash_value(v: Any) -> str:
    if pd.isna(v) or v is None:
        return ""
    return str(v).strip()


def compute_sheet_hash(df: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    header_payload = {
        "columns": [normalize_hash_value(col) for col in df.columns.tolist()],
        "rows": int(len(df)),
    }
    digest.update(json.dumps(header_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    digest.update(b"\n")
    for row in df.itertuples(index=False, name=None):
        digest.update(
            json.dumps(
                [normalize_hash_value(value) for value in row],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def parse_incremental_notes(notes: str | None) -> dict[str, Any] | None:
    text = str(notes or "").strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def build_incremental_notes(
    *,
    sheet_hash: str,
    rows_read: int,
    dates_detected: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "incremental": {
            "sheet_hash": sheet_hash,
            "rows_read": int(rows_read),
            "dates_detected": dates_detected,
            "loader_version": LOADER_VERSION,
        }
    }
    if extra:
        payload.update(extra)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def fetch_previous_sheet_hash(cur, *, source_file: str, source_sheet: str) -> tuple[str, dict[str, Any] | None]:
    cur.execute(
        """
        select notes
          from cg_audit.batch_registry
         where source_file = %s
           and source_sheet = %s
           and loader_name = %s
           and status = 'ok'
         order by batch_id desc
         limit 1
        """,
        (source_file, source_sheet, LOADER_NAME),
    )
    row = cur.fetchone()
    if not row:
        return "", None
    payload = parse_incremental_notes(row[0])
    if not payload:
        return "", None
    incremental = payload.get("incremental")
    if not isinstance(incremental, dict):
        return "", payload
    sheet_hash = clean_text(incremental.get("sheet_hash"))
    return sheet_hash, payload


def parse_date_value(v: Any) -> date | None:
    if pd.isna(v) or v is None:
        return None
    if isinstance(v, pd.Timestamp):
        return v.date()
    if isinstance(v, date):
        return v if type(v) is date else v.date()
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
    if is_empty_like(v):
        return None
    text = clean_text(v)
    if not text:
        return None
    normalized = text.replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def numeric_empty_to_null_counts(df: pd.DataFrame, columns: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for column in columns:
        if column not in df.columns:
            continue
        counts[column] = int(sum(1 for value in df[column].tolist() if is_empty_like(value)))
    return counts


def bool_from_evidence(v: Any) -> bool:
    text = clean_text(v).upper()
    if not text:
        return False
    if text in {"0", "NO", "FALSE", "SIN", "N/A"}:
        return False
    return True


def print_source_check(payload: dict[str, Any]) -> None:
    print(json.dumps({"source_check": payload}, ensure_ascii=False, indent=2))


def finalize_source_check(
    payload: dict[str, Any],
    blockers: list[str],
    warnings: list[str],
    notes: list[str],
) -> dict[str, Any]:
    payload["blockers"] = blockers
    payload["warnings"] = warnings
    payload["notes"] = notes
    if blockers:
        payload["final_verdict"] = "block"
    elif warnings:
        payload["final_verdict"] = "warn"
    else:
        payload["final_verdict"] = "ok"
    return payload


def _record_date_range(
    payload: dict[str, Any],
    *,
    key: str,
    series: pd.Series,
    dayfirst: bool,
    critical: bool,
    strict: bool,
    blockers: list[str],
    warnings: list[str],
    parser: Callable[[Any], date | None] | None = None,
    mixed_type_warning_key: str | None = None,
) -> None:
    parsed_dates: list[date] = []
    blank_count = 0
    parse_errors = 0
    raw_types: set[str] = set()

    for value in series.tolist():
        if pd.isna(value) or value is None or not clean_text(value):
            blank_count += 1
            continue

        raw_types.add(type(value).__name__)

        if parser is not None:
            parsed_value = parser(value)
        else:
            parsed_value = pd.to_datetime(value, errors="coerce", dayfirst=dayfirst)
            if pd.isna(parsed_value):
                parsed_value = None
            else:
                parsed_value = parsed_value.date()

        if parsed_value is None:
            parse_errors += 1
            continue

        parsed_dates.append(parsed_value)

    parse_ok_count = len(parsed_dates)
    payload["date_ranges"][key] = {
        "min_date": min(parsed_dates).isoformat() if parse_ok_count else "",
        "max_date": max(parsed_dates).isoformat() if parse_ok_count else "",
        "null_date_count": blank_count,
        "parse_errors_count": parse_errors,
    }
    if mixed_type_warning_key and len(raw_types) > 1:
        warnings.append(mixed_type_warning_key)
    if parse_errors > 0:
        issue = f"date_parse_error:{key}:{parse_errors}"
        if critical and (strict or parse_ok_count == 0):
            blockers.append(issue)
        else:
            warnings.append(issue)


def run_source_check_control_gestion(*, excel_path: str, strict: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "loader": LOADER_NAME,
        "file": str(excel_path),
        "sheet_scope": ["DB (KPIONE)", "DB (KPIONE2.0)", "DB (POWER_APP)"],
        "final_verdict": "ok",
        "blockers": [],
        "warnings": [],
        "rows_checked": {},
        "date_ranges": {},
        "numeric_empty_to_null_count": {},
        "notes": [],
    }
    blockers: list[str] = []
    warnings: list[str] = []
    notes: list[str] = []

    workbook_path = Path(excel_path)
    if not workbook_path.exists():
        blockers.append(f"missing_workbook:{workbook_path}")
        return finalize_source_check(payload, blockers, warnings, notes)

    try:
        book = pd.ExcelFile(workbook_path)
    except Exception as exc:
        blockers.append(f"unreadable_workbook:{type(exc).__name__}")
        notes.append(str(exc))
        return finalize_source_check(payload, blockers, warnings, notes)

    required_sheets = {
        "DB (KPIONE)": ["nombre_local", "marca", "trabajador", "Fecha_reg", "estado_foto"],
        "DB (KPIONE2.0)": ["Codigo Local", "Marca", "Reponedor", "Fecha", "VISITA"],
    }
    for sheet in ("DB (KPIONE)", "DB (KPIONE2.0)", "DB (POWER_APP)"):
        if sheet not in book.sheet_names:
            blockers.append(f"missing_sheet:{sheet}")

    if blockers:
        notes.append("available_sheets=" + ", ".join(book.sheet_names))
        return finalize_source_check(payload, blockers, warnings, notes)

    try:
        kpione_df = read_excel_sheet(workbook_path, "DB (KPIONE)", dtype=str)
        kpione_df.columns = [str(c).strip() for c in kpione_df.columns]
        payload["rows_checked"]["DB (KPIONE)"] = int(len(kpione_df))
        missing = [c for c in required_sheets["DB (KPIONE)"] if c not in kpione_df.columns]
        if missing:
            blockers.append("missing_critical_columns:DB (KPIONE):" + ",".join(missing))
        else:
            _record_date_range(
                payload,
                key="DB (KPIONE).Fecha_reg",
                series=kpione_df["Fecha_reg"],
                dayfirst=False,
                critical=True,
                strict=strict,
                blockers=blockers,
                warnings=warnings,
            )
        if "FECHA" in kpione_df.columns:
            _record_date_range(
                payload,
                key="DB (KPIONE).FECHA",
                series=kpione_df["FECHA"],
                dayfirst=True,
                critical=False,
                strict=strict,
                blockers=blockers,
                warnings=warnings,
            )
        notes.append("DB (KPIONE): SEMANA and VISITAS SEMANA are not date fields")
    except Exception as exc:
        blockers.append(f"sheet_read_error:DB (KPIONE):{type(exc).__name__}")
        notes.append(str(exc))

    try:
        kpione2_df = read_excel_sheet(workbook_path, "DB (KPIONE2.0)", dtype=str)
        kpione2_df.columns = [str(c).strip() for c in kpione2_df.columns]
        payload["rows_checked"]["DB (KPIONE2.0)"] = int(len(kpione2_df))
        payload["numeric_empty_to_null_count"]["DB (KPIONE2.0)"] = numeric_empty_to_null_counts(
            kpione2_df,
            KPIONE2_NUMERIC_COLUMNS,
        )
        missing = [c for c in required_sheets["DB (KPIONE2.0)"] if c not in kpione2_df.columns]
        if missing:
            blockers.append("missing_critical_columns:DB (KPIONE2.0):" + ",".join(missing))
        else:
            _record_date_range(
                payload,
                key="DB (KPIONE2.0).Fecha",
                series=kpione2_df["Fecha"],
                dayfirst=False,
                critical=True,
                strict=strict,
                blockers=blockers,
                warnings=warnings,
                parser=parse_date_value,
                mixed_type_warning_key="mixed_date_types:DB (KPIONE2.0).Fecha",
            )
        notes.append("DB (KPIONE2.0): SEMANA is not a date field")
    except Exception as exc:
        blockers.append(f"sheet_read_error:DB (KPIONE2.0):{type(exc).__name__}")
        notes.append(str(exc))

    try:
        power_raw = read_excel_sheet(workbook_path, "DB (POWER_APP)", header=None, dtype=str)
        raw_header = [safe_col_name(v, i) for i, v in enumerate(power_raw.iloc[0].tolist())]
        payload["rows_checked"]["DB (POWER_APP)"] = max(int(len(power_raw) - 1), 0)
        unnamed_headers = [col for col in raw_header if col.startswith("unnamed_")]
        numeric_headers = [col for col in raw_header if col.replace(".", "", 1).isdigit()]
        if unnamed_headers:
            warnings.append("power_app_unnamed_headers_present")
        if numeric_headers:
            warnings.append("power_app_numeric_headers_present")

        power_df = read_power_app_sheet(workbook_path)
        required_power = [
            "Marca: Título",
            "Local: Title",
            "Creado",
            "Creado por",
            "REGISTRO_FUERA_CRUCE",
        ]
        missing = [c for c in required_power if c not in power_df.columns]
        if missing:
            blockers.append("missing_critical_columns:DB (POWER_APP):" + ",".join(missing))
        else:
            _record_date_range(
                payload,
                key="DB (POWER_APP).Creado",
                series=power_df["Creado"],
                dayfirst=False,
                critical=True,
                strict=strict,
                blockers=blockers,
                warnings=warnings,
            )
        if "FECHA" in power_df.columns:
            _record_date_range(
                payload,
                key="DB (POWER_APP).FECHA",
                series=power_df["FECHA"],
                dayfirst=True,
                critical=False,
                strict=strict,
                blockers=blockers,
                warnings=warnings,
            )
        notes.append("DB (POWER_APP): SEM/SEMANA are not date fields")
    except Exception as exc:
        blockers.append(f"sheet_read_error:DB (POWER_APP):{type(exc).__name__}")
        notes.append(str(exc))

    notes.append("source_check runs before DB")
    return finalize_source_check(payload, blockers, warnings, notes)


def read_kpione_df(excel_path: Path) -> pd.DataFrame:
    df = read_excel_sheet(excel_path, SOURCE_TO_SHEET["KPIONE"], dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in SOURCE_REQUIRED_COLUMNS["KPIONE"] if c not in df.columns]
    if missing:
        raise ValueError(f"KPIONE missing required columns: {missing}")
    return df


def read_kpione2_df(excel_path: Path) -> pd.DataFrame:
    df = read_excel_sheet(excel_path, SOURCE_TO_SHEET["KPIONE2"], dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in SOURCE_REQUIRED_COLUMNS["KPIONE2"] if c not in df.columns]
    if missing:
        raise ValueError(f"KPIONE2 missing required columns: {missing}")
    return df


def collect_dates_detected(source_key: str, df: pd.DataFrame) -> tuple[dict[str, Any], list[str]]:
    payload: dict[str, Any] = {"date_ranges": {}}
    warnings: list[str] = []
    blockers: list[str] = []
    if source_key == "KPIONE":
        _record_date_range(
            payload,
            key="DB (KPIONE).Fecha_reg",
            series=df["Fecha_reg"],
            dayfirst=False,
            critical=False,
            strict=False,
            blockers=blockers,
            warnings=warnings,
        )
        if "FECHA" in df.columns:
            _record_date_range(
                payload,
                key="DB (KPIONE).FECHA",
                series=df["FECHA"],
                dayfirst=True,
                critical=False,
                strict=False,
                blockers=blockers,
                warnings=warnings,
            )
    elif source_key == "KPIONE2":
        _record_date_range(
            payload,
            key="DB (KPIONE2.0).Fecha",
            series=df["Fecha"],
            dayfirst=False,
            critical=False,
            strict=False,
            blockers=blockers,
            warnings=warnings,
            parser=parse_date_value,
            mixed_type_warning_key="mixed_date_types:DB (KPIONE2.0).Fecha",
        )
    elif source_key == "POWER_APP":
        _record_date_range(
            payload,
            key="DB (POWER_APP).Creado",
            series=df["Creado"],
            dayfirst=False,
            critical=False,
            strict=False,
            blockers=blockers,
            warnings=warnings,
        )
        if "FECHA" in df.columns:
            _record_date_range(
                payload,
                key="DB (POWER_APP).FECHA",
                series=df["FECHA"],
                dayfirst=True,
                critical=False,
                strict=False,
                blockers=blockers,
                warnings=warnings,
            )
    return payload["date_ranges"], warnings


def collect_incremental_affected_dates(source_key: str, df: pd.DataFrame) -> tuple[list[str], list[str]]:
    if source_key == "KPIONE":
        return [], []
    if source_key == "KPIONE2":
        driver_column = "Fecha"
    elif source_key == "POWER_APP":
        driver_column = "Creado"
    else:
        return [], [f"incremental_unsupported_source:{source_key}"]

    if driver_column not in df.columns:
        return [], [f"incremental_missing_date_driver:{source_key}:{driver_column}"]

    dates: set[str] = set()
    parse_errors = 0
    for value in df[driver_column].tolist():
        parsed = parse_date_value(value)
        if parsed is None:
            if clean_text(value):
                parse_errors += 1
            continue
        dates.add(parsed.isoformat())

    warnings: list[str] = []
    if parse_errors:
        warnings.append(f"incremental_date_parse_errors:{source_key}:{driver_column}:{parse_errors}")
    return sorted(dates), warnings


def load_source_dataframe(source_key: str, excel_path: Path) -> pd.DataFrame:
    if source_key == "KPIONE":
        return read_kpione_df(excel_path)
    if source_key == "KPIONE2":
        return read_kpione2_df(excel_path)
    if source_key == "POWER_APP":
        return read_power_app_sheet(excel_path)
    raise ValueError(f"Unsupported source_key={source_key}")


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


def register_cancelled_batch_after_error(cur, source_file: str, source_sheet: str, exc: Exception) -> None:
    conn = getattr(cur, "connection", None)
    if conn is not None:
        conn.rollback()
    cancelled_batch_id = register_batch(cur, source_file, source_sheet)
    finalize_batch(cur, cancelled_batch_id, 0, "cancelled", f"error={exc}")
    if conn is not None:
        conn.commit()


def load_kpione(
    cur,
    excel_path: Path,
    *,
    df: pd.DataFrame,
    incremental_notes: str,
) -> dict[str, Any]:
    from psycopg2.extras import Json, execute_values

    sheet = SOURCE_TO_SHEET["KPIONE"]

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

    finalize_batch(cur, batch_id, len(rows), "ok", incremental_notes)
    return {
        "sheet": sheet,
        "batch_id": batch_id,
        "rows_read": int(len(df)),
        "rows_loaded": int(len(rows)),
    }


def load_kpione2(
    cur,
    excel_path: Path,
    *,
    df: pd.DataFrame,
    incremental_notes: str,
) -> dict[str, Any]:
    from psycopg2.extras import Json, execute_values

    sheet = SOURCE_TO_SHEET["KPIONE2"]

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
                visita_numeric,
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
        try:
            register_cancelled_batch_after_error(cur, excel_path.name, sheet, exc)
        except Exception as cancel_exc:
            try:
                exc.add_note(f"cancelled_batch_record_failed={cancel_exc}")
            except AttributeError:
                pass
        raise

    finalize_batch(cur, batch_id, len(rows), "ok", incremental_notes)
    return {
        "sheet": sheet,
        "batch_id": batch_id,
        "rows_read": int(len(df)),
        "rows_loaded": int(len(rows)),
        "numeric_empty_to_null_count": numeric_empty_to_null_counts(df, KPIONE2_NUMERIC_COLUMNS),
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


def load_power_app(
    cur,
    excel_path: Path,
    *,
    df: pd.DataFrame,
    incremental_notes: str,
) -> dict[str, Any]:
    from psycopg2.extras import Json, execute_values

    sheet = SOURCE_TO_SHEET["POWER_APP"]

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

    finalize_batch(cur, batch_id, len(rows), "ok", incremental_notes)
    return {
        "sheet": sheet,
        "batch_id": batch_id,
        "rows_read": int(len(df)),
        "rows_loaded": int(len(rows)),
    }


def load_source_to_db(
    source_key: str,
    *,
    cur,
    excel_path: Path,
    df: pd.DataFrame,
    incremental_notes: str,
) -> dict[str, Any]:
    if source_key == "KPIONE":
        return load_kpione(cur, excel_path, df=df, incremental_notes=incremental_notes)
    if source_key == "KPIONE2":
        return load_kpione2(cur, excel_path, df=df, incremental_notes=incremental_notes)
    if source_key == "POWER_APP":
        return load_power_app(cur, excel_path, df=df, incremental_notes=incremental_notes)
    raise ValueError(f"Unsupported source_key={source_key}")


def _mv_refresh_final_status(refresh_result: dict[str, Any]) -> tuple[str, str, str, bool]:
    refresh_status = str(refresh_result.get("refresh_status") or refresh_result.get("status") or "unknown")
    validation_status = str(refresh_result.get("validation_status") or "not_requested")
    overall_status = str(refresh_result.get("status") or "")

    if overall_status == "ok":
        return "ok", validation_status, "load_ok_refresh_ok", False
    if overall_status.startswith("validate") or validation_status in {"failed", "failed_timeout", "failed_error"}:
        return "ok", validation_status, "load_ok_refresh_ok_validate_failed", False
    if overall_status.startswith("analyze"):
        return refresh_status, validation_status, "load_ok_refresh_ok_analyze_failed", False
    return refresh_status, validation_status, "load_ok_refresh_failed", True


def _run_incremental_refresh_from_loader(
    *,
    db_url: str,
    affected_dates: list[str],
    affected_weeks: list[str],
    validate: bool,
    apply: bool,
    confirm_real_apply: bool,
    post_apply_validate: bool,
    safety_window_weeks: int,
    statement_timeout_seconds: int,
) -> dict[str, Any]:
    if not affected_dates and not affected_weeks:
        return {
            "status": "skipped",
            "final_status": "incremental_skipped_no_scope",
            "dry_run": not apply,
            "apply": bool(apply),
            "affected_dates": [],
            "affected_weeks": [],
            "warnings": [],
            "real_apply_enabled": False,
        }
    if not db_url:
        return {
            "status": "blocked" if apply else "skipped",
            "final_status": "incremental_blocked_missing_db_url" if apply else "incremental_skipped_no_db_url",
            "dry_run": not apply,
            "apply": bool(apply),
            "affected_dates": affected_dates,
            "affected_weeks": affected_weeks,
            "warnings": ["incremental_db_url_missing"],
            "real_apply_enabled": False,
        }
    if apply and not confirm_real_apply:
        return {
            "status": "blocked",
            "final_status": "incremental_blocked_missing_confirm",
            "dry_run": False,
            "apply": True,
            "affected_dates": affected_dates,
            "affected_weeks": affected_weeks,
            "warnings": ["incremental_apply_requires_confirm_real_apply"],
            "real_apply_enabled": False,
        }

    from refresh_control_gestion_v2_incremental import (
        build_week_scope,
        run_incremental_apply,
        run_incremental_dry_run,
    )

    affected_date_values = {date.fromisoformat(value) for value in affected_dates}
    affected_week_values = {date.fromisoformat(value) for value in affected_weeks}
    week_scope = build_week_scope(
        affected_date_values,
        affected_week_values,
        safety_window_weeks=safety_window_weeks,
    )
    if apply:
        return run_incremental_apply(
            db_url=db_url,
            week_scope=week_scope,
            statement_timeout_seconds=statement_timeout_seconds,
            post_apply_validate=post_apply_validate,
        )

    return run_incremental_dry_run(
        db_url=db_url,
        week_scope=week_scope,
        validate=validate,
        require_complete_safety_window=False,
        post_apply_validate=post_apply_validate,
        statement_timeout_seconds=statement_timeout_seconds,
    )


def build_incremental_scope_guard(
    *,
    selected_dates: list[str],
    selected_weeks: list[str],
    max_auto_dates: int,
    allow_wide_scope: bool,
    apply_requested: bool,
) -> dict[str, Any]:
    selected_dates_count = len(selected_dates)
    selected_weeks_count = len(selected_weeks)
    warnings: list[str] = []
    blockers: list[str] = []
    status = "ok"

    if selected_dates_count == 0 and selected_weeks_count == 0:
        status = "blocked_no_scope" if apply_requested else "skipped_no_scope"
        if apply_requested:
            blockers.append("incremental_apply_requires_affected_date_or_week")
    elif selected_dates_count > max_auto_dates and not allow_wide_scope:
        warning = f"incremental_scope_wide_dates:{selected_dates_count}>{max_auto_dates}"
        warnings.append(warning)
        if apply_requested:
            status = "blocked_wide_scope"
            blockers.append(warning)
        else:
            status = "warn_wide_scope_dry_run_allowed"

    return {
        "status": status,
        "selected_dates_count": selected_dates_count,
        "selected_weeks_count": selected_weeks_count,
        "max_auto_incremental_dates": int(max_auto_dates),
        "allow_wide_incremental_scope": bool(allow_wide_scope),
        "warnings": warnings,
        "blockers": blockers,
    }


def _apply_incremental_loader_status(result: dict[str, Any]) -> None:
    incremental_result = result.get("incremental_refresh")
    if not isinstance(incremental_result, dict):
        return

    incremental_status = str(incremental_result.get("status") or "unknown")
    incremental_final_status = str(incremental_result.get("final_status") or "")
    result["incremental_status"] = incremental_status
    result["incremental_final_status"] = incremental_final_status
    result["incremental_warnings"] = list(dict.fromkeys(
        list(result.get("incremental_warnings", [])) + list(incremental_result.get("warnings", []))
    ))
    result["affected_dates"] = list(incremental_result.get("requested_affected_dates") or incremental_result.get("affected_dates") or result.get("affected_dates") or [])
    result["affected_weeks"] = list(incremental_result.get("validation_weeks") or incremental_result.get("affected_weeks") or [])

    apply_requested = bool(result.get("incremental_apply_requested"))

    if incremental_final_status == "apply_ok":
        result["final_status"] = "load_ok_incremental_apply_ok"
        result["status"] = "ok"
        result["incremental_apply_executed"] = True
    elif incremental_final_status.startswith("apply_failed") or incremental_final_status == "apply_committed_with_post_commit_error":
        result["final_status"] = "load_ok_incremental_apply_failed"
        result["status"] = "load_ok_incremental_apply_failed"
        result["incremental_apply_executed"] = True
    elif incremental_final_status in {"incremental_blocked_scope", "incremental_blocked_missing_db_url"}:
        result["final_status"] = "load_ok_incremental_blocked_scope"
        result["status"] = "load_ok_incremental_blocked_scope"
    elif incremental_final_status == "incremental_blocked_missing_confirm":
        result["final_status"] = "load_ok_incremental_blocked_missing_confirm"
        result["status"] = "load_ok_incremental_blocked_missing_confirm"
    elif incremental_final_status == "incremental_skipped_no_scope":
        result["final_status"] = "load_ok_incremental_blocked_scope" if apply_requested else "load_ok_incremental_skipped_no_affected_dates"
        result["status"] = result["final_status"] if apply_requested else "ok"
    elif incremental_status == "ok":
        result["final_status"] = "load_ok_incremental_apply_ok" if apply_requested else "load_ok_incremental_dry_run_ok"
        result["status"] = "ok"
    elif incremental_status == "warn":
        result["final_status"] = "load_ok_incremental_apply_ok" if apply_requested else "load_ok_incremental_dry_run_warn"
        result["status"] = "ok"
    elif incremental_final_status == "incremental_skipped_no_affected_dates":
        result["final_status"] = "load_ok_incremental_skipped_no_affected_dates"
        result["status"] = "ok"
    elif incremental_final_status == "incremental_skipped_no_db_url":
        result["final_status"] = "load_ok_incremental_skipped_no_db_url"
        result["status"] = "ok"
    else:
        result["final_status"] = "load_ok_incremental_dry_run_failed"
        result["status"] = "load_ok_incremental_dry_run_failed"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default=DEFAULT_FILE)
    ap.add_argument("--db_url", default=os.getenv("DB_URL_LOAD", "") or os.getenv("DB_URL", ""))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force-source", choices=["KPIONE", "KPIONE2", "POWER_APP", "all"], default="all")
    ap.add_argument("--skip-unchanged-sheets", action="store_true")
    ap.add_argument("--skip-mv-refresh-if-no-change", action="store_true")
    ap.add_argument("--refresh-cg-v2-mv", action="store_true")
    ap.add_argument("--validate-cg-v2-mv", action="store_true")
    ap.add_argument("--cg-v2-refresh-timeout-seconds", type=int, default=DEFAULT_CG_V2_REFRESH_TIMEOUT_SECONDS)
    ap.add_argument("--refresh-cg-v2-incremental", action="store_true")
    ap.add_argument("--validate-cg-v2-incremental", action="store_true")
    ap.add_argument("--dry-run-incremental", action="store_true")
    ap.add_argument("--apply-cg-v2-incremental", action="store_true")
    ap.add_argument("--confirm-real-apply", action="store_true")
    ap.add_argument("--post-apply-validate", action="store_true")
    ap.add_argument("--affected-date", action="append", type=parse_iso_date, default=[])
    ap.add_argument("--affected-week", action="append", type=parse_iso_date, default=[])
    ap.add_argument("--allow-wide-incremental-scope", action="store_true")
    ap.add_argument("--max-auto-incremental-dates", type=int, default=3)
    ap.add_argument("--incremental-safety-window-weeks", type=int, default=1)
    ap.add_argument("--fallback-full-refresh", action="store_true")
    ap.add_argument("--skip-source-check", action="store_true")
    ap.add_argument("--source-check-strict", action="store_true")
    ap.add_argument("--source-check-only", action="store_true")
    args = ap.parse_args()

    excel_path = Path(args.excel)
    enabled_sources = get_enabled_sources(args.force_source)
    enabled_sheets = [SOURCE_TO_SHEET[source] for source in enabled_sources]
    if args.skip_source_check:
        source_check = {
            "loader": LOADER_NAME,
            "file": str(excel_path),
            "sheet_scope": enabled_sheets,
            "final_verdict": "warn",
            "blockers": [],
            "warnings": ["source_check_skipped_by_flag"],
            "rows_checked": {},
            "date_ranges": {},
            "notes": ["source_check disabled by --skip-source-check"],
        }
    else:
        source_check = run_source_check_control_gestion(
            excel_path=args.excel,
            strict=bool(args.source_check_strict),
        )
    print_source_check(source_check)

    if source_check["final_verdict"] == "block":
        raise SystemExit(1)

    if args.source_check_only:
        return

    if not excel_path.exists():
        raise SystemExit(f"No existe el archivo: {excel_path}")
    if args.validate_cg_v2_mv and not args.refresh_cg_v2_mv:
        raise SystemExit("--validate-cg-v2-mv requiere --refresh-cg-v2-mv")
    if args.validate_cg_v2_incremental and not args.refresh_cg_v2_incremental:
        raise SystemExit("--validate-cg-v2-incremental requiere --refresh-cg-v2-incremental")
    if args.dry_run_incremental and not args.refresh_cg_v2_incremental:
        raise SystemExit("--dry-run-incremental requiere --refresh-cg-v2-incremental")
    if args.apply_cg_v2_incremental and not args.refresh_cg_v2_incremental:
        raise SystemExit("--apply-cg-v2-incremental requiere --refresh-cg-v2-incremental")
    if args.confirm_real_apply and not args.apply_cg_v2_incremental:
        raise SystemExit("--confirm-real-apply requiere --apply-cg-v2-incremental")
    if args.post_apply_validate and not args.apply_cg_v2_incremental:
        raise SystemExit("--post-apply-validate requiere --apply-cg-v2-incremental")
    if args.apply_cg_v2_incremental and args.dry_run:
        raise SystemExit("--apply-cg-v2-incremental no puede combinarse con --dry-run del loader")
    if args.max_auto_incremental_dates < 1:
        raise SystemExit("--max-auto-incremental-dates debe ser >= 1")
    if args.incremental_safety_window_weeks < 0:
        raise SystemExit("--incremental-safety-window-weeks debe ser >= 0")

    db_url = ensure_sslmode(args.db_url)
    if not args.dry_run and not db_url:
        raise SystemExit("Falta DB_URL_LOAD/DB_URL")

    result: dict[str, Any] = {
        "loader": LOADER_NAME,
        "mode": "sheet-hash-skip" if args.skip_unchanged_sheets else "full-replace-all",
        "dry_run": bool(args.dry_run),
        "force_source": args.force_source,
        "source_file": excel_path.name,
        "sources": {},
        "source_check_status": source_check["final_verdict"],
        "load_status": "pending",
        "sources_changed": [],
        "affected_dates": [],
        "affected_weeks": [],
        "should_refresh_mv": False,
        "mv_refresh_skipped_reason": "",
        "mv_refresh_status": "not_requested",
        "mv_validation_status": "not_requested" if not args.validate_cg_v2_mv else "pending",
        "web_data_status": "unknown",
        "pending_refresh": False,
        "mv_stale": False,
        "cg_v2_refresh_timeout_seconds": int(args.cg_v2_refresh_timeout_seconds),
        "incremental_requested": bool(args.refresh_cg_v2_incremental),
        "incremental_dry_run": bool(args.refresh_cg_v2_incremental),
        "dry_run_incremental_requested": bool(args.dry_run_incremental),
        "incremental_apply_requested": bool(args.apply_cg_v2_incremental),
        "incremental_apply_executed": False,
        "incremental_post_apply_validate_requested": bool(args.post_apply_validate),
        "affected_dates_detected": [],
        "affected_dates_selected_for_incremental": [],
        "affected_weeks_selected_for_incremental": [],
        "incremental_scope_guard": {
            "status": "not_evaluated",
            "selected_dates_count": 0,
            "selected_weeks_count": 0,
            "max_auto_incremental_dates": int(args.max_auto_incremental_dates),
            "allow_wide_incremental_scope": bool(args.allow_wide_incremental_scope),
            "warnings": [],
            "blockers": [],
        },
        "incremental_status": "not_requested",
        "incremental_final_status": "not_requested",
        "fallback_full_refresh_requested": bool(args.fallback_full_refresh),
        "fallback_full_refresh_status": "not_requested",
        "final_status": "started",
        "incremental_warnings": [],
        "status": "ok",
    }
    incremental_affected_dates: set[str] = set()

    conn = None
    cur = None
    try:
        if db_url and (args.skip_unchanged_sheets or not args.dry_run):
            import psycopg2
            try:
                conn = psycopg2.connect(db_url)
                if args.dry_run:
                    conn.set_session(readonly=True, autocommit=False)
                cur = conn.cursor()
            except Exception:
                if args.dry_run:
                    conn = None
                    cur = None
                else:
                    raise

        for source_key in SOURCE_ORDER:
            source_result: dict[str, Any] = {
                "enabled": source_key in enabled_sources,
                "sheet": SOURCE_TO_SHEET[source_key],
                "rows_read": 0,
                "sheet_hash": "",
                "previous_sheet_hash": "",
                "changed": False,
                "rows_skipped_existing": 0,
                "rows_loaded": 0,
                "would_load": 0,
                "dates_detected": {},
                "affected_dates": [],
                "numeric_empty_to_null_count": {},
            }
            result["sources"][source_key] = source_result
            if source_key not in enabled_sources:
                continue

            df = load_source_dataframe(source_key, excel_path)
            dates_detected, date_warnings = collect_dates_detected(source_key, df)
            source_result["rows_read"] = int(len(df))
            source_result["dates_detected"] = dates_detected
            if source_key == "KPIONE2":
                source_result["numeric_empty_to_null_count"] = numeric_empty_to_null_counts(
                    df,
                    KPIONE2_NUMERIC_COLUMNS,
                )
            source_result["sheet_hash"] = compute_sheet_hash(df)
            for warning in date_warnings:
                if warning not in result["incremental_warnings"]:
                    result["incremental_warnings"].append(warning)

            changed = True
            previous_hash = ""
            if args.skip_unchanged_sheets:
                if cur is None:
                    warning_key = f"incremental_metadata_unavailable:{source_key}"
                    if warning_key not in result["incremental_warnings"]:
                        result["incremental_warnings"].append(warning_key)
                else:
                    try:
                        previous_hash, previous_payload = fetch_previous_sheet_hash(
                            cur,
                            source_file=excel_path.name,
                            source_sheet=SOURCE_TO_SHEET[source_key],
                        )
                        source_result["previous_sheet_hash"] = previous_hash
                        if previous_payload is None and previous_hash == "":
                            cur.execute(
                                """
                                select 1
                                  from cg_audit.batch_registry
                                 where source_file = %s
                                   and source_sheet = %s
                                   and loader_name = %s
                                   and status = 'ok'
                                 limit 1
                                """,
                                (excel_path.name, SOURCE_TO_SHEET[source_key], LOADER_NAME),
                            )
                            if cur.fetchone() is not None:
                                warning_key = f"incremental_metadata_unavailable:{source_key}"
                                if warning_key not in result["incremental_warnings"]:
                                    result["incremental_warnings"].append(warning_key)
                        changed = not bool(previous_hash and previous_hash == source_result["sheet_hash"])
                    except Exception:
                        changed = True
                        warning_key = f"incremental_metadata_unavailable:{source_key}"
                        if warning_key not in result["incremental_warnings"]:
                            result["incremental_warnings"].append(warning_key)

                if not changed:
                    source_result["rows_skipped_existing"] = source_result["rows_read"]

            else:
                changed = True

            source_result["changed"] = changed
            if changed:
                result["sources_changed"].append(source_key)
                affected_dates, affected_warnings = collect_incremental_affected_dates(source_key, df)
                source_result["affected_dates"] = affected_dates
                incremental_affected_dates.update(affected_dates)
                for warning in affected_warnings:
                    if warning not in result["incremental_warnings"]:
                        result["incremental_warnings"].append(warning)

            if args.dry_run:
                source_result["would_load"] = source_result["rows_read"] if changed else 0
                continue

            if args.skip_unchanged_sheets and not changed:
                continue

            if cur is None:
                raise RuntimeError("NO_DB_CURSOR_AVAILABLE")

            incremental_notes = build_incremental_notes(
                sheet_hash=source_result["sheet_hash"],
                rows_read=source_result["rows_read"],
                dates_detected=source_result["dates_detected"],
            )
            load_info = load_source_to_db(
                source_key,
                cur=cur,
                excel_path=excel_path,
                df=df,
                incremental_notes=incremental_notes,
            )
            source_result["rows_loaded"] = int(load_info["rows_loaded"])
            source_result["batch_id"] = int(load_info["batch_id"])
            if "numeric_empty_to_null_count" in load_info:
                source_result["numeric_empty_to_null_count"] = load_info["numeric_empty_to_null_count"]

        if conn is not None and not args.dry_run:
            conn.commit()
        result["load_status"] = "ok"
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    result["affected_dates"] = sorted(incremental_affected_dates)
    result["affected_dates_detected"] = list(result["affected_dates"])
    if args.refresh_cg_v2_incremental:
        manual_affected_dates = sorted_iso(set(args.affected_date or []))
        manual_affected_weeks = sorted_iso({week_start(value) for value in (args.affected_week or [])})
        selected_dates = manual_affected_dates if manual_affected_dates else list(result["affected_dates_detected"])
        selected_week_values = {
            week_start(date.fromisoformat(value)) for value in selected_dates
        }
        selected_week_values.update(date.fromisoformat(value) for value in manual_affected_weeks)
        selected_weeks = sorted_iso(selected_week_values)
        result["affected_dates_selected_for_incremental"] = selected_dates
        result["affected_weeks_selected_for_incremental"] = selected_weeks
        result["incremental_dry_run"] = not bool(args.apply_cg_v2_incremental)
        result["dry_run_incremental_forced"] = not bool(args.apply_cg_v2_incremental)
        result["incremental_apply_requested"] = bool(args.apply_cg_v2_incremental)
        result["incremental_post_apply_validate_requested"] = bool(args.post_apply_validate)
        result["fallback_full_refresh_status"] = (
            "available_not_executed_in_N3" if args.fallback_full_refresh else "not_requested"
        )
        result["incremental_scope_guard"] = build_incremental_scope_guard(
            selected_dates=selected_dates,
            selected_weeks=selected_weeks,
            max_auto_dates=args.max_auto_incremental_dates,
            allow_wide_scope=args.allow_wide_incremental_scope,
            apply_requested=args.apply_cg_v2_incremental,
        )
        result["incremental_warnings"] = list(dict.fromkeys(
            result["incremental_warnings"] + list(result["incremental_scope_guard"].get("warnings", []))
        ))
        if args.apply_cg_v2_incremental and result["incremental_scope_guard"]["status"] == "blocked_wide_scope":
            result["incremental_refresh"] = {
                "status": "blocked",
                "final_status": "incremental_blocked_scope",
                "dry_run": False,
                "apply": True,
                "affected_dates": selected_dates,
                "affected_weeks": selected_weeks,
                "warnings": list(result["incremental_scope_guard"].get("warnings", [])),
                "blockers": list(result["incremental_scope_guard"].get("blockers", [])),
                "real_apply_enabled": False,
            }
        else:
            try:
                result["incremental_refresh"] = _run_incremental_refresh_from_loader(
                    db_url=db_url,
                    affected_dates=selected_dates,
                    affected_weeks=selected_weeks,
                    validate=args.validate_cg_v2_incremental,
                    apply=args.apply_cg_v2_incremental,
                    confirm_real_apply=args.confirm_real_apply,
                    post_apply_validate=args.post_apply_validate,
                    safety_window_weeks=args.incremental_safety_window_weeks,
                    statement_timeout_seconds=args.cg_v2_refresh_timeout_seconds,
                )
            except Exception as exc:
                result["incremental_refresh"] = {
                    "status": "error",
                    "final_status": "apply_failed_rolled_back" if args.apply_cg_v2_incremental else "dry_run_error",
                    "dry_run": not bool(args.apply_cg_v2_incremental),
                    "apply": bool(args.apply_cg_v2_incremental),
                    "affected_dates": selected_dates,
                    "affected_weeks": selected_weeks,
                    "warnings": [],
                    "error": str(exc),
                    "real_apply_enabled": False,
                }
        _apply_incremental_loader_status(result)

    if args.dry_run:
        result["mv_refresh_skipped_reason"] = "dry_run"
        result["mv_refresh_status"] = "skipped"
        result["mv_validation_status"] = "not_requested"
        result["web_data_status"] = "dry_run_no_change"
        if not args.refresh_cg_v2_incremental:
            result["final_status"] = "dry_run_ok"
    elif args.skip_mv_refresh_if_no_change and not result["sources_changed"]:
        result["mv_refresh_skipped_reason"] = "no_source_change"
        result["mv_refresh_status"] = "skipped"
        result["mv_validation_status"] = "not_requested"
        result["web_data_status"] = "fresh_no_source_change"
        if not args.refresh_cg_v2_incremental:
            result["final_status"] = "load_ok_no_source_change"
    elif args.refresh_cg_v2_incremental and not args.refresh_cg_v2_mv:
        result["mv_refresh_skipped_reason"] = (
            "incremental_apply_requested" if args.apply_cg_v2_incremental else "incremental_dry_run_requested"
        )
        result["mv_refresh_status"] = "deferred" if result["sources_changed"] else "not_requested"
        result["mv_validation_status"] = "not_requested"
        result["pending_refresh"] = bool(result["sources_changed"])
        result["mv_stale"] = bool(result["sources_changed"])
        result["web_data_status"] = "stale_pending_refresh" if result["sources_changed"] else "fresh_no_source_change"
    elif not args.refresh_cg_v2_mv:
        result["mv_refresh_skipped_reason"] = "refresh_flag_not_requested"
        result["mv_refresh_status"] = "deferred" if result["sources_changed"] else "not_requested"
        result["mv_validation_status"] = "not_requested"
        result["pending_refresh"] = bool(result["sources_changed"])
        result["mv_stale"] = bool(result["sources_changed"])
        result["web_data_status"] = "stale_pending_refresh" if result["sources_changed"] else "fresh_no_source_change"
        result["final_status"] = "load_ok_refresh_deferred" if result["sources_changed"] else "load_ok_refresh_not_requested"
    else:
        result["should_refresh_mv"] = True

    if args.refresh_cg_v2_incremental and result["status"] in {
        "load_ok_incremental_dry_run_failed",
        "load_ok_incremental_apply_failed",
        "load_ok_incremental_blocked_scope",
        "load_ok_incremental_blocked_missing_confirm",
    }:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(1)

    if result["should_refresh_mv"]:
        try:
            from refresh_control_gestion_v2_mv import run_cg_v2_mv_refresh

            result["cg_v2_mv_refresh"] = run_cg_v2_mv_refresh(
                db_url=args.db_url,
                validate=args.validate_cg_v2_mv,
                statement_timeout_seconds=args.cg_v2_refresh_timeout_seconds,
            )
            (
                result["mv_refresh_status"],
                result["mv_validation_status"],
                result["final_status"],
                result["mv_stale"],
            ) = _mv_refresh_final_status(result["cg_v2_mv_refresh"])
            if result["final_status"] == "load_ok_refresh_ok":
                result["web_data_status"] = "fresh"
                result["pending_refresh"] = False
            elif result["final_status"] == "load_ok_refresh_ok_validate_failed":
                result["web_data_status"] = "unverified_after_refresh"
                result["pending_refresh"] = False
            elif result["final_status"] == "load_ok_refresh_ok_analyze_failed":
                result["web_data_status"] = "fresh_analyze_failed"
                result["pending_refresh"] = False
            else:
                result["web_data_status"] = "stale"
                result["pending_refresh"] = True
            result["status"] = "ok" if result["final_status"] == "load_ok_refresh_ok" else result["final_status"]
            if result["final_status"] != "load_ok_refresh_ok":
                print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
                raise SystemExit(1)
        except Exception as exc:
            result["status"] = "load_ok_refresh_failed"
            result["final_status"] = "load_ok_refresh_failed"
            result["mv_refresh_status"] = "failed_timeout" if "statement timeout" in str(exc).lower() else "failed"
            result["mv_validation_status"] = "unknown"
            result["web_data_status"] = "stale"
            result["pending_refresh"] = True
            result["mv_stale"] = True
            result["cg_v2_mv_refresh"] = {
                "status": result["mv_refresh_status"],
                "error": str(exc),
            }
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            raise SystemExit(1)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
