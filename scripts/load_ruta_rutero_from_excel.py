# scripts/load_ruta_rutero_from_excel.py
# -*- coding: utf-8 -*-
import argparse
import hashlib
import json
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
import unicodedata
from zoneinfo import ZoneInfo

import pandas as pd

LOADER_NAME = "load_ruta_rutero_from_excel"
LOCAL_TZ = ZoneInfo("America/Santiago")

ROUTE_POLICY_VERSION = "ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1"
ROLLBACK_CONFIRM_TOKEN = "ROUTE_WEEK_ROLLBACK_V1"

DAY_COLUMNS = ["lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo"]

ROUTE_COLUMN_KEYS = {
    "cadena": "CADENA",
    "formato": "FORMATO",
    "region": "REGION",
    "comuna": "COMUNA",
    "cod_rt": "COD KPI ONE",
    "cod_b2b": "COD B2B",
    "local_nombre": "LOCAL",
    "direccion": "DIRECCION",
    "veces_por_semana": "VECES POR SEMANA",
    "rutero": "RUTERO",
    "jefe_operaciones": "JEFE DE OPERACIONES",
    "gestores": "GESTORES",
    "cliente": "CLIENTE",
    "supervisor": "SUPERVISOR",
    "reponedor": "REPONEDOR",
    "lunes": "LUNES",
    "martes": "MARTES",
    "miercoles": "MIERCOLES",
    "jueves": "JUEVES",
    "viernes": "VIERNES",
    "sabado": "SABADO",
    "domingo": "DOMINGO",
    "visita_mensual": "VISITA MENSUAL",
    "dif": "DIF",
    "obs": "OBS",
    "aux": "AUX",
    "gg": "GG",
    "modalidad": "MODALIDAD",
}

REQUIRED_SOURCE_KEYS = list(ROUTE_COLUMN_KEYS.values())

TEXT_FIELDS = [
    "cadena",
    "formato",
    "region",
    "comuna",
    "cod_rt",
    "cod_b2b",
    "local_nombre",
    "direccion",
    "rutero",
    "jefe_operaciones",
    "gestores",
    "cliente",
    "supervisor",
    "reponedor",
    "obs",
    "aux",
    "modalidad",
]

INT_FIELDS = ["veces_por_semana", "visita_mensual", "dif", "gg"]

BUSINESS_HASH_COLUMNS = [
    "cadena",
    "formato",
    "region",
    "comuna",
    "cod_rt",
    "cod_b2b",
    "local_nombre",
    "direccion",
    "veces_por_semana",
    "rutero",
    "jefe_operaciones",
    "gestores",
    "cliente",
    "supervisor",
    "reponedor",
    "lunes",
    "martes",
    "miercoles",
    "jueves",
    "viernes",
    "sabado",
    "domingo",
    "visita_mensual",
    "dif",
    "obs",
    "aux",
    "gg",
    "modalidad",
]

PUBLIC_RUTA_COLUMNS = BUSINESS_HASH_COLUMNS + ["row_hash", "source", "source_row"]

REQUIRED_DB_OBJECTS = [
    "public.ruta_rutero",
    "cg_core.ruta_rutero_load_batch",
    "cg_core.ruta_rutero_load_rows",
    "cg_core.ruta_rutero_week_assignment",
    "cg_core.v_ruta_rutero_load_batch_week_v2",
    "cg_core.v_rr_frecuencia_base_resuelta_v2",
]

REQUIRED_ASSIGNMENT_COLUMNS = {
    "assignment_id",
    "effective_week_start",
    "route_policy_version",
    "ruta_batch_id",
    "assignment_status",
    "input_file_name",
    "input_file_sha256",
    "schema_signature",
    "current_surface_hash",
    "resolved_surface_hash",
    "assigned_at",
    "assigned_by",
    "replaces_ruta_batch_id",
    "rollback_of_assignment_id",
    "notes",
}

REQUIRED_WEEK_VIEW_COLUMNS = {
    "ruta_batch_id",
    "effective_week_start",
    "effective_week_iso",
    "route_week_source",
}

REQUIRED_RESOLVED_VIEW_COLUMNS = {
    "effective_week_start",
    "effective_week_iso",
    "cod_rt",
    "cod_rt_norm",
    "cliente_norm",
    "visitas_exigidas_semana",
    "lunes",
    "martes",
    "miercoles",
    "jueves",
    "viernes",
    "sabado",
    "domingo",
}

POSTCHECK_CONTRACT = [
    "batch_inserted",
    "history_row_count_matches",
    "current_public_row_count_matches",
    "current_surface_hash_matches",
    "active_assignment_exists",
    "assignment_week_matches",
    "assigned_batch_matches",
    "resolved_rows_positive",
    "resolved_duplicate_logical_grains_zero",
    "week_view_reports_explicit_assignment",
    "resolved_surface_hash_matches",
    "resolved_grains_equal_assigned_batch_grains",
    "no_legacy_backfill_for_assigned_week",
    "no_stale_rows_from_previous_snapshot",
]

APPLY_TRANSACTION_STEPS = [
    "validate_cli",
    "validate_hash",
    "read_and_normalize_excel",
    "classify_duplicates",
    "validate_week",
    "open_connection",
    "verify_db_contract",
    "acquire_week_assignment_lock",
    "select_active_assignment_for_update",
    "block_duplicate_assignment",
    "register_batch_pending",
    "insert_history_rows",
    "validate_history_rows",
    "snapshot_previous",
    "delete_public_source",
    "insert_current_surface",
    "validate_current_surface",
    "supersede_previous_assignment",
    "create_week_assignment",
    "run_postcheck",
    "finalize_batch_ok",
    "commit",
    "emit_json",
]

PUBLIC_DELETE_SQL = "DELETE FROM public.ruta_rutero WHERE source = %s"

PUBLIC_INSERT_SQL = """
INSERT INTO public.ruta_rutero
(cadena,formato,region,comuna,cod_rt,cod_b2b,local_nombre,direccion,
 veces_por_semana,rutero,jefe_operaciones,gestores,cliente,supervisor,reponedor,
 lunes,martes,miercoles,jueves,viernes,sabado,domingo,
 visita_mensual,dif,obs,aux,gg,modalidad,row_hash,source,source_row)
VALUES %s
"""


class LoaderUsageError(Exception):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


class MissingDBContractError(Exception):
    def __init__(self, missing: list[str]):
        self.missing = missing
        super().__init__("missing_db_contract:" + ",".join(missing))


# -----------------------------
# Generic helpers
# -----------------------------
def norm_col(c: str) -> str:
    return str(c).strip()


def clean_str(series: pd.Series) -> pd.Series:
    s = series.where(series.notna(), "")
    s = s.astype(str)
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
    return pd.to_numeric(s, errors="coerce").fillna(0).round(0).astype(int)


def to_int(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0).round(0).astype(int)


def md5_row(values) -> str:
    raw = "|".join("" if v is None else str(v).strip() for v in values)
    return hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()


def ensure_sslmode(db_url: str) -> str:
    if not db_url:
        return db_url
    if "sslmode=" in db_url:
        return db_url
    if "?" in db_url:
        return db_url + "&sslmode=require"
    return db_url + "?sslmode=require"


def normalize_header_key(value: object) -> str:
    text = str(value or "").replace("\ufeff", "").strip()
    text = " ".join(text.split())
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.upper()


def redact_secret(value: object) -> str:
    text = str(value)
    text = re.sub(r"postgres(?:ql)?://[^@\s]+@", "postgresql://<redacted>@", text, flags=re.I)
    text = re.sub(r"(password=)[^&\s]+", r"\1<redacted>", text, flags=re.I)
    return text


def print_source_check(payload: dict) -> None:
    print(json.dumps({"source_check": payload}, ensure_ascii=False, indent=2))


def emit_json(payload: dict, json_out: str | None = None) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if json_out:
        out_path = Path(json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text + "\n", encoding="utf-8")
    print(text)


def finalize_source_check(payload: dict, blockers: list[str], warnings: list[str], notes: list[str]) -> dict:
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


def sha256_file(path: str | Path) -> str:
    hasher = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest().upper()


def schema_signature_from_columns(columns) -> str:
    normalized = [normalize_header_key(c) for c in columns]
    raw = "|".join(normalized)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest().upper()


def normalize_header_map(columns) -> dict[str, str]:
    out: dict[str, str] = {}
    for col in columns:
        key = normalize_header_key(col)
        if key and key not in out:
            out[key] = col
    return out


def parse_effective_week_start(value: str) -> date:
    if not value or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise LoaderUsageError("invalid_effective_week_start_format", "effective_week_start must be ISO YYYY-MM-DD")
    try:
        week_start = date.fromisoformat(value)
    except ValueError as exc:
        raise LoaderUsageError("invalid_effective_week_start", str(exc)) from exc
    if week_start.weekday() != 0:
        raise LoaderUsageError("effective_week_start_must_be_monday", "effective_week_start must be Monday")
    return week_start


def effective_week_end(week_start: date) -> date:
    return week_start + timedelta(days=6)


def get_effective_week_info(loaded_at: datetime) -> tuple[str, int]:
    local_dt = loaded_at.astimezone(LOCAL_TZ)
    week_start = (local_dt.date() - timedelta(days=local_dt.weekday())).isoformat()
    week_iso = int(local_dt.date().isocalendar().week)
    return week_start, week_iso


def source_check_payload_skipped(excel_path: str, sheet: str) -> dict:
    return {
        "loader": "load_ruta_rutero_from_excel.py",
        "file": str(excel_path),
        "sheet_scope": [sheet],
        "final_verdict": "warn",
        "blockers": [],
        "warnings": ["source_check_skipped_by_flag"],
        "rows_checked": {},
        "date_ranges": {},
        "notes": ["source_check disabled by --skip-source-check"],
    }


def run_source_check_ruta(*, excel_path: str, sheet: str, strict: bool) -> dict:
    del strict  # reserved for future parity with other loaders
    payload = {
        "loader": "load_ruta_rutero_from_excel.py",
        "file": str(excel_path),
        "sheet_scope": [sheet],
        "final_verdict": "ok",
        "blockers": [],
        "warnings": [],
        "rows_checked": {},
        "date_ranges": {},
        "notes": [],
    }
    blockers: list[str] = []
    warnings: list[str] = []
    notes: list[str] = []

    excel_file = os.path.abspath(excel_path)
    if not os.path.exists(excel_file):
        blockers.append(f"missing_workbook:{excel_file}")
        return finalize_source_check(payload, blockers, warnings, notes)

    try:
        with pd.ExcelFile(excel_file) as book:
            sheet_names = list(book.sheet_names)
            if sheet not in sheet_names:
                blockers.append(f"missing_sheet:{sheet}")
                notes.append("available_sheets=" + ", ".join(sheet_names))
                return finalize_source_check(payload, blockers, warnings, notes)
            df = pd.read_excel(book, sheet_name=sheet)
    except ValueError as exc:
        blockers.append(f"sheet_read_error:{sheet}:{type(exc).__name__}")
        notes.append(str(exc))
        return finalize_source_check(payload, blockers, warnings, notes)
    except Exception as exc:
        blockers.append(f"unreadable_workbook:{type(exc).__name__}")
        notes.append(str(exc))
        return finalize_source_check(payload, blockers, warnings, notes)

    df.columns = [norm_col(c) for c in df.columns]
    payload["rows_checked"][sheet] = int(len(df))

    normalized_map = normalize_header_map(df.columns)
    missing = [key for key in REQUIRED_SOURCE_KEYS if key not in normalized_map]
    if missing:
        blockers.append("missing_critical_columns:" + ",".join(missing))

    extras = [col for col in df.columns if normalize_header_key(col) not in REQUIRED_SOURCE_KEYS]
    if extras:
        warnings.append("extra_columns:" + ",".join(extras))

    notes.append("LUNES-DOMINGO treated as structural flags, not dates")
    notes.append("VECES POR SEMANA treated as weekly obligation")
    notes.append("source_check runs before DB")
    return finalize_source_check(payload, blockers, warnings, notes)


# -----------------------------
# Route workbook profiling
# -----------------------------
def read_route_excel(excel_path: str | Path, sheet: str) -> pd.DataFrame:
    with pd.ExcelFile(Path(excel_path)) as book:
        df = pd.read_excel(book, sheet_name=sheet)
    df.columns = [norm_col(c) for c in df.columns]
    return df


def _source_column(df: pd.DataFrame, required_key: str) -> str:
    normalized = normalize_header_map(df.columns)
    if required_key not in normalized:
        raise LoaderUsageError("missing_source_column", f"missing source column: {required_key}")
    return normalized[required_key]


def transform_route_dataframe(df: pd.DataFrame, *, source: str) -> pd.DataFrame:
    out = pd.DataFrame()

    for field in TEXT_FIELDS:
        col = _source_column(df, ROUTE_COLUMN_KEYS[field])
        out[field] = clean_str(df[col])

    out["cod_b2b"] = out["cod_b2b"].str.replace(r"\.0$", "", regex=True)

    for field in INT_FIELDS:
        col = _source_column(df, ROUTE_COLUMN_KEYS[field])
        out[field] = to_int(df[col])

    for field in DAY_COLUMNS:
        col = _source_column(df, ROUTE_COLUMN_KEYS[field])
        out[field] = to_int01(df[col])

    normalized = normalize_header_map(df.columns)
    if "IDROW" in normalized:
        out["source_row"] = to_int(df[normalized["IDROW"]])
    else:
        out = out.reset_index(drop=True)
        out["source_row"] = out.index.astype(int) + 2

    out["source"] = source

    out = out[out["cod_rt"].astype(str).str.len() > 0]
    out = out[out["rutero"].astype(str).str.len() > 0]
    out = out.reset_index(drop=True)

    out["row_hash"] = out[BUSINESS_HASH_COLUMNS].apply(lambda r: md5_row(r.tolist()), axis=1)
    return out


def prepare_route_rows(excel_path: str | Path, *, sheet: str, source: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = read_route_excel(excel_path, sheet)
    return df, transform_route_dataframe(df, source=source)


def classify_exact_duplicates(out: pd.DataFrame) -> dict:
    duplicate_excess = out[out.duplicated(subset=["row_hash"], keep="first")]
    duplicate_groups = out[out.duplicated(subset=["row_hash"], keep=False)]
    group_count = int(duplicate_groups["row_hash"].nunique()) if not duplicate_groups.empty else 0
    return {
        "groups": group_count,
        "excess_count": int(len(duplicate_excess)),
        "excess_source_rows": [int(v) for v in duplicate_excess["source_row"].tolist()],
        "excess_row_hashes": [str(v) for v in duplicate_excess["row_hash"].tolist()],
    }


def current_surface_rows(out: pd.DataFrame) -> pd.DataFrame:
    return out.loc[~out.duplicated(subset=["row_hash"], keep="first")].copy().reset_index(drop=True)


def classify_grain_duplicates(out: pd.DataFrame) -> dict:
    if out.empty:
        return {"groups": 0, "excess_rows": 0, "keys": []}
    frame = out.copy()
    frame["_cod_rt_norm"] = frame["cod_rt"].map(normalize_text)
    frame["_cliente_norm"] = frame["cliente"].map(normalize_key)
    counts = (
        frame.groupby(["_cod_rt_norm", "_cliente_norm"], dropna=False)
        .size()
        .reset_index(name="rows")
    )
    dupes = counts[counts["rows"] > 1].copy()
    keys = [
        {"cod_rt_norm": str(row["_cod_rt_norm"]), "cliente_norm": str(row["_cliente_norm"]), "rows": int(row["rows"])}
        for _, row in dupes.sort_values(["_cod_rt_norm", "_cliente_norm"]).iterrows()
    ]
    return {
        "groups": int(len(dupes)),
        "excess_rows": int((dupes["rows"] - 1).sum()) if not dupes.empty else 0,
        "keys": keys,
    }


def frequency_day_profile(out: pd.DataFrame) -> dict:
    if out.empty:
        return {"mismatch_count": 0, "examples": []}
    frame = out.copy()
    frame["_planned_days"] = frame[DAY_COLUMNS].sum(axis=1).astype(int)
    mismatches = frame[frame["veces_por_semana"].astype(int) != frame["_planned_days"]].copy()
    examples = []
    for _, row in mismatches.head(10).iterrows():
        examples.append(
            {
                "source_row": int(row["source_row"]),
                "cod_rt": str(row["cod_rt"]),
                "cliente": str(row["cliente"]),
                "veces_por_semana": int(row["veces_por_semana"]),
                "planned_days": int(row["_planned_days"]),
            }
        )
    return {"mismatch_count": int(len(mismatches)), "examples": examples}


def build_history_rows(out: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for rec in out.to_dict(orient="records"):
        clean = {}
        for key, value in rec.items():
            if pd.isna(value):
                clean[key] = None
            elif key in {"source_row", *DAY_COLUMNS, *INT_FIELDS}:
                clean[key] = int(value)
            else:
                clean[key] = value
        rows.append(clean)
    return rows


def build_public_rows(out: pd.DataFrame) -> list[tuple]:
    return list(out[PUBLIC_RUTA_COLUMNS].itertuples(index=False, name=None))


def canonical_current_surface_hash(pairs) -> str:
    normalized = sorted((int(source_row), str(row_hash)) for source_row, row_hash in pairs)
    raw = "\n".join(f"{source_row}|{row_hash}" for source_row, row_hash in normalized)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest().upper()


def current_surface_hash(out: pd.DataFrame) -> str:
    if out.empty:
        return canonical_current_surface_hash([])
    return canonical_current_surface_hash(
        (rec.source_row, rec.row_hash)
        for rec in out[["source_row", "row_hash"]].itertuples(index=False)
    )


def normalized_operational_tuple(row: dict | pd.Series) -> tuple:
    return (
        normalize_key(row.get("modalidad", "")),
        normalize_key(row.get("reponedor", row.get("reponedor_scope", ""))),
        normalize_key(row.get("gestores", row.get("gestor", ""))),
        normalize_key(row.get("supervisor", "")),
        normalize_key(row.get("rutero", "")),
    )


def operational_completeness_score(row: dict | pd.Series) -> int:
    keys = ["modalidad", "reponedor", "gestores", "supervisor", "rutero"]
    aliases = {"reponedor": "reponedor_scope", "gestores": "gestor"}
    score = 0
    for key in keys:
        value = row.get(key, row.get(aliases.get(key, ""), ""))
        if normalize_text(value):
            score += 1
    return score


def logical_winner_sort_key(row: dict | pd.Series) -> tuple:
    day_sum = sum(int(row.get(day, 0) or 0) for day in DAY_COLUMNS)
    return (
        -int(row.get("veces_por_semana", row.get("visitas_exigidas_semana", 0)) or 0),
        -day_sum,
        -operational_completeness_score(row),
        normalized_operational_tuple(row),
        str(row.get("row_hash", "")),
    )


def select_logical_winner(rows: list[dict]) -> dict:
    if not rows:
        raise ValueError("rows required")
    return sorted(rows, key=logical_winner_sort_key)[0]


def route_person_conflict(rows: list[dict]) -> int:
    values = {
        (
            normalize_key(row.get("reponedor", row.get("reponedor_scope", ""))),
            normalize_key(row.get("gestores", row.get("gestor", ""))),
            normalize_key(row.get("supervisor", "")),
            normalize_key(row.get("rutero", "")),
        )
        for row in rows
    }
    return 1 if len(values) > 1 else 0


def canonical_resolved_surface_hash(rows: list[dict]) -> str:
    pieces = []
    for row in rows:
        day_values = [str(int(row.get(day, 0) or 0)) for day in DAY_COLUMNS]
        pieces.append(
            "|".join(
                [
                    str(row.get("cod_rt_norm", row.get("cod_rt", ""))),
                    str(row.get("cliente_norm", "")),
                    str(int(row.get("visitas_exigidas_semana", row.get("veces_por_semana", 0)) or 0)),
                    *day_values,
                    str(row.get("modalidad", "")),
                    str(row.get("reponedor_scope", row.get("reponedor", ""))),
                    str(row.get("gestor", row.get("gestores", ""))),
                    str(row.get("supervisor", "")),
                    str(row.get("rutero", "")),
                ]
            )
        )
    return hashlib.sha256("\n".join(sorted(pieces)).encode("utf-8")).hexdigest().upper()


def resolved_surface_rows_from_current(current: pd.DataFrame) -> list[dict]:
    if current.empty:
        return []
    frame = current.copy()
    frame["_cod_rt_norm"] = frame["cod_rt"].map(normalize_text)
    frame["_cliente_norm"] = frame["cliente"].map(normalize_key)
    rows: list[dict] = []
    for (_, _), group in frame.groupby(["_cod_rt_norm", "_cliente_norm"], sort=True, dropna=False):
        records = group.to_dict(orient="records")
        winner = dict(select_logical_winner(records))
        winner["cod_rt_norm"] = winner["_cod_rt_norm"]
        winner["cliente_norm"] = winner["_cliente_norm"]
        winner["visitas_exigidas_semana"] = int(winner["veces_por_semana"])
        winner["ruta_duplicada_flag"] = 1 if len(records) > 1 else 0
        winner["ruta_duplicada_rows"] = len(records)
        winner["ruta_person_conflict_flag"] = route_person_conflict(records)
        rows.append(winner)
    return rows


def build_dry_run_plan(
    *,
    excel_path: str | Path,
    sheet: str,
    source: str,
    effective_week_start_value: str,
    expected_workbook_sha256: str | None = None,
    source_check: dict | None = None,
) -> dict:
    week_start = parse_effective_week_start(effective_week_start_value)
    file_hash = sha256_file(excel_path)
    if expected_workbook_sha256 and file_hash.upper() != expected_workbook_sha256.upper():
        raise LoaderUsageError("workbook_hash_mismatch", "expected workbook hash does not match actual hash")

    df, accepted = prepare_route_rows(excel_path, sheet=sheet, source=source)
    current = current_surface_rows(accepted)
    exact = classify_exact_duplicates(accepted)
    grain = classify_grain_duplicates(current)
    frequency = frequency_day_profile(accepted)
    schema_signature = schema_signature_from_columns(df.columns)

    warnings: list[str] = []
    if source_check:
        warnings.extend([f"source_check:{w}" for w in source_check.get("warnings", [])])
    if exact["excess_count"]:
        warnings.append("exact_duplicates_excluded_current")
    if grain["groups"]:
        warnings.append("grain_duplicates_preserved_multirow")
    if frequency["mismatch_count"]:
        warnings.append("frequency_day_mismatch_warning")

    current_hash = current_surface_hash(current)
    resolved_hash = canonical_resolved_surface_hash(resolved_surface_rows_from_current(current))
    warnings.append("workbook_has_no_intrinsic_effective_week")
    return {
        "mode": "dry_run",
        "loader": LOADER_NAME,
        "source": source,
        "sheet": sheet,
        "input_file": str(Path(excel_path)),
        "input_file_name": Path(excel_path).name,
        "effective_week_start": week_start.isoformat(),
        "effective_week_end": effective_week_end(week_start).isoformat(),
        "week_source": "OPERATOR_EXPLICIT",
        "route_policy_version": ROUTE_POLICY_VERSION,
        "input_file_sha256": file_hash,
        "schema_signature": schema_signature,
        "input_rows": int(len(df)),
        "accepted_rows": int(len(accepted)),
        "history_insert_rows": int(len(build_history_rows(accepted))),
        "current_surface_insert_rows": int(len(current)),
        "exact_duplicate_excess": int(exact["excess_count"]),
        "exact_duplicate_groups": int(exact["groups"]),
        "grain_duplicate_groups": int(grain["groups"]),
        "grain_duplicate_excess_rows": int(grain["excess_rows"]),
        "frequency_day_mismatches": int(frequency["mismatch_count"]),
        "planned_public_delete_scope": {
            "source": source,
            "current_rows_expected": None,
        },
        "planned_public_insert_rows": int(len(current)),
        "planned_assignment": {
            "effective_week_start": week_start.isoformat(),
            "route_policy_version": ROUTE_POLICY_VERSION,
            "assignment_status": "ACTIVE",
            "input_file_name": Path(excel_path).name,
            "input_file_sha256": file_hash,
            "schema_signature": schema_signature,
            "current_surface_hash": current_hash,
            "resolved_surface_hash": resolved_hash,
        },
        "required_db_objects": REQUIRED_DB_OBJECTS,
        "postcheck_contract": POSTCHECK_CONTRACT,
        "rollback_contract": {
            "available": "interface_only",
            "confirm_token": ROLLBACK_CONFIRM_TOKEN,
            "requires_previous_assignment": True,
            "requires_expected_current_surface_hash": True,
            "db_contract_required": True,
        },
        "duplicate_profile": {
            "exact": exact,
            "grain": {
                "groups": grain["groups"],
                "excess_rows": grain["excess_rows"],
                "sample_keys": grain["keys"][:20],
            },
        },
        "frequency_day_profile": frequency,
        "source_check": source_check,
        "warnings": warnings,
        "writes_executed": False,
        "dsn_printed": False,
    }


# -----------------------------
# Guarded DB apply implementation
# -----------------------------
def connect_db(db_url: str):
    import psycopg2

    return psycopg2.connect(ensure_sslmode(db_url))


def _execute_values(cur, sql: str, rows: list[tuple], page_size: int = 5000) -> None:
    from psycopg2.extras import execute_values

    execute_values(cur, sql, rows, page_size=page_size)


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
        values (%s, %s, %s, 0, 'pending', %s, %s)
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
    from psycopg2.extras import Json

    records = []
    for rec in out.to_dict(orient="records"):
        payload = {key: rec[key] for key in PUBLIC_RUTA_COLUMNS}
        for key in DAY_COLUMNS + INT_FIELDS + ["source_row"]:
            payload[key] = int(payload[key])
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
    _execute_values(
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
        """,
        rows,
        page_size=5000,
    )


def delete_public_ruta_for_source(cur, source: str) -> None:
    cur.execute(PUBLIC_DELETE_SQL, (source,))


def insert_public_current_rows(cur, rows: list[tuple]) -> None:
    if rows:
        _execute_values(cur, PUBLIC_INSERT_SQL, rows, page_size=5000)


def restore_public_from_history(cur, *, source: str, ruta_batch_id: int) -> None:
    cur.execute(
        """
        insert into public.ruta_rutero
        (cadena,formato,region,comuna,cod_rt,cod_b2b,local_nombre,direccion,
         veces_por_semana,rutero,jefe_operaciones,gestores,cliente,supervisor,reponedor,
         lunes,martes,miercoles,jueves,viernes,sabado,domingo,
         visita_mensual,dif,obs,aux,gg,modalidad,row_hash,source,source_row)
        select
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
            %s as source,
            source_row
        from (
            select distinct on (row_hash)
                *
              from cg_core.ruta_rutero_load_rows
             where ruta_batch_id = %s
             order by row_hash, source_row
        ) r
        order by source_row, row_hash
        """,
        (source, ruta_batch_id),
    )


def _table_columns(cur, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        select column_name
          from information_schema.columns
         where table_schema = %s
           and table_name = %s
        """,
        (schema, table),
    )
    return {str(row[0]) for row in cur.fetchall()}


def verify_db_contract(cur) -> dict:
    missing: list[str] = []
    for relation in REQUIRED_DB_OBJECTS:
        cur.execute("select to_regclass(%s)", (relation,))
        if cur.fetchone()[0] is None:
            missing.append(f"missing_relation:{relation}")

    if "missing_relation:cg_core.ruta_rutero_week_assignment" not in missing:
        assignment_cols = _table_columns(cur, "cg_core", "ruta_rutero_week_assignment")
        missing.extend([f"missing_assignment_column:{col}" for col in sorted(REQUIRED_ASSIGNMENT_COLUMNS - assignment_cols)])

    if "missing_relation:cg_core.v_ruta_rutero_load_batch_week_v2" not in missing:
        week_cols = _table_columns(cur, "cg_core", "v_ruta_rutero_load_batch_week_v2")
        missing.extend([f"missing_week_view_column:{col}" for col in sorted(REQUIRED_WEEK_VIEW_COLUMNS - week_cols)])

    if "missing_relation:cg_core.v_rr_frecuencia_base_resuelta_v2" not in missing:
        resolved_cols = _table_columns(cur, "cg_core", "v_rr_frecuencia_base_resuelta_v2")
        missing.extend([f"missing_resolved_view_column:{col}" for col in sorted(REQUIRED_RESOLVED_VIEW_COLUMNS - resolved_cols)])

    if "missing_relation:cg_core.ruta_rutero_load_batch" not in missing:
        cur.execute(
            """
            select coalesce(string_agg(pg_get_constraintdef(oid), ' '), '')
              from pg_constraint
             where conrelid = 'cg_core.ruta_rutero_load_batch'::regclass
            """
        )
        constraints = str(cur.fetchone()[0] or "").lower()
        if "pending" not in constraints or "failed" not in constraints:
            missing.append("batch_status_contract_missing_pending_or_failed")

    cur.execute("show transaction_read_only")
    readonly_state = str(cur.fetchone()[0]).lower()
    if readonly_state not in {"off", "false", "0"}:
        missing.append("load_connection_is_read_only")

    return {"ok": not missing, "missing": missing}


def fetch_current_snapshot_summary(cur, *, source: str) -> dict:
    cur.execute(
        """
        select source_row, row_hash
          from public.ruta_rutero
         where source = %s
         order by source_row asc, row_hash asc
        """,
        (source,),
    )
    rows = cur.fetchall()
    return {
        "rows": len(rows),
        "current_surface_hash": canonical_current_surface_hash(rows),
    }


def validate_history_row_count(cur, *, ruta_batch_id: int, expected_rows: int) -> None:
    cur.execute(
        "select count(*)::bigint from cg_core.ruta_rutero_load_rows where ruta_batch_id = %s",
        (ruta_batch_id,),
    )
    found = int(cur.fetchone()[0])
    if found != expected_rows:
        raise RuntimeError(f"history_row_count_mismatch:{found}!={expected_rows}")


def validate_current_surface_count(cur, *, source: str, expected_rows: int) -> None:
    cur.execute("select count(*)::bigint from public.ruta_rutero where source = %s", (source,))
    found = int(cur.fetchone()[0])
    if found != expected_rows:
        raise RuntimeError(f"current_surface_count_mismatch:{found}!={expected_rows}")


def acquire_week_assignment_lock(cur, *, effective_week_start_value: str, route_policy_version: str) -> None:
    cur.execute(
        """
        select pg_advisory_xact_lock(
            hashtextextended(%s, 0),
            hashtextextended(%s, 0)
        )
        """,
        (effective_week_start_value, route_policy_version),
    )


def fetch_active_assignment_for_update(cur, *, effective_week_start_value: str) -> dict | None:
    cur.execute(
        """
        select
            assignment_id,
            ruta_batch_id,
            input_file_sha256,
            schema_signature,
            current_surface_hash,
            resolved_surface_hash
          from cg_core.ruta_rutero_week_assignment
         where effective_week_start = %s
           and route_policy_version = %s
           and assignment_status = 'ACTIVE'
         for update
        """,
        (effective_week_start_value, ROUTE_POLICY_VERSION),
    )
    row = cur.fetchone()
    if not row or len(row) < 6:
        return None
    return {
        "assignment_id": int(row[0]),
        "ruta_batch_id": int(row[1]),
        "input_file_sha256": str(row[2]),
        "schema_signature": str(row[3]),
        "current_surface_hash": str(row[4]),
        "resolved_surface_hash": str(row[5]),
    }


def block_if_idempotent_assignment(active_assignment: dict | None, plan: dict) -> None:
    if not active_assignment:
        return
    planned = plan["planned_assignment"]
    if (
        active_assignment["input_file_sha256"].upper() == plan["input_file_sha256"].upper()
        and active_assignment["schema_signature"].upper() == plan["schema_signature"].upper()
        and active_assignment["current_surface_hash"].upper() == planned["current_surface_hash"].upper()
        and active_assignment["resolved_surface_hash"].upper() == planned["resolved_surface_hash"].upper()
    ):
        raise LoaderUsageError("weekly_assignment_already_current")


def supersede_active_assignment(cur, active_assignment: dict | None) -> int | None:
    if not active_assignment:
        return None
    cur.execute(
        """
        update cg_core.ruta_rutero_week_assignment
           set assignment_status = 'SUPERSEDED',
               notes = concat(coalesce(notes, ''), ' | superseded by guarded weekly replacement')
         where assignment_id = %s
           and assignment_status = 'ACTIVE'
        """,
        (active_assignment["assignment_id"],),
    )
    return int(active_assignment["ruta_batch_id"])


def create_week_assignment(
    cur,
    *,
    effective_week_start_value: str,
    ruta_batch_id: int,
    plan: dict,
    assigned_by: str,
    replaces_ruta_batch_id: int | None,
) -> int:
    cur.execute(
        """
        insert into cg_core.ruta_rutero_week_assignment (
            effective_week_start,
            route_policy_version,
            ruta_batch_id,
            assignment_status,
            input_file_name,
            input_file_sha256,
            schema_signature,
            current_surface_hash,
            resolved_surface_hash,
            assigned_by,
            replaces_ruta_batch_id,
            notes
        )
        values (%s, %s, %s, 'ACTIVE', %s, %s, %s, %s, %s, %s, %s)
        returning assignment_id
        """,
        (
            effective_week_start_value,
            ROUTE_POLICY_VERSION,
            ruta_batch_id,
            plan["input_file_name"],
            plan["input_file_sha256"],
            plan["schema_signature"],
            plan["planned_assignment"]["current_surface_hash"],
            plan["planned_assignment"]["resolved_surface_hash"],
            assigned_by,
            replaces_ruta_batch_id,
            "weekly replacement assignment created by guarded loader",
        ),
    )
    return int(cur.fetchone()[0])


def fetch_current_surface_hash(cur, *, source: str) -> str:
    return fetch_current_snapshot_summary(cur, source=source)["current_surface_hash"]


def fetch_resolved_surface_rows(cur, *, effective_week_start_value: str) -> list[dict]:
    cur.execute(
        """
        select
            cod_rt_norm,
            cliente_norm,
            visitas_exigidas_semana,
            lunes,
            martes,
            miercoles,
            jueves,
            viernes,
            sabado,
            domingo,
            modalidad,
            reponedor_scope,
            gestor,
            supervisor,
            rutero
          from cg_core.v_rr_frecuencia_base_resuelta_v2
         where effective_week_start = %s
         order by cod_rt_norm, cliente_norm
        """,
        (effective_week_start_value,),
    )
    rows = []
    for row in cur.fetchall():
        rows.append(
            {
                "cod_rt_norm": str(row[0]),
                "cliente_norm": str(row[1]),
                "visitas_exigidas_semana": int(row[2]),
                "lunes": int(row[3]),
                "martes": int(row[4]),
                "miercoles": int(row[5]),
                "jueves": int(row[6]),
                "viernes": int(row[7]),
                "sabado": int(row[8]),
                "domingo": int(row[9]),
                "modalidad": str(row[10] or ""),
                "reponedor_scope": str(row[11] or ""),
                "gestor": str(row[12] or ""),
                "supervisor": str(row[13] or ""),
                "rutero": str(row[14] or ""),
            }
        )
    return rows


def fetch_assigned_batch_grains(cur, *, ruta_batch_id: int) -> set[tuple[str, str]]:
    cur.execute(
        """
        with exact_deduped as (
            select distinct on (row_hash)
                   nullif(trim(coalesce(cod_rt_norm, cod_rt)), '') as cod_rt_norm,
                   upper(trim(coalesce(nullif(trim(cliente_norm), ''), nullif(trim(cliente), ''), ''))) as cliente_norm,
                   row_hash,
                   source_row
              from cg_core.ruta_rutero_load_rows
             where ruta_batch_id = %s
               and nullif(trim(coalesce(cod_rt_norm, cod_rt)), '') is not null
               and nullif(trim(coalesce(cliente_norm, cliente)), '') is not null
             order by row_hash, source_row
        )
        select cod_rt_norm, cliente_norm
          from exact_deduped
         group by cod_rt_norm, cliente_norm
         order by cod_rt_norm, cliente_norm
        """,
        (ruta_batch_id,),
    )
    return {(str(row[0]), str(row[1])) for row in cur.fetchall()}


def fetch_resolved_grains(cur, *, effective_week_start_value: str) -> set[tuple[str, str]]:
    cur.execute(
        """
        select cod_rt_norm, cliente_norm
          from cg_core.v_rr_frecuencia_base_resuelta_v2
         where effective_week_start = %s
         order by cod_rt_norm, cliente_norm
        """,
        (effective_week_start_value,),
    )
    return {(str(row[0]), str(row[1])) for row in cur.fetchall()}


def run_postcheck(
    cur,
    *,
    source: str,
    effective_week_start_value: str,
    ruta_batch_id: int,
    assignment_id: int,
    expected_current_rows: int,
    expected_history_rows: int,
    expected_exact_duplicate_excess: int,
    expected_current_surface_hash: str,
    expected_resolved_surface_hash: str,
) -> dict:
    checks: dict[str, object] = {}

    cur.execute(
        """
        select count(*)::bigint
          from cg_core.ruta_rutero_load_batch
         where ruta_batch_id = %s
           and status = 'pending'
        """,
        (ruta_batch_id,),
    )
    if int(cur.fetchone()[0]) != 1:
        raise RuntimeError("postcheck_batch_pending_missing")
    checks["batch_inserted"] = True

    cur.execute(
        "select count(*)::bigint from cg_core.ruta_rutero_load_rows where ruta_batch_id = %s",
        (ruta_batch_id,),
    )
    history_rows = int(cur.fetchone()[0])
    if history_rows != expected_history_rows:
        raise RuntimeError(f"postcheck_history_rows_mismatch:{history_rows}!={expected_history_rows}")
    checks["history_row_count_matches"] = True

    cur.execute(
        """
        select coalesce(sum(rows - 1), 0)::bigint
          from (
            select row_hash, count(*)::bigint as rows
              from cg_core.ruta_rutero_load_rows
             where ruta_batch_id = %s
             group by row_hash
            having count(*) > 1
          ) d
        """,
        (ruta_batch_id,),
    )
    exact_duplicate_excess = int(cur.fetchone()[0])
    if exact_duplicate_excess != expected_exact_duplicate_excess:
        raise RuntimeError(
            f"postcheck_exact_duplicate_excess_mismatch:{exact_duplicate_excess}!={expected_exact_duplicate_excess}"
        )
    checks["history_exact_duplicates_preserved"] = True

    cur.execute(
        "select count(*)::bigint from public.ruta_rutero where source = %s",
        (source,),
    )
    current_rows = int(cur.fetchone()[0])
    if current_rows != expected_current_rows:
        raise RuntimeError(f"postcheck_current_rows_mismatch:{current_rows}!={expected_current_rows}")
    checks["current_public_row_count_matches"] = True

    found_current_hash = fetch_current_surface_hash(cur, source=source)
    if found_current_hash.upper() != expected_current_surface_hash.upper():
        raise RuntimeError("postcheck_current_surface_hash_mismatch")
    checks["current_surface_hash_matches"] = True

    cur.execute(
        """
        select count(*)::bigint
          from cg_core.ruta_rutero_week_assignment
         where assignment_id = %s
           and effective_week_start = %s
           and ruta_batch_id = %s
           and assignment_status = 'ACTIVE'
           and current_surface_hash = %s
           and resolved_surface_hash = %s
        """,
        (
            assignment_id,
            effective_week_start_value,
            ruta_batch_id,
            expected_current_surface_hash,
            expected_resolved_surface_hash,
        ),
    )
    if int(cur.fetchone()[0]) != 1:
        raise RuntimeError("postcheck_active_assignment_missing")
    checks["active_assignment_exists"] = True
    checks["assignment_week_matches"] = True
    checks["assigned_batch_matches"] = True

    cur.execute(
        """
        select count(*)::bigint
          from cg_core.v_rr_frecuencia_base_resuelta_v2
         where effective_week_start = %s
        """,
        (effective_week_start_value,),
    )
    resolved_rows = int(cur.fetchone()[0])
    if resolved_rows <= 0:
        raise RuntimeError("postcheck_resolved_rows_zero")
    checks["resolved_rows"] = resolved_rows
    checks["resolved_rows_positive"] = True

    cur.execute(
        """
        select count(*)::bigint
          from (
            select cod_rt_norm, cliente_norm, count(*)::bigint as rows
              from cg_core.v_rr_frecuencia_base_resuelta_v2
             where effective_week_start = %s
             group by cod_rt_norm, cliente_norm
            having count(*) > 1
          ) d
        """,
        (effective_week_start_value,),
    )
    duplicate_grains = int(cur.fetchone()[0])
    if duplicate_grains != 0:
        raise RuntimeError(f"postcheck_resolved_duplicate_grains:{duplicate_grains}")
    checks["resolved_duplicate_logical_grains_zero"] = True

    cur.execute(
        """
        select count(*)::bigint
          from cg_core.v_ruta_rutero_load_batch_week_v2
         where effective_week_start = %s
           and ruta_batch_id = %s
           and route_week_source = 'EXPLICIT_ASSIGNMENT'
        """,
        (effective_week_start_value, ruta_batch_id),
    )
    if int(cur.fetchone()[0]) < 1:
        raise RuntimeError("postcheck_week_view_not_explicit_assignment")
    checks["week_view_reports_explicit_assignment"] = True

    resolved_rows_payload = fetch_resolved_surface_rows(cur, effective_week_start_value=effective_week_start_value)
    found_resolved_hash = canonical_resolved_surface_hash(resolved_rows_payload)
    if found_resolved_hash.upper() != expected_resolved_surface_hash.upper():
        raise RuntimeError("postcheck_resolved_surface_hash_mismatch")
    checks["resolved_surface_hash_matches"] = True

    assigned_grains = fetch_assigned_batch_grains(cur, ruta_batch_id=ruta_batch_id)
    resolved_grains = fetch_resolved_grains(cur, effective_week_start_value=effective_week_start_value)
    missing = sorted(assigned_grains - resolved_grains)
    extra = sorted(resolved_grains - assigned_grains)
    if missing or extra:
        raise RuntimeError(f"postcheck_resolved_grain_set_mismatch:missing={len(missing)}:extra={len(extra)}")
    checks["resolved_grains_equal_assigned_batch_grains"] = {"missing": 0, "extra": 0}
    checks["no_legacy_backfill_for_assigned_week"] = True
    checks["no_stale_rows_from_previous_snapshot"] = True
    return checks


def run_weekly_replacement_apply(args, plan: dict) -> dict:
    loaded_at = datetime.now(tz=LOCAL_TZ)
    df, accepted = prepare_route_rows(args.excel, sheet=args.sheet, source=args.source)
    del df
    current = current_surface_rows(accepted)
    history_records = build_history_rows(accepted)
    current_hash = plan["planned_assignment"]["current_surface_hash"]
    resolved_hash = plan["planned_assignment"]["resolved_surface_hash"]
    exact_duplicate_excess = int(plan["exact_duplicate_excess"])
    conn = connect_db(args.db_url)
    conn.autocommit = False
    cur = conn.cursor()
    ruta_batch_id = None
    assignment_id = None
    previous_snapshot: dict | None = None
    try:
        contract = verify_db_contract(cur)
        if not contract["ok"]:
            raise MissingDBContractError(contract["missing"])

        acquire_week_assignment_lock(
            cur,
            effective_week_start_value=args.effective_week_start,
            route_policy_version=ROUTE_POLICY_VERSION,
        )
        active_assignment = fetch_active_assignment_for_update(
            cur,
            effective_week_start_value=args.effective_week_start,
        )
        block_if_idempotent_assignment(active_assignment, plan)

        ruta_batch_id = register_cg_ruta_batch(
            cur,
            source_file=Path(args.excel).name,
            source_sheet=args.sheet,
            loaded_at=loaded_at,
            notes=f"source={args.source} | policy={ROUTE_POLICY_VERSION}",
        )
        insert_cg_history_rows(
            cur,
            build_cg_history_rows(
                accepted,
                ruta_batch_id=ruta_batch_id,
                source_file=Path(args.excel).name,
                source_sheet=args.sheet,
                loaded_at=loaded_at,
            ),
        )
        validate_history_row_count(cur, ruta_batch_id=ruta_batch_id, expected_rows=len(history_records))
        previous_snapshot = fetch_current_snapshot_summary(cur, source=args.source)
        delete_public_ruta_for_source(cur, args.source)
        insert_public_current_rows(cur, build_public_rows(current))
        validate_current_surface_count(cur, source=args.source, expected_rows=len(current))
        replaces_ruta_batch_id = supersede_active_assignment(cur, active_assignment)
        assignment_id = create_week_assignment(
            cur,
            effective_week_start_value=args.effective_week_start,
            ruta_batch_id=ruta_batch_id,
            plan=plan,
            assigned_by=LOADER_NAME,
            replaces_ruta_batch_id=replaces_ruta_batch_id,
        )
        postcheck = run_postcheck(
            cur,
            source=args.source,
            effective_week_start_value=args.effective_week_start,
            ruta_batch_id=ruta_batch_id,
            assignment_id=assignment_id,
            expected_current_rows=len(current),
            expected_history_rows=len(history_records),
            expected_exact_duplicate_excess=exact_duplicate_excess,
            expected_current_surface_hash=current_hash,
            expected_resolved_surface_hash=resolved_hash,
        )
        finalize_cg_ruta_batch(
            cur,
            ruta_batch_id=ruta_batch_id,
            loaded_rows=len(history_records),
            status="ok",
            notes=(
                f"source={args.source} | policy={ROUTE_POLICY_VERSION} | "
                f"history_rows={len(history_records)} | current_rows={len(current)}"
            ),
        )
        conn.commit()
        result = dict(plan)
        result.update(
            {
                "mode": "apply",
                "writes_executed": True,
                "ruta_batch_id": ruta_batch_id,
                "assignment_id": assignment_id,
                "replaces_ruta_batch_id": replaces_ruta_batch_id,
                "previous_snapshot": previous_snapshot,
                "postcheck": postcheck,
                "transaction_steps": APPLY_TRANSACTION_STEPS,
            }
        )
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def run_weekly_replacement_rollback(
    *,
    db_url: str,
    source: str,
    effective_week_start_value: str,
    failed_assignment_id: int,
    expected_current_surface_hash: str,
    confirm_token: str,
) -> dict:
    parse_effective_week_start(effective_week_start_value)
    if not db_url:
        raise LoaderUsageError("rollback_requires_explicit_db_url")
    if not failed_assignment_id:
        raise LoaderUsageError("rollback_requires_failed_assignment_id")
    if not expected_current_surface_hash:
        raise LoaderUsageError("rollback_requires_expected_current_surface_hash")
    if confirm_token != ROLLBACK_CONFIRM_TOKEN:
        raise LoaderUsageError("rollback_requires_confirm_token")

    conn = connect_db(db_url)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        contract = verify_db_contract(cur)
        if not contract["ok"]:
            raise MissingDBContractError(contract["missing"])
        acquire_week_assignment_lock(
            cur,
            effective_week_start_value=effective_week_start_value,
            route_policy_version=ROUTE_POLICY_VERSION,
        )
        found_hash = fetch_current_surface_hash(cur, source=source)
        if found_hash.upper() != expected_current_surface_hash.upper():
            raise LoaderUsageError("rollback_current_surface_hash_mismatch")

        cur.execute(
            """
            select
                assignment_id,
                ruta_batch_id,
                replaces_ruta_batch_id,
                assignment_status
              from cg_core.ruta_rutero_week_assignment
             where assignment_id = %s
               and effective_week_start = %s
               and route_policy_version = %s
             for update
            """,
            (failed_assignment_id, effective_week_start_value, ROUTE_POLICY_VERSION),
        )
        current_assignment = cur.fetchone()
        if not current_assignment:
            raise LoaderUsageError("rollback_assignment_not_found")
        if str(current_assignment[3]) != "ACTIVE":
            raise LoaderUsageError("rollback_assignment_not_active")
        previous_ruta_batch_id = current_assignment[2]
        if previous_ruta_batch_id is None:
            raise LoaderUsageError("rollback_previous_assignment_missing")

        cur.execute(
            """
            select assignment_id
              from cg_core.ruta_rutero_week_assignment
             where effective_week_start = %s
               and route_policy_version = %s
               and ruta_batch_id = %s
               and assignment_status = 'SUPERSEDED'
             order by assigned_at desc, assignment_id desc
             limit 1
             for update
            """,
            (effective_week_start_value, ROUTE_POLICY_VERSION, previous_ruta_batch_id),
        )
        previous_assignment = cur.fetchone()
        if not previous_assignment:
            raise LoaderUsageError("rollback_previous_assignment_missing")
        previous_assignment_id = int(previous_assignment[0])

        delete_public_ruta_for_source(cur, source)
        restore_public_from_history(cur, source=source, ruta_batch_id=int(previous_ruta_batch_id))
        restored_summary = fetch_current_snapshot_summary(cur, source=source)

        cur.execute(
            """
            update cg_core.ruta_rutero_week_assignment
               set assignment_status = 'ROLLED_BACK',
                   rollback_of_assignment_id = %s,
                   notes = concat(coalesce(notes, ''), ' | rolled back by guarded weekly rollback')
             where assignment_id = %s
            """,
            (previous_assignment_id, failed_assignment_id),
        )
        cur.execute(
            """
            update cg_core.ruta_rutero_week_assignment
               set assignment_status = 'ACTIVE',
                   notes = concat(coalesce(notes, ''), ' | reactivated by guarded weekly rollback')
             where assignment_id = %s
            """,
            (previous_assignment_id,),
        )
        conn.commit()
        return {
            "mode": "rollback",
            "effective_week_start": effective_week_start_value,
            "failed_assignment_id": failed_assignment_id,
            "reactivated_assignment_id": previous_assignment_id,
            "restored_ruta_batch_id": int(previous_ruta_batch_id),
            "restored_current_rows": restored_summary["rows"],
            "restored_current_surface_hash": restored_summary["current_surface_hash"],
            "writes_executed": True,
            "dsn_printed": False,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        finally:
            conn.close()


# -----------------------------
# CLI
# -----------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default=r"data\DB_GLOBAL_INVENTARIO.xlsx")
    ap.add_argument("--sheet", default="RUTA_RUTERO")
    ap.add_argument("--db_url", default="")
    ap.add_argument("--source", default="DB_GLOBAL_INVENTARIO.xlsx:RUTA_RUTERO")
    ap.add_argument("--effective-week-start")
    ap.add_argument("--source-check-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--rollback-weekly-replacement", action="store_true")
    ap.add_argument("--confirm-weekly-replacement", default="")
    ap.add_argument("--confirm-rollback", default="")
    ap.add_argument("--failed-assignment-id", type=int)
    ap.add_argument("--expected-current-surface-hash", default="")
    ap.add_argument("--expected-workbook-sha256", default="")
    ap.add_argument("--json-out", default="")
    ap.add_argument("--skip-postcheck", action="store_true")
    ap.add_argument("--skip-source-check", action="store_true")
    ap.add_argument("--source-check-strict", action="store_true")

    # Legacy flags are accepted so old invocations fail safely into dry-run
    # instead of reaching the retired write path.
    ap.add_argument("--no-cg-ruta-history", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--no-refresh-cliente-mvs", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--refresh-cg-v2-mv", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--validate-cg-v2-mv", action="store_true", help=argparse.SUPPRESS)
    return ap


def validate_cli_args(args) -> str:
    if args.source_check_only and args.apply:
        raise LoaderUsageError("source_check_only_incompatible_with_apply")
    if args.dry_run and args.apply:
        raise LoaderUsageError("dry_run_incompatible_with_apply")
    if args.rollback_weekly_replacement and args.apply:
        raise LoaderUsageError("rollback_incompatible_with_apply")
    if args.rollback_weekly_replacement and args.dry_run:
        raise LoaderUsageError("rollback_incompatible_with_dry_run")
    if args.rollback_weekly_replacement and args.source_check_only:
        raise LoaderUsageError("rollback_incompatible_with_source_check_only")

    if args.rollback_weekly_replacement:
        if not args.effective_week_start:
            raise LoaderUsageError("rollback_requires_effective_week_start")
        parse_effective_week_start(args.effective_week_start)
        if not args.db_url:
            raise LoaderUsageError("rollback_requires_explicit_db_url")
        if not args.failed_assignment_id:
            raise LoaderUsageError("rollback_requires_failed_assignment_id")
        if not args.expected_current_surface_hash:
            raise LoaderUsageError("rollback_requires_expected_current_surface_hash")
        if args.confirm_rollback != ROLLBACK_CONFIRM_TOKEN:
            raise LoaderUsageError("rollback_requires_confirm_token")
        if not args.json_out:
            raise LoaderUsageError("rollback_requires_json_out")
        return "rollback"

    if args.source_check_only:
        return "source_check_only"

    if not args.apply:
        args.dry_run = True

    if not args.effective_week_start:
        raise LoaderUsageError("effective_week_start_required")
    parse_effective_week_start(args.effective_week_start)

    if args.apply:
        if args.skip_source_check:
            raise LoaderUsageError("apply_rejects_skip_source_check")
        if not args.expected_workbook_sha256:
            raise LoaderUsageError("apply_requires_expected_workbook_sha256")
        if args.confirm_weekly_replacement != ROUTE_POLICY_VERSION:
            raise LoaderUsageError("apply_requires_exact_confirm_token")
        if not args.json_out:
            raise LoaderUsageError("apply_requires_json_out")
        if not args.db_url:
            raise LoaderUsageError("apply_requires_explicit_db_url")
        if args.skip_postcheck:
            raise LoaderUsageError("apply_requires_postcheck_enabled")
        return "apply"

    return "dry_run"


def safe_error_payload(exc: Exception, *, mode: str | None = None) -> dict:
    code = getattr(exc, "code", type(exc).__name__)
    payload = {
        "mode": mode or "error",
        "status": "error",
        "error_code": str(code),
        "error": redact_secret(str(exc)),
        "writes_executed": False,
        "dsn_printed": False,
    }
    if isinstance(exc, MissingDBContractError):
        payload["error_code"] = "missing_db_contract"
        payload["missing_db_contract"] = exc.missing
    return payload


def main(argv=None):
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        mode = validate_cli_args(args)
    except LoaderUsageError as exc:
        parser.exit(2, f"{exc.code}: {redact_secret(str(exc))}\n")

    if mode == "rollback":
        try:
            result = run_weekly_replacement_rollback(
                db_url=args.db_url,
                source=args.source,
                effective_week_start_value=args.effective_week_start,
                failed_assignment_id=args.failed_assignment_id,
                expected_current_surface_hash=args.expected_current_surface_hash,
                confirm_token=args.confirm_rollback,
            )
            emit_json(result, args.json_out or None)
            return
        except Exception as exc:
            payload = safe_error_payload(exc, mode=mode)
            if args.json_out:
                emit_json(payload, args.json_out)
            else:
                emit_json(payload)
            raise SystemExit(1)

    if args.skip_source_check:
        source_check = source_check_payload_skipped(args.excel, args.sheet)
    else:
        source_check = run_source_check_ruta(
            excel_path=args.excel,
            sheet=args.sheet,
            strict=bool(args.source_check_strict),
        )

    if mode == "source_check_only":
        print_source_check(source_check)
        if source_check["final_verdict"] == "block":
            raise SystemExit(1)
        return

    if source_check["final_verdict"] == "block":
        print_source_check(source_check)
        raise SystemExit(1)

    try:
        plan = build_dry_run_plan(
            excel_path=args.excel,
            sheet=args.sheet,
            source=args.source,
            effective_week_start_value=args.effective_week_start,
            expected_workbook_sha256=args.expected_workbook_sha256 or None,
            source_check=source_check,
        )
        if mode == "dry_run":
            emit_json(plan, args.json_out or None)
            return

        result = run_weekly_replacement_apply(args, plan)
        emit_json(result, args.json_out or None)
    except Exception as exc:
        payload = safe_error_payload(exc, mode=mode)
        if args.json_out:
            emit_json(payload, args.json_out)
        else:
            emit_json(payload)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
