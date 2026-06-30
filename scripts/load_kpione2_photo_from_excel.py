# scripts/load_kpione2_photo_from_excel.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


LOADER_NAME = "load_kpione2_photo_from_excel"
PHASE = "FAST_REFORM_010C_ROUTE_B_REVIEW_AND_DRY_RUN_VALIDATION"
LOCAL_TZ = ZoneInfo("America/Santiago")

DEFAULT_EXCEL = Path("data/photo-excel-admin_1782440454408.xlsx")
DEFAULT_SHEET = "Fotos"
DEFAULT_CONTRACT = Path("contracts/control_gestion/kpione2_photo_export_contract_v1.json")
PRODUCTIVE_LOADER_PATH = "scripts/load_control_gestion_raw_v17.py"

GRAIN_CONTRACT = {
    "input_grain": "photo_row",
    "normalized_grain": "event_row",
    "compliance_grain": "day_presence",
    "forbidden_assumption": "one_excel_row_equals_one_visit",
}

COLUMN_ALIASES = {
    "event_id": ["id"],
    "sp_item_id": ["sp item id"],
    "holding": ["holding"],
    "subcadena": ["subcadena"],
    "cod_rt": ["codigo local"],
    "cliente_norm": ["marca"],
    "local_nombre": ["local"],
    "direccion": ["direccion"],
    "reponedor": ["reponedor"],
    "fecha": ["fecha"],
    "fecha_subida": ["fecha de subida"],
    "hora": ["hora"],
    "tipo_de_tarea": ["tipo de tarea"],
    "photo_count": ["n fotos", "foto no/total", "foto n/total", "foto n o/total"],
    "comentarios": ["comentarios"],
    "link_foto": ["link foto"],
}

REQUIRED_KEYS = [
    "event_id",
    "sp_item_id",
    "holding",
    "subcadena",
    "cod_rt",
    "cliente_norm",
    "local_nombre",
    "direccion",
    "reponedor",
    "fecha",
    "hora",
    "tipo_de_tarea",
    "photo_count",
    "comentarios",
    "link_foto",
]

EVENT_STABLE_KEYS = [
    "event_id",
    "sp_item_id",
    "holding",
    "subcadena",
    "cod_rt",
    "cliente_norm",
    "local_nombre",
    "direccion",
    "reponedor",
    "fecha",
    "comentarios",
]

PHOTO_LEVEL_KEYS = [
    "photo_count",
    "link_foto",
    "hora",
    "tipo_de_tarea",
    "fecha_subida",
]

BLOCKING_FLAG_KEYS = [
    "grain_contract_match",
    "forbidden_assumption_rejected",
    "photo_rows_match",
    "distinct_event_ids_match",
    "fecha_min_match",
    "fecha_max_match",
    "db_apply_false",
    "sql_apply_false",
    "productive_loader_touched_false",
    "no_null_event_id_rows",
    "no_null_fecha_rows",
    "no_null_n_fotos_calculado_rows",
    "no_event_ids_multi_fecha",
    "no_event_ids_multi_week",
    "no_event_ids_multi_sp_item",
    "no_row_count_n_fotos_mismatch_events",
    "no_real_content_conflict_event_ids",
    "day_presence_is_binary",
]


class LoaderUsageError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def normalize_column_name(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\u00c2\u00ba", "\u00ba").replace("\u00c2\u00b0", "\u00ba")
    text = text.replace("\u00b0", "\u00ba")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("\u00ba", "o")
    text = text.strip().lower()
    return re.sub(r"\s+", " ", text)


def clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        if value.time() == datetime.min.time():
            return value.date().isoformat()
        return value.isoformat()
    text = str(value).strip()
    return re.sub(r"\s+", " ", text)


def normalize_key(value: object) -> str:
    text = unicodedata.normalize("NFKD", clean_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.strip().upper()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def now_local_iso() -> str:
    return datetime.now(LOCAL_TZ).isoformat(timespec="seconds")


def load_contract(contract_path: Path) -> dict[str, Any]:
    if not contract_path.exists():
        raise LoaderUsageError("contract_not_found", f"Contract file not found: {contract_path}")
    return json.loads(contract_path.read_text(encoding="utf-8"))


def expected_from_contract(contract: dict[str, Any]) -> dict[str, Any]:
    evidence = dict(contract.get("009F_evidence") or {})
    return {
        "photo_rows": evidence.get("photo_rows"),
        "distinct_event_ids": evidence.get("distinct_event_ids"),
        "fecha_min": evidence.get("fecha_min"),
        "fecha_max": evidence.get("fecha_max"),
    }


def resolve_columns(df: pd.DataFrame) -> tuple[dict[str, str], list[str]]:
    norm_to_actual: dict[str, str] = {}
    for col in df.columns:
        norm_to_actual.setdefault(normalize_column_name(col), str(col).strip())

    resolved: dict[str, str] = {}
    missing: list[str] = []
    for key, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            actual = norm_to_actual.get(normalize_column_name(alias))
            if actual:
                resolved[key] = actual
                break
        if key in REQUIRED_KEYS and key not in resolved:
            missing.append(key)
    return resolved, missing


def parse_total_from_photo_count(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip()
    slash_total = text.str.extract(r"/\s*(\d+)\s*$")[0]
    direct_total = text.str.extract(r"^\s*(\d+)\s*$")[0]
    return pd.to_numeric(slash_total.fillna(direct_total), errors="coerce")


def week_start_monday(series: pd.Series) -> pd.Series:
    return series - pd.to_timedelta(series.dt.weekday, unit="D")


def sorted_distinct(series: pd.Series) -> list[str]:
    values = sorted({clean_text(value) for value in series if clean_text(value)})
    return values


def first_clean(series: pd.Series) -> str:
    for value in series:
        text = clean_text(value)
        if text:
            return text
    return ""


def stable_hash_frame(df: pd.DataFrame, stable_cols: list[str]) -> pd.Series:
    if not stable_cols:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    normalized = pd.DataFrame(index=df.index)
    for col in stable_cols:
        normalized[col] = df[col].map(lambda value: normalize_key(value))
    return normalized.agg("||".join, axis=1).map(sha256_text)


def photo_row_hash_frame(df: pd.DataFrame) -> pd.Series:
    normalized = pd.DataFrame(index=df.index)
    for col in df.columns:
        normalized[str(col)] = df[col].map(lambda value: clean_text(value))
    return normalized.agg("||".join, axis=1).map(sha256_text)


def _date_iso(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).date().isoformat()


def _date_list(series: pd.Series) -> list[str]:
    return sorted({x for x in (_date_iso(value) for value in series.dropna()) if x})


def analyze_photo_dataframe(
    df: pd.DataFrame,
    *,
    contract: dict[str, Any],
    expected: dict[str, Any],
    source_file: str,
    source_file_sha256: str | None,
    sheet_name: str,
) -> dict[str, Any]:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    resolved, missing = resolve_columns(df)

    base_payload: dict[str, Any] = {
        "phase": PHASE,
        "loader_name": LOADER_NAME,
        "mode": "dry_run",
        "generated_at": now_local_iso(),
        "source_file": source_file,
        "source_file_sha256": source_file_sha256,
        "sheet_name": sheet_name,
        "contract_status": contract.get("status"),
        "db_apply": False,
        "sql_apply": False,
        "writes_executed": False,
        "dsn_printed": False,
        "productive_loader_path": PRODUCTIVE_LOADER_PATH,
        "productive_loader_touched": False,
        "grain_contract": GRAIN_CONTRACT,
        "expected": expected,
        "columns": list(df.columns),
        "resolved_columns": resolved,
        "missing_required_columns": missing,
        "errors": [],
        "warnings": [],
    }

    contract_grain = contract.get("grain_contract") or {}
    if contract_grain != GRAIN_CONTRACT:
        base_payload["errors"].append("grain_contract_mismatch")

    if missing:
        base_payload["errors"].append("missing_required_columns")
        base_payload["flags"] = {
            "grain_contract_match": contract_grain == GRAIN_CONTRACT,
            "db_apply_false": True,
            "sql_apply_false": True,
            "productive_loader_touched_false": True,
        }
        base_payload["verdict"] = "BLOCKED_STRUCTURE"
        return base_payload

    col_event = resolved["event_id"]
    col_sp = resolved["sp_item_id"]
    col_fecha = resolved["fecha"]
    col_photo_count = resolved["photo_count"]

    df["_event_id"] = df[col_event].astype("string").fillna("").str.strip()
    df["_sp_item_id"] = df[col_sp].astype("string").fillna("").str.strip()
    df["_fecha_dt"] = pd.to_datetime(df[col_fecha], errors="coerce").dt.normalize()
    df["_week_start"] = week_start_monday(df["_fecha_dt"])
    df["_n_fotos_calculado"] = parse_total_from_photo_count(df[col_photo_count])

    stable_cols = [resolved[key] for key in EVENT_STABLE_KEYS if key in resolved]
    photo_level_cols = [resolved[key] for key in PHOTO_LEVEL_KEYS if key in resolved]
    df["_event_stable_hash"] = stable_hash_frame(df, stable_cols)
    df["_photo_row_hash"] = photo_row_hash_frame(df[[c for c in df.columns if not c.startswith("_")]])

    photo_rows = int(len(df))
    valid_event_mask = df["_event_id"] != ""
    valid_events = df[valid_event_mask].copy()
    distinct_event_ids = int(valid_events["_event_id"].nunique())
    distinct_event_identity_pairs = int(
        valid_events[["_event_id", "_sp_item_id"]].drop_duplicates().shape[0]
    )
    null_event_id_rows = int((df["_event_id"] == "").sum())
    null_fecha_rows = int(df["_fecha_dt"].isna().sum())
    null_n_fotos_calculado_rows = int(df["_n_fotos_calculado"].isna().sum())
    fecha_min = _date_iso(df["_fecha_dt"].min()) if not df["_fecha_dt"].dropna().empty else None
    fecha_max = _date_iso(df["_fecha_dt"].max()) if not df["_fecha_dt"].dropna().empty else None

    event_date_counts = valid_events.groupby("_event_id")["_fecha_dt"].nunique(dropna=True)
    event_week_counts = valid_events.groupby("_event_id")["_week_start"].nunique(dropna=True)
    event_sp_counts = valid_events.groupby("_event_id")["_sp_item_id"].nunique(dropna=False)
    event_hash_counts = valid_events.groupby("_event_id")["_event_stable_hash"].nunique(dropna=False)

    event_ids_multi_fecha = int((event_date_counts > 1).sum())
    event_ids_multi_week = int((event_week_counts > 1).sum())
    event_ids_multi_sp_item = int((event_sp_counts > 1).sum())
    real_content_conflict_event_ids = int((event_hash_counts > 1).sum())

    per_event = (
        valid_events.groupby("_event_id", dropna=False)
        .agg(
            source_photo_rows=("_event_id", "size"),
            n_fotos_calculado=("_n_fotos_calculado", "max"),
            fecha=("_fecha_dt", "first"),
            week_start=("_week_start", "first"),
            sp_item_id=("_sp_item_id", first_clean),
            cod_rt=(resolved["cod_rt"], first_clean),
            cliente_norm=(resolved["cliente_norm"], first_clean),
            local_nombre=(resolved["local_nombre"], first_clean),
            reponedor=(resolved["reponedor"], first_clean),
            hora_primera_foto=(resolved["hora"], first_clean),
            event_stable_hash=("_event_stable_hash", first_clean),
        )
        .reset_index()
    )
    per_event["tipos_de_tarea"] = (
        valid_events.groupby("_event_id")[resolved["tipo_de_tarea"]]
        .agg(sorted_distinct)
        .reindex(per_event["_event_id"])
        .tolist()
    )
    per_event["row_count_equals_n_fotos"] = (
        per_event["source_photo_rows"] == per_event["n_fotos_calculado"]
    )
    row_count_n_fotos_mismatch_events = int((~per_event["row_count_equals_n_fotos"]).sum())
    per_event["fecha"] = per_event["fecha"].map(_date_iso)
    per_event["week_start"] = per_event["week_start"].map(_date_iso)
    per_event["cod_rt_norm"] = per_event["cod_rt"].map(normalize_key)
    per_event["cliente_norm_key"] = per_event["cliente_norm"].map(normalize_key)

    day_presence = (
        per_event.groupby(["fecha", "cod_rt_norm", "cliente_norm_key"], dropna=False)
        .agg(event_rows=("_event_id", "size"))
        .reset_index()
    )
    day_presence["presence"] = 1
    day_presence_rows = int(len(day_presence))
    max_events_per_day_presence = int(day_presence["event_rows"].max()) if day_presence_rows else 0
    day_presence_is_binary = bool((day_presence["presence"] == 1).all())

    daily_raw = (
        df.dropna(subset=["_fecha_dt"])
        .groupby("_fecha_dt")
        .agg(photo_rows=(col_event, "size"), distinct_event_ids=("_event_id", pd.Series.nunique))
        .reset_index()
    )
    daily_raw["coverage_date"] = daily_raw["_fecha_dt"].map(_date_iso)
    daily_coverage = daily_raw[["coverage_date", "photo_rows", "distinct_event_ids"]].to_dict(
        orient="records"
    )

    metrics = {
        "photo_rows": photo_rows,
        "distinct_event_ids": distinct_event_ids,
        "distinct_event_identity_pairs": distinct_event_identity_pairs,
        "fecha_min": fecha_min,
        "fecha_max": fecha_max,
        "dates_present": _date_list(df["_fecha_dt"]),
        "weeks_present": _date_list(df["_week_start"]),
        "event_rows": int(len(per_event)),
        "day_presence_rows": day_presence_rows,
        "max_events_per_day_presence": max_events_per_day_presence,
        "null_event_id_rows": null_event_id_rows,
        "null_fecha_rows": null_fecha_rows,
        "null_n_fotos_calculado_rows": null_n_fotos_calculado_rows,
        "event_ids_multi_fecha": event_ids_multi_fecha,
        "event_ids_multi_week": event_ids_multi_week,
        "event_ids_multi_sp_item": event_ids_multi_sp_item,
        "real_content_conflict_event_ids": real_content_conflict_event_ids,
        "row_count_n_fotos_mismatch_events": row_count_n_fotos_mismatch_events,
    }

    flags = {
        "grain_contract_match": contract_grain == GRAIN_CONTRACT,
        "forbidden_assumption_rejected": photo_rows != distinct_event_ids,
        "photo_rows_match": photo_rows == expected.get("photo_rows"),
        "distinct_event_ids_match": distinct_event_ids == expected.get("distinct_event_ids"),
        "fecha_min_match": fecha_min == expected.get("fecha_min"),
        "fecha_max_match": fecha_max == expected.get("fecha_max"),
        "db_apply_false": True,
        "sql_apply_false": True,
        "productive_loader_touched_false": True,
        "no_null_event_id_rows": null_event_id_rows == 0,
        "no_null_fecha_rows": null_fecha_rows == 0,
        "no_null_n_fotos_calculado_rows": null_n_fotos_calculado_rows == 0,
        "no_event_ids_multi_fecha": event_ids_multi_fecha == 0,
        "no_event_ids_multi_week": event_ids_multi_week == 0,
        "no_event_ids_multi_sp_item": event_ids_multi_sp_item == 0,
        "no_row_count_n_fotos_mismatch_events": row_count_n_fotos_mismatch_events == 0,
        "no_real_content_conflict_event_ids": real_content_conflict_event_ids == 0,
        "day_presence_is_binary": day_presence_is_binary,
    }

    payload = {
        **base_payload,
        "column_contract": {
            "event_stable_columns_used_for_hash": stable_cols,
            "photo_level_columns_excluded_from_hash": photo_level_cols,
        },
        "normalization": {
            "photo_row_to_event_row": "group_by_event_id",
            "event_identity": ["ID", "SP Item ID"],
            "event_key": "trim(ID)",
            "day_presence_key": ["fecha", "cod_rt_norm", "cliente_norm_key"],
            "day_presence_value": "binary_1_if_any_event",
        },
        "metrics": metrics,
        "daily_coverage": daily_coverage,
        "day_presence_summary": {
            "rows": day_presence_rows,
            "max_events_per_day_presence": max_events_per_day_presence,
            "binary_presence_values": sorted(day_presence["presence"].unique().tolist())
            if day_presence_rows
            else [],
        },
        "flags": flags,
        "sample_event_rows": per_event.head(5).to_dict(orient="records"),
    }
    payload["verdict"] = "PASS_ROUTE_B_DRY_RUN" if all(flags[k] for k in BLOCKING_FLAG_KEYS) else "WARN_REVIEW_REQUIRED"
    return payload


def build_dry_run_payload(
    excel_path: Path,
    *,
    sheet_name: str = DEFAULT_SHEET,
    contract_path: Path = DEFAULT_CONTRACT,
) -> dict[str, Any]:
    contract = load_contract(contract_path)
    if not excel_path.exists():
        raise LoaderUsageError("excel_not_found", f"Excel file not found: {excel_path}")

    with pd.ExcelFile(excel_path, engine="openpyxl") as workbook:
        if sheet_name not in workbook.sheet_names:
            raise LoaderUsageError("sheet_not_found", f"Sheet not found: {sheet_name}")
        df = pd.read_excel(workbook, sheet_name=sheet_name)
    return analyze_photo_dataframe(
        df,
        contract=contract,
        expected=expected_from_contract(contract),
        source_file=str(excel_path),
        source_file_sha256=sha256_file(excel_path),
        sheet_name=sheet_name,
    )


def redact_secret_text(text: str) -> str:
    text = re.sub(r"postgres(?:ql)?://[^ \n\r\t]+", "[REDACTED_DSN]", text, flags=re.IGNORECASE)
    return re.sub(r"password=[^& \n\r\t]+", "password=[REDACTED]", text, flags=re.IGNORECASE)


def safe_error_payload(exc: BaseException) -> dict[str, Any]:
    code = getattr(exc, "code", exc.__class__.__name__)
    return {
        "phase": PHASE,
        "loader_name": LOADER_NAME,
        "mode": "dry_run",
        "verdict": "FAIL",
        "error_code": str(code),
        "error": redact_secret_text(str(exc)),
        "db_apply": False,
        "sql_apply": False,
        "writes_executed": False,
        "dsn_printed": False,
        "productive_loader_path": PRODUCTIVE_LOADER_PATH,
        "productive_loader_touched": False,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dry-run KPIONE2 photo export grain validator.")
    parser.add_argument("--excel", default=str(DEFAULT_EXCEL))
    parser.add_argument("--sheet", default=DEFAULT_SHEET)
    parser.add_argument("--contract", default=str(DEFAULT_CONTRACT))
    parser.add_argument("--json-out")
    parser.add_argument("--dry-run", action="store_true", help="Run validation without DB or SQL apply.")
    parser.add_argument("--apply", action="store_true", help="Blocked guard: DB apply is not supported here.")
    return parser


def validate_cli_args(args: argparse.Namespace) -> None:
    if args.apply:
        raise LoaderUsageError("apply_not_supported_in_route_b_dry_run", "DB apply is RED and not implemented.")


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        validate_cli_args(args)
        payload = build_dry_run_payload(
            Path(args.excel),
            sheet_name=args.sheet,
            contract_path=Path(args.contract),
        )
        exit_code = 0 if payload.get("verdict") == "PASS_ROUTE_B_DRY_RUN" else 1
    except BaseException as exc:
        payload = safe_error_payload(exc)
        exit_code = 2 if isinstance(exc, LoaderUsageError) else 1

    if args.json_out:
        write_json(Path(args.json_out), payload)
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
