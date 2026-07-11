# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

import monthly_input_layout_contract_015 as month_contract


PHASE_ID = "015B_KPIONE_MONTHLY_INPUT_VALIDATOR_NO_DB_APPLY"
RAW_SHEET = "Fotos"
RUTA_SHEET = "RUTA RUTERO"
PHOTO_PATTERN = "photo-excel-admin_*.xlsx"
ROUTE_PATTERN = "RUTA_RUTEROS_*_S*.xlsx"
OUTPUT_RELATIVE_DIR = Path("research/015B_KPIONE_MONTHLY_INPUT_VALIDATOR_NO_APPLY")
REFERENCE_MANIFEST_DIR = Path("research/015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY")
SPANISH_MONTH_DIRS = {
    "01": "01 - ENERO",
    "02": "02 - FEBRERO",
    "03": "03 - MARZO",
    "04": "04 - ABRIL",
    "05": "05 - MAYO",
    "06": "06 - JUNIO",
    "07": "07 - JULIO",
    "08": "08 - AGOSTO",
    "09": "09 - SEPTIEMBRE",
    "10": "10 - OCTUBRE",
    "11": "11 - NOVIEMBRE",
    "12": "12 - DICIEMBRE",
}


class UsageError(ValueError):
    pass


def normalize_month_id(month_id: str) -> str:
    return month_contract.validate_month_id(month_id)


def parse_as_of_date(value: str | None) -> date:
    if value is None or not str(value).strip():
        return date.today()
    raw = str(value).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        raise UsageError("as_of must use YYYY-MM-DD")
    return date.fromisoformat(raw)


def normalize_validation_mode(value: str) -> str:
    mode = str(value or "").strip().lower()
    if mode not in {"open", "close"}:
        raise UsageError("validation-mode must be open or close")
    return mode


def calendar_months_for_date_range(start: date | None, end: date | None) -> list[str]:
    if start is None or end is None or start > end:
        return []
    current = start
    months: list[str] = []
    while current <= end:
        token = f"{current.year:04d}-{current.month:02d}"
        if token not in months:
            months.append(token)
        current += timedelta(days=1)
    return months


def build_validation_window(
    month_id: str,
    *,
    as_of_date: date,
    validation_mode: str,
) -> dict[str, Any]:
    mode = normalize_validation_mode(validation_mode)
    coverage = month_contract.operational_coverage_for_month(month_id)
    start = date.fromisoformat(str(coverage["operational_coverage_start"]))
    end = date.fromisoformat(str(coverage["operational_coverage_end"]))

    if as_of_date < start:
        status = "NOT_STARTED"
    elif as_of_date >= end:
        status = "CLOSED_ELIGIBLE"
    else:
        status = "IN_PROGRESS"

    if mode == "close":
        required_through = end
        pending_start = None
        pending_end = None
    elif status == "NOT_STARTED":
        required_through = None
        pending_start = start
        pending_end = end
    elif status == "IN_PROGRESS":
        required_through = as_of_date
        pending_start = as_of_date + timedelta(days=1)
        pending_end = end
    else:
        required_through = end
        pending_start = None
        pending_end = None

    return {
        "validation_mode": mode,
        "as_of_date": as_of_date.isoformat(),
        "operational_coverage_start": start.isoformat(),
        "operational_coverage_end": end.isoformat(),
        "required_coverage_through": required_through.isoformat() if required_through else None,
        "pending_future_start": pending_start.isoformat() if pending_start else None,
        "pending_future_end": pending_end.isoformat() if pending_end else None,
        "required_calendar_months_now": calendar_months_for_date_range(start, required_through),
        "required_calendar_months_at_close": month_contract.required_calendar_months_for_operational_month(month_id),
        "operational_period_status": status,
    }


def month_start_and_next(month_id: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    normalized = normalize_month_id(month_id)
    year, month = [int(part) for part in normalized.split("-")]
    start = pd.Timestamp(date(year, month, 1))
    next_month = start + pd.offsets.MonthBegin(1)
    return start.normalize(), pd.Timestamp(next_month).normalize()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_header(value: object) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^0-9A-Za-z]+", "_", text.strip().lower())
    return text.strip("_")


def schema_signature_from_columns(columns: list[object]) -> str:
    normalized = [normalize_header(column) for column in columns]
    return hashlib.sha256("|".join(normalized).encode("utf-8")).hexdigest()


def parse_source_file_id(path_or_name: str | Path) -> str:
    name = Path(path_or_name).name
    match = re.fullmatch(r"photo-excel-admin_(\d+)\.xlsx", name, flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"invalid_photo_report_filename:{name}")
    return match.group(1)


def route_week_label_from_name(path_or_name: str | Path) -> str | None:
    match = re.search(r"_S(\d+)\.xlsx$", Path(path_or_name).name, flags=re.IGNORECASE)
    if not match:
        return None
    return f"S{int(match.group(1))}"


def calendar_month_from_photo_relative_path(relative_path: str) -> str | None:
    match = re.search(r"data/kpione_photo_reports/(\d{4}-\d{2})/", relative_path.replace("\\", "/"))
    if not match:
        return None
    return match.group(1)


def resolve_path(base: Path, value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return base / path


def relative_to_base(base: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(base.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def default_reference_manifest_path(base: Path, month_id: str) -> Path:
    token = normalize_month_id(month_id).replace("-", "_")
    return base / REFERENCE_MANIFEST_DIR / f"015_monthly_input_layout_manifest_{token}.json"


def output_json_path(base: Path, month_id: str) -> Path:
    token = normalize_month_id(month_id).replace("-", "_")
    return base / OUTPUT_RELATIVE_DIR / f"015B_kpione_monthly_input_validation_{token}.json"


def output_markdown_path(base: Path, month_id: str) -> Path:
    token = normalize_month_id(month_id).replace("-", "_")
    return base / OUTPUT_RELATIVE_DIR / f"015B_kpione_monthly_input_validation_{token}.md"


def load_reference_manifest(
    base: Path,
    month_id: str,
    override: str | None,
    *,
    required: bool,
) -> tuple[dict[str, Any] | None, dict[str, Any], list[str]]:
    path = resolve_path(base, override) if override else default_reference_manifest_path(base, month_id)
    reference = {
        "path": relative_to_base(base, path),
        "present": path.exists(),
        "sha256": sha256_file(path) if path.exists() else None,
        "required": required,
    }
    blockers: list[str] = []
    if not path.exists():
        if required:
            blockers.append("reference_manifest_required")
        return None, reference, blockers
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        blockers.append(f"reference_manifest_unreadable:{type(exc).__name__}:{exc}")
        return None, reference, blockers
    if manifest.get("month_id") != month_id:
        blockers.append(f"reference_manifest_month_mismatch:{manifest.get('month_id')}!={month_id}")
    return manifest, reference, blockers


def resolve_monthly_photo_dir(base: Path, photo_root: str | Path, month_id: str) -> Path:
    return resolve_path(base, photo_root) / month_id


def resolve_monthly_photo_dirs(
    base: Path,
    photo_root: str | Path,
    calendar_months: list[str],
) -> dict[str, Path]:
    root = resolve_path(base, photo_root)
    return {month_id: root / month_id for month_id in calendar_months}


def resolve_ruta_month_dir(
    base: Path,
    ruta_root: str | Path,
    ruta_dir_override: str | None,
    month_id: str,
) -> tuple[Path, list[str], list[str]]:
    if ruta_dir_override:
        path = resolve_path(base, ruta_dir_override)
        return path, [] if path.exists() else ["ruta_month_directory_missing"], []

    root = resolve_path(base, ruta_root)
    month_number = month_id.split("-")[1]
    expected_name = SPANISH_MONTH_DIRS[month_number]
    expected_path = root / expected_name
    if not root.exists():
        return expected_path, ["ruta_month_directory_missing"], []
    candidates = [
        item
        for item in root.iterdir()
        if item.is_dir() and item.name.upper().startswith(f"{month_number} - ")
    ]
    if len(candidates) > 1:
        details = ",".join(sorted(relative_to_base(base, item) for item in candidates))
        return expected_path, [f"ruta_month_directory_ambiguous:{details}"], []
    if not expected_path.exists():
        return expected_path, ["ruta_month_directory_missing"], []
    return expected_path, [], []


def source_file_id_duplicates_from_paths(paths: list[Path]) -> list[str]:
    ids: list[str] = []
    for path in paths:
        try:
            ids.append(parse_source_file_id(path))
        except ValueError:
            continue
    counts = Counter(ids)
    return sorted(source_file_id for source_file_id, count in counts.items() if count > 1)


def _resolved_columns(columns: list[object]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for column in columns:
        actual = str(column).strip()
        resolved.setdefault(normalize_header(actual), actual)
    return resolved


def _iso_or_none(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).date().isoformat()


def _safe_sheet_names(path: Path) -> list[str]:
    with pd.ExcelFile(path, engine="openpyxl") as workbook:
        return list(workbook.sheet_names)


def _read_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    with pd.ExcelFile(path, engine="openpyxl") as workbook:
        if sheet_name not in workbook.sheet_names:
            raise KeyError(f"sheet_missing:{sheet_name}")
        return pd.read_excel(workbook, sheet_name=sheet_name, dtype=object)


def _carry_status(source_calendar_month: str, assigned_operational_month: str) -> str:
    if assigned_operational_month > source_calendar_month:
        return "valid_carry_forward"
    if assigned_operational_month < source_calendar_month:
        return "valid_carry_backward"
    return "valid_adjacent_operational_month"


def _week_assignment_counts(
    dates: pd.Series,
    *,
    operational_month_id: str,
    source_calendar_month: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    all_assignments: dict[tuple[str, str], dict[str, Any]] = {}
    selected_assignments: dict[tuple[str, str], dict[str, Any]] = {}
    adjacent_rows: dict[tuple[str, str], dict[str, Any]] = {}
    for timestamp in dates.dropna():
        day = pd.Timestamp(timestamp).date()
        assignment = month_contract.week_assignment(day, folder_month_id=source_calendar_month)
        key = (str(assignment["week_start"]), str(assignment["assigned_operational_month"]))
        if key not in all_assignments:
            all_assignments[key] = {
                "week_start": assignment["week_start"],
                "week_end": assignment["week_end"],
                "assigned_operational_month": assignment["assigned_operational_month"],
                "source_calendar_month": source_calendar_month,
                "folder_month_governs_week_ownership": False,
                "folder_month_matches_assigned_operational_month": assignment[
                    "folder_month_matches_assigned_operational_month"
                ],
                "row_count": 0,
            }
        all_assignments[key]["row_count"] += 1
        if assignment["assigned_operational_month"] == operational_month_id:
            if key not in selected_assignments:
                selected_assignments[key] = dict(all_assignments[key])
                selected_assignments[key]["row_count"] = 0
            selected_assignments[key]["row_count"] += 1
        else:
            adjacent_key = (source_calendar_month, str(assignment["assigned_operational_month"]))
            if adjacent_key not in adjacent_rows:
                adjacent_rows[adjacent_key] = {
                    "assigned_operational_month": assignment["assigned_operational_month"],
                    "source_calendar_month": source_calendar_month,
                    "status": _carry_status(source_calendar_month, str(assignment["assigned_operational_month"])),
                    "row_count": 0,
                }
            adjacent_rows[adjacent_key]["row_count"] += 1
    return (
        sorted(all_assignments.values(), key=lambda item: item["week_start"]),
        sorted(selected_assignments.values(), key=lambda item: item["week_start"]),
        sorted(
            adjacent_rows.values(),
            key=lambda item: (item["source_calendar_month"], item["assigned_operational_month"]),
        ),
    )


def profile_photo_file(
    *,
    base: Path,
    path: Path,
    operational_month_id: str,
    source_calendar_month: str,
    as_of_date: date,
    validation_mode: str,
    expected_entry: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[str], list[str], list[dict[str, Any]]]:
    blockers: list[str] = []
    warnings: list[str] = []
    source_file_id = parse_source_file_id(path)
    relative_path = relative_to_base(base, path)
    stat = path.stat()
    profile: dict[str, Any] = {
        "source_file_id": source_file_id,
        "source_file_name": path.name,
        "relative_path": relative_path,
        "source_calendar_month": source_calendar_month,
        "sha256": sha256_file(path),
        "size_bytes": stat.st_size,
        "sheet": RAW_SHEET,
        "sheet_present": False,
        "role": "unclassified" if expected_entry is None else expected_entry.get("role", "unclassified"),
        "row_count": 0,
        "schema_signature": None,
        "columns": [],
        "fecha_min": None,
        "fecha_max": None,
        "invalid_date_rows": 0,
        "rows_outside_calendar_month": 0,
        "rows_selected_for_operational_month": 0,
        "adjacent_operational_month_row_count": 0,
        "adjacent_operational_month_rows": [],
        "pending_future_coverage_row_count": 0,
        "pending_future_coverage": [],
        "calendar_week_assignments": [],
        "operational_week_assignments": [],
        "read_error": None,
    }

    if expected_entry is not None:
        profile["manifest_expected"] = {
            "sha256": expected_entry.get("sha256"),
            "size_bytes": expected_entry.get("size_bytes"),
            "relative_path": expected_entry.get("relative_path"),
            "role": expected_entry.get("role"),
        }
        if str(expected_entry.get("source_file_id")) != source_file_id:
            blockers.append(f"photo_source_file_id_mismatch:{path.name}")
        if str(expected_entry.get("relative_path")) != relative_path:
            blockers.append(f"photo_relative_path_mismatch:{path.name}")
        if str(expected_entry.get("sha256", "")).lower() != profile["sha256"]:
            blockers.append(f"photo_sha256_mismatch:{path.name}")
        if int(expected_entry.get("size_bytes") or -1) != profile["size_bytes"]:
            blockers.append(f"photo_size_bytes_mismatch:{path.name}")

    try:
        sheet_names = _safe_sheet_names(path)
        profile["available_sheets"] = sheet_names
        if RAW_SHEET not in sheet_names:
            blockers.append(f"photo_required_sheet_missing:{path.name}:{RAW_SHEET}")
            profile["read_error"] = f"sheet_missing:{RAW_SHEET}"
            return profile, blockers, warnings, []
        frame = _read_sheet(path, RAW_SHEET)
    except Exception as exc:
        blockers.append(f"photo_read_error:{path.name}:{type(exc).__name__}:{exc}")
        profile["read_error"] = f"{type(exc).__name__}:{exc}"
        return profile, blockers, warnings, []

    profile["sheet_present"] = True
    profile["row_count"] = int(len(frame))
    profile["columns"] = [str(column).strip() for column in frame.columns]
    profile["schema_signature"] = schema_signature_from_columns(profile["columns"])
    resolved = _resolved_columns(profile["columns"])
    fecha_column = resolved.get(normalize_header("Fecha"))
    if fecha_column is None:
        blockers.append(f"photo_fecha_column_missing:{path.name}")
        profile["invalid_date_rows"] = int(len(frame))
        return profile, blockers, warnings, []

    dates = pd.to_datetime(frame[fecha_column], errors="coerce").dt.normalize()
    valid_dates = dates.dropna()
    profile["invalid_date_rows"] = int(dates.isna().sum())
    if not valid_dates.empty:
        profile["fecha_min"] = _iso_or_none(valid_dates.min())
        profile["fecha_max"] = _iso_or_none(valid_dates.max())

    month_start, next_month = month_start_and_next(source_calendar_month)
    outside_calendar = valid_dates[(valid_dates < month_start) | (valid_dates >= next_month)]
    profile["rows_outside_calendar_month"] = int(len(outside_calendar))
    if profile["rows_outside_calendar_month"]:
        blockers.append(
            f"photo_rows_outside_calendar_month:{path.name}:{profile['rows_outside_calendar_month']}"
        )

    if validation_mode == "close":
        dates_for_required_coverage = valid_dates
        pending_future_dates = valid_dates.iloc[0:0]
    else:
        as_of_ts = pd.Timestamp(as_of_date).normalize()
        dates_for_required_coverage = valid_dates[valid_dates <= as_of_ts]
        pending_future_dates = valid_dates[valid_dates > as_of_ts]

    all_assignments, selected_assignments, adjacent_rows = _week_assignment_counts(
        dates_for_required_coverage,
        operational_month_id=operational_month_id,
        source_calendar_month=source_calendar_month,
    )
    profile["calendar_week_assignments"] = all_assignments
    profile["operational_week_assignments"] = selected_assignments
    profile["rows_selected_for_operational_month"] = int(
        sum(int(item["row_count"]) for item in selected_assignments)
    )
    profile["adjacent_operational_month_rows"] = adjacent_rows
    profile["adjacent_operational_month_row_count"] = int(
        sum(int(item["row_count"]) for item in adjacent_rows)
    )
    if not pending_future_dates.empty:
        future_min = _iso_or_none(pending_future_dates.min())
        future_max = _iso_or_none(pending_future_dates.max())
        profile["pending_future_coverage_row_count"] = int(len(pending_future_dates))
        profile["pending_future_coverage"] = [
            {
                "fecha_min": future_min,
                "fecha_max": future_max,
                "row_count": int(len(pending_future_dates)),
                "status": "pending_future_coverage",
            }
        ]
    if profile["invalid_date_rows"]:
        warnings.append(f"photo_invalid_date_rows:{path.name}:{profile['invalid_date_rows']}")

    return profile, blockers, warnings, selected_assignments


def profile_route_file(
    *,
    base: Path,
    path: Path,
    month_id: str,
    expected_week: dict[str, Any] | None,
    expected_entry: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    relative_path = relative_to_base(base, path)
    stat = path.stat()
    week_label = route_week_label_from_name(path)
    profile: dict[str, Any] = {
        "source_file_name": path.name,
        "relative_path": relative_path,
        "sha256": sha256_file(path),
        "size_bytes": stat.st_size,
        "sheet": RUTA_SHEET,
        "sheet_present": False,
        "row_count": 0,
        "schema_signature": None,
        "columns": [],
        "week_label": week_label,
        "week_start": None,
        "week_end": None,
        "assigned_operational_month": None,
        "read_error": None,
    }

    if week_label is None:
        blockers.append(f"route_week_label_missing:{path.name}")
    elif expected_week is None:
        blockers.append(f"extra_route_week_label:{path.name}:{week_label}")
    else:
        profile["week_start"] = expected_week["week_start"]
        profile["week_end"] = expected_week["week_end"]
        profile["assigned_operational_month"] = expected_week["assigned_operational_month"]

    if expected_entry is not None:
        profile["manifest_expected"] = {
            "sha256": expected_entry.get("sha256"),
            "size_bytes": expected_entry.get("size_bytes"),
            "relative_path": expected_entry.get("relative_path"),
            "row_count": expected_entry.get("row_count"),
            "week_start": expected_entry.get("week_start"),
            "assigned_operational_month": expected_entry.get("assigned_operational_month"),
        }
        if str(expected_entry.get("relative_path")) != relative_path:
            blockers.append(f"route_relative_path_mismatch:{path.name}")
        if str(expected_entry.get("sha256", "")).lower() != profile["sha256"]:
            blockers.append(f"route_sha256_mismatch:{path.name}")
        if int(expected_entry.get("size_bytes") or -1) != profile["size_bytes"]:
            blockers.append(f"route_size_bytes_mismatch:{path.name}")
        if expected_entry.get("week_start") and expected_week is not None:
            if str(expected_entry.get("week_start")) != str(expected_week["week_start"]):
                blockers.append(f"route_manifest_week_start_mismatch:{path.name}")
        if expected_entry.get("assigned_operational_month") and expected_week is not None:
            if str(expected_entry.get("assigned_operational_month")) != str(
                expected_week["assigned_operational_month"]
            ):
                blockers.append(f"route_manifest_assigned_month_mismatch:{path.name}")

    try:
        sheet_names = _safe_sheet_names(path)
        profile["available_sheets"] = sheet_names
        if RUTA_SHEET not in sheet_names:
            blockers.append(f"ruta_required_sheet_missing:{path.name}:{RUTA_SHEET}")
            profile["read_error"] = f"sheet_missing:{RUTA_SHEET}"
            return profile, blockers, warnings
        frame = _read_sheet(path, RUTA_SHEET)
    except Exception as exc:
        blockers.append(f"route_read_error:{path.name}:{type(exc).__name__}:{exc}")
        profile["read_error"] = f"{type(exc).__name__}:{exc}"
        return profile, blockers, warnings

    profile["sheet_present"] = True
    profile["row_count"] = int(len(frame))
    profile["columns"] = [str(column).strip() for column in frame.columns]
    profile["schema_signature"] = schema_signature_from_columns(profile["columns"])
    if expected_entry is not None and expected_entry.get("row_count") is not None:
        if int(expected_entry.get("row_count")) != profile["row_count"]:
            blockers.append(f"route_row_count_mismatch:{path.name}")
    return profile, blockers, warnings


def reference_photo_by_id(manifest: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not manifest:
        return {}
    files = manifest.get("photo_report_files", {}).get("files", [])
    return {
        str(item.get("source_file_id")): item
        for item in files
        if isinstance(item, dict) and item.get("source_file_id") is not None
    }


def reference_route_by_name(manifest: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not manifest:
        return {}
    files = manifest.get("ruta_rutero_reference", {}).get("files", [])
    return {
        str(item.get("source_file_name")): item
        for item in files
        if isinstance(item, dict) and item.get("source_file_name")
    }


def aggregate_week_assignments(photo_files: list[dict[str, Any]], month_id: str) -> list[dict[str, Any]]:
    by_week: dict[tuple[str, str], dict[str, Any]] = {}
    for item in photo_files:
        for assignment in item.get("operational_week_assignments", []):
            key = (
                str(assignment["week_start"]),
                str(assignment["assigned_operational_month"]),
            )
            if key not in by_week:
                by_week[key] = {
                    "week_start": assignment["week_start"],
                    "week_end": assignment["week_end"],
                    "assigned_operational_month": assignment["assigned_operational_month"],
                    "folder_month_id": month_id,
                    "folder_month_governs_week_ownership": False,
                    "folder_month_matches_assigned_operational_month": assignment[
                        "folder_month_matches_assigned_operational_month"
                    ],
                    "row_count": 0,
                    "source_file_ids": set(),
                }
            by_week[key]["row_count"] += int(assignment["row_count"])
            by_week[key]["source_file_ids"].add(item["source_file_id"])
    out = []
    for item in by_week.values():
        item["source_file_ids"] = sorted(item["source_file_ids"])
        out.append(item)
    return sorted(out, key=lambda item: item["week_start"])


def aggregate_adjacent_rows(photo_files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in photo_files:
        for adjacent in item.get("adjacent_operational_month_rows", []):
            key = (
                str(adjacent["source_calendar_month"]),
                str(adjacent["assigned_operational_month"]),
                str(adjacent["status"]),
            )
            if key not in grouped:
                grouped[key] = {
                    "assigned_operational_month": adjacent["assigned_operational_month"],
                    "source_calendar_month": adjacent["source_calendar_month"],
                    "status": adjacent["status"],
                    "row_count": 0,
                    "source_file_ids": set(),
                }
            grouped[key]["row_count"] += int(adjacent["row_count"])
            grouped[key]["source_file_ids"].add(str(item["source_file_id"]))
    out = []
    for item in grouped.values():
        item["source_file_ids"] = sorted(item["source_file_ids"])
        out.append(item)
    return sorted(
        out,
        key=lambda item: (
            item["source_calendar_month"],
            item["assigned_operational_month"],
            item["status"],
        ),
    )


def validate_route_mappings(
    *,
    month_id: str,
    route_files: list[dict[str, Any]],
    expected_route_mapping: list[dict[str, Any]],
    allowed_route_mapping: list[dict[str, Any]] | None = None,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    missing: list[str] = []
    by_label = {item.get("week_label"): item for item in route_files if item.get("week_label")}
    expected_by_label = {item["week_label"]: item for item in expected_route_mapping}
    allowed_by_label = {
        item["week_label"]: item
        for item in (allowed_route_mapping if allowed_route_mapping is not None else expected_route_mapping)
    }
    for label, expected in expected_by_label.items():
        found = by_label.get(label)
        if found is None:
            missing.append(label)
            blockers.append(f"missing_expected_route_week:{month_id}:{label}:{expected['week_start']}")
            continue
        if found.get("week_start") != expected["week_start"]:
            blockers.append(
                f"route_week_start_mismatch:{month_id}:{label}:{found.get('week_start')}!={expected['week_start']}"
            )
        if found.get("assigned_operational_month") != month_id:
            blockers.append(f"route_assigned_month_mismatch:{month_id}:{label}")
    for label in sorted(set(by_label) - set(allowed_by_label)):
        blockers.append(f"extra_route_week_label:{month_id}:{label}")
    return blockers, missing


def build_validation_payload(
    *,
    base: Path,
    month_id: str,
    photo_root: str | Path = Path("data/kpione_photo_reports"),
    ruta_root: str | Path = Path("data/RUTA_RUTERO"),
    ruta_dir: str | None = None,
    reference_manifest_path: str | None = None,
    as_of: str | date | None = None,
    validation_mode: str = "open",
) -> dict[str, Any]:
    base = base.resolve()
    month_id = normalize_month_id(month_id)
    mode = normalize_validation_mode(validation_mode)
    as_of_date = as_of if isinstance(as_of, date) else parse_as_of_date(as_of)
    validation_window = build_validation_window(
        month_id,
        as_of_date=as_of_date,
        validation_mode=mode,
    )
    required_calendar_months_now = list(validation_window["required_calendar_months_now"])
    required_calendar_months_at_close = list(validation_window["required_calendar_months_at_close"])
    required_coverage_through = (
        date.fromisoformat(str(validation_window["required_coverage_through"]))
        if validation_window["required_coverage_through"]
        else None
    )
    blockers: list[str] = []
    warnings: list[str] = []

    reference_manifest, contract_reference, reference_blockers = load_reference_manifest(
        base,
        month_id,
        reference_manifest_path,
        required=mode == "close",
    )
    blockers.extend(reference_blockers)
    photo_reference = reference_photo_by_id(reference_manifest)
    route_reference = reference_route_by_name(reference_manifest)

    operational_weeks = month_contract.route_week_mapping_for_month(month_id)
    operational_coverage = month_contract.operational_coverage_for_month(month_id)
    monthly_photo_dirs = resolve_monthly_photo_dirs(base, photo_root, required_calendar_months_now)
    ruta_month_dir, ruta_blockers, ruta_warnings = resolve_ruta_month_dir(
        base, ruta_root, ruta_dir, month_id
    )
    required_route_weeks = [
        week
        for week in operational_weeks
        if required_coverage_through is not None
        and date.fromisoformat(str(week["week_start"])) <= required_coverage_through
    ]
    route_directory_required = mode == "close" or bool(required_route_weeks)
    if route_directory_required:
        blockers.extend(ruta_blockers)
    warnings.extend(ruta_warnings)
    legacy_files = sorted((base / "data").glob(PHOTO_PATTERN)) if (base / "data").exists() else []
    if legacy_files:
        warnings.append("legacy_files_detected_not_used")

    photo_paths_by_calendar_month: dict[str, list[Path]] = {}
    for calendar_month, monthly_photo_dir in monthly_photo_dirs.items():
        if not monthly_photo_dir.exists():
            if mode == "close" or calendar_month in required_calendar_months_now:
                blockers.append(f"monthly_photo_directory_missing:{calendar_month}")
            photo_paths_by_calendar_month[calendar_month] = []
            continue
        paths = sorted(monthly_photo_dir.glob(PHOTO_PATTERN))
        photo_paths_by_calendar_month[calendar_month] = paths
        if not paths:
            blockers.append(f"monthly_photo_files_missing:{calendar_month}")
    photo_paths = [
        path
        for paths in photo_paths_by_calendar_month.values()
        for path in paths
    ]

    duplicate_ids = source_file_id_duplicates_from_paths(photo_paths)
    for source_file_id in duplicate_ids:
        blockers.append(f"duplicate_source_file_id:{source_file_id}")

    photo_files: list[dict[str, Any]] = []
    discovered_ids: set[str] = set()
    unexpected_photo_files: list[str] = []
    for calendar_month, paths in photo_paths_by_calendar_month.items():
        for path in paths:
            try:
                source_file_id = parse_source_file_id(path)
            except ValueError as exc:
                blockers.append(str(exc))
                continue
            discovered_ids.add(source_file_id)
            expected_entry = photo_reference.get(source_file_id)
            if reference_manifest is not None and expected_entry is None:
                warnings.append(f"unexpected_photo_file_not_in_reference_manifest:{path.name}")
                unexpected_photo_files.append(relative_to_base(base, path))
            profile, item_blockers, item_warnings, _ = profile_photo_file(
                base=base,
                path=path,
                operational_month_id=month_id,
                source_calendar_month=calendar_month,
                as_of_date=as_of_date,
                validation_mode=mode,
                expected_entry=expected_entry,
            )
            photo_files.append(profile)
            blockers.extend(item_blockers)
            warnings.extend(item_warnings)

    missing_expected_files = []
    if reference_manifest is not None:
        for source_file_id, expected in sorted(photo_reference.items()):
            expected_month = calendar_month_from_photo_relative_path(str(expected.get("relative_path") or ""))
            expected_now = mode == "close" or expected_month in required_calendar_months_now
            if source_file_id not in discovered_ids and expected_now:
                missing_expected_files.append(expected.get("relative_path") or expected.get("source_file_name"))
                blockers.append(f"missing_expected_photo_file:{expected.get('source_file_name')}")

    route_paths = sorted(ruta_month_dir.glob(ROUTE_PATTERN)) if ruta_month_dir.exists() else []
    route_files: list[dict[str, Any]] = []
    unmapped_route_files: list[str] = []
    discovered_route_names: set[str] = set()
    expected_route_by_label = {item["week_label"]: item for item in operational_weeks}
    for path in route_paths:
        expected_entry = route_reference.get(path.name)
        route_label = route_week_label_from_name(path)
        expected_week = expected_route_by_label.get(route_label)
        discovered_route_names.add(path.name)
        if expected_entry is None:
            unmapped_route_files.append(relative_to_base(base, path))
        profile, item_blockers, item_warnings = profile_route_file(
            base=base,
            path=path,
            month_id=month_id,
            expected_week=expected_week,
            expected_entry=expected_entry,
        )
        route_files.append(profile)
        blockers.extend(item_blockers)
        warnings.extend(item_warnings)

    missing_declared_routes = []
    if reference_manifest is not None:
        required_route_labels = {item["week_label"] for item in required_route_weeks}
        for source_file_name, expected in sorted(route_reference.items()):
            expected_label = str(expected.get("week_label") or "")
            expected_now = mode == "close" or expected_label in required_route_labels
            if source_file_name not in discovered_route_names and expected_now:
                missing_declared_routes.append(expected.get("relative_path") or source_file_name)
                blockers.append(f"missing_declared_route_file:{source_file_name}")

    route_mapping_blockers, missing_expected_route_weeks = validate_route_mappings(
        month_id=month_id,
        route_files=route_files,
        expected_route_mapping=operational_weeks if mode == "close" else required_route_weeks,
        allowed_route_mapping=operational_weeks,
    )
    blockers.extend(route_mapping_blockers)

    role_counts = Counter(str(item.get("role") or "unclassified") for item in photo_files)
    operational_week_assignments = aggregate_week_assignments(photo_files, month_id)
    adjacent_operational_month_rows = aggregate_adjacent_rows(photo_files)
    transition_week = (
        reference_manifest.get("ruta_rutero_reference", {}).get("transition_week", {})
        if reference_manifest
        else {}
    )

    blockers = sorted(set(blockers))
    warnings = sorted(set(warnings))
    if blockers:
        verdict = "BLOCKED"
    elif warnings:
        verdict = "WARN"
    elif mode == "open" and validation_window["operational_period_status"] != "CLOSED_ELIGIBLE":
        verdict = "IN_PROGRESS"
    else:
        verdict = "PASS"

    return {
        "phase_id": PHASE_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "month_id": month_id,
        "validation_mode": mode,
        "as_of_date": validation_window["as_of_date"],
        "operational_coverage_start": validation_window["operational_coverage_start"],
        "operational_coverage_end": validation_window["operational_coverage_end"],
        "required_coverage_through": validation_window["required_coverage_through"],
        "pending_future_start": validation_window["pending_future_start"],
        "pending_future_end": validation_window["pending_future_end"],
        "required_calendar_months_now": required_calendar_months_now,
        "required_calendar_months_at_close": required_calendar_months_at_close,
        "operational_period_status": validation_window["operational_period_status"],
        "guardrails": {
            "mode": "LOCAL_INPUT_VALIDATION_NO_APPLY",
            "db_access": {"used": False},
            "supabase_used": False,
            "sql_apply": False,
            "ddl": False,
            "productive_loader_run": False,
            "app_runtime_modified": False,
            "data_movement": False,
            "excel_files_modified": False,
        },
        "input_contract": {
            "month_semantics": "operational_month",
            "photo_reports": "data/kpione_photo_reports/{calendar_month}/photo-excel-admin_*.xlsx",
            "ruta_rutero": "data/RUTA_RUTERO/<CARPETA_MES>/RUTA_RUTEROS_<MES>_S*.xlsx",
            "photo_reports_are_operational_source": True,
            "ruta_rutero_defines_operational_expected": True,
            "cumplimiento_frecuencia_is_authority": False,
            "legacy_and_monthly_layouts_are_not_merged": True,
            "week_rule_authority": "scripts/monthly_input_layout_contract_015.py",
        },
        "contract_reference": contract_reference,
        "discovery": {
            "monthly_photo_dir": relative_to_base(base, resolve_monthly_photo_dir(base, photo_root, month_id)),
            "monthly_photo_dirs": {
                calendar_month: relative_to_base(base, path)
                for calendar_month, path in monthly_photo_dirs.items()
            },
            "required_calendar_months": required_calendar_months_now,
            "required_calendar_months_now": required_calendar_months_now,
            "required_calendar_months_at_close": required_calendar_months_at_close,
            "ruta_month_dir": relative_to_base(base, ruta_month_dir),
            "legacy_files_detected_not_used": [relative_to_base(base, path) for path in legacy_files],
        },
        "photo_reports": {
            "files": photo_files,
            "source_file_id_duplicates": duplicate_ids,
            "role_counts": dict(sorted(role_counts.items())),
            "unexpected_files": unexpected_photo_files,
            "missing_expected_files": missing_expected_files,
        },
        "ruta_rutero": {
            "files": route_files,
            "unmapped_files": unmapped_route_files,
            "missing_declared_routes": missing_declared_routes,
            "expected_week_count": len(operational_weeks),
            "expected_route_mapping": operational_weeks,
            "missing_expected_route_weeks": missing_expected_route_weeks,
        },
        "operational_coverage": operational_coverage,
        "operational_week_assignments": operational_week_assignments,
        "adjacent_operational_month_rows": adjacent_operational_month_rows,
        "transition_week": transition_week,
        "blockers": blockers,
        "warnings": warnings,
        "verdict": verdict,
    }


def _md_table(headers: list[str], rows: list[list[object]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join("" if value is None else str(value) for value in row) + " |")
    return lines


def report_markdown(payload: dict[str, Any]) -> str:
    lines: list[str] = [
        "# 015B KPIONE monthly input validation - No Apply",
        "",
        "## Executive summary",
        "",
        f"- Operational month: `{payload['month_id']}`",
        f"- Validation mode: `{payload['validation_mode']}`",
        f"- As-of date: `{payload['as_of_date']}`",
        f"- Operational period status: `{payload['operational_period_status']}`",
        f"- Verdict: `{payload['verdict']}`",
        f"- Photo reports: `{len(payload['photo_reports']['files'])}`",
        f"- RUTA_RUTERO files: `{len(payload['ruta_rutero']['files'])}`",
        f"- Blockers: `{len(payload['blockers'])}`",
        f"- Warnings: `{len(payload['warnings'])}`",
        f"- Operational coverage: `{payload['operational_coverage']['operational_coverage_start']}..{payload['operational_coverage']['operational_coverage_end']}`",
        f"- Required coverage through: `{payload['required_coverage_through']}`",
        f"- Pending future coverage: `{payload['pending_future_start']}..{payload['pending_future_end']}`",
        f"- Expected operational weeks: `{payload['ruta_rutero']['expected_week_count']}`",
        "",
        "## Discovered inputs",
        "",
        f"- Monthly photo dir: `{payload['discovery']['monthly_photo_dir']}`",
        f"- Required calendar months now: `{', '.join(payload['discovery']['required_calendar_months_now'])}`",
        f"- Required calendar months at close: `{', '.join(payload['discovery']['required_calendar_months_at_close'])}`",
        f"- Ruta month dir: `{payload['discovery']['ruta_month_dir']}`",
        f"- Reference manifest present: `{payload['contract_reference']['present']}`",
        f"- Reference manifest: `{payload['contract_reference']['path']}`",
        "",
        "## Photo reports",
        "",
    ]
    lines.extend(
        _md_table(
            [
                "source_file_id",
                "file",
                "role",
                "rows",
                "fecha_min",
                "fecha_max",
                "outside_calendar",
                "selected_rows",
                "adjacent_rows",
            ],
            [
                [
                    item.get("source_file_id"),
                    item.get("source_file_name"),
                    item.get("role"),
                    item.get("row_count"),
                    item.get("fecha_min"),
                    item.get("fecha_max"),
                    item.get("rows_outside_calendar_month"),
                    item.get("rows_selected_for_operational_month"),
                    item.get("adjacent_operational_month_row_count"),
                ]
                for item in payload["photo_reports"]["files"]
            ],
        )
    )
    lines.extend(["", "## Adjacent operational rows", ""])
    lines.extend(
        _md_table(
            ["source_calendar_month", "assigned_month", "rows", "status", "source_files"],
            [
                [
                    item.get("source_calendar_month"),
                    item.get("assigned_operational_month"),
                    item.get("row_count"),
                    item.get("status"),
                    ",".join(item.get("source_file_ids", [])),
                ]
                for item in payload["adjacent_operational_month_rows"]
            ],
        )
        if payload["adjacent_operational_month_rows"]
        else ["- None"]
    )
    lines.extend(["", "## RUTA_RUTERO", ""])
    lines.extend(
        _md_table(
            ["file", "week_label", "week_start", "assigned_month", "rows"],
            [
                [
                    item.get("source_file_name"),
                    item.get("week_label"),
                    item.get("week_start"),
                    item.get("assigned_operational_month"),
                    item.get("row_count"),
                ]
                for item in payload["ruta_rutero"]["files"]
            ],
        )
    )
    lines.extend(["", "## Operational weeks", ""])
    lines.extend(
        _md_table(
            ["week_start", "week_end", "assigned_month", "rows", "source_files"],
            [
                [
                    item.get("week_start"),
                    item.get("week_end"),
                    item.get("assigned_operational_month"),
                    item.get("row_count"),
                    ",".join(item.get("source_file_ids", [])),
                ]
                for item in payload["operational_week_assignments"]
            ],
        )
    )
    lines.extend(["", "## Transition week", ""])
    if payload["transition_week"]:
        for key, value in payload["transition_week"].items():
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- Not declared.")
    lines.extend(["", "## Blockers", ""])
    lines.extend([f"- `{item}`" for item in payload["blockers"]] or ["- None"])
    lines.extend(["", "## Warnings", ""])
    lines.extend([f"- `{item}`" for item in payload["warnings"]] or ["- None"])
    lines.extend(
        [
            "",
            "## Guardrails no-apply",
            "",
            f"- DB access used: `{payload['guardrails']['db_access']['used']}`",
            f"- Supabase used: `{payload['guardrails']['supabase_used']}`",
            f"- SQL apply: `{payload['guardrails']['sql_apply']}`",
            f"- DDL: `{payload['guardrails']['ddl']}`",
            f"- Productive loader run: `{payload['guardrails']['productive_loader_run']}`",
            f"- Data movement: `{payload['guardrails']['data_movement']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def write_outputs(
    base: Path,
    payload: dict[str, Any],
    json_out: str | None,
    md_out: str | None,
) -> tuple[Path, Path]:
    json_path = resolve_path(base, json_out) if json_out else output_json_path(base, payload["month_id"])
    md_path = resolve_path(base, md_out) if md_out else output_markdown_path(base, payload["month_id"])
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
        newline="\n",
    )
    md_path.write_text(report_markdown(payload), encoding="utf-8", newline="\n")
    return json_path, md_path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="015B local/no-apply monthly KPIONE input validator."
    )
    parser.add_argument("--month", required=True, help="Operational input month, YYYY-MM.")
    parser.add_argument("--base", default=".")
    parser.add_argument("--photo-root", default="data/kpione_photo_reports")
    parser.add_argument("--ruta-root", default="data/RUTA_RUTERO")
    parser.add_argument("--ruta-dir", default=None)
    parser.add_argument("--reference-manifest", default=None)
    parser.add_argument("--json-out", default=None)
    parser.add_argument("--md-out", default=None)
    parser.add_argument("--as-of", default=None, help="Validation date, YYYY-MM-DD. Defaults to local current date.")
    parser.add_argument(
        "--validation-mode",
        choices=["open", "close"],
        default="open",
        help="open validates only coverage due by --as-of; close validates full operational month.",
    )
    parser.add_argument("--soft-exit", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    try:
        args = parser.parse_args(argv)
        base = Path(args.base)
        payload = build_validation_payload(
            base=base,
            month_id=args.month,
            photo_root=args.photo_root,
            ruta_root=args.ruta_root,
            ruta_dir=args.ruta_dir,
            reference_manifest_path=args.reference_manifest,
            as_of=args.as_of,
            validation_mode=args.validation_mode,
        )
        json_path, md_path = write_outputs(base, payload, args.json_out, args.md_out)
        print(
            json.dumps(
                {
                    "phase_id": PHASE_ID,
                    "month_id": payload["month_id"],
                    "validation_mode": payload["validation_mode"],
                    "as_of_date": payload["as_of_date"],
                    "verdict": payload["verdict"],
                    "blocker_count": len(payload["blockers"]),
                    "warning_count": len(payload["warnings"]),
                    "json_path": str(json_path),
                    "md_path": str(md_path),
                    "db_access": {"used": False},
                    "sql_apply": False,
                    "data_movement": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    except SystemExit:
        raise
    except Exception as exc:
        print(f"015B execution_error:{type(exc).__name__}:{exc}", file=sys.stderr)
        return 2

    if payload["verdict"] == "BLOCKED" and not args.soft_exit:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
