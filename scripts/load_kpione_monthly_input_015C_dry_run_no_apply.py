# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

import load_kpione_raw_exports_014E_dry_run_no_apply as dry_run_014e
import monthly_input_layout_contract_015 as month_contract
import validate_kpione_raw_exports_014C_no_apply as baseline_014c


PHASE_ID = "015C_KPIONE_MONTHLY_LOAD_DRY_RUN_NO_DB_APPLY"
EXPECTED_INPUT_PHASE = "015B_KPIONE_MONTHLY_INPUT_VALIDATOR_NO_DB_APPLY"
LAYER = "raw_candidate_photo_rows"
OUTPUT_RELATIVE_DIR = Path("research/015C_KPIONE_MONTHLY_LOAD_DRY_RUN_NO_APPLY")
REFERENCE_MANIFEST_DIR = Path("research/015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY")
INPUT_VALIDATION_DIR = Path("research/015B_KPIONE_MONTHLY_INPUT_VALIDATOR_NO_APPLY")
GRAIN_CONTRACT_PATH = Path("contracts/control_gestion/kpione2_photo_export_contract_v1.json")
OPERATIONAL_CALENDAR_CONTRACT_PATH = Path(
    "contracts/control_gestion/operational_calendar_contract_v1.json"
)
BASELINE_014E_MANIFEST_PATH = Path(
    "research/014E_KPIONE_RAW_DRY_RUN_LOADER_NO_APPLY/014E_dry_run_loader_manifest.json"
)

ALLOWED_ROLES = {"include_candidate", "compare_only", "quarantine_truncation"}
DEDUPE_KEY = ["event_id", "photo_row_hash"]
KEEP_SORT = ["source_file_id", "source_row_number"]
PAYLOAD_COLUMNS = [
    "event_id",
    "source_file_id",
    "source_file_name",
    "source_file_sha256",
    "source_row_number",
    "fecha",
    "week_start",
    "assigned_operational_month",
    "cod_rt",
    "local_nombre",
    "cliente_norm",
    "reponedor",
    "tipo_tarea",
    "n_fotos",
    "link_foto",
    "event_stable_hash",
    "photo_row_hash",
    "dry_run_batch_id",
]


def duplicate_metric_definitions() -> dict[str, dict[str, str]]:
    return {
        "exact_duplicate_rows_detected": {
            "unit": "rows",
            "universe": "normalized_candidate_rows",
            "definition": "All rows participating in exact duplicate groups identified by event_id plus photo_row_hash, including both retained and removed rows.",
            "calculation": "duplicated(subset=[event_id, photo_row_hash], keep=False)",
        },
        "exact_duplicate_rows_removed": {
            "unit": "rows",
            "universe": "normalized_candidate_rows",
            "definition": "Rows removed after sorting by source_file_id and source_row_number and keeping the first row for each event_id plus photo_row_hash key.",
            "calculation": "normalized_candidate_rows minus survivor_rows_after_dedupe",
        },
        "same_id_same_hash_count": {
            "unit": "distinct_event_ids",
            "universe": "event_ids_present_in_multiple_source_files",
            "definition": "Distinct event IDs present in more than one source file whose per-file photo-row-hash fingerprint is identical.",
        },
        "same_id_diff_hash_count": {
            "unit": "distinct_event_ids",
            "universe": "event_ids_present_in_multiple_source_files",
            "definition": "Distinct event IDs present in more than one source file whose per-file photo-row-hash fingerprint differs.",
        },
        "event_stable_hash_conflict_count": {
            "unit": "distinct_event_ids",
            "universe": "normalized_candidate_rows",
            "definition": "Distinct event IDs associated with more than one event_stable_hash.",
        },
        "cross_file_exact_photo_row_count": {
            "unit": "distinct_duplicate_keys",
            "universe": "normalized_candidate_rows",
            "definition": "Distinct event_id plus photo_row_hash keys present in more than one source file.",
        },
    }


def has_required_metric_definitions(payload: dict[str, Any]) -> bool:
    definitions = payload.get("metric_definitions")
    expected = duplicate_metric_definitions()
    if not isinstance(definitions, dict):
        return False
    for metric, spec in expected.items():
        found = definitions.get(metric)
        if not isinstance(found, dict):
            return False
        for key in ("unit", "universe", "definition"):
            if found.get(key) != spec.get(key):
                return False
    return True


class UsageError(ValueError):
    pass


def normalize_month_id(month_id: str) -> str:
    try:
        return month_contract.validate_month_id(month_id)
    except ValueError as exc:
        raise UsageError(str(exc)) from exc


def normalize_validation_mode(value: str) -> str:
    mode = str(value or "").strip().lower()
    if mode not in {"open", "close"}:
        raise UsageError("validation-mode must be open or close")
    return mode


def parse_as_of_date(value: str | None) -> str:
    if value is None or not str(value).strip():
        return date.today().isoformat()
    raw = str(value).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        raise UsageError("as-of must use YYYY-MM-DD")
    return date.fromisoformat(raw).isoformat()


def token_for_month(month_id: str) -> str:
    return normalize_month_id(month_id).replace("-", "_")


def default_input_validation_path(base: Path, month_id: str) -> Path:
    return (
        base
        / INPUT_VALIDATION_DIR
        / f"015B_kpione_monthly_input_validation_{token_for_month(month_id)}.json"
    )


def default_reference_manifest_path(base: Path, month_id: str) -> Path:
    return (
        base
        / REFERENCE_MANIFEST_DIR
        / f"015_monthly_input_layout_manifest_{token_for_month(month_id)}.json"
    )


def default_json_output_path(base: Path, month_id: str) -> Path:
    return (
        base
        / OUTPUT_RELATIVE_DIR
        / f"015C_kpione_monthly_load_dry_run_{token_for_month(month_id)}.json"
    )


def default_md_output_path(base: Path, month_id: str) -> Path:
    return (
        base
        / OUTPUT_RELATIVE_DIR
        / f"015C_kpione_monthly_load_dry_run_{token_for_month(month_id)}.md"
    )


def resolve_path(base: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else base / path


def relative_to_base(base: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(base.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def sha256_file(path: Path) -> str:
    return baseline_014c.sha256_file(path)


def read_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_canonical(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _sorted_dict_list(values: Any, keys: list[str]) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []
    return sorted(
        [item for item in values if isinstance(item, dict)],
        key=lambda item: tuple(str(item.get(key) or "") for key in keys),
    )


def semantic_input_validation_sha256(input_validation: dict[str, Any]) -> str:
    photo_files = _sorted_dict_list(
        input_validation.get("photo_reports", {}).get("files", []),
        ["source_file_id", "source_file_name", "relative_path"],
    )
    compact_files = [
        {
            "source_file_id": item.get("source_file_id"),
            "source_file_name": item.get("source_file_name"),
            "relative_path": item.get("relative_path"),
            "sha256": item.get("sha256"),
            "size_bytes": item.get("size_bytes"),
            "row_count": item.get("row_count"),
            "role": item.get("role"),
        }
        for item in photo_files
    ]
    semantic = {
        "phase_id": input_validation.get("phase_id"),
        "month_id": input_validation.get("month_id"),
        "validation_mode": input_validation.get("validation_mode"),
        "as_of_date": input_validation.get("as_of_date"),
        "verdict": input_validation.get("verdict"),
        "blockers": sorted(str(item) for item in input_validation.get("blockers", [])),
        "warnings": sorted(str(item) for item in input_validation.get("warnings", [])),
        "input_contract": input_validation.get("input_contract", {}),
        "operational_coverage": input_validation.get("operational_coverage", {}),
        "photo_report_files": compact_files,
    }
    return sha256_canonical(semantic)


def semantic_reference_manifest_sha256(reference_manifest: dict[str, Any] | None) -> str | None:
    if reference_manifest is None:
        return None
    photo_files = _sorted_dict_list(
        reference_manifest.get("photo_report_files", {}).get("files", []),
        ["source_file_id", "source_file_name", "relative_path"],
    )
    route_files = _sorted_dict_list(
        reference_manifest.get("ruta_rutero_reference", {}).get("files", []),
        ["week_start", "source_file_name", "relative_path"],
    )
    semantic = {
        "phase_id": reference_manifest.get("phase_id"),
        "month_id": reference_manifest.get("month_id"),
        "photo_report_files": photo_files,
        "ruta_rutero_reference": {
            "files": route_files,
            "transition_week": reference_manifest.get("ruta_rutero_reference", {}).get("transition_week"),
        },
    }
    return sha256_canonical(semantic)


def _json_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def _date_iso(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _entry_sha(entry: dict[str, Any]) -> str:
    return str(entry.get("sha256") or entry.get("source_file_sha256") or "")


def _entry_size(entry: dict[str, Any]) -> int | None:
    value = entry.get("size_bytes")
    return int(value) if value is not None else None


def _entry_row_count(entry: dict[str, Any]) -> int | None:
    value = entry.get("row_count")
    return int(value) if value is not None else None


def compact_file_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_file_id": str(entry.get("source_file_id") or ""),
        "source_file_name": str(entry.get("source_file_name") or ""),
        "relative_path": str(entry.get("relative_path") or ""),
        "sha256": _entry_sha(entry),
        "size_bytes": _entry_size(entry),
        "row_count": _entry_row_count(entry),
        "role": str(entry.get("role") or ""),
    }


def _safe_relative_data_path(base: Path, relative_path: str) -> tuple[Path | None, str | None]:
    if not relative_path or Path(relative_path).is_absolute():
        return None, "relative_path_missing_or_absolute"
    normalized = relative_path.replace("\\", "/")
    if normalized.startswith("../") or "/../" in normalized:
        return None, "relative_path_parent_traversal"
    candidate = (base / normalized).resolve()
    data_root = (base / "data").resolve()
    try:
        candidate.relative_to(data_root)
    except ValueError:
        return None, "relative_path_outside_data"
    return candidate, None


def load_authority(
    base: Path,
    relative_path: Path,
    *,
    required_keys: list[str] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any], list[str]]:
    path = base / relative_path
    summary = {
        "path": relative_path.as_posix(),
        "present": path.exists(),
        "sha256": sha256_file(path) if path.exists() else None,
    }
    blockers: list[str] = []
    if not path.exists():
        blockers.append(f"authority_missing:{relative_path.as_posix()}")
        return None, summary, blockers
    try:
        payload = read_json_file(path)
    except Exception as exc:
        blockers.append(f"authority_unreadable:{relative_path.as_posix()}:{type(exc).__name__}")
        return None, summary, blockers
    for key in required_keys or []:
        if key not in payload:
            blockers.append(f"authority_key_missing:{relative_path.as_posix()}:{key}")
    return payload, summary, blockers


def resolve_reference_manifest(
    base: Path,
    *,
    month_id: str,
    input_validation: dict[str, Any],
    override: str | None,
    validation_mode: str,
) -> tuple[dict[str, Any] | None, dict[str, Any], list[str]]:
    if override:
        path = resolve_path(base, override)
    else:
        candidate = input_validation.get("contract_reference", {}).get("path")
        path = resolve_path(base, candidate) if candidate else default_reference_manifest_path(base, month_id)

    summary = {
        "path": relative_to_base(base, path),
        "present": path.exists(),
        "required": validation_mode == "close",
        "sha256": sha256_file(path) if path.exists() else None,
        "expected_sha256_from_015b": input_validation.get("contract_reference", {}).get("sha256"),
    }
    blockers: list[str] = []
    if not path.exists():
        if validation_mode == "close":
            blockers.append("reference_manifest_missing_in_close")
        return None, summary, blockers

    try:
        manifest = read_json_file(path)
    except Exception as exc:
        blockers.append(f"reference_manifest_unreadable:{type(exc).__name__}")
        return None, summary, blockers

    expected_sha = summary["expected_sha256_from_015b"]
    if expected_sha and summary["sha256"] != expected_sha:
        blockers.append("reference_manifest_sha256_mismatch")
    if manifest.get("month_id") != month_id:
        blockers.append(f"reference_manifest_month_mismatch:{manifest.get('month_id')}!={month_id}")
    return manifest, summary, blockers


def file_map_from_reference(reference_manifest: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    files = []
    if reference_manifest:
        files = reference_manifest.get("photo_report_files", {}).get("files", [])
    return {
        str(item.get("source_file_id")): item
        for item in files
        if isinstance(item, dict) and item.get("source_file_id") is not None
    }


def validate_input_preconditions(
    *,
    month_id: str,
    validation_mode: str,
    as_of_date: str,
    input_validation: dict[str, Any],
    reference_manifest: dict[str, Any] | None,
    grain_contract: dict[str, Any] | None,
) -> list[str]:
    blockers: list[str] = []
    if input_validation.get("phase_id") != EXPECTED_INPUT_PHASE:
        blockers.append(f"input_validation_phase_mismatch:{input_validation.get('phase_id')}")
    if input_validation.get("verdict") == "BLOCKED":
        blockers.append("input_validation_blocked")
    if input_validation.get("month_id") != month_id:
        blockers.append(f"input_validation_month_mismatch:{input_validation.get('month_id')}!={month_id}")
    if input_validation.get("validation_mode") != validation_mode:
        blockers.append(
            f"input_validation_mode_mismatch:{input_validation.get('validation_mode')}!={validation_mode}"
        )
    if input_validation.get("as_of_date") != as_of_date:
        blockers.append(f"input_validation_as_of_mismatch:{input_validation.get('as_of_date')}!={as_of_date}")
    if input_validation.get("blockers"):
        blockers.append("input_validation_has_blockers")

    if reference_manifest is not None and reference_manifest.get("month_id") != month_id:
        blockers.append(f"reference_manifest_month_mismatch:{reference_manifest.get('month_id')}!={month_id}")

    input_contract = input_validation.get("input_contract", {})
    if input_contract.get("legacy_and_monthly_layouts_are_not_merged") is not True:
        blockers.append("legacy_monthly_non_merge_contract_missing")
    legacy_seen = input_validation.get("discovery", {}).get("legacy_files_detected_not_used") or []
    if legacy_seen:
        blockers.append("legacy_layout_detected_alongside_monthly_layout")

    if not grain_contract or "grain_contract" not in grain_contract or "event_identity" not in grain_contract:
        blockers.append("grain_contract_missing")
    if not hasattr(baseline_014c, "_canonical_dedupe") or not hasattr(baseline_014c, "_duplicate_summary"):
        blockers.append("dedupe_authority_missing")
    if not hasattr(dry_run_014e, "parse_photo_total"):
        blockers.append("dry_run_payload_authority_missing")
    return blockers


def select_files(input_validation: dict[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    files = input_validation.get("photo_reports", {}).get("files", [])
    blockers: list[str] = []
    by_role = {
        "include_candidate": [],
        "compare_only": [],
        "quarantine_truncation": [],
        "unclassified": [],
    }
    seen: set[str] = set()
    duplicated: set[str] = set()
    for item in files:
        if not isinstance(item, dict):
            continue
        source_file_id = str(item.get("source_file_id") or "")
        if source_file_id in seen:
            duplicated.add(source_file_id)
        seen.add(source_file_id)
        role = str(item.get("role") or "unclassified")
        if role not in ALLOWED_ROLES:
            by_role["unclassified"].append(item)
        else:
            by_role[role].append(item)
    for source_file_id in sorted(duplicated):
        blockers.append(f"duplicate_source_file_id:{source_file_id}")
    return by_role, blockers


def validate_reference_consistency(
    selected_files: list[dict[str, Any]],
    reference_by_id: dict[str, dict[str, Any]],
) -> list[str]:
    blockers: list[str] = []
    for entry in selected_files:
        source_file_id = str(entry.get("source_file_id") or "")
        reference = reference_by_id.get(source_file_id)
        if reference is None:
            blockers.append(f"reference_entry_missing:{source_file_id}")
            continue
        for key in ("relative_path", "sha256", "size_bytes", "row_count", "role"):
            if key in reference and key in entry and str(reference.get(key)) != str(entry.get(key)):
                blockers.append(f"reference_entry_{key}_mismatch:{source_file_id}")
    return blockers


def build_semantic_plan(
    *,
    month_id: str,
    validation_mode: str,
    as_of_date: str,
    input_validation_summary: dict[str, Any],
    reference_summary: dict[str, Any],
    grain_contract_summary: dict[str, Any],
    calendar_contract_summary: dict[str, Any],
    selected_files: list[dict[str, Any]],
) -> dict[str, Any]:
    coverage = month_contract.operational_coverage_for_month(month_id)
    source_tokens = sorted(
        f"{entry.get('source_file_id')}:{_entry_sha(entry)}" for entry in selected_files
    )
    return {
        "phase_id": PHASE_ID,
        "month_id": month_id,
        "validation_mode": validation_mode,
        "as_of_date": as_of_date,
        "input_validation_015b_sha256": input_validation_summary.get("semantic_sha256"),
        "reference_manifest_015a_sha256": reference_summary.get("semantic_sha256"),
        "operational_calendar_contract_sha256": calendar_contract_summary.get("sha256"),
        "grain_contract_sha256": grain_contract_summary.get("sha256"),
        "source_file_tokens": source_tokens,
        "dedupe_key": DEDUPE_KEY,
        "sort_before_keep": KEEP_SORT,
        "keep": "first",
        "operational_coverage": coverage,
        "required_calendar_months_at_close": month_contract.required_calendar_months_for_operational_month(
            month_id
        ),
        "logical_batch_definition": {
            "mode": "logical_batch_only",
            "reason": "014E has no contractual batch size authority",
            "batch_count": 1,
        },
        "payload_layer": LAYER,
        "payload_columns": PAYLOAD_COLUMNS,
    }


def build_dry_run_batch_id(load_plan_sha256: str) -> str:
    return "015C_" + load_plan_sha256[:24]


def _load_candidate_file(
    base: Path,
    entry: dict[str, Any],
    *,
    dry_run_batch_id: str,
) -> tuple[dict[str, Any], pd.DataFrame | None]:
    source_file_id = str(entry.get("source_file_id") or "")
    source_file_name = str(entry.get("source_file_name") or "")
    expected_sha = _entry_sha(entry)
    expected_size = _entry_size(entry)
    expected_rows = _entry_row_count(entry)
    relative_path = str(entry.get("relative_path") or "")
    path, path_error = _safe_relative_data_path(base, relative_path)
    integrity = {
        "source_file_id": source_file_id,
        "source_file_name": source_file_name,
        "relative_path": relative_path,
        "role": str(entry.get("role") or ""),
        "exists": bool(path and path.exists()),
        "path_error": path_error,
        "expected_sha256": expected_sha,
        "actual_sha256": None,
        "sha256_match": False,
        "expected_size_bytes": expected_size,
        "actual_size_bytes": None,
        "size_bytes_match": False,
        "expected_row_count": expected_rows,
        "actual_row_count": None,
        "row_count_match": False,
        "critical_columns_missing": [],
        "read_error": None,
        "invalid_date_rows": 0,
    }
    if path is None or not path.exists():
        integrity["read_error"] = path_error or "file_not_found"
        return integrity, None

    integrity["actual_size_bytes"] = path.stat().st_size
    integrity["size_bytes_match"] = expected_size is None or path.stat().st_size == expected_size
    integrity["actual_sha256"] = sha256_file(path)
    integrity["sha256_match"] = integrity["actual_sha256"] == expected_sha

    try:
        raw_df = baseline_014c.read_excel_sheet(path, baseline_014c.RAW_SHEET)
    except Exception as exc:
        integrity["read_error"] = f"{type(exc).__name__}:{exc}"
        return integrity, None

    raw_df.columns = [str(column).strip() for column in raw_df.columns]
    resolved = baseline_014c._resolved_columns(raw_df)
    critical_missing = [
        column
        for column in baseline_014c.CRITICAL_RAW_COLUMNS
        if baseline_014c.normalize_header(column) not in resolved
    ]
    integrity["critical_columns_missing"] = critical_missing
    integrity["actual_row_count"] = int(len(raw_df))
    integrity["row_count_match"] = expected_rows is None or int(len(raw_df)) == expected_rows
    if critical_missing:
        return integrity, None

    canonical = baseline_014c.normalize_raw_events(
        raw_df,
        source_file_id=source_file_id,
        source_file_sha256=str(integrity["actual_sha256"]),
    )
    integrity["invalid_date_rows"] = int(canonical["fecha"].isna().sum())

    photo_count_column = resolved.get(baseline_014c.normalize_header("Foto Nº/Total"))
    if photo_count_column is None:
        n_fotos = pd.Series(
            pd.array([pd.NA] * len(raw_df), dtype="Int64"),
            index=raw_df.index,
        )
    else:
        n_fotos = dry_run_014e.parse_photo_total(raw_df[photo_count_column])

    payload = canonical.copy()
    payload["source_file_name"] = source_file_name
    payload["n_fotos"] = n_fotos.reset_index(drop=True)
    payload["photo_row_hash"] = payload["_photo_row_hash"]
    payload["dry_run_batch_id"] = dry_run_batch_id
    return integrity, payload


def _add_operational_assignment(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if result.empty:
        result["assigned_operational_month"] = pd.Series(dtype="string")
        return result
    result["week_start"] = result["fecha"] - pd.to_timedelta(result["fecha"].dt.weekday, unit="D")

    def assign_month(value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None
        return month_contract.assigned_operational_month_from_fecha(value)

    result["assigned_operational_month"] = result["fecha"].map(assign_month)
    return result


def _summary_by_week(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    work = frame.copy()
    work["week_start_iso"] = work["week_start"].map(_date_iso)
    grouped = work.groupby("week_start_iso", dropna=False).size().reset_index(name="row_count")
    return [
        {"week_start": row["week_start_iso"], "row_count": int(row["row_count"])}
        for _, row in grouped.sort_values("week_start_iso").iterrows()
    ]


def _partition_summary(frame: pd.DataFrame, *, direction: str) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    work = frame.copy()
    work["fecha_iso"] = work["fecha"].map(_date_iso)
    work["week_start_iso"] = work["week_start"].map(_date_iso)
    rows: list[dict[str, Any]] = []
    for assigned_month, group in work.groupby("assigned_operational_month", dropna=False):
        rows.append(
            {
                "direction": direction,
                "assigned_operational_month": assigned_month,
                "row_count": int(len(group)),
                "fecha_min": min(value for value in group["fecha_iso"].dropna()) if group["fecha_iso"].notna().any() else None,
                "fecha_max": max(value for value in group["fecha_iso"].dropna()) if group["fecha_iso"].notna().any() else None,
                "operational_week_starts": sorted(
                    {value for value in group["week_start_iso"].dropna().tolist()}
                ),
                "source_file_ids": sorted({str(value) for value in group["source_file_id"].dropna().tolist()}),
            }
        )
    return sorted(rows, key=lambda item: str(item["assigned_operational_month"]))


def _row_identity(row: pd.Series) -> dict[str, Any]:
    return {
        "source_file_id": str(row["source_file_id"]),
        "source_row_number": int(row["source_row_number"]),
    }


def deterministic_payload_hash(frame: pd.DataFrame) -> tuple[str, dict[str, Any] | None, dict[str, Any] | None]:
    digest = hashlib.sha256()
    if frame.empty:
        return digest.hexdigest(), None, None
    ordered = frame.sort_values(KEEP_SORT, kind="mergesort").reset_index(drop=True)
    first_identity = _row_identity(ordered.iloc[0])
    last_identity = _row_identity(ordered.iloc[-1])
    hash_frame = ordered[PAYLOAD_COLUMNS].copy()
    for column in ("fecha", "week_start"):
        hash_frame[column] = hash_frame[column].map(_date_iso)
    hash_frame = hash_frame.astype(object).where(pd.notna(hash_frame), None)
    payload_lines = hash_frame.to_json(
        orient="records",
        lines=True,
        date_format="iso",
        force_ascii=False,
    )
    digest.update(payload_lines.encode("utf-8"))
    return digest.hexdigest(), first_identity, last_identity


def _safe_distinct_event_ids(frame: pd.DataFrame) -> int:
    if frame.empty or "event_id" not in frame:
        return 0
    valid = frame.loc[frame["event_id"] != "", "event_id"]
    return int(valid.nunique())


def _read_014e_baseline(base: Path) -> dict[str, Any]:
    path = base / BASELINE_014E_MANIFEST_PATH
    if not path.exists():
        return {"present": False, "path": BASELINE_014E_MANIFEST_PATH.as_posix()}
    try:
        payload = read_json_file(path)
    except Exception as exc:
        return {
            "present": True,
            "path": BASELINE_014E_MANIFEST_PATH.as_posix(),
            "read_error": f"{type(exc).__name__}:{exc}",
        }
    summary = payload.get("dry_run_payload_summary", {})
    return {
        "present": True,
        "path": BASELINE_014E_MANIFEST_PATH.as_posix(),
        "phase_id": payload.get("phase_id"),
        "verdict": payload.get("verdict"),
        "source_rows_total": summary.get("source_rows_total"),
        "exact_duplicate_rows_removed": summary.get("exact_duplicate_rows_removed"),
        "would_stage_calendar_rows": summary.get("would_stage_rows"),
        "distinct_event_ids_calendar": summary.get("distinct_event_ids"),
        "date_min": summary.get("date_min"),
        "date_max": summary.get("date_max"),
        "note": "014E is calendar-month historical baseline; 015C stages by operational month.",
    }


def _file_warning_rows(files: list[dict[str, Any]], roles: set[str]) -> list[str]:
    warnings: list[str] = []
    for entry in files:
        role = str(entry.get("role") or "")
        invalid_rows = int(entry.get("invalid_date_rows") or 0)
        if role in roles and invalid_rows:
            warnings.append(
                f"excluded_invalid_date_rows:{entry.get('source_file_name')}:{invalid_rows}"
            )
    return warnings


def candidate_file_identity_matches(base: Path, selected_files: list[dict[str, Any]]) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    for entry in selected_files:
        source_file_id = str(entry.get("source_file_id") or "")
        path, path_error = _safe_relative_data_path(base, str(entry.get("relative_path") or ""))
        if path is None or not path.exists():
            blockers.append(f"candidate_file_missing_for_reuse:{source_file_id}:{path_error or 'file_not_found'}")
            continue
        if _entry_size(entry) is not None and path.stat().st_size != _entry_size(entry):
            blockers.append(f"candidate_size_changed_for_reuse:{source_file_id}")
        if sha256_file(path) != _entry_sha(entry):
            blockers.append(f"candidate_sha256_changed_for_reuse:{source_file_id}")
    return not blockers, blockers


def reusable_existing_canonical_payload(
    *,
    base: Path,
    existing_json_path: Path | None,
    month_id: str,
    validation_mode: str,
    as_of_date: str,
    load_plan_sha256: str,
    selected_files: list[dict[str, Any]],
    current_blockers: list[str],
) -> dict[str, Any] | None:
    if existing_json_path is None or current_blockers or not existing_json_path.exists():
        return None
    try:
        existing = read_json_file(existing_json_path)
    except Exception:
        return None
    if existing.get("phase_id") != PHASE_ID:
        return None
    if existing.get("month_id") != month_id:
        return None
    if existing.get("validation_mode") != validation_mode:
        return None
    if existing.get("as_of_date") != as_of_date:
        return None
    if existing.get("load_plan_sha256") != load_plan_sha256:
        return None
    if existing.get("guardrails", {}).get("payload_rows_persisted") is not False:
        return None
    identity_ok, _ = candidate_file_identity_matches(base, selected_files)
    if not identity_ok:
        return None
    if not has_required_metric_definitions(existing):
        upgraded = dict(existing)
        upgraded["metric_definitions"] = duplicate_metric_definitions()
        return upgraded
    return existing


def build_dry_run_payload(
    *,
    base: Path,
    month_id: str,
    validation_mode: str = "close",
    as_of_date: str | None = None,
    input_validation_json: str | None = None,
    reference_manifest: str | None = None,
    canonical: bool = False,
    existing_canonical_json: Path | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    month_id = normalize_month_id(month_id)
    validation_mode = normalize_validation_mode(validation_mode)
    as_of_date = parse_as_of_date(as_of_date)

    input_validation_path = (
        resolve_path(base, input_validation_json)
        if input_validation_json
        else default_input_validation_path(base, month_id)
    )
    blockers: list[str] = []
    warnings: list[str] = []
    try:
        input_validation = read_json_file(input_validation_path)
    except Exception as exc:
        input_validation = {}
        blockers.append(f"input_validation_unreadable:{type(exc).__name__}:{exc}")
    input_validation_summary = {
        "path": relative_to_base(base, input_validation_path),
        "present": input_validation_path.exists(),
        "sha256": sha256_file(input_validation_path) if input_validation_path.exists() else None,
        "semantic_sha256": semantic_input_validation_sha256(input_validation) if input_validation else None,
        "phase_id": input_validation.get("phase_id"),
        "verdict": input_validation.get("verdict"),
    }

    grain_contract, grain_summary, grain_blockers = load_authority(
        base, GRAIN_CONTRACT_PATH, required_keys=["grain_contract", "event_identity"]
    )
    calendar_contract, calendar_summary, calendar_blockers = load_authority(
        base, OPERATIONAL_CALENDAR_CONTRACT_PATH, required_keys=["rules"]
    )
    del calendar_contract
    reference, reference_summary, reference_blockers = resolve_reference_manifest(
        base,
        month_id=month_id,
        input_validation=input_validation,
        override=reference_manifest,
        validation_mode=validation_mode,
    )
    reference_summary["semantic_sha256"] = semantic_reference_manifest_sha256(reference)
    blockers.extend(grain_blockers)
    blockers.extend(calendar_blockers)
    blockers.extend(reference_blockers)
    blockers.extend(
        validate_input_preconditions(
            month_id=month_id,
            validation_mode=validation_mode,
            as_of_date=as_of_date,
            input_validation=input_validation,
            reference_manifest=reference,
            grain_contract=grain_contract,
        )
    )

    by_role, role_blockers = select_files(input_validation)
    blockers.extend(role_blockers)
    if validation_mode == "close" and by_role["unclassified"]:
        blockers.append(f"unclassified_role_in_close:{len(by_role['unclassified'])}")

    selected_files = by_role["include_candidate"]
    blockers.extend(validate_reference_consistency(selected_files, file_map_from_reference(reference)))
    for entry in selected_files:
        relative_path = str(entry.get("relative_path") or "")
        if not relative_path.replace("\\", "/").startswith("data/kpione_photo_reports/"):
            blockers.append(f"candidate_not_monthly_layout:{entry.get('source_file_id')}")

    semantic_plan = build_semantic_plan(
        month_id=month_id,
        validation_mode=validation_mode,
        as_of_date=as_of_date,
        input_validation_summary=input_validation_summary,
        reference_summary=reference_summary,
        grain_contract_summary=grain_summary,
        calendar_contract_summary=calendar_summary,
        selected_files=selected_files,
    )
    load_plan_sha256 = sha256_canonical(semantic_plan)
    dry_run_batch_id = build_dry_run_batch_id(load_plan_sha256)
    reusable = reusable_existing_canonical_payload(
        base=base,
        existing_json_path=existing_canonical_json,
        month_id=month_id,
        validation_mode=validation_mode,
        as_of_date=as_of_date,
        load_plan_sha256=load_plan_sha256,
        selected_files=selected_files,
        current_blockers=blockers,
    )
    if reusable is not None:
        return reusable

    integrity_rows: list[dict[str, Any]] = []
    candidate_frames: list[pd.DataFrame] = []
    for entry in selected_files:
        integrity, payload = _load_candidate_file(
            base,
            entry,
            dry_run_batch_id=dry_run_batch_id,
        )
        integrity_rows.append(integrity)
        if integrity["read_error"]:
            blockers.append(f"candidate_file_unreadable:{integrity['source_file_id']}:{integrity['read_error']}")
            continue
        if not integrity["sha256_match"]:
            blockers.append(f"candidate_sha256_mismatch:{integrity['source_file_id']}")
            continue
        if not integrity["size_bytes_match"]:
            blockers.append(f"candidate_size_bytes_mismatch:{integrity['source_file_id']}")
        if not integrity["row_count_match"]:
            blockers.append(f"candidate_row_count_mismatch:{integrity['source_file_id']}")
        if integrity["critical_columns_missing"]:
            blockers.append(
                f"candidate_critical_columns_missing:{integrity['source_file_id']}:"
                + ",".join(integrity["critical_columns_missing"])
            )
            continue
        if payload is not None:
            candidate_frames.append(payload)

    raw_candidate = (
        pd.concat(candidate_frames, ignore_index=True)
        if candidate_frames
        else pd.DataFrame(columns=baseline_014c.CANONICAL_COLUMNS + ["_photo_row_hash", "source_file_name", "n_fotos", "photo_row_hash", "dry_run_batch_id"])
    )

    invalid_date_rows_eligible = int(raw_candidate["fecha"].isna().sum()) if "fecha" in raw_candidate else 0
    if invalid_date_rows_eligible:
        blockers.append(f"eligible_invalid_date_rows:{invalid_date_rows_eligible}")

    duplicate_group_rows = (
        int(raw_candidate.duplicated(subset=DEDUPE_KEY, keep=False).sum())
        if not raw_candidate.empty
        else 0
    )
    duplicate_summary = baseline_014c._duplicate_summary(raw_candidate)
    deduped, exact_duplicate_rows_removed = baseline_014c._canonical_dedupe(raw_candidate)
    deduped = _add_operational_assignment(deduped)

    target = deduped[deduped["assigned_operational_month"] == month_id].copy()
    carry_forward = deduped[deduped["assigned_operational_month"].fillna("") > month_id].copy()
    carry_backfill = deduped[deduped["assigned_operational_month"].fillna("") < month_id].copy()

    if duplicate_summary["same_id_diff_hash_count"]:
        blockers.append(f"candidate_same_id_diff_hash:{duplicate_summary['same_id_diff_hash_count']}")
    if duplicate_summary["event_stable_hash_conflict_count"]:
        blockers.append(
            f"candidate_event_stable_hash_conflicts:{duplicate_summary['event_stable_hash_conflict_count']}"
        )

    upstream_validation_warnings = sorted(str(item) for item in input_validation.get("warnings", []))
    eligible_data_warnings: list[str] = []
    if exact_duplicate_rows_removed:
        eligible_data_warnings.append(f"exact_duplicate_rows_removed:{exact_duplicate_rows_removed}")
    if duplicate_summary["same_id_same_hash_count"]:
        eligible_data_warnings.append(f"same_id_same_hash:{duplicate_summary['same_id_same_hash_count']}")
    if invalid_date_rows_eligible:
        eligible_data_warnings.append(f"eligible_invalid_date_rows:{invalid_date_rows_eligible}")

    all_non_candidate = by_role["compare_only"] + by_role["quarantine_truncation"] + by_role["unclassified"]
    excluded_file_warnings = _file_warning_rows(
        all_non_candidate, {"compare_only", "quarantine_truncation", "unclassified"}
    )
    warnings.extend(upstream_validation_warnings)
    warnings.extend(eligible_data_warnings)
    warnings.extend(excluded_file_warnings)
    if input_validation.get("verdict") == "WARN":
        warnings.append("input_validation_verdict_warn")
    if by_role["compare_only"] or by_role["quarantine_truncation"]:
        warnings.append(
            "non_candidate_files_excluded:"
            + str(len(by_role["compare_only"]) + len(by_role["quarantine_truncation"]))
        )

    payload_hash, first_identity, last_identity = deterministic_payload_hash(target)
    target_week_starts = sorted(
        {value for value in target["week_start"].map(_date_iso).dropna().tolist()}
    ) if not target.empty else []
    target_source_ids = sorted({str(value) for value in target["source_file_id"].dropna().tolist()}) if not target.empty else []

    row_accounting = {
        "source_rows_selected_files": int(sum(item.get("actual_row_count") or 0 for item in integrity_rows)),
        "normalized_candidate_rows": int(len(raw_candidate)),
        "invalid_date_rows_eligible": invalid_date_rows_eligible,
        "exact_duplicate_rows_detected": duplicate_group_rows,
        "exact_duplicate_rows_removed": int(exact_duplicate_rows_removed),
        "survivor_rows_after_dedupe": int(len(deduped)),
        "would_stage_rows": int(len(target)),
        "carry_forward_out_rows": int(len(carry_forward)),
        "carry_backfill_out_rows": int(len(carry_backfill)),
        "distinct_event_ids_would_stage": _safe_distinct_event_ids(target),
    }

    verdict = "BLOCKED" if blockers else ("WARN" if warnings else "PASS")
    duration_seconds = round(time.perf_counter() - started, 3)
    run_metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_seconds": duration_seconds,
        "canonical": bool(canonical),
        "local_paths": {
            "input_validation_json": input_validation_summary["path"],
            "reference_manifest": reference_summary["path"],
        },
    }
    guardrails = {
        "db_access": False,
        "supabase_access": False,
        "sql_apply": False,
        "ddl": False,
        "data_movement": False,
        "payload_rows_persisted": False,
    }

    return {
        "phase_id": PHASE_ID,
        "month_id": month_id,
        "validation_mode": validation_mode,
        "as_of_date": as_of_date,
        "guardrails": guardrails,
        "authorities": {
            "input_validation_015b": input_validation_summary,
            "reference_manifest_015a": reference_summary,
            "grain_contract": grain_summary,
            "operational_calendar_contract": calendar_summary,
            "normalization_and_dedupe": {
                "014c_script": "scripts/validate_kpione_raw_exports_014C_no_apply.py",
                "014e_script": "scripts/load_kpione_raw_exports_014E_dry_run_no_apply.py",
                "dedupe_key": DEDUPE_KEY,
                "keep_rule": "first after source_file_id/source_row_number sort",
            },
        },
        "run_metadata": run_metadata,
        "semantic_plan": semantic_plan,
        "input_validation": {
            "phase_id": input_validation.get("phase_id"),
            "verdict": input_validation.get("verdict"),
            "blockers": input_validation.get("blockers", []),
            "warnings": input_validation.get("warnings", []),
            "operational_coverage": input_validation.get("operational_coverage"),
            "operational_period_status": input_validation.get("operational_period_status"),
        },
        "selection": {
            "include_candidate_files": [compact_file_entry(item) for item in by_role["include_candidate"]],
            "compare_only_files": [compact_file_entry(item) for item in by_role["compare_only"]],
            "quarantine_files": [compact_file_entry(item) for item in by_role["quarantine_truncation"]],
            "unclassified_files": [compact_file_entry(item) for item in by_role["unclassified"]],
        },
        "row_accounting": row_accounting,
        "metric_definitions": duplicate_metric_definitions(),
        "operational_partition": {
            "target_month": {
                "month_id": month_id,
                "row_count": int(len(target)),
                "fecha_min": _date_iso(target["fecha"].min()) if not target.empty else None,
                "fecha_max": _date_iso(target["fecha"].max()) if not target.empty else None,
                "operational_week_starts": target_week_starts,
                "week_start_counts": _summary_by_week(target),
                "coverage_start": semantic_plan["operational_coverage"]["operational_coverage_start"],
                "coverage_end": semantic_plan["operational_coverage"]["operational_coverage_end"],
            },
            "carry_forward_out": _partition_summary(carry_forward, direction="future"),
            "carry_backfill_out": _partition_summary(carry_backfill, direction="past"),
        },
        "dedupe_summary": {
            **duplicate_summary,
            "key": DEDUPE_KEY,
            "sort_before_keep": KEEP_SORT,
            "keep": "first",
            "exact_duplicate_rows_detected": duplicate_group_rows,
            "exact_duplicate_rows_removed": int(exact_duplicate_rows_removed),
        },
        "batch_plan": {
            "mode": "logical_batch_only",
            "batch_count": 1,
            "dry_run_batch_id": dry_run_batch_id,
            "batches": [
                {
                    "batch_number": 1,
                    "row_count": int(len(target)),
                    "source_file_ids": target_source_ids,
                    "operational_week_starts": target_week_starts,
                    "first_source_row_identity": first_identity,
                    "last_source_row_identity": last_identity,
                    "deterministic_payload_sha256": payload_hash,
                }
            ],
        },
        "excluded_evidence": {
            "compare_only": [compact_file_entry(item) for item in by_role["compare_only"]],
            "quarantine_truncation": [compact_file_entry(item) for item in by_role["quarantine_truncation"]],
            "unclassified": [compact_file_entry(item) for item in by_role["unclassified"]],
        },
        "baseline_014e": _read_014e_baseline(base),
        "upstream_validation_warnings": upstream_validation_warnings,
        "eligible_data_warnings": sorted(set(eligible_data_warnings)),
        "excluded_file_warnings": sorted(set(excluded_file_warnings)),
        "load_plan_sha256": load_plan_sha256,
        "blockers": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
        "verdict": verdict,
    }


def _md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join("" if value is None else str(value) for value in row) + " |")
    return lines


def report_markdown(payload: dict[str, Any]) -> str:
    accounting = payload["row_accounting"]
    partition = payload["operational_partition"]
    batch = payload["batch_plan"]["batches"][0]
    baseline = payload["baseline_014e"]
    dedupe = payload["dedupe_summary"]
    definitions = payload.get("metric_definitions", duplicate_metric_definitions())
    lines = [
        "# 015C KPIONE monthly load dry-run no apply",
        "",
        "## Verdict",
        "",
        f"- Verdict: `{payload['verdict']}`",
        f"- Operational month: `{payload['month_id']}`",
        f"- Validation mode: `{payload['validation_mode']}`",
        f"- As-of date: `{payload['as_of_date']}`",
        "",
        "## Guardrails",
        "",
        f"- DB access: `{payload['guardrails']['db_access']}`",
        f"- Supabase access: `{payload['guardrails']['supabase_access']}`",
        f"- SQL apply: `{payload['guardrails']['sql_apply']}`",
        f"- DDL: `{payload['guardrails']['ddl']}`",
        f"- Data movement: `{payload['guardrails']['data_movement']}`",
        f"- Payload rows persisted: `{payload['guardrails']['payload_rows_persisted']}`",
        "",
        "## Authorities",
        "",
        f"- 015B validation: `{payload['authorities']['input_validation_015b']['path']}`",
        f"- 015A manifest: `{payload['authorities']['reference_manifest_015a']['path']}`",
        f"- Grain contract: `{payload['authorities']['grain_contract']['path']}`",
        f"- Operational calendar contract: `{payload['authorities']['operational_calendar_contract']['path']}`",
        f"- Dedupe authority: `014C/014E historical dry-run`",
        "",
        "## Selected and excluded files",
        "",
        f"- include_candidate: `{len(payload['selection']['include_candidate_files'])}`",
        f"- compare_only: `{len(payload['selection']['compare_only_files'])}`",
        f"- quarantine_truncation: `{len(payload['selection']['quarantine_files'])}`",
        f"- unclassified: `{len(payload['selection']['unclassified_files'])}`",
        "",
        "## 014E baseline versus 015C result",
        "",
        f"- 014E source rows: `{baseline.get('source_rows_total')}`",
        f"- 014E would stage calendar rows: `{baseline.get('would_stage_calendar_rows')}`",
        f"- 014E exact duplicates removed: `{baseline.get('exact_duplicate_rows_removed')}`",
        f"- 015C would stage operational rows: `{accounting['would_stage_rows']}`",
        f"- 015C carry-forward out rows: `{accounting['carry_forward_out_rows']}`",
        "",
        "## Row accounting",
        "",
    ]
    lines.extend(
        _md_table(
            ["metric", "value"],
            [[key, value] for key, value in accounting.items()],
        )
    )
    lines.extend(
        [
            "",
            "## Operational partition",
            "",
            f"- Target month rows: `{partition['target_month']['row_count']}`",
            f"- Target dates: `{partition['target_month']['fecha_min']}..{partition['target_month']['fecha_max']}`",
            f"- Target weeks: `{', '.join(partition['target_month']['operational_week_starts'])}`",
            f"- Carry-forward out: `{accounting['carry_forward_out_rows']}`",
            f"- Carry-backfill out: `{accounting['carry_backfill_out_rows']}`",
            "",
            "## Dedupe",
            "",
            f"- Key: `{dedupe['key']}`",
            f"- Sort before keep: `{dedupe['sort_before_keep']}`",
            f"- Keep: `{dedupe['keep']}`",
            f"- Rows participating in exact duplicate groups: `{dedupe['exact_duplicate_rows_detected']}`",
            f"- Exact duplicate rows removed: `{dedupe['exact_duplicate_rows_removed']}`",
            f"- Distinct event IDs with same hash fingerprint across files: `{dedupe['same_id_same_hash_count']}`",
            f"- Distinct event IDs with different hash fingerprints across files: `{dedupe['same_id_diff_hash_count']}`",
            f"- Distinct event IDs with stable-hash conflicts: `{dedupe['event_stable_hash_conflict_count']}`",
            "",
            "## Duplicate metric definitions",
            "",
        ]
    )
    lines.extend(
        _md_table(
            ["Metric", "Unit", "Exact meaning"],
            [
                [
                    metric,
                    spec["unit"],
                    f"{spec['definition']} Universe: {spec['universe']}.",
                ]
                for metric in duplicate_metric_definitions()
                for spec in [definitions[metric]]
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Batch plan",
            "",
            f"- Mode: `{payload['batch_plan']['mode']}`",
            f"- Batch count: `{payload['batch_plan']['batch_count']}`",
            f"- Batch row count: `{batch['row_count']}`",
            f"- Deterministic payload sha256: `{batch['deterministic_payload_sha256']}`",
            "",
            "## Hashes",
            "",
            f"- load_plan_sha256: `{payload['load_plan_sha256']}`",
            f"- dry_run_batch_id: `{payload['batch_plan']['dry_run_batch_id']}`",
            "",
            "## Blockers",
            "",
        ]
    )
    lines.extend([f"- `{item}`" for item in payload["blockers"]] or ["- None"])
    lines.extend(["", "## Warnings", ""])
    lines.extend([f"- `{item}`" for item in payload["warnings"]] or ["- None"])
    lines.extend(
        [
            "",
            "## No-apply declaration",
            "",
            "This artifact is an aggregate local dry-run. No DB connection, SQL apply, productive loader run, data movement, or row-level payload export was performed.",
            "",
        ]
    )
    return "\n".join(lines)


def _with_preserved_run_metadata(new_payload: dict[str, Any], existing_path: Path) -> dict[str, Any]:
    if not existing_path.exists():
        return new_payload
    try:
        existing = read_json_file(existing_path)
    except Exception:
        return new_payload
    same_semantics = (
        existing.get("load_plan_sha256") == new_payload.get("load_plan_sha256")
        and existing.get("row_accounting") == new_payload.get("row_accounting")
        and existing.get("operational_partition") == new_payload.get("operational_partition")
        and existing.get("batch_plan") == new_payload.get("batch_plan")
        and existing.get("blockers") == new_payload.get("blockers")
        and existing.get("warnings") == new_payload.get("warnings")
        and existing.get("verdict") == new_payload.get("verdict")
    )
    if same_semantics and isinstance(existing.get("run_metadata"), dict):
        preserved = dict(new_payload)
        preserved["run_metadata"] = {
            **new_payload.get("run_metadata", {}),
            "generated_at": existing["run_metadata"].get("generated_at"),
            "duration_seconds": existing["run_metadata"].get("duration_seconds"),
        }
        return preserved
    return new_payload


def write_outputs(
    *,
    base: Path,
    payload: dict[str, Any],
    json_out: str | None = None,
    md_out: str | None = None,
    canonical: bool = False,
) -> dict[str, Any]:
    if not canonical and not json_out and not md_out:
        return {"json_path": None, "md_path": None, "json_changed": False, "md_changed": False}

    json_path = resolve_path(base, json_out) if json_out else default_json_output_path(base, payload["month_id"])
    md_path = resolve_path(base, md_out) if md_out else default_md_output_path(base, payload["month_id"])
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)

    payload_to_write = _with_preserved_run_metadata(payload, json_path) if canonical else payload
    json_text = json.dumps(payload_to_write, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    md_text = report_markdown(payload_to_write)
    json_changed = not json_path.exists() or json_path.read_text(encoding="utf-8") != json_text
    md_changed = not md_path.exists() or md_path.read_text(encoding="utf-8") != md_text
    if json_changed:
        json_path.write_text(json_text, encoding="utf-8", newline="\n")
    if md_changed:
        md_path.write_text(md_text, encoding="utf-8", newline="\n")
    return {
        "json_path": relative_to_base(base, json_path),
        "md_path": relative_to_base(base, md_path),
        "json_changed": json_changed,
        "md_changed": md_changed,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="015C KPIONE monthly raw load dry-run no apply")
    parser.add_argument("--month", required=True, help="Operational month in YYYY-MM format")
    parser.add_argument("--validation-mode", choices=["open", "close"], default="close")
    parser.add_argument("--as-of", default=None, help="YYYY-MM-DD; defaults to local date")
    parser.add_argument("--input-validation-json", default=None)
    parser.add_argument("--reference-manifest", default=None)
    parser.add_argument("--json-out", default=None)
    parser.add_argument("--md-out", default=None)
    parser.add_argument("--canonical", action="store_true")
    parser.add_argument("--soft-exit", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    base = Path.cwd()
    try:
        normalized_month = normalize_month_id(args.month)
        existing_canonical_json = None
        if args.canonical:
            existing_canonical_json = (
                resolve_path(base, args.json_out)
                if args.json_out
                else default_json_output_path(base, normalized_month)
            )
        payload = build_dry_run_payload(
            base=base,
            month_id=normalized_month,
            validation_mode=args.validation_mode,
            as_of_date=args.as_of,
            input_validation_json=args.input_validation_json,
            reference_manifest=args.reference_manifest,
            canonical=args.canonical,
            existing_canonical_json=existing_canonical_json,
        )
    except UsageError as exc:
        print(f"usage_error: {exc}", file=sys.stderr)
        return 2
    outputs = write_outputs(
        base=base,
        payload=payload,
        json_out=args.json_out,
        md_out=args.md_out,
        canonical=args.canonical,
    )
    print(
        json.dumps(
            {
                "phase_id": payload["phase_id"],
                "month_id": payload["month_id"],
                "validation_mode": payload["validation_mode"],
                "as_of_date": payload["as_of_date"],
                "verdict": payload["verdict"],
                "blocker_count": len(payload["blockers"]),
                "warning_count": len(payload["warnings"]),
                "would_stage_rows": payload["row_accounting"]["would_stage_rows"],
                "carry_forward_out_rows": payload["row_accounting"]["carry_forward_out_rows"],
                "load_plan_sha256": payload["load_plan_sha256"],
                "json_path": outputs["json_path"],
                "md_path": outputs["md_path"],
                "json_changed": outputs["json_changed"],
                "md_changed": outputs["md_changed"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    if payload["verdict"] == "BLOCKED" and not args.soft_exit:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
