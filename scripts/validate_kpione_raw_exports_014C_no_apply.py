# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any

import pandas as pd


PHASE_ID = "014C_KPIONE_RAW_EXPORT_VALIDATOR_NO_APPLY"
RAW_PATTERN = "data/photo-excel-admin_*.xlsx"
RAW_SHEET = "Fotos"
LEGACY_RELATIVE_PATH = Path("data/CUMPLIMIENTO_FRECUENCIA.xlsx")
LEGACY_SHEET = "DB (KPIONE2.0)"
PARITY_DATE = "2026-06-01"
PARITY_READY_THRESHOLD = 0.99
TRUNCATION_ROW_THRESHOLD = 50_000
OUTPUT_RELATIVE_DIR = Path("research/014C_KPIONE_RAW_EXPORT_VALIDATOR_NO_APPLY")
MANIFEST_FILENAME = "014C_kpione_raw_export_manifest.json"
REPORT_FILENAME = "014C_kpione_raw_export_report.md"

EXPECTED_RAW_COLUMNS = [
    "ID",
    "SP Item ID",
    "Holding",
    "Subcadena",
    "Codigo Local",
    "Marca",
    "Local",
    "Direccion",
    "Reponedor",
    "Fecha",
    "Fecha de subida",
    "Hora",
    "Tipo de Tarea",
    "Foto Nº/Total",
    "Comentarios",
    "Link Foto",
]
CRITICAL_RAW_COLUMNS = [
    "ID",
    "SP Item ID",
    "Codigo Local",
    "Marca",
    "Local",
    "Fecha",
    "Reponedor",
    "Tipo de Tarea",
    "Link Foto",
]
EVENT_STABLE_COLUMNS = [
    "ID",
    "SP Item ID",
    "Holding",
    "Subcadena",
    "Codigo Local",
    "Marca",
    "Local",
    "Direccion",
    "Reponedor",
    "Fecha",
    "Comentarios",
]
CANONICAL_COLUMNS = [
    "event_id",
    "sp_item_id",
    "cod_rt",
    "local_nombre",
    "cliente_norm",
    "fecha",
    "week_start",
    "reponedor",
    "tipo_tarea",
    "link_foto",
    "event_stable_hash",
    "source_file_id",
    "source_file_sha256",
    "source_row_number",
]

VERDICT_READY = "RAW_EXPORTS_READY_FOR_DRY_RUN_LOADER"
VERDICT_PARTIAL = "RAW_EXPORTS_PARTIAL_NEEDS_MORE_EXPORTS"
VERDICT_BLOCKED_CONFLICT = "RAW_EXPORTS_BLOCKED_BY_TRUNCATION_OR_CONFLICT"
VERDICT_BLOCKED_SCHEMA = "RAW_EXPORTS_BLOCKED_BY_SCHEMA"


def normalize_header(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("Âº", "º").replace("Â°", "º").replace("°", "º")
    text = re.sub(r"n\s*º", "n", text, flags=re.IGNORECASE)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("º", "n")
    text = re.sub(r"[^0-9A-Za-z]+", "_", text.strip().lower())
    return text.strip("_")


def parse_source_file_id(path_or_name: str | Path) -> str:
    name = Path(path_or_name).name
    match = re.fullmatch(r"photo-excel-admin_(\d+)\.xlsx", name, flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"invalid raw export filename: {name}")
    return match.group(1)


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_excel_sheet(path: str | Path, sheet_name: str) -> pd.DataFrame:
    source = Path(path)
    with pd.ExcelFile(source, engine="openpyxl") as workbook:
        if sheet_name not in workbook.sheet_names:
            raise ValueError(f"sheet_not_found:{sheet_name}")
        return pd.read_excel(
            workbook,
            sheet_name=sheet_name,
            dtype=object,
        )


def _clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        if value.time() == datetime.min.time():
            return value.date().isoformat()
        return value.isoformat()
    text = str(value).strip()
    return re.sub(r"\s+", " ", text)


def _clean_id(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return _clean_text(value)


def _normalize_key(value: object) -> str:
    text = unicodedata.normalize("NFKD", _clean_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text.strip()).upper()


def _parse_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def _date_iso(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).date().isoformat()


def _date_list(values: pd.Series) -> list[str]:
    return sorted(
        {
            value
            for value in (_date_iso(item) for item in values.dropna())
            if value is not None
        }
    )


def _resolved_columns(df: pd.DataFrame) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for column in df.columns:
        resolved.setdefault(normalize_header(column), str(column).strip())
    return resolved


def _column_series(
    df: pd.DataFrame,
    resolved: dict[str, str],
    expected_name: str,
) -> pd.Series:
    actual = resolved.get(normalize_header(expected_name))
    if actual is None:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[actual]


def _hash_text_series(values: pd.Series) -> pd.Series:
    return values.map(lambda text: hashlib.sha256(str(text).encode("utf-8")).hexdigest())


def _normalized_hash_frame(
    df: pd.DataFrame,
    resolved: dict[str, str],
    source_columns: list[str],
    *,
    case_insensitive: bool,
) -> pd.Series:
    normalized = pd.DataFrame(index=df.index)
    for source_name in source_columns:
        series = _column_series(df, resolved, source_name)
        if normalize_header(source_name) == "fecha":
            normalized[source_name] = _parse_dates(series).map(_date_iso).fillna("")
        elif case_insensitive:
            normalized[source_name] = series.map(_normalize_key)
        else:
            normalized[source_name] = series.map(_clean_text)
    joined = normalized.astype("string").fillna("").agg("\x1f".join, axis=1)
    return _hash_text_series(joined)


def normalize_raw_events(
    raw_df: pd.DataFrame,
    *,
    source_file_id: str,
    source_file_sha256: str,
) -> pd.DataFrame:
    df = raw_df.copy(deep=True)
    df.columns = [str(column).strip() for column in df.columns]
    resolved = _resolved_columns(df)
    missing = [
        column
        for column in CRITICAL_RAW_COLUMNS
        if normalize_header(column) not in resolved
    ]
    if missing:
        raise ValueError(f"critical_columns_missing:{','.join(missing)}")

    fecha = _parse_dates(_column_series(df, resolved, "Fecha"))
    canonical = pd.DataFrame(index=df.index)
    canonical["event_id"] = _column_series(df, resolved, "ID").map(_clean_id)
    canonical["sp_item_id"] = _column_series(df, resolved, "SP Item ID").map(_clean_id)
    canonical["cod_rt"] = _column_series(df, resolved, "Codigo Local").map(_clean_text)
    canonical["local_nombre"] = _column_series(df, resolved, "Local").map(_clean_text)
    canonical["cliente_norm"] = _column_series(df, resolved, "Marca").map(_normalize_key)
    canonical["fecha"] = fecha
    canonical["week_start"] = fecha - pd.to_timedelta(fecha.dt.weekday, unit="D")
    canonical["reponedor"] = _column_series(df, resolved, "Reponedor").map(_clean_text)
    canonical["tipo_tarea"] = _column_series(df, resolved, "Tipo de Tarea").map(_clean_text)
    canonical["link_foto"] = _column_series(df, resolved, "Link Foto").map(_clean_text)
    canonical["event_stable_hash"] = _normalized_hash_frame(
        df,
        resolved,
        EVENT_STABLE_COLUMNS,
        case_insensitive=True,
    )
    canonical["source_file_id"] = str(source_file_id)
    canonical["source_file_sha256"] = str(source_file_sha256)
    canonical["source_row_number"] = pd.RangeIndex(start=2, stop=2 + len(df))
    canonical["_photo_row_hash"] = _normalized_hash_frame(
        df,
        resolved,
        EXPECTED_RAW_COLUMNS,
        case_insensitive=False,
    )
    return canonical.reset_index(drop=True)


def compute_visit_fraction(
    canonical_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = canonical_df.copy(deep=True)
    cod_key = rows["cod_rt"].map(_normalize_key)
    local_key = rows["local_nombre"].map(_normalize_key)
    cliente_key = rows["cliente_norm"].map(_normalize_key)
    rows["_visit_location_type"] = cod_key.map(lambda value: "COD_RT" if value else "LOCAL")
    rows["_visit_location_key"] = cod_key.where(cod_key != "", local_key)
    rows["_visit_cliente_key"] = cliente_key
    rows["visit_fraction"] = float("nan")

    eligible = (
        rows["fecha"].notna()
        & (rows["_visit_location_key"] != "")
        & (rows["_visit_cliente_key"] != "")
    )
    group_columns = [
        "_visit_location_type",
        "_visit_location_key",
        "fecha",
        "_visit_cliente_key",
    ]
    if eligible.any():
        group_size = rows.loc[eligible].groupby(group_columns, dropna=False)[
            "event_id"
        ].transform("size")
        rows.loc[eligible, "visit_fraction"] = 1.0 / group_size.astype(float)

    group_summary = (
        rows.loc[eligible]
        .groupby(group_columns, dropna=False)
        .agg(
            row_count=("event_id", "size"),
            visit_sum=("visit_fraction", "sum"),
        )
        .reset_index()
        .rename(
            columns={
                "_visit_location_type": "location_key_type",
                "_visit_location_key": "location_key",
                "_visit_cliente_key": "cliente_norm",
            }
        )
    )
    return rows, group_summary


def compute_overlap_matrix(
    ids_by_source: dict[str, set[str]],
) -> dict[str, Any]:
    source_ids = sorted(ids_by_source)
    matrix: list[dict[str, Any]] = []
    for source_a in source_ids:
        row: dict[str, Any] = {"source_file_id": source_a}
        for source_b in source_ids:
            row[source_b] = len(ids_by_source[source_a] & ids_by_source[source_b])
        matrix.append(row)

    pairs: list[dict[str, Any]] = []
    for source_a, source_b in combinations(source_ids, 2):
        overlap = sorted(ids_by_source[source_a] & ids_by_source[source_b])
        pairs.append(
            {
                "source_file_id_a": source_a,
                "source_file_id_b": source_b,
                "overlap_id_count": len(overlap),
                "sample_ids": overlap[:20],
            }
        )
    return {
        "source_file_ids": source_ids,
        "matrix": matrix,
        "pairs": pairs,
    }


def compute_june_coverage(
    canonical_df: pd.DataFrame,
    month: str,
) -> dict[str, Any]:
    period = pd.Period(month, freq="M")
    month_start = period.start_time.normalize()
    month_end = period.end_time.normalize()
    month_days = pd.date_range(month_start, month_end, freq="D")
    covered = {
        pd.Timestamp(value).normalize()
        for value in canonical_df["fecha"].dropna()
        if month_start <= pd.Timestamp(value).normalize() <= month_end
    }
    missing = [day for day in month_days if day not in covered]

    operational_weeks: list[dict[str, Any]] = []
    week_start = month_start
    while week_start.weekday() != 0:
        week_start += pd.Timedelta(days=1)
    week_number = 1
    while week_start + pd.Timedelta(days=6) <= month_end:
        week_end = week_start + pd.Timedelta(days=6)
        week_days = pd.date_range(week_start, week_end, freq="D")
        week_covered = [day for day in week_days if day in covered]
        week_missing = [day for day in week_days if day not in covered]
        status = (
            "COMPLETE"
            if not week_missing
            else "PARTIAL"
            if week_covered
            else "MISSING"
        )
        operational_weeks.append(
            {
                "label": f"S{week_number}",
                "start": _date_iso(week_start),
                "end": _date_iso(week_end),
                "status": status,
                "covered_days": [_date_iso(day) for day in week_covered],
                "missing_days": [_date_iso(day) for day in week_missing],
            }
        )
        week_start += pd.Timedelta(days=7)
        week_number += 1

    rollover_week_start = week_start
    rollover_month_days = [
        day
        for day in pd.date_range(
            rollover_week_start,
            rollover_week_start + pd.Timedelta(days=6),
            freq="D",
        )
        if month_start <= day <= month_end
    ]
    focus_start = max(month_start, pd.Timestamp(f"{month}-25"))
    focus_days = pd.date_range(focus_start, month_end, freq="D")
    focus_missing = [day for day in focus_days if day not in covered]
    return {
        "month": month,
        "month_start": _date_iso(month_start),
        "month_end": _date_iso(month_end),
        "covered_day_count": len(covered),
        "covered_days": [_date_iso(day) for day in sorted(covered)],
        "missing_day_count": len(missing),
        "missing_days": [_date_iso(day) for day in missing],
        "operational_weeks": operational_weeks,
        "operational_coverage_complete": all(
            week["status"] == "COMPLETE" for week in operational_weeks
        ),
        "calendar_month_complete": not missing,
        "rollover_week": {
            "start": _date_iso(rollover_week_start),
            "end": _date_iso(rollover_week_start + pd.Timedelta(days=6)),
            "belongs_to_next_month": True,
            "days_inside_requested_month": [
                _date_iso(day) for day in rollover_month_days
            ],
        },
        "missing_2026_06_25_to_30": [
            _date_iso(day) for day in focus_missing
        ]
        if month == "2026-06"
        else [],
    }


def compute_legacy_parity(
    canonical_df: pd.DataFrame,
    legacy_df: pd.DataFrame,
    *,
    target_date: str = PARITY_DATE,
) -> dict[str, Any]:
    resolved = _resolved_columns(legacy_df)
    required = ["ID", "Fecha"]
    missing = [
        column
        for column in required
        if normalize_header(column) not in resolved
    ]
    if missing:
        return {
            "available": False,
            "target_date": target_date,
            "missing_columns": missing,
            "raw_id_count": 0,
            "legacy_id_count": 0,
            "matched_id_count": 0,
            "raw_only_count": 0,
            "legacy_only_count": 0,
            "match_rate": None,
            "raw_only_sample": [],
            "legacy_only_sample": [],
        }

    target = pd.Timestamp(target_date).normalize()
    raw_ids = {
        value
        for value in canonical_df.loc[
            canonical_df["fecha"] == target,
            "event_id",
        ].map(_clean_id)
        if value
    }
    legacy_dates = _parse_dates(_column_series(legacy_df, resolved, "Fecha"))
    legacy_ids = {
        value
        for value in _column_series(legacy_df, resolved, "ID")
        .loc[legacy_dates == target]
        .map(_clean_id)
        if value
    }
    matched = raw_ids & legacy_ids
    raw_only = sorted(raw_ids - legacy_ids)
    legacy_only = sorted(legacy_ids - raw_ids)
    match_rate = len(matched) / len(legacy_ids) if legacy_ids else None
    return {
        "available": True,
        "target_date": target_date,
        "missing_columns": [],
        "raw_id_count": len(raw_ids),
        "legacy_id_count": len(legacy_ids),
        "matched_id_count": len(matched),
        "raw_only_count": len(raw_only),
        "legacy_only_count": len(legacy_only),
        "match_rate": match_rate,
        "ready_threshold": PARITY_READY_THRESHOLD,
        "threshold_met": (
            match_rate is not None and match_rate >= PARITY_READY_THRESHOLD
        ),
        "raw_only_sample": raw_only[:20],
        "legacy_only_sample": legacy_only[:20],
    }


def determine_verdict(
    *,
    schema_blocked: bool,
    truncation_suspect: bool,
    conflict_blocked: bool,
    operational_coverage_complete: bool,
    parity_match_rate: float | None,
) -> str:
    if schema_blocked:
        return VERDICT_BLOCKED_SCHEMA
    if truncation_suspect or conflict_blocked:
        return VERDICT_BLOCKED_CONFLICT
    if (
        not operational_coverage_complete
        or parity_match_rate is None
        or parity_match_rate < PARITY_READY_THRESHOLD
    ):
        return VERDICT_PARTIAL
    return VERDICT_READY


def _profile_raw_file(path: Path) -> tuple[dict[str, Any], pd.DataFrame | None]:
    source_file_id = parse_source_file_id(path)
    source_sha256 = sha256_file(path)
    base_manifest: dict[str, Any] = {
        "source_file_id": source_file_id,
        "source_file_name": path.name,
        "source_path": path.as_posix(),
        "source_file_sha256": source_sha256,
        "sheet": RAW_SHEET,
        "read_error": None,
    }
    try:
        raw_df = read_excel_sheet(path, RAW_SHEET)
    except Exception as exc:
        return {
            **base_manifest,
            "row_count": 0,
            "distinct_id_count": 0,
            "fecha_min": None,
            "fecha_max": None,
            "columns_present": [],
            "expected_columns_missing": EXPECTED_RAW_COLUMNS,
            "critical_columns_missing": CRITICAL_RAW_COLUMNS,
            "blank_id_rows": 0,
            "invalid_date_rows": 0,
            "truncation_suspect": False,
            "read_error": f"{exc.__class__.__name__}:{exc}",
        }, None

    raw_df.columns = [str(column).strip() for column in raw_df.columns]
    resolved = _resolved_columns(raw_df)
    expected_missing = [
        column
        for column in EXPECTED_RAW_COLUMNS
        if normalize_header(column) not in resolved
    ]
    critical_missing = [
        column
        for column in CRITICAL_RAW_COLUMNS
        if normalize_header(column) not in resolved
    ]
    ids = _column_series(raw_df, resolved, "ID").map(_clean_id)
    dates = _parse_dates(_column_series(raw_df, resolved, "Fecha"))
    manifest = {
        **base_manifest,
        "row_count": int(len(raw_df)),
        "distinct_id_count": int(ids[ids != ""].nunique()),
        "fecha_min": _date_iso(dates.min()) if not dates.dropna().empty else None,
        "fecha_max": _date_iso(dates.max()) if not dates.dropna().empty else None,
        "columns_present": list(raw_df.columns),
        "expected_columns_missing": expected_missing,
        "critical_columns_missing": critical_missing,
        "blank_id_rows": int((ids == "").sum()),
        "invalid_date_rows": int(dates.isna().sum()),
        "truncation_suspect": int(len(raw_df)) >= TRUNCATION_ROW_THRESHOLD,
    }
    if critical_missing:
        return manifest, None
    canonical = normalize_raw_events(
        raw_df,
        source_file_id=source_file_id,
        source_file_sha256=source_sha256,
    )
    return manifest, canonical


def _duplicate_summary(canonical_df: pd.DataFrame) -> dict[str, Any]:
    valid = canonical_df[canonical_df["event_id"] != ""].copy()
    file_counts = valid.groupby("event_id")["source_file_id"].nunique()
    overlap_ids = set(file_counts[file_counts > 1].index)

    same_hash_ids: list[str] = []
    diff_hash_ids: list[str] = []
    for event_id in sorted(overlap_ids):
        event_rows = valid[valid["event_id"] == event_id]
        fingerprints: list[str] = []
        for _, file_rows in event_rows.groupby("source_file_id"):
            row_hashes = sorted(set(file_rows["_photo_row_hash"]))
            fingerprint_text = "\n".join(row_hashes)
            fingerprints.append(
                hashlib.sha256(fingerprint_text.encode("utf-8")).hexdigest()
            )
        if len(set(fingerprints)) == 1:
            same_hash_ids.append(event_id)
        else:
            diff_hash_ids.append(event_id)

    stable_hash_counts = valid.groupby("event_id")["event_stable_hash"].nunique()
    stable_conflict_ids = sorted(stable_hash_counts[stable_hash_counts > 1].index)
    cross_file_exact = (
        valid.groupby(["event_id", "_photo_row_hash"])["source_file_id"]
        .nunique()
        .gt(1)
    )
    return {
        "overlapping_id_count": len(overlap_ids),
        "same_id_same_hash_count": len(same_hash_ids),
        "same_id_diff_hash_count": len(diff_hash_ids),
        "dedupe_silent_candidate_ids_sample": same_hash_ids[:20],
        "conflict_blocker_ids_sample": diff_hash_ids[:20],
        "event_stable_hash_conflict_count": len(stable_conflict_ids),
        "event_stable_hash_conflict_ids_sample": stable_conflict_ids[:20],
        "cross_file_exact_photo_row_count": int(cross_file_exact.sum()),
    }


def _canonical_dedupe(canonical_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if canonical_df.empty:
        return canonical_df.copy(), 0
    ordered = canonical_df.sort_values(
        ["source_file_id", "source_row_number"],
        kind="mergesort",
    )
    duplicate_mask = ordered.duplicated(
        subset=["event_id", "_photo_row_hash"],
        keep="first",
    )
    return ordered.loc[~duplicate_mask].reset_index(drop=True), int(duplicate_mask.sum())


def _visit_formula_summary(
    rows: pd.DataFrame,
    groups: pd.DataFrame,
) -> dict[str, Any]:
    eligible = rows["visit_fraction"].notna()
    cod_groups = (
        int((groups["location_key_type"] == "COD_RT").sum())
        if not groups.empty
        else 0
    )
    local_groups = (
        int((groups["location_key_type"] == "LOCAL").sum())
        if not groups.empty
        else 0
    )
    sample = groups.head(20).copy()
    if "fecha" in sample.columns:
        sample["fecha"] = sample["fecha"].map(_date_iso)
    return {
        "formula": "1 / count(Codigo Local|Local fallback, Fecha, Marca)",
        "eligible_row_count": int(eligible.sum()),
        "excluded_row_count": int((~eligible).sum()),
        "group_count": int(len(groups)),
        "cod_rt_group_count": cod_groups,
        "local_fallback_group_count": local_groups,
        "visit_sum": float(rows.loc[eligible, "visit_fraction"].sum()),
        "all_group_sums_equal_one": bool(
            groups.empty or groups["visit_sum"].sub(1.0).abs().le(1e-12).all()
        ),
        "group_summary_sample": sample.to_dict(orient="records"),
    }


def _report_markdown(payload: dict[str, Any]) -> str:
    file_rows = []
    for item in payload["file_manifest"]:
        file_rows.append(
            "| {name} | {rows} | {ids} | {date_min} | {date_max} | {truncation} | {missing} |".format(
                name=item["source_file_name"],
                rows=item["row_count"],
                ids=item["distinct_id_count"],
                date_min=item["fecha_min"] or "",
                date_max=item["fecha_max"] or "",
                truncation="YES" if item["truncation_suspect"] else "NO",
                missing=", ".join(item["critical_columns_missing"]) or "none",
            )
        )

    coverage_rows = []
    for week in payload["june_coverage"]["operational_weeks"]:
        coverage_rows.append(
            f"| {week['label']} | {week['start']} | {week['end']} | "
            f"{week['status']} | {', '.join(week['missing_days']) or 'none'} |"
        )

    parity = payload["legacy_parity_2026_06_01"]
    match_rate = parity.get("match_rate")
    match_rate_text = f"{match_rate:.4f}" if match_rate is not None else "unavailable"
    truncation = payload["truncation_summary"]
    duplicate = payload["duplicate_summary"]
    coverage = payload["june_coverage"]
    recommendation = {
        VERDICT_READY: "Proceed only to a local dry-run loader design; productive apply remains forbidden.",
        VERDICT_PARTIAL: "Obtain the missing raw export windows and rerun this validator.",
        VERDICT_BLOCKED_CONFLICT: "Resolve truncation/conflict blockers and rerun 014C before any loader design.",
        VERDICT_BLOCKED_SCHEMA: "Restore the required export schema and rerun 014C.",
    }[payload["verdict"]]
    next_phase = (
        "014D_KPIONE_RAW_DRY_RUN_LOADER_NO_APPLY"
        if payload["verdict"] == VERDICT_READY
        else "014C_REMEDIATE_RAW_EXPORT_GAPS_OR_BLOCKERS_NO_APPLY"
    )
    blocker_lines = "\n".join(f"- {item}" for item in payload["blockers"]) or "- none"
    warning_lines = "\n".join(f"- {item}" for item in payload["warnings"]) or "- none"

    return f"""# 014C KPIONE Raw Export Validator — No Apply

## Resumen ejecutivo

- Verdict: `{payload["verdict"]}`
- Raw files inspected: {len(payload["file_manifest"])}
- Raw rows: {payload["canonical_summary"]["input_row_count"]}
- Canonical local rows after exact-row dedupe: {payload["canonical_summary"]["canonical_row_count"]}
- Distinct event IDs: {payload["canonical_summary"]["distinct_event_id_count"]}
- Covered June days: {coverage["covered_day_count"]}
- Missing June days: {coverage["missing_day_count"]}

## Guardrails

| Guardrail | State |
|---|---|
| DB / Supabase access | not used |
| SQL / DDL apply | not used |
| Productive loader / refresh | not used |
| UX modification | not used |
| Input file mutation or movement | not used |
| Output | lightweight JSON/Markdown only |

## Manifest por archivo

| File | Rows | Distinct IDs | Fecha min | Fecha max | truncation_suspect | Critical missing |
|---|---:|---:|---|---|---|---|
{chr(10).join(file_rows)}

## Truncation and duplicate gates

- Threshold: rows >= {TRUNCATION_ROW_THRESHOLD}
- Suspect files: {truncation["suspect_file_count"]}
- Suspect IDs: {", ".join(truncation["suspect_source_file_ids"]) or "none"}
- Overlapping IDs: {duplicate["overlapping_id_count"]}
- same_id_same_hash: {duplicate["same_id_same_hash_count"]}
- same_id_diff_hash conflicts: {duplicate["same_id_diff_hash_count"]}
- event_stable_hash conflicts: {duplicate["event_stable_hash_conflict_count"]}

## Cobertura junio

| Week | Start | End | Status | Missing days |
|---|---|---|---|---|
{chr(10).join(coverage_rows)}

- Covered days: {", ".join(coverage["covered_days"]) or "none"}
- Missing days: {", ".join(coverage["missing_days"]) or "none"}
- Missing 2026-06-25..2026-06-30: {", ".join(coverage["missing_2026_06_25_to_30"]) or "none"}
- Week 2026-06-29..2026-07-05 belongs to July: yes

## Paridad 2026-06-01 contra legacy

- raw_id_count: {parity["raw_id_count"]}
- legacy_id_count: {parity["legacy_id_count"]}
- matched_id_count: {parity["matched_id_count"]}
- raw_only_count: {parity["raw_only_count"]}
- legacy_only_count: {parity["legacy_only_count"]}
- match_rate: {match_rate_text}
- informative threshold >= {PARITY_READY_THRESHOLD:.2f}: {"met" if parity.get("threshold_met") else "not met"}
- raw-only sample (max 20): {", ".join(parity["raw_only_sample"]) or "none"}
- legacy-only sample (max 20): {", ".join(parity["legacy_only_sample"]) or "none"}

## Replica local de VISITA

- Formula: `1 / count(Codigo Local, Fecha, Marca)`, with `Local` fallback when Codigo Local is blank.
- Eligible rows: {payload["visit_formula_summary"]["eligible_row_count"]}
- Groups / VISITA sum: {payload["visit_formula_summary"]["group_count"]} / {payload["visit_formula_summary"]["visit_sum"]:.6f}
- Local fallback groups: {payload["visit_formula_summary"]["local_fallback_group_count"]}
- Every group sums to 1: {str(payload["visit_formula_summary"]["all_group_sums_equal_one"]).lower()}

## Blockers

{blocker_lines}

## Warnings

{warning_lines}

## Decision recomendada

{recommendation}

## Siguiente fase propuesta

`{next_phase}`

## Declaracion explicita

This validation used no Supabase, no DB, no SQL/DDL apply, no productive loader,
no refresh, no UX modification, no backfill, no cutover and no data movement.
The raw exports and legacy master were read-only inputs.
"""


def build_validation_payload(base: Path, month: str) -> dict[str, Any]:
    raw_paths = sorted(base.glob(RAW_PATTERN))
    legacy_path = base / LEGACY_RELATIVE_PATH
    file_manifest: list[dict[str, Any]] = []
    canonical_frames: list[pd.DataFrame] = []
    schema_blockers: list[str] = []
    warnings: list[str] = []

    if not raw_paths:
        schema_blockers.append(f"no_raw_files:{RAW_PATTERN}")

    for raw_path in raw_paths:
        try:
            profile, canonical = _profile_raw_file(raw_path)
        except Exception as exc:
            schema_blockers.append(
                f"raw_file_profile_failed:{raw_path.name}:{exc.__class__.__name__}:{exc}"
            )
            continue
        file_manifest.append(profile)
        if profile["read_error"]:
            schema_blockers.append(
                f"raw_file_read_error:{profile['source_file_name']}:{profile['read_error']}"
            )
        if profile["critical_columns_missing"]:
            schema_blockers.append(
                "raw_critical_columns_missing:"
                f"{profile['source_file_name']}:"
                f"{','.join(profile['critical_columns_missing'])}"
            )
        if profile["expected_columns_missing"]:
            warnings.append(
                "raw_expected_columns_missing:"
                f"{profile['source_file_name']}:"
                f"{','.join(profile['expected_columns_missing'])}"
            )
        if profile["blank_id_rows"]:
            warnings.append(
                f"raw_blank_id_rows:{profile['source_file_name']}:{profile['blank_id_rows']}"
            )
        if profile["invalid_date_rows"]:
            warnings.append(
                f"raw_invalid_date_rows:{profile['source_file_name']}:{profile['invalid_date_rows']}"
            )
        if canonical is not None:
            canonical_frames.append(canonical)

    canonical_input = (
        pd.concat(canonical_frames, ignore_index=True)
        if canonical_frames
        else pd.DataFrame(columns=CANONICAL_COLUMNS + ["_photo_row_hash"])
    )
    ids_by_source = {
        source_id: {
            value
            for value in rows["event_id"]
            if value
        }
        for source_id, rows in canonical_input.groupby("source_file_id")
    }
    overlap_matrix = compute_overlap_matrix(ids_by_source)
    duplicate_summary = _duplicate_summary(canonical_input)
    canonical, duplicate_rows_removed = _canonical_dedupe(canonical_input)
    visit_rows, visit_groups = compute_visit_fraction(canonical)
    visit_summary = _visit_formula_summary(visit_rows, visit_groups)
    coverage = compute_june_coverage(canonical, month)

    legacy_schema_missing: list[str] = []
    if not legacy_path.exists():
        legacy_schema_missing.append(str(LEGACY_RELATIVE_PATH))
        legacy_df = pd.DataFrame()
    else:
        try:
            legacy_df = read_excel_sheet(legacy_path, LEGACY_SHEET)
        except Exception as exc:
            legacy_schema_missing.append(f"{LEGACY_SHEET}:{exc.__class__.__name__}:{exc}")
            legacy_df = pd.DataFrame()
    if legacy_schema_missing:
        schema_blockers.extend(
            f"legacy_input_missing_or_unreadable:{item}" for item in legacy_schema_missing
        )
        legacy_parity = {
            "available": False,
            "target_date": PARITY_DATE,
            "missing_columns": legacy_schema_missing,
            "raw_id_count": 0,
            "legacy_id_count": 0,
            "matched_id_count": 0,
            "raw_only_count": 0,
            "legacy_only_count": 0,
            "match_rate": None,
            "raw_only_sample": [],
            "legacy_only_sample": [],
        }
    else:
        legacy_parity = compute_legacy_parity(canonical, legacy_df)
        if not legacy_parity["available"]:
            schema_blockers.append(
                "legacy_critical_columns_missing:"
                + ",".join(legacy_parity["missing_columns"])
            )

    suspect_files = [
        item for item in file_manifest if item["truncation_suspect"]
    ]
    truncation_summary = {
        "threshold_rows": TRUNCATION_ROW_THRESHOLD,
        "suspect_file_count": len(suspect_files),
        "suspect_source_file_ids": [
            item["source_file_id"] for item in suspect_files
        ],
        "suspect_source_file_names": [
            item["source_file_name"] for item in suspect_files
        ],
    }
    conflict_blocked = bool(
        duplicate_summary["same_id_diff_hash_count"]
        or duplicate_summary["event_stable_hash_conflict_count"]
    )
    verdict = determine_verdict(
        schema_blocked=bool(schema_blockers),
        truncation_suspect=bool(suspect_files),
        conflict_blocked=conflict_blocked,
        operational_coverage_complete=coverage["operational_coverage_complete"],
        parity_match_rate=legacy_parity.get("match_rate"),
    )

    blockers = list(schema_blockers)
    if suspect_files:
        blockers.append(
            "truncation_suspect:"
            + ",".join(item["source_file_name"] for item in suspect_files)
        )
    if duplicate_summary["same_id_diff_hash_count"]:
        blockers.append(
            "same_id_diff_hash_conflicts:"
            + str(duplicate_summary["same_id_diff_hash_count"])
        )
    if duplicate_summary["event_stable_hash_conflict_count"]:
        blockers.append(
            "event_stable_hash_conflicts:"
            + str(duplicate_summary["event_stable_hash_conflict_count"])
        )
    if coverage["missing_days"]:
        warnings.append(
            "june_missing_days:" + ",".join(coverage["missing_days"])
        )
    if not coverage["operational_coverage_complete"]:
        warnings.append("june_operational_weeks_incomplete")
    parity_rate = legacy_parity.get("match_rate")
    if parity_rate is None:
        warnings.append("legacy_parity_unavailable")
    elif parity_rate < PARITY_READY_THRESHOLD:
        warnings.append(
            f"legacy_parity_below_{PARITY_READY_THRESHOLD:.2f}:{parity_rate:.6f}"
        )
    if duplicate_summary["same_id_same_hash_count"]:
        warnings.append(
            "dedupe_silent_candidate_ids:"
            + str(duplicate_summary["same_id_same_hash_count"])
        )

    valid_event_ids = canonical.loc[canonical["event_id"] != "", "event_id"]
    event_stable_conflicts = duplicate_summary["event_stable_hash_conflict_count"]
    canonical_summary = {
        "canonical_columns": CANONICAL_COLUMNS,
        "input_row_count": int(len(canonical_input)),
        "canonical_row_count": int(len(canonical)),
        "exact_duplicate_rows_removed": duplicate_rows_removed,
        "distinct_event_id_count": int(valid_event_ids.nunique()),
        "fecha_min": _date_iso(canonical["fecha"].min())
        if not canonical["fecha"].dropna().empty
        else None,
        "fecha_max": _date_iso(canonical["fecha"].max())
        if not canonical["fecha"].dropna().empty
        else None,
        "blank_event_id_rows": int((canonical["event_id"] == "").sum()),
        "invalid_fecha_rows": int(canonical["fecha"].isna().sum()),
        "event_stable_hash_conflict_count": event_stable_conflicts,
        "canonical_rows_persisted": False,
    }
    return {
        "phase_id": PHASE_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "guardrails": {
            "mode": "LOCAL_READ_ONLY_INPUTS_NO_APPLY",
            "db_access": {"used": False},
            "supabase_used": False,
            "sql_apply": False,
            "ddl": False,
            "productive_loader_run": False,
            "productive_refresh_run": False,
            "ux_modified": False,
            "data_movement": False,
            "raw_inputs_modified": False,
            "legacy_master_modified": False,
            "canonical_rows_persisted": False,
            "lightweight_research_outputs_written": True,
        },
        "input_files": {
            "base": str(base),
            "month": month,
            "raw_pattern": RAW_PATTERN,
            "raw_sheet": RAW_SHEET,
            "raw_files": [path.relative_to(base).as_posix() for path in raw_paths],
            "legacy_file": LEGACY_RELATIVE_PATH.as_posix(),
            "legacy_sheet": LEGACY_SHEET,
        },
        "file_manifest": file_manifest,
        "overlap_matrix": overlap_matrix,
        "duplicate_summary": duplicate_summary,
        "truncation_summary": truncation_summary,
        "canonical_summary": canonical_summary,
        "visit_formula_summary": visit_summary,
        "june_coverage": coverage,
        "legacy_parity_2026_06_01": legacy_parity,
        "verdict": verdict,
        "blockers": blockers,
        "warnings": sorted(set(warnings)),
    }


def write_outputs(base: Path, payload: dict[str, Any]) -> tuple[Path, Path]:
    output_dir = base / OUTPUT_RELATIVE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / MANIFEST_FILENAME
    report_path = output_dir / REPORT_FILENAME
    manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
        newline="\n",
    )
    report_path.write_text(
        _report_markdown(payload),
        encoding="utf-8",
        newline="\n",
    )
    return manifest_path, report_path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="014C local/no-apply KPIONE raw export validator."
    )
    parser.add_argument("--base", default=".")
    parser.add_argument("--month", default="2026-06")
    parser.add_argument(
        "--soft-exit",
        action="store_true",
        help="Return exit 0 after emitting a blocked/partial evidence verdict.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    base = Path(args.base)
    payload = build_validation_payload(base, args.month)
    manifest_path, report_path = write_outputs(base, payload)
    print(
        json.dumps(
            {
                "phase_id": PHASE_ID,
                "verdict": payload["verdict"],
                "blockers": payload["blockers"],
                "warnings": payload["warnings"],
                "manifest_path": str(manifest_path),
                "report_path": str(report_path),
                "db_access": {"used": False},
                "sql_apply": False,
                "data_movement": False,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    if args.soft_exit:
        return 0
    return 0 if payload["verdict"] == VERDICT_READY else 1


if __name__ == "__main__":
    raise SystemExit(main())
