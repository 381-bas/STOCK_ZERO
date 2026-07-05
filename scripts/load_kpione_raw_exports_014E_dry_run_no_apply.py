# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

import validate_kpione_raw_exports_014C_no_apply as baseline


PHASE_ID = "014E_KPIONE_RAW_DRY_RUN_LOADER_NO_APPLY"
EXPECTED_INPUT_PHASE = "014D_KPIONE_RAW_EXPORT_REMEDIATION_NO_APPLY"
ALLOWED_INPUT_VERDICTS = {
    "REMEDIATION_READY_FOR_DRY_RUN_LOADER",
    "REMEDIATION_READY_FOR_DRY_RUN_LOADER_WITH_WARNINGS",
}
OUTPUT_RELATIVE_DIR = Path("research/014E_KPIONE_RAW_DRY_RUN_LOADER_NO_APPLY")
MANIFEST_FILENAME = "014E_dry_run_loader_manifest.json"
REPORT_FILENAME = "014E_dry_run_loader_report.md"

VERDICT_READY = "DRY_RUN_READY_FOR_DB_STAGING_DESIGN"
VERDICT_READY_WARN = "DRY_RUN_READY_WITH_WARNINGS"
VERDICT_MANIFEST = "DRY_RUN_BLOCKED_BY_MANIFEST_MISMATCH"
VERDICT_CONFLICT = "DRY_RUN_BLOCKED_BY_CANDIDATE_CONFLICT"
VERDICT_SCHEMA = "DRY_RUN_BLOCKED_BY_SCHEMA"
VERDICT_COVERAGE = "DRY_RUN_BLOCKED_BY_COVERAGE_GAP"

DRY_RUN_PAYLOAD_COLUMNS = [
    "event_id",
    "source_file_id",
    "source_file_name",
    "source_file_sha256",
    "source_row_number",
    "fecha",
    "week_start",
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


def parse_photo_total(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip()
    slash_total = text.str.extract(r"/\s*(\d+)\s*$")[0]
    direct_total = text.str.extract(r"^\s*(\d+)\s*$")[0]
    return pd.to_numeric(
        slash_total.fillna(direct_total),
        errors="coerce",
    ).astype("Int64")


def select_manifest_files(
    manifest: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    file_manifest = manifest.get("file_manifest")
    if not isinstance(file_manifest, list):
        return [], [], ["file_manifest_missing_or_invalid"]

    candidate_ids = manifest.get("candidate_set", {}).get("source_file_ids")
    role_ids = manifest.get("file_roles", {}).get("include_candidate")
    if not isinstance(candidate_ids, list):
        errors.append("candidate_set_source_file_ids_missing")
        candidate_ids = []
    if not isinstance(role_ids, list):
        errors.append("file_roles_include_candidate_missing")
        role_ids = []
    if set(candidate_ids) != set(role_ids):
        errors.append("candidate_set_and_file_roles_disagree")

    by_id = {
        str(item.get("source_file_id")): item
        for item in file_manifest
        if isinstance(item, dict) and item.get("source_file_id") is not None
    }
    candidates: list[dict[str, Any]] = []
    for source_file_id in candidate_ids:
        entry = by_id.get(str(source_file_id))
        if entry is None:
            errors.append(f"candidate_entry_missing:{source_file_id}")
            continue
        if entry.get("role") != "include_candidate":
            errors.append(
                f"candidate_role_mismatch:{source_file_id}:{entry.get('role')}"
            )
            continue
        candidates.append(entry)

    excluded = [
        {
            "source_file_id": str(item.get("source_file_id") or ""),
            "source_file_name": str(item.get("source_file_name") or ""),
            "role": str(item.get("role") or ""),
            "reason": f"manifest_role:{item.get('role')}",
        }
        for item in file_manifest
        if isinstance(item, dict) and item.get("role") != "include_candidate"
    ]
    return candidates, excluded, errors


def build_dry_run_batch_id(
    month: str,
    input_manifest_sha256: str,
    candidate_entries: list[dict[str, Any]],
) -> str:
    candidate_tokens = sorted(
        f"{entry.get('source_file_id')}:{entry.get('source_file_sha256')}"
        for entry in candidate_entries
    )
    material = "|".join(
        [PHASE_ID, month, input_manifest_sha256, *candidate_tokens]
    )
    return "014E_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def _safe_candidate_path(
    base: Path,
    entry: dict[str, Any],
) -> tuple[Path | None, str | None]:
    raw_path = entry.get("source_path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        return None, "source_path_missing"
    candidate = (base / raw_path).resolve()
    data_root = (base / "data").resolve()
    try:
        candidate.relative_to(data_root)
    except ValueError:
        return None, "source_path_outside_data"
    expected_name = str(entry.get("source_file_name") or "")
    if candidate.name != expected_name:
        return None, "source_file_name_path_mismatch"
    return candidate, None


def _load_candidate_file(
    base: Path,
    entry: dict[str, Any],
    *,
    dry_run_batch_id: str,
) -> tuple[dict[str, Any], pd.DataFrame | None]:
    source_file_id = str(entry.get("source_file_id") or "")
    source_file_name = str(entry.get("source_file_name") or "")
    expected_sha256 = str(entry.get("source_file_sha256") or "")
    path, path_error = _safe_candidate_path(base, entry)
    integrity = {
        "source_file_id": source_file_id,
        "source_file_name": source_file_name,
        "manifest_role": entry.get("role"),
        "expected_sha256": expected_sha256,
        "actual_sha256": None,
        "sha256_match": False,
        "exists": bool(path and path.exists()),
        "path_error": path_error,
        "row_count": 0,
        "manifest_row_count": entry.get("row_count"),
        "row_count_match": False,
        "truncation_suspect": bool(entry.get("truncation_suspect")),
        "critical_columns_missing": [],
        "read_error": None,
    }
    if path is None or not path.exists():
        integrity["read_error"] = path_error or "file_not_found"
        return integrity, None

    actual_sha256 = baseline.sha256_file(path)
    integrity["actual_sha256"] = actual_sha256
    integrity["sha256_match"] = actual_sha256 == expected_sha256
    try:
        raw_df = baseline.read_excel_sheet(path, baseline.RAW_SHEET)
    except Exception as exc:
        integrity["read_error"] = f"{exc.__class__.__name__}:{exc}"
        return integrity, None

    raw_df.columns = [str(column).strip() for column in raw_df.columns]
    resolved = baseline._resolved_columns(raw_df)
    critical_missing = [
        column
        for column in baseline.CRITICAL_RAW_COLUMNS
        if baseline.normalize_header(column) not in resolved
    ]
    integrity["critical_columns_missing"] = critical_missing
    integrity["row_count"] = int(len(raw_df))
    integrity["row_count_match"] = int(len(raw_df)) == int(
        entry.get("row_count") or -1
    )
    integrity["truncation_suspect"] = bool(
        entry.get("truncation_suspect")
        or len(raw_df) >= baseline.TRUNCATION_ROW_THRESHOLD
    )
    if critical_missing:
        return integrity, None

    canonical = baseline.normalize_raw_events(
        raw_df,
        source_file_id=source_file_id,
        source_file_sha256=actual_sha256,
    )
    photo_count_column = resolved.get(
        baseline.normalize_header("Foto Nº/Total")
    )
    if photo_count_column is None:
        n_fotos = pd.Series(
            pd.array([pd.NA] * len(raw_df), dtype="Int64"),
            index=raw_df.index,
        )
    else:
        n_fotos = parse_photo_total(raw_df[photo_count_column])

    payload = canonical.copy()
    payload["source_file_name"] = source_file_name
    payload["n_fotos"] = n_fotos.reset_index(drop=True)
    payload["photo_row_hash"] = payload["_photo_row_hash"]
    payload["dry_run_batch_id"] = dry_run_batch_id
    return integrity, payload


def event_preservation_summary(
    before_dedupe: pd.DataFrame,
    after_dedupe: pd.DataFrame,
) -> dict[str, Any]:
    def key_pairs(frame: pd.DataFrame) -> set[tuple[str, str, str, str]]:
        cod_key = frame["cod_rt"].map(baseline._normalize_key)
        local_key = frame["local_nombre"].map(baseline._normalize_key)
        location_key = cod_key.where(cod_key != "", local_key)
        fecha = frame["fecha"].map(baseline._date_iso).fillna("")
        cliente = frame["cliente_norm"].map(baseline._normalize_key)
        return set(
            zip(
                location_key,
                fecha,
                cliente,
                frame["event_id"].map(baseline._clean_id),
            )
        )

    def multi_event_groups(frame: pd.DataFrame) -> int:
        work = pd.DataFrame(
            {
                "location_key": frame["cod_rt"]
                .map(baseline._normalize_key)
                .where(
                    frame["cod_rt"].map(baseline._normalize_key) != "",
                    frame["local_nombre"].map(baseline._normalize_key),
                ),
                "fecha": frame["fecha"],
                "cliente_norm": frame["cliente_norm"].map(
                    baseline._normalize_key
                ),
                "event_id": frame["event_id"].map(baseline._clean_id),
            }
        )
        counts = work.groupby(
            ["location_key", "fecha", "cliente_norm"],
            dropna=False,
        )["event_id"].nunique()
        return int((counts > 1).sum())

    before_pairs = key_pairs(before_dedupe)
    after_pairs = key_pairs(after_dedupe)
    return {
        "dedupe_key": ["event_id", "photo_row_hash"],
        "same_local_date_brand_different_id_is_duplicate": False,
        "multi_event_daily_groups_before": multi_event_groups(before_dedupe),
        "multi_event_daily_groups_after": multi_event_groups(after_dedupe),
        "different_event_ids_preserved": before_pairs == after_pairs,
        "lost_event_key_pairs_count": len(before_pairs - after_pairs),
    }


def _summary_records(
    frame: pd.DataFrame,
    group_column: str,
) -> list[dict[str, Any]]:
    valid = frame[frame[group_column].notna()].copy()
    if valid.empty:
        return []
    summary = (
        valid.groupby(group_column, dropna=False)
        .agg(
            would_stage_rows=("event_id", "size"),
            distinct_event_ids=("event_id", "nunique"),
            distinct_photo_hashes=("photo_row_hash", "nunique"),
            date_min=("fecha", "min"),
            date_max=("fecha", "max"),
            derived_visit_sum=("visit_fraction", "sum"),
        )
        .reset_index()
    )
    records: list[dict[str, Any]] = []
    for row in summary.to_dict(orient="records"):
        group_value = row[group_column]
        if isinstance(group_value, pd.Timestamp):
            group_value = baseline._date_iso(group_value)
        records.append(
            {
                group_column: str(group_value),
                "would_stage_rows": int(row["would_stage_rows"]),
                "distinct_event_ids": int(row["distinct_event_ids"]),
                "distinct_photo_hashes": int(row["distinct_photo_hashes"]),
                "date_min": baseline._date_iso(row["date_min"]),
                "date_max": baseline._date_iso(row["date_max"]),
                "derived_visit_sum": float(row["derived_visit_sum"]),
            }
        )
    return records


def determine_dry_run_verdict(
    *,
    manifest_mismatch: bool,
    schema_blocked: bool,
    candidate_conflict: bool,
    coverage_gap: bool,
    warnings_present: bool,
) -> str:
    if manifest_mismatch:
        return VERDICT_MANIFEST
    if schema_blocked:
        return VERDICT_SCHEMA
    if candidate_conflict:
        return VERDICT_CONFLICT
    if coverage_gap:
        return VERDICT_COVERAGE
    return VERDICT_READY_WARN if warnings_present else VERDICT_READY


def _report_markdown(payload: dict[str, Any]) -> str:
    candidate_rows = [
        f"| {item['source_file_id']} | {item['source_file_name']} | "
        f"{item['row_count']} | {str(item['sha256_match']).lower()} |"
        for item in payload["candidate_files_used"]
    ]
    excluded_rows = [
        f"| {item['source_file_id']} | {item['source_file_name']} | "
        f"{item['role']} | {item['reason']} |"
        for item in payload["excluded_files"]
    ]
    week_rows = [
        f"| {item['week_start']} | {item['would_stage_rows']} | "
        f"{item['distinct_event_ids']} | {item['distinct_photo_hashes']} | "
        f"{item['derived_visit_sum']:.6f} |"
        for item in payload["would_insert_summary_by_week"]
    ]
    day_rows = [
        f"| {item['fecha']} | {item['would_stage_rows']} | "
        f"{item['distinct_event_ids']} | {item['derived_visit_sum']:.6f} |"
        for item in payload["would_insert_summary_by_day"]
    ]
    client_rows = [
        f"| {item['cliente_norm']} | {item['would_stage_rows']} | "
        f"{item['distinct_event_ids']} | {item['derived_visit_sum']:.6f} |"
        for item in payload["would_insert_summary_by_cliente"][:20]
    ]
    summary = payload["dry_run_payload_summary"]
    integrity = payload["manifest_integrity_check"]
    coverage = payload["coverage_summary"]
    visit = payload["visit_validation_summary"]
    blocker_lines = "\n".join(f"- {item}" for item in payload["blockers"]) or "- none"
    warning_lines = "\n".join(f"- {item}" for item in payload["warnings"]) or "- none"
    recommendation = {
        VERDICT_READY: "Proceed to DB staging design only; DB execution remains unauthorized.",
        VERDICT_READY_WARN: "Proceed to DB staging design with exact-overlap warnings documented.",
        VERDICT_MANIFEST: "Repair or regenerate the 014D manifest integrity evidence before staging design.",
        VERDICT_CONFLICT: "Resolve candidate conflicts or VISITA validation failures before staging design.",
        VERDICT_SCHEMA: "Restore the declared candidate schema before staging design.",
        VERDICT_COVERAGE: "Complete the candidate date coverage before staging design.",
    }[payload["verdict"]]
    next_phase = (
        "014F_KPIONE_RAW_DB_STAGING_DESIGN_NO_APPLY"
        if payload["verdict"] in {VERDICT_READY, VERDICT_READY_WARN}
        else "014E_REMEDIATE_DRY_RUN_BLOCKERS_NO_APPLY"
    )
    return f"""# 014E KPIONE Raw Dry-Run Loader — No Apply

## Resumen ejecutivo

- Verdict: `{payload["verdict"]}`
- Dry-run batch: `{payload["dry_run_batch_id"]}`
- Candidate files used: {summary["candidate_files_count"]}
- Source rows: {summary["source_rows_total"]}
- Would stage rows: {summary["would_stage_rows"]}
- Distinct event IDs: {summary["distinct_event_ids"]}
- Coverage: {coverage["covered_day_count"]}/30 days

## Input manifest 014D

- Path: `{payload["input_manifest_014D"]["path"]}`
- SHA256: `{payload["input_manifest_014D"]["sha256"]}`
- Phase: `{payload["input_manifest_014D"]["phase_id"]}`
- Verdict: `{payload["input_manifest_014D"]["verdict"]}`

## Candidate set usado

| Source ID | File | Rows | SHA match |
|---|---|---:|---|
{chr(10).join(candidate_rows)}

## Archivos excluidos

| Source ID | File | Role | Reason |
|---|---|---|---|
{chr(10).join(excluded_rows)}

## Integridad SHA256 / manifest

- Candidate entries consistent: {str(integrity["candidate_role_consistency"]).lower()}
- All files exist: {str(integrity["all_files_exist"]).lower()}
- All SHA256 match: {str(integrity["all_sha256_match"]).lower()}
- All row counts match: {str(integrity["all_row_counts_match"]).lower()}
- Candidate truncation count: {integrity["candidate_truncation_count"]}

## Payload dry-run

- Grain: raw/event-photo
- Payload columns: {", ".join(summary["payload_columns"])}
- Source rows total: {summary["source_rows_total"]}
- Exact duplicates removed: {summary["exact_duplicate_rows_removed"]}
- Would stage rows: {summary["would_stage_rows"]}
- Date range: {summary["date_min"]}..{summary["date_max"]}
- Payload rows persisted: false
- Official compliance calculated: false

## Dedupe

- Candidate same_id_same_hash: {payload["dedupe_summary"]["same_id_same_hash_count"]}
- Candidate same_id_diff_hash: {payload["dedupe_summary"]["same_id_diff_hash_count"]}
- event_stable_hash conflicts: {payload["dedupe_summary"]["event_stable_hash_conflict_count"]}
- Same local/date/brand with different ID preserved: {str(payload["dedupe_summary"]["event_preservation"]["different_event_ids_preserved"]).lower()}

## Cobertura

- Covered days: {", ".join(coverage["covered_days"])}
- Missing days: {", ".join(coverage["missing_days"]) or "none"}
- S1-S4 complete: {str(coverage["operational_coverage_complete"]).lower()}
- Calendar June complete: {str(coverage["calendar_month_complete"]).lower()}

## VISITA validation

- Derived validation only; not an official/productive load field.
- group_count: {visit["group_count"]}
- visit_sum: {visit["visit_sum"]:.6f}
- groups where sum != 1: {visit["groups_sum_not_one_count"]}
- local fallback groups: {visit["local_fallback_group_count"]}

## Would-insert summary by week

| Week | Rows | Event IDs | Photo hashes | Derived VISITA |
|---|---:|---:|---:|---:|
{chr(10).join(week_rows)}

## Would-insert summary by day

| Day | Rows | Event IDs | Derived VISITA |
|---|---:|---:|---:|
{chr(10).join(day_rows)}

## Would-insert summary by cliente (first 20)

| Cliente | Rows | Event IDs | Derived VISITA |
|---|---:|---:|---:|
{chr(10).join(client_rows)}

## Blockers

{blocker_lines}

## Warnings

{warning_lines}

## Decision recomendada

{recommendation}

## Siguiente fase propuesta

`{next_phase}`

## Declaracion no-apply

No Supabase, no DB connection, no SQL/DDL apply, no productive loader, no
refresh, no UX modification and no data movement were used. The dry-run payload
was held in memory and only lightweight aggregate JSON/Markdown was written.
"""


def build_dry_run_payload(
    base: Path,
    month: str,
    input_manifest_path: Path,
) -> dict[str, Any]:
    manifest_full_path = (
        input_manifest_path
        if input_manifest_path.is_absolute()
        else base / input_manifest_path
    )
    manifest_errors: list[str] = []
    warnings: list[str] = []
    rejected_files: list[dict[str, Any]] = []
    if not manifest_full_path.exists():
        raise FileNotFoundError(f"input_manifest_not_found:{manifest_full_path}")

    input_manifest_sha256 = baseline.sha256_file(manifest_full_path)
    input_manifest = json.loads(
        manifest_full_path.read_text(encoding="utf-8")
    )
    if input_manifest.get("phase_id") != EXPECTED_INPUT_PHASE:
        manifest_errors.append(
            f"input_phase_mismatch:{input_manifest.get('phase_id')}"
        )
    if input_manifest.get("verdict") not in ALLOWED_INPUT_VERDICTS:
        manifest_errors.append(
            f"input_verdict_not_ready:{input_manifest.get('verdict')}"
        )

    candidate_entries, excluded_files, selection_errors = select_manifest_files(
        input_manifest
    )
    manifest_errors.extend(selection_errors)
    dry_run_batch_id = build_dry_run_batch_id(
        month,
        input_manifest_sha256,
        candidate_entries,
    )

    integrity_rows: list[dict[str, Any]] = []
    candidate_frames: list[pd.DataFrame] = []
    schema_blockers: list[str] = []
    for entry in candidate_entries:
        integrity, payload = _load_candidate_file(
            base,
            entry,
            dry_run_batch_id=dry_run_batch_id,
        )
        integrity_rows.append(integrity)
        if not integrity["exists"] or integrity["path_error"] or integrity["read_error"]:
            manifest_errors.append(
                f"candidate_file_unreadable:{integrity['source_file_id']}:"
                f"{integrity['path_error'] or integrity['read_error']}"
            )
            rejected_files.append(
                {
                    "source_file_id": integrity["source_file_id"],
                    "source_file_name": integrity["source_file_name"],
                    "reason": integrity["path_error"] or integrity["read_error"],
                }
            )
            continue
        if not integrity["sha256_match"]:
            manifest_errors.append(
                f"sha256_mismatch:{integrity['source_file_id']}"
            )
            rejected_files.append(
                {
                    "source_file_id": integrity["source_file_id"],
                    "source_file_name": integrity["source_file_name"],
                    "reason": "sha256_mismatch",
                }
            )
            continue
        if not integrity["row_count_match"]:
            manifest_errors.append(
                f"row_count_mismatch:{integrity['source_file_id']}"
            )
        if integrity["truncation_suspect"]:
            manifest_errors.append(
                f"candidate_truncation_suspect:{integrity['source_file_id']}"
            )
        if integrity["critical_columns_missing"]:
            schema_blockers.append(
                f"critical_columns_missing:{integrity['source_file_id']}:"
                f"{','.join(integrity['critical_columns_missing'])}"
            )
            rejected_files.append(
                {
                    "source_file_id": integrity["source_file_id"],
                    "source_file_name": integrity["source_file_name"],
                    "reason": "critical_columns_missing",
                }
            )
            continue
        if payload is not None:
            candidate_frames.append(payload)

    raw_candidate = (
        pd.concat(candidate_frames, ignore_index=True)
        if candidate_frames
        else pd.DataFrame(
            columns=baseline.CANONICAL_COLUMNS
            + ["_photo_row_hash", "source_file_name", "n_fotos", "photo_row_hash", "dry_run_batch_id"]
        )
    )
    candidate_duplicate = baseline._duplicate_summary(raw_candidate)
    deduped, exact_duplicate_rows_removed = baseline._canonical_dedupe(
        raw_candidate
    )
    preservation = event_preservation_summary(raw_candidate, deduped)
    visit_rows, visit_groups = baseline.compute_visit_fraction(deduped)
    groups_not_one = (
        int(visit_groups["visit_sum"].sub(1.0).abs().gt(1e-12).sum())
        if not visit_groups.empty
        else 0
    )
    coverage = baseline.compute_june_coverage(deduped, month)
    parity = input_manifest.get("legacy_parity_2026_06_01") or {}
    parity_rate = parity.get("match_rate")

    candidate_conflict = bool(
        candidate_duplicate["same_id_diff_hash_count"]
        or candidate_duplicate["event_stable_hash_conflict_count"]
        or groups_not_one
        or not preservation["different_event_ids_preserved"]
        or parity_rate is None
        or parity_rate < baseline.PARITY_READY_THRESHOLD
    )
    coverage_gap = bool(
        coverage["missing_days"]
        or not coverage["calendar_month_complete"]
        or not coverage["operational_coverage_complete"]
    )
    if exact_duplicate_rows_removed:
        warnings.append(
            f"expected_exact_overlap_rows_removed:{exact_duplicate_rows_removed}"
        )
    if candidate_duplicate["same_id_same_hash_count"]:
        warnings.append(
            "expected_same_id_same_hash:"
            + str(candidate_duplicate["same_id_same_hash_count"])
        )
    if excluded_files:
        warnings.append(
            "manifest_non_candidate_files_excluded:"
            + str(len(excluded_files))
        )
    if input_manifest.get("warnings"):
        warnings.append("input_manifest_014D_has_documented_warnings")

    blockers = list(manifest_errors) + list(schema_blockers)
    if candidate_duplicate["same_id_diff_hash_count"]:
        blockers.append(
            "candidate_same_id_diff_hash:"
            + str(candidate_duplicate["same_id_diff_hash_count"])
        )
    if candidate_duplicate["event_stable_hash_conflict_count"]:
        blockers.append(
            "candidate_event_stable_hash_conflicts:"
            + str(candidate_duplicate["event_stable_hash_conflict_count"])
        )
    if groups_not_one:
        blockers.append(f"derived_visit_groups_not_one:{groups_not_one}")
    if not preservation["different_event_ids_preserved"]:
        blockers.append("different_event_ids_not_preserved")
    if parity_rate is None or parity_rate < baseline.PARITY_READY_THRESHOLD:
        blockers.append(
            "legacy_parity_below_threshold:"
            + ("unavailable" if parity_rate is None else f"{parity_rate:.6f}")
        )
    if coverage["missing_days"]:
        blockers.append(
            "coverage_missing_days:" + ",".join(coverage["missing_days"])
        )

    verdict = determine_dry_run_verdict(
        manifest_mismatch=bool(manifest_errors),
        schema_blocked=bool(schema_blockers),
        candidate_conflict=candidate_conflict,
        coverage_gap=coverage_gap,
        warnings_present=bool(warnings),
    )

    valid_event_ids = deduped.loc[deduped["event_id"] != "", "event_id"]
    dry_run_summary = {
        "layer": "raw_candidate_photo_rows",
        "payload_columns": DRY_RUN_PAYLOAD_COLUMNS,
        "candidate_files_count": len(candidate_frames),
        "source_rows_total": int(len(raw_candidate)),
        "exact_duplicate_rows_removed": exact_duplicate_rows_removed,
        "would_stage_rows": int(len(deduped)),
        "distinct_event_ids": int(valid_event_ids.nunique()),
        "distinct_photo_hashes": int(deduped["photo_row_hash"].nunique()),
        "date_min": baseline._date_iso(deduped["fecha"].min())
        if not deduped["fecha"].dropna().empty
        else None,
        "date_max": baseline._date_iso(deduped["fecha"].max())
        if not deduped["fecha"].dropna().empty
        else None,
        "covered_days": coverage["covered_days"],
        "missing_days": coverage["missing_days"],
        "rejected_files": rejected_files,
        "excluded_files": excluded_files,
        "payload_rows_persisted": False,
    }
    dedupe_summary = {
        **candidate_duplicate,
        "exact_duplicate_rows_removed": exact_duplicate_rows_removed,
        "event_preservation": preservation,
    }
    visit_summary = {
        "layer": "derived_validation_only",
        "official_compliance_calculated": False,
        "formula": "1 / count(Codigo Local|Local fallback, Fecha, Marca)",
        "eligible_row_count": int(visit_rows["visit_fraction"].notna().sum()),
        "excluded_row_count": int(visit_rows["visit_fraction"].isna().sum()),
        "group_count": int(len(visit_groups)),
        "visit_sum": float(visit_rows["visit_fraction"].sum()),
        "groups_sum_not_one_count": groups_not_one,
        "all_group_sums_equal_one": groups_not_one == 0,
        "local_fallback_group_count": int(
            (visit_groups["location_key_type"] == "LOCAL").sum()
        )
        if not visit_groups.empty
        else 0,
        "legacy_parity_2026_06_01": parity,
    }
    summary_frame = visit_rows.copy()
    by_week = _summary_records(summary_frame, "week_start")
    by_day = _summary_records(summary_frame, "fecha")
    by_cliente = _summary_records(summary_frame, "cliente_norm")
    candidate_files_used = [
        {
            "source_file_id": item["source_file_id"],
            "source_file_name": item["source_file_name"],
            "source_file_sha256": item["actual_sha256"],
            "row_count": item["row_count"],
            "sha256_match": item["sha256_match"],
        }
        for item in integrity_rows
        if item["sha256_match"]
        and not item["critical_columns_missing"]
        and not item["read_error"]
    ]
    manifest_integrity = {
        "candidate_role_consistency": not selection_errors,
        "all_files_exist": bool(integrity_rows)
        and all(item["exists"] for item in integrity_rows),
        "all_sha256_match": bool(integrity_rows)
        and all(item["sha256_match"] for item in integrity_rows),
        "all_row_counts_match": bool(integrity_rows)
        and all(item["row_count_match"] for item in integrity_rows),
        "candidate_truncation_count": sum(
            bool(item["truncation_suspect"]) for item in integrity_rows
        ),
        "file_checks": integrity_rows,
        "errors": manifest_errors,
    }
    return {
        "phase_id": PHASE_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "input_manifest_014D": {
            "path": input_manifest_path.as_posix(),
            "sha256": input_manifest_sha256,
            "phase_id": input_manifest.get("phase_id"),
            "verdict": input_manifest.get("verdict"),
        },
        "dry_run_batch_id": dry_run_batch_id,
        "guardrails": {
            "mode": "LOCAL_DRY_RUN_NO_APPLY",
            "db_access": {"used": False},
            "sql_apply": False,
            "ddl": False,
            "data_movement": False,
            "productive_loader_touched": False,
            "productive_loader_run": False,
            "app_touched": False,
            "ux_modified": False,
            "data_files_modified": False,
            "input_manifest_modified": False,
            "payload_rows_persisted": False,
            "lightweight_research_outputs_written": True,
        },
        "layer_separation": {
            "raw_payload_candidate": "normalized event-photo rows held in memory",
            "derived_validation": "VISITA, coverage, dedupe and parity checks only",
            "future_mart_logic": {
                "computed": False,
                "reason": "official compliance remains outside 014E",
            },
        },
        "candidate_files_used": candidate_files_used,
        "excluded_files": excluded_files,
        "manifest_integrity_check": manifest_integrity,
        "dry_run_payload_summary": dry_run_summary,
        "dedupe_summary": dedupe_summary,
        "coverage_summary": coverage,
        "visit_validation_summary": visit_summary,
        "would_insert_summary_by_week": by_week,
        "would_insert_summary_by_day": by_day,
        "would_insert_summary_by_cliente": by_cliente,
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
        description="014E local/no-apply KPIONE candidate dry-run loader."
    )
    parser.add_argument("--base", default=".")
    parser.add_argument("--month", default="2026-06")
    parser.add_argument(
        "--manifest",
        default=(
            "research/014D_KPIONE_RAW_EXPORT_REMEDIATION_NO_APPLY/"
            "014D_remediation_manifest.json"
        ),
    )
    parser.add_argument(
        "--soft-exit",
        action="store_true",
        help="Return exit 0 after emitting a blocked dry-run verdict.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    base = Path(args.base)
    input_manifest_path = Path(args.manifest)
    payload = build_dry_run_payload(base, args.month, input_manifest_path)
    manifest_path, report_path = write_outputs(base, payload)
    print(
        json.dumps(
            {
                "phase_id": PHASE_ID,
                "dry_run_batch_id": payload["dry_run_batch_id"],
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
    return 0 if payload["verdict"] in {VERDICT_READY, VERDICT_READY_WARN} else 1


if __name__ == "__main__":
    raise SystemExit(main())
