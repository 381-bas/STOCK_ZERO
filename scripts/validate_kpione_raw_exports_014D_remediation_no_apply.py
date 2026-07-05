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


PHASE_ID = "014D_KPIONE_RAW_EXPORT_REMEDIATION_NO_APPLY"
OUTPUT_RELATIVE_DIR = Path("research/014D_KPIONE_RAW_EXPORT_REMEDIATION_NO_APPLY")
MANIFEST_FILENAME = "014D_remediation_manifest.json"
REPORT_FILENAME = "014D_remediation_report.md"
BASELINE_MANIFEST = Path(
    "research/014C_KPIONE_RAW_EXPORT_VALIDATOR_NO_APPLY/"
    "014C_kpione_raw_export_manifest.json"
)
TARGET_EVENT_ID = "862144"

INCLUDE_SOURCE_IDS = [
    "1781975989376",
    "1783219885210",
    "1783220552913",
    "1783219914054",
    "1781976423312",
    "1781973512473",
    "1782440454408",
    "1783220085725",
    "1783220157694",
]
QUARANTINE_SOURCE_IDS = ["1781976368641"]
COMPARE_ONLY_SOURCE_IDS = ["1782012877303"]
ORIGINAL_BASELINE_SOURCE_IDS = [
    "1781975989376",
    "1781976368641",
    "1782012877303",
    "1781976423312",
    "1781973512473",
    "1782440454408",
]
REMEDIATION_SOURCE_IDS = [
    "1783219885210",
    "1783220552913",
    "1783219914054",
    "1783220085725",
    "1783220157694",
]

VERDICT_READY = "REMEDIATION_READY_FOR_DRY_RUN_LOADER"
VERDICT_READY_WARN = "REMEDIATION_READY_FOR_DRY_RUN_LOADER_WITH_WARNINGS"
VERDICT_CONFLICT = "REMEDIATION_BLOCKED_BY_NEW_CONFLICTS"
VERDICT_TRUNCATION = "REMEDIATION_BLOCKED_BY_TRUNCATION"
VERDICT_SCHEMA = "REMEDIATION_SCHEMA_BLOCKED"
VERDICT_PARTIAL = "REMEDIATION_PARTIAL_STILL_HAS_GAPS"


def source_path(base: Path, source_file_id: str) -> Path:
    return base / "data" / f"photo-excel-admin_{source_file_id}.xlsx"


def policy_role(source_file_id: str) -> str:
    if source_file_id in INCLUDE_SOURCE_IDS:
        return "include_candidate"
    if source_file_id in QUARANTINE_SOURCE_IDS:
        return "quarantine_truncation"
    return "compare_only"


def _profile_file(
    path: Path,
    *,
    requested_role: str,
) -> tuple[dict[str, Any], pd.DataFrame | None, list[dict[str, Any]]]:
    source_file_id = baseline.parse_source_file_id(path)
    source_sha256 = baseline.sha256_file(path)
    base_profile = {
        "source_file_id": source_file_id,
        "source_file_name": path.name,
        "source_path": path.as_posix(),
        "source_file_sha256": source_sha256,
        "requested_role": requested_role,
        "role": requested_role,
        "sheet": baseline.RAW_SHEET,
        "read_error": None,
    }
    try:
        raw_df = baseline.read_excel_sheet(path, baseline.RAW_SHEET)
    except Exception as exc:
        return {
            **base_profile,
            "role": "rejected_schema",
            "row_count": 0,
            "distinct_id_count": 0,
            "fecha_min": None,
            "fecha_max": None,
            "columns_present": [],
            "critical_columns_missing": baseline.CRITICAL_RAW_COLUMNS,
            "expected_columns_missing": baseline.EXPECTED_RAW_COLUMNS,
            "truncation_suspect": False,
            "blank_id_rows": 0,
            "invalid_date_rows": 0,
            "read_error": f"{exc.__class__.__name__}:{exc}",
        }, None, []

    raw_df.columns = [str(column).strip() for column in raw_df.columns]
    resolved = baseline._resolved_columns(raw_df)
    critical_missing = [
        column
        for column in baseline.CRITICAL_RAW_COLUMNS
        if baseline.normalize_header(column) not in resolved
    ]
    expected_missing = [
        column
        for column in baseline.EXPECTED_RAW_COLUMNS
        if baseline.normalize_header(column) not in resolved
    ]
    event_ids = baseline._column_series(raw_df, resolved, "ID").map(
        baseline._clean_id
    )
    dates = baseline._parse_dates(
        baseline._column_series(raw_df, resolved, "Fecha")
    )
    profile = {
        **base_profile,
        "role": "rejected_schema" if critical_missing else requested_role,
        "row_count": int(len(raw_df)),
        "distinct_id_count": int(event_ids[event_ids != ""].nunique()),
        "fecha_min": baseline._date_iso(dates.min())
        if not dates.dropna().empty
        else None,
        "fecha_max": baseline._date_iso(dates.max())
        if not dates.dropna().empty
        else None,
        "columns_present": list(raw_df.columns),
        "critical_columns_missing": critical_missing,
        "expected_columns_missing": expected_missing,
        "truncation_suspect": int(len(raw_df))
        >= baseline.TRUNCATION_ROW_THRESHOLD,
        "blank_id_rows": int((event_ids == "").sum()),
        "invalid_date_rows": int(dates.isna().sum()),
    }
    if critical_missing:
        return profile, None, []

    canonical = baseline.normalize_raw_events(
        raw_df,
        source_file_id=source_file_id,
        source_file_sha256=source_sha256,
    )
    target_rows: list[dict[str, Any]] = []
    target_indexes = list(raw_df.index[event_ids == TARGET_EVENT_ID])
    for raw_index in target_indexes:
        canonical_row = canonical.loc[raw_index]

        def value(expected_column: str) -> str:
            actual = resolved.get(baseline.normalize_header(expected_column))
            return (
                baseline._clean_text(raw_df.at[raw_index, actual])
                if actual is not None
                else ""
            )

        target_rows.append(
            {
                "source_file_id": source_file_id,
                "source_file_name": path.name,
                "role": profile["role"],
                "source_row_number": int(canonical_row["source_row_number"]),
                "cod_rt": value("Codigo Local"),
                "local_nombre": value("Local"),
                "cliente_norm": value("Marca"),
                "fecha": value("Fecha"),
                "hora": value("Hora"),
                "tipo_tarea": value("Tipo de Tarea"),
                "n_fotos": value("Foto Nº/Total"),
                "link_foto": value("Link Foto"),
                "row_hash": str(canonical_row["_photo_row_hash"]),
            }
        )
    return profile, canonical, target_rows


def _event_fingerprints(frame: pd.DataFrame) -> dict[str, str]:
    fingerprints: dict[str, str] = {}
    valid = frame[frame["event_id"] != ""]
    for event_id, rows in valid.groupby("event_id"):
        row_hashes = sorted(set(rows["_photo_row_hash"]))
        fingerprints[str(event_id)] = hashlib.sha256(
            "\n".join(row_hashes).encode("utf-8")
        ).hexdigest()
    return fingerprints


def compare_event_sets(
    *,
    label: str,
    left_frames: list[pd.DataFrame],
    right_frames: list[pd.DataFrame],
    date_filter: str | None = None,
) -> dict[str, Any]:
    left = pd.concat(left_frames, ignore_index=True) if left_frames else pd.DataFrame()
    right = (
        pd.concat(right_frames, ignore_index=True) if right_frames else pd.DataFrame()
    )
    if date_filter is not None:
        target = pd.Timestamp(date_filter).normalize()
        left = left[left["fecha"] == target]
        right = right[right["fecha"] == target]

    left_fingerprints = _event_fingerprints(left) if not left.empty else {}
    right_fingerprints = _event_fingerprints(right) if not right.empty else {}
    left_ids = set(left_fingerprints)
    right_ids = set(right_fingerprints)
    matched = sorted(left_ids & right_ids)
    same_hash = [
        event_id
        for event_id in matched
        if left_fingerprints[event_id] == right_fingerprints[event_id]
    ]
    diff_hash = [
        event_id
        for event_id in matched
        if left_fingerprints[event_id] != right_fingerprints[event_id]
    ]

    stable_conflicts: list[str] = []
    if not left.empty or not right.empty:
        combined = pd.concat([left, right], ignore_index=True)
        stable_counts = combined.groupby("event_id")["event_stable_hash"].nunique()
        stable_conflicts = sorted(stable_counts[stable_counts > 1].index)
    return {
        "label": label,
        "date_filter": date_filter,
        "left_source_file_ids": sorted(
            set(left["source_file_id"]) if not left.empty else set()
        ),
        "right_source_file_ids": sorted(
            set(right["source_file_id"]) if not right.empty else set()
        ),
        "left_row_count": int(len(left)),
        "right_row_count": int(len(right)),
        "left_id_count": len(left_ids),
        "right_id_count": len(right_ids),
        "matched_id_count": len(matched),
        "left_only_count": len(left_ids - right_ids),
        "right_only_count": len(right_ids - left_ids),
        "same_id_same_hash_count": len(same_hash),
        "same_id_diff_hash_count": len(diff_hash),
        "event_stable_hash_conflict_count": len(stable_conflicts),
        "left_only_sample": sorted(left_ids - right_ids)[:20],
        "right_only_sample": sorted(right_ids - left_ids)[:20],
        "same_hash_sample": same_hash[:20],
        "diff_hash_sample": diff_hash[:20],
        "event_stable_conflict_sample": stable_conflicts[:20],
    }


def candidate_overlap_by_date(candidate: pd.DataFrame) -> list[dict[str, Any]]:
    if candidate.empty:
        return []
    source_counts = candidate.groupby("event_id")["source_file_id"].nunique()
    overlap_ids = set(source_counts[source_counts > 1].index)
    overlap_rows = candidate[candidate["event_id"].isin(overlap_ids)]
    result: list[dict[str, Any]] = []
    for fecha, rows in overlap_rows.groupby("fecha"):
        result.append(
            {
                "fecha": baseline._date_iso(fecha),
                "overlapping_id_count": int(rows["event_id"].nunique()),
                "source_file_ids": sorted(set(rows["source_file_id"])),
            }
        )
    return result


def classify_target_event(
    target_rows: list[dict[str, Any]],
    frame_by_source: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    candidate_frames = [
        frame_by_source[source_id]
        for source_id in INCLUDE_SOURCE_IDS
        if source_id in frame_by_source
        and TARGET_EVENT_ID in set(frame_by_source[source_id]["event_id"])
    ]
    quarantine_frames = [
        frame_by_source[source_id]
        for source_id in QUARANTINE_SOURCE_IDS
        if source_id in frame_by_source
        and TARGET_EVENT_ID in set(frame_by_source[source_id]["event_id"])
    ]

    if not candidate_frames:
        classification = "not_found"
    else:
        candidate_fingerprints = [
            _event_fingerprints(frame).get(TARGET_EVENT_ID)
            for frame in candidate_frames
        ]
        candidate_fingerprints = [
            item for item in candidate_fingerprints if item is not None
        ]
        if len(set(candidate_fingerprints)) > 1:
            classification = "still_conflict_in_candidate"
        elif quarantine_frames:
            quarantine_fingerprints = [
                _event_fingerprints(frame).get(TARGET_EVENT_ID)
                for frame in quarantine_frames
            ]
            quarantine_fingerprints = [
                item for item in quarantine_fingerprints if item is not None
            ]
            classification = (
                "normalized_equivalent"
                if set(candidate_fingerprints) == set(quarantine_fingerprints)
                else "resolved_by_excluding_truncated"
            )
        else:
            classification = "normalized_equivalent"

    def rows_for(source_ids: list[str]) -> list[dict[str, Any]]:
        return [
            row
            for row in target_rows
            if row["source_file_id"] in source_ids
        ]

    return {
        "event_id": TARGET_EVENT_ID,
        "classification": classification,
        "baseline_rows": rows_for(ORIGINAL_BASELINE_SOURCE_IDS),
        "candidate_rows": rows_for(INCLUDE_SOURCE_IDS),
        "quarantine_rows": rows_for(QUARANTINE_SOURCE_IDS),
        "compare_only_rows": rows_for(COMPARE_ONLY_SOURCE_IDS),
    }


def determine_remediation_verdict(
    *,
    candidate_schema_blocked: bool,
    candidate_truncation: bool,
    candidate_conflicts: bool,
    calendar_complete: bool,
    operational_complete: bool,
    visit_formula_closes: bool,
    parity_match_rate: float | None,
    warnings_present: bool,
) -> str:
    if candidate_schema_blocked:
        return VERDICT_SCHEMA
    if candidate_truncation:
        return VERDICT_TRUNCATION
    if candidate_conflicts or not visit_formula_closes:
        return VERDICT_CONFLICT
    if (
        not calendar_complete
        or not operational_complete
        or parity_match_rate is None
        or parity_match_rate < baseline.PARITY_READY_THRESHOLD
    ):
        return VERDICT_PARTIAL
    return VERDICT_READY_WARN if warnings_present else VERDICT_READY


def _report_markdown(payload: dict[str, Any]) -> str:
    candidate_rows = [
        "| {source_file_id} | {source_file_name} | {row_count} | {fecha_min} | "
        "{fecha_max} | {truncation_suspect} |".format(**item)
        for item in payload["file_manifest"]
        if item["role"] == "include_candidate"
    ]
    quarantine_rows = [
        f"| {item['source_file_id']} | {item['source_file_name']} | "
        f"{item['role']} | {item['row_count']} |"
        for item in payload["file_manifest"]
        if item["role"] in {"quarantine_truncation", "compare_only", "rejected_schema"}
    ]
    comparison_rows = [
        f"| {item['label']} | {item['matched_id_count']} | "
        f"{item['same_id_same_hash_count']} | {item['same_id_diff_hash_count']} | "
        f"{item['event_stable_hash_conflict_count']} |"
        for item in payload["comparison_set"]
    ]
    target_rows = [
        f"| {row['role']} | {row['source_file_id']} | {row['source_row_number']} | "
        f"{row['fecha']} | {row['n_fotos']} | {row['tipo_tarea']} |"
        for row in payload["id_862144_inspection"]["baseline_rows"]
        + payload["id_862144_inspection"]["candidate_rows"]
        if row["role"] in {"quarantine_truncation", "compare_only", "include_candidate"}
    ]
    coverage_after = payload["coverage_before_after"]["candidate"]
    parity = payload["legacy_parity_2026_06_01"]
    parity_rate = parity.get("match_rate")
    parity_text = f"{parity_rate:.4f}" if parity_rate is not None else "unavailable"
    blocker_lines = "\n".join(f"- {item}" for item in payload["blockers"]) or "- none"
    warning_lines = "\n".join(f"- {item}" for item in payload["warnings"]) or "- none"
    recommendation = {
        VERDICT_READY: "Proceed to a local dry-run loader design; productive apply remains forbidden.",
        VERDICT_READY_WARN: "Proceed to a local dry-run loader design with the documented overlap warnings.",
        VERDICT_CONFLICT: "Resolve candidate conflicts before any dry-run loader design.",
        VERDICT_TRUNCATION: "Replace or remove truncated candidate inputs and rerun 014D.",
        VERDICT_SCHEMA: "Restore the candidate export schema and rerun 014D.",
        VERDICT_PARTIAL: "Obtain the missing dates or parity evidence and rerun 014D.",
    }[payload["verdict"]]
    next_phase = (
        "014E_KPIONE_RAW_DRY_RUN_LOADER_NO_APPLY"
        if payload["verdict"] in {VERDICT_READY, VERDICT_READY_WARN}
        else "014D_REMEDIATE_REMAINING_BLOCKERS_NO_APPLY"
    )
    return f"""# 014D KPIONE Raw Export Remediation — No Apply

## Resumen ejecutivo

- Verdict: `{payload["verdict"]}`
- Candidate files: {len(payload["candidate_set"]["source_file_ids"])}
- Candidate canonical rows: {payload["candidate_set"]["canonical_row_count"]}
- Candidate distinct event IDs: {payload["candidate_set"]["distinct_event_id_count"]}
- Candidate coverage: {coverage_after["covered_day_count"]}/30 calendar days
- ID 862144: `{payload["id_862144_inspection"]["classification"]}`

## Candidate set

| Source file ID | File | Rows | Fecha min | Fecha max | Truncation |
|---|---|---:|---|---|---|
{chr(10).join(candidate_rows)}

## Archivos en cuarentena / compare-only

| Source file ID | File | Role | Rows |
|---|---|---|---:|
{chr(10).join(quarantine_rows)}

## Cobertura global

- Baseline 014C covered days: {payload["coverage_before_after"]["baseline_014C"]["covered_day_count"]}
- Baseline 014C missing: {", ".join(payload["coverage_before_after"]["baseline_014C"]["missing_days"]) or "none"}
- Candidate covered days: {coverage_after["covered_day_count"]}
- Candidate missing: {", ".join(coverage_after["missing_days"]) or "none"}
- S1-S4 complete: {str(coverage_after["operational_coverage_complete"]).lower()}
- June calendar complete: {str(coverage_after["calendar_month_complete"]).lower()}
- 2026-06-29..30 are reported in June calendar; their operational week belongs to July.

## Resolucion del truncado y comparisons

| Comparison | Matched IDs | Same hash | Different hash | Stable conflicts |
|---|---:|---:|---:|---:|
{chr(10).join(comparison_rows)}

## Resolucion ID 862144

- Classification: `{payload["id_862144_inspection"]["classification"]}`

| Role | Source file ID | Row | Fecha | N Fotos | Tipo tarea |
|---|---|---:|---|---|---|
{chr(10).join(target_rows)}

Full lightweight row details, links and hashes are preserved in the JSON manifest.

## Dedupe / overlaps del candidate

- overlapping IDs: {payload["overlap_summary"]["candidate"]["overlapping_id_count"]}
- same_id_same_hash: {payload["overlap_summary"]["candidate"]["same_id_same_hash_count"]}
- same_id_diff_hash: {payload["overlap_summary"]["candidate"]["same_id_diff_hash_count"]}
- event_stable_hash conflicts: {payload["overlap_summary"]["candidate"]["event_stable_hash_conflict_count"]}
- exact duplicate photo rows removed: {payload["candidate_set"]["exact_duplicate_rows_removed"]}

## VISITA candidate

- Formula: `1 / count(Codigo Local, Fecha, Marca)`, with `Local` fallback.
- group_count: {payload["visit_formula_summary"]["group_count"]}
- visit_sum: {payload["visit_formula_summary"]["visit_sum"]:.6f}
- groups where sum != 1: {payload["visit_formula_summary"]["groups_sum_not_one_count"]}
- fallback group count: {payload["visit_formula_summary"]["local_fallback_group_count"]}

## Paridad legacy 2026-06-01

- raw_id_count: {parity["raw_id_count"]}
- legacy_id_count: {parity["legacy_id_count"]}
- matched_id_count: {parity["matched_id_count"]}
- raw_only_count: {parity["raw_only_count"]}
- legacy_only_count: {parity["legacy_only_count"]}
- match_rate: {parity_text}

## Blockers

{blocker_lines}

## Warnings

{warning_lines}

## Decision recomendada

{recommendation}

## Siguiente fase

`{next_phase}`

## Declaracion no-apply

No Supabase, no DB, no SQL/DDL apply, no productive loader, no refresh, no UX
modification, no backfill, no cutover and no data movement were used. Baseline
014C and every Excel input remained read-only.
"""


def build_remediation_payload(base: Path, month: str) -> dict[str, Any]:
    all_source_ids = (
        INCLUDE_SOURCE_IDS + QUARANTINE_SOURCE_IDS + COMPARE_ONLY_SOURCE_IDS
    )
    file_manifest: list[dict[str, Any]] = []
    frame_by_source: dict[str, pd.DataFrame] = {}
    target_rows: list[dict[str, Any]] = []
    schema_blockers: list[str] = []
    warnings: list[str] = []

    for source_file_id in all_source_ids:
        path = source_path(base, source_file_id)
        requested_role = policy_role(source_file_id)
        if not path.exists():
            profile = {
                "source_file_id": source_file_id,
                "source_file_name": path.name,
                "source_path": path.as_posix(),
                "source_file_sha256": None,
                "requested_role": requested_role,
                "role": "rejected_schema",
                "sheet": baseline.RAW_SHEET,
                "row_count": 0,
                "distinct_id_count": 0,
                "fecha_min": None,
                "fecha_max": None,
                "columns_present": [],
                "critical_columns_missing": baseline.CRITICAL_RAW_COLUMNS,
                "expected_columns_missing": baseline.EXPECTED_RAW_COLUMNS,
                "truncation_suspect": False,
                "blank_id_rows": 0,
                "invalid_date_rows": 0,
                "read_error": "file_not_found",
            }
            file_manifest.append(profile)
            if requested_role == "include_candidate":
                schema_blockers.append(f"candidate_file_missing:{path.name}")
            else:
                warnings.append(f"noncandidate_file_missing:{path.name}")
            continue

        profile, canonical, inspected_rows = _profile_file(
            path,
            requested_role=requested_role,
        )
        file_manifest.append(profile)
        target_rows.extend(inspected_rows)
        if profile["role"] == "rejected_schema":
            message = (
                f"schema_rejected:{profile['source_file_name']}:"
                f"{','.join(profile['critical_columns_missing']) or profile['read_error']}"
            )
            if requested_role == "include_candidate":
                schema_blockers.append(message)
            else:
                warnings.append(message)
        if profile["expected_columns_missing"]:
            warnings.append(
                f"expected_columns_missing:{profile['source_file_name']}:"
                f"{','.join(profile['expected_columns_missing'])}"
            )
        if profile["blank_id_rows"]:
            warning = (
                f"blank_id_rows:{profile['source_file_name']}:"
                f"{profile['blank_id_rows']}"
            )
            if requested_role == "include_candidate":
                warnings.append(f"candidate_{warning}")
            else:
                warnings.append(f"noncandidate_{warning}")
        if profile["invalid_date_rows"]:
            warning = (
                f"invalid_date_rows:{profile['source_file_name']}:"
                f"{profile['invalid_date_rows']}"
            )
            if requested_role == "include_candidate":
                warnings.append(f"candidate_{warning}")
            else:
                warnings.append(f"noncandidate_{warning}")
        if canonical is not None:
            frame_by_source[source_file_id] = canonical

    candidate_frames = [
        frame_by_source[source_id]
        for source_id in INCLUDE_SOURCE_IDS
        if source_id in frame_by_source
    ]
    candidate_input = (
        pd.concat(candidate_frames, ignore_index=True)
        if candidate_frames
        else pd.DataFrame(
            columns=baseline.CANONICAL_COLUMNS + ["_photo_row_hash"]
        )
    )
    candidate_duplicate = baseline._duplicate_summary(candidate_input)
    candidate, duplicate_rows_removed = baseline._canonical_dedupe(candidate_input)
    visit_rows, visit_groups = baseline.compute_visit_fraction(candidate)
    visit_base_summary = baseline._visit_formula_summary(visit_rows, visit_groups)
    groups_not_one = (
        int(visit_groups["visit_sum"].sub(1.0).abs().gt(1e-12).sum())
        if not visit_groups.empty
        else 0
    )
    visit_summary = {
        **{
            key: value
            for key, value in visit_base_summary.items()
            if key != "group_summary_sample"
        },
        "groups_sum_not_one_count": groups_not_one,
    }
    candidate_coverage = baseline.compute_june_coverage(candidate, month)

    baseline_manifest_path = base / BASELINE_MANIFEST
    if baseline_manifest_path.exists():
        baseline_payload = json.loads(
            baseline_manifest_path.read_text(encoding="utf-8")
        )
        baseline_coverage = baseline_payload["june_coverage"]
        baseline_verdict = baseline_payload["verdict"]
    else:
        baseline_coverage = {
            "covered_day_count": 0,
            "covered_days": [],
            "missing_day_count": 30,
            "missing_days": [],
        }
        baseline_verdict = None
        warnings.append("baseline_014C_manifest_missing")

    legacy_path = base / baseline.LEGACY_RELATIVE_PATH
    try:
        legacy_df = baseline.read_excel_sheet(legacy_path, baseline.LEGACY_SHEET)
        legacy_parity = baseline.compute_legacy_parity(candidate, legacy_df)
    except Exception as exc:
        legacy_parity = {
            "available": False,
            "target_date": baseline.PARITY_DATE,
            "raw_id_count": 0,
            "legacy_id_count": 0,
            "matched_id_count": 0,
            "raw_only_count": 0,
            "legacy_only_count": 0,
            "match_rate": None,
            "raw_only_sample": [],
            "legacy_only_sample": [],
            "error": f"{exc.__class__.__name__}:{exc}",
        }
        warnings.append("legacy_parity_unavailable")

    ids_by_source = {
        source_id: {
            value
            for value in frame["event_id"]
            if value
        }
        for source_id, frame in frame_by_source.items()
        if source_id in INCLUDE_SOURCE_IDS
    }
    candidate_overlap_matrix = baseline.compute_overlap_matrix(ids_by_source)
    overlap_dates = candidate_overlap_by_date(candidate_input)

    comparison_set = [
        compare_event_sets(
            label="truncated_06_08_13_vs_replacements",
            left_frames=[
                frame_by_source[source_id]
                for source_id in QUARANTINE_SOURCE_IDS
                if source_id in frame_by_source
            ],
            right_frames=[
                frame_by_source[source_id]
                for source_id in [
                    "1783219885210",
                    "1783220552913",
                    "1783219914054",
                ]
                if source_id in frame_by_source
            ],
        ),
        compare_event_sets(
            label="old_06_08_patch_vs_new_06_08",
            left_frames=[
                frame_by_source["1782012877303"]
            ]
            if "1782012877303" in frame_by_source
            else [],
            right_frames=[
                frame_by_source["1783219885210"]
            ]
            if "1783219885210" in frame_by_source
            else [],
            date_filter="2026-06-08",
        ),
        compare_event_sets(
            label="old_06_20_24_vs_new_06_24_28_on_06_24",
            left_frames=[
                frame_by_source["1782440454408"]
            ]
            if "1782440454408" in frame_by_source
            else [],
            right_frames=[
                frame_by_source["1783220085725"]
            ]
            if "1783220085725" in frame_by_source
            else [],
            date_filter="2026-06-24",
        ),
    ]
    target_inspection = classify_target_event(target_rows, frame_by_source)

    candidate_profiles = [
        item for item in file_manifest if item["role"] == "include_candidate"
    ]
    candidate_truncation_profiles = [
        item for item in candidate_profiles if item["truncation_suspect"]
    ]
    candidate_conflicts = bool(
        candidate_duplicate["same_id_diff_hash_count"]
        or candidate_duplicate["event_stable_hash_conflict_count"]
    )
    if candidate_duplicate["same_id_same_hash_count"]:
        warnings.append(
            "candidate_dedupe_same_hash_ids:"
            + str(candidate_duplicate["same_id_same_hash_count"])
        )
    if duplicate_rows_removed:
        warnings.append(
            f"candidate_exact_duplicate_rows_removed:{duplicate_rows_removed}"
        )
    for comparison in comparison_set:
        if comparison["same_id_diff_hash_count"]:
            warnings.append(
                f"compare_only_diff_hash:{comparison['label']}:"
                f"{comparison['same_id_diff_hash_count']}"
            )
    if overlap_dates:
        warnings.append(
            "candidate_expected_overlap_dates:"
            + ",".join(item["fecha"] for item in overlap_dates)
        )

    blockers = list(schema_blockers)
    if candidate_truncation_profiles:
        blockers.append(
            "candidate_truncation_suspect:"
            + ",".join(
                item["source_file_name"]
                for item in candidate_truncation_profiles
            )
        )
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
    if candidate_coverage["missing_days"]:
        blockers.append(
            "candidate_june_missing_days:"
            + ",".join(candidate_coverage["missing_days"])
        )
    if groups_not_one:
        blockers.append(f"candidate_visit_groups_not_one:{groups_not_one}")
    parity_rate = legacy_parity.get("match_rate")
    if parity_rate is None or parity_rate < baseline.PARITY_READY_THRESHOLD:
        blockers.append(
            "candidate_legacy_parity_below_threshold:"
            + ("unavailable" if parity_rate is None else f"{parity_rate:.6f}")
        )

    verdict = determine_remediation_verdict(
        candidate_schema_blocked=bool(schema_blockers),
        candidate_truncation=bool(candidate_truncation_profiles),
        candidate_conflicts=candidate_conflicts,
        calendar_complete=candidate_coverage["calendar_month_complete"],
        operational_complete=candidate_coverage[
            "operational_coverage_complete"
        ],
        visit_formula_closes=not groups_not_one,
        parity_match_rate=parity_rate,
        warnings_present=bool(warnings),
    )

    valid_ids = candidate.loc[candidate["event_id"] != "", "event_id"]
    file_roles = {
        role: [
            item["source_file_id"]
            for item in file_manifest
            if item["role"] == role
        ]
        for role in (
            "include_candidate",
            "quarantine_truncation",
            "compare_only",
            "rejected_schema",
        )
    }
    candidate_set = {
        "source_file_ids": file_roles["include_candidate"],
        "source_file_names": [
            item["source_file_name"]
            for item in candidate_profiles
        ],
        "input_row_count": int(len(candidate_input)),
        "canonical_row_count": int(len(candidate)),
        "exact_duplicate_rows_removed": duplicate_rows_removed,
        "distinct_event_id_count": int(valid_ids.nunique()),
        "fecha_min": baseline._date_iso(candidate["fecha"].min())
        if not candidate["fecha"].dropna().empty
        else None,
        "fecha_max": baseline._date_iso(candidate["fecha"].max())
        if not candidate["fecha"].dropna().empty
        else None,
        "canonical_rows_persisted": False,
    }
    quarantine_set = {
        "source_file_ids": file_roles["quarantine_truncation"],
        "compare_only_source_file_ids": file_roles["compare_only"],
        "candidate_excludes_all_quarantine": all(
            source_id not in candidate_set["source_file_ids"]
            for source_id in file_roles["quarantine_truncation"]
        ),
    }
    return {
        "phase_id": PHASE_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "guardrails": {
            "mode": "LOCAL_REMEDIATION_NO_APPLY",
            "db_access": {"used": False},
            "supabase_used": False,
            "sql_apply": False,
            "ddl": False,
            "productive_loader_run": False,
            "productive_refresh_run": False,
            "ux_modified": False,
            "data_movement": False,
            "data_files_modified": False,
            "baseline_014C_modified": False,
            "canonical_rows_persisted": False,
            "lightweight_research_outputs_written": True,
        },
        "input_files": {
            "base": str(base),
            "month": month,
            "baseline_014C_manifest": BASELINE_MANIFEST.as_posix(),
            "legacy_file": baseline.LEGACY_RELATIVE_PATH.as_posix(),
        },
        "file_manifest": file_manifest,
        "file_roles": file_roles,
        "candidate_set": candidate_set,
        "quarantine_set": quarantine_set,
        "comparison_set": comparison_set,
        "overlap_summary": {
            "candidate": candidate_duplicate,
            "candidate_matrix": candidate_overlap_matrix,
            "candidate_overlaps_by_date": overlap_dates,
        },
        "conflict_summary": {
            "candidate_real_conflict_count": (
                candidate_duplicate["same_id_diff_hash_count"]
                + candidate_duplicate["event_stable_hash_conflict_count"]
            ),
            "candidate_conflicts_blocking": candidate_conflicts,
            "compare_only_diff_hash_count": sum(
                item["same_id_diff_hash_count"]
                for item in comparison_set
            ),
        },
        "id_862144_inspection": target_inspection,
        "coverage_before_after": {
            "baseline_014C_verdict": baseline_verdict,
            "baseline_014C": baseline_coverage,
            "candidate": candidate_coverage,
        },
        "visit_formula_summary": visit_summary,
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
        description="014D local/no-apply KPIONE remediation validator."
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
    payload = build_remediation_payload(base, args.month)
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
    return 0 if payload["verdict"] in {VERDICT_READY, VERDICT_READY_WARN} else 1


if __name__ == "__main__":
    raise SystemExit(main())
