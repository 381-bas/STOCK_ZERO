# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

import load_kpione2_photo_from_excel as photo_loader


MODE = "ORANGE_NO_APPLY_DRY_RUN"
PASS_VERDICT = "PASS_ROUTE_B_DENOMINATOR_DRY_RUN"
BLOCK_VERDICT = "BLOCK_ROUTE_B_DENOMINATOR_DRY_RUN"
PROTECTED_METRICS = [
    "EXIGIDAS",
    "VISITA",
    "VISITA_REALIZADA",
    "VISITA_REALIZADA_RAW",
    "VISITA_REALIZADA_CAP",
    "PENDIENTE",
    "ALERTA",
]
DENOMINATOR_METRICS = ["EXIGIDAS", "VISITA"]
NUMERATOR_METRICS = [
    "VISITA_REALIZADA",
    "VISITA_REALIZADA_RAW",
    "VISITA_REALIZADA_CAP",
    "PENDIENTE",
    "ALERTA",
]
METRICS_DEFINITION = {
    "scope": "controlled_local_sample_only_not_productive_semantics",
    "grain": ["semana_inicio", "cod_rt", "cliente_norm"],
    "EXIGIDAS": "route_rows.EXIGIDAS; copied before/after and never sourced from photos",
    "VISITA": "local alias of EXIGIDAS; copied before/after and never sourced from photos",
    "VISITA_REALIZADA": "count of distinct day_presence dates; equal to VISITA_REALIZADA_RAW in this local sample",
    "VISITA_REALIZADA_RAW": "count of distinct day_presence dates before/after Route B union",
    "VISITA_REALIZADA_CAP": "min(VISITA_REALIZADA_RAW, VISITA)",
    "PENDIENTE": "max(VISITA - VISITA_REALIZADA_CAP, 0)",
    "ALERTA": "CUMPLE when VISITA_REALIZADA_RAW >= VISITA, otherwise INCUMPLE",
    "before": "existing_presence_dates supplied by the controlled local route fixture",
    "after": "set union of existing_presence_dates and structurally derived Route B day_presence",
}
LOADER_STRUCTURAL_FLAGS = [
    "grain_contract_match",
    "no_null_event_id_rows",
    "no_null_fecha_rows",
    "no_null_n_fotos_calculado_rows",
    "no_event_ids_multi_fecha",
    "no_event_ids_multi_week",
    "no_event_ids_multi_sp_item",
    "no_row_count_n_fotos_mismatch_events",
    "no_real_content_conflict_event_ids",
]


class LocalInputError(ValueError):
    pass


def _clean_key(value: object) -> str:
    return photo_loader.normalize_key(value)


def _timestamp(value: object) -> pd.Timestamp | None:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).normalize()


def _date_iso(value: pd.Timestamp | None) -> str | None:
    return value.date().isoformat() if value is not None else None


def _week_start(value: pd.Timestamp) -> pd.Timestamp:
    return value - pd.Timedelta(days=value.weekday())


def _nonnegative_int(value: object) -> int | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not parsed.is_integer() or parsed < 0:
        return None
    return int(parsed)


def _photo_expected(df: pd.DataFrame, resolved: dict[str, str]) -> dict[str, Any]:
    event_col = resolved.get("event_id")
    date_col = resolved.get("fecha")
    event_ids = (
        df[event_col].astype("string").fillna("").str.strip()
        if event_col
        else pd.Series(dtype="string")
    )
    dates = (
        pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
        if date_col
        else pd.Series(dtype="datetime64[ns]")
    )
    valid_dates = dates.dropna()
    return {
        "photo_rows": int(len(df)),
        "distinct_event_ids": int(event_ids[event_ids != ""].nunique()),
        "fecha_min": _date_iso(pd.Timestamp(valid_dates.min())) if not valid_dates.empty else None,
        "fecha_max": _date_iso(pd.Timestamp(valid_dates.max())) if not valid_dates.empty else None,
    }


def _loader_analysis(df: pd.DataFrame) -> tuple[dict[str, Any], dict[str, str], list[str]]:
    resolved, missing = photo_loader.resolve_columns(df)
    contract = {
        "status": "ACTIVE",
        "grain_contract": dict(photo_loader.GRAIN_CONTRACT),
    }
    analysis = photo_loader.analyze_photo_dataframe(
        df,
        contract=contract,
        expected=_photo_expected(df, resolved),
        source_file="controlled_local_json_fixture",
        source_file_sha256=None,
        sheet_name="Fotos",
    )
    return analysis, resolved, missing


def _photo_count_anomalies(df: pd.DataFrame, photo_count_col: str | None) -> dict[str, int]:
    if not photo_count_col:
        return {
            "missing_or_invalid_total_rows": int(len(df)),
            "nonpositive_total_rows": 0,
            "sequence_above_total_rows": 0,
            "exact_duplicate_excess_rows": int(df.duplicated(keep="first").sum()),
        }

    text = df[photo_count_col].astype("string").fillna("").str.strip()
    totals = photo_loader.parse_total_from_photo_count(df[photo_count_col])
    parts = text.str.extract(r"^\s*(\d+)\s*/\s*(\d+)\s*$")
    sequence = pd.to_numeric(parts[0], errors="coerce")
    sequence_total = pd.to_numeric(parts[1], errors="coerce")
    return {
        "missing_or_invalid_total_rows": int(totals.isna().sum()),
        "nonpositive_total_rows": int((totals.fillna(1) <= 0).sum()),
        "sequence_above_total_rows": int(
            ((sequence.notna()) & (sequence_total.notna()) & (sequence > sequence_total)).sum()
        ),
        "exact_duplicate_excess_rows": int(df.duplicated(keep="first").sum()),
    }


def _structural_photo_grain(
    df: pd.DataFrame,
    analysis: dict[str, Any],
    resolved: dict[str, str],
    missing: list[str],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, int], dict[str, bool]]:
    metrics = analysis.get("metrics") or {}
    loader_flags = analysis.get("flags") or {}
    empty_counts = {
        "photo_blank_cod_rt_rows": int(len(df)),
        "photo_blank_cliente_norm_rows": int(len(df)),
    }
    anomaly_counts = _photo_count_anomalies(df, resolved.get("photo_count"))

    if missing:
        invariant = {
            "chain": "photo_row -> event_row -> day_presence",
            "missing_required_columns": missing,
            "photo_rows": int(len(df)),
            "event_rows": 0,
            "day_presence_rows": 0,
            "photo_rows_equal_event_rows": False,
            "legacy_aggregate_inequality_used_as_proof": False,
            "day_presence_constant_check_used_as_proof": False,
            "photo_row_to_event_row_structural_pass": False,
            "event_row_to_day_presence_structural_pass": False,
            "day_presence_records": [],
        }
        structural_flags = {
            "loader_structure_usable": False,
            "photo_row_to_event_row_structural_pass": False,
            "event_row_to_day_presence_structural_pass": False,
            "no_blank_photo_cod_rt": False,
            "no_blank_photo_cliente_norm": False,
            "no_missing_photo_count_total": anomaly_counts["missing_or_invalid_total_rows"] == 0,
            "positive_photo_count_totals": anomaly_counts["nonpositive_total_rows"] == 0,
            "photo_sequence_not_above_total": anomaly_counts["sequence_above_total_rows"] == 0,
            "no_exact_duplicate_photo_rows": anomaly_counts["exact_duplicate_excess_rows"] == 0,
        }
        return invariant, [], empty_counts | anomaly_counts, structural_flags

    work = df.copy()
    work["_event_id"] = work[resolved["event_id"]].astype("string").fillna("").str.strip()
    work["_sp_item_id"] = work[resolved["sp_item_id"]].astype("string").fillna("").str.strip()
    work["_fecha"] = pd.to_datetime(work[resolved["fecha"]], errors="coerce").dt.normalize()
    work["_week_start"] = work["_fecha"] - pd.to_timedelta(work["_fecha"].dt.weekday, unit="D")
    work["_cod_rt"] = work[resolved["cod_rt"]].map(_clean_key)
    work["_cliente_norm"] = work[resolved["cliente_norm"]].map(_clean_key)

    key_counts = {
        "photo_blank_cod_rt_rows": int((work["_cod_rt"] == "").sum()),
        "photo_blank_cliente_norm_rows": int((work["_cliente_norm"] == "").sum()),
    }
    valid = work[work["_event_id"] != ""].copy()
    events = (
        valid.groupby("_event_id", dropna=False)
        .agg(
            photo_rows=("_event_id", "size"),
            sp_item_values=("_sp_item_id", lambda values: values.nunique(dropna=False)),
            date_values=("_fecha", lambda values: values.nunique(dropna=False)),
            week_values=("_week_start", lambda values: values.nunique(dropna=False)),
            cod_rt_values=("_cod_rt", lambda values: values.nunique(dropna=False)),
            cliente_values=("_cliente_norm", lambda values: values.nunique(dropna=False)),
            fecha=("_fecha", "first"),
            week_start=("_week_start", "first"),
            cod_rt=("_cod_rt", "first"),
            cliente_norm=("_cliente_norm", "first"),
        )
        .reset_index()
    )
    photo_to_event_pass = bool(
        len(valid) == len(work)
        and int(events["photo_rows"].sum()) == len(work)
        and len(events) == int(metrics.get("distinct_event_ids", -1))
    )
    event_ready = (
        (events["sp_item_values"] == 1)
        & (events["date_values"] == 1)
        & (events["week_values"] == 1)
        & (events["cod_rt_values"] == 1)
        & (events["cliente_values"] == 1)
        & events["fecha"].notna()
        & (events["cod_rt"] != "")
        & (events["cliente_norm"] != "")
    )
    event_to_presence_pass = bool(event_ready.all() and len(events) > 0)
    ready_events = events[event_ready].copy()
    day_presence = (
        ready_events.groupby(
            ["week_start", "fecha", "cod_rt", "cliente_norm"],
            dropna=False,
        )
        .agg(event_rows=("_event_id", "size"))
        .reset_index()
    )
    records: list[dict[str, Any]] = []
    for row in day_presence.itertuples(index=False):
        records.append(
            {
                "semana_inicio": _date_iso(pd.Timestamp(row.week_start)),
                "fecha": _date_iso(pd.Timestamp(row.fecha)),
                "cod_rt": str(row.cod_rt),
                "cliente_norm": str(row.cliente_norm),
                "event_rows": int(row.event_rows),
                "presence": 1,
            }
        )

    invariant = {
        "chain": "photo_row -> event_row -> day_presence",
        "missing_required_columns": [],
        "photo_rows": int(len(work)),
        "event_rows": int(len(events)),
        "day_presence_rows": int(len(day_presence)),
        "max_events_per_day_presence": (
            int(day_presence["event_rows"].max()) if not day_presence.empty else 0
        ),
        "photo_rows_equal_event_rows": bool(len(work) == len(events)),
        "legacy_aggregate_inequality_observed": bool(len(work) != len(events)),
        "legacy_aggregate_inequality_used_as_proof": False,
        "day_presence_constant_check_used_as_proof": False,
        "photo_row_to_event_row_structural_pass": photo_to_event_pass,
        "event_row_to_day_presence_structural_pass": event_to_presence_pass,
        "day_presence_records": records,
        "imported_loader_advisory": {
            "verdict": analysis.get("verdict"),
            "legacy_forbidden_assumption_rejected": loader_flags.get(
                "forbidden_assumption_rejected"
            ),
            "legacy_day_presence_is_binary": loader_flags.get("day_presence_is_binary"),
            "legacy_flags_used_as_blocking_proof": False,
        },
    }
    structural_flags = {
        "loader_structure_usable": not missing,
        "photo_row_to_event_row_structural_pass": photo_to_event_pass,
        "event_row_to_day_presence_structural_pass": event_to_presence_pass,
        "no_blank_photo_cod_rt": key_counts["photo_blank_cod_rt_rows"] == 0,
        "no_blank_photo_cliente_norm": key_counts["photo_blank_cliente_norm_rows"] == 0,
        "no_missing_photo_count_total": anomaly_counts["missing_or_invalid_total_rows"] == 0,
        "positive_photo_count_totals": anomaly_counts["nonpositive_total_rows"] == 0,
        "photo_sequence_not_above_total": anomaly_counts["sequence_above_total_rows"] == 0,
        "no_exact_duplicate_photo_rows": anomaly_counts["exact_duplicate_excess_rows"] == 0,
    }
    for flag_name in LOADER_STRUCTURAL_FLAGS:
        structural_flags[f"loader_{flag_name}"] = bool(loader_flags.get(flag_name, False))
    return invariant, records, key_counts | anomaly_counts, structural_flags


def _prepare_route_rows(
    route_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, bool]]:
    prepared: list[dict[str, Any]] = []
    counts = {
        "route_blank_cod_rt_rows": 0,
        "route_blank_cliente_norm_rows": 0,
        "route_invalid_week_rows": 0,
        "route_non_monday_week_rows": 0,
        "route_invalid_exigidas_rows": 0,
        "route_visita_mismatch_rows": 0,
        "route_existing_presence_outside_week_rows": 0,
        "duplicate_route_grain_rows": 0,
    }

    for source in route_rows:
        week = _timestamp(source.get("semana_inicio"))
        cod_rt = _clean_key(source.get("cod_rt"))
        cliente_norm = _clean_key(source.get("cliente_norm"))
        exigidas = _nonnegative_int(source.get("EXIGIDAS"))
        visita_supplied = source.get("VISITA")
        visita = _nonnegative_int(visita_supplied) if visita_supplied is not None else exigidas
        if not cod_rt:
            counts["route_blank_cod_rt_rows"] += 1
        if not cliente_norm:
            counts["route_blank_cliente_norm_rows"] += 1
        if week is None:
            counts["route_invalid_week_rows"] += 1
        elif week.weekday() != 0:
            counts["route_non_monday_week_rows"] += 1
        if exigidas is None:
            counts["route_invalid_exigidas_rows"] += 1
        if visita != exigidas:
            counts["route_visita_mismatch_rows"] += 1

        existing_dates: set[str] = set()
        raw_dates = source.get("existing_presence_dates", [])
        if not isinstance(raw_dates, list):
            raw_dates = []
            counts["route_existing_presence_outside_week_rows"] += 1
        for raw_date in raw_dates:
            date_value = _timestamp(raw_date)
            if (
                date_value is None
                or week is None
                or _week_start(date_value) != week
            ):
                counts["route_existing_presence_outside_week_rows"] += 1
                continue
            existing_dates.add(_date_iso(date_value) or "")

        if week is not None and exigidas is not None:
            prepared.append(
                {
                    "semana_inicio": _date_iso(week),
                    "cod_rt": cod_rt,
                    "cliente_norm": cliente_norm,
                    "EXIGIDAS": exigidas,
                    "existing_presence_dates": existing_dates,
                }
            )

    keys = [
        (row["semana_inicio"], row["cod_rt"], row["cliente_norm"])
        for row in prepared
    ]
    counts["duplicate_route_grain_rows"] = len(keys) - len(set(keys))
    flags = {
        "route_rows_present": len(route_rows) > 0,
        "no_blank_route_cod_rt": counts["route_blank_cod_rt_rows"] == 0,
        "no_blank_route_cliente_norm": counts["route_blank_cliente_norm_rows"] == 0,
        "valid_route_week_start": counts["route_invalid_week_rows"] == 0,
        "route_week_start_is_monday": counts["route_non_monday_week_rows"] == 0,
        "valid_route_exigidas": counts["route_invalid_exigidas_rows"] == 0,
        "route_visita_matches_exigidas": counts["route_visita_mismatch_rows"] == 0,
        "existing_presence_within_route_week": (
            counts["route_existing_presence_outside_week_rows"] == 0
        ),
        "unique_route_grain": counts["duplicate_route_grain_rows"] == 0,
    }
    return prepared, counts, flags


def _metrics(exigidas: int, presence_dates: set[str]) -> dict[str, Any]:
    raw = len(presence_dates)
    cap = min(raw, exigidas)
    return {
        "EXIGIDAS": exigidas,
        "VISITA": exigidas,
        "VISITA_REALIZADA": raw,
        "VISITA_REALIZADA_RAW": raw,
        "VISITA_REALIZADA_CAP": cap,
        "PENDIENTE": max(exigidas - cap, 0),
        "ALERTA": "CUMPLE" if raw >= exigidas else "INCUMPLE",
    }


def _delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    delta: dict[str, Any] = {}
    for metric in PROTECTED_METRICS:
        if metric == "ALERTA":
            delta[metric] = {
                "before": before[metric],
                "after": after[metric],
                "changed": before[metric] != after[metric],
            }
        else:
            delta[metric] = int(after[metric]) - int(before[metric])
    return delta


def _reconcile(
    route_rows: list[dict[str, Any]],
    photo_presence: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, bool]]:
    photo_by_route: dict[tuple[str, str, str], set[str]] = {}
    for row in photo_presence:
        key = (row["semana_inicio"], row["cod_rt"], row["cliente_norm"])
        photo_by_route.setdefault(key, set()).add(row["fecha"])

    route_keys = {
        (row["semana_inicio"], row["cod_rt"], row["cliente_norm"])
        for row in route_rows
    }
    unmatched = [
        {
            "semana_inicio": key[0],
            "cod_rt": key[1],
            "cliente_norm": key[2],
            "dates": sorted(dates),
        }
        for key, dates in sorted(photo_by_route.items())
        if key not in route_keys
    ]

    rows: list[dict[str, Any]] = []
    for route in route_rows:
        key = (
            route["semana_inicio"],
            route["cod_rt"],
            route["cliente_norm"],
        )
        before_dates = set(route["existing_presence_dates"])
        after_dates = before_dates | photo_by_route.get(key, set())
        before = _metrics(route["EXIGIDAS"], before_dates)
        after = _metrics(route["EXIGIDAS"], after_dates)
        delta = _delta(before, after)
        changed_metrics = [
            metric
            for metric in PROTECTED_METRICS
            if (
                delta[metric]["changed"]
                if metric == "ALERTA"
                else delta[metric] != 0
            )
        ]
        rows.append(
            {
                "grain": {
                    "semana_inicio": key[0],
                    "cod_rt": key[1],
                    "cliente_norm": key[2],
                },
                "before_presence_dates": sorted(before_dates),
                "route_b_presence_dates": sorted(photo_by_route.get(key, set())),
                "after_presence_dates": sorted(after_dates),
                "before": before,
                "after": after,
                "delta": delta,
                "changed_metrics": changed_metrics,
            }
        )

    denominator_delta_zero = all(
        row["delta"]["EXIGIDAS"] == 0 and row["delta"]["VISITA"] == 0
        for row in rows
    )
    only_numerator_metrics_changed = all(
        set(row["changed_metrics"]).issubset(NUMERATOR_METRICS)
        for row in rows
    )
    route_rows_before = len(route_rows)
    route_rows_after = len(route_rows)
    reconciliation = {
        "scope": "controlled_local_sample_only",
        "grain": ["semana_inicio", "cod_rt", "cliente_norm"],
        "protected_metrics": PROTECTED_METRICS,
        "denominator_metrics": DENOMINATOR_METRICS,
        "numerator_metrics": NUMERATOR_METRICS,
        "route_rows_before": route_rows_before,
        "route_rows_after": route_rows_after,
        "photo_day_presence_rows": len(photo_presence),
        "unmatched_photo_day_presence": unmatched,
        "denominator_delta_zero": denominator_delta_zero,
        "only_numerator_metrics_changed": only_numerator_metrics_changed,
        "rows": rows,
    }
    flags = {
        "no_unmatched_photo_day_presence": not unmatched,
        "route_row_count_unchanged": route_rows_before == route_rows_after,
        "denominator_delta_zero": denominator_delta_zero,
        "only_numerator_metrics_changed": only_numerator_metrics_changed,
    }
    return reconciliation, flags


def validate_local_sample(payload: dict[str, Any]) -> dict[str, Any]:
    route_input = payload.get("route_rows")
    photo_input = payload.get("photo_rows")
    if not isinstance(route_input, list) or not all(
        isinstance(row, dict) for row in route_input
    ):
        raise LocalInputError("route_rows must be a list of JSON objects")
    if not isinstance(photo_input, list) or not all(
        isinstance(row, dict) for row in photo_input
    ):
        raise LocalInputError("photo_rows must be a list of JSON objects")

    photo_df = pd.DataFrame(photo_input)
    analysis, resolved, missing = _loader_analysis(photo_df)
    invariant, photo_presence, photo_counts, photo_flags = _structural_photo_grain(
        photo_df,
        analysis,
        resolved,
        missing,
    )
    routes, route_counts, route_flags = _prepare_route_rows(route_input)
    reconciliation, reconciliation_flags = _reconcile(routes, photo_presence)

    flags = {
        **photo_flags,
        **route_flags,
        **reconciliation_flags,
        "no_blank_cod_rt": (
            photo_counts["photo_blank_cod_rt_rows"] == 0
            and route_counts["route_blank_cod_rt_rows"] == 0
        ),
        "no_blank_cliente_norm": (
            photo_counts["photo_blank_cliente_norm_rows"] == 0
            and route_counts["route_blank_cliente_norm_rows"] == 0
        ),
    }
    blocking_reasons = sorted(name for name, passed in flags.items() if not passed)
    return {
        "verdict": PASS_VERDICT if not blocking_reasons else BLOCK_VERDICT,
        "mode": MODE,
        "db_access": {"used": False},
        "sql_apply": False,
        "writes_executed": False,
        "metrics_definition": METRICS_DEFINITION,
        "grain_invariant": invariant,
        "denominator_reconciliation": reconciliation,
        "blocking_flags": {
            **flags,
            "counts": photo_counts | route_counts,
            "blocking_reasons": blocking_reasons,
        },
    }


def _safe_error_payload(exc: BaseException) -> dict[str, Any]:
    return {
        "verdict": BLOCK_VERDICT,
        "mode": MODE,
        "db_access": {"used": False},
        "sql_apply": False,
        "writes_executed": False,
        "metrics_definition": METRICS_DEFINITION,
        "grain_invariant": {
            "chain": "photo_row -> event_row -> day_presence",
            "structural_validation_completed": False,
        },
        "denominator_reconciliation": {
            "scope": "controlled_local_sample_only",
            "completed": False,
        },
        "blocking_flags": {
            "input_valid": False,
            "blocking_reasons": ["input_valid"],
        },
        "error": f"{exc.__class__.__name__}: {exc}",
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate Route B denominator integration from a local JSON fixture."
    )
    parser.add_argument(
        "--input-json",
        required=True,
        help="Local JSON with route_rows and photo_rows. The validator only reads this file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        source = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
        if not isinstance(source, dict):
            raise LocalInputError("input JSON root must be an object")
        result = validate_local_sample(source)
    except BaseException as exc:
        result = _safe_error_payload(exc)
    print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    return 0 if result["verdict"] == PASS_VERDICT else 1


if __name__ == "__main__":
    raise SystemExit(main())
