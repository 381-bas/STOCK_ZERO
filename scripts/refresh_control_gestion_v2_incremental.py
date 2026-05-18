#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
import json
import time
from typing import Any


PHASE = "FASE_9C5N_N2C_CONTROL_GESTION_INCREMENTAL_HELPER_EXPLICIT_ROUTE_WEEK_AND_PREAPPLY_DRYRUN_PATCH"
DEFAULT_STATEMENT_TIMEOUT_SECONDS = 1800
REAL_APPLY_ENABLED = False

DAILY_SOURCE = "cg_core.v_cg_visita_dia_precedencia_v2"
DAILY_FACT = "cg_mart.fact_cg_visita_dia_resuelta_v2"
WEEKLY_FREQ = "cg_core.v_rr_frecuencia_base_resuelta_v2"
WEEKLY_MV = "cg_mart.mv_cg_out_weekly_v2"


def _now_ms() -> int:
    return int(time.perf_counter() * 1000)


def ensure_sslmode(db_url: str) -> str:
    if not db_url:
        return db_url
    if "sslmode=" in db_url:
        return db_url
    return db_url + ("&sslmode=require" if "?" in db_url else "?sslmode=require")


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


def week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def sorted_iso(values: set[date]) -> list[str]:
    return [value.isoformat() for value in sorted(values)]


def build_week_scope(
    affected_dates: set[date],
    explicit_weeks: set[date],
    safety_window_weeks: int,
) -> dict[str, set[date]]:
    derived_weeks = {week_start(value) for value in affected_dates}
    explicit_affected_weeks = {week_start(value) for value in explicit_weeks}
    strict_weeks = derived_weeks | explicit_affected_weeks

    safety_weeks: set[date] = set()
    for base_week in strict_weeks:
        for offset in range(1, safety_window_weeks + 1):
            safety_weeks.add(base_week - timedelta(days=7 * offset))
    safety_weeks -= strict_weeks

    return {
        "requested_affected_dates": affected_dates,
        "explicit_affected_weeks": explicit_affected_weeks,
        "derived_affected_weeks": derived_weeks,
        "strict_affected_weeks": strict_weeks,
        "safety_weeks": safety_weeks,
        "validation_weeks": strict_weeks | safety_weeks,
    }


def build_week_origin_by_week(week_scope: dict[str, set[date]]) -> dict[str, str]:
    origins: dict[str, str] = {}
    for week_value in sorted(week_scope["validation_weeks"]):
        labels = []
        if week_value in week_scope["derived_affected_weeks"]:
            labels.append("derived")
        if week_value in week_scope["explicit_affected_weeks"]:
            labels.append("explicit")
        if not labels and week_value in week_scope["safety_weeks"]:
            labels.append("safety")
        origins[week_value.isoformat()] = "+".join(labels) if labels else "unknown"
    return origins


def _statement_timeout_ms(statement_timeout_seconds: int) -> int:
    seconds = int(statement_timeout_seconds)
    if seconds <= 0:
        return 0
    return seconds * 1000


def _fetch_relation_exists(cur, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation_name,))
    row = cur.fetchone()
    return bool(row and row[0])


def _metric_row(row: dict[str, Any], metrics: list[str]) -> dict[str, int]:
    return {metric: int(row.get(metric) or 0) for metric in metrics}


def _sum_metric_rows(rows: list[dict[str, Any]], metrics: list[str]) -> dict[str, int]:
    totals = {metric: 0 for metric in metrics}
    for row in rows:
        for metric in metrics:
            totals[metric] += int(row.get(metric) or 0)
    return totals


def _diff_rows(
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    *,
    key_name: str,
    metrics: list[str],
) -> dict[str, Any]:
    left_by_key = {str(row[key_name]): _metric_row(row, metrics) for row in left_rows}
    right_by_key = {str(row[key_name]): _metric_row(row, metrics) for row in right_rows}
    keys = sorted(set(left_by_key) | set(right_by_key))

    diffs: list[dict[str, Any]] = []
    totals = {f"{metric}_diff": 0 for metric in metrics}
    max_abs_diff = 0
    for key in keys:
        item: dict[str, Any] = {key_name: key}
        has_diff = False
        for metric in metrics:
            diff_value = right_by_key.get(key, {}).get(metric, 0) - left_by_key.get(key, {}).get(metric, 0)
            item[f"{metric}_diff"] = diff_value
            totals[f"{metric}_diff"] += diff_value
            if diff_value != 0:
                has_diff = True
                max_abs_diff = max(max_abs_diff, abs(diff_value))
        if has_diff:
            diffs.append(item)

    return {
        "ok": not diffs and all(value == 0 for value in totals.values()),
        "diff_rows_count": len(diffs),
        "max_abs_diff": max_abs_diff,
        "total_diffs": totals,
        "diff_rows": diffs[:20],
    }


def _daily_stats_query(source_relation: str) -> str:
    return f"""
    WITH affected_dates AS (
        SELECT unnest(%s::date[]) AS fecha_visita
    )
    SELECT
        v.fecha_visita::date AS fecha_visita,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(v.useful_day), 0)::bigint AS useful_day,
        COALESCE(SUM(v.tiene_kpione2), 0)::bigint AS tiene_kpione2,
        COALESCE(SUM(v.tiene_power_app), 0)::bigint AS tiene_power_app,
        COALESCE(SUM(v.kpione1_audit_only), 0)::bigint AS kpione1_audit_only,
        COALESCE(SUM(v.power_app_fallback), 0)::bigint AS power_app_fallback,
        COALESCE(SUM(v.raw_evidence_count), 0)::bigint AS raw_evidence_count,
        COUNT(*) FILTER (WHERE v.fuente_ganadora = 'KPIONE2')::bigint AS kpione2_winner_rows,
        COUNT(*) FILTER (WHERE v.fuente_ganadora = 'POWER_APP')::bigint AS power_app_winner_rows,
        COUNT(*) FILTER (WHERE v.fuente_ganadora IS NULL)::bigint AS no_winner_rows
    FROM {source_relation} v
    JOIN affected_dates ad
      ON ad.fecha_visita = v.fecha_visita
    GROUP BY v.fecha_visita::date
    ORDER BY v.fecha_visita::date
    """


def _daily_week_coverage_query(source_relation: str) -> str:
    return f"""
    WITH affected_weeks AS (
        SELECT unnest(%s::date[]) AS semana_inicio
    )
    SELECT
        date_trunc('week', v.fecha_visita)::date AS semana_inicio,
        COUNT(*)::bigint AS rows
    FROM {source_relation} v
    JOIN affected_weeks aw
      ON aw.semana_inicio = date_trunc('week', v.fecha_visita)::date
    GROUP BY date_trunc('week', v.fecha_visita)::date
    ORDER BY date_trunc('week', v.fecha_visita)::date
    """


def _route_scope_query() -> str:
    return f"""
    WITH affected_weeks AS (
        SELECT unnest(%s::date[]) AS semana_inicio
    )
    SELECT
        f.effective_week_start::date AS semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(COALESCE(f.visitas_exigidas_semana, 0)), 0)::bigint AS visita
    FROM {WEEKLY_FREQ} f
    JOIN affected_weeks aw
      ON aw.semana_inicio = f.effective_week_start
    GROUP BY f.effective_week_start::date
    ORDER BY f.effective_week_start::date
    """


def _weekly_candidate_query() -> str:
    return f"""
    WITH affected_weeks AS (
        SELECT unnest(%s::date[]) AS semana_inicio
    ),
    base AS MATERIALIZED (
        SELECT
            f.effective_week_start,
            f.effective_week_iso,
            f.ruta_batch_id,
            f.cod_rt,
            f.cod_b2b,
            f.local_nombre,
            f.cliente,
            f.cliente_norm,
            f.gestor,
            f.supervisor,
            f.rutero,
            f.reponedor_scope,
            f.modalidad,
            f.visitas_exigidas_semana,
            f.ruta_duplicada_flag,
            f.ruta_duplicada_rows
        FROM {WEEKLY_FREQ} f
        JOIN affected_weeks aw
          ON aw.semana_inicio = f.effective_week_start
    ),
    row_level AS (
        SELECT
            b.effective_week_start::date AS semana_inicio,
            COALESCE(b.visitas_exigidas_semana, 0)::bigint AS visita,
            COALESCE(SUM(d.useful_day), 0)::bigint AS visita_realizada_raw,
            LEAST(
                COALESCE(SUM(d.useful_day), 0)::bigint,
                COALESCE(b.visitas_exigidas_semana, 0)::bigint
            ) AS visita_realizada_cap,
            COALESCE(b.ruta_duplicada_flag, 0)::integer AS ruta_duplicada_flag,
            COALESCE(b.ruta_duplicada_rows, 0)::integer AS ruta_duplicada_rows,
            b.gestor,
            b.rutero
        FROM base b
        LEFT JOIN {DAILY_FACT} d
          ON d.cod_rt = b.cod_rt
         AND d.cliente_norm = b.cliente_norm
         AND d.semana_inicio = b.effective_week_start
        GROUP BY
            b.effective_week_start,
            b.effective_week_iso,
            b.ruta_batch_id,
            b.cod_rt,
            b.cod_b2b,
            b.local_nombre,
            b.cliente,
            b.cliente_norm,
            b.gestor,
            b.supervisor,
            b.rutero,
            b.reponedor_scope,
            b.modalidad,
            b.visitas_exigidas_semana,
            b.ruta_duplicada_flag,
            b.ruta_duplicada_rows
    )
    SELECT
        semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(visita), 0)::bigint AS visita,
        COALESCE(SUM(visita_realizada_raw), 0)::bigint AS visita_realizada_raw,
        COALESCE(SUM(visita_realizada_cap), 0)::bigint AS visita_realizada_cap,
        COALESCE(SUM(GREATEST(visita - visita_realizada_cap, 0)), 0)::bigint AS visitas_pendientes_calc,
        COUNT(*) FILTER (WHERE visita_realizada_raw >= visita)::bigint AS cumple_rows,
        COUNT(*) FILTER (WHERE visita_realizada_raw < visita)::bigint AS incumple_rows,
        COALESCE(SUM(CASE
            WHEN ruta_duplicada_flag = 1
              OR ruta_duplicada_rows > 1
              OR CAST(gestor AS text) LIKE '%%|%%'
              OR CAST(rutero AS text) LIKE '%%|%%'
            THEN 1 ELSE 0 END), 0)::bigint AS gestion_compartida_rows
    FROM row_level
    GROUP BY semana_inicio
    ORDER BY semana_inicio
    """


def _weekly_mv_query() -> str:
    return f"""
    WITH affected_weeks AS (
        SELECT unnest(%s::date[]) AS semana_inicio
    )
    SELECT
        m."SEMANA_INICIO"::date AS semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(COALESCE(m."VISITA", 0)), 0)::bigint AS visita,
        COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
        COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
        COALESCE(SUM(COALESCE(
            m."VISITAS_PENDIENTES_CALC",
            GREATEST(COALESCE(m."VISITA", 0) - COALESCE(m."VISITA_REALIZADA_CAP", 0), 0)
        )), 0)::bigint AS visitas_pendientes_calc,
        COUNT(*) FILTER (
            WHERE COALESCE(m."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(m."ALERTA" AS text), '')))) = 'CUMPLE'
        )::bigint AS cumple_rows,
        COUNT(*) FILTER (
            WHERE COALESCE(m."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(m."ALERTA" AS text), '')))) = 'INCUMPLE'
        )::bigint AS incumple_rows,
        COALESCE(SUM(COALESCE(
            m."GESTION_COMPARTIDA_FLAG_CALC",
            CASE
                WHEN COALESCE(m."RUTA_DUPLICADA_FLAG", 0) = 1
                  OR COALESCE(m."RUTA_DUPLICADA_ROWS", 0) > 1
                  OR CAST(m."GESTOR" AS text) LIKE '%%|%%'
                  OR CAST(m."RUTERO" AS text) LIKE '%%|%%'
                THEN 1 ELSE 0
            END
        )), 0)::bigint AS gestion_compartida_rows
    FROM {WEEKLY_MV} m
    JOIN affected_weeks aw
      ON aw.semana_inicio = m."SEMANA_INICIO"
    GROUP BY m."SEMANA_INICIO"::date
    ORDER BY m."SEMANA_INICIO"::date
    """


def _fetch_dicts(cur, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cur.execute(query, params)
    keys = [desc[0] for desc in cur.description]
    return [dict(zip(keys, row)) for row in cur.fetchall()]


def _rows_by_week(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {str(row["semana_inicio"]): int(row.get("rows") or 0) for row in rows}


def _route_scope_by_week(cur, validation_weeks: list[str], week_origin_by_week: dict[str, str]) -> list[dict[str, Any]]:
    if not validation_weeks:
        return []
    route_rows = {
        str(row["semana_inicio"]): row
        for row in _fetch_dicts(cur, _route_scope_query(), (validation_weeks,))
    }
    scoped: list[dict[str, Any]] = []
    for week_value in validation_weeks:
        row = route_rows.get(week_value, {})
        rows = int(row.get("rows") or 0)
        visita = int(row.get("visita") or 0)
        scoped.append({
            "semana_inicio": week_value,
            "rows": rows,
            "visita": visita,
            "scope_status": "present" if rows > 0 else "missing",
            "week_origin": week_origin_by_week.get(week_value, "unknown"),
        })
    return scoped


def _run_daily_check(cur, affected_dates: list[str], validate: bool) -> dict[str, Any]:
    metrics = [
        "rows",
        "useful_day",
        "tiene_kpione2",
        "tiene_power_app",
        "kpione1_audit_only",
        "power_app_fallback",
        "raw_evidence_count",
        "kpione2_winner_rows",
        "power_app_winner_rows",
        "no_winner_rows",
    ]
    source_started_ms = _now_ms()
    source_rows = _fetch_dicts(cur, _daily_stats_query(DAILY_SOURCE), (affected_dates,))
    source_elapsed_ms = _now_ms() - source_started_ms
    source_totals = _sum_metric_rows(source_rows, metrics)

    result: dict[str, Any] = {
        "source": DAILY_SOURCE,
        "fact": DAILY_FACT,
        "source_rows_by_date": source_rows,
        "source_totals": source_totals,
        "source_elapsed_ms": source_elapsed_ms,
        "validation_status": "skipped" if not validate else "pending",
    }
    if not validate:
        return result

    if not _fetch_relation_exists(cur, DAILY_FACT):
        result["validation_status"] = "fact_missing"
        result["fact_available"] = False
        return result

    fact_started_ms = _now_ms()
    fact_rows = _fetch_dicts(cur, _daily_stats_query(DAILY_FACT), (affected_dates,))
    fact_elapsed_ms = _now_ms() - fact_started_ms
    result["fact_available"] = True
    result["fact_rows_by_date"] = fact_rows
    result["fact_totals"] = _sum_metric_rows(fact_rows, metrics)
    result["fact_elapsed_ms"] = fact_elapsed_ms
    result["diff"] = _diff_rows(source_rows, fact_rows, key_name="fecha_visita", metrics=metrics)
    result["validation_status"] = "ok" if result["diff"]["ok"] else "diff"
    return result


def _daily_fact_coverage_by_week(
    cur,
    validation_weeks: list[str],
    week_origin_by_week: dict[str, str],
) -> tuple[list[dict[str, Any]], bool]:
    if not validation_weeks:
        return [], _fetch_relation_exists(cur, DAILY_FACT)

    fact_available = _fetch_relation_exists(cur, DAILY_FACT)
    source_rows = _rows_by_week(_fetch_dicts(cur, _daily_week_coverage_query(DAILY_SOURCE), (validation_weeks,)))
    fact_rows: dict[str, int] = {}
    if fact_available:
        fact_rows = _rows_by_week(_fetch_dicts(cur, _daily_week_coverage_query(DAILY_FACT), (validation_weeks,)))

    coverage: list[dict[str, Any]] = []
    for week_value in validation_weeks:
        source_count = int(source_rows.get(week_value, 0))
        fact_count = int(fact_rows.get(week_value, 0))
        if not fact_available:
            coverage_status = "unknown"
        elif fact_count == 0:
            coverage_status = "empty"
        elif source_count == fact_count:
            coverage_status = "complete"
        else:
            coverage_status = "partial"
        coverage.append({
            "semana_inicio": week_value,
            "source_rows": source_count,
            "daily_fact_rows": fact_count,
            "coverage_status": coverage_status,
            "week_origin": week_origin_by_week.get(week_value, "unknown"),
        })
    return coverage, fact_available


def _run_weekly_check(
    cur,
    validation_weeks: list[str],
    derived_weeks: set[date],
    explicit_weeks: set[date],
    safety_weeks: set[date],
    week_origin_by_week: dict[str, str],
    validate: bool,
    require_complete_safety_window: bool,
    post_apply_validate: bool,
) -> dict[str, Any]:
    metrics = [
        "rows",
        "visita",
        "visita_realizada_raw",
        "visita_realizada_cap",
        "visitas_pendientes_calc",
        "cumple_rows",
        "incumple_rows",
        "gestion_compartida_rows",
    ]
    result: dict[str, Any] = {
        "candidate_source": f"{WEEKLY_FREQ} + {DAILY_FACT}",
        "comparison_target": WEEKLY_MV,
        "validation_status": "skipped" if not validate else "pending",
        "derived_weeks_ok": True,
        "explicit_weeks_ok": True,
        "safety_weeks_skipped": [],
        "safety_weeks_with_warnings": [],
        "expected_pre_apply_diff_weeks": [],
        "warnings": [],
    }

    coverage, fact_available = _daily_fact_coverage_by_week(cur, validation_weeks, week_origin_by_week)
    result["daily_fact_available"] = fact_available
    result["daily_fact_coverage_by_week"] = coverage
    if not fact_available:
        result["validation_status"] = "daily_fact_missing"
        result["derived_weeks_ok"] = False
        result["blocking"] = True
        return result

    safety_week_values = {value.isoformat() for value in safety_weeks}
    derived_week_values = {value.isoformat() for value in derived_weeks}
    explicit_week_values = {value.isoformat() for value in explicit_weeks}
    coverage_by_week = {row["semana_inicio"]: row for row in coverage}
    incomplete_derived = [
        week_value for week_value in sorted(derived_week_values)
        if coverage_by_week.get(week_value, {}).get("coverage_status") != "complete"
    ]
    incomplete_safety = [
        week_value for week_value in sorted(safety_week_values)
        if coverage_by_week.get(week_value, {}).get("coverage_status") != "complete"
    ]

    result["safety_weeks_skipped"] = [] if require_complete_safety_window else incomplete_safety
    if incomplete_derived:
        result["warnings"].append("derived_week_incomplete_daily_fact:" + ",".join(incomplete_derived))
        if post_apply_validate:
            result["derived_weeks_ok"] = False
    if incomplete_safety:
        warning_key = "safety_week_incomplete_daily_fact:" + ",".join(incomplete_safety)
        result["warnings"].append(warning_key)
        if require_complete_safety_window:
            result["safety_weeks_with_warnings"] = incomplete_safety

    route_scope = _route_scope_by_week(cur, validation_weeks, week_origin_by_week)
    result["route_scope_by_week"] = route_scope
    route_scope_by_week = {row["semana_inicio"]: row for row in route_scope}
    missing_explicit_route = [
        week_value for week_value in sorted(explicit_week_values)
        if route_scope_by_week.get(week_value, {}).get("scope_status") != "present"
    ]
    if missing_explicit_route:
        result["explicit_weeks_ok"] = False
        result["warnings"].append("explicit_week_no_route_scope:" + ",".join(missing_explicit_route))

    weeks_to_compare = set(derived_week_values | explicit_week_values)
    complete_safety = sorted(safety_week_values - set(incomplete_safety))
    weeks_to_compare.update(complete_safety)
    compare_week_values = sorted(weeks_to_compare)
    result["compared_weeks"] = compare_week_values

    if not validate:
        result["validation_status"] = "skipped"
        result["blocking"] = bool(missing_explicit_route)
        result["warning_only"] = bool(incomplete_safety and not require_complete_safety_window)
        return result

    if missing_explicit_route or (post_apply_validate and incomplete_derived) or (incomplete_safety and require_complete_safety_window):
        result["validation_status"] = "error"
        result["blocking"] = True
        return result

    if not compare_week_values:
        result["validation_status"] = "skipped_incomplete_daily_fact"
        result["blocking"] = False
        result["warning_only"] = bool(incomplete_safety)
        return result

    candidate_started_ms = _now_ms()
    candidate_rows = _fetch_dicts(cur, _weekly_candidate_query(), (compare_week_values,))
    candidate_elapsed_ms = _now_ms() - candidate_started_ms
    result["candidate_rows_by_week"] = candidate_rows
    result["candidate_totals"] = _sum_metric_rows(candidate_rows, metrics)
    result["candidate_elapsed_ms"] = candidate_elapsed_ms

    mv_started_ms = _now_ms()
    mv_rows = _fetch_dicts(cur, _weekly_mv_query(), (compare_week_values,))
    mv_elapsed_ms = _now_ms() - mv_started_ms
    result["mv_rows_by_week"] = mv_rows
    result["mv_totals"] = _sum_metric_rows(mv_rows, metrics)
    result["mv_elapsed_ms"] = mv_elapsed_ms
    result["diff"] = _diff_rows(mv_rows, candidate_rows, key_name="semana_inicio", metrics=metrics)
    diff_weeks = {str(row["semana_inicio"]) for row in result["diff"]["diff_rows"]}
    derived_diff_weeks = sorted(diff_weeks & derived_week_values)
    explicit_diff_weeks = sorted(diff_weeks & explicit_week_values)
    safety_diff_weeks = sorted(diff_weeks & safety_week_values)
    if derived_diff_weeks:
        result["warnings"].append("derived_week_diff:" + ",".join(derived_diff_weeks))
        if post_apply_validate:
            result["derived_weeks_ok"] = False
    if explicit_diff_weeks:
        result["warnings"].append("explicit_week_diff:" + ",".join(explicit_diff_weeks))
        if post_apply_validate:
            result["explicit_weeks_ok"] = False
    if safety_diff_weeks:
        result["safety_weeks_with_warnings"] = safety_diff_weeks
        result["warnings"].append("safety_week_diff:" + ",".join(safety_diff_weeks))

    pre_apply_diff_weeks = sorted(set(incomplete_derived) | set(derived_diff_weeks) | set(explicit_diff_weeks))
    result["expected_pre_apply_diff_weeks"] = [] if post_apply_validate else pre_apply_diff_weeks

    if (
        missing_explicit_route
        or (post_apply_validate and (derived_diff_weeks or explicit_diff_weeks))
        or (safety_diff_weeks and require_complete_safety_window)
    ):
        result["validation_status"] = "diff"
        result["blocking"] = True
    elif pre_apply_diff_weeks and not post_apply_validate:
        result["validation_status"] = "would_update"
        result["blocking"] = False
        result["warning_only"] = True
    elif incomplete_safety:
        result["validation_status"] = "ok_with_skipped_safety_weeks"
        result["blocking"] = False
        result["warning_only"] = True
    elif safety_diff_weeks:
        result["validation_status"] = "diff"
        result["blocking"] = False
        result["warning_only"] = True
    else:
        result["validation_status"] = "ok"
        result["blocking"] = False
        result["warning_only"] = False
    return result


def run_incremental_dry_run(
    *,
    db_url: str,
    week_scope: dict[str, set[date]],
    validate: bool,
    require_complete_safety_window: bool,
    post_apply_validate: bool,
    statement_timeout_seconds: int,
) -> dict[str, Any]:
    if not db_url:
        raise RuntimeError("NO_DB_URL_AVAILABLE")
    affected_dates = week_scope["requested_affected_dates"]
    validation_weeks = week_scope["validation_weeks"]
    if not affected_dates and not validation_weeks:
        raise RuntimeError("NO_AFFECTED_DATES_OR_WEEKS")

    import psycopg2

    db_url = ensure_sslmode(db_url)
    timeout_ms = _statement_timeout_ms(statement_timeout_seconds)
    requested_affected_date_values = sorted_iso(week_scope["requested_affected_dates"])
    explicit_affected_week_values = sorted_iso(week_scope["explicit_affected_weeks"])
    derived_affected_week_values = sorted_iso(week_scope["derived_affected_weeks"])
    safety_week_values = sorted_iso(week_scope["safety_weeks"])
    validation_week_values = sorted_iso(week_scope["validation_weeks"])
    week_origin_by_week = build_week_origin_by_week(week_scope)

    started_ms = _now_ms()
    result: dict[str, Any] = {
        "phase": PHASE,
        "status": "started",
        "dry_run": True,
        "affected_dates": requested_affected_date_values,
        "affected_weeks": validation_week_values,
        "requested_affected_dates": requested_affected_date_values,
        "explicit_affected_weeks": explicit_affected_week_values,
        "derived_affected_weeks": derived_affected_week_values,
        "safety_weeks": safety_week_values,
        "validation_weeks": validation_week_values,
        "week_origin_by_week": week_origin_by_week,
        "skipped_weeks": [],
        "warnings": [],
        "route_scope_by_week": [],
        "daily_fact_coverage_by_week": [],
        "pre_apply_diffs": [],
        "expected_updates": {
            "daily_fact_dates": [],
            "weekly_fact_weeks": [],
        },
        "daily_check": {},
        "weekly_check": {},
        "would_update": {
            "daily_fact": bool(requested_affected_date_values),
            "weekly_fact": bool(validation_week_values),
        },
        "real_apply_enabled": REAL_APPLY_ENABLED,
        "validate": bool(validate),
        "post_apply_validate": bool(post_apply_validate),
        "require_complete_safety_window": bool(require_complete_safety_window),
        "statement_timeout_seconds": int(statement_timeout_seconds),
        "final_status": "dry_run_started",
    }

    with psycopg2.connect(db_url) as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = %s", (timeout_ms,))
            if requested_affected_date_values:
                result["daily_check"] = _run_daily_check(cur, requested_affected_date_values, validate)
            else:
                result["daily_check"] = {
                    "validation_status": "skipped",
                    "reason": "no_affected_dates",
                }
            if validation_week_values:
                result["weekly_check"] = _run_weekly_check(
                    cur,
                    validation_week_values,
                    week_scope["derived_affected_weeks"],
                    week_scope["explicit_affected_weeks"],
                    week_scope["safety_weeks"],
                    week_origin_by_week,
                    validate,
                    require_complete_safety_window,
                    post_apply_validate,
                )
            else:
                result["weekly_check"] = {
                    "validation_status": "skipped",
                    "reason": "no_affected_weeks",
                    "derived_weeks_ok": True,
                    "explicit_weeks_ok": True,
                    "safety_weeks_skipped": [],
                }
            conn.rollback()

    daily_status = result["daily_check"].get("validation_status")
    weekly_check = result["weekly_check"]
    weekly_status = weekly_check.get("validation_status")
    result["route_scope_by_week"] = weekly_check.get("route_scope_by_week", [])
    result["daily_fact_coverage_by_week"] = weekly_check.get("daily_fact_coverage_by_week", [])
    result["skipped_weeks"] = list(weekly_check.get("safety_weeks_skipped", []))
    result["warnings"].extend(result["daily_check"].get("warnings", []))
    result["warnings"].extend(weekly_check.get("warnings", []))

    daily_diff_dates = []
    daily_diff = result["daily_check"].get("diff")
    if isinstance(daily_diff, dict):
        daily_diff_dates = [str(row.get("fecha_visita")) for row in daily_diff.get("diff_rows", [])]
    if daily_status == "diff" and not post_apply_validate:
        result["daily_check"]["validation_status"] = "would_update"
        result["pre_apply_diffs"].append({
            "scope": "daily_source_vs_fact",
            "diff_dates": daily_diff_dates,
            "diff": daily_diff,
        })
        result["expected_updates"]["daily_fact_dates"] = daily_diff_dates or requested_affected_date_values
    if daily_status == "fact_missing":
        result["expected_updates"]["daily_fact_dates"] = requested_affected_date_values

    weekly_diff = weekly_check.get("diff")
    weekly_expected_weeks = list(weekly_check.get("expected_pre_apply_diff_weeks", []))
    if weekly_expected_weeks:
        result["pre_apply_diffs"].append({
            "scope": "weekly_candidate_vs_mv",
            "diff_weeks": weekly_expected_weeks,
            "diff": weekly_diff,
        })
        result["expected_updates"]["weekly_fact_weeks"] = weekly_expected_weeks

    blocking_statuses = {"fact_missing", "daily_fact_missing"}
    if post_apply_validate:
        blocking_statuses.add("diff")
    weekly_blocking = bool(weekly_check.get("blocking"))
    if daily_status in blocking_statuses or weekly_blocking or weekly_status == "error":
        result["status"] = "error"
        result["final_status"] = "dry_run_error"
    elif result["expected_updates"]["daily_fact_dates"] or result["expected_updates"]["weekly_fact_weeks"]:
        result["status"] = "warn"
        result["final_status"] = "dry_run_would_update"
    elif result["skipped_weeks"] or weekly_check.get("warning_only"):
        result["status"] = "warn"
        result["final_status"] = "dry_run_ok_with_skipped_safety_weeks"
    else:
        result["status"] = "ok"
        result["final_status"] = "dry_run_ok"
    result["elapsed_ms"] = _now_ms() - started_ms
    return result


def blocked_real_apply_result(
    args: argparse.Namespace,
    week_scope: dict[str, set[date]],
    elapsed_ms: int,
) -> dict[str, Any]:
    raw_dates = set(args.affected_date or [])
    return {
        "phase": PHASE,
        "status": "error",
        "error": "real_apply_not_enabled_in_N2",
        "dry_run": False,
        "affected_dates": sorted_iso(raw_dates),
        "affected_weeks": sorted_iso(week_scope["validation_weeks"]),
        "requested_affected_dates": sorted_iso(week_scope["requested_affected_dates"]),
        "explicit_affected_weeks": sorted_iso(week_scope["explicit_affected_weeks"]),
        "derived_affected_weeks": sorted_iso(week_scope["derived_affected_weeks"]),
        "safety_weeks": sorted_iso(week_scope["safety_weeks"]),
        "validation_weeks": sorted_iso(week_scope["validation_weeks"]),
        "week_origin_by_week": build_week_origin_by_week(week_scope),
        "skipped_weeks": [],
        "warnings": [],
        "route_scope_by_week": [],
        "daily_fact_coverage_by_week": [],
        "pre_apply_diffs": [],
        "expected_updates": {
            "daily_fact_dates": [],
            "weekly_fact_weeks": [],
        },
        "daily_check": {},
        "weekly_check": {},
        "would_update": {
            "daily_fact": bool(raw_dates),
            "weekly_fact": bool(week_scope["validation_weeks"]),
        },
        "real_apply_enabled": REAL_APPLY_ENABLED,
        "final_status": "dry_run_error",
        "elapsed_ms": elapsed_ms,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", default="")
    parser.add_argument("--affected-date", action="append", type=parse_iso_date, default=[])
    parser.add_argument("--affected-week", action="append", type=parse_iso_date, default=[])
    parser.add_argument("--safety-window-weeks", type=int, default=1)
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--post-apply-validate", action="store_true")
    parser.add_argument("--require-complete-safety-window", action="store_true")
    parser.add_argument("--statement-timeout-seconds", type=int, default=DEFAULT_STATEMENT_TIMEOUT_SECONDS)
    return parser


def main() -> int:
    started_ms = _now_ms()
    parser = build_parser()
    args = parser.parse_args()

    if args.safety_window_weeks < 0:
        parser.error("--safety-window-weeks must be >= 0")

    raw_dates = set(args.affected_date or [])
    raw_weeks = set(args.affected_week or [])
    week_scope = build_week_scope(raw_dates, raw_weeks, args.safety_window_weeks)

    if not args.dry_run:
        print(json.dumps(
            blocked_real_apply_result(args, week_scope, _now_ms() - started_ms),
            ensure_ascii=False,
            indent=2,
        ))
        return 1

    try:
        result = run_incremental_dry_run(
            db_url=args.db_url,
            week_scope=week_scope,
            validate=args.validate,
            require_complete_safety_window=args.require_complete_safety_window,
            post_apply_validate=args.post_apply_validate,
            statement_timeout_seconds=args.statement_timeout_seconds,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0 if result.get("status") in {"ok", "warn"} else 1
    except Exception as exc:
        error_result = {
            "phase": PHASE,
            "status": "error",
            "dry_run": True,
            "affected_dates": sorted_iso(week_scope["requested_affected_dates"]),
            "affected_weeks": sorted_iso(week_scope["validation_weeks"]),
            "requested_affected_dates": sorted_iso(week_scope["requested_affected_dates"]),
            "explicit_affected_weeks": sorted_iso(week_scope["explicit_affected_weeks"]),
            "derived_affected_weeks": sorted_iso(week_scope["derived_affected_weeks"]),
            "safety_weeks": sorted_iso(week_scope["safety_weeks"]),
            "validation_weeks": sorted_iso(week_scope["validation_weeks"]),
            "week_origin_by_week": build_week_origin_by_week(week_scope),
            "skipped_weeks": [],
            "warnings": [],
            "route_scope_by_week": [],
            "daily_fact_coverage_by_week": [],
            "pre_apply_diffs": [],
            "expected_updates": {
                "daily_fact_dates": [],
                "weekly_fact_weeks": [],
            },
            "daily_check": {},
            "weekly_check": {},
            "would_update": {
                "daily_fact": bool(raw_dates),
                "weekly_fact": bool(week_scope["validation_weeks"]),
            },
            "real_apply_enabled": REAL_APPLY_ENABLED,
            "error": str(exc),
            "final_status": "dry_run_error",
            "elapsed_ms": _now_ms() - started_ms,
        }
        print(json.dumps(error_result, ensure_ascii=False, indent=2, default=str))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
