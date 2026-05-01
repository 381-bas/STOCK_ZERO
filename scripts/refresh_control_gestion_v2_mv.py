#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

MV_NAME = "cg_mart.mv_cg_out_weekly_v2"
SOURCE_VIEW = "cg_mart.v_cg_out_weekly_v2"


def ensure_sslmode(db_url: str) -> str:
    if not db_url:
        return db_url
    if "sslmode=" in db_url:
        return db_url
    if "?" in db_url:
        return db_url + "&sslmode=require"
    return db_url + "?sslmode=require"


def resolve_db_url(explicit_db_url: str = "") -> str:
    db_url = (
        explicit_db_url
        or os.getenv("DB_URL_LOAD", "")
        or os.getenv("DB_URL_APP", "")
        or os.getenv("DB_URL", "")
    )
    return ensure_sslmode(db_url)


def _now_ms() -> int:
    return int(time.perf_counter() * 1000)


def _print_trace(tag: str, **fields: Any) -> None:
    parts = [tag]
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    print(" ".join(parts))


def _validation_query() -> str:
    return f"""
    WITH source_stats AS (
        SELECT
            COUNT(*)::bigint AS total_rows,
            COALESCE(SUM(COALESCE("VISITA", 0)), 0)::bigint AS visita_plan,
            COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
            COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
            COALESCE(SUM(COALESCE("SOBRE_CUMPLIMIENTO", 0)), 0)::bigint AS sobre_cumplimiento,
            COALESCE(SUM(GREATEST(COALESCE("VISITA", 0) - COALESCE("VISITA_REALIZADA_CAP", 0), 0)), 0)::bigint AS visitas_pendientes,
            COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(CAST("ALERTA" AS text), ''))) = 'CUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS cumple_rows,
            COALESCE(SUM(CASE WHEN UPPER(TRIM(COALESCE(CAST("ALERTA" AS text), ''))) = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS incumple_rows,
            COALESCE(SUM(
                CASE
                    WHEN COALESCE("RUTA_DUPLICADA_FLAG", 0) = 1
                      OR COALESCE("RUTA_DUPLICADA_ROWS", 0) > 1
                      OR CAST("GESTOR" AS text) LIKE '%%|%%'
                      OR CAST("RUTERO" AS text) LIKE '%%|%%'
                    THEN 1 ELSE 0
                END
            ), 0)::bigint AS gestion_compartida_rows
        FROM {SOURCE_VIEW}
    ),
    mv_stats AS (
        SELECT
            COUNT(*)::bigint AS total_rows,
            COALESCE(SUM(COALESCE("VISITA", 0)), 0)::bigint AS visita_plan,
            COALESCE(SUM(COALESCE("VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
            COALESCE(SUM(COALESCE("VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
            COALESCE(SUM(COALESCE("SOBRE_CUMPLIMIENTO", 0)), 0)::bigint AS sobre_cumplimiento,
            COALESCE(SUM(COALESCE("VISITAS_PENDIENTES_CALC", GREATEST(COALESCE("VISITA", 0) - COALESCE("VISITA_REALIZADA_CAP", 0), 0))), 0)::bigint AS visitas_pendientes,
            COALESCE(SUM(CASE WHEN COALESCE("ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST("ALERTA" AS text), '')))) = 'CUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS cumple_rows,
            COALESCE(SUM(CASE WHEN COALESCE("ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST("ALERTA" AS text), '')))) = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::bigint AS incumple_rows,
            COALESCE(SUM(
                COALESCE(
                    "GESTION_COMPARTIDA_FLAG_CALC",
                    CASE
                        WHEN COALESCE("RUTA_DUPLICADA_FLAG", 0) = 1
                          OR COALESCE("RUTA_DUPLICADA_ROWS", 0) > 1
                          OR CAST("GESTOR" AS text) LIKE '%%|%%'
                          OR CAST("RUTERO" AS text) LIKE '%%|%%'
                        THEN 1 ELSE 0
                    END
                )
            ), 0)::bigint AS gestion_compartida_rows
        FROM {MV_NAME}
    )
    SELECT
        (mv.total_rows - src.total_rows)::bigint AS total_rows_diff,
        (mv.visita_plan - src.visita_plan)::bigint AS visita_diff,
        (mv.visita_realizada_raw - src.visita_realizada_raw)::bigint AS visita_realizada_raw_diff,
        (mv.visita_realizada_cap - src.visita_realizada_cap)::bigint AS visita_realizada_cap_diff,
        (mv.sobre_cumplimiento - src.sobre_cumplimiento)::bigint AS sobre_cumplimiento_diff,
        (mv.visitas_pendientes - src.visitas_pendientes)::bigint AS visitas_pendientes_diff,
        (mv.cumple_rows - src.cumple_rows)::bigint AS cumple_diff,
        (mv.incumple_rows - src.incumple_rows)::bigint AS incumple_diff,
        (mv.gestion_compartida_rows - src.gestion_compartida_rows)::bigint AS gestion_compartida_diff
    FROM source_stats src
    CROSS JOIN mv_stats mv
    """


def _run_validation(cur) -> dict[str, Any]:
    cur.execute(_validation_query())
    row = cur.fetchone()
    assert row is not None
    keys = [desc[0] for desc in cur.description]
    diffs = {key: int(value or 0) for key, value in zip(keys, row)}
    ok = all(value == 0 for value in diffs.values())
    return {"ok": ok, "diffs": diffs}


def run_cg_v2_mv_refresh(
    *,
    db_url: str = "",
    skip_analyze: bool = False,
    validate: bool = False,
    statement_timeout_seconds: int = 300,
) -> dict[str, Any]:
    resolved_db_url = resolve_db_url(db_url)
    if not resolved_db_url:
        raise RuntimeError("NO_DB_URL_AVAILABLE")

    import psycopg2

    started_ms = _now_ms()
    timeout_ms = max(int(statement_timeout_seconds), 1) * 1000
    result: dict[str, Any] = {
        "mv_name": MV_NAME,
        "source_view": SOURCE_VIEW,
        "skip_analyze": bool(skip_analyze),
        "validate": bool(validate),
        "statement_timeout_seconds": int(statement_timeout_seconds),
        "status": "started",
    }

    _print_trace(
        "MV_REFRESH_START",
        mv=MV_NAME,
        validate=int(bool(validate)),
        analyze=int(not skip_analyze),
        statement_timeout_seconds=int(statement_timeout_seconds),
    )

    with psycopg2.connect(resolved_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout TO {timeout_ms}")

            refresh_started_ms = _now_ms()
            cur.execute(f"REFRESH MATERIALIZED VIEW {MV_NAME};")
            conn.commit()
            refresh_elapsed_ms = _now_ms() - refresh_started_ms
            result["refresh_elapsed_ms"] = refresh_elapsed_ms
            _print_trace("MV_REFRESH_OK", mv=MV_NAME, elapsed_ms=refresh_elapsed_ms)

            if skip_analyze:
                result["analyze_status"] = "skipped"
            else:
                analyze_started_ms = _now_ms()
                cur.execute(f"ANALYZE {MV_NAME};")
                conn.commit()
                analyze_elapsed_ms = _now_ms() - analyze_started_ms
                result["analyze_status"] = "ok"
                result["analyze_elapsed_ms"] = analyze_elapsed_ms
                _print_trace("MV_ANALYZE_OK", mv=MV_NAME, elapsed_ms=analyze_elapsed_ms)

            if validate:
                validation_started_ms = _now_ms()
                validation = _run_validation(cur)
                validation["elapsed_ms"] = _now_ms() - validation_started_ms
                result["validation"] = validation
                if validation["ok"]:
                    _print_trace("MV_VALIDATE_OK", mv=MV_NAME, elapsed_ms=validation["elapsed_ms"])
                else:
                    _print_trace(
                        "MV_VALIDATE_FAIL",
                        mv=MV_NAME,
                        elapsed_ms=validation["elapsed_ms"],
                        diffs=json.dumps(validation["diffs"], ensure_ascii=False, sort_keys=True),
                    )
                    result["status"] = "error"
                    result["elapsed_ms"] = _now_ms() - started_ms
                    raise RuntimeError(f"MV_VALIDATE_FAIL {json.dumps(validation['diffs'], ensure_ascii=False, sort_keys=True)}")

    result["status"] = "ok"
    result["elapsed_ms"] = _now_ms() - started_ms
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", default="")
    ap.add_argument("--skip-analyze", action="store_true")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--statement-timeout-seconds", type=int, default=300)
    args = ap.parse_args()

    try:
        result = run_cg_v2_mv_refresh(
            db_url=args.db_url,
            skip_analyze=args.skip_analyze,
            validate=args.validate,
            statement_timeout_seconds=args.statement_timeout_seconds,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    except Exception as exc:
        if str(exc) == "NO_DB_URL_AVAILABLE":
            print("NO_DB_URL_AVAILABLE")
        error_result = {
            "mv_name": MV_NAME,
            "status": "error",
            "error": str(exc),
        }
        print(json.dumps(error_result, indent=2, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
