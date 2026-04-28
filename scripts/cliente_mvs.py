#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import json
import os
import time

import psycopg2


REFRESH_ORDER = [
    "public.mv_scope_fact_latest_cliente",
    "public.mv_cliente_scope_inventory_enriched",
    "public.mv_cliente_scope_ranking_cliente",
    "public.mv_cliente_scope_ranking_responsable",
]

SMOKE_QUERIES = {
    "inventory_count": """
        SELECT COUNT(*)::bigint
        FROM public.mv_cliente_scope_inventory_enriched
    """,
    "inventory_duplicate_keys": """
        SELECT COUNT(*)::bigint
        FROM (
            SELECT cod_rt, cliente_norm, sku, COUNT(*) AS n
            FROM public.mv_cliente_scope_inventory_enriched
            GROUP BY cod_rt, cliente_norm, sku
            HAVING COUNT(*) > 1
        ) d
    """,
    "ranking_cliente_total_skus": """
        SELECT COALESCE(SUM(total_skus), 0)::bigint
        FROM public.mv_cliente_scope_ranking_cliente
    """,
    "ranking_responsable_count": """
        SELECT COUNT(*)::bigint
        FROM public.mv_cliente_scope_ranking_responsable
    """,
}


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
    return int(time.time() * 1000)


def build_plan(db_url: str, execute: bool, run_smoke: bool) -> dict:
    return {
        "mode": "refresh" if execute else "dry-run",
        "db_url_source": (
            "explicit"
            if db_url
            else "DB_URL_LOAD|DB_URL_APP|DB_URL"
        ),
        "refresh_order": REFRESH_ORDER,
        "run_smoke": run_smoke,
        "smoke_checks": {
            "inventory_count_gt_zero": "count(*) from public.mv_cliente_scope_inventory_enriched > 0",
            "inventory_duplicate_keys_zero": "duplicates by (cod_rt, cliente_norm, sku) = 0",
            "ranking_cliente_matches_inventory": "sum(total_skus) from public.mv_cliente_scope_ranking_cliente equals inventory count",
            "ranking_responsable_count_gt_zero": "count(*) from public.mv_cliente_scope_ranking_responsable > 0",
        },
    }


def run_smoke_checks(cur) -> dict:
    inventory_count = _fetch_scalar(cur, SMOKE_QUERIES["inventory_count"])
    duplicate_keys = _fetch_scalar(cur, SMOKE_QUERIES["inventory_duplicate_keys"])
    ranking_cliente_total_skus = _fetch_scalar(cur, SMOKE_QUERIES["ranking_cliente_total_skus"])
    ranking_responsable_count = _fetch_scalar(cur, SMOKE_QUERIES["ranking_responsable_count"])

    checks = {
        "inventory_count_gt_zero": inventory_count > 0,
        "inventory_duplicate_keys_zero": duplicate_keys == 0,
        "ranking_cliente_matches_inventory": ranking_cliente_total_skus == inventory_count,
        "ranking_responsable_count_gt_zero": ranking_responsable_count > 0,
    }
    result = {
        "inventory_count": inventory_count,
        "inventory_duplicate_keys": duplicate_keys,
        "ranking_cliente_total_skus": ranking_cliente_total_skus,
        "ranking_responsable_count": ranking_responsable_count,
        "checks": checks,
        "ok": all(checks.values()),
    }
    return result


def _fetch_scalar(cur, sql: str) -> int:
    cur.execute(sql)
    row = cur.fetchone()
    return int(row[0] or 0)


def run_cliente_mvs_refresh(
    *,
    db_url: str = "",
    execute: bool = False,
    run_smoke: bool = True,
) -> dict:
    resolved_db_url = resolve_db_url(db_url)
    plan = build_plan(db_url, execute=execute, run_smoke=run_smoke)
    plan["resolved_db_url_present"] = bool(resolved_db_url)

    if not execute:
        plan["status"] = "planned"
        return plan

    if not resolved_db_url:
        raise RuntimeError("Falta DB URL para refresh MVs CLIENTE. Revisa DB_URL_LOAD, DB_URL_APP o DB_URL.")

    started_ms = _now_ms()
    steps = []
    smoke = None

    with psycopg2.connect(resolved_db_url) as conn:
        with conn.cursor() as cur:
            for mv_name in REFRESH_ORDER:
                step_started_ms = _now_ms()
                cur.execute(f"REFRESH MATERIALIZED VIEW {mv_name};")
                steps.append(
                    {
                        "mv": mv_name,
                        "status": "refreshed",
                        "elapsed_ms": _now_ms() - step_started_ms,
                    }
                )

            if run_smoke:
                smoke = run_smoke_checks(cur)
                if not smoke["ok"]:
                    raise RuntimeError(f"Smoke refresh MVs CLIENTE falló: {smoke}")

        conn.commit()

    result = {
        "mode": "refresh",
        "status": "ok",
        "refresh_order": REFRESH_ORDER,
        "steps": steps,
        "run_smoke": run_smoke,
        "smoke": smoke,
        "elapsed_ms": _now_ms() - started_ms,
    }
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", default="")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--skip-smoke", action="store_true")
    args = ap.parse_args()

    execute = bool(args.refresh and not args.dry_run)
    try:
        result = run_cliente_mvs_refresh(
            db_url=args.db_url,
            execute=execute,
            run_smoke=not args.skip_smoke,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    except Exception as exc:
        error_result = {
            "mode": "refresh" if execute else "dry-run",
            "status": "error",
            "error": str(exc),
        }
        print(json.dumps(error_result, indent=2, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
