from pathlib import Path
import os
import json
import pandas as pd
import psycopg
from psycopg.rows import dict_row

ROOT = Path(r"C:\Users\basti\Desktop\STOCK_ZERO")

DB_URL = os.environ["SUPABASE_DB_URL_ADMIN"]

SQL_3 = ROOT / "research" / "FAST_REFORM_009E_SQL_PACKAGE_AFTER_CLAUDE_GATE" / "3_BUILD_TEMP_CONTRACT.sql"

EVENTS = ROOT / "data" / "normalized" / "fast_reform_009c_route_b_20260620_2345_events.parquet"
FILE_AUDIT = ROOT / "data" / "exports" / "fast_reform_009c_route_b_20260620_2345_file_audit.csv"
DAY_COVERAGE = ROOT / "data" / "exports" / "fast_reform_009c_route_b_20260620_2345_day_coverage.csv"

def copy_df(cur, table_name: str, df: pd.DataFrame):
    cols = list(df.columns)
    col_sql = ", ".join(cols)
    csv_payload = df.to_csv(index=False)
    with cur.copy(
        f"COPY {table_name} ({col_sql}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
    ) as copy:
        copy.write(csv_payload)

def fetch_all(cur, sql: str):
    cur.execute(sql)
    return [dict(r) for r in cur.fetchall()]

def scalar(cur, sql: str):
    cur.execute(sql)
    row = cur.fetchone()
    return list(row.values())[0]

result = {
    "phase": "BASTIAN_MANUAL_009E_UNRESOLVED_SCOPE_DIAGNOSTIC_V2",
    "verdict": None,
    "connection_user": None,
    "transaction_read_only": None,
    "counts": {},
    "unresolved_summary": [],
    "unresolved_sample": [],
    "denominator_near_matches_by_cod_rt": [],
    "denominator_near_matches_by_cliente": [],
    "rollback_executed": False,
    "commit": False,
    "error": None,
}

conn = None

try:
    events_df = pd.read_parquet(EVENTS)
    file_audit_df = pd.read_csv(FILE_AUDIT)
    day_coverage_df = pd.read_csv(DAY_COVERAGE)
    day_coverage_df = day_coverage_df.rename(columns={"date": "coverage_date"})

    conn = psycopg.connect(DB_URL, autocommit=False, row_factory=dict_row)

    with conn.cursor() as cur:
        cur.execute("select current_user as current_user")
        result["connection_user"] = cur.fetchone()["current_user"]

        cur.execute("show transaction_read_only")
        result["transaction_read_only"] = cur.fetchone()["transaction_read_only"]

        if result["transaction_read_only"] != "off":
            raise RuntimeError("transaction_read_only must be off for CREATE TEMP TABLE")

        cur.execute(SQL_3.read_text(encoding="utf-8-sig"))

        copy_df(cur, "pg_temp.route_b_events_input", events_df)
        copy_df(cur, "pg_temp.route_b_file_manifest_input", file_audit_df)
        copy_df(cur, "pg_temp.route_b_day_coverage_input", day_coverage_df)

        apply_scope_filter = """
            e.week_start in (date '2026-06-01', date '2026-06-08')
        """

        result["counts"]["events_built"] = scalar(
            cur,
            "select count(*) from pg_temp.route_b_events_built"
        )

        result["counts"]["apply_scope_events"] = scalar(
            cur,
            f"""
            select count(*)
            from pg_temp.route_b_events_built e
            where {apply_scope_filter}
            """
        )

        result["counts"]["unresolved_events"] = scalar(
            cur,
            f"""
            select count(*)
            from pg_temp.route_b_events_built e
            left join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
              on rr.effective_week_start::date = e.week_start
             and upper(trim(rr.cod_rt)) = e.cod_rt
             and upper(trim(rr.cliente_norm)) = e.cliente_norm
            where {apply_scope_filter}
              and rr.cod_rt is null
            """
        )

        result["unresolved_summary"] = fetch_all(
            cur,
            f"""
            select
                e.week_start,
                e.cod_rt,
                e.cliente_norm,
                e.holding_norm,
                e.reponedor_norm,
                count(*) as event_rows,
                min(e.fecha) as min_fecha,
                max(e.fecha) as max_fecha,
                min(e.event_key) as sample_event_key
            from pg_temp.route_b_events_built e
            left join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
              on rr.effective_week_start::date = e.week_start
             and upper(trim(rr.cod_rt)) = e.cod_rt
             and upper(trim(rr.cliente_norm)) = e.cliente_norm
            where {apply_scope_filter}
              and rr.cod_rt is null
            group by
                e.week_start,
                e.cod_rt,
                e.cliente_norm,
                e.holding_norm,
                e.reponedor_norm
            order by event_rows desc, e.week_start, e.cod_rt, e.cliente_norm
            """
        )

        result["unresolved_sample"] = fetch_all(
            cur,
            f"""
            select
                e.event_key,
                e.week_start,
                e.fecha,
                e.cod_rt,
                e.cliente_norm,
                e.holding_norm,
                e.reponedor_norm,
                e.source_file_count,
                e.n_fotos_calculado
            from pg_temp.route_b_events_built e
            left join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
              on rr.effective_week_start::date = e.week_start
             and upper(trim(rr.cod_rt)) = e.cod_rt
             and upper(trim(rr.cliente_norm)) = e.cliente_norm
            where {apply_scope_filter}
              and rr.cod_rt is null
            order by e.week_start, e.cod_rt, e.cliente_norm, e.fecha, e.event_key
            limit 80
            """
        )

        result["denominator_near_matches_by_cod_rt"] = fetch_all(
            cur,
            f"""
            with unresolved as (
              select distinct
                  e.week_start,
                  e.cod_rt,
                  e.cliente_norm
              from pg_temp.route_b_events_built e
              left join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
                on rr.effective_week_start::date = e.week_start
               and upper(trim(rr.cod_rt)) = e.cod_rt
               and upper(trim(rr.cliente_norm)) = e.cliente_norm
              where {apply_scope_filter}
                and rr.cod_rt is null
            )
            select
                u.week_start,
                u.cod_rt,
                u.cliente_norm as event_cliente_norm,
                upper(trim(rr.cliente_norm)) as denominator_cliente_norm,
                count(*) as denominator_rows
            from unresolved u
            join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
              on rr.effective_week_start::date = u.week_start
             and upper(trim(rr.cod_rt)) = u.cod_rt
            group by
                u.week_start,
                u.cod_rt,
                u.cliente_norm,
                upper(trim(rr.cliente_norm))
            order by u.week_start, u.cod_rt, u.cliente_norm, denominator_rows desc
            limit 200
            """
        )

        result["denominator_near_matches_by_cliente"] = fetch_all(
            cur,
            f"""
            with unresolved as (
              select distinct
                  e.week_start,
                  e.cod_rt,
                  e.cliente_norm
              from pg_temp.route_b_events_built e
              left join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
                on rr.effective_week_start::date = e.week_start
               and upper(trim(rr.cod_rt)) = e.cod_rt
               and upper(trim(rr.cliente_norm)) = e.cliente_norm
              where {apply_scope_filter}
                and rr.cod_rt is null
            )
            select
                u.week_start,
                u.cod_rt as event_cod_rt,
                u.cliente_norm,
                upper(trim(rr.cod_rt)) as denominator_cod_rt,
                count(*) as denominator_rows
            from unresolved u
            join cg_core.v_rr_frecuencia_base_resuelta_v2 rr
              on rr.effective_week_start::date = u.week_start
             and upper(trim(rr.cliente_norm)) = u.cliente_norm
            group by
                u.week_start,
                u.cod_rt,
                u.cliente_norm,
                upper(trim(rr.cod_rt))
            order by u.week_start, u.cliente_norm, u.cod_rt, denominator_rows desc
            limit 200
            """
        )

        result["verdict"] = "DIAGNOSTIC_COMPLETE"

    conn.rollback()
    result["rollback_executed"] = True

except Exception as e:
    result["verdict"] = "BLOCKED_OR_ERROR"
    result["error"] = str(e)

    if conn is not None:
        try:
            conn.rollback()
            result["rollback_executed"] = True
        except Exception as rollback_error:
            result["rollback_error"] = str(rollback_error)

finally:
    if conn is not None:
        conn.close()

print(json.dumps(result, indent=2, ensure_ascii=False, default=str))