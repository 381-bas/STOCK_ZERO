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

EXPECTED = {
    "artifact_event_rows": 22195,
    "artifact_distinct_event_keys": 22195,
    "apply_scope_event_rows": 15198,
    "deferred_source_event_rows": 6997,
    "file_manifest_rows": 5,
    "day_coverage_rows": 19,
}

def copy_df(cur, table_name: str, df: pd.DataFrame):
    cols = list(df.columns)
    col_sql = ", ".join(cols)
    csv_payload = df.to_csv(index=False)
    with cur.copy(
        f"COPY {table_name} ({col_sql}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
    ) as copy:
        copy.write(csv_payload)

def scalar(cur, sql: str):
    cur.execute(sql)
    row = cur.fetchone()
    return list(row.values())[0]

def table_count(cur, table_name: str):
    return scalar(cur, f"select count(*) from {table_name}")

result = {
    "phase": "BASTIAN_MANUAL_009E_TEMP_ONLY_DRY_RUN",
    "verdict": None,
    "connection_user": None,
    "transaction_read_only": None,
    "artifacts_seen": {},
    "temp_tables_loaded": {},
    "invariant_results": {},
    "persistent_writes": False,
    "rollback_executed": False,
    "commit": False,
}

conn = None

try:
    # ------------------------------------------------------------
    # 1) Load local artifacts
    # ------------------------------------------------------------
    events_df = pd.read_parquet(EVENTS)
    file_audit_df = pd.read_csv(FILE_AUDIT)
    day_coverage_df = pd.read_csv(DAY_COVERAGE)

    # Contract expects pg_temp.route_b_day_coverage_input.coverage_date,
    # but the CSV artifact may provide the column as "date".
    day_coverage_df = day_coverage_df.rename(columns={"date": "coverage_date"})

    result["artifacts_seen"] = {
        "events_parquet": {
            "exists": EVENTS.exists(),
            "rows": len(events_df),
            "distinct_event_keys": int(events_df["event_key"].nunique()),
        },
        "file_audit_csv": {
            "exists": FILE_AUDIT.exists(),
            "rows": len(file_audit_df),
        },
        "day_coverage_csv": {
            "exists": DAY_COVERAGE.exists(),
            "rows": len(day_coverage_df),
            "columns": list(day_coverage_df.columns),
        },
    }

    # ------------------------------------------------------------
    # 2) Connect to DB
    # ------------------------------------------------------------
    conn = psycopg.connect(DB_URL, autocommit=False, row_factory=dict_row)

    with conn.cursor() as cur:
        cur.execute("select current_user as current_user")
        result["connection_user"] = cur.fetchone()["current_user"]

        cur.execute("show transaction_read_only")
        result["transaction_read_only"] = cur.fetchone()["transaction_read_only"]

        if result["transaction_read_only"] != "off":
            raise RuntimeError("transaction_read_only must be off for CREATE TEMP TABLE")

        # ------------------------------------------------------------
        # 3) Execute temp contract DDL only
        # ------------------------------------------------------------
        sql3 = SQL_3.read_text(encoding="utf-8-sig")
        cur.execute(sql3)

        # ------------------------------------------------------------
        # 4) Load pg_temp input tables
        # ------------------------------------------------------------
        copy_df(cur, "pg_temp.route_b_events_input", events_df)
        copy_df(cur, "pg_temp.route_b_file_manifest_input", file_audit_df)
        copy_df(cur, "pg_temp.route_b_day_coverage_input", day_coverage_df)

        # route_b_conflicts_input is expected to remain empty.
        result["temp_tables_loaded"] = {
            "route_b_events_input": table_count(cur, "pg_temp.route_b_events_input"),
            "route_b_file_manifest_input": table_count(cur, "pg_temp.route_b_file_manifest_input"),
            "route_b_day_coverage_input": table_count(cur, "pg_temp.route_b_day_coverage_input"),
            "route_b_conflicts_input": table_count(cur, "pg_temp.route_b_conflicts_input"),
        }

        # ------------------------------------------------------------
        # 5) Invariant checks
        # ------------------------------------------------------------
        checks = {
            "artifact_event_rows": """
                select count(*)
                from pg_temp.route_b_events_built
            """,
            "artifact_distinct_event_keys": """
                select count(distinct event_key)
                from pg_temp.route_b_events_built
            """,
            "apply_scope_event_rows": """
                select count(*)
                from pg_temp.route_b_events_built
                where week_start in (date '2026-06-01', date '2026-06-08')
            """,
            "deferred_source_event_rows": """
                select count(*)
                from pg_temp.route_b_events_built
                where week_start = date '2026-06-15'
            """,
            "file_manifest_rows": """
                select count(*)
                from pg_temp.route_b_file_manifest_input
            """,
            "day_coverage_rows": """
                select count(*)
                from pg_temp.route_b_day_coverage_input
            """,
            "event_key_multi_fecha": """
                select count(*)
                from (
                    select event_key
                    from pg_temp.route_b_events_built
                    group by event_key
                    having count(distinct fecha) > 1
                ) x
            """,
            "event_key_multi_week": """
                select count(*)
                from (
                    select event_key
                    from pg_temp.route_b_events_built
                    group by event_key
                    having count(distinct week_start) > 1
                ) x
            """,
            "corrected_identifier_conflicts": """
                select count(*)
                from pg_temp.route_b_conflicts_input
                where coalesce(content_hash_event_count, 0) > 1
            """,
            "week_2026_06_22_in_apply_scope": """
                select count(*)
                from pg_temp.route_b_events_built
                where week_start = date '2026-06-22'
            """,
        }

        for name, sql in checks.items():
            result["invariant_results"][name] = scalar(cur, sql)

        result["invariant_results"]["new_file_included_in_009E"] = bool(
            file_audit_df["source_file"]
            .astype(str)
            .str.contains("1782440454408", regex=False)
            .any()
        )

        expected_ok = (
            result["invariant_results"]["artifact_event_rows"] == EXPECTED["artifact_event_rows"]
            and result["invariant_results"]["artifact_distinct_event_keys"] == EXPECTED["artifact_distinct_event_keys"]
            and result["invariant_results"]["apply_scope_event_rows"] == EXPECTED["apply_scope_event_rows"]
            and result["invariant_results"]["deferred_source_event_rows"] == EXPECTED["deferred_source_event_rows"]
            and result["invariant_results"]["file_manifest_rows"] == EXPECTED["file_manifest_rows"]
            and result["invariant_results"]["day_coverage_rows"] == EXPECTED["day_coverage_rows"]
            and result["invariant_results"]["event_key_multi_fecha"] == 0
            and result["invariant_results"]["event_key_multi_week"] == 0
            and result["invariant_results"]["corrected_identifier_conflicts"] == 0
            and result["invariant_results"]["new_file_included_in_009E"] is False
            and result["invariant_results"]["week_2026_06_22_in_apply_scope"] == 0
        )

        result["verdict"] = "TEMP_CONTRACT_DRY_RUN_PASS" if expected_ok else "REWORK_REQUIRED"

    # ------------------------------------------------------------
    # 6) Mandatory rollback
    # ------------------------------------------------------------
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