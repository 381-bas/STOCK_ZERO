from pathlib import Path
import os
import json
import pandas as pd
import psycopg
from psycopg.rows import dict_row

ROOT = Path(r"C:\Users\basti\Desktop\STOCK_ZERO")

DB_URL = os.environ["SUPABASE_DB_URL_ADMIN"]

SQL_3 = ROOT / "research" / "FAST_REFORM_009E_SQL_PACKAGE_AFTER_CLAUDE_GATE" / "3_BUILD_TEMP_CONTRACT.sql"
SQL_4 = ROOT / "research" / "FAST_REFORM_009E_SQL_PACKAGE_AFTER_CLAUDE_GATE" / "4_APPLY_ONE_TRANSACTION.sql"

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
    "mapped_canonical_event_rows": 15145,
    "unresolved_scope_event_rows": 53,
}

TARGET_REPORT_ID = "route_b_report_existence_first_202606_20260620_2345"
TARGET_RUN_ID = "fast_reform_009c_route_b_20260620_2345"


def date_iso(value):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


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


def fetch_one(cur, sql: str):
    cur.execute(sql)
    row = cur.fetchone()
    return dict(row) if row else None


result = {
    "phase": "BASTIAN_MANUAL_009E_APPLY_ONE_TRANSACTION",
    "verdict": None,
    "connection_user": None,
    "transaction_read_only": None,
    "target_report_id": TARGET_REPORT_ID,
    "target_run_id": TARGET_RUN_ID,
    "artifacts_seen": {},
    "temp_tables_loaded": {},
    "pre_apply_invariants": {},
    "post_apply_in_transaction_checks": {},
    "check_flags": {},
    "commit_executed": False,
    "rollback_executed": False,
    "error": None,
}

conn = None

try:
    # ------------------------------------------------------------
    # 1) Load local artifacts
    # ------------------------------------------------------------
    events_df = pd.read_parquet(EVENTS)
    file_audit_df = pd.read_csv(FILE_AUDIT)
    day_coverage_df = pd.read_csv(DAY_COVERAGE)
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
    # 2) Connect and start transaction
    # ------------------------------------------------------------
    conn = psycopg.connect(DB_URL, autocommit=False, row_factory=dict_row)

    with conn.cursor() as cur:
        cur.execute("select current_user as current_user")
        result["connection_user"] = cur.fetchone()["current_user"]

        cur.execute("show transaction_read_only")
        result["transaction_read_only"] = cur.fetchone()["transaction_read_only"]

        if result["transaction_read_only"] != "off":
            raise RuntimeError("transaction_read_only must be off for controlled apply")

        cur.execute("set local statement_timeout = '15min'")
        cur.execute("set local lock_timeout = '10s'")

        # ------------------------------------------------------------
        # 3) Build pg_temp contract and load local artifacts
        # ------------------------------------------------------------
        cur.execute(SQL_3.read_text(encoding="utf-8-sig"))

        copy_df(cur, "pg_temp.route_b_events_input", events_df)
        copy_df(cur, "pg_temp.route_b_file_manifest_input", file_audit_df)
        copy_df(cur, "pg_temp.route_b_day_coverage_input", day_coverage_df)

        result["temp_tables_loaded"] = {
            "route_b_events_input": scalar(cur, "select count(*) from pg_temp.route_b_events_input"),
            "route_b_file_manifest_input": scalar(cur, "select count(*) from pg_temp.route_b_file_manifest_input"),
            "route_b_day_coverage_input": scalar(cur, "select count(*) from pg_temp.route_b_day_coverage_input"),
            "route_b_conflicts_input": scalar(cur, "select count(*) from pg_temp.route_b_conflicts_input"),
        }

        # ------------------------------------------------------------
        # 4) Pre-apply invariant checks
        # ------------------------------------------------------------
        pre_checks = {
            "artifact_event_rows": """
                select count(*) from pg_temp.route_b_events_built
            """,
            "artifact_distinct_event_keys": """
                select count(distinct event_key) from pg_temp.route_b_events_built
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
                select count(*) from pg_temp.route_b_file_manifest_input
            """,
            "day_coverage_rows": """
                select count(*) from pg_temp.route_b_day_coverage_input
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

        for name, sql in pre_checks.items():
            result["pre_apply_invariants"][name] = scalar(cur, sql)

        result["pre_apply_invariants"]["new_file_included_in_009E"] = bool(
            file_audit_df["source_file"]
            .astype(str)
            .str.contains("1782440454408", regex=False)
            .any()
        )

        pre_ok = (
            result["pre_apply_invariants"]["artifact_event_rows"] == EXPECTED["artifact_event_rows"]
            and result["pre_apply_invariants"]["artifact_distinct_event_keys"] == EXPECTED["artifact_distinct_event_keys"]
            and result["pre_apply_invariants"]["apply_scope_event_rows"] == EXPECTED["apply_scope_event_rows"]
            and result["pre_apply_invariants"]["deferred_source_event_rows"] == EXPECTED["deferred_source_event_rows"]
            and result["pre_apply_invariants"]["file_manifest_rows"] == EXPECTED["file_manifest_rows"]
            and result["pre_apply_invariants"]["day_coverage_rows"] == EXPECTED["day_coverage_rows"]
            and result["pre_apply_invariants"]["event_key_multi_fecha"] == 0
            and result["pre_apply_invariants"]["event_key_multi_week"] == 0
            and result["pre_apply_invariants"]["corrected_identifier_conflicts"] == 0
            and result["pre_apply_invariants"]["week_2026_06_22_in_apply_scope"] == 0
            and result["pre_apply_invariants"]["new_file_included_in_009E"] is False
        )

        if not pre_ok:
            raise RuntimeError("Pre-apply invariants failed; refusing to execute 4_APPLY")

        # ------------------------------------------------------------
        # 5) Execute controlled apply
        # ------------------------------------------------------------
        cur.execute(SQL_4.read_text(encoding="utf-8-sig"))

        # ------------------------------------------------------------
        # 6) Immediate in-transaction sanity checks
        # ------------------------------------------------------------
        result["post_apply_in_transaction_checks"]["report_registry"] = fetch_one(
            cur,
            f"""
            select
                count(*) as report_registry_rows,
                max(report_id) as report_id,
                max(source_run_id) as source_run_id,
                max(source_artifact) as source_artifact,
                max(source_coverage_max_date) as source_coverage_max_date,
                max(route_decision) as route_decision,
                max(rebuild_strategy) as rebuild_strategy,
                max(hot_rows) as hot_rows,
                max(unique_event_keys) as unique_event_keys
            from cg_reform.report_registry
            where report_id = '{TARGET_REPORT_ID}'
              and source_run_id = '{TARGET_RUN_ID}'
            """
        )

        result["post_apply_in_transaction_checks"]["canonical_hot_keys"] = fetch_one(
            cur,
            f"""
            select
                count(*) as canonical_rows,
                count(distinct event_key) as distinct_event_keys,
                min(week_start) as min_week,
                max(week_start) as max_week,
                count(*) filter (where week_start = date '2026-06-15') as deferred_week_rows,
                count(*) filter (where week_start = date '2026-06-22') as week_2026_06_22_rows
            from cg_reform.canonical_hot_keys
            where source_run_id = '{TARGET_RUN_ID}'
            """
        )

        result["post_apply_in_transaction_checks"]["report_existence_week"] = fetch_one(
            cur,
            f"""
            select
                count(*) as existence_rows,
                min(week_start) as min_week,
                max(week_start) as max_week,
                count(*) filter (where week_start = date '2026-06-15') as deferred_week_rows,
                count(*) filter (where week_start = date '2026-06-22') as week_2026_06_22_rows
            from cg_reform.report_existence_week
            where report_id = '{TARGET_REPORT_ID}'
            """
        )

        result["post_apply_in_transaction_checks"]["quarantine_unresolved_scope"] = fetch_one(
            cur,
            f"""
            select
                count(*) as unresolved_scope_rows
            from cg_reform.quarantine_min
            where report_id = '{TARGET_REPORT_ID}'
              and source_run_id = '{TARGET_RUN_ID}'
              and reason = 'UNRESOLVED_SCOPE'
            """
        )

        result["post_apply_in_transaction_checks"]["quarantine_by_reason"] = fetch_one(
            cur,
            f"""
            select
                count(*) as quarantine_total_rows,
                count(*) filter (where reason = 'UNRESOLVED_SCOPE') as unresolved_scope_rows,
                count(*) filter (where severity = 'MEDIUM') as medium_rows
            from cg_reform.quarantine_min
            where report_id = '{TARGET_REPORT_ID}'
              and source_run_id = '{TARGET_RUN_ID}'
            """
        )

        post = result["post_apply_in_transaction_checks"]

        report_ok = (
            post["report_registry"]["report_registry_rows"] == 1
            and date_iso(post["report_registry"]["source_coverage_max_date"]) == "2026-06-19"
            and post["report_registry"]["route_decision"] == "ROUTE_B_PARTIAL_JUNE_REBUILD_SOURCE_VISIBLE_DENOMINATOR_GATED"
            and post["report_registry"]["rebuild_strategy"] == "ROUTE_B_PARTIAL_JUNE_REBUILD"
            and post["report_registry"]["hot_rows"] == EXPECTED["mapped_canonical_event_rows"]
            and post["report_registry"]["unique_event_keys"] == EXPECTED["mapped_canonical_event_rows"]
        )

        canonical_ok = (
            post["canonical_hot_keys"]["canonical_rows"] == EXPECTED["mapped_canonical_event_rows"]
            and post["canonical_hot_keys"]["distinct_event_keys"] == EXPECTED["mapped_canonical_event_rows"]
            and date_iso(post["canonical_hot_keys"]["min_week"]) == "2026-06-01"
            and date_iso(post["canonical_hot_keys"]["max_week"]) == "2026-06-08"
            and post["canonical_hot_keys"]["deferred_week_rows"] == 0
            and post["canonical_hot_keys"]["week_2026_06_22_rows"] == 0
        )

        existence_ok = (
            date_iso(post["report_existence_week"]["min_week"]) == "2026-06-01"
            and date_iso(post["report_existence_week"]["max_week"]) == "2026-06-08"
            and post["report_existence_week"]["deferred_week_rows"] == 0
            and post["report_existence_week"]["week_2026_06_22_rows"] == 0
        )

        quarantine_ok = (
            post["quarantine_unresolved_scope"]["unresolved_scope_rows"] == EXPECTED["unresolved_scope_event_rows"]
            and post["quarantine_by_reason"]["unresolved_scope_rows"] == EXPECTED["unresolved_scope_event_rows"]
            and post["quarantine_by_reason"]["medium_rows"] >= EXPECTED["unresolved_scope_event_rows"]
        )

        result["check_flags"] = {
            "pre_ok": pre_ok,
            "report_ok": report_ok,
            "canonical_ok": canonical_ok,
            "existence_ok": existence_ok,
            "quarantine_ok": quarantine_ok,
        }

        if not (report_ok and canonical_ok and existence_ok and quarantine_ok):
            raise RuntimeError("Post-apply in-transaction checks failed; refusing COMMIT")

    # ------------------------------------------------------------
    # 7) Commit only after all checks pass
    # ------------------------------------------------------------
    conn.commit()
    result["commit_executed"] = True
    result["rollback_executed"] = False
    result["verdict"] = "APPLY_COMMITTED"

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