from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

try:
    from scripts.kpione_route_b_v1 import (
        classify_global_photo_duplicates,
        inspect_workbook,
        semantic_content_hash,
        stable_json,
    )
except ModuleNotFoundError:  # Direct execution from scripts/.
    from kpione_route_b_v1 import (
        classify_global_photo_duplicates,
        inspect_workbook,
        semantic_content_hash,
        stable_json,
    )


ROOT = Path(__file__).resolve().parents[1]
PLAN_TYPE = "kpione_route_b_productive_apply_plan"
ALLOWED_DB_ENV = "DB_URL_CODEX_RO"
EXPECTED_READONLY_ROLE = "stock_zero_codex_ro"


class PrecheckBlock(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_lf_normalized(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def load_plan(path: Path) -> dict[str, Any]:
    plan = json.loads(path.read_text(encoding="utf-8"))
    if plan.get("document_type") != PLAN_TYPE:
        raise PrecheckBlock("invalid_plan_type")
    if plan.get("productive_apply_authorized") is not False:
        raise PrecheckBlock("plan_must_not_authorize_productive_apply")
    return plan


def validate_plan_readiness(plan: dict[str, Any]) -> None:
    blockers: list[str] = []
    if plan.get("status") not in {"SOURCE_PACKAGE_READY_TARGET_IDENTITY_PENDING", "READY_FOR_READ_ONLY_PRECHECK"}:
        blockers.append("plan_status_not_ready")
    target = plan["target"]
    if not target.get("expected_supabase_project_ref") or not target.get("expected_hostname"):
        blockers.append("target_identity_not_registered")
    if not target.get("allowed_productive_roles"):
        blockers.append("productive_role_not_registered")
    if blockers:
        raise PrecheckBlock("plan_blocked:" + ",".join(sorted(set(blockers))))


def _date_range(start: str, end: str) -> list[str]:
    current, final = date.fromisoformat(start), date.fromisoformat(end)
    values: list[str] = []
    while current <= final:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def summarize_approved_workbooks(workbooks: list[Any]) -> dict[str, Any]:
    rows = [row for workbook in workbooks for row in workbook.rows]
    classified = classify_global_photo_duplicates(workbooks)
    canonical_rows = [row for row in classified if row["duplicate_classification"] == "UNIQUE"]
    event_stability: dict[str, tuple[str, str]] = {}
    conflicts: set[str] = set()
    for row in rows:
        value = (row["sp_item_id"], row["event_stable_hash"])
        if row["event_id"] in event_stability and event_stability[row["event_id"]] != value:
            conflicts.add(row["event_id"])
        event_stability.setdefault(row["event_id"], value)
    presence = {(row["fecha"], row["location_key"], row["cliente_norm"]) for row in canonical_rows}
    dates = sorted({row["fecha"] for row in rows})
    semantic_hashes = sorted(semantic_content_hash(workbook) for workbook in workbooks)
    plan_hash = hashlib.sha256(stable_json({
        "runner_version": "017_ROUTE_B_V1",
        "approved_semantic_hashes": semantic_hashes,
        "grain": "immutable_event_photo_staging_row",
    }).encode("utf-8")).hexdigest()
    return {
        "approved_file_count": len(workbooks),
        "expected_source_rows": len(rows),
        "expected_distinct_events": len({row["event_id"] for row in canonical_rows}),
        "expected_duplicate_photo_rows": sum(row["duplicate_classification"] != "UNIQUE" for row in classified),
        "expected_exact_duplicate_rows": sum(row["duplicate_classification"] == "EXACT_DUPLICATE" for row in classified),
        "expected_cross_file_duplicate_rows": sum(row["duplicate_classification"] == "CROSS_FILE_DUPLICATE" for row in classified),
        "expected_event_conflicts": len(conflicts),
        "expected_day_presence_rows": len(presence),
        "coverage_start": dates[0],
        "coverage_end": dates[-1],
        "distinct_dates": len(dates),
        "missing_dates": sorted(set(_date_range(dates[0], dates[-1])) - set(dates)),
        "semantic_plan_hash": plan_hash,
    }


def validate_source_manifest(plan: dict[str, Any], root: Path = ROOT) -> dict[str, Any]:
    input_dir = root / plan["input_directory"]["repository_relative_value"]
    package = plan["source_package"]
    approved = {item["filename"]: item for item in package["approved_files"]}
    excluded = {item["filename"]: item for item in package["excluded_files"]}
    inventory = {item["filename"]: item for item in package["observed_directory_inventory"]}
    if set(approved) & set(excluded):
        raise PrecheckBlock("approved_excluded_filename_overlap")
    if {item["sha256"] for item in approved.values()} & {item["sha256"] for item in excluded.values()}:
        raise PrecheckBlock("approved_excluded_hash_overlap")
    if any(item.get("classification") == "NEGATIVE_TEST_FIXTURE_TRUNCATED" for item in approved.values()):
        raise PrecheckBlock("truncated_fixture_in_approved_manifest")
    actual = {
        path.name: path
        for path in sorted(input_dir.glob(plan["input_directory"]["file_pattern"]))
        if not path.name.startswith("~$")
    }
    unknown = sorted(set(actual) - set(inventory))
    if unknown:
        raise PrecheckBlock("unknown_matching_files:" + ",".join(unknown))
    missing = sorted(set(approved) - set(actual))
    if missing:
        raise PrecheckBlock("missing_approved_files:" + ",".join(missing))
    for filename, path in actual.items():
        expected_hash = inventory[filename]["sha256"]
        if sha256_file(path) != expected_hash:
            raise PrecheckBlock("observed_file_hash_mismatch:" + filename)
    workbooks = []
    for filename, entry in approved.items():
        if filename in excluded:
            raise PrecheckBlock("excluded_file_in_approved_manifest:" + filename)
        workbook = inspect_workbook(actual[filename])
        if workbook.source_file_sha256 != entry["sha256"]:
            raise PrecheckBlock("approved_raw_hash_mismatch:" + filename)
        if semantic_content_hash(workbook) != entry["semantic_content_hash"]:
            raise PrecheckBlock("approved_semantic_hash_mismatch:" + filename)
        for field, observed in (
            ("row_count", len(workbook.rows)),
            ("distinct_event_count", workbook.event_count),
            ("day_presence_count", workbook.day_presence_count),
            ("duplicate_photo_rows", workbook.duplicate_rows),
            ("coverage_start", workbook.coverage_start),
            ("coverage_end", workbook.coverage_end),
        ):
            if entry[field] != observed:
                raise PrecheckBlock(f"approved_metric_mismatch:{filename}:{field}")
        workbooks.append(workbook)
    summary = summarize_approved_workbooks(workbooks)
    approved_bytes = sum(actual[item["filename"]].stat().st_size for item in package["approved_files"])
    excluded_bytes = sum(actual[item["filename"]].stat().st_size for item in package["excluded_files"] if item["filename"] in actual)
    summary.update({
        "approved_source_bytes": approved_bytes,
        "excluded_source_bytes": excluded_bytes,
        "observed_directory_bytes": sum(path.stat().st_size for path in actual.values()),
        "excluded_file_count": len(package["excluded_files"]),
        "observed_file_count": len(actual),
    })
    expected_summary = {
        "approved_file_count": package["approved_file_count"],
        "expected_source_rows": package["expected_source_rows"],
        "expected_distinct_events": package["expected_distinct_events"],
        "expected_duplicate_photo_rows": package["expected_duplicate_photo_rows"],
        "expected_exact_duplicate_rows": package["expected_exact_duplicate_rows"],
        "expected_cross_file_duplicate_rows": package["expected_cross_file_duplicate_rows"],
        "expected_event_conflicts": package["expected_event_conflicts"],
        "expected_day_presence_rows": package["expected_day_presence_rows"],
        "coverage_start": package["expected_coverage"]["start"],
        "coverage_end": package["expected_coverage"]["end"],
        "distinct_dates": package["expected_coverage"]["distinct_dates"],
        "missing_dates": package["expected_coverage"]["missing_dates"],
        "semantic_plan_hash": package["semantic_plan_hash"],
        **package["source_byte_metrics"],
    }
    if summary != expected_summary:
        raise PrecheckBlock("approved_aggregate_mismatch")
    return summary


def validate_local_artifacts(plan: dict[str, Any], root: Path = ROOT) -> dict[str, Any]:
    sql_path = root / plan["physical_contract"]["sql_file"]
    if sha256_lf_normalized(sql_path) != plan["physical_contract"]["sql_sha256"]:
        raise PrecheckBlock("sql_sha256_mismatch")
    return validate_source_manifest(plan, root)


def validate_target(dsn: str | None, env_name: str, plan: dict[str, Any], expected_project_ref: str) -> str:
    if env_name != ALLOWED_DB_ENV or not dsn:
        raise PrecheckBlock("readonly_db_env_required")
    parsed = urlparse(dsn)
    host = (parsed.hostname or "").lower()
    target = plan["target"]
    registered_ref = target.get("expected_supabase_project_ref")
    registered_host = (target.get("expected_hostname") or "").lower()
    if not registered_ref or expected_project_ref != registered_ref:
        raise PrecheckBlock("project_ref_mismatch")
    if host != registered_host or registered_ref not in host or "supabase" not in host:
        raise PrecheckBlock("target_host_mismatch")
    return host


def run_precheck(plan: dict[str, Any], dsn: str, connect_fn: Callable[[str], Any]) -> dict[str, Any]:
    connection = connect_fn(dsn)
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("BEGIN READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '30s'")
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            cursor.execute("SELECT current_user,session_user,current_setting('transaction_read_only'),current_setting('server_version'),current_database(),COALESCE(inet_server_addr()::text,'')")
            current_user, session_user, readonly, server_version, database, server_host = cursor.fetchone()
            if current_user not in plan["target"]["allowed_readonly_roles"] or current_user != EXPECTED_READONLY_ROLE:
                raise PrecheckBlock("readonly_role_mismatch")
            if readonly != "on":
                raise PrecheckBlock("transaction_not_read_only")
            cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name=ANY(%s) ORDER BY schema_name", (plan["physical_contract"]["required_schemas"],))
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.execute("SELECT name,default_version,installed_version FROM pg_available_extensions WHERE name=ANY(%s) ORDER BY name", (plan["physical_contract"]["required_extensions"],))
            extensions = cursor.fetchall()
            cursor.execute("SELECT n.nspname||'.'||c.relname,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('cg_raw','cg_core') AND c.relname LIKE 'kpione_%' ORDER BY 1")
            objects = cursor.fetchall()
            expected_signatures = plan["physical_contract"]["object_signatures"]
            route_b_objects = [(name, kind) for name, kind in objects if name in expected_signatures]
            if route_b_objects:
                expected_kinds = sorted((name, spec["relation_kind"]) for name, spec in expected_signatures.items())
                if sorted(route_b_objects) != expected_kinds:
                    raise PrecheckBlock("route_b_object_set_or_kind_mismatch")
                cursor.execute("SELECT table_schema||'.'||table_name,column_name FROM information_schema.columns WHERE table_schema IN ('cg_raw','cg_core') AND table_name LIKE 'kpione_%' ORDER BY table_schema,table_name,ordinal_position")
                actual_columns: dict[str, list[str]] = {}
                for relation, column in cursor.fetchall():
                    if relation in expected_signatures:
                        actual_columns.setdefault(relation, []).append(column)
                expected_columns = {name: spec["columns"] for name, spec in expected_signatures.items()}
                if actual_columns != expected_columns:
                    raise PrecheckBlock("route_b_object_column_signature_mismatch")
                cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_ingest_batch_v1 WHERE status='ACTIVE'")
                active_batches = cursor.fetchone()[0]
            else:
                active_batches = 0
            cursor.execute("SELECT to_regclass('cg_raw.kpione2_raw')::text")
            legacy_object = cursor.fetchone()[0]
            cursor.execute("SELECT has_schema_privilege(%s,'cg_raw','CREATE'),has_schema_privilege(%s,'cg_core','CREATE')", (plan["target"]["allowed_productive_roles"][0], plan["target"]["allowed_productive_roles"][0]))
            productive_privileges = cursor.fetchone()
        if active_batches:
            raise PrecheckBlock("unexpected_active_route_b_batch")
        return {
            "verdict": "PASS_READ_ONLY_PRECHECK",
            "current_user": current_user,
            "session_user": session_user,
            "transaction_read_only": readonly,
            "server_version": server_version,
            "database": database,
            "server_host_matches_connection": bool(server_host),
            "schemas": schemas,
            "extensions": extensions,
            "route_b_objects": objects,
            "active_batch_count": active_batches,
            "legacy_object": legacy_object,
            "productive_schema_create_privileges": list(productive_privileges),
            **plan["source_package"]["source_byte_metrics"],
            "writes_attempted": False,
        }
    finally:
        connection.rollback()
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="018 KPIONE Route B read-only productive precheck")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--db-url-env", default=ALLOWED_DB_ENV)
    parser.add_argument("--expected-project-ref", required=True)
    parser.add_argument("--report-json", type=Path)
    args = parser.parse_args()
    try:
        plan = load_plan(args.plan)
        validate_plan_readiness(plan)
        validate_local_artifacts(plan)
        dsn = os.environ.get(args.db_url_env)
        validate_target(dsn, args.db_url_env, plan, args.expected_project_ref)
        import psycopg
        report = run_precheck(plan, dsn or "", psycopg.connect)
        rendered = json.dumps(report, indent=2, sort_keys=True)
        print(rendered)
        if args.report_json:
            args.report_json.write_text(rendered + "\n", encoding="utf-8")
        return 0
    except (OSError, ValueError, PrecheckBlock) as exc:
        print(json.dumps({"verdict": "BLOCKED", "error": str(exc), "writes_attempted": False}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
