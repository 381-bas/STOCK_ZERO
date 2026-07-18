from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from psycopg import sql

try:
    from scripts.kpione_route_b_evidence_v1 import (
        EvidenceContractError,
        atomic_write_json,
        validate_run_id,
    )
    from scripts.kpione_route_b_v1 import ADVISORY_LOCK_KEY
    from scripts.precheck_kpione_route_b_018_read_only import (
        EXPECTED_READONLY_ROLE,
        _legacy_snapshot,
        _public_acl_snapshot,
        legacy_structural_identity,
        target_fingerprint,
    )
    from scripts.provision_kpione_route_b_role import (
        ADMIN_DB_ENV,
        EXPECTED_ADMIN_ROLE,
        ProvisioningError,
        validate_admin_dsn_target,
        validate_admin_git_guard,
        validate_provisioning_plan,
    )
except ModuleNotFoundError:
    from kpione_route_b_evidence_v1 import (
        EvidenceContractError,
        atomic_write_json,
        validate_run_id,
    )
    from kpione_route_b_v1 import ADVISORY_LOCK_KEY
    from precheck_kpione_route_b_018_read_only import (
        EXPECTED_READONLY_ROLE,
        _legacy_snapshot,
        _public_acl_snapshot,
        legacy_structural_identity,
        target_fingerprint,
    )
    from provision_kpione_route_b_role import (
        ADMIN_DB_ENV,
        EXPECTED_ADMIN_ROLE,
        ProvisioningError,
        validate_admin_dsn_target,
        validate_admin_git_guard,
        validate_provisioning_plan,
    )


ROOT = Path(__file__).resolve().parents[1]
DOCUMENT_TYPE = "stock_zero_route_b_readonly_observer_grants_reconciliation_v1"
CONFIRM_TOKEN = "STOCK_ZERO_022_RECONCILE_ROUTE_B_READONLY_OBSERVER"
MAINTENANCE_LOCK_KEY = ADVISORY_LOCK_KEY + 22
ROUTE_B_SEQUENCE_TABLE = "cg_raw.kpione_raw_event_photo_staging_v1"
ROUTE_B_SEQUENCE_COLUMN = "staging_id"
TABLE_PRIVILEGES = (
    "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER",
)
SEQUENCE_PRIVILEGES = ("USAGE", "SELECT", "UPDATE")


class ObserverReconciliationError(RuntimeError):
    pass


def _maintenance_evidence_path(path: Path, root: Path, maintenance_run_id: str) -> Path:
    validate_run_id(maintenance_run_id)
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve()
    expected = (
        root / "evidence" / "runtime" / "022" / maintenance_run_id
        / "01_route_b_readonly_observer_grants.json"
    ).resolve()
    if resolved != expected:
        raise ObserverReconciliationError("maintenance_evidence_path_not_canonical")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    if resolved.exists():
        raise ObserverReconciliationError("maintenance_evidence_already_exists")
    return resolved


def _load_plan_from_authority(authority: Mapping[str, Any]) -> dict[str, Any]:
    plan_blob = authority.get("_plan_blob")
    if not isinstance(plan_blob, bytes):
        raise ObserverReconciliationError("authority_plan_blob_missing")
    plan = json.loads(plan_blob.decode("utf-8"))
    validate_provisioning_plan(plan)
    return plan


def _physical_route_b_signatures(cursor: Any, plan: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    expected = plan["physical_contract"]["object_signatures"]
    cursor.execute(
        "SELECT n.nspname||'.'||c.relname,c.relkind,a.attname FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
        "WHERE n.nspname||'.'||c.relname=ANY(%s) "
        "ORDER BY n.nspname,c.relname,a.attnum",
        (list(expected),),
    )
    observed: dict[str, dict[str, Any]] = {}
    for relation, relation_kind, column in cursor.fetchall():
        entry = observed.setdefault(relation, {"relation_kind": relation_kind, "columns": []})
        if column is not None:
            entry["columns"].append(column)
    return observed


def _signature_diffs(
    observed: Mapping[str, Mapping[str, Any]],
    plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    expected = plan["physical_contract"]["object_signatures"]
    diffs: list[dict[str, Any]] = []
    for name in sorted(expected):
        expected_spec = expected[name]
        actual_spec = observed.get(name, {})
        signature_match = (
            actual_spec.get("relation_kind") == expected_spec["relation_kind"]
            and actual_spec.get("columns", []) == expected_spec["columns"]
        )
        if not signature_match:
            diffs.append({
                "object": name,
                "expected_relation_kind": expected_spec["relation_kind"],
                "actual_relation_kind": actual_spec.get("relation_kind"),
                "expected_columns": expected_spec["columns"],
                "actual_columns": actual_spec.get("columns", []),
                "signature_match": False,
            })
    return diffs


def _privilege_snapshot(cursor: Any, role: str, plan: Mapping[str, Any], sequence_name: str | None) -> dict[str, Any]:
    objects = sorted(plan["physical_contract"]["object_signatures"])
    schemas = ("cg_raw", "cg_core")
    schema_privileges: dict[str, dict[str, bool]] = {}
    for schema_name in schemas:
        cursor.execute(
            "SELECT has_schema_privilege(%s,%s,'USAGE'),has_schema_privilege(%s,%s,'CREATE')",
            (role, schema_name, role, schema_name),
        )
        usage, create = cursor.fetchone()
        schema_privileges[schema_name] = {"USAGE": bool(usage), "CREATE": bool(create)}
    table_privileges: dict[str, dict[str, bool]] = {}
    for name in objects:
        item: dict[str, bool] = {}
        for privilege in TABLE_PRIVILEGES:
            cursor.execute("SELECT has_table_privilege(%s,%s,%s)", (role, name, privilege))
            item[privilege] = bool(cursor.fetchone()[0])
        table_privileges[name] = item
    sequence_privileges: dict[str, bool] = {}
    if sequence_name:
        for privilege in SEQUENCE_PRIVILEGES:
            cursor.execute("SELECT has_sequence_privilege(%s,%s,%s)", (role, sequence_name, privilege))
            sequence_privileges[privilege] = bool(cursor.fetchone()[0])
    else:
        sequence_privileges = {privilege: False for privilege in SEQUENCE_PRIVILEGES}
    return {
        "schemas": schema_privileges,
        "objects": table_privileges,
        "sequence": {
            "name": sequence_name,
            "privileges": sequence_privileges,
        },
    }


def _role_attributes(cursor: Any, role: str) -> dict[str, Any]:
    cursor.execute(
        "SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,"
        "rolbypassrls,rolinherit,rolconnlimit FROM pg_roles WHERE rolname=%s",
        (role,),
    )
    row = cursor.fetchone()
    if row is None:
        raise ObserverReconciliationError("readonly_observer_role_missing")
    return {
        "login": row[0],
        "superuser": row[1],
        "createdb": row[2],
        "createrole": row[3],
        "replication": row[4],
        "bypassrls": row[5],
        "inherit": row[6],
        "connection_limit": row[7],
    }


def _route_b_sequence(cursor: Any) -> str | None:
    cursor.execute(
        "SELECT pg_get_serial_sequence(%s,%s)",
        (ROUTE_B_SEQUENCE_TABLE, ROUTE_B_SEQUENCE_COLUMN),
    )
    return cursor.fetchone()[0]


def _is_compliant(snapshot: Mapping[str, Any], plan: Mapping[str, Any]) -> bool:
    if any(not item.get("USAGE") or item.get("CREATE") for item in snapshot["schemas"].values()):
        return False
    for privileges in snapshot["objects"].values():
        if privileges.get("SELECT") is not True:
            return False
        for privilege in TABLE_PRIVILEGES:
            if privilege != "SELECT" and privileges.get(privilege) is not False:
                return False
    if any(snapshot["sequence"]["privileges"].get(privilege) for privilege in SEQUENCE_PRIVILEGES):
        return False
    expected_objects = set(plan["physical_contract"]["object_signatures"])
    return set(snapshot["objects"]) == expected_objects


def _grant_observer_privileges(cursor: Any, role: str, plan: Mapping[str, Any]) -> None:
    cursor.execute(
        sql.SQL("GRANT USAGE ON SCHEMA cg_raw,cg_core TO {}").format(sql.Identifier(role))
    )
    for name in sorted(plan["physical_contract"]["object_signatures"]):
        schema_name, relation_name = name.split(".", 1)
        cursor.execute(
            sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(
                sql.Identifier(schema_name, relation_name), sql.Identifier(role),
            )
        )


def _admin_identity_check(cursor: Any, expected_admin_username: str, plan: Mapping[str, Any]) -> None:
    cursor.execute(
        "SELECT current_user,session_user,current_database(),current_setting('transaction_read_only')"
    )
    current_user, session_user, database, readonly = cursor.fetchone()
    if current_user != expected_admin_username or session_user != expected_admin_username:
        raise ObserverReconciliationError("admin_session_role_mismatch")
    if database != plan["target"]["expected_database"]:
        raise ObserverReconciliationError("admin_session_database_mismatch")
    if readonly not in {"on", "off"}:
        raise ObserverReconciliationError("admin_session_readonly_state_invalid")


def reconcile_readonly_observer(
    plan: dict[str, Any],
    dsn: str,
    evidence_json: Path,
    *,
    maintenance_run_id: str,
    git_guard: Mapping[str, str],
    connect_fn: Callable[[str], Any] | None = None,
    expected_admin_username: str = EXPECTED_ADMIN_ROLE,
) -> dict[str, Any]:
    validate_run_id(maintenance_run_id)
    validate_provisioning_plan(plan)
    validate_admin_dsn_target(dsn, plan, expected_admin_username=expected_admin_username)
    if connect_fn is None:
        import psycopg
        connect_fn = psycopg.connect
    relations = list(plan["physical_contract"]["object_signatures"]) + ["cg_raw.kpione2_raw"]
    diagnostic_connection = connect_fn(dsn)
    try:
        diagnostic_connection.autocommit = False
        with diagnostic_connection.cursor() as cursor:
            cursor.execute("BEGIN READ ONLY")
            _admin_identity_check(cursor, expected_admin_username, plan)
            observed_signatures = _physical_route_b_signatures(cursor, plan)
            signature_diffs = _signature_diffs(observed_signatures, plan)
            sequence_name = _route_b_sequence(cursor)
            before_privileges = _privilege_snapshot(cursor, EXPECTED_READONLY_ROLE, plan, sequence_name)
            before_legacy = _legacy_snapshot(cursor)
            before_public_acl = _public_acl_snapshot(cursor, relations)
            before_attributes = _role_attributes(cursor, EXPECTED_READONLY_ROLE)
        diagnostic_connection.rollback()
    finally:
        diagnostic_connection.close()
    if signature_diffs:
        raise ObserverReconciliationError(json.dumps({
            "verdict": "BLOCKED_PHYSICAL_ROUTE_B_SIGNATURE_MISMATCH",
            "physical_signatures_match": False,
            "mismatched_objects": signature_diffs,
        }, sort_keys=True))

    if _is_compliant(before_privileges, plan):
        report = {
            "document_type": DOCUMENT_TYPE,
            "maintenance_run_id": maintenance_run_id,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "verdict": "PASS_READONLY_OBSERVER_ALREADY_COMPLIANT",
            "approved_git_sha": git_guard["approved_git_sha"],
            "plan_sha256": git_guard["plan_sha256"],
            "sql_sha256": plan["physical_contract"]["sql_sha256"],
            "target_fingerprint": target_fingerprint(plan),
            "physical_signatures_match": True,
            "before_privileges": before_privileges,
            "after_privileges": before_privileges,
            "readonly_observer_attributes_before": before_attributes,
            "readonly_observer_attributes_after": before_attributes,
            "legacy_unchanged": True,
            "public_acl_unchanged": True,
            "writes_attempted": False,
            "committed": False,
            "secrets_printed": False,
            "dsn_printed": False,
        }
        atomic_write_json(evidence_json, report)
        return report

    connection = connect_fn(dsn)
    committed = False
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '15min'")
            cursor.execute("SET LOCAL lock_timeout = '10s'")
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (MAINTENANCE_LOCK_KEY,))
            _grant_observer_privileges(cursor, EXPECTED_READONLY_ROLE, plan)
            sequence_name = _route_b_sequence(cursor)
            after_privileges = _privilege_snapshot(cursor, EXPECTED_READONLY_ROLE, plan, sequence_name)
            after_legacy = _legacy_snapshot(cursor)
            after_public_acl = _public_acl_snapshot(cursor, relations)
            after_attributes = _role_attributes(cursor, EXPECTED_READONLY_ROLE)
            if not _is_compliant(after_privileges, plan):
                raise ObserverReconciliationError("readonly_observer_privileges_not_exact")
            if legacy_structural_identity(after_legacy) != legacy_structural_identity(before_legacy):
                raise ObserverReconciliationError("legacy_structural_drift")
            if after_public_acl != before_public_acl:
                raise ObserverReconciliationError("public_acl_drift")
            if after_attributes != before_attributes:
                raise ObserverReconciliationError("readonly_observer_attributes_changed")
        connection.commit()
        committed = True
    except Exception:
        if not committed:
            connection.rollback()
        raise
    finally:
        connection.close()
    report = {
        "document_type": DOCUMENT_TYPE,
        "maintenance_run_id": maintenance_run_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "verdict": "PASS_READONLY_OBSERVER_GRANTS_RECONCILED",
        "approved_git_sha": git_guard["approved_git_sha"],
        "plan_sha256": git_guard["plan_sha256"],
        "sql_sha256": plan["physical_contract"]["sql_sha256"],
        "target_fingerprint": target_fingerprint(plan),
        "physical_signatures_match": True,
        "before_privileges": before_privileges,
        "after_privileges": after_privileges,
        "readonly_observer_attributes_before": before_attributes,
        "readonly_observer_attributes_after": after_attributes,
        "legacy_unchanged": True,
        "public_acl_unchanged": True,
        "writes_attempted": True,
        "committed": True,
        "secrets_printed": False,
        "dsn_printed": False,
    }
    atomic_write_json(evidence_json, report)
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Reconcile Route B read-only observer grants")
    result.add_argument("--plan", type=Path, required=True)
    result.add_argument("--maintenance-run-id", required=True)
    result.add_argument("--expected-plan-git-ref", required=True)
    result.add_argument("--db-url-env", required=True)
    result.add_argument("--expected-project-ref", required=True)
    result.add_argument("--confirm", required=True)
    result.add_argument("--evidence-json", type=Path, required=True)
    result.add_argument("--authority-precheck-only", action="store_true")
    return result


def execute_cli(args: argparse.Namespace, environ: Mapping[str, str] = os.environ) -> dict[str, Any]:
    if args.db_url_env != ADMIN_DB_ENV:
        raise ObserverReconciliationError("admin_db_url_env_required")
    if args.confirm != CONFIRM_TOKEN:
        raise ObserverReconciliationError("observer_reconciliation_confirmation_required")
    authority = validate_admin_git_guard(args.plan, args.expected_plan_git_ref, ROOT)
    plan = _load_plan_from_authority(authority)
    if args.expected_project_ref != plan["target"]["expected_supabase_project_ref"]:
        raise ObserverReconciliationError("expected_project_ref_mismatch")
    public_guard = {key: value for key, value in authority.items() if not key.startswith("_")}
    evidence_path = _maintenance_evidence_path(args.evidence_json, ROOT, args.maintenance_run_id)
    if args.authority_precheck_only:
        return {
            "verdict": "PASS_READONLY_OBSERVER_AUTHORITY_PRECHECK",
            **public_guard,
            "connection_attempted": False,
            "writes_attempted": False,
            "committed": False,
        }
    dsn = environ.get(ADMIN_DB_ENV)
    if not dsn:
        raise ObserverReconciliationError("admin_dsn_missing")
    return reconcile_readonly_observer(
        plan,
        dsn,
        evidence_path,
        maintenance_run_id=args.maintenance_run_id,
        git_guard=public_guard,
    )


def main() -> int:
    args = parser().parse_args()
    try:
        print(json.dumps(execute_cli(args), sort_keys=True))
        return 0
    except (OSError, ValueError, json.JSONDecodeError, EvidenceContractError,
            ProvisioningError, ObserverReconciliationError) as exc:
        identifier = str(exc)
        try:
            payload = json.loads(identifier)
            if isinstance(payload, dict):
                payload.setdefault("writes_attempted", False)
                print(json.dumps(payload, sort_keys=True))
                return 2
        except json.JSONDecodeError:
            pass
        print(json.dumps({
            "verdict": "BLOCKED",
            "error": identifier,
            "writes_attempted": False,
            "committed": False,
            "secrets_printed": False,
            "dsn_printed": False,
        }, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
