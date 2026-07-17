from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import parse_qs, urlparse

from psycopg import sql

try:
    from .kpione_route_b_evidence_v1 import (
        EvidenceContractError,
        atomic_write_json,
        prepare_run_directory,
        require_canonical_evidence_path,
        validate_run_id,
    )
    from .kpione_route_b_v1 import (
        ADVISORY_LOCK_KEY,
        EXPECTED_PRODUCTIVE_DATABASE,
        EXPECTED_PRODUCTIVE_HOSTNAME,
        EXPECTED_PRODUCTIVE_PROJECT_REF,
        PLANNED_PRODUCTIVE_ROLE,
        _assert_route_b_object_signatures,
        sha256_lf_normalized,
    )
    from .precheck_kpione_route_b_018_read_only import (
        _legacy_snapshot,
        _public_acl_snapshot,
        legacy_structural_identity,
        target_fingerprint,
    )
except ImportError:
    from kpione_route_b_evidence_v1 import (
        EvidenceContractError,
        atomic_write_json,
        prepare_run_directory,
        require_canonical_evidence_path,
        validate_run_id,
    )
    from kpione_route_b_v1 import (
        ADVISORY_LOCK_KEY,
        EXPECTED_PRODUCTIVE_DATABASE,
        EXPECTED_PRODUCTIVE_HOSTNAME,
        EXPECTED_PRODUCTIVE_PROJECT_REF,
        PLANNED_PRODUCTIVE_ROLE,
        _assert_route_b_object_signatures,
        sha256_lf_normalized,
    )
    from precheck_kpione_route_b_018_read_only import (
        _legacy_snapshot,
        _public_acl_snapshot,
        legacy_structural_identity,
        target_fingerprint,
    )


ADMIN_DB_ENV = "DB_URL_ADMIN"
PRODUCTIVE_ROLE_PASSWORD_ENV = "KPIONE_ROUTE_B_PRODUCTIVE_PASSWORD"
PROVISION_CONFIRM_TOKEN = "STOCK_ZERO_019_PROVISION_ROUTE_B_ROLE"
RECONCILE_CONFIRM_TOKEN = "STOCK_ZERO_020B_RECONCILE_PROVISIONING_EVIDENCE"
EXPECTED_ADMIN_ROLE = "postgres"
PRODUCTIVE_CONNECTION_LIMIT = 5
PROVISION_LOCK_KEY = ADVISORY_LOCK_KEY + 19
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PLAN = ROOT / "plans" / "018_kpione_route_b_productive_apply_plan.json"
ROUTE_B_SQL_FILE = "sql/17_kpione_route_b_ingestion_v1.sql"
ROUTE_B_OBJECTS = {
    "cg_raw.kpione_raw_ingest_batch_v1",
    "cg_raw.kpione_raw_ingest_batch_file_v1",
    "cg_raw.kpione_raw_event_photo_staging_v1",
    "cg_core.kpione_event_normalized_v1",
    "cg_core.kpione_day_presence_v1",
}


class ProvisioningError(RuntimeError):
    def __init__(self, identifier: str, *, connection_attempted: bool = False,
                 writes_attempted: bool = False, committed: bool = False,
                 report: dict[str, Any] | None = None,
                 failed_stage: str | None = None,
                 exception_type: str | None = None,
                 sqlstate: str | None = None,
                 fixed_error_category: str | None = None) -> None:
        super().__init__(identifier)
        self.connection_attempted = connection_attempted
        self.writes_attempted = writes_attempted
        self.committed = committed
        self.report = report or {}
        self.failed_stage = failed_stage
        self.exception_type = exception_type
        self.sqlstate = sqlstate
        self.fixed_error_category = fixed_error_category


def _sqlstate(exc: BaseException) -> str | None:
    value = getattr(exc, "sqlstate", None)
    return value if isinstance(value, str) and value else None


def _fixed_error_category(exc: BaseException) -> str:
    sqlstate = _sqlstate(exc)
    if sqlstate == "42501":
        return "INSUFFICIENT_PRIVILEGE"
    if sqlstate == "42710":
        return "OBJECT_ALREADY_EXISTS"
    if sqlstate == "42P07":
        return "RELATION_ALREADY_EXISTS"
    if sqlstate == "42704":
        return "OBJECT_NOT_FOUND"
    if sqlstate == "0A000":
        return "FEATURE_NOT_SUPPORTED"
    if sqlstate == "23505":
        return "UNIQUE_VIOLATION"
    if sqlstate == "23P01":
        return "EXCLUSION_VIOLATION"
    if sqlstate:
        return "DATABASE_ERROR"
    return "UNCLASSIFIED_RUNTIME_ERROR"


def _run_git(root: Path, *args: str, text: bool = True) -> subprocess.CompletedProcess[Any]:
    return subprocess.run(
        ["git", *args], cwd=root, capture_output=True, text=text, check=False,
    )


def _repository_relative(path: Path, root: Path, error: str) -> str:
    try:
        return path.resolve().relative_to(root.resolve(strict=True)).as_posix()
    except ValueError as exc:
        raise ProvisioningError(error) from exc


def _head_blob(root: Path, relative_path: str, *, label: str) -> bytes:
    tracked = _run_git(root, "cat-file", "-e", f"HEAD:{relative_path}")
    if tracked.returncode != 0:
        raise ProvisioningError(f"{label}_not_tracked_at_head")
    result = _run_git(root, "show", f"HEAD:{relative_path}", text=False)
    if result.returncode != 0:
        raise ProvisioningError(f"{label}_blob_unavailable")
    return result.stdout


def _sha256_lf_bytes(value: bytes) -> str:
    return hashlib.sha256(value.replace(b"\r\n", b"\n")).hexdigest()


def _lf_normalized_bytes(value: bytes) -> bytes:
    return value.replace(b"\r\n", b"\n")


def _validate_admin_plan_git_guard(
    plan_path: Path,
    expected_git_ref: str,
    root: Path = ROOT,
) -> dict[str, Any]:
    if not expected_git_ref or not re.fullmatch(r"[0-9a-f]{40}", expected_git_ref):
        raise ProvisioningError("expected_plan_git_ref_required")
    head_result = _run_git(root, "rev-parse", "HEAD")
    if head_result.returncode != 0:
        raise ProvisioningError("repository_head_unavailable")
    head = head_result.stdout.strip()
    if head != expected_git_ref:
        raise ProvisioningError("repository_head_mismatch")

    unstaged = _run_git(root, "diff", "--quiet")
    untracked = _run_git(root, "ls-files", "--others", "--exclude-standard")
    if unstaged.returncode not in {0, 1} or untracked.returncode != 0:
        raise ProvisioningError("repository_worktree_status_unavailable")
    if unstaged.returncode == 1 or untracked.stdout.strip():
        raise ProvisioningError("repository_worktree_not_clean")
    staged = _run_git(root, "diff", "--cached", "--quiet")
    if staged.returncode not in {0, 1}:
        raise ProvisioningError("repository_index_status_unavailable")
    if staged.returncode == 1:
        raise ProvisioningError("repository_index_not_clean")

    plan_path = plan_path if plan_path.is_absolute() else root / plan_path
    plan_relative = _repository_relative(plan_path, root, "approved_plan_outside_repository")
    plan_blob = _head_blob(root, plan_relative, label="approved_plan")
    if (not plan_path.is_file()
            or _lf_normalized_bytes(plan_path.read_bytes()) != _lf_normalized_bytes(plan_blob)):
        raise ProvisioningError("approved_plan_worktree_blob_mismatch")
    return {
        "approved_git_sha": head,
        "plan_path": plan_relative,
        "plan_sha256": hashlib.sha256(plan_blob).hexdigest(),
        "_plan_blob": plan_blob,
    }


def _validate_admin_sql_blob_guard(authority: dict[str, Any], root: Path) -> dict[str, Any]:
    ddl_path = root / ROUTE_B_SQL_FILE
    ddl_relative = _repository_relative(ddl_path, root, "ddl_path_outside_repository")
    ddl_blob = _head_blob(root, ddl_relative, label="route_b_sql")
    if (not ddl_path.is_file()
            or _lf_normalized_bytes(ddl_path.read_bytes()) != _lf_normalized_bytes(ddl_blob)):
        raise ProvisioningError("route_b_sql_worktree_blob_mismatch")
    return {
        **authority,
        "ddl_path": ddl_relative,
        "ddl_sha256": _sha256_lf_bytes(ddl_blob),
        "_ddl_blob": ddl_blob,
    }


def validate_admin_git_guard(
    plan_path: Path,
    expected_git_ref: str,
    root: Path = ROOT,
) -> dict[str, Any]:
    authority = _validate_admin_plan_git_guard(plan_path, expected_git_ref, root)
    return _validate_admin_sql_blob_guard(authority, root)


def validate_admin_dsn_target(
    dsn: str,
    plan: dict[str, Any],
    *,
    expected_admin_username: str = EXPECTED_ADMIN_ROLE,
) -> dict[str, str]:
    parsed = urlparse(dsn)
    params = parse_qs(parsed.query, keep_blank_values=True)
    username = parsed.username or ""
    hostname = (parsed.hostname or "").lower()
    database = parsed.path.lstrip("/")
    sslmodes = params.get("sslmode", [])
    target = plan.get("target", {})
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ProvisioningError("admin_dsn_scheme_mismatch")
    if username != expected_admin_username:
        raise ProvisioningError("admin_credential_role_mismatch")
    if not parsed.password:
        raise ProvisioningError("admin_dsn_password_required")
    if hostname != target.get("expected_hostname"):
        raise ProvisioningError("admin_target_hostname_mismatch")
    if target.get("expected_supabase_project_ref") not in hostname:
        raise ProvisioningError("admin_target_project_ref_mismatch")
    if database != target.get("expected_database"):
        raise ProvisioningError("admin_target_database_mismatch")
    if sslmodes != ["require"]:
        raise ProvisioningError("admin_sslmode_require_required")
    return {
        "credential_class": "admin-provisioning",
        "username": username,
        "hostname": hostname,
        "database": database,
        "sslmode": "require",
    }


def validate_provisioning_plan(plan: dict[str, Any]) -> None:
    target = plan.get("target", {})
    physical = plan.get("physical_contract", {})
    if plan.get("document_type") != "kpione_route_b_productive_apply_plan":
        raise ProvisioningError("invalid_productive_plan_type")
    if target.get("planned_productive_role") != PLANNED_PRODUCTIVE_ROLE:
        raise ProvisioningError("planned_productive_role_mismatch")
    if target.get("expected_supabase_project_ref") != EXPECTED_PRODUCTIVE_PROJECT_REF:
        raise ProvisioningError("registered_target_project_ref_mismatch")
    if target.get("expected_hostname") != EXPECTED_PRODUCTIVE_HOSTNAME:
        raise ProvisioningError("registered_target_hostname_mismatch")
    if target.get("expected_database") != EXPECTED_PRODUCTIVE_DATABASE:
        raise ProvisioningError("registered_target_database_mismatch")
    if physical.get("sql_file") != ROUTE_B_SQL_FILE:
        raise ProvisioningError("route_b_sql_file_mismatch")
    if set(physical.get("objects", [])) != ROUTE_B_OBJECTS:
        raise ProvisioningError("route_b_object_scope_mismatch")
    if set(physical.get("object_signatures", {})) != ROUTE_B_OBJECTS:
        raise ProvisioningError("route_b_signature_scope_mismatch")


def _qualified_identifier(name: str) -> sql.Composed:
    parts = name.split(".")
    if len(parts) != 2 or not all(parts):
        raise ProvisioningError("route_b_object_name_invalid")
    return sql.Identifier(*parts)


def _connect(dsn: str, connect_fn: Callable[[str], Any] | None) -> Any:
    if connect_fn is None:
        import psycopg
        connect_fn = psycopg.connect
    return connect_fn(dsn)


def _load_plan_blob(value: bytes) -> dict[str, Any]:
    plan = json.loads(value.decode("utf-8"))
    if plan.get("document_type") != "kpione_route_b_productive_apply_plan":
        raise ProvisioningError("invalid_productive_plan_type")
    return plan


def _load_ddl(plan: dict[str, Any], root: Path) -> tuple[str, str]:
    relative = Path(plan["physical_contract"]["sql_file"])
    path = (root / relative).resolve(strict=True)
    resolved_root = root.resolve(strict=True)
    if resolved_root not in path.parents:
        raise ProvisioningError("ddl_path_outside_repository")
    expected = plan["physical_contract"]["sql_sha256"]
    if sha256_lf_normalized(path) != expected:
        raise ProvisioningError("sql_sha256_mismatch")
    return path.read_text(encoding="utf-8"), expected


def _role_statement(role: str, password: str) -> sql.Composed:
    return sql.SQL(
        "ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
        "NOREPLICATION NOBYPASSRLS NOINHERIT CONNECTION LIMIT {} PASSWORD {}"
    ).format(
        sql.Identifier(role),
        sql.Literal(PRODUCTIVE_CONNECTION_LIMIT),
        sql.Literal(password),
    )


def provision_route_b_role(
    plan: dict[str, Any],
    dsn: str,
    role_password: str,
    evidence_json: Path,
    *,
    root: Path = ROOT,
    connect_fn: Callable[[str], Any] | None = None,
    expected_admin_username: str = EXPECTED_ADMIN_ROLE,
    git_guard: Mapping[str, str],
    ddl: str | None = None,
    run_id: str,
) -> dict[str, Any]:
    validate_run_id(run_id)
    validate_provisioning_plan(plan)
    if not git_guard.get("approved_git_sha") or not git_guard.get("plan_sha256"):
        raise ProvisioningError("admin_git_guard_required")
    target = validate_admin_dsn_target(
        dsn, plan, expected_admin_username=expected_admin_username,
    )
    if not role_password or len(role_password) < 16:
        raise ProvisioningError("productive_role_password_invalid")
    if ddl is None:
        ddl, ddl_sha256 = _load_ddl(plan, root)
    else:
        ddl_sha256 = git_guard.get("ddl_sha256", "")
        if ddl_sha256 != plan["physical_contract"]["sql_sha256"]:
            raise ProvisioningError("sql_sha256_mismatch")
    connection: Any | None = None
    writes_attempted = False
    committed = False
    role_created = False
    legacy_before: str | None = None
    legacy_after: str | None = None
    legacy_snapshot_before: dict[str, Any] = {}
    legacy_snapshot_after: dict[str, Any] = {}
    public_acl_before: dict[str, Any] = {}
    public_acl_after: dict[str, Any] = {}
    sequence_name: str | None = None
    current_stage = "bootstrap"
    try:
        current_stage = "admin_connect"
        connection = _connect(dsn, connect_fn)
        connection.autocommit = False
        with connection.cursor() as cursor:
            current_stage = "session_identity"
            cursor.execute(
                "SELECT current_user,session_user,current_database(),"
                "current_setting('transaction_read_only')"
            )
            current_user, session_user, database, readonly = cursor.fetchone()
            if current_user != expected_admin_username or session_user != expected_admin_username:
                raise ProvisioningError("admin_session_role_mismatch")
            if database != plan["target"]["expected_database"]:
                raise ProvisioningError("admin_session_database_mismatch")
            if readonly != "off":
                raise ProvisioningError("admin_session_read_write_required")
            cursor.execute("SET LOCAL statement_timeout = '15min'")
            cursor.execute("SET LOCAL lock_timeout = '10s'")
            current_stage = "advisory_lock"
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (PROVISION_LOCK_KEY,))
            current_stage = "legacy_snapshot"
            cursor.execute("SELECT to_regclass('cg_raw.kpione2_raw')::text")
            legacy_before = cursor.fetchone()[0]
            legacy_snapshot_before = _legacy_snapshot(cursor)
            acl_relations = list(plan["physical_contract"]["object_signatures"]) + [
                "cg_raw.kpione2_raw"
            ]
            current_stage = "public_acl_snapshot"
            public_acl_before = _public_acl_snapshot(cursor, acl_relations)
            current_stage = "role_existence_check"
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (PLANNED_PRODUCTIVE_ROLE,))
            if cursor.fetchone() is not None:
                raise ProvisioningError(
                    "productive_role_exists_password_rotation_not_authorized"
                )
            writes_attempted = True
            current_stage = "create_role"
            cursor.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(PLANNED_PRODUCTIVE_ROLE)))
            role_created = True
            writes_attempted = True
            current_stage = "alter_role_attributes"
            cursor.execute(_role_statement(PLANNED_PRODUCTIVE_ROLE, role_password))
            current_stage = "ddl_apply"
            cursor.execute(ddl)
            current_stage = "object_signature_validation"
            _assert_route_b_object_signatures(cursor, plan, allow_empty=False)

            tables = [
                name for name, spec in plan["physical_contract"]["object_signatures"].items()
                if spec["relation_kind"] == "r"
            ]
            views = [
                name for name, spec in plan["physical_contract"]["object_signatures"].items()
                if spec["relation_kind"] == "v"
            ]
            current_stage = "ownership_tables"
            for name in tables:
                cursor.execute(
                    sql.SQL("ALTER TABLE {} OWNER TO {}").format(
                        _qualified_identifier(name), sql.Identifier(expected_admin_username),
                    )
                )
            current_stage = "ownership_views"
            for name in views:
                cursor.execute(
                    sql.SQL("ALTER VIEW {} OWNER TO {}").format(
                        _qualified_identifier(name), sql.Identifier(expected_admin_username),
                    )
                )
            current_stage = "sequence_resolution"
            cursor.execute(
                "SELECT pg_get_serial_sequence(%s,%s)",
                ("cg_raw.kpione_raw_event_photo_staging_v1", "staging_id"),
            )
            sequence_name = cursor.fetchone()[0]
            if not sequence_name:
                raise ProvisioningError("route_b_identity_sequence_missing")
            current_stage = "sequence_ownership"
            cursor.execute(
                sql.SQL("ALTER SEQUENCE {} OWNER TO {}").format(
                    _qualified_identifier(sequence_name), sql.Identifier(expected_admin_username),
                )
            )

            current_stage = "schema_privileges"
            cursor.execute(
                sql.SQL("REVOKE CREATE ON SCHEMA cg_raw,cg_core FROM {}").format(
                    sql.Identifier(PLANNED_PRODUCTIVE_ROLE)
                )
            )
            cursor.execute(
                sql.SQL("GRANT USAGE ON SCHEMA cg_raw,cg_core TO {}").format(
                    sql.Identifier(PLANNED_PRODUCTIVE_ROLE)
                )
            )
            current_stage = "table_privileges"
            for name in tables + views:
                identifier = _qualified_identifier(name)
                cursor.execute(
                    sql.SQL("REVOKE ALL PRIVILEGES ON TABLE {} FROM {}").format(
                        identifier, sql.Identifier(PLANNED_PRODUCTIVE_ROLE),
                    )
                )
            role_identifier = sql.Identifier(PLANNED_PRODUCTIVE_ROLE)
            batch_identifier = _qualified_identifier("cg_raw.kpione_raw_ingest_batch_v1")
            current_stage = "table_privileges"
            cursor.execute(
                sql.SQL("GRANT SELECT,INSERT ON TABLE {} TO {}").format(
                    batch_identifier, role_identifier,
                )
            )
            cursor.execute(
                sql.SQL("GRANT UPDATE(status,activated_at,rolled_back_at) ON TABLE {} TO {}").format(
                    batch_identifier, role_identifier,
                )
            )
            for name in (
                "cg_raw.kpione_raw_ingest_batch_file_v1",
                "cg_raw.kpione_raw_event_photo_staging_v1",
            ):
                cursor.execute(
                    sql.SQL("GRANT SELECT,INSERT ON TABLE {} TO {}").format(
                        _qualified_identifier(name), role_identifier,
                    )
                )
            for name in views:
                current_stage = "view_privileges"
                cursor.execute(
                    sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(
                        _qualified_identifier(name), sql.Identifier(PLANNED_PRODUCTIVE_ROLE),
                    )
                )
            sequence_identifier = _qualified_identifier(sequence_name)
            current_stage = "sequence_privileges"
            cursor.execute(
                sql.SQL("REVOKE ALL PRIVILEGES ON SEQUENCE {} FROM {}").format(
                    sequence_identifier, sql.Identifier(PLANNED_PRODUCTIVE_ROLE),
                )
            )
            cursor.execute(
                sql.SQL("GRANT USAGE ON SEQUENCE {} TO {}").format(
                    sequence_identifier, sql.Identifier(PLANNED_PRODUCTIVE_ROLE),
                )
            )
            current_stage = "role_attribute_validation"
            cursor.execute(
                "SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,"
                "rolbypassrls,rolconnlimit FROM pg_roles WHERE rolname=%s",
                (PLANNED_PRODUCTIVE_ROLE,),
            )
            role_attributes = cursor.fetchone()
            if role_attributes != (True, False, False, False, False, False, PRODUCTIVE_CONNECTION_LIMIT):
                raise ProvisioningError("productive_role_attributes_mismatch")
            current_stage = "legacy_validation"
            cursor.execute("SELECT to_regclass('cg_raw.kpione2_raw')::text")
            legacy_after = cursor.fetchone()[0]
            legacy_snapshot_after = _legacy_snapshot(cursor)
            current_stage = "public_acl_validation"
            public_acl_after = _public_acl_snapshot(cursor, acl_relations)
            current_stage = "legacy_validation"
            if legacy_structural_identity(legacy_snapshot_after) != legacy_structural_identity(
                legacy_snapshot_before
            ):
                raise ProvisioningError("legacy_structural_state_changed")
            current_stage = "public_acl_validation"
            if public_acl_after != public_acl_before:
                raise ProvisioningError("public_acl_changed")
        current_stage = "commit"
        connection.commit()
        committed = True
    except ProvisioningError as exc:
        if connection is not None and not committed:
            connection.rollback()
        raise ProvisioningError(
            str(exc), connection_attempted=connection is not None,
            writes_attempted=writes_attempted, committed=committed,
            failed_stage=getattr(exc, "failed_stage", None) or current_stage,
            exception_type=getattr(exc, "exception_type", None) or type(exc).__name__,
            sqlstate=getattr(exc, "sqlstate", None) or _sqlstate(exc),
            fixed_error_category=(
                getattr(exc, "fixed_error_category", None)
                or _fixed_error_category(exc)
            ),
        ) from None
    except Exception as exc:
        if connection is not None and not committed:
            connection.rollback()
        raise ProvisioningError(
            "route_b_admin_provisioning_failed",
            connection_attempted=connection is not None,
            writes_attempted=writes_attempted,
            committed=committed,
            failed_stage=current_stage,
            exception_type=type(exc).__name__,
            sqlstate=_sqlstate(exc),
            fixed_error_category=_fixed_error_category(exc),
        ) from None
    finally:
        if connection is not None:
            connection.close()

    report = {
        "document_type": "kpione_route_b_role_provisioning_evidence_v1",
        "run_id": run_id,
        "evidence_sequence_step": 2,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "verdict": "PASS_ADMIN_PROVISIONING",
        "evidence_mode": "DIRECT_COMMITTED_EXECUTION",
        "credential_class": target["credential_class"],
        "target_fingerprint": target_fingerprint(plan),
        "planned_productive_role": PLANNED_PRODUCTIVE_ROLE,
        "approved_git_sha": git_guard["approved_git_sha"],
        "plan_sha256": git_guard["plan_sha256"],
        "role_created": role_created,
        "role_attributes": {
            "login": True,
            "superuser": False,
            "createdb": False,
            "createrole": False,
            "replication": False,
            "bypassrls": False,
            "connection_limit": PRODUCTIVE_CONNECTION_LIMIT,
        },
        "ddl_sha256": ddl_sha256,
        "sql_sha256": ddl_sha256,
        "route_b_objects_validated": sorted(plan["physical_contract"]["objects"]),
        "route_b_sequence": sequence_name,
        "legacy_object_before": legacy_before,
        "legacy_object_after": legacy_after,
        "legacy_object_unchanged": legacy_before == legacy_after,
        "legacy_structure_before": legacy_structural_identity(legacy_snapshot_before),
        "legacy_structure_after": legacy_structural_identity(legacy_snapshot_after),
        "legacy_activity_before": {
            "row_count": legacy_snapshot_before.get("row_count", "unavailable"),
            "relation_size_bytes": legacy_snapshot_before.get("relation_size_bytes", "unavailable"),
        },
        "legacy_activity_after": {
            "row_count": legacy_snapshot_after.get("row_count", "unavailable"),
            "relation_size_bytes": legacy_snapshot_after.get("relation_size_bytes", "unavailable"),
        },
        "public_acl_before": public_acl_before,
        "public_acl_after": public_acl_after,
        "connection_attempted": True,
        "writes_attempted": True,
        "committed": True,
        "rollback_or_reconciliation_required": False,
    }
    try:
        current_stage = "evidence_write"
        _write_provisioning_evidence(evidence_json, report)
    except OSError:
        failure_report = dict(report)
        failure_report.update({
            "verdict": "BLOCKED",
            "error": "admin_provisioning_evidence_write_failed",
            "failed_stage": "evidence_write",
            "exception_type": "OSError",
            "sqlstate": None,
            "fixed_error_category": "UNCLASSIFIED_RUNTIME_ERROR",
            "rollback_or_reconciliation_required": True,
        })
        raise ProvisioningError(
            "admin_provisioning_evidence_write_failed",
            connection_attempted=True,
            writes_attempted=True,
            committed=True,
            report=failure_report,
            failed_stage="evidence_write",
            exception_type="OSError",
            sqlstate=None,
            fixed_error_category="UNCLASSIFIED_RUNTIME_ERROR",
        ) from None
    return report


def _write_provisioning_evidence(evidence_json: Path, report: dict[str, Any]) -> None:
    atomic_write_json(evidence_json, report)


def _validate_prior_failure_mapping(
    report: Mapping[str, Any],
    run_id: str,
    git_guard: Mapping[str, str],
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    required = {
        "verdict": "BLOCKED",
        "error": "admin_provisioning_evidence_write_failed",
        "run_id": run_id,
        "evidence_sequence_step": 2,
        "committed": True,
        "rollback_or_reconciliation_required": True,
        "approved_git_sha": git_guard["approved_git_sha"],
        "plan_sha256": git_guard["plan_sha256"],
        "sql_sha256": plan["physical_contract"]["sql_sha256"],
        "target_fingerprint": target_fingerprint(plan),
    }
    for key, expected in required.items():
        if report.get(key) != expected:
            raise ProvisioningError(f"prior_failure_report_mismatch:{key}")
    if not isinstance(report.get("legacy_structure_before"), dict):
        raise ProvisioningError("prior_failure_legacy_structure_missing")
    if not isinstance(report.get("public_acl_before"), dict):
        raise ProvisioningError("prior_failure_public_acl_missing")
    return dict(report)


def _validate_prior_failure_report(
    path: Path,
    run_id: str,
    git_guard: Mapping[str, str],
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProvisioningError("prior_failure_report_unreadable") from exc
    return _validate_prior_failure_mapping(report, run_id, git_guard, plan)


def _observe_reconciled_state(
    cursor: Any,
    plan: dict[str, Any],
    expected_admin_username: str,
) -> dict[str, Any]:
    cursor.execute(
        "SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,"
        "rolbypassrls,rolconnlimit FROM pg_roles WHERE rolname=%s",
        (PLANNED_PRODUCTIVE_ROLE,),
    )
    role_attributes = cursor.fetchone()
    expected_attributes = (
        True, False, False, False, False, False, PRODUCTIVE_CONNECTION_LIMIT,
    )
    if role_attributes != expected_attributes:
        raise ProvisioningError("reconciliation_role_attributes_mismatch")
    _assert_route_b_object_signatures(cursor, plan, allow_empty=False)
    cursor.execute(
        "SELECT DISTINCT c.relowner::regrole::text FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname||'.'||c.relname=ANY(%s)",
        (plan["physical_contract"]["objects"],),
    )
    owners = sorted({row[0] for row in cursor.fetchall()})
    if owners != [expected_admin_username]:
        raise ProvisioningError("reconciliation_route_b_owner_mismatch")
    cursor.execute(
        "SELECT pg_get_serial_sequence(%s,%s)",
        ("cg_raw.kpione_raw_event_photo_staging_v1", "staging_id"),
    )
    sequence_name = cursor.fetchone()[0]
    if not sequence_name:
        raise ProvisioningError("reconciliation_sequence_missing")
    cursor.execute("SELECT %s::regclass::oid", (sequence_name,))
    sequence_oid = cursor.fetchone()[0]
    cursor.execute("SELECT relowner::regrole::text FROM pg_class WHERE oid=%s", (sequence_oid,))
    if cursor.fetchone()[0] != expected_admin_username:
        raise ProvisioningError("reconciliation_sequence_owner_mismatch")

    role = PLANNED_PRODUCTIVE_ROLE
    batch = "cg_raw.kpione_raw_ingest_batch_v1"
    batch_file = "cg_raw.kpione_raw_ingest_batch_file_v1"
    staging = "cg_raw.kpione_raw_event_photo_staging_v1"
    cursor.execute(
        "SELECT "
        "has_schema_privilege(%s,'cg_raw','USAGE'),has_schema_privilege(%s,'cg_core','USAGE'),"
        "has_schema_privilege(%s,'cg_raw','CREATE'),has_schema_privilege(%s,'cg_core','CREATE'),"
        "has_table_privilege(%s,%s,'SELECT'),has_table_privilege(%s,%s,'INSERT'),"
        "has_table_privilege(%s,%s,'UPDATE'),has_table_privilege(%s,%s,'DELETE'),"
        "has_column_privilege(%s,%s,'status','UPDATE'),"
        "has_column_privilege(%s,%s,'activated_at','UPDATE'),"
        "has_column_privilege(%s,%s,'rolled_back_at','UPDATE'),"
        "has_column_privilege(%s,%s,'semantic_plan_hash','UPDATE'),"
        "has_table_privilege(%s,%s,'SELECT'),has_table_privilege(%s,%s,'INSERT'),"
        "has_table_privilege(%s,%s,'UPDATE'),has_table_privilege(%s,%s,'DELETE'),"
        "has_table_privilege(%s,%s,'SELECT'),has_table_privilege(%s,%s,'INSERT'),"
        "has_table_privilege(%s,%s,'UPDATE'),has_table_privilege(%s,%s,'DELETE'),"
        "has_table_privilege(%s,'cg_core.kpione_event_normalized_v1','SELECT'),"
        "has_table_privilege(%s,'cg_core.kpione_day_presence_v1','SELECT'),"
        "has_sequence_privilege(%s,%s,'USAGE'),has_sequence_privilege(%s,%s,'SELECT'),"
        "has_table_privilege(%s,'cg_raw.kpione2_raw','INSERT'),"
        "has_table_privilege(%s,'cg_raw.kpione2_raw','UPDATE'),"
        "has_table_privilege(%s,'cg_raw.kpione2_raw','DELETE')",
        (
            role, role, role, role,
            role, batch, role, batch, role, batch, role, batch,
            role, batch, role, batch, role, batch, role, batch,
            role, batch_file, role, batch_file, role, batch_file, role, batch_file,
            role, staging, role, staging, role, staging, role, staging,
            role, role, role, sequence_name, role, sequence_name,
            role, role, role,
        ),
    )
    privileges = tuple(cursor.fetchone())
    expected_privileges = (
        True, True, False, False,
        True, True, False, False,
        True, True, True, False,
        True, True, False, False,
        True, True, False, False,
        True, True, True, False,
        False, False, False,
    )
    if privileges != expected_privileges:
        raise ProvisioningError("reconciliation_grant_matrix_mismatch")
    relations = list(plan["physical_contract"]["object_signatures"]) + ["cg_raw.kpione2_raw"]
    return {
        "role_attributes": {
            "login": role_attributes[0], "superuser": role_attributes[1],
            "createdb": role_attributes[2], "createrole": role_attributes[3],
            "replication": role_attributes[4], "bypassrls": role_attributes[5],
            "connection_limit": role_attributes[6],
        },
        "route_b_owners": owners,
        "route_b_sequence": sequence_name,
        "grant_matrix_verified": True,
        "legacy": _legacy_snapshot(cursor),
        "public_acl": _public_acl_snapshot(cursor, relations),
    }


def reconcile_provisioning_evidence(
    plan: dict[str, Any],
    dsn: str,
    run_id: str,
    prior_failure: Mapping[str, Any],
    evidence_json: Path,
    *,
    git_guard: Mapping[str, str],
    connect_fn: Callable[[str], Any] | None = None,
    expected_admin_username: str = EXPECTED_ADMIN_ROLE,
) -> dict[str, Any]:
    validate_run_id(run_id)
    validate_provisioning_plan(plan)
    prior_failure = _validate_prior_failure_mapping(
        prior_failure, run_id, git_guard, plan,
    )
    target = validate_admin_dsn_target(
        dsn, plan, expected_admin_username=expected_admin_username,
    )
    connection: Any | None = None
    try:
        connection = _connect(dsn, connect_fn)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("BEGIN READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '30s'")
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            cursor.execute(
                "SELECT current_user,session_user,current_database(),"
                "current_setting('transaction_read_only')"
            )
            current_user, session_user, database, readonly = cursor.fetchone()
            if current_user != expected_admin_username or session_user != expected_admin_username:
                raise ProvisioningError("reconciliation_admin_session_role_mismatch")
            if database != plan["target"]["expected_database"] or readonly != "on":
                raise ProvisioningError("reconciliation_readonly_session_mismatch")
            observed = _observe_reconciled_state(cursor, plan, expected_admin_username)
            if legacy_structural_identity(observed["legacy"]) != prior_failure["legacy_structure_before"]:
                raise ProvisioningError("reconciliation_legacy_structural_drift")
            if observed["public_acl"] != prior_failure["public_acl_before"]:
                raise ProvisioningError("reconciliation_public_acl_drift")
        connection.rollback()
    except ProvisioningError:
        if connection is not None:
            connection.rollback()
        raise
    except Exception:
        if connection is not None:
            connection.rollback()
        raise ProvisioningError("admin_provisioning_reconciliation_failed") from None
    finally:
        if connection is not None:
            connection.close()
    report = {
        "document_type": "kpione_route_b_role_provisioning_evidence_v1",
        "run_id": run_id,
        "evidence_sequence_step": 2,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "verdict": "PASS_ADMIN_PROVISIONING",
        "evidence_mode": "RECONCILED_COMMITTED_STATE",
        "credential_class": target["credential_class"],
        "target_fingerprint": target_fingerprint(plan),
        "approved_git_sha": git_guard["approved_git_sha"],
        "plan_sha256": git_guard["plan_sha256"],
        "ddl_sha256": plan["physical_contract"]["sql_sha256"],
        "sql_sha256": plan["physical_contract"]["sql_sha256"],
        "planned_productive_role": PLANNED_PRODUCTIVE_ROLE,
        "role_attributes": observed["role_attributes"],
        "route_b_objects_validated": sorted(plan["physical_contract"]["objects"]),
        "route_b_owners": observed["route_b_owners"],
        "route_b_sequence": observed["route_b_sequence"],
        "grant_matrix_verified": True,
        "legacy_structure_before": prior_failure["legacy_structure_before"],
        "legacy_structure_after": legacy_structural_identity(observed["legacy"]),
        "public_acl_before": prior_failure["public_acl_before"],
        "public_acl_after": observed["public_acl"],
        "connection_attempted": True,
        "writes_attempted": False,
        "committed": True,
        "transaction_outcome": "ROLLED_BACK_READ_ONLY",
        "rollback_or_reconciliation_required": False,
    }
    _write_provisioning_evidence(evidence_json, report)
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Provision the restricted KPIONE Route B role")
    result.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    result.add_argument("--run-id", required=True)
    result.add_argument("--expected-plan-git-ref", required=True)
    result.add_argument("--db-url-env", required=True)
    result.add_argument("--expected-project-ref", required=True)
    result.add_argument("--confirm", required=True)
    result.add_argument("--evidence-json", type=Path, required=True)
    result.add_argument("--reconcile-provisioning-evidence", action="store_true")
    result.add_argument("--prior-failure-report", type=Path)
    result.add_argument("--authority-precheck-only", action="store_true")
    return result


def _prepare_admin_operation(args: argparse.Namespace, root: Path = ROOT) -> tuple[dict[str, Any], str, dict[str, str]]:
    validate_run_id(args.run_id)
    args.evidence_json = require_canonical_evidence_path(
        args.evidence_json, root, args.run_id, "admin_provisioning",
    )
    if args.evidence_json.exists():
        raise ProvisioningError("evidence_file_already_exists")
    if args.db_url_env != ADMIN_DB_ENV:
        raise ProvisioningError("admin_db_url_env_required")
    authority = _validate_admin_plan_git_guard(args.plan, args.expected_plan_git_ref, root)
    plan = _load_plan_blob(authority["_plan_blob"])
    validate_provisioning_plan(plan)
    if args.expected_project_ref != EXPECTED_PRODUCTIVE_PROJECT_REF:
        raise ProvisioningError("admin_expected_project_ref_mismatch")
    authority = _validate_admin_sql_blob_guard(authority, root)
    if authority["ddl_path"] != plan["physical_contract"]["sql_file"]:
        raise ProvisioningError("route_b_sql_file_mismatch")
    if authority["ddl_sha256"] != plan["physical_contract"]["sql_sha256"]:
        raise ProvisioningError("sql_sha256_mismatch")
    ddl = authority["_ddl_blob"].decode("utf-8")
    prepare_run_directory(root, args.run_id)
    if not args.evidence_json.parent.is_dir():
        raise ProvisioningError("provisioning_evidence_parent_missing")
    if args.reconcile_provisioning_evidence:
        if args.confirm != RECONCILE_CONFIRM_TOKEN:
            raise ProvisioningError("admin_reconciliation_confirmation_required")
        if args.prior_failure_report is None:
            raise ProvisioningError("prior_failure_report_required")
        args._prior_failure = _validate_prior_failure_report(
            args.prior_failure_report, args.run_id, authority, plan,
        )
    else:
        if args.prior_failure_report is not None:
            raise ProvisioningError("prior_failure_report_not_allowed_for_initial_provisioning")
        if args.confirm != PROVISION_CONFIRM_TOKEN:
            raise ProvisioningError("admin_provision_confirmation_required")
    public_guard = {
        key: value for key, value in authority.items() if not key.startswith("_")
    }
    return plan, ddl, public_guard


def execute_cli(
    args: argparse.Namespace,
    *,
    environ: Mapping[str, str] = os.environ,
    root: Path = ROOT,
) -> dict[str, Any]:
    plan, ddl, git_guard = _prepare_admin_operation(args, root)
    if args.authority_precheck_only:
        return {
            "verdict": "PASS_ADMIN_AUTHORITY_PRECHECK",
            "credential_class": "admin-provisioning",
            **git_guard,
            "connection_attempted": False,
            "writes_attempted": False,
            "committed": False,
        }
    dsn = environ.get(ADMIN_DB_ENV)
    if not dsn:
        raise ProvisioningError("admin_dsn_missing")
    if args.reconcile_provisioning_evidence:
        return reconcile_provisioning_evidence(
            plan, dsn, args.run_id, args._prior_failure, args.evidence_json,
            git_guard=git_guard,
        )
    role_password = environ.get(PRODUCTIVE_ROLE_PASSWORD_ENV)
    if not role_password:
        raise ProvisioningError("productive_role_password_missing")
    return provision_route_b_role(
        plan, dsn, role_password, args.evidence_json,
        root=root, git_guard=git_guard, ddl=ddl, run_id=args.run_id,
    )


def _blocked_report(exc: BaseException) -> dict[str, Any]:
    report = {
        "verdict": "BLOCKED",
        "error": str(exc),
        "credential_class": "admin-provisioning",
        "connection_attempted": getattr(exc, "connection_attempted", False),
        "writes_attempted": getattr(exc, "writes_attempted", False),
        "committed": getattr(exc, "committed", False),
    }
    for key in ("failed_stage", "exception_type", "sqlstate", "fixed_error_category"):
        value = getattr(exc, key, None)
        if key == "sqlstate" or value is not None:
            report[key] = value
    safe_fields = {
        "approved_git_sha", "plan_sha256", "ddl_sha256", "target_fingerprint",
        "run_id", "evidence_sequence_step", "sql_sha256", "planned_productive_role",
        "role_created", "committed", "legacy_structure_before", "legacy_structure_after",
        "legacy_activity_before", "legacy_activity_after", "public_acl_before", "public_acl_after",
        "legacy_object_unchanged", "rollback_or_reconciliation_required",
        "failed_stage", "exception_type", "sqlstate", "fixed_error_category",
    }
    report.update({
        key: value for key, value in getattr(exc, "report", {}).items()
        if key in safe_fields
    })
    return report


def main() -> int:
    args = parser().parse_args()
    try:
        print(json.dumps(execute_cli(args), sort_keys=True))
        return 0
    except (ProvisioningError, EvidenceContractError, OSError, ValueError) as exc:
        print(json.dumps(_blocked_report(exc), sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
