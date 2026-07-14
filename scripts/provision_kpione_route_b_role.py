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
    from .kpione_route_b_v1 import (
        ADVISORY_LOCK_KEY,
        EXPECTED_PRODUCTIVE_DATABASE,
        EXPECTED_PRODUCTIVE_HOSTNAME,
        EXPECTED_PRODUCTIVE_PROJECT_REF,
        PLANNED_PRODUCTIVE_ROLE,
        _assert_route_b_object_signatures,
        sha256_lf_normalized,
    )
except ImportError:
    from kpione_route_b_v1 import (
        ADVISORY_LOCK_KEY,
        EXPECTED_PRODUCTIVE_DATABASE,
        EXPECTED_PRODUCTIVE_HOSTNAME,
        EXPECTED_PRODUCTIVE_PROJECT_REF,
        PLANNED_PRODUCTIVE_ROLE,
        _assert_route_b_object_signatures,
        sha256_lf_normalized,
    )


ADMIN_DB_ENV = "DB_URL_ADMIN"
PRODUCTIVE_ROLE_PASSWORD_ENV = "KPIONE_ROUTE_B_PRODUCTIVE_PASSWORD"
PROVISION_CONFIRM_TOKEN = "STOCK_ZERO_019_PROVISION_ROUTE_B_ROLE"
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
                 report: dict[str, Any] | None = None) -> None:
        super().__init__(identifier)
        self.connection_attempted = connection_attempted
        self.writes_attempted = writes_attempted
        self.committed = committed
        self.report = report or {}


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
) -> dict[str, Any]:
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
    sequence_name: str | None = None
    try:
        connection = _connect(dsn, connect_fn)
        connection.autocommit = False
        with connection.cursor() as cursor:
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
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (PROVISION_LOCK_KEY,))
            cursor.execute("SELECT to_regclass('cg_raw.kpione2_raw')::text")
            legacy_before = cursor.fetchone()[0]
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (PLANNED_PRODUCTIVE_ROLE,))
            if cursor.fetchone() is not None:
                raise ProvisioningError(
                    "productive_role_exists_password_rotation_not_authorized"
                )
            writes_attempted = True
            cursor.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(PLANNED_PRODUCTIVE_ROLE)))
            role_created = True
            writes_attempted = True
            cursor.execute(_role_statement(PLANNED_PRODUCTIVE_ROLE, role_password))
            cursor.execute(ddl)
            _assert_route_b_object_signatures(cursor, plan, allow_empty=False)

            tables = [
                name for name, spec in plan["physical_contract"]["object_signatures"].items()
                if spec["relation_kind"] == "r"
            ]
            views = [
                name for name, spec in plan["physical_contract"]["object_signatures"].items()
                if spec["relation_kind"] == "v"
            ]
            for name in tables:
                cursor.execute(
                    sql.SQL("ALTER TABLE {} OWNER TO {}").format(
                        _qualified_identifier(name), sql.Identifier(expected_admin_username),
                    )
                )
            for name in views:
                cursor.execute(
                    sql.SQL("ALTER VIEW {} OWNER TO {}").format(
                        _qualified_identifier(name), sql.Identifier(expected_admin_username),
                    )
                )
            cursor.execute(
                "SELECT pg_get_serial_sequence(%s,%s)",
                ("cg_raw.kpione_raw_event_photo_staging_v1", "staging_id"),
            )
            sequence_name = cursor.fetchone()[0]
            if not sequence_name:
                raise ProvisioningError("route_b_identity_sequence_missing")
            cursor.execute(
                sql.SQL("ALTER SEQUENCE {} OWNER TO {}").format(
                    _qualified_identifier(sequence_name), sql.Identifier(expected_admin_username),
                )
            )

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
            for name in tables + views:
                identifier = _qualified_identifier(name)
                cursor.execute(
                    sql.SQL("REVOKE ALL PRIVILEGES ON TABLE {} FROM {}").format(
                        identifier, sql.Identifier(PLANNED_PRODUCTIVE_ROLE),
                    )
                )
            role_identifier = sql.Identifier(PLANNED_PRODUCTIVE_ROLE)
            batch_identifier = _qualified_identifier("cg_raw.kpione_raw_ingest_batch_v1")
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
                cursor.execute(
                    sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(
                        _qualified_identifier(name), sql.Identifier(PLANNED_PRODUCTIVE_ROLE),
                    )
                )
            sequence_identifier = _qualified_identifier(sequence_name)
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
            cursor.execute(
                "SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,"
                "rolbypassrls,rolconnlimit FROM pg_roles WHERE rolname=%s",
                (PLANNED_PRODUCTIVE_ROLE,),
            )
            role_attributes = cursor.fetchone()
            if role_attributes != (True, False, False, False, False, False, PRODUCTIVE_CONNECTION_LIMIT):
                raise ProvisioningError("productive_role_attributes_mismatch")
            cursor.execute("SELECT to_regclass('cg_raw.kpione2_raw')::text")
            legacy_after = cursor.fetchone()[0]
            if legacy_after != legacy_before:
                raise ProvisioningError("legacy_object_changed")
        connection.commit()
        committed = True
    except ProvisioningError as exc:
        if connection is not None and not committed:
            connection.rollback()
        raise ProvisioningError(
            str(exc), connection_attempted=connection is not None,
            writes_attempted=writes_attempted, committed=committed,
        ) from None
    except Exception:
        if connection is not None and not committed:
            connection.rollback()
        raise ProvisioningError(
            "route_b_admin_provisioning_failed",
            connection_attempted=connection is not None,
            writes_attempted=writes_attempted,
            committed=committed,
        ) from None
    finally:
        if connection is not None:
            connection.close()

    report = {
        "document_type": "kpione_route_b_role_provisioning_evidence_v1",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "verdict": "PASS_ADMIN_PROVISIONING",
        "credential_class": target["credential_class"],
        "target_fingerprint": hashlib.sha256(
            "|".join((
                plan["target"]["expected_supabase_project_ref"],
                target["hostname"],
                target["database"],
            )).encode("utf-8")
        ).hexdigest(),
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
        "connection_attempted": True,
        "writes_attempted": True,
        "committed": True,
        "rollback_or_reconciliation_required": False,
    }
    try:
        _write_provisioning_evidence(evidence_json, report)
    except OSError:
        failure_report = dict(report)
        failure_report.update({
            "verdict": "BLOCKED",
            "error": "admin_provisioning_evidence_write_failed",
            "rollback_or_reconciliation_required": True,
        })
        raise ProvisioningError(
            "admin_provisioning_evidence_write_failed",
            connection_attempted=True,
            writes_attempted=True,
            committed=True,
            report=failure_report,
        ) from None
    return report


def _write_provisioning_evidence(evidence_json: Path, report: dict[str, Any]) -> None:
    evidence_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Provision the restricted KPIONE Route B role")
    result.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    result.add_argument("--expected-plan-git-ref", required=True)
    result.add_argument("--db-url-env", required=True)
    result.add_argument("--expected-project-ref", required=True)
    result.add_argument("--confirm", required=True)
    result.add_argument("--evidence-json", type=Path, required=True)
    result.add_argument("--authority-precheck-only", action="store_true")
    return result


def _prepare_admin_operation(args: argparse.Namespace, root: Path = ROOT) -> tuple[dict[str, Any], str, dict[str, str]]:
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
    if not args.evidence_json.parent.is_dir():
        raise ProvisioningError("provisioning_evidence_parent_missing")
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
    role_password = environ.get(PRODUCTIVE_ROLE_PASSWORD_ENV)
    if not dsn:
        raise ProvisioningError("admin_dsn_missing")
    if not role_password:
        raise ProvisioningError("productive_role_password_missing")
    return provision_route_b_role(
        plan, dsn, role_password, args.evidence_json,
        root=root, git_guard=git_guard, ddl=ddl,
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
    safe_fields = {
        "approved_git_sha", "plan_sha256", "ddl_sha256", "target_fingerprint",
        "planned_productive_role", "role_created", "committed",
        "legacy_object_unchanged", "rollback_or_reconciliation_required",
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
    except (ProvisioningError, OSError, ValueError) as exc:
        print(json.dumps(_blocked_report(exc), sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
