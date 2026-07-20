from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse, urlunparse

import psycopg
from psycopg import sql


ROOT = Path(__file__).resolve().parents[1]
PLAN_DEFAULT = ROOT / "plans" / "023_control_gestion_route_b_bridge_refresh_plan.json"
PLAN_TYPE = "stock_zero_control_gestion_route_b_bridge_refresh_plan_v1"
ALLOWED_STATUS = "CONTRACT_READY_GATE_CLOSED"
ROLE = "stock_zero_cg_mart_refresh"
ADMIN_ENV = "DB_URL_ADMIN"
PASSWORD_ENV = "CG_MART_REFRESH_PASSWORD"
EXPECTED_HOST = "db.xheyrgfagpoigpgakilu.supabase.co"
EXPECTED_DATABASE = "postgres"
EXPECTED_PROJECT = "xheyrgfagpoigpgakilu"
CONFIRM_TOKEN = "STOCK_ZERO_023_PROVISION_CG_MART_REFRESH"
LOCK_KEY = 5426926728611921736
AUTHORIZATION_NAMES = {
    "provision_refresh_role_authorized", "apply_bridge_authorized",
    "apply_june_refresh_authorized", "runtime_app_validation_authorized",
}

SELECT_OBJECTS = (
    "cg_core.v_cg_visita_dia_precedencia_route_b_v1",
    "cg_core.v_rr_frecuencia_base_resuelta_v2",
    "cg_mart.fact_cg_visita_dia_resuelta_v2",
    "cg_mart.fact_cg_out_weekly_v2",
    "cg_mart.mv_cg_out_weekly_v2",
)
WRITE_OBJECTS = (
    "cg_mart.fact_cg_visita_dia_resuelta_v2",
    "cg_mart.fact_cg_out_weekly_v2",
)


class ProvisioningError(RuntimeError):
    pass


def _git(*args: str, text: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=text, check=False,
    )


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _load_authorized_plan(path: Path, expected_git_ref: str) -> tuple[dict, dict[str, str]]:
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProvisioningError("plan_023_unavailable_or_invalid") from exc
    if plan.get("document_type") != PLAN_TYPE or plan.get("status") != ALLOWED_STATUS:
        raise ProvisioningError("provisioning_plan_state_mismatch")
    authorizations = plan.get("authorizations")
    if not isinstance(authorizations, dict) or set(authorizations) != AUTHORIZATION_NAMES:
        raise ProvisioningError("provisioning_authorizations_missing")
    if any(type(value) is not bool for value in authorizations.values()):
        raise ProvisioningError("provisioning_authorization_value_invalid")
    active = [name for name, value in authorizations.items() if value is True]
    if active != ["provision_refresh_role_authorized"]:
        raise ProvisioningError("provisioning_not_exclusively_authorized")
    if plan.get("gate_open") is not True or plan.get("productive_actions_authorized") is not True:
        raise ProvisioningError("provisioning_gate_closed")
    if plan.get("approved_git_sha") != expected_git_ref:
        raise ProvisioningError("approved_git_sha_mismatch")
    if plan.get("confirmation_tokens", {}).get("provision_refresh_role") != CONFIRM_TOKEN:
        raise ProvisioningError("provisioning_plan_confirmation_token_mismatch")
    if re.fullmatch(r"[0-9a-f]{40}", expected_git_ref or "") is None:
        raise ProvisioningError("expected_git_ref_required")
    head = _git("rev-parse", "HEAD")
    if head.returncode != 0 or head.stdout.strip() != expected_git_ref:
        raise ProvisioningError("repository_head_mismatch")
    if _git("diff", "--quiet").returncode != 0:
        raise ProvisioningError("repository_worktree_not_clean")
    if _git("diff", "--cached", "--quiet").returncode != 0:
        raise ProvisioningError("repository_index_not_clean")
    untracked = _git("ls-files", "--others", "--exclude-standard")
    if untracked.returncode != 0 or untracked.stdout.strip():
        raise ProvisioningError("repository_untracked_files_present")
    try:
        relative = path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise ProvisioningError("plan_outside_repository") from exc
    blob = _git("show", f"HEAD:{relative}", text=False)
    if blob.returncode != 0:
        raise ProvisioningError("plan_not_tracked_at_head")
    if path.read_bytes() != blob.stdout:
        raise ProvisioningError("plan_worktree_blob_mismatch")
    return plan, {
        "approved_git_sha": expected_git_ref,
        "plan_path": relative,
        "plan_raw_sha256": _sha256(blob.stdout),
    }


def _validate_admin_dsn(dsn: str, plan: dict) -> None:
    parsed = urlparse(dsn)
    target = plan["target"]
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ProvisioningError("unsupported_admin_db_scheme")
    if parsed.username != "postgres":
        raise ProvisioningError("admin_role_required")
    if (parsed.hostname or "").lower() != target["hostname"]:
        raise ProvisioningError("admin_hostname_mismatch")
    if target["project_ref"] not in (parsed.hostname or ""):
        raise ProvisioningError("admin_project_mismatch")
    if (parsed.path or "").lstrip("/") != target["database"]:
        raise ProvisioningError("admin_database_mismatch")
    if parse_qs(parsed.query, keep_blank_values=True).get("sslmode", []) != ["require"]:
        raise ProvisioningError("admin_sslmode_require_required")


def _role_statement(password: str) -> sql.Composed:
    # A newly created PostgreSQL role starts with the required negative
    # administrative attributes. Supabase-compatible provisioning requests
    # only the attributes that must change, then verifies every negative bit.
    return sql.SQL("ALTER ROLE {} WITH LOGIN NOINHERIT PASSWORD {}").format(
        sql.Identifier(ROLE), sql.Literal(password)
    )


def _qualified(name: str) -> sql.Composed:
    schema, relation = name.split(".", 1)
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(relation))


def _expected_role_state(cursor) -> dict[str, object]:
    cursor.execute(
        "SELECT rolcanlogin,rolinherit,rolsuper,rolcreatedb,rolcreaterole,"
        "rolreplication,rolbypassrls,rolconnlimit FROM pg_roles WHERE rolname=%s",
        (ROLE,),
    )
    row = cursor.fetchone()
    if row is None:
        raise ProvisioningError("mart_refresh_role_missing")
    keys = (
        "login", "inherit", "superuser", "createdb", "createrole",
        "replication", "bypassrls", "connection_limit",
    )
    return dict(zip(keys, row))


def _assert_exact_role_state(cursor) -> dict[str, object]:
    observed = _expected_role_state(cursor)
    expected = {
        "login": True,
        "inherit": False,
        "superuser": False,
        "createdb": False,
        "createrole": False,
        "replication": False,
        "bypassrls": False,
        "connection_limit": -1,
    }
    if observed != expected:
        raise ProvisioningError("mart_refresh_role_attribute_drift")
    cursor.execute(
        "SELECT has_database_privilege(%s,current_database(),'CONNECT'),"
        "has_database_privilege(%s,current_database(),'TEMPORARY'),"
        "has_database_privilege(%s,current_database(),'CREATE'),"
        "has_schema_privilege(%s,'cg_core','USAGE'),"
        "has_schema_privilege(%s,'cg_mart','USAGE'),"
        "has_schema_privilege(%s,'cg_core','CREATE'),"
        "has_schema_privilege(%s,'cg_mart','CREATE')",
        (ROLE, ROLE, ROLE, ROLE, ROLE, ROLE, ROLE),
    )
    database_acl = cursor.fetchone()
    if database_acl != (True, True, False, True, True, False, False):
        raise ProvisioningError("mart_refresh_database_or_schema_acl_drift")
    for name in SELECT_OBJECTS:
        cursor.execute("SELECT has_table_privilege(%s,%s,'SELECT')", (ROLE, name))
        if cursor.fetchone() != (True,):
            raise ProvisioningError("mart_refresh_select_acl_drift")
    for name in WRITE_OBJECTS:
        cursor.execute(
            "SELECT has_table_privilege(%s,%s,'INSERT'),"
            "has_table_privilege(%s,%s,'DELETE'),"
            "has_table_privilege(%s,%s,'UPDATE'),"
            "has_table_privilege(%s,%s,'TRUNCATE')",
            (ROLE, name, ROLE, name, ROLE, name, ROLE, name),
        )
        if cursor.fetchone() != (True, True, False, False):
            raise ProvisioningError("mart_refresh_fact_acl_drift")
    expected_direct_grants = {
        (*name.split(".", 1), "SELECT") for name in SELECT_OBJECTS
    } | {
        (*name.split(".", 1), privilege)
        for name in WRITE_OBJECTS for privilege in ("INSERT", "DELETE")
    }
    cursor.execute(
        "SELECT table_schema,table_name,privilege_type "
        "FROM information_schema.role_table_grants "
        "WHERE grantee=%s AND table_schema IN ('cg_core','cg_mart')",
        (ROLE,),
    )
    if {tuple(row) for row in cursor.fetchall()} != expected_direct_grants:
        raise ProvisioningError("mart_refresh_direct_table_grant_drift")
    cursor.execute(
        "SELECT has_table_privilege(%s,'public.fact_stock_venta','SELECT'),"
        "has_table_privilege(%s,'public.fact_stock_venta','INSERT'),"
        "has_table_privilege(%s,'public.fact_stock_venta','UPDATE'),"
        "has_table_privilege(%s,'public.fact_stock_venta','DELETE')",
        (ROLE, ROLE, ROLE, ROLE),
    )
    if cursor.fetchone() != (False, False, False, False):
        raise ProvisioningError("mart_refresh_inventory_access_forbidden")
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member "
        "WHERE r.rolname=%s),"
        "EXISTS(SELECT 1 FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner WHERE r.rolname=%s),"
        "EXISTS(SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE r.rolname=%s),"
        "EXISTS(SELECT 1 FROM pg_database d JOIN pg_roles r ON r.oid=d.datdba WHERE r.rolname=%s)",
        (ROLE, ROLE, ROLE, ROLE),
    )
    if cursor.fetchone() != (False, False, False, False):
        raise ProvisioningError("mart_refresh_membership_or_ownership_drift")
    return observed


def _grant_exact_privileges(cursor) -> None:
    cursor.execute(
        sql.SQL("GRANT CONNECT,TEMPORARY ON DATABASE {} TO {}").format(
            sql.Identifier(EXPECTED_DATABASE), sql.Identifier(ROLE)
        )
    )
    cursor.execute(
        sql.SQL("GRANT USAGE ON SCHEMA cg_core,cg_mart TO {}").format(sql.Identifier(ROLE))
    )
    for name in SELECT_OBJECTS:
        cursor.execute(
            sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(
                _qualified(name), sql.Identifier(ROLE)
            )
        )
    for name in WRITE_OBJECTS:
        cursor.execute(
            sql.SQL("GRANT INSERT,DELETE ON TABLE {} TO {}").format(
                _qualified(name), sql.Identifier(ROLE)
            )
        )


def _role_dsn(admin_dsn: str, password: str) -> str:
    parsed = urlparse(admin_dsn)
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{quote(ROLE, safe='')}:{quote(password, safe='')}@{host}{port}"
    return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))


def _canonical_evidence_path(path: Path, run_id: str) -> Path:
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
        run_id or "",
    ) is None:
        raise ProvisioningError("canonical_run_id_required")
    expected = (
        ROOT / "evidence" / "runtime" / "023" / run_id / "02_refresh_role_provisioning.json"
    ).resolve()
    actual = (ROOT / path).resolve() if not path.is_absolute() else path.resolve()
    if actual != expected or actual.exists() or not actual.parent.is_dir():
        raise ProvisioningError("canonical_unused_provisioning_evidence_path_required")
    return actual


def provision(
    plan: dict, authority: dict[str, str], admin_dsn: str, password: str
) -> dict[str, object]:
    created = False
    committed = False
    with psycopg.connect(admin_dsn) as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT current_user,session_user,current_database(),current_setting('role'),"
                    "current_setting('transaction_read_only')"
                )
                if cursor.fetchone() != ("postgres", "postgres", "postgres", "none", "off"):
                    raise ProvisioningError("admin_session_identity_mismatch")
                cursor.execute("SET LOCAL statement_timeout='10min'")
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_KEY,))
                cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=%s)", (ROLE,))
                exists = bool(cursor.fetchone()[0])
                if exists:
                    _assert_exact_role_state(cursor)
                else:
                    cursor.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(ROLE)))
                    created = True
                    cursor.execute(_role_statement(password))
                    _grant_exact_privileges(cursor)
                    _assert_exact_role_state(cursor)
            connection.commit()
            committed = True
        except Exception:
            connection.rollback()
            raise

    with psycopg.connect(_role_dsn(admin_dsn, password)) as verification:
        with verification.cursor() as cursor:
            cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute(
                "SELECT current_user,session_user,current_database(),current_setting('role'),"
                "current_setting('transaction_read_only')"
            )
            if cursor.fetchone() != (ROLE, ROLE, EXPECTED_DATABASE, "none", "on"):
                raise ProvisioningError("mart_refresh_login_verification_failed")
        verification.rollback()
    return {
        **authority,
        "document_type": "stock_zero_cg_mart_refresh_role_provisioning_v1",
        "schema_version": 1,
        "verdict": "PASS_023_CG_MART_REFRESH_ROLE_PROVISIONING",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "role": ROLE,
        "created": created,
        "idempotent_existing_match": not created,
        "committed": committed,
        "post_commit_login_verified": True,
        "writes_attempted": created,
        "writes_executed": created,
        "secrets_printed": False,
        "dsn_printed": False,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Provision the dedicated CG mart refresh role")
    result.add_argument("--plan-023", type=Path, default=PLAN_DEFAULT)
    result.add_argument("--expected-git-ref", required=True)
    result.add_argument("--expected-project-ref", required=True)
    result.add_argument("--confirm", required=True)
    result.add_argument("--run-id", required=True)
    result.add_argument("--evidence-json", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.expected_project_ref != EXPECTED_PROJECT or args.confirm != CONFIRM_TOKEN:
            raise ProvisioningError("provisioning_authority_mismatch")
        if os.getenv("STOCK_ZERO_OPERATION_PROFILE") != "cg-mart-refresh-provisioning":
            raise ProvisioningError("provisioning_wrapper_profile_required")
        if os.getenv("STOCK_ZERO_OPERATION") != "provision-cg-mart-refresh-023":
            raise ProvisioningError("provisioning_wrapper_operation_required")
        plan, authority = _load_authorized_plan(args.plan_023, args.expected_git_ref)
        evidence_path = _canonical_evidence_path(args.evidence_json, args.run_id)
        admin_dsn = os.getenv(ADMIN_ENV, "")
        password = os.getenv(PASSWORD_ENV, "")
        if not admin_dsn or not password:
            raise ProvisioningError("provisioning_child_environment_required")
        _validate_admin_dsn(admin_dsn, plan)
        report = provision(plan, authority, admin_dsn, password)
        report["run_id"] = args.run_id
        rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        evidence_path.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
        return 0
    except (ProvisioningError, OSError) as exc:
        print(json.dumps({"verdict": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2
    except Exception as exc:
        print(json.dumps({"verdict": "BLOCKED", "error": type(exc).__name__}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
