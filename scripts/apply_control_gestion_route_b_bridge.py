from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import psycopg
try:
    from scripts.refresh_control_gestion_v2_incremental import (
        COMMITTED_EVIDENCE_PENDING,
        COMMITTED_EVIDENCE_RECORDED,
        COMMITTED_EVIDENCE_RECOVERY_REQUIRED,
        write_committed_recovery_receipt,
        write_json_exclusive,
    )
except ModuleNotFoundError:  # direct script execution
    from refresh_control_gestion_v2_incremental import (  # type: ignore
        COMMITTED_EVIDENCE_PENDING,
        COMMITTED_EVIDENCE_RECORDED,
        COMMITTED_EVIDENCE_RECOVERY_REQUIRED,
        write_committed_recovery_receipt,
        write_json_exclusive,
    )


ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "sql" / "18_control_gestion_route_b_app_bridge_v1.sql"
PLAN_PATH_DEFAULT = ROOT / "plans" / "023_control_gestion_route_b_bridge_refresh_plan.json"
PLAN_DOCUMENT_TYPE = "stock_zero_control_gestion_route_b_bridge_refresh_plan_v1"
ALLOWED_PLAN_STATUS = "PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED"
EXPECTED_HOST = "db.xheyrgfagpoigpgakilu.supabase.co"
EXPECTED_DATABASE = "postgres"
EXPECTED_ADMIN_ROLE = "postgres"
CONFIRM_TOKEN = "STOCK_ZERO_023_APPLY_ROUTE_B_BRIDGE"
# Retained only so pre-023 static consumers can identify the retired token.
LEGACY_CONFIRM_TOKEN = "STOCK_ZERO_022_APPLY_ROUTE_B_APP_BRIDGE"
LOCK_KEY = 5426926728611921735
STATEMENT_TIMEOUT = "10min"
AUTHORIZATION_NAMES = {
    "provision_refresh_role_authorized", "apply_bridge_authorized",
    "apply_june_refresh_authorized", "runtime_app_validation_authorized",
}
BRIDGE_SIGNATURE = (
    ("semana_inicio", "date"), ("fecha_visita", "date"), ("cod_rt", "text"),
    ("cod_b2b", "text"), ("cliente", "text"), ("cliente_norm", "text"),
    ("local_nombre", "text"), ("gestor", "text"), ("gestor_norm", "text"),
    ("rutero", "text"), ("reponedor_scope", "text"),
    ("reponedor_scope_norm", "text"), ("supervisor", "text"),
    ("jefe_operaciones", "text"), ("modalidad", "text"),
    ("semana_iso", "integer"), ("fuente_ganadora", "text"),
    ("fuentes_presentes", "text"), ("tiene_kpione2", "integer"),
    ("tiene_power_app", "integer"), ("tiene_kpione1", "integer"),
    ("power_app_fallback", "integer"), ("kpione1_audit_only", "integer"),
    ("useful_day", "integer"), ("raw_evidence_count", "integer"),
    ("same_source_multimark", "integer"), ("multisource_overlap", "integer"),
    ("kpione_rows_dia", "integer"), ("kpione2_rows_dia", "integer"),
    ("power_app_rows_dia", "integer"), ("persona_conflicto_rows_dia", "integer"),
    ("match_quality", "text"), ("registro_fuera_cruce", "text"),
)


class BridgeApplyError(RuntimeError):
    pass


def _git(*args: str, text: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=text, check=False,
    )


def _load_plan(plan_path: Path) -> dict[str, object]:
    try:
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BridgeApplyError("plan_023_unavailable_or_invalid") from exc
    if plan.get("document_type") != PLAN_DOCUMENT_TYPE:
        raise BridgeApplyError("plan_023_document_type_mismatch")
    if plan.get("status") != ALLOWED_PLAN_STATUS:
        raise BridgeApplyError("bridge_plan_status_mismatch")
    authorizations = plan.get("authorizations")
    if not isinstance(authorizations, dict) or set(authorizations) != AUTHORIZATION_NAMES:
        raise BridgeApplyError("bridge_authorizations_missing")
    if any(type(value) is not bool for value in authorizations.values()):
        raise BridgeApplyError("bridge_authorization_value_invalid")
    active = [name for name, enabled in authorizations.items() if enabled is True]
    if active != ["apply_bridge_authorized"]:
        raise BridgeApplyError("bridge_not_exclusively_authorized")
    if plan.get("gate_open") is not True or plan.get("productive_actions_authorized") is not True:
        raise BridgeApplyError("bridge_gate_closed")
    if plan.get("confirmation_tokens", {}).get("apply_bridge") != CONFIRM_TOKEN:
        raise BridgeApplyError("bridge_plan_confirmation_token_mismatch")
    if plan.get("advisory_lock_key") != LOCK_KEY:
        raise BridgeApplyError("bridge_advisory_lock_mismatch")
    return plan


def validate_git_guard(
    expected_git_ref: str, plan_path: Path, plan: dict[str, object]
) -> dict[str, str]:
    if not re.fullmatch(r"[0-9a-f]{40}", expected_git_ref or ""):
        raise BridgeApplyError("expected_git_ref_required")
    head = _git("rev-parse", "HEAD")
    if head.returncode != 0 or head.stdout.strip() != expected_git_ref:
        raise BridgeApplyError("repository_head_mismatch")
    checks = (
        _git("diff", "--quiet"),
        _git("diff", "--cached", "--quiet"),
        _git("ls-files", "--others", "--exclude-standard"),
    )
    if checks[0].returncode != 0 or checks[1].returncode != 0 or checks[2].stdout.strip():
        raise BridgeApplyError("repository_not_clean")
    try:
        relative_plan = plan_path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise BridgeApplyError("plan_outside_repository") from exc
    plan_blob = _git("show", f"HEAD:{relative_plan}", text=False)
    if plan_blob.returncode != 0:
        raise BridgeApplyError("plan_not_tracked_at_head")
    if plan_path.read_bytes() != plan_blob.stdout:
        raise BridgeApplyError("plan_worktree_blob_mismatch")
    if plan.get("approved_git_sha") != expected_git_ref:
        raise BridgeApplyError("plan_approved_git_sha_mismatch")
    relative = SQL_PATH.relative_to(ROOT).as_posix()
    blob = _git("show", f"HEAD:{relative}", text=False)
    if blob.returncode != 0 or not SQL_PATH.is_file():
        raise BridgeApplyError("bridge_sql_not_tracked")
    working = SQL_PATH.read_bytes()
    tracked = blob.stdout
    if working != tracked:
        raise BridgeApplyError("bridge_sql_worktree_blob_mismatch")
    observed_sql_sha = hashlib.sha256(tracked).hexdigest()
    bridge_contract = plan.get("bridge_contract")
    if not isinstance(bridge_contract, dict):
        raise BridgeApplyError("bridge_plan_contract_missing")
    if bridge_contract.get("sql_path") != relative:
        raise BridgeApplyError("bridge_sql_path_mismatch")
    if bridge_contract.get("sql_raw_sha256") != observed_sql_sha:
        raise BridgeApplyError("bridge_sql_sha256_mismatch")
    return {
        "approved_git_sha": expected_git_ref,
        "plan_path": relative_plan,
        "plan_raw_sha256": hashlib.sha256(plan_blob.stdout).hexdigest(),
        "sql_path": relative,
        "sql_raw_sha256": observed_sql_sha,
    }


def validate_dsn(dsn: str, expected_project_ref: str, plan: dict[str, object]) -> None:
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise BridgeApplyError("unsupported_admin_db_scheme")
    if expected_project_ref != "xheyrgfagpoigpgakilu":
        raise BridgeApplyError("expected_project_ref_mismatch")
    if parsed.hostname != EXPECTED_HOST:
        raise BridgeApplyError("registered_target_hostname_mismatch")
    if (parsed.path or "").lstrip("/") != EXPECTED_DATABASE:
        raise BridgeApplyError("registered_target_database_mismatch")
    if parsed.username != EXPECTED_ADMIN_ROLE:
        raise BridgeApplyError("admin_role_mismatch")
    if parse_qs(parsed.query, keep_blank_values=True).get("sslmode", []) != ["require"]:
        raise BridgeApplyError("admin_sslmode_require_required")
    target = plan.get("target")
    if target != {
        "project_ref": expected_project_ref,
        "hostname": EXPECTED_HOST,
        "database": EXPECTED_DATABASE,
        "sslmode": "require",
    }:
        raise BridgeApplyError("bridge_registered_target_mismatch")


def apply_bridge(
    dsn: str, authority: dict[str, str], plan: dict[str, object]
) -> dict[str, object]:
    connection_attempted = False
    writes_attempted = False
    try:
        connection_attempted = True
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT current_user,session_user,current_database(),"
                    "current_setting('role'),current_setting('transaction_read_only')"
                )
                current_user, session_user, database, active_role, readonly = cursor.fetchone()
                if (
                    current_user != EXPECTED_ADMIN_ROLE
                    or session_user != EXPECTED_ADMIN_ROLE
                    or current_user != session_user
                ):
                    raise BridgeApplyError("admin_session_role_mismatch")
                if database != EXPECTED_DATABASE or active_role != "none" or readonly != "off":
                    raise BridgeApplyError("admin_session_is_readonly")
                cursor.execute("SET LOCAL statement_timeout = %s", (STATEMENT_TIMEOUT,))
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (int(plan["advisory_lock_key"]),),
                )
                writes_attempted = True
                cursor.execute(SQL_PATH.read_text(encoding="utf-8"))
                cursor.execute(
                    "SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname='cg_core' "
                    "AND c.relname='v_cg_visita_dia_precedencia_route_b_v1'"
                )
                relation = cursor.fetchone()
                if relation != ("v",):
                    raise BridgeApplyError("bridge_view_missing_after_apply")
                cursor.execute(
                    "SELECT a.attname,format_type(a.atttypid,a.atttypmod) "
                    "FROM pg_attribute a WHERE a.attrelid="
                    "'cg_core.v_cg_visita_dia_precedencia_route_b_v1'::regclass "
                    "AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
                )
                if tuple(cursor.fetchall()) != BRIDGE_SIGNATURE:
                    raise BridgeApplyError("bridge_view_signature_mismatch")
                cursor.execute(
                    "SELECT has_table_privilege('stock_zero_codex_ro',%s,'SELECT'),"
                    "has_table_privilege('stock_zero_app_ro',%s,'SELECT')",
                    (
                        "cg_core.v_cg_visita_dia_precedencia_route_b_v1",
                        "cg_core.v_cg_visita_dia_precedencia_route_b_v1",
                    ),
                )
                if cursor.fetchone() != (True, True):
                    raise BridgeApplyError("bridge_readonly_acl_mismatch")
                cursor.execute(
                    "SELECT grantee,privilege_type FROM information_schema.role_table_grants "
                    "WHERE table_schema='cg_core' AND table_name="
                    "'v_cg_visita_dia_precedencia_route_b_v1' "
                    "AND grantee IN ('stock_zero_codex_ro','stock_zero_app_ro')"
                )
                if {tuple(row) for row in cursor.fetchall()} != {
                    ("stock_zero_codex_ro", "SELECT"),
                    ("stock_zero_app_ro", "SELECT"),
                }:
                    raise BridgeApplyError("bridge_direct_acl_signature_mismatch")
            connection.commit()
        return {
            **authority,
            "verdict": COMMITTED_EVIDENCE_PENDING,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "current_user": EXPECTED_ADMIN_ROLE,
            "session_user": EXPECTED_ADMIN_ROLE,
            "transaction_read_only": "off",
            "database": EXPECTED_DATABASE,
            "active_role": "none",
            "bridge_object": "cg_core.v_cg_visita_dia_precedencia_route_b_v1",
            "connection_attempted": connection_attempted,
            "writes_attempted": writes_attempted,
            "committed": True,
            "rolled_back": False,
            "commit_state": COMMITTED_EVIDENCE_PENDING,
        }
    except Exception as exc:
        if isinstance(exc, BridgeApplyError):
            raise
        raise BridgeApplyError(type(exc).__name__) from exc


def _canonical_evidence_path(path: Path, run_id: str) -> Path:
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
        run_id or "",
    ) is None:
        raise BridgeApplyError("canonical_run_id_required")
    expected = (
        ROOT / "evidence" / "runtime" / "023" / run_id / "03_route_b_bridge_apply.json"
    ).resolve()
    actual = (ROOT / path).resolve() if not path.is_absolute() else path.resolve()
    if actual != expected:
        raise BridgeApplyError("canonical_bridge_evidence_path_required")
    if actual.exists():
        raise BridgeApplyError("bridge_evidence_already_exists")
    if not actual.parent.is_dir():
        raise BridgeApplyError("bridge_evidence_parent_missing")
    return actual


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Apply the scoped Route B app bridge")
    result.add_argument("--expected-git-ref", required=True)
    result.add_argument("--db-url-env", required=True)
    result.add_argument("--expected-project-ref", required=True)
    result.add_argument("--confirm", required=True)
    result.add_argument("--plan-023", type=Path, default=PLAN_PATH_DEFAULT)
    result.add_argument("--run-id", required=True)
    result.add_argument("--evidence-json", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.confirm != CONFIRM_TOKEN:
            raise BridgeApplyError("bridge_confirmation_required")
        if args.db_url_env != "DB_URL_ADMIN":
            raise BridgeApplyError("admin_db_url_env_required")
        if os.getenv("STOCK_ZERO_OPERATION_PROFILE") != "admin-ddl":
            raise BridgeApplyError("admin_ddl_wrapper_profile_required")
        if os.getenv("STOCK_ZERO_OPERATION") != "apply-route-b-bridge-023":
            raise BridgeApplyError("bridge_wrapper_operation_required")
        plan = _load_plan(args.plan_023)
        authority = validate_git_guard(args.expected_git_ref, args.plan_023, plan)
        evidence_path = _canonical_evidence_path(args.evidence_json, args.run_id)
        dsn = os.environ.get(args.db_url_env, "")
        if not dsn:
            raise BridgeApplyError("admin_dsn_required")
        validate_dsn(dsn, args.expected_project_ref, plan)
        report = apply_bridge(dsn, authority, plan)
        report.update({
            "document_type": "stock_zero_cg_route_b_bridge_apply_v1",
            "schema_version": 1,
            "run_id": args.run_id,
            "project_ref": args.expected_project_ref,
            "writes_executed": True,
            "transaction_outcome": "COMMITTED",
        })
        report["verdict"] = "PASS_ROUTE_B_APP_BRIDGE_APPLY"
        report["commit_state"] = COMMITTED_EVIDENCE_RECORDED
        try:
            write_json_exclusive(evidence_path, report)
        except Exception as exc:
            receipt = {
                "verdict": COMMITTED_EVIDENCE_RECOVERY_REQUIRED,
                "run_id": args.run_id,
                "commit_state": COMMITTED_EVIDENCE_PENDING,
                "committed": True,
                "rolled_back": False,
                "target_evidence_path": evidence_path.relative_to(ROOT).as_posix(),
                "approved_git_sha": authority["approved_git_sha"],
                "sql_raw_sha256": authority["sql_raw_sha256"],
                "evidence_error": type(exc).__name__,
            }
            receipt["receipt_path"] = str(write_committed_recovery_receipt(receipt))
            print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))
            return 3
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (BridgeApplyError, OSError) as exc:
        print(json.dumps({"verdict": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
