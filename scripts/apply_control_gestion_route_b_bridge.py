from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import psycopg


ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "sql" / "18_control_gestion_route_b_app_bridge_v1.sql"
EXPECTED_HOST = "db.xheyrgfagpoigpgakilu.supabase.co"
EXPECTED_DATABASE = "postgres"
EXPECTED_ADMIN_ROLE = "postgres"
CONFIRM_TOKEN = "STOCK_ZERO_022_APPLY_ROUTE_B_APP_BRIDGE"
LOCK_KEY = 5426926728611921735


class BridgeApplyError(RuntimeError):
    pass


def _git(*args: str, text: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=text, check=False,
    )


def validate_git_guard(expected_git_ref: str) -> dict[str, str]:
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
    relative = SQL_PATH.relative_to(ROOT).as_posix()
    blob = _git("show", f"HEAD:{relative}", text=False)
    if blob.returncode != 0 or not SQL_PATH.is_file():
        raise BridgeApplyError("bridge_sql_not_tracked")
    working = SQL_PATH.read_bytes().replace(b"\r\n", b"\n")
    tracked = blob.stdout.replace(b"\r\n", b"\n")
    if working != tracked:
        raise BridgeApplyError("bridge_sql_worktree_blob_mismatch")
    return {
        "approved_git_sha": expected_git_ref,
        "sql_path": relative,
        "sql_lf_sha256": hashlib.sha256(tracked).hexdigest(),
    }


def validate_dsn(dsn: str, expected_project_ref: str) -> None:
    parsed = urlparse(dsn)
    if expected_project_ref != "xheyrgfagpoigpgakilu":
        raise BridgeApplyError("expected_project_ref_mismatch")
    if parsed.hostname != EXPECTED_HOST:
        raise BridgeApplyError("registered_target_hostname_mismatch")
    if (parsed.path or "").lstrip("/") != EXPECTED_DATABASE:
        raise BridgeApplyError("registered_target_database_mismatch")
    if parsed.username != EXPECTED_ADMIN_ROLE:
        raise BridgeApplyError("admin_role_mismatch")


def apply_bridge(dsn: str, authority: dict[str, str]) -> dict[str, object]:
    connection_attempted = False
    writes_attempted = False
    try:
        connection_attempted = True
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT current_user,session_user,current_setting('transaction_read_only')"
                )
                current_user, session_user, readonly = cursor.fetchone()
                if current_user != EXPECTED_ADMIN_ROLE or session_user != EXPECTED_ADMIN_ROLE:
                    raise BridgeApplyError("admin_session_role_mismatch")
                if readonly != "off":
                    raise BridgeApplyError("admin_session_is_readonly")
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_KEY,))
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
            connection.commit()
        return {
            **authority,
            "verdict": "PASS_ROUTE_B_APP_BRIDGE_APPLY",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "current_user": EXPECTED_ADMIN_ROLE,
            "session_user": EXPECTED_ADMIN_ROLE,
            "transaction_read_only": "off",
            "bridge_object": "cg_core.v_cg_visita_dia_precedencia_route_b_v1",
            "connection_attempted": connection_attempted,
            "writes_attempted": writes_attempted,
            "committed": True,
        }
    except Exception as exc:
        if isinstance(exc, BridgeApplyError):
            raise
        raise BridgeApplyError(type(exc).__name__) from exc


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Apply the scoped Route B app bridge")
    result.add_argument("--expected-git-ref", required=True)
    result.add_argument("--db-url-env", required=True)
    result.add_argument("--expected-project-ref", required=True)
    result.add_argument("--confirm", required=True)
    result.add_argument("--report-json", type=Path)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.confirm != CONFIRM_TOKEN:
            raise BridgeApplyError("bridge_confirmation_required")
        if args.db_url_env != "DB_URL_ADMIN":
            raise BridgeApplyError("admin_db_url_env_required")
        authority = validate_git_guard(args.expected_git_ref)
        dsn = os.environ.get(args.db_url_env, "")
        if not dsn:
            raise BridgeApplyError("admin_dsn_required")
        validate_dsn(dsn, args.expected_project_ref)
        report = apply_bridge(dsn, authority)
        rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        print(rendered)
        if args.report_json:
            args.report_json.write_text(rendered + "\n", encoding="utf-8")
        return 0
    except (BridgeApplyError, OSError) as exc:
        print(json.dumps({"verdict": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
