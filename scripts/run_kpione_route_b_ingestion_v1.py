from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from kpione_route_b_v1 import (
    LOCAL_DB_ENV, PRODUCTIVE_CONFIRM_TOKEN, PRODUCTIVE_DB_ENV,
    PRODUCTIVE_ROLLBACK_CONFIRM_TOKEN, RouteBError, apply_local, assert_local_target,
    build_approved_plan_from_manifest, build_plan, load_approved_productive_plan,
    productive_blocked_report, public_plan, require_productive_gate_open, rollback_local,
    run_productive_apply, run_productive_rollback, validate_productive_git_guard,
    validate_productive_role_contract, validate_registered_productive_target,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="KPIONE Route B local-only ingestion runner")
    result.add_argument("--input-dir", type=Path)
    modes = result.add_mutually_exclusive_group()
    modes.add_argument("--dry-run", action="store_true")
    modes.add_argument("--apply-local", action="store_true")
    modes.add_argument("--apply-productive", action="store_true")
    modes.add_argument("--rollback-productive", action="store_true")
    result.add_argument("--approved-plan", type=Path)
    result.add_argument("--expected-plan-git-ref")
    result.add_argument("--expected-project-ref")
    result.add_argument("--confirm-productive")
    result.add_argument("--confirm-rollback")
    result.add_argument("--db-url-env")
    result.add_argument("--supersede-batch-id")
    result.add_argument("--rollback-batch-id")
    result.add_argument("--report-json", type=Path)
    result.add_argument("--postcheck-report-json", type=Path)
    return result


def _require_productive_arguments(args: argparse.Namespace) -> None:
    required = {
        "approved_plan": args.approved_plan,
        "expected_plan_git_ref": args.expected_plan_git_ref,
        "expected_project_ref": args.expected_project_ref,
        "db_url_env": args.db_url_env,
        "postcheck_report_json": args.postcheck_report_json,
    }
    missing = [name for name, value in required.items() if value in {None, ""}]
    if missing:
        raise RouteBError("productive_arguments_required:" + ",".join(sorted(missing)))
    if args.report_json:
        raise RouteBError("report_json_not_allowed_for_productive_mode")
    if not args.postcheck_report_json.parent.is_dir():
        raise RouteBError("productive_postcheck_report_parent_missing")
    if args.apply_productive:
        if args.confirm_productive != PRODUCTIVE_CONFIRM_TOKEN:
            raise RouteBError("productive_confirmation_required")
        if args.confirm_rollback:
            raise RouteBError("rollback_confirmation_not_allowed_for_apply")
    else:
        if args.confirm_rollback != PRODUCTIVE_ROLLBACK_CONFIRM_TOKEN:
            raise RouteBError("productive_rollback_confirmation_required")
        if args.confirm_productive:
            raise RouteBError("apply_confirmation_not_allowed_for_rollback")
        if not args.rollback_batch_id:
            raise RouteBError("rollback_batch_id_required")


def main() -> int:
    args = parser().parse_args()
    dsn_read = False
    try:
        if args.apply_productive or args.rollback_productive:
            _require_productive_arguments(args)
            root = Path(__file__).resolve().parents[1]
            git_guard = validate_productive_git_guard(
                args.approved_plan, args.expected_plan_git_ref, root,
            )
            plan = load_approved_productive_plan(args.approved_plan)
            validate_registered_productive_target(plan)
            validate_productive_role_contract(plan)
            if args.db_url_env != PRODUCTIVE_DB_ENV:
                raise RouteBError("productive_db_url_env_required")
            if args.expected_project_ref != plan["target"]["expected_supabase_project_ref"]:
                raise RouteBError("productive_expected_project_ref_mismatch")
            approved_source_plan = build_approved_plan_from_manifest(plan, root)
            mode = "apply_productive" if args.apply_productive else "rollback_productive"
            if plan.get("activation_gate", {}).get("gate_open") is not True:
                report = {
                    **public_plan(approved_source_plan),
                    **git_guard,
                    **productive_blocked_report(plan, mode),
                }
            else:
                require_productive_gate_open(plan, mode)
                dsn = os.environ.get(args.db_url_env)
                dsn_read = True
                if not dsn:
                    raise RouteBError("productive_dsn_required_after_gate_open")
                if args.apply_productive:
                    report = run_productive_apply(
                        plan, approved_source_plan, dsn, args.postcheck_report_json,
                        git_guard=git_guard, root=root,
                    )
                else:
                    report = run_productive_rollback(
                        plan, approved_source_plan, args.rollback_batch_id, dsn,
                        args.postcheck_report_json, git_guard=git_guard,
                    )
        elif args.rollback_batch_id:
            if not args.apply_local:
                raise RouteBError("rollback_requires_apply_local")
            db_env = args.db_url_env or LOCAL_DB_ENV
            dsn = os.environ.get(db_env)
            assert_local_target(db_env, dsn)
            report = rollback_local(dsn or "", args.rollback_batch_id)
        else:
            if not args.input_dir:
                raise RouteBError("input_dir_required")
            plan = build_plan(args.input_dir)
            if args.apply_local:
                db_env = args.db_url_env or LOCAL_DB_ENV
                dsn = os.environ.get(db_env)
                assert_local_target(db_env, dsn)
                ddl = Path(__file__).resolve().parents[1] / "sql" / "17_kpione_route_b_ingestion_v1.sql"
                report = {
                    **public_plan(plan),
                    **apply_local(plan, dsn or "", ddl, args.supersede_batch_id),
                }
            else:
                report = public_plan(plan)
        rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        print(rendered)
        if args.report_json:
            args.report_json.write_text(rendered + "\n", encoding="utf-8")
        return 0
    except (RouteBError, OSError, ValueError) as exc:
        report = getattr(exc, "report", None) or {}
        committed = getattr(exc, "committed", False)
        report.update({
            "verdict": "BLOCKED",
            "outcome": (
                "POSTCHECK_REJECTED_REQUIRES_EXPLICIT_ROLLBACK_AUTHORIZATION"
                if committed else "QUARANTINED_OR_FAILED_INACTIVE"
            ),
            "lifecycle_status": (
                "COMMITTED_POSTCHECK_REJECTED" if committed
                else "FAILED_BEFORE_REGISTRATION"
            ),
            "error": str(exc),
            "apply_authorized": False,
            "connection_attempted": getattr(exc, "connection_attempted", False),
            "writes_attempted": getattr(exc, "writes_attempted", False),
            "committed": committed,
            "dsn_read": dsn_read,
        })
        print(json.dumps(report, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
