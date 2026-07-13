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
    result.add_argument("--dry-run", action="store_true")
    result.add_argument("--apply-local", action="store_true")
    result.add_argument("--apply-productive", action="store_true")
    result.add_argument("--rollback-productive", action="store_true")
    result.add_argument("--approved-plan", type=Path)
    result.add_argument("--expected-plan-git-ref")
    result.add_argument("--expected-project-ref")
    result.add_argument("--confirm-productive")
    result.add_argument("--confirm-rollback")
    result.add_argument("--db-url-env", default=LOCAL_DB_ENV)
    result.add_argument("--supersede-batch-id")
    result.add_argument("--rollback-batch-id")
    result.add_argument("--report-json", type=Path)
    result.add_argument("--postcheck-report-json", type=Path)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.apply_productive or args.rollback_productive:
            if not args.approved_plan:
                raise RouteBError("approved_plan_required")
            plan = load_approved_productive_plan(args.approved_plan)
            validate_productive_role_contract(plan)
            validate_registered_productive_target(plan)
            if args.db_url_env != PRODUCTIVE_DB_ENV:
                raise RouteBError("productive_db_url_env_required")
            if args.expected_project_ref != plan["target"]["expected_supabase_project_ref"]:
                raise RouteBError("productive_expected_project_ref_mismatch")
            root = Path(__file__).resolve().parents[1]
            approved_source_plan = build_approved_plan_from_manifest(plan, root)
            if args.expected_plan_git_ref:
                validate_productive_git_guard(plan, args.expected_plan_git_ref, root)
            mode = "apply_productive" if args.apply_productive else "rollback_productive"
            if args.apply_productive and args.confirm_productive != PRODUCTIVE_CONFIRM_TOKEN:
                raise RouteBError("productive_confirmation_required")
            if args.rollback_productive:
                if args.confirm_rollback != PRODUCTIVE_ROLLBACK_CONFIRM_TOKEN:
                    raise RouteBError("productive_rollback_confirmation_required")
                if not args.rollback_batch_id:
                    raise RouteBError("rollback_batch_id_required")
            if plan.get("activation_gate", {}).get("gate_open") is not True:
                report = productive_blocked_report(plan, mode)
            else:
                require_productive_gate_open(plan, mode)
                if args.apply_productive:
                    dsn = os.environ.get(args.db_url_env)
                    if not dsn:
                        raise RouteBError("productive_dsn_required_after_gate_open")
                    report = run_productive_apply(
                        plan, approved_source_plan, dsn, args.postcheck_report_json
                    )
                else:
                    dsn = os.environ.get(args.db_url_env)
                    if not dsn:
                        raise RouteBError("productive_dsn_required_after_gate_open")
                    report = run_productive_rollback(
                        plan, args.rollback_batch_id or "", dsn, args.postcheck_report_json
                    )
            report = {**public_plan(approved_source_plan), **report}
        elif args.rollback_batch_id:
            if not args.apply_local:
                raise RouteBError("rollback_requires_apply_local")
            dsn = os.environ.get(args.db_url_env)
            assert_local_target(args.db_url_env, dsn)
            report = rollback_local(dsn or "", args.rollback_batch_id)
        else:
            if not args.input_dir:
                raise RouteBError("input_dir_required")
            plan = build_plan(args.input_dir)
            if args.apply_local:
                dsn = os.environ.get(args.db_url_env)
                assert_local_target(args.db_url_env, dsn)
                ddl = Path(__file__).resolve().parents[1] / "sql" / "17_kpione_route_b_ingestion_v1.sql"
                report = {**public_plan(plan), **apply_local(plan, dsn or "", ddl, args.supersede_batch_id)}
            else:
                report = public_plan(plan)
        rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        print(rendered)
        if args.report_json:
            args.report_json.write_text(rendered + "\n", encoding="utf-8")
        return 0
    except (RouteBError, OSError) as exc:
        print(json.dumps({
            "verdict": "BLOCKED",
            "outcome": "QUARANTINED_OR_FAILED_INACTIVE",
            "lifecycle_status": "FAILED_BEFORE_REGISTRATION",
            "error": str(exc),
            "apply_authorized": False,
        }, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
