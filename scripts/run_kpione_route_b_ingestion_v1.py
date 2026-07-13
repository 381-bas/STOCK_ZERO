from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from kpione_route_b_v1 import (
    LOCAL_DB_ENV, RouteBError, apply_local, assert_local_target, build_plan, public_plan,
    rollback_local,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="KPIONE Route B local-only ingestion runner")
    result.add_argument("--input-dir", type=Path)
    result.add_argument("--dry-run", action="store_true")
    result.add_argument("--apply-local", action="store_true")
    result.add_argument("--db-url-env", default=LOCAL_DB_ENV)
    result.add_argument("--supersede-batch-id")
    result.add_argument("--rollback-batch-id")
    result.add_argument("--report-json", type=Path)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.rollback_batch_id:
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
            "error": str(exc),
            "apply_authorized": False,
        }, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
