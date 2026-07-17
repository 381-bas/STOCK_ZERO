from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.provision_kpione_route_b_role import (
    PLANNED_PRODUCTIVE_ROLE,
    PRODUCTIVE_CONNECTION_LIMIT,
    ProvisioningError,
    _blocked_report,
    provision_route_b_role,
)


ROOT = Path(__file__).resolve().parents[1]
PLAN = json.loads((ROOT / "plans" / "018_kpione_route_b_productive_apply_plan.json").read_text(encoding="utf-8"))


class SyntheticDatabaseError(Exception):
    def __init__(self, sqlstate: str | None = None) -> None:
        super().__init__("synthetic database failure with hidden details")
        self.sqlstate = sqlstate


class StageFailingCursor:
    def __init__(self, failure_token: str, sqlstate: str | None) -> None:
        self.failure_token = failure_token
        self.sqlstate = sqlstate
        self.last_query = ""

    def __enter__(self) -> "StageFailingCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: object, _params: object = None) -> None:
        rendered = str(query)
        self.last_query = rendered
        if self.failure_token in rendered:
            raise SyntheticDatabaseError(self.sqlstate)

    def fetchone(self) -> tuple[object, ...]:
        if "current_user" in self.last_query:
            return ("postgres", "postgres", "postgres", "off")
        if "SELECT 1 FROM pg_roles" in self.last_query:
            return None
        if "pg_get_serial_sequence" in self.last_query:
            return ("cg_raw.kpione_raw_event_photo_staging_v1_staging_id_seq",)
        if "rolcanlogin" in self.last_query:
            return (True, False, False, False, False, False, PRODUCTIVE_CONNECTION_LIMIT)
        if "to_regclass" in self.last_query:
            return ("cg_raw.kpione2_raw",)
        return (None,)


class StageFailingConnection:
    def __init__(self, failure_token: str, sqlstate: str | None) -> None:
        self.cursor_instance = StageFailingCursor(failure_token, sqlstate)
        self.autocommit = True
        self.rollback_called = False
        self.commit_called = False
        self.closed = False

    def cursor(self) -> StageFailingCursor:
        return self.cursor_instance

    def rollback(self) -> None:
        self.rollback_called = True

    def commit(self) -> None:
        self.commit_called = True

    def close(self) -> None:
        self.closed = True


class RouteBProvisionerStageReportingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = copy.deepcopy(PLAN)
        self.git_guard = {
            "approved_git_sha": "a" * 40,
            "plan_path": "plans/018_kpione_route_b_productive_apply_plan.json",
            "plan_sha256": "b" * 64,
            "ddl_sha256": self.plan["physical_contract"]["sql_sha256"],
        }
        self.admin_dsn = (
            "postgresql://postgres:admin-secret-never-report@"
            f"{self.plan['target']['expected_hostname']}/postgres?sslmode=require"
        )
        self.role_password = "role-password-never-report"

    def _run_failure(self, failure_token: str, sqlstate: str | None) -> tuple[dict[str, object], StageFailingConnection, Path]:
        connection = StageFailingConnection(failure_token, sqlstate)
        with tempfile.TemporaryDirectory() as folder, patch(
            "scripts.provision_kpione_route_b_role._legacy_snapshot",
            return_value={"legacy": "unchanged"},
        ), patch(
            "scripts.provision_kpione_route_b_role._public_acl_snapshot",
            return_value={"public_acl": "unchanged"},
        ), patch(
            "scripts.provision_kpione_route_b_role.legacy_structural_identity",
            side_effect=lambda value: value,
        ), patch(
            "scripts.provision_kpione_route_b_role._assert_route_b_object_signatures",
            return_value=None,
        ):
            evidence_path = Path(folder) / "02_admin_provisioning.json"
            with self.assertRaisesRegex(ProvisioningError, "route_b_admin_provisioning_failed") as context:
                provision_route_b_role(
                    self.plan,
                    self.admin_dsn,
                    self.role_password,
                    evidence_path,
                    root=ROOT,
                    connect_fn=lambda _dsn: connection,
                    expected_admin_username="postgres",
                    git_guard=self.git_guard,
                    ddl="SYNTHETIC_DDL_APPLY",
                    run_id="27dd51bc-fec8-4ce2-9fb6-5f863ac57d26",
                )
            report = _blocked_report(context.exception)
            self.assertFalse(evidence_path.exists())
            return report, connection, evidence_path

    def assert_sanitized_failure(
        self,
        failure_token: str,
        sqlstate: str | None,
        expected_stage: str,
        expected_category: str,
    ) -> None:
        report, connection, _evidence_path = self._run_failure(failure_token, sqlstate)
        self.assertEqual(report["verdict"], "BLOCKED")
        self.assertEqual(report["error"], "route_b_admin_provisioning_failed")
        self.assertEqual(report["failed_stage"], expected_stage)
        self.assertEqual(report["exception_type"], "SyntheticDatabaseError")
        self.assertEqual(report["sqlstate"], sqlstate)
        self.assertEqual(report["fixed_error_category"], expected_category)
        self.assertTrue(report["connection_attempted"])
        self.assertTrue(report["writes_attempted"])
        self.assertFalse(report["committed"])
        self.assertTrue(connection.rollback_called)
        self.assertFalse(connection.commit_called)
        rendered = json.dumps(report, sort_keys=True)
        self.assertNotIn(self.admin_dsn, rendered)
        self.assertNotIn("admin-secret-never-report", rendered)
        self.assertNotIn(self.role_password, rendered)
        self.assertNotIn("synthetic database failure with hidden details", rendered)

    def test_create_role_failure_reports_create_role_stage(self) -> None:
        self.assert_sanitized_failure("CREATE ROLE", "42501", "create_role", "INSUFFICIENT_PRIVILEGE")

    def test_alter_role_failure_reports_alter_role_attributes_stage(self) -> None:
        self.assert_sanitized_failure("ALTER ROLE", "0A000", "alter_role_attributes", "FEATURE_NOT_SUPPORTED")

    def test_ddl_failure_reports_ddl_apply_stage(self) -> None:
        self.assert_sanitized_failure("SYNTHETIC_DDL_APPLY", "42P07", "ddl_apply", "RELATION_ALREADY_EXISTS")

    def test_ownership_failure_reports_ownership_stage(self) -> None:
        self.assert_sanitized_failure("ALTER TABLE", "42704", "ownership_tables", "OBJECT_NOT_FOUND")

    def test_grant_failure_reports_grant_stage(self) -> None:
        self.assert_sanitized_failure("GRANT SELECT ON TABLE", "23505", "view_privileges", "UNIQUE_VIOLATION")

    def test_error_without_sqlstate_uses_unclassified_runtime_category(self) -> None:
        self.assert_sanitized_failure("GRANT USAGE ON SEQUENCE", None, "sequence_privileges", "UNCLASSIFIED_RUNTIME_ERROR")


if __name__ == "__main__":
    unittest.main()
