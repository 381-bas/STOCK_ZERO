from __future__ import annotations

import copy
import inspect
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

from scripts.diagnose_stock_zero_db_credentials import diagnose
from scripts.kpione_route_b_v1 import run_productive_apply
from scripts.provision_kpione_route_b_role import (
    EXPECTED_ADMIN_ROLE,
    PLANNED_PRODUCTIVE_ROLE,
    ProvisioningError,
    validate_admin_dsn_target,
    validate_provisioning_plan,
)


ROOT = Path(__file__).resolve().parents[1]
KERNEL = ROOT / "governance" / "kernel" / "current" / "01_project_kernel_stock_zero_v2026_06_16.json"
STATE = ROOT / "governance" / "kernel" / "current" / "02_project_state_stock_zero_v2026_06_30_011.json"
PLAN = ROOT / "plans" / "018_kpione_route_b_productive_apply_plan.json"
PRODUCTIVE_RUNNER = ROOT / "scripts" / "run_kpione_route_b_ingestion_v1.py"
PROVISIONER = ROOT / "scripts" / "provision_kpione_route_b_role.py"
SECRET_WRAPPER = ROOT / "scripts" / "invoke_stock_zero_db_operation.ps1"
DIAGNOSTIC = ROOT / "scripts" / "diagnose_stock_zero_db_credentials.py"


def synthetic_dsn(username: str, *, hostname: str = "db.xheyrgfagpoigpgakilu.supabase.co") -> tuple[str, str]:
    password = "synthetic-credential-password"
    dsn = (
        "postgresql://" + f"{username}:{password}@" +
        f"{hostname}/postgres?sslmode=require"
    )
    return dsn, password


class DatabaseCredentialArchitecture019Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.kernel = json.loads(KERNEL.read_text(encoding="utf-8"))
        cls.state = json.loads(STATE.read_text(encoding="utf-8"))
        cls.plan = json.loads(PLAN.read_text(encoding="utf-8"))

    def test_kernel_has_exact_credential_classes_vault_and_operation_order(self) -> None:
        contract = self.kernel["database_access_and_secret_contract"]
        self.assertEqual(contract["status"], "ACTIVE")
        self.assertEqual(contract["vault"]["name"], "STOCK_ZERO")
        self.assertEqual(
            contract["vault"]["provider"],
            "Microsoft.PowerShell.SecretManagement + Microsoft.PowerShell.SecretStore",
        )
        self.assertEqual(set(contract["credential_classes"]), {
            "DB_URL_CODEX_RO", "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE", "DB_URL_ADMIN",
        })
        self.assertFalse(contract["credential_classes"]["DB_URL_CODEX_RO"]["writes_allowed"])
        self.assertFalse(contract["credential_classes"]["DB_URL_KPIONE_ROUTE_B_PRODUCTIVE"]["ddl_allowed"])
        self.assertTrue(contract["credential_classes"]["DB_URL_ADMIN"]["ddl_allowed"])
        self.assertEqual(contract["ddl_boundary"]["exclusive_credential"], "DB_URL_ADMIN")
        self.assertEqual(contract["operation_order"], [
            "read-only precheck", "explicit authorization", "productive apply", "read-only postcheck",
        ])
        self.assertEqual(set(contract["vault"]["secret_names"].values()), {
            "STOCK_ZERO_DB_CODEX_RO",
            "STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE",
            "STOCK_ZERO_DB_ADMIN",
            "STOCK_ZERO_DB_KPIONE_ROUTE_B_PASSWORD",
        })

    def test_project_state_is_secret_free_and_all_productive_gates_remain_closed(self) -> None:
        credential_state = self.state["database_credential_status"]
        self.assertEqual(credential_state, {
            "secret_vault": "NOT_CONFIGURED_OR_NOT_VERIFIED",
            "productive_role": "PLANNED_NOT_PROVISIONED",
            "admin_provisioning": "NOT_EXECUTED",
            "readonly_precheck": "NOT_AUTHORIZED_OR_EXECUTED",
            "productive_gate": "CLOSED",
        })
        rendered = json.dumps(self.state)
        for token in ("postgresql://", "STOCK_ZERO_DB_", "DB_URL_ADMIN", "password"):
            self.assertNotIn(token, rendered)
        authorization = self.state["authorization"]
        for key in (
            "018_authorized", "supabase_access_authorized", "sql_execution_authorized",
            "db_reads_authorized", "db_writes_authorized", "apply_authorized",
            "legacy_destructive_action_authorized",
        ):
            self.assertFalse(authorization[key])
        self.assertFalse(self.state["current_preparation"]["activation_gate"]["gate_open"])
        self.assertFalse(self.state["current_preparation"]["productive_apply_authorized"])

    def test_productive_runner_has_no_admin_credential_or_ddl_execution(self) -> None:
        runner_source = PRODUCTIVE_RUNNER.read_text(encoding="utf-8")
        core_source = inspect.getsource(run_productive_apply)
        self.assertNotIn("DB_URL_ADMIN", runner_source)
        self.assertNotIn("DB_URL_ADMIN", core_source)
        self.assertNotIn("sql_file", core_source)
        self.assertNotIn("read_text", core_source)
        for token in ("CREATE ", "ALTER ", "DROP ", "GRANT ", "REVOKE "):
            self.assertNotIn(token, core_source.upper())
        self.assertIn("_assert_route_b_object_signatures", core_source)
        self.assertIn("allow_empty=False", core_source)

    def test_admin_target_rejects_wrong_host_and_non_admin_role_without_connection(self) -> None:
        wrong_host, password = synthetic_dsn(EXPECTED_ADMIN_ROLE, hostname="db.wrong.supabase.co")
        with self.assertRaisesRegex(ProvisioningError, "admin_target_hostname_mismatch"):
            validate_admin_dsn_target(wrong_host, self.plan)
        wrong_role, _ = synthetic_dsn(PLANNED_PRODUCTIVE_ROLE)
        with self.assertRaisesRegex(ProvisioningError, "admin_credential_role_mismatch"):
            validate_admin_dsn_target(wrong_role, self.plan)
        self.assertNotIn(password, "admin_target_hostname_mismatch")

    def test_provisioner_rejects_alternate_sql_object_or_registered_target_scope(self) -> None:
        mutations = (
            lambda plan: plan["physical_contract"].update({"sql_file": "sql/unapproved.sql"}),
            lambda plan: plan["physical_contract"]["objects"].append("public.unrelated"),
            lambda plan: plan["target"].update({"expected_hostname": "db.wrong.supabase.co"}),
            lambda plan: plan["target"].update({"planned_productive_role": "wrong_role"}),
        )
        for index, mutation in enumerate(mutations):
            altered = copy.deepcopy(self.plan)
            mutation(altered)
            with self.subTest(case=index), self.assertRaises(ProvisioningError):
                validate_provisioning_plan(altered)

    def test_provisioner_declares_restrictive_role_and_only_route_b_grants(self) -> None:
        source = PROVISIONER.read_text(encoding="utf-8")
        for token in (
            "LOGIN", "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE",
            "NOREPLICATION", "NOBYPASSRLS", "CONNECTION LIMIT",
        ):
            self.assertIn(token, source)
        self.assertIn("GRANT SELECT,INSERT,UPDATE ON TABLE", source)
        self.assertIn("GRANT USAGE,SELECT ON SEQUENCE", source)
        self.assertIn("REVOKE CREATE ON SCHEMA", source)
        self.assertNotIn("GRANT ALL", source.upper())
        legacy_lines = [line for line in source.splitlines() if "kpione2_raw" in line]
        self.assertTrue(legacy_lines)
        self.assertTrue(all("SELECT to_regclass" in line for line in legacy_lines))

    def test_secret_wrapper_uses_child_environment_only_and_prints_no_values(self) -> None:
        source = SECRET_WRAPPER.read_text(encoding="utf-8")
        for profile in ("readonly", "route-b-productive", "admin-provisioning"):
            self.assertIn(f"'{profile}'", source)
        for secret_name in (
            "STOCK_ZERO_DB_CODEX_RO", "STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE",
            "STOCK_ZERO_DB_ADMIN", "STOCK_ZERO_DB_KPIONE_ROUTE_B_PASSWORD",
        ):
            self.assertIn(secret_name, source)
        self.assertIn("ProcessStartInfo", source)
        self.assertIn("$startInfo.Environment", source)
        self.assertNotIn("SetEnvironmentVariable", source)
        self.assertNotIn("[EnvironmentVariableTarget]::User", source)
        self.assertNotIn("[EnvironmentVariableTarget]::Machine", source)
        self.assertNotIn("Write-Host", source)
        self.assertNotIn("Write-Output", source)

    def test_diagnostic_reports_booleans_without_dsn_or_password(self) -> None:
        dsn, password = synthetic_dsn("stock_zero_codex_ro")
        environment = {"DB_URL_CODEX_RO": dsn}
        report = diagnose("readonly", environment)
        self.assertTrue(report["secret_env_present"])
        self.assertTrue(report["username_matches"])
        self.assertTrue(report["hostname_matches"])
        self.assertTrue(report["database_matches"])
        self.assertTrue(report["ssl_required"])
        rendered = json.dumps(report)
        self.assertNotIn(dsn, rendered)
        self.assertNotIn(password, rendered)

        process_environment = os.environ.copy()
        process_environment["DB_URL_CODEX_RO"] = dsn
        completed = subprocess.run(
            [sys.executable, str(DIAGNOSTIC), "--credential-class", "readonly"],
            cwd=ROOT, env=process_environment, capture_output=True, text=True, check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertNotIn(dsn, completed.stdout)
        self.assertNotIn(password, completed.stdout)

    def test_secret_provider_can_change_without_runner_changes(self) -> None:
        contract = self.kernel["database_access_and_secret_contract"]["secret_handling"]
        self.assertIn("invocation adapter", contract["provider_abstraction"])
        runner_source = PRODUCTIVE_RUNNER.read_text(encoding="utf-8")
        self.assertNotIn("SecretManagement", runner_source)
        self.assertNotIn("SecretStore", runner_source)


if __name__ == "__main__":
    unittest.main()
