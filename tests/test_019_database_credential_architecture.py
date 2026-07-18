from __future__ import annotations

import copy
import hashlib
import inspect
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace

from scripts.diagnose_stock_zero_db_credentials import diagnose
from scripts.kpione_route_b_v1 import run_productive_apply
from scripts.provision_kpione_route_b_role import (
    EXPECTED_ADMIN_ROLE,
    PLANNED_PRODUCTIVE_ROLE,
    PRODUCTIVE_CONNECTION_LIMIT,
    PROVISION_CONFIRM_TOKEN,
    ProvisioningError,
    _role_statement,
    execute_cli,
    validate_admin_git_guard,
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


def find_pwsh() -> str | None:
    discovered = shutil.which("pwsh")
    if discovered:
        return discovered
    bundled = Path("C:/Program Files/PowerShell/7/pwsh.exe")
    if bundled.exists():
        return str(bundled)
    return None


def run_git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=root, capture_output=True, text=True, check=True,
    )
    return completed.stdout.strip()


def make_authority_repository(root: Path) -> tuple[Path, Path, str]:
    plan_path = root / "plans" / PLAN.name
    sql_path = root / "sql" / "17_kpione_route_b_ingestion_v1.sql"
    plan_path.parent.mkdir(parents=True)
    sql_path.parent.mkdir(parents=True)
    sql_path.write_bytes(b"SELECT 17;\n")
    plan = {"physical_contract": {"sql_file": "sql/17_kpione_route_b_ingestion_v1.sql"}}
    plan["physical_contract"]["sql_sha256"] = hashlib.sha256(sql_path.read_bytes()).hexdigest()
    plan_path.write_bytes((json.dumps(plan, indent=2) + "\n").encode("utf-8"))
    run_git(root, "init", "--quiet")
    run_git(root, "config", "user.email", "stock-zero-tests@example.invalid")
    run_git(root, "config", "user.name", "STOCK_ZERO Tests")
    run_git(root, "add", "plans", "sql")
    run_git(root, "commit", "--quiet", "-m", "authority fixture")
    return plan_path, sql_path, run_git(root, "rev-parse", "HEAD")


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
        for token in ("LOGIN", "NOINHERIT", "CONNECTION LIMIT", "PASSWORD"):
            self.assertIn(token, source)
        self.assertNotIn("NOSUPERUSER", source)
        self.assertNotIn("NOCREATEDB", source)
        self.assertNotIn("NOCREATEROLE", source)
        self.assertNotIn("NOREPLICATION", source)
        self.assertNotIn("NOBYPASSRLS", source)
        self.assertIn("sql.Literal(password)", source)
        self.assertIn("sql.Literal(PRODUCTIVE_CONNECTION_LIMIT)", source)
        self.assertIn("rolinherit", source)
        self.assertIn(
            "True, False, False, False, False, False, False, PRODUCTIVE_CONNECTION_LIMIT",
            source,
        )
        self.assertIn('"inherit": False', source)
        self.assertEqual(PRODUCTIVE_CONNECTION_LIMIT, 5)
        rendered_role_statement = str(_role_statement("synthetic_role", "synthetic_password"))
        for token in ("LOGIN", "NOINHERIT", "CONNECTION LIMIT", "PASSWORD"):
            self.assertIn(token, rendered_role_statement)
        for token in ("SUPERUSER", "CREATEDB", "CREATEROLE", "REPLICATION", "BYPASSRLS"):
            self.assertNotIn(token, rendered_role_statement)
        self.assertIn("Literal('synthetic_password')", rendered_role_statement)
        self.assertIn("GRANT SELECT,INSERT ON TABLE", source)
        self.assertIn("GRANT UPDATE(status,activated_at,rolled_back_at) ON TABLE", source)
        self.assertIn("GRANT USAGE ON SEQUENCE", source)
        self.assertNotIn("GRANT USAGE,SELECT ON SEQUENCE", source)
        self.assertNotIn("FROM PUBLIC", source)
        self.assertIn("REVOKE CREATE ON SCHEMA", source)
        self.assertNotIn("GRANT ALL", source.upper())
        legacy_lines = [line for line in source.splitlines() if "kpione2_raw" in line]
        self.assertTrue(legacy_lines)
        for mutation in (
            "ALTER TABLE cg_raw.kpione2_raw", "DROP TABLE cg_raw.kpione2_raw",
            "TRUNCATE cg_raw.kpione2_raw", "INSERT INTO cg_raw.kpione2_raw",
            "UPDATE cg_raw.kpione2_raw", "DELETE FROM cg_raw.kpione2_raw",
        ):
            self.assertNotIn(mutation, source)

    def test_admin_git_guard_accepts_only_exact_clean_head_plan_and_sql_blobs(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            plan_path, sql_path, head = make_authority_repository(root)
            guard = validate_admin_git_guard(plan_path, head, root)
            self.assertEqual(guard["approved_git_sha"], head)
            self.assertEqual(guard["plan_path"], f"plans/{PLAN.name}")
            self.assertEqual(guard["ddl_path"], "sql/17_kpione_route_b_ingestion_v1.sql")
            self.assertEqual(guard["ddl_sha256"], hashlib.sha256(b"SELECT 17;\n").hexdigest())
            with self.assertRaisesRegex(ProvisioningError, "repository_head_mismatch"):
                validate_admin_git_guard(plan_path, "0" * 40, root)

            marker = root / "tracked.txt"
            marker.write_text("clean\n", encoding="utf-8")
            run_git(root, "add", "tracked.txt")
            run_git(root, "commit", "--quiet", "-m", "tracked marker")
            head = run_git(root, "rev-parse", "HEAD")
            marker.write_text("dirty\n", encoding="utf-8")
            with self.assertRaisesRegex(ProvisioningError, "repository_worktree_not_clean"):
                validate_admin_git_guard(plan_path, head, root)
            marker.write_text("clean\n", encoding="utf-8")
            marker.write_text("staged\n", encoding="utf-8")
            run_git(root, "add", "tracked.txt")
            with self.assertRaisesRegex(ProvisioningError, "repository_index_not_clean"):
                validate_admin_git_guard(plan_path, head, root)

    def test_admin_git_guard_rejects_untracked_or_blob_divergent_authority(self) -> None:
        for case in ("plan_missing", "plan_local", "sql_local", "plan_and_sql"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                plan_path, sql_path, head = make_authority_repository(root)
                if case == "plan_missing":
                    plan_path.unlink()
                    run_git(root, "add", "plans")
                    run_git(root, "commit", "--quiet", "-m", "remove plan")
                    head = run_git(root, "rev-parse", "HEAD")
                    expected_error = "approved_plan_not_tracked_at_head"
                else:
                    changed = [plan_path] if case == "plan_local" else [sql_path]
                    if case == "plan_and_sql":
                        changed = [plan_path, sql_path]
                    for path in changed:
                        run_git(root, "update-index", "--assume-unchanged", path.relative_to(root).as_posix())
                    if sql_path in changed:
                        sql_path.write_text("SELECT 19;\n", encoding="utf-8")
                    if plan_path in changed:
                        altered = json.loads(plan_path.read_text(encoding="utf-8"))
                        altered["physical_contract"]["sql_sha256"] = "f" * 64
                        plan_path.write_text(json.dumps(altered, indent=2) + "\n", encoding="utf-8")
                    expected_error = (
                        "approved_plan_worktree_blob_mismatch"
                        if plan_path in changed else "route_b_sql_worktree_blob_mismatch"
                    )
                with self.assertRaisesRegex(ProvisioningError, expected_error):
                    validate_admin_git_guard(plan_path, head, root)

    def test_invalid_git_authority_does_not_read_secret_environment(self) -> None:
        class NoSecretReads(dict[str, str]):
            def get(self, key: str, default: str | None = None) -> str | None:
                raise AssertionError(f"secret read before authority completed: {key}")

        run_id = str(uuid.uuid4())
        args = SimpleNamespace(
            plan=PLAN,
            run_id=run_id,
            expected_plan_git_ref="0" * 40,
            db_url_env="DB_URL_ADMIN",
            expected_project_ref="xheyrgfagpoigpgakilu",
            confirm=PROVISION_CONFIRM_TOKEN,
            evidence_json=ROOT / "evidence" / "runtime" / "020B" / run_id / "02_admin_provisioning.json",
            reconcile_provisioning_evidence=False,
            prior_failure_report=None,
            authority_precheck_only=False,
        )
        with self.assertRaisesRegex(ProvisioningError, "repository_head_mismatch"):
            execute_cli(args, environ=NoSecretReads(), root=ROOT)

    def test_complete_authority_precheck_reads_no_secret_environment(self) -> None:
        class NoSecretReads(dict[str, str]):
            def get(self, key: str, default: str | None = None) -> str | None:
                raise AssertionError(f"authority-only precheck read a secret: {key}")

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            plan_path = root / "plans" / PLAN.name
            sql_path = root / "sql" / "17_kpione_route_b_ingestion_v1.sql"
            plan_path.parent.mkdir(parents=True)
            sql_path.parent.mkdir(parents=True)
            plan_path.write_bytes(PLAN.read_bytes())
            sql_path.write_bytes((ROOT / "sql" / sql_path.name).read_bytes())
            run_git(root, "init", "--quiet")
            run_git(root, "config", "user.email", "stock-zero-tests@example.invalid")
            run_git(root, "config", "user.name", "STOCK_ZERO Tests")
            run_git(root, "add", "plans", "sql")
            run_git(root, "commit", "--quiet", "-m", "full authority fixture")
            head = run_git(root, "rev-parse", "HEAD")
            run_id = str(uuid.uuid4())
            args = SimpleNamespace(
                plan=plan_path,
                run_id=run_id,
                expected_plan_git_ref=head,
                db_url_env="DB_URL_ADMIN",
                expected_project_ref="xheyrgfagpoigpgakilu",
                confirm=PROVISION_CONFIRM_TOKEN,
                evidence_json=root / "evidence" / "runtime" / "020B" / run_id / "02_admin_provisioning.json",
                reconcile_provisioning_evidence=False,
                prior_failure_report=None,
                authority_precheck_only=True,
            )
            report = execute_cli(args, environ=NoSecretReads(), root=root)
            self.assertEqual(report["verdict"], "PASS_ADMIN_AUTHORITY_PRECHECK")
            self.assertEqual(report["approved_git_sha"], head)
            self.assertFalse(report["connection_attempted"])

    def test_secret_wrapper_uses_typed_entrypoints_and_scrubbed_child_environment(self) -> None:
        source = SECRET_WRAPPER.read_text(encoding="utf-8")
        for operation in (
            "readonly-precheck", "readonly-postcheck", "verify-route-b-role",
            "route-b-apply", "route-b-rollback", "admin-provision",
            "admin-reconcile-provisioning-evidence",
            "admin-reconcile-existing-provisioned-state",
            "admin-reconcile-route-b-readonly-observer",
            "diagnose-readonly", "diagnose-route-b", "diagnose-admin",
        ):
            self.assertIn(f"'{operation}'", source)
        for secret_name in (
            "STOCK_ZERO_DB_CODEX_RO", "STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE",
            "STOCK_ZERO_DB_ADMIN", "STOCK_ZERO_DB_KPIONE_ROUTE_B_PASSWORD",
        ):
            self.assertIn(secret_name, source)
        self.assertIn("ProcessStartInfo", source)
        self.assertIn("$startInfo.Environment", source)
        self.assertNotIn("[string]$FilePath", source)
        self.assertIn("scripts/provision_kpione_route_b_role.py", source)
        self.assertIn("AuthorityPrecheck = $true", source)
        self.assertLess(source.index("AuthorityPrecheck"), source.index("Get-SecretVault"))
        for name in (
            "DB_URL_CODEX_RO", "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE", "DB_URL_ADMIN",
            "KPIONE_ROUTE_B_PRODUCTIVE_PASSWORD", "DB_URL_LOAD", "DB_URL_CODEX_LOCAL",
        ):
            self.assertIn(name, source)
        self.assertIn("$startInfo.Environment.Remove($name)", source)
        self.assertNotIn("SetEnvironmentVariable", source)
        self.assertNotIn("[EnvironmentVariableTarget]::User", source)
        self.assertNotIn("[EnvironmentVariableTarget]::Machine", source)
        self.assertNotIn("Write-Host", source)
        self.assertNotIn("Write-Output", source)

    def test_secret_wrapper_parses_with_windows_powershell_ast(self) -> None:
        command = (
            "$errors=$null; [System.Management.Automation.Language.Parser]::ParseFile("
            f"'{SECRET_WRAPPER}', [ref]$null, [ref]$errors) | Out-Null; "
            "if($errors.Count){$errors | ForEach-Object {$_.Message}; exit 1}"
        )
        completed = subprocess.run(
            ["powershell", "-NoLogo", "-NoProfile", "-Command", command],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_secret_wrapper_all_operations_run_under_strictmode_with_synthetic_children(self) -> None:
        pwsh = find_pwsh()
        if not pwsh:
            self.skipTest("PowerShell 7 is required for the strict wrapper probe")
        operations = (
            "readonly-precheck", "readonly-postcheck", "verify-route-b-role",
            "route-b-apply", "route-b-rollback", "admin-provision",
            "admin-reconcile-provisioning-evidence",
            "admin-reconcile-existing-provisioned-state",
            "admin-reconcile-route-b-readonly-observer",
            "apply-route-b-app-bridge", "diagnose-readonly", "diagnose-route-b",
            "diagnose-admin",
        )
        source = SECRET_WRAPPER.read_text(encoding="utf-8")
        for operation in operations:
            self.assertIn(f"'{operation}'", source)
        self.assertIn("$entrypoint.ContainsKey('AuthorityPrecheck')", source)
        self.assertIn("[bool]$entrypoint['AuthorityPrecheck']", source)
        self.assertNotIn("if ($entrypoint.AuthorityPrecheck)", source)
        command = (
            "Set-StrictMode -Version Latest; "
            "$without=@{Script='x';Profile='readonly';PrefixArguments=@()}; "
            "$with=@{Script='x';Profile='admin';PrefixArguments=@();AuthorityPrecheck=$true}; "
            "function Test-Enabled($entrypoint) { "
            "($entrypoint.ContainsKey('AuthorityPrecheck') -and [bool]$entrypoint['AuthorityPrecheck']) "
            "}; "
            "if (Test-Enabled $without) { exit 1 }; "
            "if (-not (Test-Enabled $with)) { exit 2 }"
        )
        completed = subprocess.run(
            [pwsh, "-NoLogo", "-NoProfile", "-Command", command],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_secret_wrapper_child_probe_receives_only_operation_secrets(self) -> None:
        if os.environ.get("STOCK_ZERO_ENABLE_POWERSHELL_SECRETSTORE_RUNTIME_TESTS") != "1":
            self.skipTest("PowerShell SecretStore runtime probes are opt-in to avoid interactive vault prompts")
        pwsh = find_pwsh()
        if not pwsh:
            self.skipTest("PowerShell 7 is required for the child isolation probe")
        managed = (
            "DB_URL_CODEX_RO", "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE", "DB_URL_ADMIN",
            "KPIONE_ROUTE_B_PRODUCTIVE_PASSWORD", "DB_URL_LOAD", "DB_URL_CODEX_LOCAL",
        )
        expected_by_operation = {
            "readonly-precheck": ["DB_URL_CODEX_RO"],
            "readonly-postcheck": ["DB_URL_CODEX_RO"],
            "verify-route-b-role": ["DB_URL_KPIONE_ROUTE_B_PRODUCTIVE"],
            "route-b-apply": ["DB_URL_KPIONE_ROUTE_B_PRODUCTIVE"],
            "route-b-rollback": ["DB_URL_KPIONE_ROUTE_B_PRODUCTIVE"],
            "admin-provision": ["DB_URL_ADMIN", "KPIONE_ROUTE_B_PRODUCTIVE_PASSWORD"],
            "admin-reconcile-provisioning-evidence": ["DB_URL_ADMIN"],
            "admin-reconcile-existing-provisioned-state": ["DB_URL_ADMIN"],
            "admin-reconcile-route-b-readonly-observer": ["DB_URL_ADMIN"],
            "diagnose-readonly": ["DB_URL_CODEX_RO"],
            "diagnose-route-b": ["DB_URL_KPIONE_ROUTE_B_PRODUCTIVE"],
            "diagnose-admin": ["DB_URL_ADMIN", "KPIONE_ROUTE_B_PRODUCTIVE_PASSWORD"],
        }
        secret_values = (
            "synthetic-vault-readonly", "synthetic-vault-route-b",
            "synthetic-vault-admin", "synthetic-vault-role-password",
        )
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            scripts = root / "scripts"
            scripts.mkdir()
            (scripts / SECRET_WRAPPER.name).write_bytes(SECRET_WRAPPER.read_bytes())
            probe = (
                "import json, os\n"
                f"managed = {managed!r}\n"
                "print(json.dumps({'entrypoint': os.path.basename(__file__), "
                "'managed': sorted(name for name in managed if name in os.environ)}))\n"
            )
            for name in (
                "precheck_kpione_route_b_018_read_only.py",
                "verify_kpione_route_b_productive_role.py",
                "run_kpione_route_b_ingestion_v1.py",
                "provision_kpione_route_b_role.py",
                "reconcile_route_b_readonly_observer.py",
                "diagnose_stock_zero_db_credentials.py",
            ):
                (scripts / name).write_text(probe, encoding="utf-8")

            modules = root / "modules"
            secret_management = modules / "Microsoft.PowerShell.SecretManagement"
            secret_store = modules / "Microsoft.PowerShell.SecretStore"
            secret_management.mkdir(parents=True)
            secret_store.mkdir(parents=True)
            secret_management.joinpath("Microsoft.PowerShell.SecretManagement.psm1").write_text(
                "function Get-SecretVault { [pscustomobject]@{Name='STOCK_ZERO'} }\n"
                "function Get-Secret { param([string]$Vault,[string]$Name) switch($Name) {\n"
                "'STOCK_ZERO_DB_CODEX_RO' {'synthetic-vault-readonly'}\n"
                "'STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE' {'synthetic-vault-route-b'}\n"
                "'STOCK_ZERO_DB_ADMIN' {'synthetic-vault-admin'}\n"
                "'STOCK_ZERO_DB_KPIONE_ROUTE_B_PASSWORD' {'synthetic-vault-role-password'}\n"
                "default { throw 'unknown synthetic secret' } } }\n"
                "Export-ModuleMember -Function Get-SecretVault,Get-Secret\n",
                encoding="utf-8",
            )
            secret_store.joinpath("Microsoft.PowerShell.SecretStore.psm1").write_text(
                "# Synthetic empty module for the wrapper contract probe.\n",
                encoding="utf-8",
            )
            environment = os.environ.copy()
            environment["PSModulePath"] = str(modules) + os.pathsep + environment.get("PSModulePath", "")
            for name in managed:
                environment[name] = f"synthetic-parent-{name.lower()}"

            run_id = str(uuid.uuid4())
            maintenance_run_id = str(uuid.uuid4())
            evidence_base = f"evidence/runtime/020B/{run_id}"
            maintenance_base = f"evidence/runtime/022/{maintenance_run_id}"
            operation_arguments = {
                "readonly-precheck": ["--run-id", run_id, "--report-json", f"{evidence_base}/01_readonly_baseline.json"],
                "admin-provision": ["--run-id", run_id, "--evidence-json", f"{evidence_base}/02_admin_provisioning.json"],
                "admin-reconcile-provisioning-evidence": ["--run-id", run_id, "--evidence-json", f"{evidence_base}/02_admin_provisioning.json"],
                "admin-reconcile-existing-provisioned-state": ["--run-id", run_id, "--evidence-json", f"{evidence_base}/02_admin_provisioning.json"],
                "admin-reconcile-route-b-readonly-observer": [
                    "--maintenance-run-id", maintenance_run_id,
                    "--evidence-json", f"{maintenance_base}/01_route_b_readonly_observer_grants.json",
                ],
                "verify-route-b-role": ["--run-id", run_id, "--evidence-json", f"{evidence_base}/03_productive_role_verification.json"],
                "readonly-postcheck": ["--run-id", run_id, "--report-json", f"{evidence_base}/04_readonly_postcheck.json"],
            }

            for operation, expected_names in expected_by_operation.items():
                with self.subTest(operation=operation):
                    arguments = operation_arguments.get(operation, [])
                    rendered_arguments = ",".join(
                        "'" + value.replace("'", "''") + "'" for value in arguments
                    )
                    command = (
                        f"& '{scripts / SECRET_WRAPPER.name}' -Operation '{operation}'"
                        + (f" -ArgumentList @({rendered_arguments})" if arguments else "")
                    )
                    completed = subprocess.run(
                        [
                            pwsh, "-NoLogo", "-NoProfile", "-Command", command,
                        ],
                        cwd=root, env=environment, capture_output=True, text=True, check=False,
                        timeout=30,
                    )
                    self.assertEqual(completed.returncode, 0, completed.stderr)
                    reports = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
                    self.assertEqual(reports[-1]["managed"], sorted(expected_names))
                    if operation in (
                        "admin-provision",
                        "admin-reconcile-provisioning-evidence",
                        "admin-reconcile-existing-provisioned-state",
                        "admin-reconcile-route-b-readonly-observer",
                    ):
                        self.assertEqual(reports[0]["managed"], [])
                        expected_entrypoint = (
                            "reconcile_route_b_readonly_observer.py"
                            if operation == "admin-reconcile-route-b-readonly-observer"
                            else "provision_kpione_route_b_role.py"
                        )
                        self.assertEqual(reports[-1]["entrypoint"], expected_entrypoint)
                    for secret in secret_values:
                        self.assertNotIn(secret, completed.stdout)
                        self.assertNotIn(secret, completed.stderr)

            arbitrary = subprocess.run(
                [
                    pwsh, "-NoLogo", "-NoProfile", "-File",
                    str(scripts / SECRET_WRAPPER.name), "-Operation", "arbitrary-entrypoint",
                ],
                cwd=root, env=environment, capture_output=True, text=True, check=False,
                timeout=30,
            )
            self.assertNotEqual(arbitrary.returncode, 0)

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
