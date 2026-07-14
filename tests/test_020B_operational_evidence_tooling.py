from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts import precheck_kpione_route_b_018_read_only as precheck
from scripts.build_kpione_route_b_infrastructure_evidence import (
    COMPONENT_CONTRACT,
    EvidenceBundleError,
    build_bundle,
)
from scripts.provision_kpione_route_b_role import (
    ProvisioningError,
    provision_route_b_role,
)


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "plans" / "018_kpione_route_b_productive_apply_plan.json"
WRAPPER = ROOT / "scripts" / "invoke_stock_zero_db_operation.ps1"
VAULT = ROOT / "scripts" / "manage_stock_zero_secret_vault.ps1"


class ExistingRoleCursor:
    def __init__(self) -> None:
        self.rows: list[tuple[object, ...]] = []
        self.commands: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, _params=None) -> None:
        rendered = str(statement)
        normalized = " ".join(rendered.split()).lower()
        self.commands.append(normalized)
        if "select current_user,session_user,current_database" in normalized:
            self.rows = [("postgres", "postgres", "postgres", "off")]
        elif "to_regclass('cg_raw.kpione2_raw')" in normalized:
            self.rows = [("cg_raw.kpione2_raw",)]
        elif "from pg_roles where rolname" in normalized:
            self.rows = [(1,)]
        else:
            self.rows = [(None,)]

    def fetchone(self):
        return self.rows[0] if self.rows else None


class ExistingRoleConnection:
    def __init__(self) -> None:
        self.autocommit = True
        self.cursor_value = ExistingRoleCursor()
        self.rollback_called = False
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def rollback(self) -> None:
        self.rollback_called = True

    def close(self) -> None:
        self.closed = True


class OperationalEvidenceTooling020BTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))

    def test_baseline_accepts_empty_productive_allowlist_and_rejects_open_gate(self) -> None:
        self.assertEqual(self.plan["target"]["allowed_productive_roles"], [])
        precheck.validate_plan_readiness(self.plan, "baseline")
        altered = copy.deepcopy(self.plan)
        altered["activation_gate"]["gate_open"] = True
        with self.assertRaisesRegex(precheck.PrecheckBlock, "productive_gate_must_be_closed"):
            precheck.validate_plan_readiness(altered, "baseline")

    def test_baseline_target_requires_exact_readonly_role_database_and_ssl(self) -> None:
        ref = self.plan["target"]["expected_supabase_project_ref"]
        host = self.plan["target"]["expected_hostname"]
        valid = f"postgresql://stock_zero_codex_ro:synthetic@{host}/postgres?sslmode=require"
        self.assertEqual(precheck.validate_target(valid, "DB_URL_CODEX_RO", self.plan, ref), host)
        for dsn, error in (
            (valid.replace("stock_zero_codex_ro", "wrong_role"), "readonly_dsn_role_mismatch"),
            (valid.replace("/postgres?", "/wrong?"), "target_database_mismatch"),
            (valid.replace("sslmode=require", "sslmode=disable"), "readonly_sslmode_require_required"),
        ):
            with self.subTest(error=error), self.assertRaisesRegex(precheck.PrecheckBlock, error):
                precheck.validate_target(dsn, "DB_URL_CODEX_RO", self.plan, ref)

    def test_postcheck_requires_baseline_before_connection(self) -> None:
        def forbidden_connect(_dsn: str):
            raise AssertionError("postcheck connected before requiring baseline")

        with self.assertRaisesRegex(precheck.PrecheckBlock, "baseline_evidence_required"):
            precheck.run_precheck(
                self.plan,
                "synthetic",
                forbidden_connect,
                check_stage="post-provision",
            )

    def _bundle_fixture(self, root: Path) -> tuple[dict[str, Path], str]:
        git_sha = "a" * 40
        plan_sha = hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest()
        sql_sha = self.plan["physical_contract"]["sql_sha256"]
        fingerprint = "b" * 64
        common = {
            "target_fingerprint": fingerprint,
            "approved_git_sha": git_sha,
            "plan_sha256": plan_sha,
            "sql_sha256": sql_sha,
        }
        baseline = {
            "document_type": COMPONENT_CONTRACT["readonly_baseline_precheck"][0],
            "verdict": COMPONENT_CONTRACT["readonly_baseline_precheck"][1],
            **common,
            "legacy": {"object_identity": "cg_raw.kpione2_raw", "oid": "19", "row_count": 1},
            "public_acl": {"schemas": {"cg_raw": ["USAGE"]}, "relations": {}},
        }
        baseline_path = root / "baseline.json"
        baseline_path.write_text(json.dumps(baseline, sort_keys=True) + "\n", encoding="utf-8")
        components: dict[str, dict[str, object]] = {
            "admin_provisioning": {
                "document_type": COMPONENT_CONTRACT["admin_provisioning"][0],
                "verdict": COMPONENT_CONTRACT["admin_provisioning"][1],
                **common,
            },
            "productive_role_verification": {
                "document_type": COMPONENT_CONTRACT["productive_role_verification"][0],
                "verdict": COMPONENT_CONTRACT["productive_role_verification"][1],
                **common,
            },
            "readonly_postcheck": {
                "document_type": COMPONENT_CONTRACT["readonly_postcheck"][0],
                "verdict": COMPONENT_CONTRACT["readonly_postcheck"][1],
                **common,
                "baseline_evidence_sha256": hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
                "legacy": baseline["legacy"],
                "public_acl": baseline["public_acl"],
            },
        }
        paths = {"readonly_baseline_precheck": baseline_path}
        for name, evidence in components.items():
            path = root / f"{name}.json"
            path.write_text(json.dumps(evidence, sort_keys=True) + "\n", encoding="utf-8")
            paths[name] = path
        return paths, git_sha

    def test_bundle_is_deterministic_and_has_exact_four_components(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            paths, git_sha = self._bundle_fixture(Path(folder))
            first = build_bundle(paths, PLAN_PATH, git_sha)
            second = build_bundle(paths, PLAN_PATH, git_sha)
        self.assertEqual(first, second)
        self.assertEqual(first["document_type"], "kpione_route_b_infrastructure_evidence_bundle_v1")
        self.assertEqual(first["status"], "PASSED")
        self.assertEqual(set(first["components"]), set(COMPONENT_CONTRACT))
        self.assertRegex(first["bundle_sha256"], r"^[0-9a-f]{64}$")

    def test_bundle_rejects_missing_extra_tampered_target_baseline_and_secret(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            paths, git_sha = self._bundle_fixture(root)
            for changed, error in (
                ({key: value for key, value in paths.items() if key != "admin_provisioning"}, "component_name_set_mismatch"),
                ({**paths, "extra": root / "extra.json"}, "component_name_set_mismatch"),
            ):
                with self.subTest(error=error), self.assertRaisesRegex(EvidenceBundleError, error):
                    build_bundle(changed, PLAN_PATH, git_sha)

            post = json.loads(paths["readonly_postcheck"].read_text(encoding="utf-8"))
            post["baseline_evidence_sha256"] = "0" * 64
            paths["readonly_postcheck"].write_text(json.dumps(post) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(EvidenceBundleError, "postcheck_baseline_reference_mismatch"):
                build_bundle(paths, PLAN_PATH, git_sha)

            paths, git_sha = self._bundle_fixture(root)
            admin = json.loads(paths["admin_provisioning"].read_text(encoding="utf-8"))
            admin["target_fingerprint"] = "c" * 64
            paths["admin_provisioning"].write_text(json.dumps(admin) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(EvidenceBundleError, "target_fingerprint_mismatch"):
                build_bundle(paths, PLAN_PATH, git_sha)

            paths, git_sha = self._bundle_fixture(root)
            verify = json.loads(paths["productive_role_verification"].read_text(encoding="utf-8"))
            verify["diagnostic"] = "postgresql://synthetic.invalid/postgres"
            paths["productive_role_verification"].write_text(json.dumps(verify) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(EvidenceBundleError, "suspicious_evidence_value"):
                build_bundle(paths, PLAN_PATH, git_sha)

    def test_existing_role_blocks_before_password_rotation_or_other_write(self) -> None:
        connection = ExistingRoleConnection()
        dsn = (
            "postgresql://postgres:synthetic@"
            f"{self.plan['target']['expected_hostname']}/postgres?sslmode=require"
        )
        guard = {
            "approved_git_sha": "a" * 40,
            "plan_sha256": "b" * 64,
            "ddl_sha256": self.plan["physical_contract"]["sql_sha256"],
        }
        with self.assertRaisesRegex(
            ProvisioningError, "productive_role_exists_password_rotation_not_authorized"
        ) as context:
            provision_route_b_role(
                self.plan,
                dsn,
                "synthetic-password-long-enough",
                Path(tempfile.gettempdir()) / "never-written.json",
                connect_fn=lambda _dsn: connection,
                git_guard=guard,
                ddl="SELECT 1",
            )
        self.assertFalse(context.exception.writes_attempted)
        self.assertTrue(connection.rollback_called)
        self.assertTrue(connection.closed)
        rendered = "\n".join(connection.cursor_value.commands).upper()
        self.assertNotIn("ALTER ROLE", rendered)
        self.assertNotIn("CREATE ROLE", rendered)

    def test_wrapper_and_vault_have_only_typed_operations_and_safe_storage(self) -> None:
        wrapper = WRAPPER.read_text(encoding="utf-8")
        for operation in ("readonly-postcheck", "verify-route-b-role"):
            self.assertIn(f"'{operation}'", wrapper)
        self.assertIn("@('--check-stage', 'post-provision')", wrapper)
        self.assertIn("scripts/verify_kpione_route_b_productive_role.py", wrapper)
        self.assertNotIn("[string]$FilePath", wrapper)

        vault = VAULT.read_text(encoding="utf-8")
        for operation in (
            "bootstrap", "inventory", "set-readonly", "set-admin-temporary",
            "generate-role-password-temporary", "build-and-store-route-b-dsn",
            "remove-temporary",
        ):
            self.assertIn(f"'{operation}'", vault)
        self.assertIn("'1.1.2'", vault)
        self.assertIn("'1.0.6'", vault)
        self.assertIn("-Authentication Password", vault)
        self.assertNotIn("-Authentication None", vault)
        self.assertIn("-AsSecureString", vault)
        self.assertIn("[Security.Cryptography.RandomNumberGenerator]::Fill", vault)
        self.assertNotIn("[EnvironmentVariableTarget]::User", vault)
        self.assertNotIn("[EnvironmentVariableTarget]::Machine", vault)

    @unittest.skipUnless(shutil.which("pwsh"), "PowerShell 7 is required for the vault runtime probe")
    def test_vault_runtime_uses_secure_types_and_evidence_gated_removal(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            log = root / "mock-log.jsonl"
            modules = root / "modules"
            management = modules / "Microsoft.PowerShell.SecretManagement" / "1.1.2"
            store = modules / "Microsoft.PowerShell.SecretStore" / "1.0.6"
            management.mkdir(parents=True)
            store.mkdir(parents=True)
            management.joinpath("Microsoft.PowerShell.SecretManagement.psd1").write_text(
                "@{RootModule='Microsoft.PowerShell.SecretManagement.psm1';ModuleVersion='1.1.2';"
                "GUID='11111111-1111-1111-1111-111111111112';FunctionsToExport='*'}\n",
                encoding="utf-8",
            )
            management.joinpath("Microsoft.PowerShell.SecretManagement.psm1").write_text(
                "function Add-MockEvent { param($Value) Add-Content -LiteralPath $env:SZ_MOCK_LOG "
                "-Value ($Value | ConvertTo-Json -Compress) }\n"
                "function Get-SecretVault { [CmdletBinding()]param([string]$Name) "
                "[pscustomobject]@{Name='STOCK_ZERO'} }\n"
                "function Register-SecretVault { [CmdletBinding()]param([string]$Name,[string]$ModuleName,[switch]$DefaultVault) "
                "Add-MockEvent @{action='register';name=$Name;module=$ModuleName} }\n"
                "function Get-SecretInfo { [CmdletBinding()]param([string]$Vault,[string]$Name) "
                "[pscustomobject]@{Name=$Name;Type='SecureString'} }\n"
                "function Get-Secret { [CmdletBinding()]param([string]$Vault,[string]$Name) "
                "ConvertTo-SecureString 'synthetic-vault-secret' -AsPlainText -Force }\n"
                "function Set-Secret { [CmdletBinding()]param([string]$Vault,[string]$Name,$Secret) "
                "$event=@{action='set';name=$Name;type=$Secret.GetType().FullName}; "
                "if($Name -eq 'STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE'){"
                "$p=[Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secret);try{$v=[Runtime.InteropServices.Marshal]::PtrToStringBSTR($p);"
                "$event.role=$v.StartsWith('postgresql://stock_zero_kpione_route_b_load:');"
                "$event.host=$v.Contains('@db.xheyrgfagpoigpgakilu.supabase.co/');"
                "$event.database=$v.Contains('/postgres?');$event.ssl=$v.EndsWith('sslmode=require')}finally{[Runtime.InteropServices.Marshal]::ZeroFreeBSTR($p)}};"
                "Add-MockEvent $event }\n"
                "function Remove-Secret { [CmdletBinding()]param([string]$Vault,[string]$Name) "
                "Add-MockEvent @{action='remove';name=$Name} }\n"
                "Export-ModuleMember -Function *\n",
                encoding="utf-8",
            )
            store.joinpath("Microsoft.PowerShell.SecretStore.psd1").write_text(
                "@{RootModule='Microsoft.PowerShell.SecretStore.psm1';ModuleVersion='1.0.6';"
                "GUID='11111111-1111-1111-1111-111111111106';FunctionsToExport='*'}\n",
                encoding="utf-8",
            )
            store.joinpath("Microsoft.PowerShell.SecretStore.psm1").write_text(
                "function Set-SecretStoreConfiguration { [CmdletBinding()]param([string]$Authentication,"
                "[string]$Interaction,[int]$PasswordTimeout,[switch]$Confirm) "
                "Add-Content -LiteralPath $env:SZ_MOCK_LOG -Value (@{action='configuration';authentication=$Authentication;interaction=$Interaction}|ConvertTo-Json -Compress) }\n"
                "Export-ModuleMember -Function *\n",
                encoding="utf-8",
            )
            verify_path = root / "verify.json"
            post_path = root / "post.json"
            verify_path.write_text(json.dumps({
                "document_type": "kpione_route_b_productive_role_verification_evidence_v1",
                "verdict": "PASS_PRODUCTIVE_ROLE_VERIFICATION",
            }), encoding="utf-8")
            post_path.write_text(json.dumps({
                "document_type": "kpione_route_b_readonly_postcheck_evidence_v1",
                "verdict": "PASS_READONLY_POSTCHECK",
            }), encoding="utf-8")
            environment = os.environ.copy()
            environment["PSModulePath"] = str(modules) + os.pathsep + environment.get("PSModulePath", "")
            environment["SZ_MOCK_LOG"] = str(log)

            def invoke(operation: str, *, prelude: str = "", arguments: list[str] | None = None):
                args = " ".join(
                    value if value.startswith("-") else f"'{value}'"
                    for value in (arguments or [])
                )
                command = f"{prelude}\n& '{VAULT}' -Operation '{operation}' {args}"
                completed = subprocess.run(
                    ["pwsh", "-NoLogo", "-NoProfile", "-Command", command],
                    cwd=ROOT,
                    env=environment,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
                self.assertNotIn("synthetic-vault-secret", completed.stdout)
                self.assertNotIn("synthetic-vault-secret", completed.stderr)
                return completed

            bootstrap_prelude = (
                "function global:Install-Module { param($Name,$RequiredVersion,$Scope,$Repository,"
                "[switch]$Force,[switch]$AllowClobber) Add-Content -LiteralPath $env:SZ_MOCK_LOG "
                "-Value (@{action='install';name=$Name;version=$RequiredVersion;scope=$Scope}|ConvertTo-Json -Compress) }"
            )
            invoke("bootstrap", prelude=bootstrap_prelude)
            read_prelude = (
                "function global:Read-Host { param($Prompt,[switch]$AsSecureString) "
                "ConvertTo-SecureString 'synthetic-vault-secret' -AsPlainText -Force }"
            )
            invoke("set-readonly", prelude=read_prelude)
            invoke("set-admin-temporary", prelude=read_prelude)
            invoke("generate-role-password-temporary")
            invoke("build-and-store-route-b-dsn")
            inventory = invoke("inventory")
            self.assertIn("STOCK_ZERO_DB_CODEX_RO", inventory.stdout)
            blocked_remove = subprocess.run(
                [
                    "pwsh", "-NoLogo", "-NoProfile", "-File", str(VAULT),
                    "-Operation", "remove-temporary",
                ],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(blocked_remove.returncode, 0)
            self.assertNotIn("synthetic-vault-secret", blocked_remove.stdout + blocked_remove.stderr)
            invoke(
                "remove-temporary",
                arguments=[
                    "-ProductiveRoleVerificationEvidence", str(verify_path),
                    "-ReadonlyPostcheckEvidence", str(post_path),
                ],
            )
            events = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
            installs = {event["name"]: event for event in events if event["action"] == "install"}
            self.assertEqual(installs["Microsoft.PowerShell.SecretManagement"]["version"], "1.1.2")
            self.assertEqual(installs["Microsoft.PowerShell.SecretStore"]["version"], "1.0.6")
            configuration = next(event for event in events if event["action"] == "configuration")
            self.assertEqual((configuration["authentication"], configuration["interaction"]), ("Password", "Prompt"))
            stored = [event for event in events if event["action"] == "set"]
            self.assertTrue(stored)
            self.assertTrue(all(event["type"] == "System.Security.SecureString" for event in stored))
            dsn_event = next(event for event in stored if event["name"] == "STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE")
            self.assertTrue(all(dsn_event[key] for key in ("role", "host", "database", "ssl")))
            removed = {event["name"] for event in events if event["action"] == "remove"}
            self.assertEqual(removed, {
                "STOCK_ZERO_DB_ADMIN", "STOCK_ZERO_DB_KPIONE_ROUTE_B_PASSWORD",
            })


if __name__ == "__main__":
    unittest.main()
