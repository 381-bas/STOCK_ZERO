from __future__ import annotations

import copy
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

from scripts import ci_repository_checks
from scripts.run_test_group import load_registry, validate_local_requirements, validate_registry


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "repository-quality.yml"
STATE = ROOT / "governance" / "kernel" / "current" / "02_project_state_stock_zero_v2026_06_30_011.json"
PROTECTED = {
    "01_project_kernel_stock_zero_v2026_06_16.json": "324741809e920b12ed8a34b35996014cae95af5968512c6042c362dbbfca5b92",
    "02_project_state_stock_zero_v2026_06_30_011.json": "29b522bd15f8e1d86b51059edff80401bb8d01c8fcf1b6aee66631236702b903",
    "03_project_ledger_stock_zero_v2026_06_30_011.json": "c54b6d63d871ebcbb9d3a8c3131cabeb4cf15da5c063907ccee19669b1fd6bbc",
}


def lf_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


class CIQualityGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow_text = WORKFLOW.read_text(encoding="utf-8")
        cls.workflow = yaml.safe_load(cls.workflow_text)
        cls.registry = load_registry()

    def test_workflow_parses_and_has_exact_triggers(self) -> None:
        triggers = self.workflow[True]
        self.assertEqual(set(triggers), {"pull_request", "push", "workflow_dispatch"})
        self.assertEqual(triggers["push"]["branches"], ["main"])

    def test_workflow_permissions_runtime_and_jobs(self) -> None:
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        self.assertEqual(
            set(self.workflow["jobs"]),
            {"governance-fast", "safety-static", "unit-core", "postgresql-integration"},
        )
        setup_steps = [
            step
            for job in self.workflow["jobs"].values()
            for step in job["steps"]
            if step.get("uses", "").startswith("actions/setup-python@")
        ]
        self.assertTrue(setup_steps)
        self.assertTrue(all(step.get("with", {}).get("python-version") == "3.12" for step in setup_steps))
        self.assertTrue(all(job["runs-on"] == "ubuntu-latest" for job in self.workflow["jobs"].values()))
        action_refs = re.findall(r"uses:\s+[^@\s]+@([^\s]+)", self.workflow_text)
        self.assertTrue(action_refs)
        self.assertTrue(all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in action_refs))

    def test_workflow_excludes_productive_authority(self) -> None:
        for token in ci_repository_checks.FORBIDDEN_WORKFLOW_TOKENS:
            self.assertNotIn(token, self.workflow_text)
        self.assertNotIn("secrets.", self.workflow_text)
        postgres_env = self.workflow["jobs"]["postgresql-integration"]["env"]
        self.assertEqual(set(postgres_env), {"DB_URL_CODEX_LOCAL"})
        self.assertIn("@127.0.0.1:", postgres_env["DB_URL_CODEX_LOCAL"])

    def test_diff_gate_uses_actual_event_ranges(self) -> None:
        governance_steps = self.workflow["jobs"]["governance-fast"]["steps"]
        runs = "\n".join(step.get("run", "") for step in governance_steps)
        self.assertIn("github.event.pull_request.base.sha", runs)
        self.assertIn("github.event.pull_request.head.sha", runs)
        self.assertIn("github.event.before", runs)
        self.assertNotIn('--head "${{ github.sha }}" --mode pull_request', runs)

    def test_registry_is_complete_and_unique(self) -> None:
        groups = validate_registry(self.registry)
        self.assertEqual(sum(map(len, groups.values())), len(list((ROOT / "tests").glob("test_*.py"))))
        self.assertIn("tests/test_ci_quality_gates.py", groups["CI_CORE"])
        self.assertEqual({category: len(modules) for category, modules in groups.items()}, {
            "CI_CORE": 24,
            "CI_POSTGRESQL": 2,
            "LOCAL_SOURCE_INTEGRATION": 2,
            "LOCAL_ENVIRONMENT": 2,
            "PRODUCTIVE_NEVER_CI": 0,
        })

    def test_unknown_and_duplicate_modules_fail_classification(self) -> None:
        unknown = copy.deepcopy(self.registry)
        unknown["modules"]["CI_CORE"].append("tests/test_not_tracked.py")
        with self.assertRaisesRegex(ValueError, "unknown"):
            validate_registry(unknown)
        duplicate = copy.deepcopy(self.registry)
        duplicate["modules"]["LOCAL_ENVIRONMENT"].append(duplicate["modules"]["CI_CORE"][0])
        duplicate["reasons"][duplicate["modules"]["CI_CORE"][0]] = "duplicate fixture"
        with self.assertRaisesRegex(ValueError, "duplicates"):
            validate_registry(duplicate)

    def test_unit_core_uses_registry_and_excludes_local_groups(self) -> None:
        unit_runs = [step["run"] for step in self.workflow["jobs"]["unit-core"]["steps"] if "run" in step]
        self.assertIn("python scripts/run_test_group.py --group CI_CORE", unit_runs)
        self.assertFalse(any("LOCAL_" in command for command in unit_runs))
        self.assertFalse(set(self.registry["modules"]["CI_CORE"]) & set(self.registry["modules"]["LOCAL_SOURCE_INTEGRATION"]))
        worktree_test = "tests/test_sz_worktree_tooling.py"
        self.assertNotIn(worktree_test, self.registry["modules"]["CI_CORE"])
        self.assertIn(worktree_test, self.registry["modules"]["LOCAL_ENVIRONMENT"])
        self.assertTrue(all(self.registry["reasons"].get(module, "").strip() for module in self.registry["modules"]["LOCAL_ENVIRONMENT"]))

    def test_safety_command_does_not_require_json5(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "json5.py").write_text("raise RuntimeError('json5 import attempted')\n", encoding="utf-8")
            environment = dict(__import__("os").environ, PYTHONPATH=tmp)
            completed = subprocess.run(
                [sys.executable, "scripts/ci_repository_checks.py", "safety"],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("safety_static=pass", completed.stdout)

    def test_real_productive_credential_assignments_are_detected_without_values(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            root = Path(tmp)
            fixture = root / "fixture.txt"
            secret_fragment = "eyJ" + "not-a-real-token"
            database_value = "postgresql://user:password@host/database"
            fixture.write_text(
                f"SUPABASE_SERVICE_ROLE_KEY={secret_fragment}\nDB_URL_CODEX_LOAD={database_value}\n",
                encoding="utf-8",
            )
            findings = ci_repository_checks.find_productive_credential_assignments([fixture], root)
        self.assertEqual([finding["variable"] for finding in findings], ["SUPABASE_SERVICE_ROLE_KEY", "DB_URL_CODEX_LOAD"])
        rendered = json.dumps(findings)
        self.assertNotIn(secret_fragment, rendered)
        self.assertNotIn(database_value, rendered)

    def test_placeholders_and_documentation_references_are_not_credentials(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            root = Path(tmp)
            fixture = root / "fixture.txt"
            fixture.write_text(
                "SUPABASE_SERVICE_ROLE_KEY=${SUPABASE_SERVICE_ROLE_KEY}\n"
                "DB_URL_CODEX_LOAD=<PLACEHOLDER>\n"
                "The variable DB_URL_CODEX_LOAD must not be used in CI.\n",
                encoding="utf-8",
            )
            findings = ci_repository_checks.find_productive_credential_assignments([fixture], root)
        self.assertEqual(findings, [])

    def test_local_source_runner_fails_when_sources_are_absent(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "local test prerequisites failed"):
            validate_local_requirements(self.registry, "LOCAL_SOURCE_INTEGRATION", ROOT / "missing-fixture-root")

    def test_kernel04_retirement_and_protected_governance(self) -> None:
        kernel_dir = STATE.parent
        self.assertFalse(any(kernel_dir.glob("04_project_technical_evidence*.json")))
        self.assertEqual({name: lf_digest(kernel_dir / name) for name in PROTECTED}, PROTECTED)
        state = json.loads(STATE.read_text(encoding="utf-8"))
        self.assertFalse(state["authorization"]["018_authorized"])
        self.assertFalse(state["authorization"]["apply_authorized"])
        self.assertFalse(state["authorization"]["db_writes_authorized"])


if __name__ == "__main__":
    unittest.main()
