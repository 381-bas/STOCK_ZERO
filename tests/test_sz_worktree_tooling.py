from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "scripts" / "sz_worktree_audit.ps1"
SETUP = ROOT / "scripts" / "sz_local_env_setup.ps1"


def run_ps(script: Path, *args: str):
    cp = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script), *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    payload = json.loads(cp.stdout)
    return cp, payload


class WorktreeToolingTests(unittest.TestCase):
    def test_01_scripts_exist(self):
        self.assertTrue(AUDIT.is_file())
        self.assertTrue(SETUP.is_file())

    def test_02_powershell_parseable(self):
        for script in (AUDIT, SETUP):
            cp = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    f"$null = [scriptblock]::Create((Get-Content -Raw -LiteralPath '{script}'))",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
            )
            self.assertEqual(cp.returncode, 0, cp.stderr)

    def test_03_audit_default_no_write(self):
        before = subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True)
        cp, payload = run_ps(AUDIT)
        after = subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True)
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertTrue(payload["ok"])
        self.assertNotIn("usage: git", cp.stdout)
        self.assertIsInstance(payload["staged_files"], list)
        self.assertIsInstance(payload["modified_files"], list)
        self.assertEqual(before, after)

    def test_04_setup_default_dry_run(self):
        cp, payload = run_ps(SETUP)
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["dry_run"])
        self.assertFalse((ROOT / ".venv").exists())

    def test_05_no_git_reset(self):
        self.assertNotIn("git reset", AUDIT.read_text(encoding="utf-8").lower())
        self.assertNotIn("git reset", SETUP.read_text(encoding="utf-8").lower())

    def test_06_no_git_clean(self):
        self.assertNotIn("git clean", AUDIT.read_text(encoding="utf-8").lower())
        self.assertNotIn("git clean", SETUP.read_text(encoding="utf-8").lower())

    def test_07_no_stash(self):
        self.assertNotIn("stash", AUDIT.read_text(encoding="utf-8").lower())
        self.assertNotIn("stash", SETUP.read_text(encoding="utf-8").lower())

    def test_08_no_db_commands_or_env_probe(self):
        combined = AUDIT.read_text(encoding="utf-8") + SETUP.read_text(encoding="utf-8")
        lowered = combined.lower()
        self.assertNotIn("db_url", lowered)
        self.assertNotIn("get-childitem env:", lowered)
        self.assertNotIn("psql", lowered)

    def test_09_no_docker_commands(self):
        combined = AUDIT.read_text(encoding="utf-8") + SETUP.read_text(encoding="utf-8")
        self.assertNotIn("& docker", combined.lower())
        self.assertNotIn("docker compose", combined.lower())

    def test_10_no_loader_references(self):
        combined = AUDIT.read_text(encoding="utf-8") + SETUP.read_text(encoding="utf-8")
        lowered = combined.lower()
        self.assertNotIn("load_control_gestion", lowered)
        self.assertNotIn("load_ruta_rutero", lowered)
        self.assertNotIn("load_fact_from_excel", lowered)

    def test_11_no_secret_print(self):
        combined = AUDIT.read_text(encoding="utf-8") + SETUP.read_text(encoding="utf-8")
        lowered = combined.lower()
        self.assertNotIn("secrets.toml", lowered)
        self.assertNotIn("credential", lowered)
        self.assertNotIn("password", lowered)

    def test_12_no_data_copy(self):
        combined = AUDIT.read_text(encoding="utf-8") + SETUP.read_text(encoding="utf-8")
        lowered = combined.lower()
        for needle in ("copy-item", "robocopy", "xcopy", "copy data"):
            self.assertNotIn(needle, lowered)

    def test_13_audit_json_valid(self):
        cp, payload = run_ps(AUDIT, "-Pretty")
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertIsInstance(payload["status_short"], list)
        self.assertIsInstance(payload["untracked_files"], list)
        self.assertEqual(payload["branch"], payload["expected_branch"])
        self.assertIn("presence", payload)

    def test_14_baseline_mismatch_fails(self):
        cp, payload = run_ps(AUDIT, "-ExpectedBaseline", "0" * 40)
        self.assertNotEqual(cp.returncode, 0)
        self.assertIn("baseline_mismatch", payload["errors"])

    def test_15_require_clean_works(self):
        marker = ROOT / "tmp_require_clean_marker.tmp"
        try:
            marker.write_text("temporary test marker", encoding="utf-8")
            cp, payload = run_ps(AUDIT, "-RequireClean")
            self.assertNotEqual(cp.returncode, 0)
            self.assertIn("worktree_not_clean", payload["errors"])
        finally:
            if marker.exists():
                marker.unlink()

    def test_16_setup_detects_python(self):
        cp, payload = run_ps(SETUP)
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertIn("python_3_12_found", payload)

    def test_17_create_venv_restricted_to_worktree(self):
        cp, payload = run_ps(SETUP, "-DryRun", "-CreateVenv")
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertEqual(payload["venv_path_relative"], ".venv")
        self.assertIn("create_venv_under_worktree", payload["actions"])
        self.assertFalse(payload["venv_created"])
        self.assertFalse((ROOT / ".venv").exists())

    def test_18_no_requirements_install(self):
        combined = AUDIT.read_text(encoding="utf-8") + SETUP.read_text(encoding="utf-8")
        lowered = combined.lower()
        self.assertNotIn("pip install", lowered)
        self.assertNotIn("install -r", lowered)

    def test_19_no_codex_created(self):
        self.assertFalse((ROOT / ".codex").exists())

    def test_20_no_primary_checkout_modification(self):
        primary = Path(r"C:\Users\basti\Desktop\STOCK_ZERO")
        if primary.exists():
            before = subprocess.check_output(["git", "status", "--short"], cwd=primary, text=True)
            cp, _payload = run_ps(AUDIT)
            after = subprocess.check_output(["git", "status", "--short"], cwd=primary, text=True)
            self.assertEqual(cp.returncode, 0)
            self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
