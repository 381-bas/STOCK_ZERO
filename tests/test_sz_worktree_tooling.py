from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "scripts" / "sz_worktree_audit.ps1"
SETUP = ROOT / "scripts" / "sz_local_env_setup.ps1"
EXPECTED_BRANCH = "codex/PLATFORM_005B-load-observation-correction"


def run_ps(script: Path, *args: str, cwd: Path = ROOT):
    cp = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script), *args],
        cwd=cwd,
        text=True,
        capture_output=True,
    )
    payload = json.loads(cp.stdout)
    return cp, payload


def git_text(*args: str, cwd: Path = ROOT) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()


def is_detached() -> bool:
    return git_text("rev-parse", "--abbrev-ref", "HEAD") == "HEAD"


def current_head() -> str:
    return git_text("rev-parse", "HEAD")


def safe_audit_args() -> tuple[str, ...]:
    return ("-AllowDetached",) if is_detached() else ()


def assert_no_absolute_paths(testcase: unittest.TestCase, payload: dict) -> None:
    text = json.dumps(payload, sort_keys=True)
    testcase.assertNotIn("C:/Users/", text)
    testcase.assertNotIn("C:\\\\Users\\\\", text)
    testcase.assertNotIn(str(ROOT).replace("\\", "/"), text)


class WorktreeToolingTests(unittest.TestCase):
    def test_01_expected_branch_matches_when_on_branch(self):
        if is_detached():
            cp, payload = run_ps(AUDIT, "-AllowDetached", "-ExpectedBranch", "not-used-in-detached")
            self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
            self.assertEqual(payload["branch_mode"], "DETACHED")
        else:
            cp, payload = run_ps(AUDIT, "-ExpectedBranch", EXPECTED_BRANCH)
            self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
            self.assertEqual(payload["branch"], EXPECTED_BRANCH)

    def test_02_expected_branch_mismatch_fails_on_branch(self):
        if is_detached():
            self.skipTest("branch mismatch applies only to branch mode")
        cp, payload = run_ps(AUDIT, "-ExpectedBranch", "codex/not-this-branch")
        self.assertNotEqual(cp.returncode, 0)
        self.assertIn("branch_mismatch", payload["errors"])

    def test_03_expected_branch_is_optional(self):
        cp, payload = run_ps(AUDIT, *safe_audit_args())
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertIsNone(payload["expected_branch"])

    def test_04_detached_blocked_by_default(self):
        if not is_detached():
            self.skipTest("detached behavior covered in detached review worktree")
        cp, payload = run_ps(AUDIT)
        self.assertNotEqual(cp.returncode, 0)
        self.assertIn("detached_not_allowed", payload["errors"])

    def test_05_detached_allowed_explicitly(self):
        if not is_detached():
            self.skipTest("detached behavior covered in detached review worktree")
        cp, payload = run_ps(AUDIT, "-AllowDetached")
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertEqual(payload["branch_mode"], "DETACHED")

    def test_06_expected_baseline_matches(self):
        cp, payload = run_ps(AUDIT, *safe_audit_args(), "-ExpectedBaseline", current_head())
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertEqual(payload["head"], current_head())

    def test_07_expected_baseline_mismatch_fails(self):
        cp, payload = run_ps(AUDIT, *safe_audit_args(), "-ExpectedBaseline", "0" * 40)
        self.assertNotEqual(cp.returncode, 0)
        self.assertIn("baseline_mismatch", payload["errors"])

    def test_08_require_clean_detects_dirty(self):
        marker = ROOT / "tmp_require_clean_marker.tmp"
        try:
            marker.write_text("temporary test marker", encoding="utf-8")
            cp, payload = run_ps(AUDIT, *safe_audit_args(), "-RequireClean")
            self.assertNotEqual(cp.returncode, 0)
            self.assertIn("worktree_not_clean", payload["errors"])
        finally:
            if marker.exists():
                marker.unlink()

    def test_09_paths_redacted_by_default(self):
        cp, payload = run_ps(AUDIT, *safe_audit_args())
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertTrue(payload["path_redacted"])
        self.assertNotIn("repo_root", payload)
        assert_no_absolute_paths(self, payload)

    def test_10_absolute_paths_only_with_switch(self):
        cp, payload = run_ps(AUDIT, *safe_audit_args(), "-IncludeAbsolutePaths")
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertFalse(payload["path_redacted"])
        self.assertIn("repo_root", payload)

    def test_11_audit_read_only(self):
        before = git_text("status", "--short")
        cp, payload = run_ps(AUDIT, *safe_audit_args())
        after = git_text("status", "--short")
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertTrue(payload["ok"])
        self.assertEqual(before, after)
        self.assertTrue(all(item["success"] for item in payload["git_results"]))

    def test_12_git_failure_is_normalized(self):
        with tempfile.TemporaryDirectory() as tmp:
            cp, payload = run_ps(AUDIT, cwd=Path(tmp))
        self.assertNotEqual(cp.returncode, 0)
        self.assertFalse(payload["ok"])
        self.assertIn("not_a_git_repository", payload["errors"])
        self.assertIn("git_results", payload)

    def test_13_setup_default_dry_run(self):
        cp, payload = run_ps(SETUP)
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["environment_source"], "NONE")
        self.assertFalse(payload["environment_reproducible"])
        self.assertFalse((ROOT / ".venv").exists())

    def test_14_import_smoke_without_venv_fails(self):
        if (ROOT / ".venv").exists():
            self.skipTest(".venv exists; cannot verify missing-venv failure")
        cp, payload = run_ps(SETUP, "-RunImportSmoke")
        self.assertNotEqual(cp.returncode, 0)
        self.assertIn("worktree_venv_required", payload["errors"])
        self.assertEqual(payload["environment_source"], "NONE")

    def test_15_system_python_smoke_requires_switch(self):
        if (ROOT / ".venv").exists():
            self.skipTest(".venv exists; system fallback is intentionally bypassed")
        cp, payload = run_ps(SETUP, "-RunImportSmoke", "-AllowSystemPythonSmoke")
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertEqual(payload["environment_source"], "SYSTEM_PYTHON")
        self.assertIn("system_python_not_worktree_reproducible", payload["warnings"])

    def test_16_system_python_is_never_reproducible(self):
        if (ROOT / ".venv").exists():
            self.skipTest(".venv exists; system fallback is intentionally bypassed")
        cp, payload = run_ps(SETUP, "-RunImportSmoke", "-AllowSystemPythonSmoke")
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertFalse(payload["environment_reproducible"])

    def test_17_venv_smoke_marks_worktree_venv(self):
        self._with_temporary_venv(lambda payload: self.assertEqual(payload["environment_source"], "WORKTREE_VENV"))

    def test_18_venv_smoke_is_reproducible(self):
        self._with_temporary_venv(lambda payload: self.assertTrue(payload["environment_reproducible"]))

    def test_19_no_requirements_install(self):
        combined = AUDIT.read_text(encoding="utf-8") + SETUP.read_text(encoding="utf-8")
        lowered = combined.lower()
        self.assertNotIn("pip install", lowered)
        self.assertNotIn("install -r", lowered)
        cp, payload = run_ps(SETUP)
        self.assertFalse(payload["installs_performed"])

    def test_20_no_db(self):
        combined = AUDIT.read_text(encoding="utf-8") + SETUP.read_text(encoding="utf-8")
        lowered = combined.lower()
        self.assertNotIn("db_url", lowered)
        self.assertNotIn("get-childitem env:", lowered)
        self.assertNotIn("psql", lowered)
        self.assertEqual(run_ps(SETUP)[1]["db_access"], "none")

    def test_21_no_docker(self):
        combined = AUDIT.read_text(encoding="utf-8") + SETUP.read_text(encoding="utf-8")
        self.assertNotIn("& docker", combined.lower())
        self.assertNotIn("docker compose", combined.lower())
        self.assertFalse(run_ps(SETUP)[1]["docker_executed"])

    def test_22_no_data_copy(self):
        combined = AUDIT.read_text(encoding="utf-8") + SETUP.read_text(encoding="utf-8")
        lowered = combined.lower()
        for needle in ("copy-item", "robocopy", "xcopy", "copy data"):
            self.assertNotIn(needle, lowered)
        self.assertFalse(run_ps(SETUP)[1]["data_copy"])

    def test_23_no_codex_created(self):
        run_ps(AUDIT, *safe_audit_args())
        run_ps(SETUP)
        self.assertFalse((ROOT / ".codex").exists())

    def test_24_no_kernels_copy(self):
        cp, payload = run_ps(SETUP)
        self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        self.assertFalse(payload["kernels_copied"])
        self.assertNotIn("KERNEL_REFERENCE_ROOT", SETUP.read_text(encoding="utf-8"))

    def test_25_primary_checkout_unchanged(self):
        primary = Path(r"C:\Users\basti\Desktop\STOCK_ZERO")
        if primary.exists():
            before = subprocess.check_output(["git", "status", "--short"], cwd=primary, text=True)
            cp, _payload = run_ps(AUDIT, *safe_audit_args())
            after = subprocess.check_output(["git", "status", "--short"], cwd=primary, text=True)
            self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
            self.assertEqual(before, after)

    def _with_temporary_venv(self, assertion):
        venv = ROOT / ".venv"
        if venv.exists():
            self.skipTest(".venv exists before test")
        try:
            subprocess.check_call(["python", "-m", "venv", str(venv)], cwd=ROOT)
            cp, payload = run_ps(SETUP, "-RunImportSmoke")
            self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr)
            assertion(payload)
        finally:
            if venv.exists():
                shutil.rmtree(venv)


if __name__ == "__main__":
    unittest.main()
