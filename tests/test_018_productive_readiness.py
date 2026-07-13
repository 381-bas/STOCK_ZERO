from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from scripts import precheck_kpione_route_b_018_read_only as precheck


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "plans" / "018_kpione_route_b_productive_apply_plan.json"


class ProductiveReadiness018Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan = precheck.load_plan(PLAN_PATH)

    def test_plan_is_explicitly_blocked_and_never_authorizes_apply(self) -> None:
        self.assertEqual(self.plan["status"], "BLOCKED_SOURCE_PACKAGE")
        self.assertFalse(self.plan["productive_apply_authorized"])
        self.assertFalse(self.plan["activation_gate"]["gate_open"])
        with self.assertRaisesRegex(precheck.PrecheckBlock, "SOURCE_EXPORT_TRUNCATED"):
            precheck.validate_plan_readiness(self.plan)

    def test_candidate_inventory_and_committed_hashes_match_local_files(self) -> None:
        self.assertEqual(self.plan["source_package"]["candidate_count"], 11)
        self.assertEqual(len(self.plan["source_package"]["files"]), 11)
        truncated = [item for item in self.plan["source_package"]["files"] if item["classification"] == "INVALID_OR_QUARANTINED"]
        self.assertEqual([item["filename"] for item in truncated], ["photo-excel-admin_1781976368641.xlsx"])
        precheck.validate_local_artifacts(self.plan, ROOT)

    def test_target_guard_requires_registered_exact_supabase_identity(self) -> None:
        with self.assertRaises(precheck.PrecheckBlock):
            precheck.validate_target("postgresql://u:p@localhost/db", "DB_URL_CODEX_RO", self.plan, "unknown")
        ready = copy.deepcopy(self.plan)
        ready["target"]["expected_supabase_project_ref"] = "project-ref"
        ready["target"]["expected_hostname"] = "db.project-ref.supabase.co"
        self.assertEqual(
            precheck.validate_target("postgresql://u:p@db.project-ref.supabase.co/postgres", "DB_URL_CODEX_RO", ready, "project-ref"),
            "db.project-ref.supabase.co",
        )

    def test_precheck_source_contains_no_mutating_sql(self) -> None:
        source = (ROOT / "scripts" / "precheck_kpione_route_b_018_read_only.py").read_text(encoding="utf-8").upper()
        self.assertIn("BEGIN READ ONLY", source)
        for token in ("INSERT INTO", "UPDATE ", "DELETE FROM", "DROP ", "ALTER TABLE", "CREATE TABLE", "TRUNCATE "):
            self.assertNotIn(token, source)

    def test_plan_has_exact_future_controls_and_postchecks(self) -> None:
        command = self.plan["future_productive_command"]["command"]
        for token in ("--apply-productive", "--approved-plan", "--expected-plan-git-ref", "--expected-project-ref", "--confirm-productive", "--postcheck-report-json"):
            self.assertIn(token, command)
        self.assertGreaterEqual(len(self.plan["postcheck_queries"]), 10)
        self.assertEqual(self.plan["target"]["future_productive_env"], "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE")


if __name__ == "__main__":
    unittest.main()
