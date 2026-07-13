from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from scripts import precheck_kpione_route_b_018_read_only as precheck
from scripts.kpione_route_b_v1 import inspect_workbook, semantic_content_hash


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "plans" / "018_kpione_route_b_productive_apply_plan.json"


class ProductiveReadiness018Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan = precheck.load_plan(PLAN_PATH)

    def test_plan_source_ready_but_never_authorizes_apply(self) -> None:
        self.assertEqual(self.plan["status"], "SOURCE_PACKAGE_READY_TARGET_IDENTITY_PENDING")
        self.assertTrue(self.plan["source_package"]["approved_for_apply"])
        self.assertFalse(self.plan["productive_apply_authorized"])
        self.assertFalse(self.plan["activation_gate"]["gate_open"])
        with self.assertRaisesRegex(precheck.PrecheckBlock, "target_identity_not_registered"):
            precheck.validate_plan_readiness(self.plan)

    def test_approved_manifest_ignores_known_truncated_fixture(self) -> None:
        package = self.plan["source_package"]
        truncated = [item for item in package["excluded_files"] if item["classification"] == "NEGATIVE_TEST_FIXTURE_TRUNCATED"]
        self.assertEqual([item["filename"] for item in truncated], ["photo-excel-admin_1781976368641.xlsx"])
        self.assertNotIn(truncated[0]["filename"], {item["filename"] for item in package["approved_files"]})
        summary = precheck.validate_local_artifacts(self.plan, ROOT)
        self.assertEqual(summary["expected_source_rows"], 239159)

    def test_truncated_fixture_cannot_enter_approved_list(self) -> None:
        altered = copy.deepcopy(self.plan)
        altered["source_package"]["approved_files"].append(
            {**altered["source_package"]["excluded_files"][0], "semantic_content_hash": "0" * 64}
        )
        with self.assertRaisesRegex(precheck.PrecheckBlock, "approved_excluded_filename_overlap"):
            precheck.validate_source_manifest(altered, ROOT)

    def test_two_day8_files_are_semantically_equivalent(self) -> None:
        folder = ROOT / self.plan["input_directory"]["repository_relative_value"]
        first = inspect_workbook(folder / "photo-excel-admin_1783219885210.xlsx")
        second = inspect_workbook(folder / "photo-excel-admin_1782012877303.xlsx")
        self.assertEqual(semantic_content_hash(first), semantic_content_hash(second))
        self.assertEqual((len(first.rows), first.event_count, first.day_presence_count),
                         (len(second.rows), second.event_count, second.day_presence_count))

    def test_day8_canonical_selection_is_deterministic(self) -> None:
        package = self.plan["source_package"]
        approved = next(item for item in package["approved_files"] if item["coverage_start"] == "2026-06-08")
        excluded = next(item for item in package["excluded_files"] if item["classification"] == "DUPLICATE_SEMANTIC_EXPORT")
        self.assertEqual(approved["sha256"], min(approved["sha256"], excluded["sha256"]))
        self.assertEqual(approved["semantic_content_hash"], excluded["semantic_content_hash"])

    def test_excluded_duplicate_does_not_affect_aggregate_counts(self) -> None:
        package = self.plan["source_package"]
        self.assertEqual(package["approved_file_count"], len(package["approved_files"]))
        self.assertEqual(package["approved_file_count"], 9)
        self.assertEqual(package["expected_source_rows"], 239159)
        self.assertEqual(package["expected_day_presence_rows"], 34996)

    def test_unknown_matching_file_blocks(self) -> None:
        altered = copy.deepcopy(self.plan)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            altered["input_directory"]["repository_relative_value"] = "."
            (root / "photo-excel-admin_unknown.xlsx").write_bytes(b"unknown")
            with self.assertRaisesRegex(precheck.PrecheckBlock, "unknown_matching_files"):
                precheck.validate_source_manifest(altered, root)

    def test_missing_approved_file_blocks(self) -> None:
        altered = copy.deepcopy(self.plan)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            altered["input_directory"]["repository_relative_value"] = "."
            with self.assertRaisesRegex(precheck.PrecheckBlock, "missing_approved_files"):
                precheck.validate_source_manifest(altered, root)

    def test_altered_approved_hash_blocks(self) -> None:
        altered = copy.deepcopy(self.plan)
        approved = altered["source_package"]["approved_files"][0]
        altered["source_package"]["approved_files"] = [approved]
        altered["source_package"]["excluded_files"] = []
        altered["source_package"]["observed_directory_inventory"] = [
            {"filename": approved["filename"], "sha256": approved["sha256"], "manifest_role": "APPROVED"}
        ]
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            altered["input_directory"]["repository_relative_value"] = "."
            (root / approved["filename"]).write_bytes(b"altered")
            with self.assertRaisesRegex(precheck.PrecheckBlock, "observed_file_hash_mismatch"):
                precheck.validate_source_manifest(altered, root)

    def test_folder_path_and_filename_do_not_affect_semantic_hash(self) -> None:
        source = ROOT / self.plan["input_directory"]["repository_relative_value"] / "photo-excel-admin_1783219885210.xlsx"
        with tempfile.TemporaryDirectory() as folder:
            renamed = Path(folder) / "photo-excel-admin_renamed.xlsx"
            renamed.write_bytes(source.read_bytes())
            self.assertEqual(semantic_content_hash(inspect_workbook(source)),
                             semantic_content_hash(inspect_workbook(renamed)))

    def test_approved_manifest_covers_every_june_date(self) -> None:
        coverage = self.plan["source_package"]["expected_coverage"]
        self.assertEqual((coverage["start"], coverage["end"], coverage["distinct_dates"]),
                         ("2026-06-01", "2026-06-30", 30))
        self.assertEqual(coverage["missing_dates"], [])

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
