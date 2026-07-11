import json
import re
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import monthly_input_layout_contract_015 as contract


class MonthlyInputLayoutContract015Tests(unittest.TestCase):
    def test_2026_06_22_belongs_to_2026_06(self):
        assignment = contract.week_assignment("2026-06-22", folder_month_id="2026-06")

        self.assertEqual(assignment["week_start"], "2026-06-22")
        self.assertEqual(assignment["week_end"], "2026-06-28")
        self.assertEqual(assignment["assigned_operational_month"], "2026-06")
        self.assertTrue(assignment["folder_month_matches_assigned_operational_month"])

    def test_2026_06_29_belongs_to_2026_07(self):
        assignment = contract.week_assignment("2026-06-29", folder_month_id="2026-06")

        self.assertEqual(assignment["week_start"], "2026-06-29")
        self.assertEqual(assignment["week_end"], "2026-07-05")
        self.assertEqual(assignment["assigned_operational_month"], "2026-07")
        self.assertFalse(assignment["folder_month_matches_assigned_operational_month"])

    def test_folder_month_does_not_govern_week_ownership(self):
        assignment = contract.week_assignment("2026-06-29", folder_month_id="2026-06")

        self.assertEqual(assignment["folder_month_id"], "2026-06")
        self.assertFalse(assignment["folder_month_governs_week_ownership"])
        self.assertEqual(
            contract.assigned_operational_month_from_fecha("2026-06-29"),
            "2026-07",
        )

    def test_manifest_promotes_014a_rule_without_rewriting_history(self):
        manifest_path = (
            ROOT
            / "research"
            / "015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY"
            / "015_monthly_input_layout_manifest_2026_06.json"
        )
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["phase_id"], contract.PHASE_ID)
        self.assertEqual(payload["rule_authority"]["promoted_from"], "014A_JUNE_DATA_FOUNDATION_GATE_NO_APPLY")
        self.assertEqual(
            payload["operational_week_month_rule"]["examples"][-1]["assigned_operational_month"],
            "2026-07",
        )

    def test_manifest_photo_reports_have_versioned_hash_identity(self):
        manifest_path = (
            ROOT
            / "research"
            / "015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY"
            / "015_monthly_input_layout_manifest_2026_06.json"
        )
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        files = payload["photo_report_files"]["files"]

        self.assertEqual(len(files), 11)
        self.assertEqual(len({item["source_file_id"] for item in files}), 11)
        for item in files:
            with self.subTest(source_file_id=item["source_file_id"]):
                self.assertRegex(item["sha256"], r"^[0-9a-f]{64}$")
                self.assertGreater(item["size_bytes"], 0)
                self.assertGreater(item["row_count"], 0)
                self.assertTrue(item["relative_path"].startswith("data/"))
                self.assertFalse(re.match(r"^[A-Za-z]:[\\/]", item["relative_path"]))
                self.assertIn(item["role"], {"include_candidate", "quarantine_truncation", "compare_only"})

    def test_manifest_route_week_starts_are_explicit(self):
        manifest_path = (
            ROOT
            / "research"
            / "015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY"
            / "015_monthly_input_layout_manifest_2026_06.json"
        )
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        route_by_label = {
            item["week_label"]: item
            for item in payload["ruta_rutero_reference"]["files"]
        }
        self.assertEqual(
            {label: item["week_start"] for label, item in route_by_label.items()},
            {
                "S1": "2026-06-01",
                "S2": "2026-06-08",
                "S3": "2026-06-15",
                "S4": "2026-06-22",
            },
        )
        for item in route_by_label.values():
            self.assertRegex(item["sha256"], r"^[0-9a-f]{64}$")
            self.assertGreater(item["size_bytes"], 0)
            self.assertTrue(item["relative_path"].startswith("data/"))
            self.assertEqual(item["assigned_operational_month"], "2026-06")

    def test_manifest_declares_transition_week_without_june_s5(self):
        manifest_path = (
            ROOT
            / "research"
            / "015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY"
            / "015_monthly_input_layout_manifest_2026_06.json"
        )
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        transition = payload["ruta_rutero_reference"]["transition_week"]

        self.assertEqual(transition["week_start"], "2026-06-29")
        self.assertEqual(transition["week_end"], "2026-07-05")
        self.assertEqual(transition["assigned_operational_month"], "2026-07")
        self.assertEqual(transition["days_in_june"], 2)
        self.assertEqual(transition["days_in_july"], 5)
        self.assertIsNone(transition["ruta_file_in_june_layout"])
        self.assertFalse(transition["blocking_for_015a_traceability"])
        self.assertTrue(transition["required_before_future_operational_load"])
        self.assertNotIn(
            "S5",
            {item["week_label"] for item in payload["ruta_rutero_reference"]["files"]},
        )


if __name__ == "__main__":
    unittest.main()
