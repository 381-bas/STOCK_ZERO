import json
import sys
import unittest
from calendar import day_name
from datetime import date, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import monthly_input_layout_contract_015 as contract


class ControlGestionOperationalCalendarTests(unittest.TestCase):
    def months_2026_2027(self):
        return [f"{year:04d}-{month:02d}" for year in (2026, 2027) for month in range(1, 13)]

    def test_contract_json_promotes_existing_rule(self):
        path = ROOT / "contracts" / "control_gestion" / "operational_calendar_contract_v1.json"
        payload = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(payload["rules"]["week_start_day"], "monday")
        self.assertEqual(payload["rules"]["week_end_day"], "sunday")
        self.assertEqual(
            payload["rules"]["week_assignment_rule"],
            "month_containing_at_least_4_days",
        )
        self.assertTrue(payload["rules"]["ruta_rutero_month_governs_operational_weeks"])
        self.assertTrue(payload["not_new_rule"])
        promoted_paths = {item["path"] for item in payload["promoted_from"]}
        self.assertIn("scripts/validate_june_data_foundation_gate_014A_no_apply.py", promoted_paths)
        self.assertIn("contracts/control_gestion/kpione2_photo_export_contract_v1.json", promoted_paths)

    def test_all_2026_2027_months_have_valid_week_shape(self):
        for month_id in self.months_2026_2027():
            with self.subTest(month_id=month_id):
                weeks = contract.operational_weeks_for_month(month_id)
                self.assertIn(len(weeks), {4, 5})
                self.assertEqual(
                    [week["week_label"] for week in weeks],
                    [f"S{index}" for index in range(1, len(weeks) + 1)],
                )
                for week in weeks:
                    start = date.fromisoformat(str(week["week_start"]))
                    end = date.fromisoformat(str(week["week_end"]))
                    self.assertEqual(start.weekday(), 0)
                    self.assertEqual(end.weekday(), 6)
                    self.assertEqual(end - start, timedelta(days=6))
                    self.assertGreaterEqual(week["days_in_assigned_month"], 4)
                    self.assertEqual(
                        contract.assigned_operational_month_from_week_start(start),
                        month_id,
                    )

    def test_no_week_is_assigned_to_two_months(self):
        seen: dict[str, str] = {}
        for month_id in self.months_2026_2027():
            for week in contract.operational_weeks_for_month(month_id):
                start = str(week["week_start"])
                self.assertNotIn(start, seen)
                seen[start] = month_id

    def test_required_case_2026_06(self):
        weeks = contract.route_week_mapping_for_month("2026-06")
        coverage = contract.operational_coverage_for_month("2026-06")

        self.assertEqual(weeks[0]["week_start"], "2026-06-01")
        self.assertEqual(weeks[3]["week_start"], "2026-06-22")
        self.assertEqual(len(weeks), 4)
        self.assertEqual(coverage["operational_coverage_start"], "2026-06-01")
        self.assertEqual(coverage["operational_coverage_end"], "2026-06-28")

    def test_required_case_2026_07(self):
        weeks = contract.route_week_mapping_for_month("2026-07")
        coverage = contract.operational_coverage_for_month("2026-07")

        self.assertEqual(weeks[0]["week_start"], "2026-06-29")
        self.assertEqual(weeks[4]["week_start"], "2026-07-27")
        self.assertEqual(len(weeks), 5)
        self.assertEqual(coverage["operational_coverage_start"], "2026-06-29")
        self.assertEqual(coverage["operational_coverage_end"], "2026-08-02")
        self.assertEqual(
            contract.required_calendar_months_for_operational_month("2026-07"),
            ["2026-06", "2026-07", "2026-08"],
        )

    def test_required_cases_2027_01_and_2027_04(self):
        jan = contract.route_week_mapping_for_month("2027-01")
        apr = contract.route_week_mapping_for_month("2027-04")

        self.assertEqual(jan[0]["week_start"], "2027-01-04")
        self.assertEqual(len(jan), 4)
        self.assertEqual(apr[0]["week_start"], "2027-03-29")
        self.assertEqual(len(apr), 5)

    def test_matrix_doc_is_reproducible_from_functions(self):
        matrix_path = ROOT / "docs" / "governance" / "CONTROL_GESTION_OPERATIONAL_CALENDAR_2026_2027.md"
        text = matrix_path.read_text(encoding="utf-8")

        for month_id in self.months_2026_2027():
            year, month = (int(part) for part in month_id.split("-"))
            first = date(year, month, 1)
            weeks = contract.operational_weeks_for_month(month_id)
            coverage = contract.operational_coverage_for_month(month_id)
            required = ", ".join(contract.required_calendar_months_for_operational_month(month_id))
            week_cells = []
            for index in range(5):
                if index < len(weeks):
                    week_cells.append(f"{weeks[index]['week_start']}..{weeks[index]['week_end']}")
                else:
                    week_cells.append("")
            expected_row = "| " + " | ".join(
                [
                    month_id,
                    day_name[first.weekday()],
                    str(len(weeks)),
                    *week_cells,
                    f"{coverage['operational_coverage_start']}..{coverage['operational_coverage_end']}",
                    required,
                ]
            ) + " |"
            self.assertIn(expected_row, text)

    def test_general_logic_has_no_june_july_route_hardcode(self):
        validator_source = (ROOT / "scripts" / "validate_kpione_monthly_input_015B_no_apply.py").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("JUNE_2026_ROUTE_WEEK_STARTS", validator_source)
        self.assertNotIn('if month_id == "2026-06"', validator_source)
        self.assertNotIn('if month_id == "2026-07"', validator_source)


if __name__ == "__main__":
    unittest.main()
