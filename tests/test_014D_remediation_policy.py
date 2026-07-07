import ast
import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import validate_kpione_raw_exports_014D_remediation_no_apply as validator


def canonical_rows(
    source_file_id: str,
    rows: list[tuple[str, str, str]],
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_id": [row[0] for row in rows],
            "_photo_row_hash": [row[1] for row in rows],
            "event_stable_hash": [row[2] for row in rows],
            "source_file_id": source_file_id,
            "fecha": pd.to_datetime(["2026-06-08"] * len(rows)),
        }
    )


class KpioneRawRemediation014DTests(unittest.TestCase):
    def test_policy_roles_are_explicit(self):
        self.assertEqual(
            validator.policy_role("1783219885210"),
            "include_candidate",
        )
        self.assertEqual(
            validator.policy_role("1781976368641"),
            "quarantine_truncation",
        )
        self.assertEqual(
            validator.policy_role("1782012877303"),
            "compare_only",
        )

    def test_event_comparison_classifies_same_and_different_hashes(self):
        left = canonical_rows(
            "LEFT",
            [("1", "ROW-A", "STABLE-1"), ("2", "ROW-B", "STABLE-2")],
        )
        right = canonical_rows(
            "RIGHT",
            [("1", "ROW-A", "STABLE-1"), ("2", "ROW-C", "STABLE-2")],
        )

        result = validator.compare_event_sets(
            label="fixture",
            left_frames=[left],
            right_frames=[right],
        )

        self.assertEqual(result["same_id_same_hash_count"], 1)
        self.assertEqual(result["same_id_diff_hash_count"], 1)
        self.assertEqual(result["event_stable_hash_conflict_count"], 0)
        self.assertEqual(result["diff_hash_sample"], ["2"])

    def test_target_event_is_resolved_when_candidate_excludes_truncated(self):
        candidate = canonical_rows(
            "CANDIDATE",
            [
                (validator.TARGET_EVENT_ID, "ROW-1", "STABLE"),
                (validator.TARGET_EVENT_ID, "ROW-2", "STABLE"),
            ],
        )
        quarantine = canonical_rows(
            "QUARANTINE",
            [(validator.TARGET_EVENT_ID, "ROW-1", "STABLE")],
        )
        original_include = validator.INCLUDE_SOURCE_IDS
        original_quarantine = validator.QUARANTINE_SOURCE_IDS
        try:
            validator.INCLUDE_SOURCE_IDS = ["CANDIDATE"]
            validator.QUARANTINE_SOURCE_IDS = ["QUARANTINE"]
            result = validator.classify_target_event(
                [],
                {
                    "CANDIDATE": candidate,
                    "QUARANTINE": quarantine,
                },
            )
        finally:
            validator.INCLUDE_SOURCE_IDS = original_include
            validator.QUARANTINE_SOURCE_IDS = original_quarantine

        self.assertEqual(
            result["classification"],
            "resolved_by_excluding_truncated",
        )

    def test_verdict_precedence_and_warning_lane(self):
        ready = {
            "calendar_complete": True,
            "operational_complete": True,
            "visit_formula_closes": True,
            "parity_match_rate": 1.0,
        }
        self.assertEqual(
            validator.determine_remediation_verdict(
                candidate_schema_blocked=True,
                candidate_truncation=True,
                candidate_conflicts=True,
                warnings_present=True,
                **ready,
            ),
            validator.VERDICT_SCHEMA,
        )
        self.assertEqual(
            validator.determine_remediation_verdict(
                candidate_schema_blocked=False,
                candidate_truncation=True,
                candidate_conflicts=False,
                warnings_present=False,
                **ready,
            ),
            validator.VERDICT_TRUNCATION,
        )
        self.assertEqual(
            validator.determine_remediation_verdict(
                candidate_schema_blocked=False,
                candidate_truncation=False,
                candidate_conflicts=True,
                warnings_present=False,
                **ready,
            ),
            validator.VERDICT_CONFLICT,
        )
        self.assertEqual(
            validator.determine_remediation_verdict(
                candidate_schema_blocked=False,
                candidate_truncation=False,
                candidate_conflicts=False,
                warnings_present=True,
                **ready,
            ),
            validator.VERDICT_READY_WARN,
        )

    def test_validator_imports_only_local_no_db_baseline(self):
        source = (
            ROOT
            / "scripts"
            / "validate_kpione_raw_exports_014D_remediation_no_apply.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)

        self.assertTrue(
            imported.isdisjoint({"psycopg", "psycopg2", "sqlalchemy", "supabase"})
        )
        self.assertNotIn("DB_URL", source)
        self.assertNotIn("load_control_gestion_raw_v17", source)
        self.assertNotIn("refresh_control_gestion_v2", source)


if __name__ == "__main__":
    unittest.main()
