import ast
import copy
import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import load_kpione_raw_exports_014E_dry_run_no_apply as loader


class KpioneRawDryRun014ETests(unittest.TestCase):
    def test_manifest_selection_uses_only_include_candidate(self):
        manifest = {
            "candidate_set": {"source_file_ids": ["A"]},
            "file_roles": {"include_candidate": ["A"]},
            "file_manifest": [
                {
                    "source_file_id": "A",
                    "source_file_name": "a.xlsx",
                    "role": "include_candidate",
                },
                {
                    "source_file_id": "B",
                    "source_file_name": "b.xlsx",
                    "role": "quarantine_truncation",
                },
                {
                    "source_file_id": "C",
                    "source_file_name": "c.xlsx",
                    "role": "compare_only",
                },
            ],
        }

        candidates, excluded, errors = loader.select_manifest_files(manifest)

        self.assertEqual([item["source_file_id"] for item in candidates], ["A"])
        self.assertEqual(
            {item["source_file_id"] for item in excluded},
            {"B", "C"},
        )
        self.assertEqual(errors, [])

    def test_manifest_role_disagreement_blocks_selection_integrity(self):
        manifest = {
            "candidate_set": {"source_file_ids": ["A"]},
            "file_roles": {"include_candidate": ["B"]},
            "file_manifest": [
                {"source_file_id": "A", "role": "include_candidate"},
                {"source_file_id": "B", "role": "include_candidate"},
            ],
        }

        _, _, errors = loader.select_manifest_files(manifest)

        self.assertIn("candidate_set_and_file_roles_disagree", errors)

    def test_batch_id_is_deterministic_and_order_independent(self):
        entries = [
            {"source_file_id": "B", "source_file_sha256": "2"},
            {"source_file_id": "A", "source_file_sha256": "1"},
        ]

        first = loader.build_dry_run_batch_id("2026-06", "MANIFEST", entries)
        second = loader.build_dry_run_batch_id(
            "2026-06",
            "MANIFEST",
            list(reversed(entries)),
        )

        self.assertEqual(first, second)
        self.assertTrue(first.startswith("014E_"))

    def test_photo_total_parser_handles_sequence_and_missing(self):
        parsed = loader.parse_photo_total(
            pd.Series(["1/4", "4/4", "3", "", None])
        )

        self.assertEqual(parsed.iloc[0], 4)
        self.assertEqual(parsed.iloc[1], 4)
        self.assertEqual(parsed.iloc[2], 3)
        self.assertTrue(pd.isna(parsed.iloc[3]))
        self.assertTrue(pd.isna(parsed.iloc[4]))

    def test_dedupe_preserves_different_event_ids_same_daily_group(self):
        before = pd.DataFrame(
            {
                "event_id": ["E1", "E2"],
                "cod_rt": ["100", "100"],
                "local_nombre": ["LOCAL", "LOCAL"],
                "cliente_norm": ["MARCA", "MARCA"],
                "fecha": pd.to_datetime(["2026-06-01", "2026-06-01"]),
            }
        )
        after = copy.deepcopy(before)

        summary = loader.event_preservation_summary(before, after)

        self.assertEqual(summary["multi_event_daily_groups_before"], 1)
        self.assertTrue(summary["different_event_ids_preserved"])
        self.assertFalse(
            summary["same_local_date_brand_different_id_is_duplicate"]
        )

    def test_verdict_precedence_and_warning_lane(self):
        ready = {
            "schema_blocked": False,
            "candidate_conflict": False,
            "coverage_gap": False,
            "warnings_present": False,
        }
        self.assertEqual(
            loader.determine_dry_run_verdict(
                manifest_mismatch=True,
                **ready,
            ),
            loader.VERDICT_MANIFEST,
        )
        self.assertEqual(
            loader.determine_dry_run_verdict(
                manifest_mismatch=False,
                schema_blocked=True,
                candidate_conflict=False,
                coverage_gap=False,
                warnings_present=False,
            ),
            loader.VERDICT_SCHEMA,
        )
        self.assertEqual(
            loader.determine_dry_run_verdict(
                manifest_mismatch=False,
                schema_blocked=False,
                candidate_conflict=True,
                coverage_gap=False,
                warnings_present=False,
            ),
            loader.VERDICT_CONFLICT,
        )
        self.assertEqual(
            loader.determine_dry_run_verdict(
                manifest_mismatch=False,
                schema_blocked=False,
                candidate_conflict=False,
                coverage_gap=True,
                warnings_present=False,
            ),
            loader.VERDICT_COVERAGE,
        )
        self.assertEqual(
            loader.determine_dry_run_verdict(
                manifest_mismatch=False,
                schema_blocked=False,
                candidate_conflict=False,
                coverage_gap=False,
                warnings_present=True,
            ),
            loader.VERDICT_READY_WARN,
        )

    def test_loader_has_no_glob_db_or_productive_loader_dependency(self):
        source = (
            ROOT
            / "scripts"
            / "load_kpione_raw_exports_014E_dry_run_no_apply.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)

        self.assertNotIn(".glob(", source)
        self.assertTrue(
            imported.isdisjoint({"psycopg", "psycopg2", "sqlalchemy", "supabase"})
        )
        self.assertNotIn("DB_URL", source)
        self.assertNotIn("load_control_gestion_raw_v17", source)
        self.assertNotIn("refresh_control_gestion_v2", source)


if __name__ == "__main__":
    unittest.main()
