import ast
import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import validate_kpione_raw_exports_014C_no_apply as validator


class KpioneRawExport014CTests(unittest.TestCase):
    def test_header_and_source_file_id_normalization(self):
        self.assertEqual(validator.normalize_header("Foto Nº/Total"), "foto_n_total")
        self.assertEqual(validator.normalize_header("Código Local"), "codigo_local")
        self.assertEqual(
            validator.parse_source_file_id(
                "photo-excel-admin_1781973512473.xlsx"
            ),
            "1781973512473",
        )

    def test_visit_fraction_uses_cod_rt_and_local_fallback(self):
        rows = pd.DataFrame(
            {
                "event_id": ["A", "B", "C", "D", "E"],
                "cod_rt": ["100", "100", "100", "", None],
                "local_nombre": ["L1", "L2", "L3", "LOCAL X", "LOCAL X"],
                "cliente_norm": ["MARCA", "MARCA", "MARCA", "MARCA", "MARCA"],
                "fecha": pd.to_datetime(["2026-06-01"] * 5),
            }
        )

        enriched, groups = validator.compute_visit_fraction(rows)

        self.assertEqual(enriched["visit_fraction"].tolist(), [1 / 3, 1 / 3, 1 / 3, 1 / 2, 1 / 2])
        self.assertEqual(len(groups), 2)
        self.assertTrue(groups["visit_sum"].sub(1.0).abs().le(1e-12).all())
        self.assertEqual(
            groups.set_index("location_key_type")["row_count"].to_dict(),
            {"COD_RT": 3, "LOCAL": 2},
        )

    def test_overlap_matrix_does_not_assume_zero(self):
        result = validator.compute_overlap_matrix(
            {
                "A": {"1", "2"},
                "B": {"2", "3"},
                "C": {"4"},
            }
        )

        pair = next(
            item
            for item in result["pairs"]
            if item["source_file_id_a"] == "A"
            and item["source_file_id_b"] == "B"
        )
        self.assertEqual(pair["overlap_id_count"], 1)
        self.assertEqual(pair["sample_ids"], ["2"])

    def test_june_coverage_excludes_rollover_week_from_operational_weeks(self):
        dates = pd.date_range("2026-06-01", "2026-06-28", freq="D")
        canonical = pd.DataFrame({"fecha": dates})

        coverage = validator.compute_june_coverage(canonical, "2026-06")

        self.assertTrue(coverage["operational_coverage_complete"])
        self.assertFalse(coverage["calendar_month_complete"])
        self.assertEqual(len(coverage["operational_weeks"]), 4)
        self.assertEqual(
            coverage["rollover_week"]["days_inside_requested_month"],
            ["2026-06-29", "2026-06-30"],
        )

    def test_legacy_parity_uses_legacy_population_as_rate_denominator(self):
        canonical = pd.DataFrame(
            {
                "event_id": ["1", "2", "3"],
                "fecha": pd.to_datetime(["2026-06-01"] * 3),
            }
        )
        legacy = pd.DataFrame(
            {
                "ID": ["1", "2", "4"],
                "Fecha": pd.to_datetime(["2026-06-01"] * 3),
            }
        )

        parity = validator.compute_legacy_parity(canonical, legacy)

        self.assertEqual(parity["matched_id_count"], 2)
        self.assertEqual(parity["raw_only_count"], 1)
        self.assertEqual(parity["legacy_only_count"], 1)
        self.assertAlmostEqual(parity["match_rate"], 2 / 3)

    def test_verdict_precedence(self):
        common = {
            "operational_coverage_complete": True,
            "parity_match_rate": 1.0,
        }
        self.assertEqual(
            validator.determine_verdict(
                schema_blocked=True,
                truncation_suspect=True,
                conflict_blocked=True,
                **common,
            ),
            validator.VERDICT_BLOCKED_SCHEMA,
        )
        self.assertEqual(
            validator.determine_verdict(
                schema_blocked=False,
                truncation_suspect=True,
                conflict_blocked=False,
                **common,
            ),
            validator.VERDICT_BLOCKED_CONFLICT,
        )
        self.assertEqual(
            validator.determine_verdict(
                schema_blocked=False,
                truncation_suspect=False,
                conflict_blocked=False,
                operational_coverage_complete=False,
                parity_match_rate=1.0,
            ),
            validator.VERDICT_PARTIAL,
        )
        self.assertEqual(
            validator.determine_verdict(
                schema_blocked=False,
                truncation_suspect=False,
                conflict_blocked=False,
                **common,
            ),
            validator.VERDICT_READY,
        )

    def test_validator_imports_no_db_clients_or_productive_loaders(self):
        source = (
            ROOT / "scripts" / "validate_kpione_raw_exports_014C_no_apply.py"
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
        self.assertNotIn("refresh_control_gestion_v2_incremental", source)
        self.assertNotIn("refresh_control_gestion_v2_mv", source)


if __name__ == "__main__":
    unittest.main()
