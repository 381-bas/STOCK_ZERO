import argparse
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import load_kpione2_photo_from_excel as loader


def fixture_contract(expected: dict) -> dict:
    return {
        "status": "ACTIVE",
        "grain_contract": dict(loader.GRAIN_CONTRACT),
        "009F_evidence": dict(expected),
    }


def sample_photo_df(comment_conflict: bool = False) -> pd.DataFrame:
    columns = [
        "ID",
        "SP Item ID",
        "Holding",
        "Subcadena",
        "Codigo Local",
        "Marca",
        "Local",
        "Direccion",
        "Reponedor",
        "Fecha",
        "Fecha de subida",
        "Hora",
        "Tipo de Tarea",
        "Foto N/Total",
        "Comentarios",
        "Link Foto",
    ]
    rows = [
        ["E1", "SP1", "H", "S", "100", "Marca A", "Local A", "Dir", "Repo", "2026-06-20", "2026-06-20 09:01", "09:00", "A", "1/3", "OK", "https://x/1"],
        ["E1", "SP1", "H", "S", "100", "Marca A", "Local A", "Dir", "Repo", "2026-06-20", "2026-06-20 09:02", "09:01", "B", "2/3", "OK", "https://x/2"],
        ["E1", "SP1", "H", "S", "100", "Marca A", "Local A", "Dir", "Repo", "2026-06-20", "2026-06-20 09:03", "09:02", "C", "3/3", "OK", "https://x/3"],
        ["E2", "SP2", "H", "S", "100", "Marca A", "Local A", "Dir", "Repo", "2026-06-20", "2026-06-20 10:01", "10:00", "A", "1/2", "OK", "https://x/4"],
        ["E2", "SP2", "H", "S", "100", "Marca A", "Local A", "Dir", "Repo", "2026-06-20", "2026-06-20 10:02", "10:01", "B", "2/2", "OK", "https://x/5"],
        ["E3", "SP3", "H", "S", "200", "Marca B", "Local B", "Dir", "Repo", "2026-06-21", "2026-06-21 11:01", "11:00", "A", "1/1", "OK", "https://x/6"],
    ]
    if comment_conflict:
        rows[1][14] = "DIFFERENT"
    return pd.DataFrame(rows, columns=columns)


class Kpione2PhotoGrainTests(unittest.TestCase):
    def _payload(self, df: pd.DataFrame, expected: dict | None = None) -> dict:
        expected = expected or {
            "photo_rows": 6,
            "distinct_event_ids": 3,
            "fecha_min": "2026-06-20",
            "fecha_max": "2026-06-21",
        }
        return loader.analyze_photo_dataframe(
            df,
            contract=fixture_contract(expected),
            expected=expected,
            source_file="fixture.xlsx",
            source_file_sha256="A" * 64,
            sheet_name="Fotos",
        )

    def test_photo_rows_are_grouped_to_event_rows(self):
        payload = self._payload(sample_photo_df())
        self.assertEqual(payload["metrics"]["photo_rows"], 6)
        self.assertEqual(payload["metrics"]["distinct_event_ids"], 3)
        self.assertEqual(payload["metrics"]["event_rows"], 3)
        self.assertTrue(payload["flags"]["forbidden_assumption_rejected"])
        self.assertEqual(payload["verdict"], "PASS_ROUTE_B_DRY_RUN")

    def test_source_row_number_maps_each_photo_row_to_excel_origin(self):
        payload = self._payload(sample_photo_df())
        metrics = payload["metrics"]
        traceability = payload["photo_row_traceability"]

        self.assertEqual(metrics["source_row_number_min"], 2)
        self.assertEqual(metrics["source_row_number_max"], 7)
        self.assertEqual(metrics["source_row_number_distinct"], 6)
        self.assertEqual(metrics["source_row_number_null_rows"], 0)
        self.assertTrue(payload["flags"]["source_row_number_present"])
        self.assertTrue(payload["flags"]["source_row_number_complete"])
        self.assertTrue(payload["flags"]["source_row_number_unique"])
        self.assertTrue(payload["flags"]["source_row_number_matches_excel_rows"])
        self.assertEqual(traceability["photo_rows_mapped"], 6)
        self.assertEqual(traceability["event_identity"], ["ID", "SP Item ID"])
        self.assertFalse(traceability["event_identity_replaced"])
        self.assertEqual(
            [row["source_row_number"] for row in traceability["sample_rows"]],
            [2, 3, 4, 5, 6, 7],
        )

    def test_source_row_number_is_stable_within_workbook_sheet(self):
        original = sample_photo_df()
        reindexed = original.copy()
        reindexed.index = [90, 70, 50, 30, 10, 0]

        first = self._payload(original)["photo_row_traceability"]
        second = self._payload(reindexed)["photo_row_traceability"]

        self.assertEqual(first["trace_manifest_sha256"], second["trace_manifest_sha256"])
        self.assertEqual(first["sample_rows"], second["sample_rows"])

    def test_day_presence_is_binary_not_event_count(self):
        payload = self._payload(sample_photo_df())
        self.assertEqual(payload["metrics"]["day_presence_rows"], 2)
        self.assertEqual(payload["metrics"]["max_events_per_day_presence"], 2)
        self.assertEqual(payload["day_presence_summary"]["binary_presence_values"], [1])
        self.assertTrue(payload["flags"]["day_presence_is_binary"])

    def test_photo_level_columns_are_excluded_from_event_hash(self):
        payload = self._payload(sample_photo_df())
        self.assertEqual(payload["metrics"]["real_content_conflict_event_ids"], 0)
        self.assertIn("Hora", payload["column_contract"]["photo_level_columns_excluded_from_hash"])
        self.assertIn("Tipo de Tarea", payload["column_contract"]["photo_level_columns_excluded_from_hash"])
        self.assertIn("Link Foto", payload["column_contract"]["photo_level_columns_excluded_from_hash"])

    def test_event_stable_column_conflict_is_flagged(self):
        payload = self._payload(sample_photo_df(comment_conflict=True))
        self.assertEqual(payload["metrics"]["real_content_conflict_event_ids"], 1)
        self.assertFalse(payload["flags"]["no_real_content_conflict_event_ids"])
        self.assertEqual(payload["verdict"], "WARN_REVIEW_REQUIRED")

    def test_apply_flag_is_blocked(self):
        args = argparse.Namespace(apply=True)
        with self.assertRaises(loader.LoaderUsageError) as ctx:
            loader.validate_cli_args(args)
        self.assertEqual(ctx.exception.code, "apply_not_supported_in_route_b_dry_run")

    def test_cli_writes_json_without_db(self):
        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            excel = tmp / "photo.xlsx"
            contract = tmp / "contract.json"
            out = tmp / "out.json"
            expected = {
                "photo_rows": 6,
                "distinct_event_ids": 3,
                "fecha_min": "2026-06-20",
                "fecha_max": "2026-06-21",
            }
            sample_photo_df().to_excel(excel, sheet_name="Fotos", index=False)
            contract.write_text(json.dumps(fixture_contract(expected)), encoding="utf-8")
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = loader.main(
                    [
                        "--dry-run",
                        "--excel",
                        str(excel),
                        "--contract",
                        str(contract),
                        "--json-out",
                        str(out),
                    ]
                )
            self.assertEqual(code, 0)
            payload = json.loads(out.read_text(encoding="utf-8"))
            self.assertFalse(payload["db_apply"])
            self.assertFalse(payload["sql_apply"])
            self.assertFalse(payload["writes_executed"])
            self.assertFalse(payload["productive_loader_touched"])
            self.assertEqual(payload["metrics"]["source_row_number_min"], 2)
            self.assertEqual(payload["metrics"]["source_row_number_max"], 7)
            self.assertEqual(payload["photo_row_traceability"]["photo_rows_mapped"], 6)

    def test_real_workbook_matches_required_010c_evidence(self):
        source = ROOT / "data" / "photo-excel-admin_1782440454408.xlsx"
        if not source.exists():
            self.skipTest("route b source workbook is not present")
        payload = loader.build_dry_run_payload(
            source,
            sheet_name="Fotos",
            contract_path=ROOT / "contracts" / "control_gestion" / "kpione2_photo_export_contract_v1.json",
        )
        self.assertEqual(payload["metrics"]["photo_rows"], 37908)
        self.assertEqual(payload["metrics"]["distinct_event_ids"], 5892)
        self.assertEqual(payload["metrics"]["fecha_min"], "2026-06-20")
        self.assertEqual(payload["metrics"]["fecha_max"], "2026-06-24")
        self.assertEqual(payload["metrics"]["source_row_number_min"], 2)
        self.assertEqual(payload["metrics"]["source_row_number_max"], 37909)
        self.assertEqual(payload["metrics"]["source_row_number_distinct"], 37908)
        self.assertEqual(payload["metrics"]["source_row_number_null_rows"], 0)
        self.assertEqual(payload["photo_row_traceability"]["photo_rows_mapped"], 37908)
        self.assertFalse(payload["photo_row_traceability"]["event_identity_replaced"])
        self.assertFalse(payload["db_apply"])
        self.assertFalse(payload["sql_apply"])
        self.assertFalse(payload["productive_loader_touched"])
        self.assertEqual(payload["verdict"], "PASS_ROUTE_B_DRY_RUN")

    def test_sql_files_are_review_only(self):
        ddl = (ROOT / "sql" / "15_kpione2_photo_raw_ddl.sql").read_text(encoding="utf-8")
        rollback = (ROOT / "sql" / "16_kpione2_photo_raw_ddl_rollback.sql").read_text(encoding="utf-8")
        self.assertTrue(ddl.startswith("-- NO APPLY"))
        self.assertTrue(rollback.startswith("-- NO APPLY"))
        self.assertIn("create table if not exists cg_raw.kpione2_photo_raw", ddl)
        self.assertIn("drop table if exists cg_raw.kpione2_photo_raw", rollback)

    def test_new_loader_does_not_import_productive_loader_or_db_clients(self):
        source = (ROOT / "scripts" / "load_kpione2_photo_from_excel.py").read_text(encoding="utf-8")
        self.assertNotIn("import load_control_gestion_raw_v17", source)
        self.assertNotIn("from load_control_gestion_raw_v17", source)
        self.assertNotIn("psycopg2", source)
        self.assertNotIn("sqlalchemy", source.lower())
        self.assertNotIn("DB_URL", source)


if __name__ == "__main__":
    unittest.main()
