import json
import re
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import cg_route_weekly_local_lab as lab
import load_ruta_rutero_from_excel as loader


class LocalLabSafetyTests(unittest.TestCase):
    def test_input_file_code_is_contract_safe(self):
        self.assertEqual(lab.INPUT_FILE_CODE, "DB_GLOBAL_INVENTARIO_XLSX")
        self.assertRegex(lab.INPUT_FILE_CODE, r"^[A-Z][A-Z0-9_:-]{0,119}$")
        self.assertIsNotNone(re.fullmatch(r"^[A-Z][A-Z0-9_:-]{0,119}$", lab.INPUT_FILE_CODE))
        for forbidden in (".", "/", "\\", " ", "C:", "Users"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, lab.INPUT_FILE_CODE)

    def test_platform_008_observation_uses_technical_input_code(self):
        import sz_load_observation as obs

        seen = {}

        def fake_run(argv):
            if argv[0] == "draft":
                phase_json = Path(argv[argv.index("--phase-json") + 1])
                phase_payload = json.loads(phase_json.read_text(encoding="utf-8"))
                draft = phase_payload["observation_draft"]
                seen["draft"] = draft
                print(
                    json.dumps(
                        {
                            "observation_id": "OBS_TEST",
                            "input_file_name": draft["input_file_name"],
                            "input_file_sha256": draft["input_file_sha256"],
                            "anomaly_label": draft["anomaly_label"],
                            "implementation_authorized": draft["implementation_authorized"],
                        }
                    )
                )
                return 0
            if argv[0] == "validate":
                print(json.dumps({"validate": "ok", "observation_id": "OBS_TEST", "record": {}}))
                return 0
            raise AssertionError(f"unexpected argv: {argv}")

        lab_summary = {
            "cg005j": {
                "workbook_sha256": "A" * 64,
                "schema_signature": "B" * 64,
                "snapshot_a_rows": 10,
                "exact_duplicate_excess": 1,
                "history_rows": 11,
                "source_check": "ok",
            },
            "cg005k": {
                "snapshot_b": {
                    "synthetic_highs": 1,
                    "removed_logical_grains": 2,
                    "changed_responsables": 2,
                    "changed_frequency_or_days": 1,
                }
            },
        }
        with tempfile.TemporaryDirectory() as raw:
            with mock.patch.object(obs, "register_test_input_root", lambda _path: None):
                with mock.patch.object(obs, "run", side_effect=fake_run):
                    result = lab.run_platform_008(Path(raw), lab_summary)

        draft = seen["draft"]
        self.assertEqual(draft["input_file_name"], lab.INPUT_FILE_CODE)
        self.assertNotEqual(draft["input_file_name"], "DB_GLOBAL_INVENTARIO.xlsx")
        self.assertEqual(len(draft["input_file_sha256"]), 64)
        self.assertEqual(draft["input_file_sha256"], "A" * 64)
        self.assertTrue(result["candidate_validated"])
        self.assertTrue(result["ledger_unchanged"])
        self.assertEqual(result["input_file_name"], lab.INPUT_FILE_CODE)
        self.assertEqual(result["anomaly_label"], "UNREVIEWED")
        self.assertFalse(result["implementation_authorized"])
        self.assertFalse(result["ledger_write_executed"])
        self.assertTrue(result["temporary_candidate_deleted"])

    def test_loopback_dsn_is_allowed(self):
        info = lab.parse_loopback_dsn("postgresql://postgres@127.0.0.1:55433/stock_zero_cg005_lab?sslmode=disable")
        self.assertEqual(info.host, "127.0.0.1")
        self.assertEqual(info.database, "stock_zero_cg005_lab")
        self.assertEqual(info.sslmode, "disable")

    def test_remote_dsn_is_rejected(self):
        for dsn in (
            "postgresql://user@example.com/db",
            "postgresql://user@db.supabase.co/db",
            "postgresql://user@aws-prod.example.com/db",
            "postgresql://user@10.0.0.5/db",
        ):
            with self.subTest(dsn=dsn):
                with self.assertRaises(lab.LabError):
                    lab.parse_loopback_dsn(dsn)

    def test_database_dsn_rewrite_preserves_query(self):
        dsn = lab.dsn_for_database("postgresql://postgres@localhost:55433/stock_zero_cg005_lab?sslmode=disable", "postgres")
        self.assertEqual(dsn, "postgresql://postgres@localhost:55433/postgres?sslmode=disable")

    def test_execute_values_statement_shape(self):
        stmt = lab._values_statement("insert into x(a,b,c) values %s", 3)
        self.assertIn("values (%s,%s,%s)", stmt)

    def test_sql11_body_extracts_no_apply_wrapper(self):
        body, meta = lab.extract_sql11_body()
        self.assertTrue(meta["no_apply_header"])
        self.assertTrue(meta["begin_rollback_wrapper"])
        self.assertNotIn("-- NO APPLY", body)
        self.assertNotRegex(body.lower(), r"^\s*begin\s*;")
        self.assertNotRegex(body.lower(), r"rollback\s*;\s*$")
        self.assertIn("create table if not exists cg_core.ruta_rutero_week_assignment", body.lower())


class SnapshotBTests(unittest.TestCase):
    def _workbook(self, tmp: Path) -> Path:
        cols = list(loader.ROUTE_COLUMN_KEYS.values())
        rows = []
        for i in range(8):
            row = {col: "" for col in cols}
            row.update(
                {
                    "CADENA": "C",
                    "FORMATO": "F",
                    "REGION": "R",
                    "COMUNA": "COM",
                    "COD KPI ONE": f"RT{i:03d}",
                    "COD B2B": f"B2B{i:03d}",
                    "LOCAL": f"L{i:03d}",
                    "DIRECCION": f"D{i:03d}",
                    "VECES POR SEMANA": 2,
                    "RUTERO": f"RUT{i:03d}",
                    "JEFE DE OPERACIONES": "J",
                    "GESTORES": "G",
                    "CLIENTE": f"CLIENTE{i:03d}",
                    "SUPERVISOR": "S",
                    "REPONEDOR": f"P{i:03d}",
                    "LUNES": 1,
                    "MARTES": 1,
                    "MIERCOLES": 0,
                    "JUEVES": 0,
                    "VIERNES": 0,
                    "SABADO": 0,
                    "DOMINGO": 0,
                    "VISITA MENSUAL": 0,
                    "DIF": 0,
                    "OBS": "",
                    "AUX": "",
                    "GG": 0,
                    "MODALIDAD": "M",
                }
            )
            rows.append(row)
        path = tmp / "a.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=lab.SHEET)
        return path

    def test_snapshot_b_profile_is_metric_only(self):
        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            workbook = self._workbook(tmp)
            snapshot_b, profile = lab.make_snapshot_b(loader, workbook, tmp)
            self.assertTrue(snapshot_b.exists())
            self.assertEqual(profile["removed_logical_grains"], 2)
            self.assertEqual(profile["changed_responsables"], 2)
            self.assertEqual(profile["changed_frequency_or_days"], 1)
            self.assertEqual(profile["synthetic_highs"], 1)
            self.assertEqual(len(profile["sha256"]), 64)
            public_profile = {k: v for k, v in profile.items() if not k.startswith("_")}
            self.assertNotIn("LAB_ONLY_CLIENTE", str(public_profile))


class LocalPostgresIntegrationTests(unittest.TestCase):
    DSN = "postgresql://postgres@127.0.0.1:55433/stock_zero_cg005_lab?sslmode=disable"

    def setUp(self):
        try:
            lab.query_one(self.DSN, "select 1")
        except Exception as exc:
            self.skipTest(f"local PostgreSQL lab unavailable: {type(exc).__name__}")

    def test_assignment_insert_executes_with_notes_on_local_postgres(self):
        lab.apply_bootstrap_and_sql11(self.DSN)
        plan = {
            "input_file_name": "lab.xlsx",
            "input_file_sha256": "A" * 64,
            "schema_signature": "B" * 64,
            "planned_assignment": {
                "current_surface_hash": "C" * 64,
                "resolved_surface_hash": "D" * 64,
            },
        }
        with lab.connect(self.DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into cg_core.ruta_rutero_load_batch
                    (source_file, source_sheet, loader_name, loaded_rows, status, loaded_at, notes)
                    values (%s, %s, %s, 0, 'pending', %s, %s)
                    returning ruta_batch_id
                    """,
                    ("lab.xlsx", lab.SHEET, "test", datetime.now(timezone.utc), "arity integration"),
                )
                batch_id = int(cur.fetchone()[0])
                assignment_id = loader.create_week_assignment(
                    cur,
                    effective_week_start_value="2026-07-06",
                    ruta_batch_id=batch_id,
                    plan=plan,
                    assigned_by="arity-integration-test",
                    replaces_ruta_batch_id=None,
                )
                cur.execute(
                    """
                    select assigned_by, replaces_ruta_batch_id, notes
                      from cg_core.ruta_rutero_week_assignment
                     where assignment_id = %s
                    """,
                    (assignment_id,),
                )
                assigned_by, replaces_ruta_batch_id, notes = cur.fetchone()
            conn.rollback()
        self.assertEqual(assigned_by, "arity-integration-test")
        self.assertIsNone(replaces_ruta_batch_id)
        self.assertEqual(notes, "weekly replacement assignment created by guarded loader")


if __name__ == "__main__":
    unittest.main()
