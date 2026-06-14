import tempfile
import unittest
from pathlib import Path

import pandas as pd

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import cg_route_weekly_local_lab as lab
import load_ruta_rutero_from_excel as loader


class LocalLabSafetyTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
