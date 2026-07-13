from __future__ import annotations

import os
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from scripts.kpione_route_b_v1 import (
    RouteBError, assert_local_target, build_plan, identity_key, inspect_workbook,
    normalize_numeric_string, parse_date, sha256_file,
)


HEADERS = ["ID", "SP Item ID", "Holding", "Subcadena", "Codigo Local", "Marca",
           "Local", "Direccion", "Reponedor", "Fecha", "Fecha de subida", "Hora",
           "Tipo de Tarea", "Foto No/Total", "Comentarios", "Link Foto"]


def write_book(path: Path, rows: list[list[object]], headers: list[str] = HEADERS) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Fotos"
    sheet.append(headers)
    for row in rows:
        sheet.append(row)
    workbook.save(path)


def row(event_id: object = 100, sp: object = 900, fecha: object = "2026-07-01",
        link: str = "https://example.invalid/a.jpg", task: str = "EXHIBICION") -> list[object]:
    return [event_id, sp, "H", "S", "RT-1", "Marca Uno", "Local Uno", "Calle 1",
            "Persona", fecha, "2026-07-01", "09:00", task, "1/1", "ok", link]


class RouteBUnitTests(unittest.TestCase):
    def test_normalization_is_deterministic(self) -> None:
        self.assertEqual(identity_key("  Gestión   zéro "), "GESTION ZERO")
        self.assertEqual(normalize_numeric_string("001.0"), "1")
        self.assertEqual(parse_date("01/07/2026"), "2026-07-01")

    def test_plan_hash_and_identity_ignore_folder_path(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            a = Path(first) / "photo-excel-admin_1.xlsx"
            b = Path(second) / "photo-excel-admin_renamed.xlsx"
            write_book(a, [row()])
            b.write_bytes(a.read_bytes())
            pa, pb = build_plan(Path(first)), build_plan(Path(second))
            self.assertEqual(sha256_file(a), sha256_file(b))
            self.assertEqual(pa["semantic_plan_hash"], pb["semantic_plan_hash"])
            self.assertEqual(pa["_workbooks"][0].rows[0]["source_row_identity"], pb["_workbooks"][0].rows[0]["source_row_identity"])

    def test_duplicate_photo_is_classified_and_presence_binary(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "photo-excel-admin_1.xlsx"
            write_book(path, [row(), row()])
            plan = build_plan(Path(folder))
            self.assertEqual(plan["duplicate_rows"], 1)
            self.assertEqual(plan["distinct_events"], 1)
            self.assertEqual(plan["day_presence_count"], 1)

    def test_schema_and_event_conflicts_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "photo-excel-admin_1.xlsx"
            write_book(path, [row()], HEADERS[:-1])
            with self.assertRaisesRegex(RouteBError, "missing_required_columns"):
                inspect_workbook(path)
            write_book(path, [row(), row(sp=901, link="https://example.invalid/b.jpg")])
            with self.assertRaisesRegex(RouteBError, "event_stability_conflict"):
                inspect_workbook(path)

    def test_local_target_guard(self) -> None:
        self.assertEqual(assert_local_target("DB_URL_CODEX_LOCAL", "postgresql://u:p@127.0.0.1:5432/x"), "LOCAL_POSTGRESQL_LOOPBACK")
        unsafe = [
            ("DATABASE_URL", "postgresql://u:p@localhost/x"),
            ("DB_URL_CODEX_LOCAL", "postgresql://u:p@db.example.com/x"),
            ("DB_URL_CODEX_LOCAL", "postgresql://u:p@abc.supabase.co/x"),
            ("DB_URL_CODEX_LOCAL", "postgresql://u:p@localhost/x?sslmode=require"),
            ("DB_URL_CODEX_LOCAL", None),
        ]
        for env_name, dsn in unsafe:
            with self.subTest(env_name=env_name, dsn=dsn), self.assertRaises(RouteBError):
                assert_local_target(env_name, dsn)

    def test_cli_defaults_to_non_mutating_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            write_book(Path(folder) / "photo-excel-admin_1.xlsx", [row()])
            runner = Path(__file__).resolve().parents[1] / "scripts" / "run_kpione_route_b_ingestion_v1.py"
            completed = subprocess.run(
                [sys.executable, str(runner), "--input-dir", folder],
                check=True,
                capture_output=True,
                text=True,
            )
            report = json.loads(completed.stdout)
            self.assertFalse(report["apply_authorized"])
            self.assertEqual(report["db_target_classification"], "NOT_EVALUATED_DRY_RUN")


if __name__ == "__main__":
    unittest.main()
