import argparse
import contextlib
import io
import json
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import load_ruta_rutero_from_excel as loader


def sample_route_df() -> pd.DataFrame:
    columns = [
        "CADENA",
        "FORMATO",
        "REGION",
        "COMUNA",
        "COD KPI ONE",
        "COD B2B",
        "LOCAL",
        "DIRECCION",
        "VECES POR SEMANA",
        "RUTERO",
        "JEFE DE OPERACIONES",
        "GESTORES",
        "CLIENTE",
        "SUPERVISOR",
        "REPONEDOR",
        "LUNES",
        "MARTES",
        "MIERCOLES",
        "JUEVES",
        "VIERNES",
        "SABADO",
        "DOMINGO",
        "Visita mensual",
        "DIF",
        "OBS",
        "AUX",
        "GG",
        "MODALIDAD",
    ]
    row1 = [
        "CAD",
        "FMT",
        "RM",
        "SCL",
        "A1",
        "100.0",
        "Local 1",
        "Addr",
        2,
        "Ruta 1",
        "Jefe",
        "Gestor",
        "Cliente X",
        "Super",
        "Repo 1",
        1,
        1,
        0,
        0,
        0,
        0,
        0,
        8,
        0,
        "",
        "",
        1,
        "Presencial",
    ]
    row2 = list(row1)
    row3 = list(row1)
    row3[9] = "Ruta 2"
    row3[14] = "Repo 2"
    row3[8] = 3
    row3[17] = 1
    row4 = list(row1)
    row4[4] = "B2"
    row4[12] = "Cliente Y"
    row4[8] = 5
    row4[16] = 0
    return pd.DataFrame([row1, row2, row3, row4], columns=columns)


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.closed = False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return [0]

    def fetchall(self):
        return []

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.autocommit = None
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class WeeklyReplacementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.tmp = Path(cls.tmpdir.name)
        cls.excel = cls.tmp / "route.xlsx"
        sample_route_df().to_excel(cls.excel, index=False, sheet_name="RUTA_RUTERO")
        cls.source = "route.xlsx:RUTA_RUTERO"
        cls.plan = loader.build_dry_run_plan(
            excel_path=cls.excel,
            sheet="RUTA_RUTERO",
            source=cls.source,
            effective_week_start_value="2026-06-08",
        )
        _, cls.accepted = loader.prepare_route_rows(cls.excel, sheet="RUTA_RUTERO", source=cls.source)
        cls.current = loader.current_surface_rows(cls.accepted)
        cls.sql_contract = (ROOT / "sql" / "11_control_gestion_route_week_replacement_contract.sql").read_text(
            encoding="utf-8"
        )

    @classmethod
    def tearDownClass(cls):
        cls.tmpdir.cleanup()

    def _run_main_json(self, args):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            loader.main(args)
        return json.loads(buf.getvalue())

    def _apply_args(self):
        return argparse.Namespace(
            excel=str(self.excel),
            sheet="RUTA_RUTERO",
            source=self.source,
            effective_week_start="2026-06-08",
            db_url="postgresql://user:password@localhost/db",
        )

    def test_01_default_is_dry_run(self):
        with mock.patch.object(loader, "connect_db") as connect_db:
            payload = self._run_main_json(
                [
                    "--excel",
                    str(self.excel),
                    "--sheet",
                    "RUTA_RUTERO",
                    "--source",
                    self.source,
                    "--effective-week-start",
                    "2026-06-08",
                ]
            )
        self.assertEqual(payload["mode"], "dry_run")
        self.assertFalse(payload["writes_executed"])
        connect_db.assert_not_called()

    def test_02_source_check_only_uses_no_db(self):
        with mock.patch.object(loader, "connect_db") as connect_db:
            payload = self._run_main_json(
                [
                    "--excel",
                    str(self.excel),
                    "--sheet",
                    "RUTA_RUTERO",
                    "--source",
                    self.source,
                    "--source-check-only",
                ]
            )
        self.assertIn("source_check", payload)
        connect_db.assert_not_called()

    def test_03_apply_requires_week(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as ctx:
            loader.main(["--apply", "--expected-workbook-sha256", "ABC", "--confirm-weekly-replacement", loader.ROUTE_POLICY_VERSION, "--json-out", str(self.tmp / "x.json"), "--db_url", "postgresql://u:p@h/db"])
        self.assertEqual(ctx.exception.code, 2)

    def test_04_week_must_be_monday(self):
        with self.assertRaises(loader.LoaderUsageError) as ctx:
            loader.parse_effective_week_start("2026-06-09")
        self.assertEqual(ctx.exception.code, "effective_week_start_must_be_monday")

    def test_05_apply_requires_hash(self):
        args = loader.build_arg_parser().parse_args(
            [
                "--apply",
                "--effective-week-start",
                "2026-06-08",
                "--confirm-weekly-replacement",
                loader.ROUTE_POLICY_VERSION,
                "--json-out",
                str(self.tmp / "x.json"),
                "--db_url",
                "postgresql://u:p@h/db",
            ]
        )
        with self.assertRaises(loader.LoaderUsageError) as ctx:
            loader.validate_cli_args(args)
        self.assertEqual(ctx.exception.code, "apply_requires_expected_workbook_sha256")

    def test_06_hash_mismatch_blocks(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), self.assertRaises(SystemExit) as ctx:
            loader.main(
                [
                    "--excel",
                    str(self.excel),
                    "--sheet",
                    "RUTA_RUTERO",
                    "--source",
                    self.source,
                    "--effective-week-start",
                    "2026-06-08",
                    "--expected-workbook-sha256",
                    "0" * 64,
                    "--dry-run",
                ]
            )
        self.assertEqual(ctx.exception.code, 1)
        self.assertEqual(json.loads(buf.getvalue())["error_code"], "workbook_hash_mismatch")

    def test_07_apply_requires_exact_confirm_token(self):
        args = loader.build_arg_parser().parse_args(
            [
                "--apply",
                "--effective-week-start",
                "2026-06-08",
                "--expected-workbook-sha256",
                "A" * 64,
                "--confirm-weekly-replacement",
                "WRONG",
                "--json-out",
                str(self.tmp / "x.json"),
                "--db_url",
                "postgresql://u:p@h/db",
            ]
        )
        with self.assertRaises(loader.LoaderUsageError) as ctx:
            loader.validate_cli_args(args)
        self.assertEqual(ctx.exception.code, "apply_requires_exact_confirm_token")

    def test_08_source_check_only_apply_incompatible(self):
        args = loader.build_arg_parser().parse_args(["--source-check-only", "--apply"])
        with self.assertRaises(loader.LoaderUsageError) as ctx:
            loader.validate_cli_args(args)
        self.assertEqual(ctx.exception.code, "source_check_only_incompatible_with_apply")

    def test_09_dry_run_apply_incompatible(self):
        args = loader.build_arg_parser().parse_args(["--dry-run", "--apply"])
        with self.assertRaises(loader.LoaderUsageError) as ctx:
            loader.validate_cli_args(args)
        self.assertEqual(ctx.exception.code, "dry_run_incompatible_with_apply")

    def test_10_json_output_valid(self):
        json_out = self.tmp / "plan.json"
        payload = self._run_main_json(
            [
                "--excel",
                str(self.excel),
                "--sheet",
                "RUTA_RUTERO",
                "--source",
                self.source,
                "--effective-week-start",
                "2026-06-08",
                "--json-out",
                str(json_out),
            ]
        )
        self.assertEqual(payload, json.loads(json_out.read_text(encoding="utf-8")))

    def test_11_output_deterministic(self):
        plan_a = loader.build_dry_run_plan(
            excel_path=self.excel,
            sheet="RUTA_RUTERO",
            source=self.source,
            effective_week_start_value="2026-06-08",
        )
        plan_b = loader.build_dry_run_plan(
            excel_path=self.excel,
            sheet="RUTA_RUTERO",
            source=self.source,
            effective_week_start_value="2026-06-08",
        )
        self.assertEqual(plan_a, plan_b)

    def test_12_history_preserves_exact_duplicates(self):
        history = loader.build_history_rows(self.accepted)
        self.assertEqual(len(history), 4)

    def test_13_current_excludes_exact_duplicates(self):
        self.assertEqual(len(self.current), 3)

    def test_14_current_preserves_non_exact_multirow(self):
        grain = loader.classify_grain_duplicates(self.current)
        self.assertEqual(grain["groups"], 1)
        self.assertEqual(grain["excess_rows"], 1)

    def test_15_no_collapse_by_cod_rt_cliente(self):
        frame = self.current.copy()
        frame["_cod_rt_norm"] = frame["cod_rt"].map(loader.normalize_text)
        frame["_cliente_norm"] = frame["cliente"].map(loader.normalize_key)
        rows = frame[(frame["_cod_rt_norm"] == "A1") & (frame["_cliente_norm"] == "CLIENTE X")]
        self.assertEqual(len(rows), 2)

    def test_16_frequency_days_is_warning(self):
        profile = loader.frequency_day_profile(self.accepted)
        self.assertEqual(profile["mismatch_count"], 1)

    def test_17_no_automatic_frequency_correction(self):
        mismatch = self.accepted[self.accepted["cod_rt"] == "B2"].iloc[0]
        self.assertEqual(int(mismatch["veces_por_semana"]), 5)
        self.assertEqual(int(mismatch[loader.DAY_COLUMNS].sum()), 1)

    def test_18_full_source_delete_is_parameterized(self):
        self.assertIn("WHERE source = %s", loader.PUBLIC_DELETE_SQL)

    def test_19_delete_restricted_by_source(self):
        sql = loader.PUBLIC_DELETE_SQL.upper()
        self.assertIn("DELETE FROM PUBLIC.RUTA_RUTERO", sql)
        self.assertIn("WHERE SOURCE", sql)

    def test_20_replacement_insert_has_no_source_row_upsert(self):
        self.assertNotIn("ON CONFLICT", loader.PUBLIC_INSERT_SQL.upper())
        self.assertNotIn("ON CONFLICT (SOURCE, SOURCE_ROW)", loader.PUBLIC_INSERT_SQL.upper())

    def test_21_transaction_order_is_documented(self):
        self.assertEqual(loader.APPLY_TRANSACTION_STEPS[8], "register_batch_pending")
        self.assertLess(
            loader.APPLY_TRANSACTION_STEPS.index("run_postcheck"),
            loader.APPLY_TRANSACTION_STEPS.index("finalize_batch_ok"),
        )

    def test_22_rollback_on_apply_error(self):
        conn = FakeConnection()
        args = self._apply_args()
        with mock.patch.object(loader, "connect_db", return_value=conn), \
            mock.patch.object(loader, "verify_db_contract", return_value={"ok": True, "missing": []}), \
            mock.patch.object(loader, "register_cg_ruta_batch", return_value=1), \
            mock.patch.object(loader, "build_cg_history_rows", return_value=[]), \
            mock.patch.object(loader, "insert_cg_history_rows", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                loader.run_weekly_replacement_apply(args, dict(self.plan))
        self.assertTrue(conn.rolled_back)
        self.assertFalse(conn.committed)

    def test_23_batch_not_ok_before_postcheck(self):
        conn = FakeConnection()
        args = self._apply_args()
        with mock.patch.object(loader, "connect_db", return_value=conn), \
            mock.patch.object(loader, "verify_db_contract", return_value={"ok": True, "missing": []}), \
            mock.patch.object(loader, "register_cg_ruta_batch", return_value=1), \
            mock.patch.object(loader, "build_cg_history_rows", return_value=[]), \
            mock.patch.object(loader, "insert_cg_history_rows"), \
            mock.patch.object(loader, "validate_history_row_count"), \
            mock.patch.object(loader, "fetch_current_snapshot_summary", return_value={"rows": 0}), \
            mock.patch.object(loader, "delete_public_ruta_for_source"), \
            mock.patch.object(loader, "insert_public_current_rows"), \
            mock.patch.object(loader, "validate_current_surface_count"), \
            mock.patch.object(loader, "create_week_assignment", return_value=2), \
            mock.patch.object(loader, "run_postcheck", side_effect=RuntimeError("postcheck failed")), \
            mock.patch.object(loader, "finalize_cg_ruta_batch") as finalize:
            with self.assertRaises(RuntimeError):
                loader.run_weekly_replacement_apply(args, dict(self.plan))
        finalize.assert_not_called()
        self.assertTrue(conn.rolled_back)

    def test_24_missing_assignment_table_blocks_apply(self):
        conn = FakeConnection()
        args = self._apply_args()
        with mock.patch.object(loader, "connect_db", return_value=conn), \
            mock.patch.object(
                loader,
                "verify_db_contract",
                return_value={"ok": False, "missing": ["missing_relation:cg_core.ruta_rutero_week_assignment"]},
            ), \
            mock.patch.object(loader, "delete_public_ruta_for_source") as delete_source:
            with self.assertRaises(loader.MissingDBContractError):
                loader.run_weekly_replacement_apply(args, dict(self.plan))
        delete_source.assert_not_called()
        self.assertTrue(conn.rolled_back)

    def test_25_missing_view_contract_blocks_apply(self):
        conn = FakeConnection()
        args = self._apply_args()
        with mock.patch.object(loader, "connect_db", return_value=conn), \
            mock.patch.object(
                loader,
                "verify_db_contract",
                return_value={"ok": False, "missing": ["missing_week_view_column:route_week_source"]},
            ), \
            mock.patch.object(loader, "delete_public_ruta_for_source") as delete_source:
            with self.assertRaises(loader.MissingDBContractError):
                loader.run_weekly_replacement_apply(args, dict(self.plan))
        delete_source.assert_not_called()

    def test_26_dry_run_works_with_schema_absent(self):
        with mock.patch.object(loader, "verify_db_contract") as verify:
            plan = loader.build_dry_run_plan(
                excel_path=self.excel,
                sheet="RUTA_RUTERO",
                source=self.source,
                effective_week_start_value="2026-06-08",
            )
        self.assertEqual(plan["mode"], "dry_run")
        verify.assert_not_called()

    def test_27_no_db_with_dry_run(self):
        with mock.patch.object(loader, "connect_db") as connect_db:
            loader.build_dry_run_plan(
                excel_path=self.excel,
                sheet="RUTA_RUTERO",
                source=self.source,
                effective_week_start_value="2026-06-08",
            )
        connect_db.assert_not_called()

    def test_28_no_secrets_in_error_output(self):
        payload = loader.safe_error_payload(RuntimeError("postgresql://u:secret@host/db?password=hidden"))
        self.assertNotIn("secret", json.dumps(payload))
        self.assertNotIn("hidden", json.dumps(payload))

    def test_29_no_dsn_printed_flag(self):
        self.assertFalse(self.plan["dsn_printed"])

    def test_30_no_shell_true(self):
        source = (ROOT / "scripts" / "load_ruta_rutero_from_excel.py").read_text(encoding="utf-8")
        self.assertNotIn("shell=True", source)

    def test_31_no_free_sql_cli(self):
        options = {action.dest for action in loader.build_arg_parser()._actions}
        self.assertNotIn("sql", options)
        self.assertNotIn("stdin", options)

    def test_32_sql_file_contains_no_apply_header(self):
        self.assertTrue(self.sql_contract.startswith("-- NO APPLY IN CG005C"))

    def test_33_assignment_table_contract(self):
        self.assertIn("create table if not exists cg_core.ruta_rutero_week_assignment", self.sql_contract)
        self.assertIn("effective_week_start date not null", self.sql_contract)

    def test_34_unique_active_assignment(self):
        self.assertIn("ux_ruta_rutero_week_assignment_active", self.sql_contract)
        self.assertIn("where assignment_status = 'ACTIVE'", self.sql_contract)

    def test_35_explicit_week_preferred(self):
        explicit_pos = self.sql_contract.index("EXPLICIT_ASSIGNMENT")
        legacy_pos = self.sql_contract.index("LEGACY_INFERRED")
        self.assertLess(explicit_pos, legacy_pos)

    def test_36_legacy_fallback_visible(self):
        self.assertIn("LEGACY_INFERRED", self.sql_contract)
        self.assertIn("loaded_at", self.sql_contract)

    def test_37_exact_duplicates_preserved_in_history(self):
        history = loader.build_history_rows(self.accepted)
        counts = Counter(row["row_hash"] for row in history)
        self.assertIn(2, counts.values())

    def test_38_exact_duplicates_excluded_current(self):
        counts = Counter(self.current["row_hash"].tolist())
        self.assertTrue(all(count == 1 for count in counts.values()))

    def test_39_postcheck_contract(self):
        self.assertIn("resolved_duplicate_logical_grains_zero", self.plan["postcheck_contract"])
        self.assertIn("week_view_reports_explicit_assignment", self.plan["postcheck_contract"])

    def test_40_rollback_contract(self):
        self.assertEqual(self.plan["rollback_contract"]["confirm_token"], loader.ROLLBACK_CONFIRM_TOKEN)
        self.assertTrue(callable(loader.run_weekly_replacement_rollback))

    def test_41_apply_requires_explicit_db_url(self):
        args = loader.build_arg_parser().parse_args(
            [
                "--apply",
                "--effective-week-start",
                "2026-06-08",
                "--expected-workbook-sha256",
                "A" * 64,
                "--confirm-weekly-replacement",
                loader.ROUTE_POLICY_VERSION,
                "--json-out",
                str(self.tmp / "x.json"),
            ]
        )
        with self.assertRaises(loader.LoaderUsageError) as ctx:
            loader.validate_cli_args(args)
        self.assertEqual(ctx.exception.code, "apply_requires_explicit_db_url")

    def test_42_source_row_original_position(self):
        self.assertEqual(self.accepted["source_row"].tolist(), [2, 3, 4, 5])

    def test_43_schema_signature_is_stable(self):
        df = sample_route_df()
        self.assertEqual(loader.schema_signature_from_columns(df.columns), loader.schema_signature_from_columns(df.columns))

    def test_44_hash_function_uppercase(self):
        digest = loader.sha256_file(self.excel)
        self.assertEqual(digest, digest.upper())
        self.assertEqual(len(digest), 64)

    def test_45_public_rows_are_current_surface_only(self):
        self.assertEqual(len(loader.build_public_rows(self.current)), 3)

    def test_46_workbook_validated_counts(self):
        workbook = ROOT / "data" / "DB_GLOBAL_INVENTARIO.xlsx"
        if not workbook.exists():
            self.skipTest("authorized workbook not present")
        actual_hash = loader.sha256_file(workbook)
        expected_hash = "454F3CF414B031FB793CA6298CF03638C2EDE3B53B918E6ACAB636C9B4B6AD83"
        if actual_hash != expected_hash:
            self.skipTest("authorized workbook hash differs")
        plan = loader.build_dry_run_plan(
            excel_path=workbook,
            sheet="RUTA_RUTERO",
            source="DB_GLOBAL_INVENTARIO.xlsx:RUTA_RUTERO",
            effective_week_start_value="2026-06-08",
            expected_workbook_sha256=expected_hash,
        )
        self.assertEqual(plan["input_rows"], 3542)
        self.assertEqual(plan["history_insert_rows"], 3542)
        self.assertEqual(plan["exact_duplicate_excess"], 3)
        self.assertEqual(plan["planned_public_insert_rows"], 3539)


if __name__ == "__main__":
    unittest.main()
