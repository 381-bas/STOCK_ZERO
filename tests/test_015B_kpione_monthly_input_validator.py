import json
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import validate_kpione_monthly_input_015B_no_apply as validator


def write_photo(path: Path, *, sheet: str = "Fotos", fechas: list[str] | None = None) -> None:
    fechas = fechas or ["2026-06-01", "2026-06-02"]
    frame = pd.DataFrame(
        {
            "ID": [str(i + 1) for i in range(len(fechas))],
            "Fecha": fechas,
            "Codigo Local": ["A"] * len(fechas),
        }
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name=sheet, index=False)


def write_route(path: Path, *, sheet: str = "RUTA RUTERO") -> None:
    frame = pd.DataFrame(
        {
            "CADENA": ["CAD"],
            "LOCAL": ["LOCAL A"],
            "LUNES": [1],
        }
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name=sheet, index=False)


def write_route_files(route_dir: Path, month_name: str, labels: list[str]) -> list[Path]:
    route_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for label in labels:
        path = route_dir / f"RUTA_RUTEROS_{month_name}_{label}.xlsx"
        write_route(path)
        paths.append(path)
    return paths


def manifest_photo_entry(base: Path, path: Path, *, role: str = "include_candidate") -> dict:
    return {
        "source_file_id": validator.parse_source_file_id(path),
        "source_file_name": path.name,
        "relative_path": path.relative_to(base).as_posix(),
        "sha256": validator.sha256_file(path),
        "size_bytes": path.stat().st_size,
        "role": role,
    }


def manifest_route_entry(base: Path, path: Path, week_label: str, week_start: str, assigned_month: str) -> dict:
    return {
        "source_file_name": path.name,
        "relative_path": path.relative_to(base).as_posix(),
        "sha256": validator.sha256_file(path),
        "size_bytes": path.stat().st_size,
        "row_count": 1,
        "week_label": week_label,
        "week_start": week_start,
        "assigned_operational_month": assigned_month,
    }


def minimal_manifest(
    *,
    month_id: str = "2026-06",
    photos: list[dict] | None = None,
    routes: list[dict] | None = None,
    transition: dict | None = None,
) -> dict:
    return {
        "phase_id": "015A_MONTHLY_INPUT_LAYOUT_TRACEABILITY_NO_APPLY",
        "month_id": month_id,
        "photo_report_files": {"files": photos or []},
        "ruta_rutero_reference": {
            "files": routes or [],
            "transition_week": transition or {},
        },
    }


class MonthlyInputValidator015BTests(unittest.TestCase):
    def test_month_id_valid_invalid(self):
        self.assertEqual(validator.normalize_month_id("2026-06"), "2026-06")
        with self.assertRaises(ValueError):
            validator.normalize_month_id("2026-13")
        with self.assertRaises(ValueError):
            validator.normalize_month_id("202606")

    def test_reuses_015a_contract_for_week_rule(self):
        self.assertEqual(
            validator.month_contract.assigned_operational_month_from_fecha("2026-06-29"),
            "2026-07",
        )
        source = (SCRIPTS / "validate_kpione_monthly_input_015B_no_apply.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("import monthly_input_layout_contract_015 as month_contract", source)
        self.assertNotIn("for offset in range(7)", source)

    def test_only_monthly_layout_and_legacy_detected_not_used(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            monthly = base / "data" / "kpione_photo_reports" / "2026-06"
            monthly.mkdir(parents=True)
            write_photo(monthly / "photo-excel-admin_1.xlsx")
            legacy = base / "data" / "photo-excel-admin_1.xlsx"
            write_photo(legacy)
            payload = validator.build_validation_payload(base=base, month_id="2026-06")
            self.assertIn("legacy_files_detected_not_used", payload["warnings"])
            self.assertEqual(len(payload["photo_reports"]["files"]), 1)
            self.assertEqual(
                payload["photo_reports"]["files"][0]["relative_path"],
                "data/kpione_photo_reports/2026-06/photo-excel-admin_1.xlsx",
            )

    def test_missing_monthly_directory_blocks(self):
        with tempfile.TemporaryDirectory() as raw:
            payload = validator.build_validation_payload(base=Path(raw), month_id="2026-06")
            self.assertIn("monthly_photo_directory_missing:2026-06", payload["blockers"])

    def test_duplicate_source_file_id_helper_blocks(self):
        duplicates = validator.source_file_id_duplicates_from_paths(
            [
                Path("photo-excel-admin_123.xlsx"),
                Path("photo-excel-admin_123.xlsx"),
            ]
        )
        self.assertEqual(duplicates, ["123"])

    def test_hash_size_schema_and_rows_are_calculated(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            monthly = base / "data" / "kpione_photo_reports" / "2026-06"
            monthly.mkdir(parents=True)
            path = monthly / "photo-excel-admin_1.xlsx"
            write_photo(path)
            profile, blockers, _, _ = validator.profile_photo_file(
                base=base,
                path=path,
                operational_month_id="2026-06",
                source_calendar_month="2026-06",
                as_of_date=date.fromisoformat("2026-06-02"),
                validation_mode="open",
                expected_entry=None,
            )
            self.assertFalse(blockers)
            self.assertRegex(profile["sha256"], r"^[0-9a-f]{64}$")
            self.assertGreater(profile["size_bytes"], 0)
            self.assertEqual(profile["row_count"], 2)
            self.assertRegex(profile["schema_signature"], r"^[0-9a-f]{64}$")

    def test_missing_fotos_sheet_blocks(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            monthly = base / "data" / "kpione_photo_reports" / "2026-06"
            monthly.mkdir(parents=True)
            path = monthly / "photo-excel-admin_1.xlsx"
            write_photo(path, sheet="Other")
            payload = validator.build_validation_payload(base=base, month_id="2026-06")
            self.assertTrue(
                any(item.startswith("photo_required_sheet_missing:") for item in payload["blockers"])
            )

    def test_dates_outside_month_are_blocked(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            monthly = base / "data" / "kpione_photo_reports" / "2026-06"
            monthly.mkdir(parents=True)
            write_photo(monthly / "photo-excel-admin_1.xlsx", fechas=["2026-07-01"])
            payload = validator.build_validation_payload(base=base, month_id="2026-06")
            self.assertTrue(
                any(item.startswith("photo_rows_outside_calendar_month:") for item in payload["blockers"])
            )

    def test_transition_day_assigned_to_july(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            monthly = base / "data" / "kpione_photo_reports" / "2026-06"
            monthly.mkdir(parents=True)
            write_photo(monthly / "photo-excel-admin_1.xlsx", fechas=["2026-06-29"])
            payload = validator.build_validation_payload(base=base, month_id="2026-06")
            self.assertEqual(payload["operational_week_assignments"], [])
            self.assertEqual(
                payload["adjacent_operational_month_rows"][0]["assigned_operational_month"],
                "2026-07",
            )
            self.assertEqual(
                payload["adjacent_operational_month_rows"][0]["status"],
                "valid_carry_forward",
            )
            self.assertFalse(
                any("photo_rows_assigned_outside_operational_month" in item for item in payload["warnings"])
            )

    def test_month_argument_is_operational_month_and_reads_required_calendar_months(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            june = base / "data" / "kpione_photo_reports" / "2026-06"
            june.mkdir(parents=True)
            write_photo(june / "photo-excel-admin_1.xlsx", fechas=["2026-06-29", "2026-06-30"])

            payload = validator.build_validation_payload(
                base=base,
                month_id="2026-07",
                as_of="2026-07-11",
                validation_mode="open",
            )

            self.assertEqual(
                payload["required_calendar_months_now"],
                ["2026-06", "2026-07"],
            )
            self.assertEqual(
                payload["required_calendar_months_at_close"],
                ["2026-06", "2026-07", "2026-08"],
            )
            self.assertEqual(payload["operational_coverage"]["operational_coverage_start"], "2026-06-29")
            self.assertEqual(payload["operational_coverage"]["operational_coverage_end"], "2026-08-02")
            self.assertEqual(
                payload["operational_week_assignments"][0]["assigned_operational_month"],
                "2026-07",
            )
            self.assertEqual(payload["operational_week_assignments"][0]["row_count"], 2)
            self.assertEqual(payload["adjacent_operational_month_rows"], [])
            self.assertFalse(
                any("photo_rows_assigned_outside_operational_month" in item for item in payload["warnings"])
            )

    def test_open_month_before_coverage_start_is_in_progress_without_future_blockers(self):
        with tempfile.TemporaryDirectory() as raw:
            payload = validator.build_validation_payload(
                base=Path(raw),
                month_id="2026-07",
                as_of="2026-06-20",
                validation_mode="open",
            )

            self.assertEqual(payload["operational_period_status"], "NOT_STARTED")
            self.assertEqual(payload["required_calendar_months_now"], [])
            self.assertEqual(payload["verdict"], "IN_PROGRESS")
            self.assertEqual(payload["blockers"], [])

    def test_open_month_mid_cycle_ignores_future_month_folder_but_not_current_requirements(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            june = base / "data" / "kpione_photo_reports" / "2026-06"
            july = base / "data" / "kpione_photo_reports" / "2026-07"
            june.mkdir(parents=True)
            july.mkdir(parents=True)
            write_photo(june / "photo-excel-admin_1.xlsx", fechas=["2026-06-29", "2026-06-30"])
            write_photo(july / "photo-excel-admin_2.xlsx", fechas=["2026-07-10"])
            write_route_files(base / "data" / "RUTA_RUTERO" / "07 - JULIO", "JULIO", ["S1", "S2", "S3"])

            payload = validator.build_validation_payload(
                base=base,
                month_id="2026-07",
                as_of="2026-07-15",
                validation_mode="open",
            )

            self.assertEqual(payload["operational_period_status"], "IN_PROGRESS")
            self.assertEqual(payload["required_coverage_through"], "2026-07-15")
            self.assertEqual(payload["pending_future_start"], "2026-07-16")
            self.assertEqual(payload["pending_future_end"], "2026-08-02")
            self.assertEqual(payload["required_calendar_months_now"], ["2026-06", "2026-07"])
            self.assertNotIn("monthly_photo_directory_missing:2026-08", payload["blockers"])
            self.assertEqual(payload["verdict"], "IN_PROGRESS")

    def test_open_month_during_trailing_next_calendar_month_week_requires_that_month_now(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            for month_id, fechas in {
                "2026-06": ["2026-06-29"],
                "2026-07": ["2026-07-15"],
                "2026-08": ["2026-08-01"],
            }.items():
                folder = base / "data" / "kpione_photo_reports" / month_id
                folder.mkdir(parents=True)
                write_photo(folder / f"photo-excel-admin_{month_id[-2:]}.xlsx", fechas=fechas)
            write_route_files(
                base / "data" / "RUTA_RUTERO" / "07 - JULIO",
                "JULIO",
                ["S1", "S2", "S3", "S4", "S5"],
            )

            payload = validator.build_validation_payload(
                base=base,
                month_id="2026-07",
                as_of="2026-08-01",
                validation_mode="open",
            )

            self.assertEqual(payload["required_calendar_months_now"], ["2026-06", "2026-07", "2026-08"])
            self.assertEqual(payload["required_coverage_through"], "2026-08-01")
            self.assertEqual(payload["pending_future_start"], "2026-08-02")
            self.assertEqual(payload["verdict"], "IN_PROGRESS")

    def test_close_mode_complete_passes_with_manifest_and_all_routes(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            monthly = base / "data" / "kpione_photo_reports" / "2026-06"
            monthly.mkdir(parents=True)
            photo = monthly / "photo-excel-admin_1.xlsx"
            write_photo(photo, fechas=["2026-06-01"])
            route_paths = write_route_files(
                base / "data" / "RUTA_RUTERO" / "06 - JUNIO",
                "JUNIO",
                ["S1", "S2", "S3", "S4"],
            )
            weeks = validator.month_contract.route_week_mapping_for_month("2026-06")
            manifest_path = base / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    minimal_manifest(
                        photos=[manifest_photo_entry(base, photo)],
                        routes=[
                            manifest_route_entry(
                                base,
                                path,
                                week["week_label"],
                                week["week_start"],
                                week["assigned_operational_month"],
                            )
                            for path, week in zip(route_paths, weeks)
                        ],
                    )
                ),
                encoding="utf-8",
            )

            payload = validator.build_validation_payload(
                base=base,
                month_id="2026-06",
                as_of="2026-06-29",
                validation_mode="close",
                reference_manifest_path=str(manifest_path),
            )

            self.assertEqual(payload["validation_mode"], "close")
            self.assertEqual(payload["verdict"], "PASS")
            self.assertEqual(payload["blockers"], [])

    def test_close_mode_missing_next_month_folder_blocks(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            for month_id in ("2026-06", "2026-07"):
                folder = base / "data" / "kpione_photo_reports" / month_id
                folder.mkdir(parents=True)
                write_photo(folder / f"photo-excel-admin_{month_id[-2:]}.xlsx", fechas=[f"{month_id}-01"])

            payload = validator.build_validation_payload(
                base=base,
                month_id="2026-07",
                as_of="2026-08-03",
                validation_mode="close",
            )

            self.assertIn("monthly_photo_directory_missing:2026-08", payload["blockers"])
            self.assertEqual(payload["verdict"], "BLOCKED")

    def test_historical_required_folder_missing_is_blocker_in_open_mode(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            june = base / "data" / "kpione_photo_reports" / "2026-06"
            june.mkdir(parents=True)
            write_photo(june / "photo-excel-admin_1.xlsx", fechas=["2026-06-29"])

            payload = validator.build_validation_payload(
                base=base,
                month_id="2026-07",
                as_of="2026-07-15",
                validation_mode="open",
            )

            self.assertIn("monthly_photo_directory_missing:2026-07", payload["blockers"])
            self.assertEqual(payload["verdict"], "BLOCKED")

    def test_open_close_windows_for_four_and_five_week_months(self):
        cases = [
            ("2026-06", 4, "2026-06-01", "2026-06-28"),
            ("2026-07", 5, "2026-06-29", "2026-08-02"),
        ]
        for month_id, week_count, start, end in cases:
            with self.subTest(month_id=month_id):
                open_window = validator.build_validation_window(
                    month_id,
                    as_of_date=date.fromisoformat(start),
                    validation_mode="open",
                )
                close_window = validator.build_validation_window(
                    month_id,
                    as_of_date=date.fromisoformat(end) + timedelta(days=1),
                    validation_mode="close",
                )

                self.assertEqual(open_window["operational_coverage_start"], start)
                self.assertEqual(open_window["operational_coverage_end"], end)
                self.assertEqual(close_window["operational_coverage_end"], end)
                self.assertEqual(len(validator.month_contract.operational_weeks_for_month(month_id)), week_count)

    def test_reference_manifest_absent_is_pending_in_open_but_blocks_close(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            monthly = base / "data" / "kpione_photo_reports" / "2026-07"
            monthly.mkdir(parents=True)
            write_photo(monthly / "photo-excel-admin_1.xlsx", fechas=["2026-07-06"])
            open_payload = validator.build_validation_payload(
                base=base,
                month_id="2026-07",
                as_of="2026-07-06",
                validation_mode="open",
            )
            close_payload = validator.build_validation_payload(
                base=base,
                month_id="2026-07",
                as_of="2026-08-03",
                validation_mode="close",
            )

            self.assertNotIn("reference_manifest_required", open_payload["blockers"])
            self.assertEqual(open_payload["photo_reports"]["role_counts"], {"unclassified": 1})
            self.assertIn("reference_manifest_required", close_payload["blockers"])

    def test_hash_mismatch_against_manifest_blocks(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            monthly = base / "data" / "kpione_photo_reports" / "2026-06"
            monthly.mkdir(parents=True)
            path = monthly / "photo-excel-admin_1.xlsx"
            write_photo(path)
            manifest_path = base / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    minimal_manifest(
                        photos=[
                            {
                                "source_file_id": "1",
                                "source_file_name": path.name,
                                "relative_path": "data/kpione_photo_reports/2026-06/photo-excel-admin_1.xlsx",
                                "sha256": "0" * 64,
                                "size_bytes": path.stat().st_size,
                                "role": "include_candidate",
                            }
                        ]
                    )
                ),
                encoding="utf-8",
            )
            payload = validator.build_validation_payload(
                base=base,
                month_id="2026-06",
                reference_manifest_path=str(manifest_path),
            )
            self.assertIn("photo_sha256_mismatch:photo-excel-admin_1.xlsx", payload["blockers"])

    def test_ruta_folder_absent_and_ambiguous_block(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            payload = validator.build_validation_payload(base=base, month_id="2026-06")
            self.assertIn("ruta_month_directory_missing", payload["blockers"])

            root = base / "data" / "RUTA_RUTERO"
            (root / "06 - JUNIO").mkdir(parents=True)
            (root / "06 - JUNIO COPIA").mkdir()
            payload = validator.build_validation_payload(base=base, month_id="2026-06")
            self.assertTrue(
                any(item.startswith("ruta_month_directory_ambiguous:") for item in payload["blockers"])
            )

    def test_ruta_required_sheet_is_explicit(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            ruta = base / "data" / "RUTA_RUTERO" / "06 - JUNIO"
            ruta.mkdir(parents=True)
            route_path = ruta / "RUTA_RUTEROS_JUNIO_S1.xlsx"
            write_route(route_path, sheet="RUTA_RUTERO")
            manifest_path = base / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    minimal_manifest(
                        routes=[
                            {
                                "source_file_name": route_path.name,
                                "relative_path": "data/RUTA_RUTERO/06 - JUNIO/RUTA_RUTEROS_JUNIO_S1.xlsx",
                                "sha256": validator.sha256_file(route_path),
                                "size_bytes": route_path.stat().st_size,
                                "row_count": 1,
                                "week_label": "S1",
                                "week_start": "2026-06-01",
                                "assigned_operational_month": "2026-06",
                            }
                        ]
                    )
                ),
                encoding="utf-8",
            )
            payload = validator.build_validation_payload(
                base=base,
                month_id="2026-06",
                reference_manifest_path=str(manifest_path),
            )
            self.assertIn(
                "ruta_required_sheet_missing:RUTA_RUTEROS_JUNIO_S1.xlsx:RUTA RUTERO",
                payload["blockers"],
            )

    def test_ruta_week_start_comes_from_calendar_contract_without_manifest_week_start(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            ruta = base / "data" / "RUTA_RUTERO" / "06 - JUNIO"
            ruta.mkdir(parents=True)
            route_path = ruta / "RUTA_RUTEROS_JUNIO_S1.xlsx"
            write_route(route_path)
            manifest_path = base / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    minimal_manifest(
                        routes=[
                            {
                                "source_file_name": route_path.name,
                                "relative_path": "data/RUTA_RUTERO/06 - JUNIO/RUTA_RUTEROS_JUNIO_S1.xlsx",
                                "sha256": validator.sha256_file(route_path),
                                "size_bytes": route_path.stat().st_size,
                                "row_count": 1,
                                "week_label": "S1",
                                "assigned_operational_month": "2026-06",
                            }
                        ]
                    )
                ),
                encoding="utf-8",
            )
            payload = validator.build_validation_payload(
                base=base,
                month_id="2026-06",
                reference_manifest_path=str(manifest_path),
            )
            self.assertEqual(payload["ruta_rutero"]["files"][0]["week_start"], "2026-06-01")
            self.assertNotIn("route_week_start_not_declared:RUTA_RUTEROS_JUNIO_S1.xlsx", payload["blockers"])

    def test_june_s1_s4_mappings_are_validated(self):
        routes = [
            {"week_label": "S1", "week_start": "2026-06-01", "assigned_operational_month": "2026-06"},
            {"week_label": "S2", "week_start": "2026-06-08", "assigned_operational_month": "2026-06"},
            {"week_label": "S3", "week_start": "2026-06-15", "assigned_operational_month": "2026-06"},
            {"week_label": "S4", "week_start": "2026-06-22", "assigned_operational_month": "2026-06"},
        ]
        blockers, missing = validator.validate_route_mappings(
            month_id="2026-06",
            route_files=routes,
            expected_route_mapping=validator.month_contract.route_week_mapping_for_month("2026-06"),
        )
        self.assertEqual(blockers, [])
        self.assertEqual(missing, [])

    def test_cli_writes_outputs_with_soft_exit(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            out = base / "out.json"
            md = base / "out.md"
            code = validator.main(
                [
                    "--month",
                    "2026-07",
                    "--base",
                    str(base),
                    "--json-out",
                    str(out),
                    "--md-out",
                    str(md),
                    "--soft-exit",
                ]
            )
            self.assertEqual(code, 0)
            payload = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(payload["verdict"], "BLOCKED")
            self.assertTrue(md.exists())

    def test_static_safety_no_forbidden_imports_or_productive_route_loader(self):
        source = (SCRIPTS / "validate_kpione_monthly_input_015B_no_apply.py").read_text(
            encoding="utf-8"
        )
        forbidden = [
            "import supabase",
            "from supabase",
            "import psycopg",
            "import psycopg2",
            "import sqlalchemy",
            "from sqlalchemy",
            "import load_ruta_rutero_from_excel",
            "from load_ruta_rutero_from_excel",
        ]
        for token in forbidden:
            with self.subTest(token=token):
                self.assertNotIn(token, source)

    def test_guardrails_no_db_sql_ddl_data_movement(self):
        with tempfile.TemporaryDirectory() as raw:
            payload = validator.build_validation_payload(base=Path(raw), month_id="2026-06")
            guardrails = payload["guardrails"]
            self.assertFalse(guardrails["db_access"]["used"])
            self.assertFalse(guardrails["supabase_used"])
            self.assertFalse(guardrails["sql_apply"])
            self.assertFalse(guardrails["ddl"])
            self.assertFalse(guardrails["productive_loader_run"])
            self.assertFalse(guardrails["data_movement"])


if __name__ == "__main__":
    unittest.main()
