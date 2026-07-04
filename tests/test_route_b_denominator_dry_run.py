import ast
import contextlib
import copy
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import validate_route_b_denominator_dry_run as validator


def photo_row(
    event_id: str,
    fecha: str,
    cod_rt: object = "100",
    cliente_norm: object = "Marca A",
    photo_count: object = "1/1",
    *,
    sp_item_id: str | None = None,
    link: str | None = None,
) -> dict:
    suffix = event_id if link is None else link
    return {
        "ID": event_id,
        "SP Item ID": sp_item_id or f"SP-{event_id}",
        "Holding": "Holding",
        "Subcadena": "Subcadena",
        "Codigo Local": cod_rt,
        "Marca": cliente_norm,
        "Local": "Local",
        "Direccion": "Direccion",
        "Reponedor": "Repo",
        "Fecha": fecha,
        "Fecha de subida": f"{fecha} 09:05",
        "Hora": "09:00",
        "Tipo de Tarea": "Tarea",
        "Foto N/Total": photo_count,
        "Comentarios": "OK",
        "Link Foto": f"https://fixture/{suffix}",
    }


def route_row(
    week: str,
    cod_rt: object = "100",
    cliente_norm: object = "Marca A",
    exigidas: object = 2,
    existing: list[str] | None = None,
) -> dict:
    return {
        "semana_inicio": week,
        "cod_rt": cod_rt,
        "cliente_norm": cliente_norm,
        "EXIGIDAS": exigidas,
        "existing_presence_dates": list(existing or []),
    }


class RouteBDenominatorDryRunTests(unittest.TestCase):
    def test_three_events_same_day_produce_one_structural_day_presence(self):
        payload = validator.validate_local_sample(
            {
                "route_rows": [route_row("2026-06-15")],
                "photo_rows": [
                    photo_row("E1", "2026-06-17"),
                    photo_row("E2", "2026-06-17"),
                    photo_row("E3", "2026-06-17"),
                ],
            }
        )

        grain = payload["grain_invariant"]
        self.assertEqual(payload["verdict"], validator.PASS_VERDICT)
        self.assertEqual(grain["photo_rows"], 3)
        self.assertEqual(grain["event_rows"], 3)
        self.assertEqual(grain["day_presence_rows"], 1)
        self.assertEqual(grain["max_events_per_day_presence"], 3)
        self.assertTrue(grain["event_row_to_day_presence_structural_pass"])
        self.assertFalse(grain["day_presence_constant_check_used_as_proof"])

    def test_one_photo_per_event_is_legitimate_and_does_not_false_block(self):
        payload = validator.validate_local_sample(
            {
                "route_rows": [route_row("2026-06-15")],
                "photo_rows": [
                    photo_row("E1", "2026-06-17"),
                    photo_row("E2", "2026-06-18"),
                ],
            }
        )

        grain = payload["grain_invariant"]
        self.assertEqual(payload["verdict"], validator.PASS_VERDICT)
        self.assertTrue(grain["photo_rows_equal_event_rows"])
        self.assertFalse(grain["legacy_aggregate_inequality_observed"])
        self.assertFalse(grain["legacy_aggregate_inequality_used_as_proof"])
        self.assertFalse(
            grain["imported_loader_advisory"]["legacy_forbidden_assumption_rejected"]
        )
        self.assertTrue(grain["photo_row_to_event_row_structural_pass"])

    def test_blank_cod_rt_or_cliente_norm_blocks(self):
        cases = [
            ("photo_cod_rt", "photo_rows", "Codigo Local", "   ", "no_blank_cod_rt"),
            ("photo_cliente", "photo_rows", "Marca", None, "no_blank_cliente_norm"),
            ("route_cod_rt", "route_rows", "cod_rt", "", "no_blank_cod_rt"),
            (
                "route_cliente",
                "route_rows",
                "cliente_norm",
                " ",
                "no_blank_cliente_norm",
            ),
        ]
        base = {
            "route_rows": [route_row("2026-06-15")],
            "photo_rows": [photo_row("E1", "2026-06-17")],
        }
        for name, section, field, value, flag in cases:
            with self.subTest(name=name):
                sample = copy.deepcopy(base)
                sample[section][0][field] = value
                payload = validator.validate_local_sample(sample)
                self.assertEqual(payload["verdict"], validator.BLOCK_VERDICT)
                self.assertFalse(payload["blocking_flags"][flag])
                self.assertIn(flag, payload["blocking_flags"]["blocking_reasons"])

    def test_golden_route_photo_reconciliation_changes_only_numerator(self):
        payload = validator.validate_local_sample(
            {
                "route_rows": [
                    route_row(
                        "2026-06-15",
                        cod_rt="100",
                        cliente_norm="Marca A",
                        exigidas=2,
                        existing=["2026-06-16"],
                    ),
                    route_row(
                        "2026-06-22",
                        cod_rt="200",
                        cliente_norm="Marca B",
                        exigidas=1,
                    ),
                ],
                "photo_rows": [
                    photo_row("E1", "2026-06-17"),
                    photo_row("E2", "2026-06-17"),
                    photo_row("E3", "2026-06-17"),
                    photo_row("E4", "2026-06-22", "200", "Marca B"),
                ],
            }
        )

        reconciliation = payload["denominator_reconciliation"]
        first = reconciliation["rows"][0]
        self.assertEqual(payload["verdict"], validator.PASS_VERDICT)
        self.assertEqual(
            set(payload["metrics_definition"]).intersection(validator.PROTECTED_METRICS),
            set(validator.PROTECTED_METRICS),
        )
        self.assertTrue(reconciliation["denominator_delta_zero"])
        self.assertTrue(reconciliation["only_numerator_metrics_changed"])
        self.assertEqual(first["delta"]["EXIGIDAS"], 0)
        self.assertEqual(first["delta"]["VISITA"], 0)
        self.assertEqual(first["delta"]["VISITA_REALIZADA"], 1)
        self.assertEqual(first["delta"]["VISITA_REALIZADA_RAW"], 1)
        self.assertEqual(first["delta"]["VISITA_REALIZADA_CAP"], 1)
        self.assertEqual(first["delta"]["PENDIENTE"], -1)
        self.assertEqual(
            first["delta"]["ALERTA"],
            {"before": "INCUMPLE", "after": "CUMPLE", "changed": True},
        )

    def test_sunday_and_monday_map_to_explicit_monday_week_starts(self):
        payload = validator.validate_local_sample(
            {
                "route_rows": [
                    route_row("2026-06-15"),
                    route_row("2026-06-22"),
                ],
                "photo_rows": [
                    photo_row("E-SUN", "2026-06-21"),
                    photo_row("E-MON", "2026-06-22"),
                ],
            }
        )

        records = payload["grain_invariant"]["day_presence_records"]
        self.assertEqual(payload["verdict"], validator.PASS_VERDICT)
        self.assertEqual(
            [(row["fecha"], row["semana_inicio"]) for row in records],
            [
                ("2026-06-21", "2026-06-15"),
                ("2026-06-22", "2026-06-22"),
            ],
        )
        self.assertTrue(payload["blocking_flags"]["route_week_start_is_monday"])

    def test_route_and_photo_keys_share_loader_normalization(self):
        payload = validator.validate_local_sample(
            {
                "route_rows": [
                    route_row(
                        "2026-06-15",
                        cod_rt="00100",
                        cliente_norm="MARCA A",
                    )
                ],
                "photo_rows": [
                    photo_row(
                        "E1",
                        "2026-06-17",
                        cod_rt="  00100  ",
                        cliente_norm="  MárCa   Á  ",
                    )
                ],
            }
        )

        presence = payload["grain_invariant"]["day_presence_records"][0]
        self.assertEqual(payload["verdict"], validator.PASS_VERDICT)
        self.assertEqual(presence["cod_rt"], "00100")
        self.assertEqual(presence["cliente_norm"], "MARCA A")
        self.assertTrue(
            payload["blocking_flags"]["no_unmatched_photo_day_presence"]
        )

    def test_one_event_crossing_sunday_monday_blocks_multi_week(self):
        payload = validator.validate_local_sample(
            {
                "route_rows": [
                    route_row("2026-06-15"),
                    route_row("2026-06-22"),
                ],
                "photo_rows": [
                    photo_row(
                        "E1",
                        "2026-06-21",
                        photo_count="1/2",
                        link="E1-1",
                    ),
                    photo_row(
                        "E1",
                        "2026-06-22",
                        photo_count="2/2",
                        link="E1-2",
                    ),
                ],
            }
        )

        self.assertEqual(payload["verdict"], validator.BLOCK_VERDICT)
        self.assertFalse(payload["blocking_flags"]["loader_no_event_ids_multi_week"])
        self.assertFalse(
            payload["blocking_flags"]["event_row_to_day_presence_structural_pass"]
        )

    def test_photo_number_total_anomalies_and_exact_duplicate_block(self):
        duplicate = photo_row("E1", "2026-06-17", photo_count="1/2")
        cases = {
            "zero_total": [photo_row("E1", "2026-06-17", photo_count="0/0")],
            "missing_total": [photo_row("E1", "2026-06-17", photo_count="")],
            "sequence_above_total": [
                photo_row("E1", "2026-06-17", photo_count="1/2", link="1"),
                photo_row("E1", "2026-06-17", photo_count="2/2", link="2"),
                photo_row("E1", "2026-06-17", photo_count="3/2", link="3"),
            ],
            "exact_duplicate": [duplicate, copy.deepcopy(duplicate)],
        }
        expected_false_flag = {
            "zero_total": "positive_photo_count_totals",
            "missing_total": "no_missing_photo_count_total",
            "sequence_above_total": "photo_sequence_not_above_total",
            "exact_duplicate": "no_exact_duplicate_photo_rows",
        }
        for name, photos in cases.items():
            with self.subTest(name=name):
                payload = validator.validate_local_sample(
                    {
                        "route_rows": [route_row("2026-06-15")],
                        "photo_rows": photos,
                    }
                )
                self.assertEqual(payload["verdict"], validator.BLOCK_VERDICT)
                self.assertFalse(
                    payload["blocking_flags"][expected_false_flag[name]]
                )

    def test_payload_declares_no_db_sql_or_writes(self):
        payload = validator.validate_local_sample(
            {
                "route_rows": [route_row("2026-06-15")],
                "photo_rows": [photo_row("E1", "2026-06-17")],
            }
        )

        self.assertEqual(payload["mode"], "ORANGE_NO_APPLY_DRY_RUN")
        self.assertEqual(payload["db_access"], {"used": False})
        self.assertFalse(payload["sql_apply"])
        self.assertFalse(payload["writes_executed"])

    def test_cli_reads_local_fixture_and_emits_structured_json(self):
        sample = {
            "route_rows": [route_row("2026-06-15")],
            "photo_rows": [photo_row("E1", "2026-06-17")],
        }
        with tempfile.TemporaryDirectory() as raw:
            input_path = Path(raw) / "fixture.json"
            input_path.write_text(json.dumps(sample), encoding="utf-8")
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = validator.main(["--input-json", str(input_path)])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["verdict"], validator.PASS_VERDICT)
        self.assertEqual(payload["db_access"], {"used": False})
        self.assertFalse(payload["sql_apply"])
        self.assertFalse(payload["writes_executed"])

    def test_validator_imports_no_db_clients_or_productive_loaders(self):
        path = ROOT / "scripts" / "validate_route_b_denominator_dry_run.py"
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)

        forbidden_modules = {
            "psycopg",
            "psycopg2",
            "sqlalchemy",
            "supabase",
            "load_control_gestion_raw_v17",
            "load_fact_from_excel",
            "load_ruta_rutero_from_excel",
            "refresh_control_gestion_v2_incremental",
            "refresh_control_gestion_v2_mv",
        }
        self.assertTrue(imported.isdisjoint(forbidden_modules))
        self.assertIn("load_kpione2_photo_from_excel", imported)
        self.assertNotIn("DB_URL", source)
        self.assertNotIn(".write_text(", source)


if __name__ == "__main__":
    unittest.main()
