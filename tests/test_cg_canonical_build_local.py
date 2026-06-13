from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from scripts import cg_canonical_build_local as builder


class CanonicalBuildLocalTests(unittest.TestCase):
    def write_context(self, payload: dict) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "context.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def minimal_context(self) -> dict:
        return {
            "build_context": {
                "build_id": "TEST_BUILD",
                "affected_date_start": "2026-05-18",
                "affected_date_end": "2026-05-24",
                "affected_weeks": ["2026-05-18"],
                "source_precedence_version": "CG_SOURCE_PRECEDENCE_V2_KPIONE2_POWER_APP_KPIONE_AUDIT",
                "daily_builder_version": "CG_DAILY_CANONICAL_PINNED_RAW_ROUTE_V1",
                "weekly_builder_version": "CG_WEEKLY_FROM_CANONICAL_DAILY_ROUTE_V1",
            },
            "raw_lineage": [
                {
                    "source_key": "KPIONE2",
                    "batch_id": 38,
                    "source_sheet": "DB (KPIONE2.0)",
                    "loaded_rows": 1,
                    "status": "ok",
                    "loader_name": "load_control_gestion_raw_v17",
                    "source_type": "CORRECTION",
                    "snapshot_hash": "abc",
                    "selection_reason": "pinned test batch",
                }
            ],
            "route_lineage": {
                "weeks": [
                    {
                        "week_start": "2026-05-18",
                        "source_ruta_batch_ids": [13],
                        "route_week_snapshot_version_id": "ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1|TEST|2026-05-18",
                        "surface_hash": "def",
                    }
                ]
            },
        }

    def test_parse_build_context(self) -> None:
        context = builder.load_build_context(self.write_context(self.minimal_context()))
        self.assertEqual(context.build_id, "TEST_BUILD")
        self.assertEqual(context.raw_lineage[0].source_key, "KPIONE2")
        self.assertEqual(context.route_weeks[0].source_ruta_batch_ids, (13,))

    def test_forbidden_latest_token_blocks_context(self) -> None:
        payload = self.minimal_context()
        payload["build_context"]["raw_batch_selection_reason"] = "use latest batch"
        with self.assertRaises(builder.BuilderBlock):
            builder.load_build_context(self.write_context(payload))

    def test_local_dsn_allowed(self) -> None:
        builder.assert_local_postgres_dsn("postgresql://user:pw@127.0.0.1:55432/db")
        builder.assert_local_postgres_dsn("postgresql://user:pw@localhost:5432/db")

    def test_supabase_dsn_rejected(self) -> None:
        with self.assertRaises(builder.BuilderBlock):
            builder.assert_local_postgres_dsn("postgresql://u:p@db.project.supabase.co:5432/postgres")

    def test_non_local_connection_rejected(self) -> None:
        with self.assertRaises(builder.BuilderBlock):
            builder.assert_local_postgres_dsn("postgresql://u:p@10.1.2.3:5432/postgres")

    def test_route_weekly_retroactive_winner_keeps_missing_older_keys(self) -> None:
        rows = [
            {"ruta_batch_id": 15, "cod_rt": "A", "cliente_norm": "ONE"},
            {"ruta_batch_id": 15, "cod_rt": "B", "cliente_norm": "TWO"},
            {"ruta_batch_id": 18, "cod_rt": "A", "cliente_norm": "ONE"},
        ]
        winners = builder.select_route_winners(rows, "2026-06-01", [15, 18])
        keys = {(row["cod_rt"], row["cliente_norm"], row["ruta_batch_id"]) for row in winners}
        self.assertEqual(keys, {("A", "ONE", 18), ("B", "TWO", 15)})

    def test_route_unique_key_detection_fixture(self) -> None:
        rows = [
            {"ruta_batch_id": 13, "cod_rt": "A", "cliente_norm": "ONE"},
            {"ruta_batch_id": 13, "cod_rt": "A", "cliente_norm": "ONE"},
        ]
        winners = builder.select_route_winners(rows, "2026-05-18", [13])
        keys = [(row["cod_rt"], row["cliente_norm"]) for row in winners]
        self.assertNotEqual(len(keys), len(set(keys)))

    def test_kpione2_only_precedence(self) -> None:
        metrics = builder.summarize_source_day([{"fuente": "KPIONE2", "persona_norm": "REP"}], "REP")
        self.assertEqual(metrics["fuente_ganadora"], "KPIONE2")
        self.assertEqual(metrics["useful_day"], 1)
        self.assertEqual(metrics["power_app_fallback"], 0)

    def test_power_app_fallback(self) -> None:
        metrics = builder.summarize_source_day([{"fuente": "POWER_APP"}])
        self.assertEqual(metrics["fuente_ganadora"], "POWER_APP")
        self.assertEqual(metrics["power_app_fallback"], 1)

    def test_kpione2_beats_power_app_overlap(self) -> None:
        metrics = builder.summarize_source_day([{"fuente": "POWER_APP"}, {"fuente": "KPIONE2"}])
        self.assertEqual(metrics["fuente_ganadora"], "KPIONE2")
        self.assertEqual(metrics["multisource_overlap"], 1)

    def test_kpione_is_audit_only(self) -> None:
        metrics = builder.summarize_source_day([{"fuente": "KPIONE"}])
        self.assertIsNone(metrics["fuente_ganadora"])
        self.assertEqual(metrics["kpione1_audit_only"], 1)
        self.assertEqual(metrics["useful_day"], 0)

    def test_same_source_double_mark(self) -> None:
        metrics = builder.summarize_source_day([{"fuente": "KPIONE2"}, {"fuente": "KPIONE2"}])
        self.assertEqual(metrics["same_source_multimark"], 1)
        self.assertEqual(metrics["kpione2_rows_dia"], 2)

    def test_triple_source_overlap(self) -> None:
        metrics = builder.summarize_source_day(
            [{"fuente": "KPIONE2"}, {"fuente": "POWER_APP"}, {"fuente": "KPIONE"}]
        )
        self.assertEqual(metrics["multisource_overlap"], 1)
        self.assertEqual(metrics["triple_source_overlap"], 1)

    def test_evidence_outside_route_is_marked(self) -> None:
        metrics = builder.summarize_source_day([{"fuente": "KPIONE2", "registro_fuera_cruce": "SI"}])
        self.assertEqual(metrics["registro_fuera_cruce"], "FUERA_CRUCE")

    def test_route_without_evidence_weekly_metrics(self) -> None:
        weekly = builder.summarize_week(plan_visits=3, realized_visits=0)
        self.assertEqual(weekly["VISITAS_PENDIENTES_CALC"], 3)
        self.assertEqual(weekly["ALERTA"], "INCUMPLE")

    def test_visit_cap_pending_and_alert(self) -> None:
        weekly = builder.summarize_week(plan_visits=3, realized_visits=5)
        self.assertEqual(weekly["VISITA_REALIZADA_CAP"], 3)
        self.assertEqual(weekly["SOBRE_CUMPLIMIENTO"], 2)
        self.assertEqual(weekly["VISITAS_PENDIENTES_CALC"], 0)
        self.assertEqual(weekly["ALERTA"], "CUMPLE")

    def test_hashes_are_deterministic(self) -> None:
        rows_a = [{"k": "2", "v": "b"}, {"k": "1", "v": "a"}]
        rows_b = list(reversed(rows_a))
        self.assertEqual(
            builder.deterministic_rows_hash(rows_a, ["k", "v"], ["k"]),
            builder.deterministic_rows_hash(rows_b, ["k", "v"], ["k"]),
        )


if __name__ == "__main__":
    unittest.main()
