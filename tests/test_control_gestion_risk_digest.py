import unittest
from unittest import mock

import pandas as pd

from app.screens import control_gestion as cg


class _MetricColumn:
    def __init__(self, sink):
        self._sink = sink

    def metric(self, label, value):
        self._sink.append((label, value))


class _StreamlitStub:
    def __init__(self):
        self.markdowns = []
        self.captions = []
        self.metrics = []
        self.dataframes = []
        self.infos = []

    def markdown(self, text, **_kwargs):
        self.markdowns.append(text)

    def caption(self, text):
        self.captions.append(text)

    def columns(self, count, **_kwargs):
        if isinstance(count, int):
            total = count
        else:
            total = len(count)
        return [_MetricColumn(self.metrics) for _ in range(total)]

    def dataframe(self, df, **_kwargs):
        self.dataframes.append(df.copy(deep=True))

    def info(self, text):
        self.infos.append(text)


def _competitive_rows(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "DIMENSION",
            "% cumplimiento",
            "Visitas exigidas",
            "Visitas válidas",
            "Visitas pendientes",
            "Rutas cumplen",
            "Rutas incumplen",
            "Sobrecumplimiento",
            "Gestión compartida",
            "Visitas reportadas",
        ],
    )


class ControlGestionRiskDigestTests(unittest.TestCase):
    def test_normal_scope_with_complete_metrics(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={
                "visita_plan": 20,
                "visita_realizada_cap": 15,
                "visitas_pendientes": 5,
                "incumple_rows": 2,
            },
            competitive_summary=_competitive_rows([
                ["Ruta A", 75.0, 20, 15, 5, 1, 2, 0, 0, 15],
            ]),
            active_filters={"semana": "2026-07-13", "gestor": "Ana", "vista": "Por rutero"},
        )

        self.assertEqual(digest["metrics"]["exigidas"], 20)
        self.assertEqual(digest["metrics"]["realizadas"], 15)
        self.assertEqual(digest["metrics"]["pendientes"], 5)
        self.assertEqual(digest["metrics"]["alertas"], 2)
        self.assertEqual(digest["metrics"]["cumplimiento_pct"], 75.0)
        self.assertEqual(digest["context"]["semana"], "2026-07-13")
        self.assertEqual(digest["focus"].iloc[0]["entidad"], "Ruta A")

    def test_priority_focus_order_is_deterministic(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={},
            competitive_summary=_competitive_rows([
                ["Ruta C", 80.0, 9, 7, 2, 1, 1, 0, 0, 7],
                ["Ruta A", 60.0, 7, 4, 3, 0, 2, 0, 0, 4],
                ["Ruta B", 50.0, 20, 10, 9, 0, 2, 0, 0, 10],
            ]),
            active_filters={},
        )

        self.assertEqual(digest["focus"]["entidad"].tolist(), ["Ruta B", "Ruta A", "Ruta C"])

    def test_selected_rutero_filters_focus_table_exactly(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={},
            competitive_summary=_competitive_rows([
                ["Ruta A", 70.0, 10, 7, 3, 1, 2, 0, 0, 7],
                ["Ruta B", 80.0, 20, 16, 4, 1, 1, 0, 0, 16],
            ]),
            active_filters={"foco": "Ruta B", "specific_focus": True},
        )

        self.assertEqual(digest["focus"]["entidad"].tolist(), ["Ruta B"])
        self.assertTrue(digest["respects_current_selection"])
        self.assertEqual(digest["context"]["foco"], "Ruta B")

    def test_selected_cliente_filters_focus_table_exactly(self):
        competitive = pd.DataFrame({
            "CLIENTE": ["Cliente Uno", "Cliente Dos"],
            "% cumplimiento": [50.0, 90.0],
            "Visitas exigidas": [10, 20],
            "Visitas pendientes": [5, 2],
            "Rutas incumplen": [3, 1],
        })

        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={},
            competitive_summary=competitive,
            active_filters={"foco": "Cliente Dos", "specific_focus": True},
        )

        self.assertEqual(digest["focus"]["entidad"].tolist(), ["Cliente Dos"])
        self.assertTrue(digest["respects_current_selection"])

    def test_missing_selected_focus_does_not_show_other_entities(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={},
            competitive_summary=_competitive_rows([
                ["Ruta A", 70.0, 10, 7, 3, 1, 2, 0, 0, 7],
            ]),
            active_filters={"foco": "Ruta X", "specific_focus": True},
        )

        self.assertTrue(digest["focus"].empty)
        self.assertEqual(digest["status"], "selected_focus_not_in_ranking")
        self.assertTrue(digest["respects_current_selection"])

    def test_tie_breaks_by_alert_pending_required_and_name(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={},
            competitive_summary=_competitive_rows([
                ["Beta", 90.0, 10, 8, 4, 1, 2, 0, 0, 8],
                ["Alpha", 70.0, 10, 7, 4, 1, 2, 0, 0, 7],
                ["Gamma", 60.0, 12, 8, 4, 1, 2, 0, 0, 8],
            ]),
            active_filters={},
        )

        self.assertEqual(digest["focus"]["entidad"].tolist(), ["Gamma", "Alpha", "Beta"])

    def test_empty_dataframe_returns_empty_focus(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={},
            competitive_summary=pd.DataFrame(),
            active_filters={},
        )

        self.assertTrue(digest["focus"].empty)
        self.assertIn("ranking_rows", digest["missing_columns"])

    def test_missing_optional_columns_fail_closed_without_exception(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={},
            competitive_summary=pd.DataFrame({"DIMENSION": ["Ruta A"], "Visitas pendientes": [1]}),
            active_filters={},
        )

        self.assertTrue(digest["focus"].empty)
        self.assertIn("Rutas incumplen", digest["missing_columns"])
        self.assertIn("Visitas exigidas", digest["missing_columns"])

    def test_null_values_are_normalized(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={
                "visita_plan": None,
                "visita_realizada_cap": None,
                "visitas_pendientes": None,
                "incumple_rows": None,
            },
            competitive_summary=_competitive_rows([
                [None, None, None, None, None, None, None, None, None, None],
            ]),
            active_filters={"semana": None},
        )

        self.assertEqual(digest["metrics"]["cumplimiento_pct"], 0.0)
        self.assertEqual(digest["focus"].iloc[0]["entidad"], "Sin dato")
        self.assertEqual(digest["context"]["semana"], "Sin semana")

    def test_zero_denominator_is_safe(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={"visita_plan": 0, "visita_realizada_cap": 7},
            competitive_summary=None,
            active_filters={},
        )

        self.assertEqual(digest["metrics"]["cumplimiento_pct"], 0.0)

    def test_focus_is_limited_to_five(self):
        rows = [
            [f"Ruta {idx}", 50.0, idx + 1, 1, idx, 0, idx, 0, 0, 1]
            for idx in range(8)
        ]

        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={},
            competitive_summary=_competitive_rows(rows),
            active_filters={},
            top_n=5,
        )

        self.assertEqual(len(digest["focus"]), 5)
        self.assertEqual(digest["focus"].iloc[0]["entidad"], "Ruta 7")
        self.assertEqual(digest["context"]["foco"], "Todos")

    def test_input_dataframe_is_not_mutated(self):
        source = _competitive_rows([
            ["Ruta A", "75,5%", "20", "15", "5", "1", "2", "0", "0", "15"],
        ])
        before = source.copy(deep=True)

        cg.build_control_gestion_risk_digest(
            scope_metrics={},
            competitive_summary=source,
            active_filters={},
        )

        pd.testing.assert_frame_equal(source, before)

    def test_input_dataframe_is_not_mutated_when_filtering_focus(self):
        source = _competitive_rows([
            ["Ruta A", "75,5%", "20", "15", "5", "1", "2", "0", "0", "15"],
            ["Ruta B", "50,0%", "10", "5", "5", "0", "1", "0", "0", "5"],
        ])
        before = source.copy(deep=True)

        cg.build_control_gestion_risk_digest(
            scope_metrics={},
            competitive_summary=source,
            active_filters={"foco": "Ruta B", "specific_focus": True},
        )

        pd.testing.assert_frame_equal(source, before)

    def test_local_view_without_ranking_has_explicit_status(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={"visita_plan": 10, "visita_realizada_cap": 8},
            competitive_summary=pd.DataFrame(),
            active_filters={
                "vista": "Por local",
                "foco": "Local 123",
                "specific_focus": True,
                "ranking_available": False,
            },
        )

        self.assertEqual(digest["status"], "local_ranking_unavailable")
        self.assertTrue(digest["focus"].empty)
        self.assertEqual(digest["context"]["foco"], "Local 123")
        self.assertTrue(digest["respects_current_selection"])

    def test_render_integration_uses_streamlit_without_db(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={"visita_plan": 10, "visita_realizada_cap": 8},
            competitive_summary=_competitive_rows([
                ["Ruta A", 80.0, 10, 8, 2, 1, 1, 0, 0, 8],
            ]),
            active_filters={"semana": "2026-07-13", "vista": "Por rutero"},
        )
        st_stub = _StreamlitStub()

        with mock.patch.object(cg, "st", st_stub):
            cg._cg_v2_render_risk_digest(digest)

        self.assertIn("#### Resumen de riesgo operacional", st_stub.markdowns)
        self.assertEqual(len(st_stub.metrics), 0)
        self.assertTrue(any("Cumplimiento 80,0%" in caption for caption in st_stub.captions))
        self.assertEqual(len(st_stub.dataframes), 1)
        self.assertEqual(st_stub.dataframes[0].iloc[0]["Entidad"], "Ruta A")

    def test_render_local_view_without_ranking_uses_specific_info(self):
        digest = cg.build_control_gestion_risk_digest(
            scope_metrics={"visita_plan": 10, "visita_realizada_cap": 8},
            competitive_summary=pd.DataFrame(),
            active_filters={
                "vista": "Por local",
                "foco": "Local 123",
                "specific_focus": True,
                "ranking_available": False,
            },
        )
        st_stub = _StreamlitStub()

        with mock.patch.object(cg, "st", st_stub):
            cg._cg_v2_render_risk_digest(digest)

        self.assertEqual(len(st_stub.metrics), 0)
        self.assertFalse(st_stub.dataframes)
        self.assertIn(
            "La vista por local no dispone de ranking competitivo; el resumen se limita al foco seleccionado.",
            st_stub.infos,
        )


if __name__ == "__main__":
    unittest.main()
