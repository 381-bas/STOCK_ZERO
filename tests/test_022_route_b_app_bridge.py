from __future__ import annotations

import unittest
from pathlib import Path

from scripts import refresh_control_gestion_v2_incremental as refresh


ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql" / "18_control_gestion_route_b_app_bridge_v1.sql"
APPLIER = ROOT / "scripts" / "apply_control_gestion_route_b_bridge.py"
WRAPPER = ROOT / "scripts" / "invoke_stock_zero_db_operation.ps1"


class RouteBAppBridgeContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sql = SQL.read_text(encoding="utf-8")
        cls.normalized = " ".join(cls.sql.lower().split())

    def test_incremental_refresh_uses_route_b_bridge(self) -> None:
        self.assertEqual(
            refresh.DAILY_SOURCE,
            "cg_core.v_cg_visita_dia_precedencia_route_b_v1",
        )

    def test_bridge_uses_route_b_presence_and_existing_denominator(self) -> None:
        self.assertIn("from cg_core.kpione_day_presence_v1 d", self.normalized)
        self.assertIn("join cg_core.v_rr_frecuencia_base_resuelta_v2 rr", self.normalized)
        self.assertIn("rr.effective_week_start = date_trunc('week', d.fecha)::date", self.normalized)
        self.assertNotIn("cumplimiento_frecuencia", self.normalized)

    def test_bridge_preserves_precedence_contract(self) -> None:
        self.assertIn("from cg_core.v_cg_visita_dia_precedencia_v2", self.normalized)
        self.assertIn("then 'kpione2'", self.normalized)
        self.assertIn("else l.fuente_ganadora", self.normalized)
        self.assertIn("as power_app_fallback", self.normalized)
        self.assertIn("as kpione1_audit_only", self.normalized)

    def test_bridge_caps_day_presence_and_does_not_touch_legacy_raw(self) -> None:
        self.assertIn("as useful_day", self.normalized)
        self.assertNotIn("insert into cg_raw.kpione2_raw", self.normalized)
        self.assertNotIn("update cg_raw.kpione2_raw", self.normalized)
        self.assertNotIn("delete from cg_raw.kpione2_raw", self.normalized)
        self.assertNotIn("truncate cg_raw.kpione2_raw", self.normalized)

    def test_readonly_roles_receive_select_only(self) -> None:
        self.assertIn(
            "grant select on cg_core.v_cg_visita_dia_precedencia_route_b_v1 to stock_zero_codex_ro",
            self.normalized,
        )
        self.assertIn(
            "grant select on cg_core.v_cg_visita_dia_precedencia_route_b_v1 to stock_zero_app_ro",
            self.normalized,
        )
        self.assertNotIn("grant insert", self.normalized)
        self.assertNotIn("grant update", self.normalized)
        self.assertNotIn("grant delete", self.normalized)

    def test_productive_applier_is_git_and_target_guarded(self) -> None:
        source = APPLIER.read_text(encoding="utf-8")
        self.assertIn("STOCK_ZERO_022_APPLY_ROUTE_B_APP_BRIDGE", source)
        self.assertIn("repository_not_clean", source)
        self.assertIn("db.xheyrgfagpoigpgakilu.supabase.co", source)
        self.assertIn('args.db_url_env != "DB_URL_ADMIN"', source)
        self.assertNotIn("DB_URL_LOAD", source)

    def test_secret_wrapper_has_scoped_admin_ddl_operation(self) -> None:
        source = WRAPPER.read_text(encoding="utf-8")
        self.assertIn("'apply-route-b-app-bridge'", source)
        self.assertIn("Script = 'scripts/apply_control_gestion_route_b_bridge.py'", source)
        self.assertIn("Profile = 'admin-ddl'", source)


if __name__ == "__main__":
    unittest.main()
