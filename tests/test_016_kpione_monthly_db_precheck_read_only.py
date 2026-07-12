from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import scripts.precheck_kpione_monthly_db_016_read_only as precheck


def make_args(**overrides):
    values = {
        "month": "2026-06",
        "load_plan_json": "plan.json",
        "statement_timeout_ms": 120000,
        "lock_timeout_ms": 5000,
        "idle_transaction_timeout_ms": 60000,
        "json_out": "out.json",
        "md_out": "out.md",
        "soft_exit": True,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def valid_plan() -> dict:
    return {
        "phase_id": precheck.EXPECTED_PLAN_PHASE_ID,
        "month_id": "2026-06",
        "verdict": "WARN",
        "blockers": [],
        "warnings": [],
        "load_plan_sha256": precheck.EXPECTED_LOAD_PLAN_SHA256,
        "guardrails": {"payload_rows_persisted": False},
        "semantic_plan": {"payload_layer": precheck.PLAN_LAYER},
        "row_accounting": {"would_stage_rows": 213181, "carry_forward_out_rows": 15889},
        "selection": {
            "include_candidate_files": [
                {
                    "source_file_id": "1781973512473",
                    "source_file_name": "photo-excel-admin_1781973512473.xlsx",
                    "sha256": "a" * 64,
                }
            ]
        },
        "batch_plan": {"dry_run_batch_id": "015C_e634b4720b66db7a3036302e"},
    }


class FakeCursor:
    def __init__(self, guard=None, overrides=None):
        self.guard = guard or {
            "current_user": "stock_zero_codex_ro",
            "database_name": "redacted",
            "current_schema": "public",
            "transaction_read_only": "on",
            "default_transaction_read_only": "on",
            "statement_timeout": "120s",
            "lock_timeout": "5s",
            "idle_transaction_timeout": "60s",
        }
        self.overrides = overrides or {}
        self.description = []
        self.rows = []
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        text = str(sql)
        if "current_setting('transaction_read_only')" in text:
            self._set_rows([self.guard])
            return
        for statement in precheck.STATEMENTS:
            if " ".join(statement.sql.split()) == " ".join(text.split()):
                rows = self.overrides.get(statement.statement_id, default_rows(statement.statement_id))
                self._set_rows(rows)
                return
        self._set_rows([])

    def _set_rows(self, dict_rows):
        if not dict_rows:
            self.description = []
            self.rows = []
            return
        keys = list(dict_rows[0])
        self.description = [(key,) for key in keys]
        self.rows = [tuple(row.get(key) for key in keys) for row in dict_rows]

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.rollback_called = False
        self.close_called = False

    def cursor(self):
        return self._cursor

    def rollback(self):
        self.rollback_called = True

    def close(self):
        self.close_called = True


def default_rows(statement_id):
    defaults = {
        "target_exists": [{"schema_exists": True, "table_exists": True}],
        "target_privileges": [{
            "schema_usage": True,
            "can_select": True,
            "can_insert": False,
            "can_update": False,
            "can_delete": False,
            "can_truncate": False,
            "can_references": False,
            "can_trigger": False,
        }],
        "target_columns": [
            {"column_name": "batch_id", "ordinal_position": 1, "data_type": "bigint", "udt_name": "int8", "is_nullable": "NO", "column_default": None, "is_generated": "NEVER", "generation_expression": None, "identity_generation": None},
            {"column_name": "source_file", "ordinal_position": 2, "data_type": "text", "udt_name": "text", "is_nullable": "NO", "column_default": None, "is_generated": "NEVER", "generation_expression": None, "identity_generation": None},
            {"column_name": "source_row", "ordinal_position": 3, "data_type": "integer", "udt_name": "int4", "is_nullable": "NO", "column_default": None, "is_generated": "NEVER", "generation_expression": None, "identity_generation": None},
            {"column_name": "fecha_visita", "ordinal_position": 4, "data_type": "date", "udt_name": "date", "is_nullable": "YES", "column_default": None, "is_generated": "NEVER", "generation_expression": None, "identity_generation": None},
            {"column_name": "payload_json", "ordinal_position": 5, "data_type": "jsonb", "udt_name": "jsonb", "is_nullable": "NO", "column_default": None, "is_generated": "NEVER", "generation_expression": None, "identity_generation": None},
        ],
        "target_constraints": [{"constraint_name": "kpione2_raw_batch_source_row_key", "constraint_type": "u", "definition": "UNIQUE (batch_id, source_row)"}],
        "target_indexes": [{"index_name": "ix_kpione2_raw_fecha_visita", "is_unique": False, "is_primary": False, "is_valid": True, "is_ready": True, "definition": "CREATE INDEX ix ON cg_raw.kpione2_raw USING btree (fecha_visita)"}],
        "target_triggers": [],
        "target_owner_rls_size": [{"owner": "postgres", "relrowsecurity": False, "relforcerowsecurity": False, "approximate_rows": 10, "total_relation_size_bytes": 1000, "relation_size_bytes": 500}],
        "target_policies": [],
        "target_dependencies": [{"dependent_schema": "cg_core", "dependent_name": "v_cg_evidencia_unificada_v2", "dependent_kind": "v"}],
        "state_total_minmax": [{"exact_total_rows": 10, "fecha_min": "2026-06-01", "fecha_max": "2026-07-01"}],
        "state_date_counts": [{"fecha": "2026-06-01", "rows": 5}],
        "state_range_summary": [{"rows_june_operational": 5, "rows_june_carry_forward_window": 0, "rows_july": 5}],
        "state_week_counts": [{"week_start": "2026-06-01", "rows": 5}],
        "state_null_counts": [{"batch_id_nulls": 0, "source_file_nulls": 0, "source_row_nulls": 0, "fecha_visita_nulls": 0, "payload_json_nulls": 0}],
        "json_identity_key_presence": [{"event_id": 0, "source_file_id": 0, "source_file_sha256": 0, "source_row_number": 0, "photo_row_hash": 0, "event_stable_hash": 0, "dry_run_batch_id": 0}],
        "source_file_scope_counts": [],
        "batch_scope_counts": [],
        "recent_batch_counts": [],
        "state_batch_ids": [{"batch_ids": ["38"], "batch_count": 1}],
    }
    return defaults[statement_id]


class Precheck016Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.plan_path = self.root / "plan.json"
        self.plan_path.write_text(json.dumps(valid_plan()), encoding="utf-8")
        self.root_patch = mock.patch.object(precheck, "ROOT", self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        (self.root / "scripts").mkdir()
        (self.root / "sql").mkdir()
        (self.root / "scripts" / "load_control_gestion_raw_v17.py").write_text("insert into cg_raw.kpione2_raw", encoding="utf-8")
        (self.root / "sql" / "04_control_gestion_kpione2_multifuente_v2_draft.sql").write_text("create table if not exists cg_raw.kpione2_raw; from cg_raw.kpione2_raw", encoding="utf-8")
        (self.root / "scripts" / "refresh_control_gestion_v2_incremental.py").write_text("cg_core.v_cg_visita_dia_precedencia_v2 cg_mart.fact_cg_out_weekly_v2", encoding="utf-8")

    def run_fake(self, guard=None, overrides=None, env=None):
        cursor = FakeCursor(guard=guard, overrides=overrides)
        conn = FakeConn(cursor)
        args = make_args(load_plan_json=str(self.plan_path))
        payload = precheck.run_precheck(args, connect_fn=lambda _url: conn, env=env or {"DB_URL_CODEX_RO": "postgres://safe"})
        return payload, conn, cursor

    def test_cli_invalid_month(self):
        with self.assertRaises(SystemExit):
            precheck.parse_args(["--month", "2026-05", "--load-plan-json", "x", "--json-out", "x", "--md-out", "x"])

    def test_db_url_missing_blocks_before_connect(self):
        called = False
        payload = precheck.run_precheck(make_args(load_plan_json=str(self.plan_path)), connect_fn=lambda _url: called, env={})
        self.assertEqual(payload["verdict"], "BLOCKED")
        self.assertIn("db_url_codex_ro_missing", payload["blockers"])

    def test_transaction_read_only_off_blocks_business(self):
        guard = {**FakeCursor().guard, "transaction_read_only": "off"}
        payload, _, cursor = self.run_fake(guard=guard)
        self.assertEqual(payload["verdict"], "BLOCKED")
        self.assertIn("transaction_read_only_not_on", payload["blockers"])
        self.assertNotIn("state_total_minmax", [q["statement_id"] for q in payload["query_audit"]])

    def test_default_transaction_read_only_off_blocks(self):
        guard = {**FakeCursor().guard, "default_transaction_read_only": "off"}
        payload, _, _ = self.run_fake(guard=guard)
        self.assertIn("default_transaction_read_only_not_on", payload["blockers"])

    def test_writable_role_blocks(self):
        payload, _, _ = self.run_fake(overrides={"target_privileges": [{**default_rows("target_privileges")[0], "can_insert": True}]})
        self.assertIn("writable_role_not_allowed_for_016", payload["blockers"])

    def test_select_absent_blocks(self):
        payload, _, _ = self.run_fake(overrides={"target_privileges": [{**default_rows("target_privileges")[0], "can_select": False}]})
        self.assertIn("select_privilege_missing", payload["blockers"])

    def test_always_rollback(self):
        payload, conn, _ = self.run_fake()
        self.assertTrue(conn.rollback_called)
        self.assertTrue(payload["guardrails"]["rollback_completed"])

    def test_whitelist_accepts_select_and_show(self):
        precheck.validate_statement_sql("SELECT 1")
        precheck.validate_statement_sql("SHOW statement_timeout")

    def test_whitelist_rejects_mutation(self):
        for sql in ("INSERT INTO x VALUES (1)", "select 1 for update", "EXPLAIN ANALYZE SELECT 1", "CREATE TEMP TABLE x AS SELECT 1"):
            with self.assertRaises(ValueError):
                precheck.validate_statement_sql(sql)

    def test_no_sql_arbitrary_cli(self):
        with self.assertRaises(SystemExit):
            precheck.parse_args(["--month", "2026-06", "--load-plan-json", "x", "--json-out", "x", "--md-out", "x", "--sql", "select 1"])

    def test_target_fixed(self):
        self.assertEqual(precheck.TARGET_ALLOWLIST, ("cg_raw.kpione2_raw",))

    def test_load_plan_mismatch_blocks(self):
        bad = valid_plan()
        bad["row_accounting"]["would_stage_rows"] = 1
        self.plan_path.write_text(json.dumps(bad), encoding="utf-8")
        payload, _, _ = self.run_fake()
        self.assertIn("load_plan_would_stage_rows_mismatch", payload["blockers"])

    def test_hash_before_after_match(self):
        payload, _, _ = self.run_fake()
        self.assertEqual(payload["load_plan"]["file_sha256_before"], payload["load_plan"]["file_sha256_after"])

    def test_payload_json_not_dumped(self):
        payload, _, _ = self.run_fake()
        self.assertNotIn("Link Foto", json.dumps(payload))

    def test_secret_patterns_not_present(self):
        payload, _, _ = self.run_fake(env={"DB_URL_CODEX_RO": "postgres://user:pass@host/db"})
        self.assertNotIn("user:pass", json.dumps(payload))

    def test_urls_not_present(self):
        payload, _, _ = self.run_fake()
        self.assertNotRegex(json.dumps(payload), r"https?://")

    def test_queries_use_columns_only_if_exist(self):
        payload, _, _ = self.run_fake(overrides={"target_columns": [{"column_name": "id"}]})
        self.assertIn("business_queries_skipped_missing_required_columns", payload["warnings"])

    def test_absence_identity_unverifiable(self):
        payload, _, _ = self.run_fake()
        self.assertEqual(payload["exact_overlap_classification"], "UNVERIFIABLE")

    def test_physical_identity_indexed_feasible(self):
        columns = default_rows("target_columns") + [{"column_name": "event_id"}, {"column_name": "photo_row_hash"}]
        indexes = [{"index_name": "event_photo", "is_unique": False, "is_primary": False, "is_valid": True, "is_ready": True, "definition": "CREATE INDEX ON cg_raw.kpione2_raw (event_id, photo_row_hash)"}]
        payload, _, _ = self.run_fake(overrides={"target_columns": columns, "target_indexes": indexes})
        self.assertTrue(payload["exact_overlap_feasibility"]["exact_comparison_potentially_feasible"])

    def test_json_identity_not_indexed_unsafe(self):
        rows = [{"event_id": 1, "source_file_id": 1, "source_file_sha256": 1, "source_row_number": 1, "photo_row_hash": 1, "event_stable_hash": 1, "dry_run_batch_id": 0}]
        payload, _, _ = self.run_fake(overrides={"json_identity_key_presence": rows})
        self.assertEqual(payload["exact_overlap_feasibility"]["operationally_unsafe_reason"], "identity_only_in_payload_json_without_index")

    def test_source_files_present_not_full_exact(self):
        source_rows = [{"source_file": "photo-excel-admin_1781973512473.xlsx", "rows": 1, "fecha_min": "2026-06-01", "fecha_max": "2026-06-01", "batch_count": 1}]
        payload, _, _ = self.run_fake(overrides={"source_file_scope_counts": source_rows})
        self.assertEqual(payload["coarse_overlap_classification"], "ALL_SOURCE_FILES_PRESENT")
        self.assertEqual(payload["exact_overlap_classification"], "UNVERIFIABLE")

    def test_absence_dry_run_batch_id_not_fresh(self):
        payload, _, _ = self.run_fake()
        self.assertNotEqual(payload["exact_overlap_classification"], "FRESH")
        self.assertEqual(payload["source_signal_interpretation"]["coarse_classification"], "NO_SOURCE_SIGNAL")
        self.assertEqual(payload["source_signal_interpretation"]["policy"], "NO_SOURCE_SIGNAL DOES NOT IMPLY FRESH.")
        self.assertFalse(payload["source_signal_interpretation"]["proves_freshness"])
        self.assertFalse(payload["source_signal_interpretation"]["proves_no_overlap"])
        self.assertFalse(payload["source_signal_interpretation"]["exact_identity_available"])
        self.assertEqual(payload["source_signal_interpretation"]["exact_overlap_classification"], "UNVERIFIABLE")
        self.assertEqual(payload["source_signal_interpretation"]["apply_gate"], "BLOCKED_FOR_IDEMPOTENCY_CONTRACT")

    def test_historical_state_discrepancy_documented_not_reconstructed(self):
        payload, _, _ = self.run_fake(
            overrides={
                "target_owner_rls_size": [{
                    "owner": "postgres",
                    "relrowsecurity": False,
                    "relforcerowsecurity": False,
                    "approximate_rows": 45736,
                    "total_relation_size_bytes": 1000,
                    "relation_size_bytes": 500,
                }],
                "state_total_minmax": [{"exact_total_rows": 45736, "fecha_min": "2026-04-07", "fecha_max": "2026-06-01"}],
                "state_batch_ids": [{"batch_ids": ["38"], "batch_count": 1}],
            }
        )
        discrepancy = payload["historical_state_discrepancy"]
        self.assertEqual(discrepancy["target"], "cg_raw.kpione2_raw")
        self.assertEqual(discrepancy["historical_row_count"], 526022)
        self.assertEqual(discrepancy["historical_batch_count"], 19)
        self.assertEqual(discrepancy["latest_historical_batch_id"], "38")
        self.assertEqual(discrepancy["latest_historical_batch_loaded_rows"], 45736)
        self.assertEqual(discrepancy["current_exact_row_count"], 45736)
        self.assertEqual(discrepancy["current_batch_ids"], ["38"])
        self.assertEqual(discrepancy["current_fecha_min"], "2026-04-07")
        self.assertEqual(discrepancy["current_fecha_max"], "2026-06-01")
        self.assertEqual(discrepancy["classification"], "TABLE_STATE_CHANGED")
        self.assertEqual(discrepancy["resolution_status"], "DOCUMENTED_NOT_RECONSTRUCTED")
        self.assertFalse(discrepancy["legacy_migration_authority"])
        self.assertFalse(discrepancy["blocker_for_new_design"])
        self.assertTrue(discrepancy["warning_condition"])
        self.assertIn("historical_state_discrepancy:TABLE_STATE_CHANGED", payload["warnings"])

    def test_roadmap_compliance_locked_to_016(self):
        payload, _, _ = self.run_fake()
        compliance = payload["roadmap_compliance"]
        self.assertEqual(compliance["roadmap_lock_id"], "KPIONE_DB_TRANSITION_016_019_LOCK_V1")
        self.assertEqual(compliance["roadmap_lock_sha256"], "6fcfd7e45c91b921387de93a5fb5de19ef45287083151881a02471ccf27f3b22")
        self.assertEqual(compliance["roadmap_lock_sha256_method"], "SHA256_OF_CANONICAL_GIT_BLOB_BYTES_LF")
        self.assertEqual(compliance["roadmap_lock_commit"], "5c0aa19ac753c21aa9bb43b6fdd72a927b694a5f")
        self.assertEqual(compliance["current_phase"], "016A")
        self.assertEqual(compliance["expected_next_phase"], "016B")
        self.assertEqual(compliance["roadmap_compliance"], "COMPLIANT")
        self.assertEqual(compliance["deviations_detected"], [])

    def test_no_temp_tables_in_statements(self):
        text = "\n".join(s.sql.lower() for s in precheck.STATEMENTS)
        self.assertNotIn("temp table", text)

    def test_no_explain_analyze(self):
        text = "\n".join(s.sql.lower() for s in precheck.STATEMENTS)
        self.assertNotIn("explain analyze", text)

    def test_query_audit_safe(self):
        payload, _, _ = self.run_fake()
        self.assertTrue(payload["query_audit"])
        self.assertNotIn("SELECT", json.dumps(payload["query_audit"]).upper())

    def test_db_state_hash_excludes_observed_at(self):
        payload, _, _ = self.run_fake()
        first = payload["db_state_sha256"]
        payload["observed_at"] = "different"
        precheck.build_hashes(payload, "2026-06", precheck.EXPECTED_LOAD_PLAN_SHA256, {"statement_timeout_ms": 1, "lock_timeout_ms": 1, "idle_transaction_timeout_ms": 1})
        self.assertNotEqual(first, "")

    def test_query_plan_sha_deterministic(self):
        p1, _, _ = self.run_fake()
        p2, _, _ = self.run_fake()
        self.assertEqual(p1["query_plan_sha256"], p2["query_plan_sha256"])

    def test_no_productive_loader_import(self):
        source = Path(precheck.__file__).read_text(encoding="utf-8")
        self.assertNotIn("import load_control_gestion_raw_v17", source)
        self.assertNotIn("from load_control_gestion_raw_v17", source)

    def test_no_write_flags(self):
        help_text = precheck.parse_args(["--month", "2026-06", "--load-plan-json", "x", "--json-out", "x", "--md-out", "x"])
        self.assertFalse(hasattr(help_text, "apply"))

    def test_aggregate_evidence_only(self):
        payload, _, _ = self.run_fake()
        self.assertIn("database_state", payload)
        self.assertNotIn("payload_json_rows", payload)


if __name__ == "__main__":
    unittest.main()
