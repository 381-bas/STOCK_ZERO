from __future__ import annotations

import contextlib
import copy
import ast
import hashlib
import importlib
import io
import json
import os
import subprocess
import sys
import tempfile
import traceback
import types
import unittest
from pathlib import Path
from unittest import mock

from scripts import refresh_control_gestion_v2_incremental as refresh
from scripts import validate_control_gestion_route_b_023_read_only as observer
from scripts import provision_control_gestion_mart_refresh_role as provisioner
from scripts import apply_control_gestion_route_b_bridge as bridge


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "plans" / "023_control_gestion_route_b_bridge_refresh_plan.json"
SQL_PATH = ROOT / "sql" / "18_control_gestion_route_b_app_bridge_v1.sql"
WRAPPER_PATH = ROOT / "scripts" / "invoke_stock_zero_db_operation.ps1"
PROVISIONER_PATH = ROOT / "scripts" / "provision_control_gestion_mart_refresh_role.py"
BRIDGE_PATH = ROOT / "scripts" / "apply_control_gestion_route_b_bridge.py"

AUTHORIZATION_NAMES = (
    "provision_refresh_role_authorized",
    "apply_bridge_authorized",
    "apply_june_refresh_authorized",
    "runtime_app_validation_authorized",
)


def completed(returncode: int = 0, stdout="") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr="")


class Phase023PlanContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))

    def test_initial_plan_is_closed_without_invented_runtime_evidence(self) -> None:
        self.assertEqual(self.plan["status"], "CONTRACT_READY_GATE_CLOSED")
        self.assertFalse(self.plan["gate_open"])
        self.assertFalse(self.plan["productive_actions_authorized"])
        self.assertTrue(all(value is False for value in self.plan["authorizations"].values()))
        self.assertEqual(self.plan["runtime_evidence"], {})
        self.assertIsNone(self.plan["approved_git_sha"])

    def test_scope_is_exact_june_and_hash_is_computed_from_canonical_payload(self) -> None:
        payload = self.plan["scope"]["canonical_payload"]
        self.assertEqual(len(payload["affected_dates"]), 30)
        self.assertEqual(payload["affected_dates"][0], "2026-06-01")
        self.assertEqual(payload["affected_dates"][-1], "2026-06-30")
        self.assertEqual(
            payload["affected_weeks"],
            ["2026-06-01", "2026-06-08", "2026-06-15", "2026-06-22", "2026-06-29"],
        )
        self.assertEqual(
            refresh._sha256_bytes(refresh._canonical_json_bytes(payload)),
            self.plan["scope"]["scope_sha256"],
        )

    def test_bridge_raw_hash_is_current_file_hash(self) -> None:
        observed = hashlib.sha256(SQL_PATH.read_bytes()).hexdigest()
        self.assertEqual(observed, self.plan["bridge_contract"]["sql_raw_sha256"])

    def test_state_transitions_are_forward_only(self) -> None:
        transitions = self.plan["state_contract"]["transitions"]
        states = self.plan["state_contract"]["states"]
        for index, state in enumerate(states):
            self.assertEqual(transitions[state], states[index + 1:index + 2])

    def test_static_plan_validator_rejects_multiple_authorizations(self) -> None:
        mutated = copy.deepcopy(self.plan)
        mutated["authorizations"]["apply_bridge_authorized"] = True
        mutated["authorizations"]["apply_june_refresh_authorized"] = True
        mutated["gate_open"] = True
        mutated["productive_actions_authorized"] = True
        mutated["approved_git_sha"] = "a" * 40
        with self.assertRaisesRegex(observer.ReadonlyValidationError, "multiple_authorizations"):
            observer.validate_static_plan_contract(mutated)

    def test_complete_requires_all_authorizations_false_and_full_raw_evidence_contract(self) -> None:
        complete = copy.deepcopy(self.plan)
        complete["status"] = "COMPLETE_GATE_CLOSED"
        complete["run_id"] = "12345678-1234-4123-8123-123456789abc"
        complete["execution_state"]["closure_complete"] = True
        complete["execution_state"]["closure_git_sha"] = "b" * 40
        complete["runtime_evidence"] = {
            key: {
                "path": f"evidence/runtime/023/{complete['run_id']}/{filename}",
                "raw_sha256": key[-1] * 64,
                "verdict": f"PASS_{key}",
            }
            for key, filename in complete["evidence_contract"]["file_templates"].items()
        }
        observer.validate_complete_closure_contract(complete)
        complete["authorizations"]["runtime_app_validation_authorized"] = True
        complete["gate_open"] = True
        complete["productive_actions_authorized"] = True
        complete["approved_git_sha"] = "c" * 40
        with self.assertRaises(observer.ReadonlyValidationError):
            observer.validate_complete_closure_contract(complete)

    def test_no_runtime_evidence_06_contract_exists(self) -> None:
        rendered = json.dumps(self.plan)
        self.assertNotIn("06_closure_bundle", rendered)
        self.assertEqual(set(self.plan["evidence_contract"]["file_templates"]), {"01", "02", "03", "04", "05"})


class Phase023RefreshGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
        self.plan["status"] = refresh.PLAN_023_REFRESH_STATE
        self.plan["approved_git_sha"] = "a" * 40

    def _load(self, operation: str):
        with mock.patch.object(refresh, "_validate_git_and_plan", return_value={"approved_git_sha": "a" * 40}):
            with mock.patch.object(Path, "read_text", return_value=json.dumps(self.plan)):
                return refresh._load_plan_023(PLAN_PATH, operation=operation)

    def test_missing_plan_fails_closed(self) -> None:
        with self.assertRaisesRegex(refresh.RefreshContractError, "unavailable_or_invalid"):
            refresh._load_plan_023(ROOT / "plans" / "missing-023.json", operation="dry-run")

    def test_wrong_state_fails_closed(self) -> None:
        self.plan["status"] = "CONTRACT_READY_GATE_CLOSED"
        with self.assertRaisesRegex(refresh.RefreshContractError, "state_mismatch"):
            self._load("dry-run")

    def test_apply_authorization_false_fails_closed(self) -> None:
        with self.assertRaisesRegex(refresh.RefreshContractError, "not_exclusively_authorized"):
            self._load("apply")

    def test_apply_rejects_multiple_authorizations(self) -> None:
        self.plan["authorizations"]["apply_bridge_authorized"] = True
        self.plan["authorizations"]["apply_june_refresh_authorized"] = True
        with self.assertRaisesRegex(refresh.RefreshContractError, "multiple"):
            self._load("apply")

    def test_scope_outside_june_or_extra_week_fails(self) -> None:
        self.plan["scope"]["canonical_payload"]["affected_dates"].append("2026-07-01")
        self.plan["scope"]["scope_sha256"] = refresh._sha256_bytes(
            refresh._canonical_json_bytes(self.plan["scope"]["canonical_payload"])
        )
        with self.assertRaisesRegex(refresh.RefreshContractError, "exact_match"):
            self._load("dry-run")

    def test_explicit_db_url_is_rejected_for_plan_apply(self) -> None:
        payload = self.plan["scope"]["canonical_payload"]
        argv = [
            "refresh", "--apply", "--plan-023", str(PLAN_PATH),
            "--safety-window-weeks", "0", "--confirm", refresh.APPLY_CONFIRM_TOKEN,
            "--db-url", "postgresql://forbidden",
        ]
        stdout = io.StringIO()
        with mock.patch.object(sys, "argv", argv), \
             mock.patch.object(refresh, "_load_plan_023", return_value=(self.plan, {"scope_sha256": self.plan["scope"]["scope_sha256"]})), \
             mock.patch.object(refresh, "_validate_wrapper_marker"), \
             contextlib.redirect_stdout(stdout):
            code = refresh.main()
        self.assertEqual(code, 1)
        self.assertIn("explicit_db_url_forbidden", stdout.getvalue())
        self.assertEqual(len(payload["affected_dates"]), 30)

    def test_missing_scoped_env_is_rejected_without_connection(self) -> None:
        argv = [
            "refresh", "--dry-run", "--plan-023", str(PLAN_PATH),
            "--safety-window-weeks", "0", "--confirm", refresh.DRY_RUN_CONFIRM_TOKEN,
            "--report-json", str(Path(tempfile.gettempdir()) / "never-created-023.json"),
        ]
        with mock.patch.object(sys, "argv", argv), \
             mock.patch.object(refresh, "_load_plan_023", return_value=(self.plan, {"scope_sha256": self.plan["scope"]["scope_sha256"]})), \
             mock.patch.object(refresh, "_validate_wrapper_marker"), \
             mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(refresh, "run_incremental_dry_run") as database_call, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(refresh.main(), 1)
        database_call.assert_not_called()

    def test_wrong_confirmation_token_is_rejected_before_connection(self) -> None:
        argv = [
            "refresh", "--dry-run", "--plan-023", str(PLAN_PATH),
            "--safety-window-weeks", "0", "--confirm", "WRONG",
        ]
        with mock.patch.object(sys, "argv", argv), \
             mock.patch.object(refresh, "_load_plan_023", return_value=(self.plan, {"scope_sha256": "x"})), \
             mock.patch.object(refresh, "run_incremental_dry_run") as database_call, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(refresh.main(), 1)
        database_call.assert_not_called()

    def test_head_mismatch_is_rejected(self) -> None:
        self.plan["approved_git_sha"] = "a" * 40
        with mock.patch.object(refresh, "_git", return_value=completed(stdout="b" * 40 + "\n")):
            with self.assertRaisesRegex(refresh.RefreshContractError, "head_mismatch"):
                refresh._validate_git_and_plan(PLAN_PATH, self.plan)

    def test_dirty_repository_is_rejected(self) -> None:
        def fake_git(*args, **kwargs):
            if args == ("rev-parse", "HEAD"):
                return completed(stdout="a" * 40 + "\n")
            if args == ("diff", "--quiet"):
                return completed(returncode=1)
            return completed()
        with mock.patch.object(refresh, "_git", side_effect=fake_git):
            with self.assertRaisesRegex(refresh.RefreshContractError, "worktree_not_clean"):
                refresh._validate_git_and_plan(PLAN_PATH, self.plan)

    def test_untracked_or_changed_plan_is_rejected(self) -> None:
        def fake_git(*args, **kwargs):
            if args == ("rev-parse", "HEAD"):
                return completed(stdout="a" * 40 + "\n")
            if args[:1] == ("show",):
                return completed(stdout=b"different")
            return completed()
        with mock.patch.object(refresh, "_git", side_effect=fake_git):
            with self.assertRaisesRegex(refresh.RefreshContractError, "blob_mismatch"):
                refresh._validate_git_and_plan(PLAN_PATH, self.plan)

    def test_existing_evidence_path_is_rejected(self) -> None:
        run_id = "12345678-1234-4123-8123-123456789abc"
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            evidence = root / "evidence" / "runtime" / "023" / run_id / "04_june_mart_refresh_apply.json"
            evidence.parent.mkdir(parents=True)
            evidence.write_text("{}", encoding="utf-8")
            with mock.patch.object(refresh, "ROOT", root):
                with self.assertRaisesRegex(refresh.RefreshContractError, "already_exists"):
                    refresh._canonical_evidence_path(evidence, run_id, evidence.name)

    def test_removed_unguarded_apply_artifacts_stay_absent(self) -> None:
        source = (ROOT / "scripts" / "refresh_control_gestion_v2_incremental.py").read_text(encoding="utf-8")
        self.assertNotIn("REAL_APPLY_ENABLED", source)
        self.assertNotIn('"real_apply_enabled": True', source)
        self.assertNotIn("DROP TABLE IF EXISTS _cg_daily_stage", source)
        self.assertNotIn("DROP TABLE IF EXISTS _cg_weekly_stage", source)
        self.assertNotIn("ANALYZE ", source)
        self.assertIn("ON COMMIT DROP", source)

    def test_direct_apply_rejects_fabricated_dsn_scope_and_contract_before_core(self) -> None:
        connection = _CoreConnection()
        with self.assertRaisesRegex(refresh.RefreshContractError, "legacy_apply_interface_removed"):
            refresh.run_incremental_apply(
                db_url="postgresql://forged", connection=connection,
                week_scope={"forged": True}, contract_023={"authorized": True},
                fingerprints={"forged": True}, advisory_lock_key=1,
            )
        self.assertEqual((connection.commits, connection.rollbacks), (0, 0))
        self.assertFalse(any("DELETE FROM" in call or "INSERT INTO" in call for call in connection.cursor_value.calls))

    def test_no_importable_helper_accepts_authority_and_contains_dml(self) -> None:
        tree = ast.parse((ROOT / "scripts" / "refresh_control_gestion_v2_incremental.py").read_text(encoding="utf-8"))
        offenders = []
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            params = {arg.arg for arg in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)}
            body = ast.get_source_segment((ROOT / "scripts" / "refresh_control_gestion_v2_incremental.py").read_text(encoding="utf-8"), node) or ""
            if (params & {"db_url", "connection", "conn", "week_scope", "contract_023", "expected_content_fingerprints", "advisory_lock_key"}) and ("_delete_daily_query" in body or "_insert_daily_query" in body or ".commit(" in body):
                offenders.append(node.name)
        self.assertEqual(offenders, [])


class Phase023FingerprintTests(unittest.TestCase):
    COLUMNS = ["grain", "count", "nullable"]

    def test_equal_aggregates_but_different_content_have_different_hashes(self) -> None:
        left = [("A", 1, None), ("B", 2, "x")]
        right = [("A", 2, None), ("B", 1, "x")]
        self.assertNotEqual(
            refresh.fingerprint_rows(left, self.COLUMNS)["sha256"],
            refresh.fingerprint_rows(right, self.COLUMNS)["sha256"],
        )

    def test_row_order_is_canonical(self) -> None:
        rows = [("B", 2, "x"), ("A", 1, None)]
        self.assertEqual(
            refresh.fingerprint_rows(rows, self.COLUMNS),
            refresh.fingerprint_rows(reversed(rows), self.COLUMNS),
        )

    def test_null_and_empty_string_are_distinct(self) -> None:
        self.assertNotEqual(
            refresh.fingerprint_rows([("A", 1, None)], self.COLUMNS)["sha256"],
            refresh.fingerprint_rows([("A", 1, "")], self.COLUMNS)["sha256"],
        )

    def test_source_or_target_drift_blocks(self) -> None:
        base = {
            key: {"schema_version": refresh.FINGERPRINT_SCHEMA_VERSION, "row_count": 1, "sha256": hashlib.sha256(key.encode()).hexdigest()}
            for key in refresh.FINGERPRINT_KEYS
        }
        for key in ("source_daily_stage_sha256", "target_weekly_sha256"):
            changed = copy.deepcopy(base)
            changed[key]["sha256"] = "changed"
            with self.assertRaisesRegex(refresh.RefreshContractError, "PRESTATE_OR_SOURCE_DRIFT"):
                refresh._assert_fingerprints_match(base, changed)

    def test_daily_loaded_at_is_part_of_the_exact_fingerprint(self) -> None:
        columns = ["key", "mart_loaded_at"]
        first = refresh.fingerprint_rows([("A", "2026-07-19T12:00:00Z")], columns)
        second = refresh.fingerprint_rows([("A", "2026-07-19T12:00:01Z")], columns)
        self.assertNotEqual(first["sha256"], second["sha256"])

    def test_weekly_stage_uses_daily_overlay_and_exact_daily_stage(self) -> None:
        query = refresh._create_weekly_stage_query()
        self.assertIn("daily_stage AS MATERIALIZED", query)
        self.assertIn("daily_overlay AS MATERIALIZED", query)
        self.assertIn("UNION ALL", query)
        self.assertIn("LEFT JOIN daily_overlay d", query)
        self.assertNotIn(f"LEFT JOIN {refresh.DAILY_FACT} d", query)
        self.assertIn("%s::timestamptz AS mart_loaded_at", query)

    def test_changed_daily_stage_changes_weekly_source_fingerprint(self) -> None:
        columns = ["week", "daily_stage_rows"]
        before = refresh.fingerprint_rows([("2026-06-01", "A:1")], columns)
        after = refresh.fingerprint_rows([("2026-06-01", "A:2")], columns)
        self.assertNotEqual(before["sha256"], after["sha256"])


class _CoreCursor:
    def __init__(self, fail_weekly: bool = False, identity=None) -> None:
        self.calls: list[str] = []
        self.rowcount = 1
        self.fail_weekly = fail_weekly
        self._one = None
        self.identity = identity or (refresh.MART_REFRESH_ROLE, refresh.MART_REFRESH_ROLE, "postgres", "none", "off")

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def execute(self, query, _params=None):
        rendered = str(query)
        self.calls.append(rendered)
        if "SELECT current_user,session_user" in rendered:
            self._one = self.identity
        if self.fail_weekly and rendered.lstrip().startswith("INSERT INTO cg_mart.fact_cg_out_weekly_v2"):
            raise RuntimeError("weekly failed")
    def fetchone(self): return self._one


class _CoreConnection:
    def __init__(self, fail_weekly: bool = False, identity=None) -> None:
        self.cursor_value = _CoreCursor(fail_weekly, identity)
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
    def set_session(self, **_kwargs): pass
    def cursor(self): return self.cursor_value
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1
    def close(self): self.closed += 1


class Phase023ApplyTransactionTests(unittest.TestCase):
    @staticmethod
    def _minimal_args():
        return types.SimpleNamespace(
            apply=True, dry_run=False, confirm_real_apply=False, plan_023=None,
            db_url=None, affected_date=[], affected_week=[], safety_window_weeks=0,
            confirm=refresh.APPLY_CONFIRM_TOKEN, evidence_json=Path("unused.json"),
            run_id="12345678-1234-4123-8123-123456789abc",
            dry_run_report_json=Path("unused-dry.json"),
            statement_timeout_seconds=1, post_apply_validate=True,
        )

    def _run(self, connection: _CoreConnection, *, observed_fingerprints=None):
        fingerprints = {key: {"schema_version": refresh.FINGERPRINT_SCHEMA_VERSION, "row_count": 1, "sha256": hashlib.sha256(key.encode()).hexdigest()} for key in refresh.FINGERPRINT_KEYS}
        observed_fingerprints = fingerprints if observed_fingerprints is None else observed_fingerprints
        present = [
            {"semana_inicio": week, "scope_status": "present"}
            for week in ("2026-06-01", "2026-06-08", "2026-06-15", "2026-06-22", "2026-06-29")
        ]
        plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
        plan["approved_mart_loaded_at_utc"] = "2026-07-19T12:00:00Z"
        authority = {"scope_sha256": plan["scope"]["scope_sha256"], "approved_git_sha": "a" * 40}
        run_id = "12345678-1234-4123-8123-123456789abc"
        with tempfile.TemporaryDirectory() as folder:
            dry_path = Path(folder) / "dry.json"
            dry_report = {"content_fingerprints": fingerprints, "approved_mart_loaded_at_utc": plan["approved_mart_loaded_at_utc"]}
            dry_path.write_text(json.dumps(dry_report), encoding="utf-8")
            plan["dry_run_authorization"] = {
                "raw_sha256": hashlib.sha256(dry_path.read_bytes()).hexdigest(),
                "scope_sha256": authority["scope_sha256"],
                "content_fingerprints": fingerprints,
                "approved_mart_loaded_at_utc": plan["approved_mart_loaded_at_utc"],
            }
            args = types.SimpleNamespace(
                apply=True, dry_run=False, confirm_real_apply=False, plan_023=None,
                db_url=None, affected_date=[], affected_week=[], safety_window_weeks=0,
                confirm=refresh.APPLY_CONFIRM_TOKEN, evidence_json=Path(folder) / "unused.json",
                run_id=run_id, dry_run_report_json=dry_path,
                statement_timeout_seconds=1, post_apply_validate=True,
            )
            parser = mock.Mock(); parser.parse_args.return_value = args
            psycopg2 = types.SimpleNamespace(connect=lambda _dsn: connection)
            with mock.patch.object(refresh, "build_parser", return_value=parser), \
                 mock.patch.object(refresh, "_load_plan_023", return_value=(plan, authority)), \
                 mock.patch.object(refresh, "_validate_wrapper_marker"), \
                 mock.patch.object(refresh, "_validate_productive_dsn"), \
                 mock.patch.object(refresh, "_canonical_evidence_path", return_value=Path(folder) / "evidence.json"), \
                 mock.patch.object(refresh, "_compute_content_fingerprints", return_value=observed_fingerprints), \
                 mock.patch.object(refresh, "_route_scope_by_week", return_value=present), \
                 mock.patch.object(refresh, "_count_temp_rows", return_value=1), \
                 mock.patch.object(refresh, "_run_daily_check", return_value={"validation_status": "ok"}), \
                 mock.patch.object(refresh, "_run_weekly_fact_validation", return_value={"validation_status": "ok"}), \
                 mock.patch.dict(sys.modules, {"psycopg2": psycopg2}), \
                 mock.patch.dict(os.environ, {refresh.MART_REFRESH_ENV: "postgresql://not-opened"}, clear=False):
                return refresh.run_authorized_june_refresh_023()

    def test_daily_success_weekly_failure_rolls_back_once_without_commit(self) -> None:
        connection = _CoreConnection(fail_weekly=True)
        result = self._run(connection)
        self.assertEqual((connection.commits, connection.rollbacks), (0, 1))
        self.assertTrue(result["rolled_back"])
        self.assertFalse(result["committed"])

    def test_daily_and_weekly_success_commit_once(self) -> None:
        connection = _CoreConnection()
        result = self._run(connection)
        self.assertEqual((connection.commits, connection.rollbacks), (1, 0))
        self.assertTrue(result["committed"])
        self.assertFalse(result["rolled_back"])
        self.assertEqual(result["commit_state"], refresh.COMMITTED_EVIDENCE_RECORDED)

    def test_postcommit_evidence_failure_is_recovery_not_reapply(self) -> None:
        connection = _CoreConnection()
        with mock.patch.object(refresh, "write_json_exclusive", side_effect=OSError("disk")), \
             mock.patch.object(refresh, "write_committed_recovery_receipt", return_value=Path(tempfile.gettempdir()) / "receipt.json"):
            result = self._run(connection)
        self.assertTrue(result["committed"])
        self.assertFalse(result["rolled_back"])
        self.assertFalse(result["reapply_allowed"])
        self.assertEqual(result["verdict"], refresh.COMMITTED_EVIDENCE_RECOVERY_REQUIRED)

    def test_each_fingerprint_mismatch_blocks_before_dml(self) -> None:
        expected = {key: {"schema_version": refresh.FINGERPRINT_SCHEMA_VERSION, "row_count": 1, "sha256": hashlib.sha256(key.encode()).hexdigest()} for key in refresh.FINGERPRINT_KEYS}
        for key in refresh.FINGERPRINT_KEYS:
            observed = copy.deepcopy(expected); observed[key]["sha256"] = "0" * 64
            connection = _CoreConnection()
            result = self._run(connection, observed_fingerprints=observed)
            self.assertEqual((connection.commits, connection.rollbacks), (0, 1), key)
            self.assertFalse(any("DELETE FROM" in call or "INSERT INTO" in call for call in connection.cursor_value.calls), key)

    def test_canonical_plan_auth_and_head_failures_never_connect(self) -> None:
        for error in ("plan_blob_mismatch", "apply_not_exclusively_authorized", "head_mismatch"):
            parser = mock.Mock(); parser.parse_args.return_value = self._minimal_args()
            connect = mock.Mock()
            with mock.patch.object(refresh, "build_parser", return_value=parser), \
                 mock.patch.object(refresh, "_load_plan_023", side_effect=refresh.RefreshContractError(error)), \
                 mock.patch.dict(sys.modules, {"psycopg2": types.SimpleNamespace(connect=connect)}):
                with self.assertRaisesRegex(refresh.RefreshContractError, error):
                    refresh.run_authorized_june_refresh_023()
            connect.assert_not_called()

    def test_wrong_dsn_target_never_connects(self) -> None:
        plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
        authority = {"scope_sha256": plan["scope"]["scope_sha256"]}
        parser = mock.Mock(); parser.parse_args.return_value = self._minimal_args()
        connect = mock.Mock()
        with mock.patch.object(refresh, "build_parser", return_value=parser), \
             mock.patch.object(refresh, "_load_plan_023", return_value=(plan, authority)), \
             mock.patch.object(refresh, "_validate_wrapper_marker"), \
             mock.patch.object(refresh, "_validate_productive_dsn", side_effect=refresh.RefreshContractError("dsn_target_mismatch")), \
             mock.patch.dict(os.environ, {refresh.MART_REFRESH_ENV: "postgresql://wrong"}, clear=False), \
             mock.patch.dict(sys.modules, {"psycopg2": types.SimpleNamespace(connect=connect)}):
            with self.assertRaisesRegex(refresh.RefreshContractError, "dsn_target_mismatch"):
                refresh.run_authorized_june_refresh_023()
        connect.assert_not_called()

    def test_wrong_session_role_rolls_back_before_dml(self) -> None:
        connection = _CoreConnection(identity=("postgres", "postgres", "postgres", "none", "off"))
        result = self._run(connection)
        self.assertEqual((connection.commits, connection.rollbacks), (0, 1))
        self.assertFalse(any("DELETE FROM" in call or "INSERT INTO" in call for call in connection.cursor_value.calls))


class Phase023EvidenceWriterTests(unittest.TestCase):
    def test_exclusive_writer_publishes_complete_json_and_cleans_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "evidence.json"
            refresh.write_json_exclusive(path, {"verdict": "PASS"})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"verdict": "PASS"})
            self.assertEqual(list(Path(folder).glob("*.tmp")), [])

    def test_exclusive_writer_rejects_existing_and_preserves_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "evidence.json"
            path.write_bytes(b"original")
            with self.assertRaises(refresh.RefreshContractError):
                refresh.write_json_exclusive(path, {"new": True})
            self.assertEqual(path.read_bytes(), b"original")

    def test_bridge_evidence_failure_emits_no_pass(self) -> None:
        argv = ["bridge", "--expected-git-ref", "a" * 40, "--db-url-env", "DB_URL_ADMIN", "--expected-project-ref", "xheyrgfagpoigpgakilu", "--confirm", bridge.CONFIRM_TOKEN, "--run-id", "12345678-1234-4123-8123-123456789abc", "--evidence-json", "unused"]
        output = io.StringIO()
        committed = {"committed": True, "rolled_back": False, "content": "ok"}
        recovery = {"verdict": refresh.COMMITTED_EVIDENCE_RECOVERY_REQUIRED, "committed": True, "rolled_back": False, "reapply_allowed": False}
        with mock.patch.object(sys, "argv", argv), mock.patch.object(bridge, "_load_plan", return_value={}), mock.patch.object(bridge, "validate_git_guard", return_value={"approved_git_sha": "a" * 40, "sql_raw_sha256": "b" * 64}), mock.patch.object(bridge, "_canonical_evidence_path", return_value=ROOT / "never.json"), mock.patch.object(bridge, "validate_dsn"), mock.patch.object(bridge, "apply_bridge", return_value=committed), mock.patch.object(bridge, "_emit_committed_recovery_nonthrowing", return_value=recovery), mock.patch.dict(os.environ, {"STOCK_ZERO_OPERATION_PROFILE": "admin-ddl", "STOCK_ZERO_OPERATION": "apply-route-b-bridge-023", "DB_URL_ADMIN": "hidden"}), contextlib.redirect_stdout(output):
            self.assertEqual(bridge.main(), 3)
        self.assertNotIn("PASS_ROUTE_B_APP_BRIDGE_APPLY", output.getvalue())
        self.assertIn(refresh.COMMITTED_EVIDENCE_RECOVERY_REQUIRED, output.getvalue())

    def test_committed_recovery_survives_all_three_emitter_failures(self) -> None:
        report = {"run_id": "12345678-1234-4123-8123-123456789abc", "committed": True}
        broken_stderr = mock.Mock()
        broken_stderr.write.side_effect = OSError("stderr")
        with mock.patch.object(refresh, "write_json_exclusive", side_effect=OSError("evidence")), \
             mock.patch.object(refresh, "write_committed_recovery_receipt", side_effect=OSError("receipt")), \
             mock.patch.object(refresh.sys, "stderr", broken_stderr):
            result = refresh._emit_committed_recovery_nonthrowing(Path("never.json"), report)
        self.assertEqual(result["verdict"], refresh.COMMITTED_EVIDENCE_RECOVERY_REQUIRED)
        self.assertTrue(result["committed"])
        self.assertFalse(result["rolled_back"])
        self.assertFalse(result["reapply_allowed"])


class Phase023SqlAndRoleContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sql = " ".join(SQL_PATH.read_text(encoding="utf-8").lower().split())
        cls.provisioner = PROVISIONER_PATH.read_text(encoding="utf-8")

    def test_overlap_replaces_legacy_kpione2_instead_of_adding_it(self) -> None:
        self.assertIn("greatest( coalesce(l.raw_evidence_count, 0) - coalesce(l.kpione2_rows_dia, 0), 0 ) + coalesce(r.event_count, 0)", self.sql)
        self.assertIn("when r.fecha is not null then coalesce(r.event_count, 0)", self.sql)

    def test_same_source_multimark_uses_corrected_kpione2_counter(self) -> None:
        self.assertIn("when r.fecha is not null then case when coalesce(r.event_count, 0) > 1 then 1 else 0 end", self.sql)

    def test_legacy_fallback_and_power_app_precedence_remain(self) -> None:
        self.assertIn("else coalesce(l.raw_evidence_count, 0)", self.sql)
        self.assertIn("when r.fecha is not null or coalesce(l.tiene_kpione2, 0) = 1 then 0 else coalesce(l.power_app_fallback, 0)", self.sql)
        self.assertIn("coalesce(l.kpione1_audit_only, 0)", self.sql)

    def test_role_statement_has_exact_security_attributes_and_literal_password(self) -> None:
        self.assertIn('ALTER ROLE {} WITH LOGIN NOINHERIT PASSWORD {}', self.provisioner)
        self.assertIn("sql.Literal(password)", self.provisioner)
        self.assertNotIn("CONNECTION LIMIT", self.provisioner)
        for forbidden_clause in (
            "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE", "NOREPLICATION", "NOBYPASSRLS",
        ):
            self.assertNotIn(forbidden_clause, self.provisioner)

    def test_role_grants_are_allowlisted_without_ddl_or_maintenance(self) -> None:
        self.assertIn("GRANT CONNECT,TEMPORARY", self.provisioner)
        self.assertIn("GRANT USAGE ON SCHEMA cg_core,cg_mart", self.provisioner)
        self.assertIn("GRANT SELECT ON TABLE", self.provisioner)
        self.assertIn("GRANT INSERT,DELETE ON TABLE", self.provisioner)
        self.assertNotIn("GRANT UPDATE", self.provisioner)
        self.assertNotIn("GRANT TRUNCATE", self.provisioner)
        self.assertNotIn("GRANT CREATE", self.provisioner)
        self.assertNotIn("GRANT MAINTAIN", self.provisioner)
        self.assertNotIn("ALTER TABLE", self.provisioner)

    def test_bridge_applier_uses_full_plan_and_raw_sql_guards(self) -> None:
        source = BRIDGE_PATH.read_text(encoding="utf-8")
        for marker in (
            "plan_approved_git_sha_mismatch", "plan_worktree_blob_mismatch",
            "bridge_sql_sha256_mismatch", "bridge_not_exclusively_authorized",
            "admin_ddl_wrapper_profile_required", "bridge_evidence_already_exists",
        ):
            self.assertIn(marker, source)

    def test_privilege_snapshot_rejects_every_direct_drift_class(self) -> None:
        base = {
            "direct_grants": list(provisioner.EXPECTED_DIRECT_GRANTS),
            "memberships": [], "ownerships": [],
            "public_grants": [("schema", "public", "USAGE")],
        }
        provisioner.evaluate_privilege_snapshot(base)
        drifts = [
            ("relation", "public.fact_stock_venta", "SELECT"),
            ("schema", "inventory", "USAGE"),
            ("sequence", "cg_mart.extra_seq", "USAGE"),
            ("function", "public.extra()", "EXECUTE"),
            ("relation", "cg_mart.fact_cg_out_weekly_v2", "UPDATE"),
        ]
        for drift in drifts:
            mutated = copy.deepcopy(base)
            mutated["direct_grants"].append(drift)
            with self.assertRaisesRegex(provisioner.ProvisioningError, "direct_grant_drift"):
                provisioner.evaluate_privilege_snapshot(mutated)

    def test_privilege_snapshot_rejects_membership_ownership_and_bad_public(self) -> None:
        base = {"direct_grants": list(provisioner.EXPECTED_DIRECT_GRANTS), "memberships": [], "ownerships": [], "public_grants": []}
        for field, value, error in (
            ("memberships", ["writer_role"], "membership"),
            ("ownerships", [("relation", "cg_mart.x")], "ownership"),
            ("public_grants", [("relation", "public.x", "UPDATE")], "public_effective"),
            ("public_grants", [("procedure", "public.p", "EXECUTE_PROCEDURE")], "public_effective"),
        ):
            mutated = copy.deepcopy(base); mutated[field] = value
            with self.assertRaisesRegex(provisioner.ProvisioningError, error):
                provisioner.evaluate_privilege_snapshot(mutated)

    def test_harmless_public_privileges_are_recorded_not_rejected(self) -> None:
        snapshot = {"direct_grants": list(provisioner.EXPECTED_DIRECT_GRANTS), "memberships": [], "ownerships": [], "public_grants": [("schema", "public", "USAGE")], "routine_privileges": [{"schema": "pg_catalog", "identity": "now()", "prokind": "f", "security_definer": False, "owner": "postgres", "source": "PUBLIC"}]}
        observed = provisioner.evaluate_privilege_snapshot(snapshot)
        self.assertEqual(len(observed["routine_privileges"]), 1)

    def test_all_non_system_routine_execute_sources_are_rejected(self) -> None:
        base = {"direct_grants": list(provisioner.EXPECTED_DIRECT_GRANTS), "memberships": [], "ownerships": [], "public_grants": []}
        for source, security_definer, prokind in (
            ("PUBLIC", True, "f"), ("PUBLIC", False, "f"),
            ("direct", False, "p"), ("membership", False, "a"),
        ):
            snapshot = {**base, "routine_privileges": [{"schema": "public", "identity": "danger(integer)", "prokind": prokind, "security_definer": security_definer, "owner": "postgres", "source": source}]}
            with self.assertRaisesRegex(provisioner.ProvisioningError, "non_system_routine_execute"):
                provisioner.evaluate_privilege_snapshot(snapshot)


class Phase023WrapperAndObserverTests(unittest.TestCase):
    def _app_snapshot(self):
        required = {"public.v_required"}
        return required, {
            "role_attributes": {"login": True, "superuser": False, "createdb": False, "createrole": False, "replication": False, "bypassrls": False},
            "memberships": [], "ownerships": [],
            "grants": [{"source": "direct", "object_type": "relation", "schema": "public", "identity": "public.v_required", "privilege": "SELECT"}],
        }

    def test_app_full_capability_snapshot_blocks_pending_allowlist(self) -> None:
        required, snapshot = self._app_snapshot()
        with self.assertRaisesRegex(observer.ReadonlyValidationError, "ALLOWLIST_NOT_FROZEN"):
            observer.evaluate_app_capability_snapshot(snapshot, required, required, observer.APP_ALLOWLIST_PENDING)

    def test_app_full_capability_snapshot_rejects_every_forbidden_class(self) -> None:
        required, base = self._app_snapshot()
        mutations = (
            ("database CREATE", {"source": "PUBLIC", "object_type": "database", "schema": "", "identity": "postgres", "privilege": "CREATE"}),
            ("schema CREATE", {"source": "direct", "object_type": "schema", "schema": "public", "identity": "public", "privilege": "CREATE"}),
            ("sequence", {"source": "membership", "object_type": "sequence", "schema": "public", "identity": "public.s", "privilege": "USAGE"}),
            ("routine", {"source": "PUBLIC", "object_type": "routine", "schema": "public", "identity": "public.f()", "privilege": "EXECUTE"}),
            ("write", {"source": "direct", "object_type": "relation", "schema": "public", "identity": "public.t", "privilege": "UPDATE"}),
        )
        for label, grant in mutations:
            snapshot = copy.deepcopy(base); snapshot["grants"].append(grant)
            with self.assertRaises(observer.ReadonlyValidationError, msg=label):
                observer.evaluate_app_capability_snapshot(snapshot, required, required, observer.APP_ALLOWLIST_FROZEN)
        for field, value in (("memberships", ["writer"]), ("ownerships", [("relation", "public.t")])):
            snapshot = copy.deepcopy(base); snapshot[field] = value
            with self.assertRaises(observer.ReadonlyValidationError):
                observer.evaluate_app_capability_snapshot(snapshot, required, required, observer.APP_ALLOWLIST_FROZEN)

    def test_app_full_capability_allows_system_public_and_requires_exact_selects(self) -> None:
        required, snapshot = self._app_snapshot()
        snapshot["grants"].append({"source": "PUBLIC", "object_type": "routine", "schema": "pg_catalog", "identity": "pg_catalog.now()", "privilege": "EXECUTE"})
        observed = observer.evaluate_app_capability_snapshot(snapshot, required, required, observer.APP_ALLOWLIST_FROZEN)
        self.assertEqual(observed["effective_select_objects"], ["public.v_required"])
        missing = copy.deepcopy(snapshot); missing["grants"] = missing["grants"][1:]
        with self.assertRaisesRegex(observer.ReadonlyValidationError, "required_control_gestion"):
            observer.evaluate_app_capability_snapshot(missing, required, required, observer.APP_ALLOWLIST_FROZEN)
    def test_wrapper_profiles_and_operations_are_explicit(self) -> None:
        source = WRAPPER_PATH.read_text(encoding="utf-8")
        for profile in ("cg-mart-refresh", "cg-mart-refresh-provisioning", "app-readonly"):
            self.assertIn(f"'{profile}'", source)
        for operation in (
            "readonly-baseline-023", "provision-cg-mart-refresh-023",
            "apply-route-b-bridge-023", "dry-run-june-refresh-023",
            "apply-june-refresh-023", "readonly-postcheck-023",
            "validate-app-readonly-023",
        ):
            self.assertIn(f"'{operation}'", source)
        self.assertIn("STOCK_ZERO_DB_CG_MART_REFRESH", source)
        self.assertIn("STOCK_ZERO_DB_CG_MART_REFRESH_PASSWORD", source)
        self.assertIn("STOCK_ZERO_DB_APP_RO", source)

    def test_wrapper_is_windows_powershell_compatible_and_does_not_render_secrets(self) -> None:
        source = WRAPPER_PATH.read_text(encoding="utf-8")
        self.assertIn("#Requires -Version 5.1", source)
        self.assertNotIn("Join-String", source)
        self.assertNotIn("Write-Host", source)
        self.assertNotIn("Write-Output", source)
        self.assertIn("EnvironmentVariables", source)

    def test_observer_modes_are_closed(self) -> None:
        self.assertEqual(observer.MODES, ("baseline", "postcheck", "app"))
        with self.assertRaises(SystemExit):
            observer.parser().parse_args([
                "--mode", "generic", "--expected-git-ref", "a" * 40,
                "--expected-project-ref", "x", "--confirm", "x",
                "--run-id", "x", "--report-json", "x",
            ])

    def test_daily_duplicate_and_null_fixtures_fail_grain_contract(self) -> None:
        metrics = observer.grain_metrics([
            ("2026-06-01", "R1", "C1"),
            ("2026-06-01", "R1", "C1"),
            ("2026-06-02", "", "C2"),
        ])
        self.assertEqual(metrics["duplicate_key_count"], 1)
        self.assertEqual(metrics["max_multiplicity"], 2)
        self.assertEqual(metrics["null_or_blank_key_rows"], 1)

    def test_weekly_duplicate_fixture_is_detected(self) -> None:
        metrics = observer.grain_metrics([
            ("2026-06-01", "R1", "C1"),
            ("2026-06-01", "R1", "C1"),
        ])
        self.assertEqual(metrics["row_count"], 2)
        self.assertEqual(metrics["distinct_grain_count"], 1)

    def test_no_secret_or_dsn_values_are_printed_by_023_entrypoints(self) -> None:
        for path in (BRIDGE_PATH, PROVISIONER_PATH, ROOT / "scripts" / "validate_control_gestion_route_b_023_read_only.py"):
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("print(dsn", source)
            self.assertNotIn("print(password", source)

    def test_observer_rejects_execution_flag_and_evidence_drift(self) -> None:
        plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
        plan["status"] = "BRIDGE_AND_REFRESH_COMMITTED_APP_VALIDATION_PENDING"
        plan["run_id"] = "12345678-1234-4123-8123-123456789abc"
        with self.assertRaisesRegex(observer.ReadonlyValidationError, "execution_state"):
            observer.validate_operational_state(plan, "postcheck")
        plan["execution_state"].update({"bridge_executed": True, "june_refresh_executed": True})
        with self.assertRaisesRegex(observer.ReadonlyValidationError, "evidence_missing"):
            observer.validate_operational_state(plan, "postcheck")

    def test_observer_verifies_evidence_paths_and_hashes(self) -> None:
        plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
        run_id = "12345678-1234-4123-8123-123456789abc"
        plan["status"] = "BRIDGE_AND_REFRESH_COMMITTED_APP_VALIDATION_PENDING"
        plan["run_id"] = run_id
        plan["execution_state"].update({"bridge_executed": True, "june_refresh_executed": True})
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            runtime = {}
            for key in ("03", "04"):
                relative = f"evidence/runtime/023/{run_id}/{plan['evidence_contract']['file_templates'][key]}"
                path = root / relative; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(key.encode())
                runtime[key] = {"path": relative, "raw_sha256": hashlib.sha256(key.encode()).hexdigest()}
            plan["runtime_evidence"] = runtime
            observer.validate_operational_state(plan, "postcheck", root)
            plan["runtime_evidence"]["04"]["raw_sha256"] = "0" * 64
            with self.assertRaisesRegex(observer.ReadonlyValidationError, "hash_mismatch"):
                observer.validate_operational_state(plan, "postcheck", root)

    def test_app_acl_exact_surface_and_public_separation(self) -> None:
        expected = {"public.v_one", "cg_mart.v_two"}
        direct = [(name, "SELECT") for name in expected]
        result = observer.evaluate_app_acl_snapshot(expected, direct, [("public.v_env", "SELECT")], [])
        self.assertEqual(len(result["public_environmental_grants"]), 1)
        for bad_direct, writes, public in (
            (direct + [("public.extra", "SELECT")], [], []),
            (direct, [("public.v_one", "UPDATE")], []),
            (direct, [], [("public.v_one", "DELETE")]),
        ):
            with self.assertRaises(observer.ReadonlyValidationError):
                observer.evaluate_app_acl_snapshot(expected, bad_direct, public, writes)

    def test_windows_51_quoting_helper_handles_empty_quotes_and_backslashes(self) -> None:
        wrapper_literal = str(WRAPPER_PATH).replace("'", "''")
        command = rf'''$tokens=$null;$errors=$null;$ast=[System.Management.Automation.Language.Parser]::ParseFile('{wrapper_literal}',[ref]$tokens,[ref]$errors);if($errors.Count){{throw $errors[0]}};$fn=$ast.Find({{param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'ConvertTo-StockZeroWindowsArgument'}},$true);Invoke-Expression $fn.Extent.Text;@('', 'a b', 'a"b', 'C:\tail\', 'C:\space \', 'a\\\"b')|ForEach-Object {{ ConvertTo-StockZeroWindowsArgument -Value $_ }}|ConvertTo-Json -Compress'''
        powershell = "powershell.exe" if os.name == "nt" else "pwsh"
        output = subprocess.run([powershell, "-NoProfile", "-Command", command], capture_output=True, text=True, check=True).stdout
        values = json.loads(output)
        self.assertEqual(values[0], '""')
        self.assertEqual(values[1], '"a b"')
        self.assertEqual(values[2], '"a\\"b"')
        self.assertEqual(values[3], "C:\\tail\\")
        self.assertEqual(values[4], '"C:\\space \\\\"')
        self.assertEqual(values[5], '"a' + ('\\' * 7) + '"b"')


class Phase023FrozenContractsTests(unittest.TestCase):
    EXPECTED_GIT_HASHES = {
        "scripts/provision_kpione_route_b_role.py": "44ae9d1837672e5bf26f3d9a8fa3e481e9cb851d",
        "scripts/precheck_kpione_route_b_018_read_only.py": "a7da8d60396f413702b6e49a2ca425e79cf2e881",
        "plans/018_kpione_route_b_productive_apply_plan.json": "1c6390d8c6aec00a063167a9657e8e77e8484e03",
        "scripts/load_control_gestion_raw_v17.py": "2ac41400453075181cd852976b61cb3bb6367a00",
        "scripts/refresh_control_gestion_v2_mv.py": "4e868aa2a1626e5869c2a362e182f05bcac45e89",
        "sql/07_control_gestion_v2_incremental_mart_draft.sql": "0b31748026d210a2952727d1c03ca2671961d133",
        "sql/08_control_gestion_v2_daily_intermediate_mart_draft.sql": "9904f63765ca2e2572c0e49237296066d4bd8037",
        "app/screens/control_gestion.py": "6a58ceab71a2bbb3d9a6e6ae70387a5aaa3d54b5",
    }

    def test_018_and_022_frozen_files_are_byte_identical_to_start(self) -> None:
        for relative, expected in self.EXPECTED_GIT_HASHES.items():
            observed = subprocess.run(
                ["git", "hash-object", relative], cwd=ROOT,
                capture_output=True, text=True, check=True,
            ).stdout.strip()
            self.assertEqual(observed, expected, relative)


class _FakeScalarResult:
    def __init__(self, row: tuple[str, ...]) -> None:
        self.row = row

    def one(self) -> tuple[str, ...]:
        return self.row


class _FakeAppConnection:
    def __init__(self, row: tuple[str, ...]) -> None:
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _statement) -> _FakeScalarResult:
        return _FakeScalarResult(self.row)


class _FakeAppEngine:
    def __init__(self, row: tuple[str, ...], *, connect_error: Exception | None = None, dispose_error: Exception | None = None) -> None:
        self.row = row
        self.connect_error = connect_error
        self.dispose_error = dispose_error
        self.disposed = False

    def connect(self) -> _FakeAppConnection:
        if self.connect_error is not None:
            raise self.connect_error
        return _FakeAppConnection(self.row)

    def dispose(self) -> None:
        self.disposed = True
        if self.dispose_error is not None:
            raise self.dispose_error


class Phase023PublicAppIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # app.db normally loads a local .env at import time. Patch that side
        # effect so this offline suite never reads local credentials.
        fake_dotenv = types.SimpleNamespace(load_dotenv=lambda *_args, **_kwargs: False)
        with mock.patch.dict(sys.modules, {"dotenv": fake_dotenv}):
            cls.app_db = importlib.import_module("app.db")

    def _assert_public_error_redacted(self, callback) -> None:
        sensitive = "postgresql://usuario:password@host:5432/postgres?sslmode=require"
        logs = io.StringIO()
        handler = self.app_db.logging.StreamHandler(logs)
        self.app_db.logger.addHandler(handler)
        try:
            with mock.patch.dict(os.environ, {"STOCKZERO_RUNTIME_ENV": "public"}, clear=False):
                with self.assertRaises(self.app_db.AppError) as captured:
                    callback(sensitive)
            rendered = "".join(traceback.format_exception(captured.exception))
            self.assertIsNone(captured.exception.__cause__)
            for forbidden in (sensitive, "usuario", "password", "host", "5432", "sslmode"):
                self.assertNotIn(forbidden, str(captured.exception))
                self.assertNotIn(forbidden, rendered)
                self.assertNotIn(forbidden, logs.getvalue())
        finally:
            self.app_db.logger.removeHandler(handler)

    def test_public_runtime_requires_db_url_app(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(self.app_db.AppError, "requires DB_URL_APP"):
                self.app_db._get_db_urls("public")

    def test_public_runtime_rejects_legacy_db_url(self) -> None:
        with mock.patch.dict(os.environ, {"DB_URL": "postgresql://legacy"}, clear=True):
            with self.assertRaisesRegex(self.app_db.AppError, "rejects legacy"):
                self.app_db._get_db_urls("public")

    def test_public_runtime_rejects_db_url_fallback(self) -> None:
        environment = {
            "DB_URL_APP": "postgresql://approved",
            "DB_URL_FALLBACK": "postgresql://fallback",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(self.app_db.AppError, "rejects legacy"):
                self.app_db._get_db_urls("public")

    def test_public_runtime_accepts_only_db_url_app(self) -> None:
        with mock.patch.dict(os.environ, {"DB_URL_APP": "postgresql://approved"}, clear=True):
            self.assertEqual(
                self.app_db._get_db_urls("public"),
                ("postgresql://approved", None),
            )

    def test_public_identity_rejects_wrong_current_user(self) -> None:
        engine = _FakeAppEngine(("postgres", "stock_zero_app_ro", "postgres", "none", "on"))
        with self.assertRaisesRegex(self.app_db.AppError, "contract mismatch"):
            self.app_db._enforce_runtime_database_identity(engine, "public")
        self.assertTrue(engine.disposed)

    def test_public_identity_rejects_different_session_user(self) -> None:
        engine = _FakeAppEngine(("stock_zero_app_ro", "postgres", "postgres", "none", "on"))
        with self.assertRaisesRegex(self.app_db.AppError, "contract mismatch"):
            self.app_db._enforce_runtime_database_identity(engine, "public")
        self.assertTrue(engine.disposed)

    def test_public_identity_rejects_transaction_read_only_off(self) -> None:
        engine = _FakeAppEngine((
            "stock_zero_app_ro", "stock_zero_app_ro", "postgres", "none", "off",
        ))
        with self.assertRaisesRegex(self.app_db.AppError, "contract mismatch"):
            self.app_db._enforce_runtime_database_identity(engine, "public")
        self.assertTrue(engine.disposed)

    def test_public_identity_accepts_exact_readonly_session(self) -> None:
        engine = _FakeAppEngine((
            "stock_zero_app_ro", "stock_zero_app_ro", "postgres", "none", "on",
        ))
        self.assertIs(self.app_db._enforce_runtime_database_identity(engine, "public"), engine)
        self.assertFalse(engine.disposed)

    def test_local_runtime_preserves_legacy_and_fallback_resolution(self) -> None:
        environment = {
            "DB_URL": "postgresql://local-primary",
            "DB_URL_FALLBACK": "postgresql://local-fallback",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            self.assertEqual(
                self.app_db._get_db_urls("local"),
                ("postgresql://local-primary", "postgresql://local-fallback"),
            )
        engine = _FakeAppEngine((), connect_error=AssertionError("must not connect"))
        self.assertIs(self.app_db._enforce_runtime_database_identity(engine, "local"), engine)

    def test_public_errors_do_not_leak_database_urls(self) -> None:
        sensitive = "postgresql://secret-user:secret-password@secret-host/postgres"
        with mock.patch.dict(os.environ, {"DB_URL": sensitive}, clear=True):
            with self.assertRaises(self.app_db.AppError) as captured:
                self.app_db._get_db_urls("public")
        self.assertNotIn(sensitive, str(captured.exception))
        self.assertNotIn("secret-password", str(captured.exception))

    def test_public_driver_exception_is_unchained_and_fully_redacted(self) -> None:
        sensitive = "postgresql://leak-user:leak-password@leak-host:6543/postgres?sslmode=require"
        logs = io.StringIO()
        handler = self.app_db.logging.StreamHandler(logs)
        self.app_db.logger.addHandler(handler)
        self.app_db._engine_cached.clear()
        try:
            with mock.patch.object(self.app_db, "create_engine", side_effect=RuntimeError(sensitive)):
                with self.assertRaises(self.app_db.AppError) as captured:
                    self.app_db._engine_cached(sensitive, "public")
            rendered = "".join(traceback.format_exception(captured.exception))
            self.assertIsNone(captured.exception.__cause__)
            for forbidden in (sensitive, "leak-user", "leak-password", "leak-host", "6543", "sslmode"):
                self.assertNotIn(forbidden, str(captured.exception))
                self.assertNotIn(forbidden, rendered)
                self.assertNotIn(forbidden, logs.getvalue())
        finally:
            self.app_db.logger.removeHandler(handler)
            self.app_db._engine_cached.clear()

    def test_real_engine_cache_keys_by_url_and_runtime_and_clear_rebuilds(self) -> None:
        self.app_db._engine_cached.clear()
        created: list[_FakeAppEngine] = []

        def factory(*_args, **_kwargs):
            engine = _FakeAppEngine((
                "stock_zero_app_ro", "stock_zero_app_ro", "postgres", "none", "on",
            ))
            created.append(engine)
            return engine

        try:
            with mock.patch.object(self.app_db, "create_engine", side_effect=factory):
                public_a = self.app_db._engine_cached("postgresql://safe-a", "public")
                self.assertIs(public_a, self.app_db._engine_cached("postgresql://safe-a", "public"))
                public_b = self.app_db._engine_cached("postgresql://safe-b", "public")
                local_a = self.app_db._engine_cached("postgresql://safe-a", "local")
                self.assertIsNot(public_a, public_b)
                self.assertIsNot(public_a, local_a)
                self.assertEqual(len(created), 3)
                self.app_db._engine_cached.clear()
                rebuilt = self.app_db._engine_cached("postgresql://safe-a", "public")
                self.assertIsNot(rebuilt, public_a)
                self.assertEqual(len(created), 4)
        finally:
            self.app_db._engine_cached.clear()

    def test_failed_identity_validation_is_not_cached_or_returned(self) -> None:
        sensitive = "postgresql://hidden:password@private-host/postgres"
        invalid = _FakeAppEngine(
            (), connect_error=RuntimeError(sensitive),
            dispose_error=RuntimeError(sensitive),
        )
        valid = _FakeAppEngine((
            "stock_zero_app_ro", "stock_zero_app_ro", "postgres", "none", "on",
        ))
        self.app_db._engine_cached.clear()
        try:
            with mock.patch.object(self.app_db, "create_engine", side_effect=[invalid, valid]) as factory:
                with self.assertRaises(self.app_db.AppError):
                    self.app_db._engine_cached("postgresql://same-cache-key", "public")
                observed = self.app_db._engine_cached("postgresql://same-cache-key", "public")
            self.assertIs(observed, valid)
            self.assertTrue(invalid.disposed)
            self.assertEqual(factory.call_count, 2)
        finally:
            self.app_db._engine_cached.clear()

    def test_identity_and_dispose_failures_preserve_sanitized_error(self) -> None:
        def exercise(sensitive: str) -> None:
            engine = _FakeAppEngine((), connect_error=RuntimeError(sensitive), dispose_error=RuntimeError(sensitive))
            self.app_db._enforce_runtime_database_identity(engine, "public")
        self._assert_public_error_redacted(exercise)

    def test_query_and_selector_cache_failures_are_redacted(self) -> None:
        for cache_name, callback in (
            ("qdf", lambda: self.app_db._qdf_cached("dv", "SELECT 1", None)),
            ("selector", lambda: self.app_db._selector_df_cached("name", "SELECT 1", None)),
        ):
            cached = self.app_db._qdf_cached if cache_name == "qdf" else self.app_db._selector_df_cached
            cached.clear()
            try:
                def exercise(sensitive: str) -> None:
                    engine = _FakeAppEngine(("unused",))
                    with mock.patch.object(self.app_db, "get_engine", return_value=engine), \
                         mock.patch.object(self.app_db.pd, "read_sql", side_effect=RuntimeError(sensitive)):
                        callback()
                self._assert_public_error_redacted(exercise)
            finally:
                cached.clear()

    def test_data_version_logs_and_structured_smokes_never_expose_driver_text(self) -> None:
        sensitive = "postgresql://usuario:password@host:5432/postgres?sslmode=require"
        logs = io.StringIO(); handler = self.app_db.logging.StreamHandler(logs)
        self.app_db.logger.addHandler(handler)
        self.app_db._get_data_version_info_cached.clear()
        try:
            with mock.patch.dict(os.environ, {"STOCKZERO_RUNTIME_ENV": "public"}, clear=False), \
                 mock.patch.object(self.app_db, "get_engine", return_value=_FakeAppEngine(("unused",))), \
                 mock.patch.object(self.app_db.pd, "read_sql", side_effect=RuntimeError(sensitive)):
                result = self.app_db._get_data_version_info_cached.__wrapped__()
            self.assertEqual(result, {"fecha_datos": None, "ingested_at": None})
            self.assertNotIn(sensitive, logs.getvalue())
            with mock.patch.object(self.app_db, "_selector_df", side_effect=RuntimeError(sensitive)):
                structured = [self.app_db.get_cg_contract_smoke(), self.app_db.get_cg_v2_contract_smoke()]
            rendered = json.dumps(structured)
            self.assertIn("DB_QUERY_FAILED", rendered)
            for forbidden in (sensitive, "usuario", "password", "host", "5432", "sslmode"):
                self.assertNotIn(forbidden, rendered)
        finally:
            self.app_db.logger.removeHandler(handler)
            self.app_db._get_data_version_info_cached.clear()


if __name__ == "__main__":
    unittest.main()
