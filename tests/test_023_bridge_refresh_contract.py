from __future__ import annotations

import contextlib
import copy
import hashlib
import importlib
import io
import json
import os
import subprocess
import sys
import tempfile
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
        with mock.patch.object(refresh, "_run_incremental_transaction") as core:
            with self.assertRaises(TypeError):
                refresh.run_incremental_apply(  # type: ignore[call-arg]
                    db_url="postgresql://forged", week_scope={"forged": True},
                    contract_023={"authorized": True}, statement_timeout_seconds=1,
                    post_apply_validate=True,
                )
        core.assert_not_called()


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


class _CoreCursor:
    def __init__(self, fail_weekly: bool = False) -> None:
        self.calls: list[str] = []
        self.rowcount = 1
        self.fail_weekly = fail_weekly
        self._one = None

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def execute(self, query, _params=None):
        rendered = str(query)
        self.calls.append(rendered)
        if "SELECT current_user,session_user" in rendered:
            self._one = (refresh.MART_REFRESH_ROLE, refresh.MART_REFRESH_ROLE, "postgres", "none", "off")
        if self.fail_weekly and rendered.lstrip().startswith("INSERT INTO cg_mart.fact_cg_out_weekly_v2"):
            raise RuntimeError("weekly failed")
    def fetchone(self): return self._one


class _CoreConnection:
    def __init__(self, fail_weekly: bool = False) -> None:
        self.cursor_value = _CoreCursor(fail_weekly)
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
    def set_session(self, **_kwargs): pass
    def cursor(self): return self.cursor_value
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1
    def close(self): self.closed += 1


class Phase023ApplyTransactionTests(unittest.TestCase):
    def _scope(self):
        dates = {refresh.date(2026, 6, 1)}
        weeks = {refresh.date(2026, 6, 1)}
        return refresh.build_week_scope(dates, weeks, 0)

    def _run(self, connection: _CoreConnection):
        fingerprints = {key: {"schema_version": refresh.FINGERPRINT_SCHEMA_VERSION, "row_count": 1, "sha256": hashlib.sha256(key.encode()).hexdigest()} for key in refresh.FINGERPRINT_KEYS}
        present = [{"semana_inicio": "2026-06-01", "scope_status": "present"}]
        with mock.patch.object(refresh, "_compute_content_fingerprints", return_value=fingerprints), \
             mock.patch.object(refresh, "_route_scope_by_week", return_value=present), \
             mock.patch.object(refresh, "_count_temp_rows", return_value=1), \
             mock.patch.object(refresh, "_run_daily_check", return_value={"validation_status": "ok"}), \
             mock.patch.object(refresh, "_run_weekly_fact_validation", return_value={"validation_status": "ok"}):
            return refresh._run_incremental_transaction(
                db_url="postgresql://not-opened", week_scope=self._scope(),
                statement_timeout_seconds=1, post_apply_validate=True,
                advisory_lock_key=1, expected_content_fingerprints=fingerprints,
                _marker=refresh._INTERNAL_APPLY_MARKER,
                _connect_fn=lambda _dsn: connection,
            )

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
        self.assertEqual(result["commit_state"], refresh.COMMITTED_EVIDENCE_PENDING)

    def test_postcommit_evidence_failure_is_recovery_not_reapply(self) -> None:
        run_id = "12345678-1234-4123-8123-123456789abc"
        fingerprints = {key: {"schema_version": refresh.FINGERPRINT_SCHEMA_VERSION, "row_count": 1, "sha256": hashlib.sha256(key.encode()).hexdigest()} for key in refresh.FINGERPRINT_KEYS}
        plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
        plan["dry_run_authorization"] = {"raw_sha256": "ignored", "scope_sha256": plan["scope"]["scope_sha256"], "content_fingerprints": fingerprints}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, dir=tempfile.gettempdir(), encoding="utf-8") as handle:
            json.dump({"content_fingerprints": fingerprints}, handle)
            dry_path = Path(handle.name)
        raw_sha = hashlib.sha256(dry_path.read_bytes()).hexdigest()
        plan["dry_run_authorization"]["raw_sha256"] = raw_sha
        committed = {"status": "ok", "final_status": "apply_ok", "committed": True, "rolled_back": False, "content_fingerprints": fingerprints}
        try:
            with mock.patch.object(refresh, "PLAN_023_DEFAULT", PLAN_PATH), \
                 mock.patch.object(refresh, "_load_plan_023", return_value=(plan, {"scope_sha256": plan["scope"]["scope_sha256"], "approved_git_sha": "a" * 40})), \
                 mock.patch.object(refresh, "_validate_wrapper_marker"), \
                 mock.patch.object(refresh, "_validate_productive_dsn"), \
                 mock.patch.object(refresh, "_canonical_evidence_path", return_value=ROOT / "not-written.json"), \
                 mock.patch.object(refresh, "_run_incremental_transaction", return_value=committed) as transaction, \
                 mock.patch.object(refresh, "write_json_exclusive", side_effect=OSError("disk")), \
                 mock.patch.object(refresh, "write_committed_recovery_receipt", return_value=Path(tempfile.gettempdir()) / "receipt.json"), \
                 mock.patch.dict(os.environ, {refresh.MART_REFRESH_ENV: "hidden"}, clear=False):
                result = refresh.run_incremental_apply(
                    plan_path=PLAN_PATH, confirm=refresh.APPLY_CONFIRM_TOKEN, run_id=run_id,
                    evidence_json=Path("unused"), dry_run_report_json=dry_path,
                    statement_timeout_seconds=1, post_apply_validate=True,
                )
            self.assertEqual(transaction.call_count, 1)
            self.assertTrue(result["committed"])
            self.assertFalse(result["rolled_back"])
            self.assertEqual(result["verdict"], refresh.COMMITTED_EVIDENCE_RECOVERY_REQUIRED)
        finally:
            dry_path.unlink(missing_ok=True)


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
        with mock.patch.object(sys, "argv", argv), mock.patch.object(bridge, "_load_plan", return_value={}), mock.patch.object(bridge, "validate_git_guard", return_value={"approved_git_sha": "a" * 40, "sql_raw_sha256": "b" * 64}), mock.patch.object(bridge, "_canonical_evidence_path", return_value=ROOT / "never.json"), mock.patch.object(bridge, "validate_dsn"), mock.patch.object(bridge, "apply_bridge", return_value=committed), mock.patch.object(bridge, "write_json_exclusive", side_effect=OSError("disk")), mock.patch.object(bridge, "write_committed_recovery_receipt", return_value=Path(tempfile.gettempdir()) / "receipt.json"), mock.patch.dict(os.environ, {"STOCK_ZERO_OPERATION_PROFILE": "admin-ddl", "STOCK_ZERO_OPERATION": "apply-route-b-bridge-023", "DB_URL_ADMIN": "hidden"}), contextlib.redirect_stdout(output):
            self.assertEqual(bridge.main(), 3)
        self.assertNotIn("PASS_ROUTE_B_APP_BRIDGE_APPLY", output.getvalue())
        self.assertIn(refresh.COMMITTED_EVIDENCE_RECOVERY_REQUIRED, output.getvalue())


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
        snapshot = {"direct_grants": list(provisioner.EXPECTED_DIRECT_GRANTS), "memberships": [], "ownerships": [], "public_grants": [("schema", "public", "USAGE"), ("function", "public.safe()", "EXECUTE")]}
        observed = provisioner.evaluate_privilege_snapshot(snapshot)
        self.assertEqual(len(observed["public_grants"]), 2)


class Phase023WrapperAndObserverTests(unittest.TestCase):
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
        command = rf'''$tokens=$null;$errors=$null;$ast=[System.Management.Automation.Language.Parser]::ParseFile('{wrapper_literal}',[ref]$tokens,[ref]$errors);if($errors.Count){{throw $errors[0]}};$fn=$ast.Find({{param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'ConvertTo-StockZeroWindowsArgument'}},$true);Invoke-Expression $fn.Extent.Text;@('', 'a b', 'a"b', 'C:\tail\', 'C:\space \')|ForEach-Object {{ ConvertTo-StockZeroWindowsArgument -Value $_ }}|ConvertTo-Json -Compress'''
        powershell = "powershell.exe" if os.name == "nt" else "pwsh"
        output = subprocess.run([powershell, "-NoProfile", "-Command", command], capture_output=True, text=True, check=True).stdout
        values = json.loads(output)
        self.assertEqual(values[0], '""')
        self.assertEqual(values[1], '"a b"')
        self.assertEqual(values[2], '"a\\"b"')
        self.assertEqual(values[3], "C:\\tail\\")
        self.assertEqual(values[4], '"C:\\space \\\\"')


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
    def __init__(self, row: tuple[str, ...], *, connect_error: Exception | None = None) -> None:
        self.row = row
        self.connect_error = connect_error
        self.disposed = False

    def connect(self) -> _FakeAppConnection:
        if self.connect_error is not None:
            raise self.connect_error
        return _FakeAppConnection(self.row)

    def dispose(self) -> None:
        self.disposed = True


class Phase023PublicAppIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # app.db normally loads a local .env at import time. Patch that side
        # effect so this offline suite never reads local credentials.
        with mock.patch("dotenv.load_dotenv", return_value=False):
            cls.app_db = importlib.import_module("app.db")

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


if __name__ == "__main__":
    unittest.main()
