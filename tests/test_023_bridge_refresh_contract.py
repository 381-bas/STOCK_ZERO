from __future__ import annotations

import contextlib
import copy
import hashlib
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


if __name__ == "__main__":
    unittest.main()
