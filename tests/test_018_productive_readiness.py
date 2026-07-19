from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from contextlib import redirect_stdout
from unittest import mock

from scripts import precheck_kpione_route_b_018_read_only as precheck
from scripts import run_kpione_route_b_ingestion_v1 as productive_runner
from scripts.kpione_route_b_v1 import (
    PLANNED_PRODUCTIVE_ROLE,
    PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_BLOCKERS,
    PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_STATUS,
    PRODUCTIVE_APPLY_RECORDED_GATE_CLOSED_STATUS,
    PRODUCTIVE_INFRASTRUCTURE_EVIDENCE_COMPONENTS,
    PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED_BLOCKERS,
    PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED_STATUS,
    RouteBError,
    WorkbookPlan,
    classify_global_photo_duplicates,
    inspect_workbook,
    productive_blocked_report,
    require_productive_gate_open,
    run_productive_apply,
    run_productive_rollback,
    semantic_content_hash,
    validate_productive_git_guard,
    validate_productive_local_artifacts,
    validate_productive_dsn_target,
    validate_productive_role_contract,
    validate_productive_state_transition,
    validate_registered_productive_target,
)


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "plans" / "018_kpione_route_b_productive_apply_plan.json"


def synthetic_workbook(source_hash: str, name: str, rows: list[dict[str, object]]) -> WorkbookPlan:
    return WorkbookPlan(
        Path(name), source_hash, name, "Fotos", 1, "2026-06-01", "2026-06-01",
        tuple(rows), len({row["event_id"] for row in rows}), 1, 0, (),
    )


def synthetic_row(row_number: int, event_id: str = "E1", photo_hash: str = "P1") -> dict[str, object]:
    return {
        "source_row_number": row_number,
        "source_row_identity": hashlib.sha256(f"row:{row_number}".encode()).hexdigest(),
        "event_id": event_id,
        "sp_item_id": "S1",
        "event_stable_hash": "H1",
        "photo_row_hash": photo_hash,
        "fecha": "2026-06-01",
        "location_key": "L1",
        "cliente_norm": "C1",
        "duplicate_classification": "UNIQUE",
        "conflict_classification": "NONE",
    }


class FakeCursor:
    def __init__(self) -> None:
        self.rows: list[tuple[object, ...]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, _params=None) -> None:
        normalized = " ".join(sql.split()).lower()
        if "select current_user,session_user" in normalized:
            self.rows = [("stock_zero_codex_ro", "stock_zero_codex_ro", "on", "16.4", "postgres")]
        elif "from information_schema.schemata" in normalized:
            self.rows = [("cg_core",), ("cg_raw",)]
        elif "from pg_available_extensions" in normalized:
            self.rows = []
        elif "from pg_class c join pg_namespace" in normalized:
            self.rows = []
        elif "to_regclass('cg_raw.kpione2_raw')" in normalized:
            self.rows = [("cg_raw.kpione2_raw",)]
        elif "has_schema_privilege" in normalized:
            self.rows = [(True, True)]
        else:
            self.rows = []

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class FakeConnection:
    def __init__(self) -> None:
        self.autocommit = True
        self.rollback_called = False
        self.close_called = False

    def cursor(self) -> FakeCursor:
        return FakeCursor()

    def rollback(self) -> None:
        self.rollback_called = True

    def close(self) -> None:
        self.close_called = True


class FakeProductiveDatabase:
    def __init__(self, plan: dict[str, object]) -> None:
        self.plan = plan
        self.state: dict[str, object] = {
            "objects_exist": True,
            "signatures_match": True,
            "batches": {},
            "files": [],
            "staging": [],
        }
        self.commands: list[str] = []
        self.connection_count = 0
        self.role = PLANNED_PRODUCTIVE_ROLE
        self.write_session_readonly = False
        self.reject_postcheck = False
        self.rollback_staging_mismatch = False
        self.declared_boolean_failures: set[int] = set()
        self.legacy_object: str | None = "cg_raw.kpione2_raw"

    def connect(self, _dsn: str):
        self.connection_count += 1
        return FakeProductiveDbConnection(self)


class FakeProductiveDbConnection:
    def __init__(self, database: FakeProductiveDatabase) -> None:
        self.database = database
        self.snapshot = copy.deepcopy(database.state)
        self.autocommit = True
        self.readonly = False
        self.closed = False

    def cursor(self):
        return FakeProductiveDbCursor(self)

    def commit(self) -> None:
        self.snapshot = copy.deepcopy(self.database.state)

    def rollback(self) -> None:
        self.database.state.clear()
        self.database.state.update(copy.deepcopy(self.snapshot))

    def close(self) -> None:
        self.closed = True


class FakeProductiveDbCursor:
    def __init__(self, connection: FakeProductiveDbConnection) -> None:
        self.connection = connection
        self.database = connection.database
        self.rows: list[tuple[object, ...]] = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def _batch_counts(self, batch_id: str) -> tuple[int, ...]:
        rows = [row for row in self.database.state["staging"] if row[0] == batch_id]
        unique = [row for row in rows if row[13] == "UNIQUE"]
        exact = [row for row in rows if row[13] == "EXACT_DUPLICATE"]
        cross = [row for row in rows if row[13] == "CROSS_FILE_DUPLICATE"]
        events = {row[5] for row in unique}
        presence = {(row[10], row[11], row[12]) for row in unique}
        conflicts = [row for row in rows if row[14] != "NONE"]
        values = (len(rows), len(unique), len(exact), len(cross), len(exact) + len(cross),
                  len(events), len(presence), len(conflicts))
        if self.connection.readonly and self.database.reject_postcheck:
            values = (values[0], values[1] + 1, *values[2:])
        return values

    def _declared_postcheck_rows(self, index: int) -> list[tuple[object, ...]]:
        batches = self.database.state["batches"]
        staging = self.database.state["staging"]
        files = self.database.state["files"]
        active_count = sum(item["status"] == "ACTIVE" for item in batches.values())
        if index == 0:
            return [(name,) for name in self.database.plan["physical_contract"]["objects"]]
        if index == 1:
            return [(batch_id, item["status"]) for batch_id, item in batches.items()]
        if index == 2:
            return [(row[0], row[1]) for row in files]
        if index == 3:
            return [(batch_id, sum(row[0] == batch_id for row in staging)) for batch_id in batches]
        if index == 4:
            return [(sum(row[13] == "EXACT_DUPLICATE" for row in staging),)]
        if index == 5:
            return [(sum(row[13] == "CROSS_FILE_DUPLICATE" for row in staging),)]
        if index == 6:
            return [(sum(row[13] != "UNIQUE" for row in staging),)]
        if index in {7, 10, 11}:
            return [(index not in self.database.declared_boolean_failures,)]
        if index == 8:
            return [(0,)]
        if index == 9:
            return [("NONE", len(staging))]
        if index == 12:
            return [(active_count,)]
        if index == 13:
            return [(0,)]
        if index == 14:
            return [(self.database.legacy_object,)]
        raise AssertionError(f"unsupported_declared_postcheck_index:{index}")

    def execute(self, sql: str, params=None) -> None:
        normalized = " ".join(sql.split()).lower()
        self.database.commands.append(normalized)
        self.rows = []
        self.rowcount = -1
        batches = self.database.state["batches"]
        declared_queries = {
            " ".join(query.split()).lower(): index
            for index, query in enumerate(self.database.plan["postcheck_queries"])
        }
        if normalized == "begin read only":
            self.connection.readonly = True
        elif normalized.startswith("select current_user,session_user,current_database()"):
            readonly = "on" if (self.connection.readonly or self.database.write_session_readonly) else "off"
            self.rows = [(self.database.role, self.database.role, "postgres", readonly)]
        elif normalized.startswith("set local") or "pg_advisory_xact_lock" in normalized:
            pass
        elif "join pg_attribute" in normalized and "not a.attisdropped" in normalized:
            expected = self.database.plan["physical_contract"]["object_signatures"]
            self.rows = [
                (name, column)
                for name, spec in sorted(expected.items())
                for column in spec["columns"]
            ]
        elif "from pg_class c join pg_namespace" in normalized:
            if self.database.state["objects_exist"]:
                expected = self.database.plan["physical_contract"]["object_signatures"]
                self.rows = [(name, spec["relation_kind"]) for name, spec in sorted(expected.items())]
                if not self.database.state["signatures_match"]:
                    self.rows = self.rows[:-1]
        elif normalized.startswith("create schema if not exists cg_raw"):
            self.database.state["objects_exist"] = True
        elif normalized.startswith("insert into cg_raw.kpione_raw_ingest_batch_v1"):
            batch_id = params[0]
            batches[batch_id] = {"status": "STAGING", "predecessor": None}
            self.rowcount = 1
        elif normalized.startswith("select batch_id::text from cg_raw.kpione_raw_ingest_batch_v1"):
            self.rows = [(batch_id,) for batch_id, item in batches.items() if item["status"] == "ACTIVE"]
        elif normalized in declared_queries:
            self.rows = self._declared_postcheck_rows(declared_queries[normalized])
        elif normalized.startswith("select count(*),count(*) filter"):
            self.rows = [self._batch_counts(params[0])]
        elif normalized.startswith("select count(*) from (select event_id,photo_row_hash"):
            self.rows = [(0,)]
        elif normalized.startswith("select count(*) from cg_raw.kpione_raw_ingest_batch_file_v1 where batch_id"):
            self.rows = [(sum(row[0] == params[0] for row in self.database.state["files"]),)]
        elif normalized.startswith("update cg_raw.kpione_raw_ingest_batch_v1 set status='active'"):
            item = batches.get(params[0])
            if item and item["status"] in {"STAGING", "SUPERSEDED"}:
                item["status"] = "ACTIVE"
                self.rowcount = 1
            else:
                self.rowcount = 0
        elif normalized.startswith("select status,supersedes_batch_id::text"):
            item = batches.get(params[0])
            self.rows = [(item["status"], item["predecessor"])] if item else []
        elif normalized.startswith("select count(*) from cg_raw.kpione_raw_event_photo_staging_v1 where batch_id"):
            item = batches.get(params[0])
            count = sum(row[0] == params[0] for row in self.database.state["staging"])
            if item and item["status"] == "ROLLED_BACK" and self.database.rollback_staging_mismatch:
                count = 0
            self.rows = [(count,)]
        elif normalized.startswith("select status from cg_raw.kpione_raw_ingest_batch_v1"):
            item = batches.get(params[0])
            self.rows = [(item["status"],)] if item else []
        elif normalized.startswith("update cg_raw.kpione_raw_ingest_batch_v1 set status='rolled_back'"):
            item = batches.get(params[0])
            if item and item["status"] == "ACTIVE":
                item["status"] = "ROLLED_BACK"
                self.rowcount = 1
            else:
                self.rowcount = 0
        elif normalized.startswith("select count(*) from cg_raw.kpione_raw_ingest_batch_v1 where status='active'"):
            self.rows = [(sum(item["status"] == "ACTIVE" for item in batches.values()),)]
        elif "join cg_raw.kpione_raw_ingest_batch_v1 b" in normalized:
            self.rows = [(0,)]
        elif "to_regclass('cg_raw.kpione2_raw')" in normalized:
            self.rows = [(self.database.legacy_object,)]
        else:
            if normalized.startswith("select"):
                raise AssertionError(f"unsupported_fake_productive_sql:{normalized}")

    def executemany(self, sql: str, rows) -> None:
        normalized = " ".join(sql.split()).lower()
        self.database.commands.append(normalized)
        materialized = list(rows)
        if "kpione_raw_ingest_batch_file_v1" in normalized:
            self.database.state["files"].extend(materialized)
        elif "kpione_raw_event_photo_staging_v1" in normalized:
            self.database.state["staging"].extend(materialized)
        self.rowcount = len(materialized)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class ProductiveReadiness018Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))

    @staticmethod
    def infrastructure_evidence_fixture() -> dict[str, object]:
        return {
            "status": "PASSED",
            "bundle_sha256": "d" * 64,
            "components": {
                "readonly_baseline_precheck": "c" * 64,
                "admin_provisioning": "e" * 64,
                "productive_role_verification": "f" * 64,
                "readonly_postcheck": "1" * 64,
            },
        }

    def productive_unit_fixture(self) -> tuple[dict[str, object], dict[str, object], str, dict[str, str]]:
        plan = copy.deepcopy(self.plan)
        plan["status"] = "READY_FOR_PRODUCTIVE_EXECUTION"
        plan["remaining_blockers"] = []
        plan["readonly_precheck"] = {"status": "PASSED", "evidence_sha256": "c" * 64}
        plan["infrastructure_evidence"] = self.infrastructure_evidence_fixture()
        plan["target"]["productive_role_status"] = "PROVISIONED_AND_VERIFIED"
        plan["target"]["allowed_productive_roles"] = [PLANNED_PRODUCTIVE_ROLE]
        plan["activation_gate"]["productive_role_registered"] = True
        plan["activation_gate"]["gate_open"] = True
        plan["productive_apply_authorized"] = True
        plan["productive_rollback_authorized"] = True
        first = synthetic_workbook("a" * 64, "first.xlsx", [synthetic_row(2)])
        second = synthetic_workbook("b" * 64, "second.xlsx", [synthetic_row(2)])
        classified = classify_global_photo_duplicates([first, second])
        plan["source_package"].update({
            "approved_file_count": 2,
            "expected_source_rows": 2,
            "expected_distinct_events": 1,
            "expected_duplicate_photo_rows": 1,
            "expected_exact_duplicate_rows": 0,
            "expected_cross_file_duplicate_rows": 1,
            "expected_event_conflicts": 0,
            "expected_day_presence_rows": 1,
        })
        approved = {
            "files": [
                {"source_file_sha256": first.source_file_sha256},
                {"source_file_sha256": second.source_file_sha256},
            ],
            "_workbooks": [first, second],
            "_classified_rows": classified,
        }
        role = PLANNED_PRODUCTIVE_ROLE
        password = "synthetic-test-password"
        hostname = plan["target"]["expected_hostname"]
        dsn = "postgresql://" + f"{role}:{password}@" + f"{hostname}/postgres?sslmode=require"
        git_guard = {
            "approved_git_sha": "a" * 40,
            "plan_path": "plans/018_kpione_route_b_productive_apply_plan.json",
            "plan_sha256": "b" * 64,
        }
        return plan, approved, dsn, git_guard

    def provisioned_gate_closed_fixture(self) -> dict[str, object]:
        plan = copy.deepcopy(self.plan)
        plan["status"] = PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED_STATUS
        plan["remaining_blockers"] = copy.deepcopy(
            PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED_BLOCKERS
        )
        plan["readonly_precheck"] = {"status": "PASSED", "evidence_sha256": "c" * 64}
        plan["infrastructure_evidence"] = self.infrastructure_evidence_fixture()
        plan["target"]["productive_role_status"] = "PROVISIONED_AND_VERIFIED"
        plan["target"]["allowed_productive_roles"] = [PLANNED_PRODUCTIVE_ROLE]
        plan["activation_gate"]["productive_role_registered"] = True
        plan["activation_gate"]["gate_open"] = False
        plan["productive_apply_authorized"] = False
        plan["productive_rollback_authorized"] = False
        return plan

    def preparation_fixture(self) -> dict[str, object]:
        plan = copy.deepcopy(self.plan)
        plan["status"] = "TECHNICAL_BOUNDARY_READY_ROLE_PROVISIONING_PENDING"
        plan["remaining_blockers"] = [
            "PRODUCTIVE_ROLE_NOT_PROVISIONED_OR_VERIFIED",
            "READ_ONLY_PRECHECK_NOT_AUTHORIZED_OR_EXECUTED",
        ]
        plan["readonly_precheck"] = {
            "status": "NOT_AUTHORIZED_OR_EXECUTED",
            "evidence_sha256": None,
        }
        plan.pop("infrastructure_evidence", None)
        plan["target"]["productive_role_status"] = "PLANNED_NOT_PROVISIONED"
        plan["target"]["allowed_productive_roles"] = []
        plan["activation_gate"]["productive_role_registered"] = False
        plan["activation_gate"]["gate_open"] = False
        plan["productive_apply_authorized"] = False
        plan["productive_rollback_authorized"] = False
        return plan

    def init_git_fixture(self, folder: str) -> tuple[Path, Path, str]:
        root = Path(folder)
        plan_path = root / "plan.json"
        plan_path.write_text('{"document_type":"test"}\n', encoding="utf-8")
        subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "tests@example.invalid"], cwd=root, check=True)
        subprocess.run(["git", "config", "user.name", "Tests"], cwd=root, check=True)
        subprocess.run(["git", "config", "core.autocrlf", "false"], cwd=root, check=True)
        subprocess.run(["git", "add", "plan.json"], cwd=root, check=True)
        subprocess.run(["git", "commit", "-m", "fixture"], cwd=root, check=True, capture_output=True)
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        return root, plan_path, head

    def test_plan_records_committed_apply_and_closes_reapply_gate(self) -> None:
        self.assertEqual(
            self.plan["status"], PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_STATUS,
        )
        self.assertTrue(self.plan["source_package"]["approved_for_apply"])
        self.assertTrue(self.plan["productive_apply_executed"])
        self.assertFalse(self.plan["productive_apply_authorized"])
        self.assertFalse(self.plan["productive_rollback_authorized"])
        self.assertFalse(self.plan["activation_gate"]["gate_open"])
        self.assertEqual(
            self.plan["remaining_blockers"],
            PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_BLOCKERS,
        )
        self.assertFalse(self.plan["evidence_closure_complete"])
        self.assertTrue(self.plan["supplemental_reattestation_required"])
        self.assertFalse(self.plan["bridge_executed"])
        self.assertFalse(self.plan["mart_refresh_executed"])
        self.assertFalse(self.plan["app_validation_executed"])
        self.assertEqual(self.plan["infrastructure_evidence"]["status"], "PASSED")
        self.assertEqual(self.plan["readonly_precheck"], {
            "status": "PASSED",
            "evidence_sha256": "162f9cbf28628eaf58e68bc7f2ac2412a82ece4d1a48a7774c08039d16d06dc3",
        })
        self.assertEqual(
            self.plan["future_productive_command"]["status"],
            PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_STATUS,
        )
        boundary = self.plan["future_productive_command"]["minimum_runner_change_required"]
        for required in (
            "Productive apply is committed",
            "reapply is closed",
            "Supplemental read-only reattestation",
            "bridge, mart refresh, app validation and rollback remain unauthorized",
        ):
            self.assertIn(required, boundary)
        requirements = self.plan["git_reference_contract"]["requirements"]
        self.assertIn(
            "authorization base SHA equals 61a2cb60209e21fe74a533c06d4cdacd8b8fdb9a",
            requirements,
        )
        self.assertTrue(any(item.startswith("authorization timestamp UTC is ") for item in requirements))
        self.assertTrue(any("a554f464-8213-442c-bf0b-d06b7acc26ca" in item for item in requirements))
        baseline = json.loads((ROOT / "evidence/runtime/020B/a554f464-8213-442c-bf0b-d06b7acc26ca/01_readonly_baseline.json").read_text(encoding="utf-8"))
        postcheck = json.loads((ROOT / "evidence/runtime/020B/a554f464-8213-442c-bf0b-d06b7acc26ca/04_readonly_postcheck.json").read_text(encoding="utf-8"))
        self.assertEqual((baseline["active_batch_count"], postcheck["active_batch_count"]), (0, 0))
        self.assertEqual(validate_productive_role_contract(self.plan), PLANNED_PRODUCTIVE_ROLE)
        with self.assertRaisesRegex(RouteBError, "apply_productive_gate_closed"):
            require_productive_gate_open(self.plan, "apply_productive")

    def test_preparation_rejects_passed_bundle_or_provisioned_role(self) -> None:
        preparation = self.preparation_fixture()
        self.assertEqual(validate_productive_role_contract(preparation), PLANNED_PRODUCTIVE_ROLE)
        precheck.validate_plan_readiness(preparation, "baseline")

        with_bundle = copy.deepcopy(preparation)
        with_bundle["infrastructure_evidence"] = self.infrastructure_evidence_fixture()
        with self.assertRaisesRegex(RouteBError, "productive_execution_state_contract_mismatch"):
            validate_productive_role_contract(with_bundle)

        with_role = copy.deepcopy(preparation)
        with_role["target"]["productive_role_status"] = "PROVISIONED_AND_VERIFIED"
        with_role["target"]["allowed_productive_roles"] = [PLANNED_PRODUCTIVE_ROLE]
        with_role["activation_gate"]["productive_role_registered"] = True
        with self.assertRaisesRegex(RouteBError, "productive_execution_state_contract_mismatch"):
            validate_productive_role_contract(with_role)

    def test_productive_role_is_registered_and_apply_gate_is_closed(self) -> None:
        target = self.plan["target"]
        self.assertEqual(target["planned_productive_role"], PLANNED_PRODUCTIVE_ROLE)
        self.assertEqual(target["productive_role_status"], "PROVISIONED_AND_VERIFIED")
        self.assertEqual(target["allowed_productive_roles"], [PLANNED_PRODUCTIVE_ROLE])
        self.assertTrue(self.plan["activation_gate"]["productive_role_registered"])
        self.assertFalse(self.plan["activation_gate"]["gate_open"])
        self.assertFalse(self.plan["productive_apply_authorized"])
        self.assertTrue(self.plan["productive_apply_executed"])
        self.assertFalse(self.plan["productive_rollback_authorized"])
        self.assertEqual(validate_productive_role_contract(self.plan), PLANNED_PRODUCTIVE_ROLE)

    def test_productive_role_gate_state_machine_and_mode_authorization(self) -> None:
        self.assertEqual(validate_productive_role_contract(self.plan), PLANNED_PRODUCTIVE_ROLE)
        closed_report = productive_blocked_report(self.plan, "apply_productive")
        self.assertFalse(closed_report["dsn_read"])
        self.assertFalse(closed_report["connection_attempted"])

        execution = copy.deepcopy(self.plan)
        execution["status"] = "READY_FOR_PRODUCTIVE_EXECUTION"
        execution["remaining_blockers"] = []
        execution["readonly_precheck"] = {"status": "PASSED", "evidence_sha256": "c" * 64}
        execution["infrastructure_evidence"] = self.infrastructure_evidence_fixture()
        execution["target"]["productive_role_status"] = "PROVISIONED_AND_VERIFIED"
        execution["target"]["allowed_productive_roles"] = [PLANNED_PRODUCTIVE_ROLE]
        execution["activation_gate"]["productive_role_registered"] = True
        execution["activation_gate"]["gate_open"] = True
        execution["productive_apply_authorized"] = True
        self.assertEqual(validate_productive_role_contract(execution), PLANNED_PRODUCTIVE_ROLE)
        require_productive_gate_open(execution, "apply_productive")

        execution["productive_apply_authorized"] = False
        execution["productive_rollback_authorized"] = True
        require_productive_gate_open(execution, "rollback_productive")

        invalid_execution_states = []
        for mutation in (
            lambda plan: plan.pop("infrastructure_evidence"),
            lambda plan: plan["infrastructure_evidence"]["components"].pop(
                "productive_role_verification"
            ),
            lambda plan: plan.update({"remaining_blockers": ["READ_ONLY_PRECHECK_NOT_AUTHORIZED_OR_EXECUTED"]}),
            lambda plan: plan.update({"status": "TECHNICAL_BOUNDARY_READY_ROLE_PROVISIONING_PENDING"}),
            lambda plan: plan.update({"readonly_precheck": {
                "status": "NOT_AUTHORIZED_OR_EXECUTED", "evidence_sha256": None,
            }}),
            lambda plan: plan.update({"readonly_precheck": {"status": "PASSED", "evidence_sha256": None}}),
            lambda plan: plan.update({"readonly_precheck": {"status": "PASSED", "evidence_sha256": "A" * 64}}),
            lambda plan: plan["target"].update({"allowed_productive_roles": []}),
            lambda plan: plan["target"].update({
                "allowed_productive_roles": [PLANNED_PRODUCTIVE_ROLE, "second_role"],
            }),
            lambda plan: plan["activation_gate"].update({"gate_open": False}),
            lambda plan: plan["activation_gate"].update({"productive_role_registered": False}),
        ):
            hybrid = copy.deepcopy(execution)
            mutation(hybrid)
            invalid_execution_states.append(hybrid)
        for index, hybrid in enumerate(invalid_execution_states):
            with self.subTest(hybrid=index), self.assertRaisesRegex(
                RouteBError, "productive_execution_state_contract_mismatch"
            ):
                validate_productive_role_contract(hybrid)

        unauthorized = copy.deepcopy(execution)
        unauthorized["productive_apply_authorized"] = False
        unauthorized["productive_rollback_authorized"] = False
        with self.assertRaisesRegex(RouteBError, "productive_apply_not_authorized"):
            require_productive_gate_open(unauthorized, "apply_productive")
        with self.assertRaisesRegex(RouteBError, "productive_rollback_not_authorized"):
            require_productive_gate_open(unauthorized, "rollback_productive")

        wrong_role = copy.deepcopy(execution)
        wrong_role["target"]["planned_productive_role"] = "different_productive_role"
        wrong_role["target"]["allowed_productive_roles"] = ["different_productive_role"]
        with self.assertRaisesRegex(RouteBError, "planned_productive_role_mismatch") as caught:
            validate_productive_role_contract(wrong_role)
        rendered = str(caught.exception)
        self.assertNotIn("postgresql://", rendered)
        self.assertNotIn("synthetic-test-password", rendered)

    def test_provisioned_gate_closed_state_contract_and_hybrids(self) -> None:
        self.assertEqual(validate_productive_role_contract(self.plan), PLANNED_PRODUCTIVE_ROLE)

        provisioned_closed = self.provisioned_gate_closed_fixture()
        self.assertEqual(
            provisioned_closed["status"],
            "PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED",
        )
        self.assertEqual(
            provisioned_closed["remaining_blockers"],
            ["PRODUCTIVE_EXECUTION_NOT_AUTHORIZED"],
        )
        self.assertEqual(
            validate_productive_role_contract(provisioned_closed),
            PLANNED_PRODUCTIVE_ROLE,
        )

        execution, _, _, _ = self.productive_unit_fixture()
        self.assertEqual(validate_productive_role_contract(execution), PLANNED_PRODUCTIVE_ROLE)

        invalid_mutations = {
            "bundle_missing": lambda plan: plan.pop("infrastructure_evidence"),
            "bundle_status": lambda plan: plan["infrastructure_evidence"].update({
                "status": "PENDING",
            }),
            "bundle_hash_missing": lambda plan: plan["infrastructure_evidence"].pop(
                "bundle_sha256"
            ),
            "bundle_hash_invalid": lambda plan: plan["infrastructure_evidence"].update({
                "bundle_sha256": "D" * 64,
            }),
            "components_missing": lambda plan: plan["infrastructure_evidence"].pop(
                "components"
            ),
            "component_missing": lambda plan: plan["infrastructure_evidence"][
                "components"
            ].pop("readonly_postcheck"),
            "component_extra": lambda plan: plan["infrastructure_evidence"][
                "components"
            ].update({"unapproved_component": "2" * 64}),
            "component_hash_invalid": lambda plan: plan["infrastructure_evidence"][
                "components"
            ].update({"admin_provisioning": "invalid"}),
            "baseline_component_mismatch": lambda plan: plan["infrastructure_evidence"][
                "components"
            ].update({"readonly_baseline_precheck": "2" * 64}),
            "gate_open": lambda plan: plan["activation_gate"].update({"gate_open": True}),
            "apply_authorized": lambda plan: plan.update({"productive_apply_authorized": True}),
            "rollback_authorized": lambda plan: plan.update({"productive_rollback_authorized": True}),
            "wrong_blocker": lambda plan: plan.update({"remaining_blockers": ["WRONG_BLOCKER"]}),
            "precheck_pending": lambda plan: plan.update({"readonly_precheck": {
                "status": "NOT_AUTHORIZED_OR_EXECUTED", "evidence_sha256": None,
            }}),
            "missing_hash": lambda plan: plan["readonly_precheck"].pop("evidence_sha256"),
            "invalid_hash": lambda plan: plan["readonly_precheck"].update({
                "evidence_sha256": "A" * 64,
            }),
            "role_not_registered": lambda plan: plan["activation_gate"].update({
                "productive_role_registered": False,
            }),
            "role_not_provisioned": lambda plan: plan["target"].update({
                "productive_role_status": "PLANNED_NOT_PROVISIONED",
            }),
            "wrong_role_list": lambda plan: plan["target"].update({
                "allowed_productive_roles": [],
            }),
        }
        for case, mutation in invalid_mutations.items():
            hybrid = copy.deepcopy(provisioned_closed)
            mutation(hybrid)
            with self.subTest(case=case), self.assertRaisesRegex(
                RouteBError, "productive_execution_state_contract_mismatch"
            ):
                validate_productive_role_contract(hybrid)

        for mode in ("apply_productive", "rollback_productive"):
            with self.subTest(mode=mode), self.assertRaisesRegex(
                RouteBError, f"{mode}_gate_closed"
            ):
                require_productive_gate_open(provisioned_closed, mode)

        report = productive_blocked_report(provisioned_closed, "apply_productive")
        self.assertEqual(report["verdict"], "BLOCKED")
        self.assertEqual(report["status"], PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED_STATUS)
        self.assertEqual(
            report["remaining_blockers"],
            PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED_BLOCKERS,
        )
        self.assertFalse(report["productive_apply_authorized"])
        self.assertFalse(report["productive_rollback_authorized"])
        self.assertFalse(report["dsn_read"])
        self.assertFalse(report["connection_attempted"])
        self.assertFalse(report["writes_attempted"])
        self.assertEqual(set(report), {
            "verdict", "mode", "status", "productive_apply_authorized",
            "productive_rollback_authorized", "planned_productive_role",
            "productive_role_status", "allowed_productive_roles",
            "remaining_blockers", "connection_attempted", "writes_attempted",
            "committed", "dsn_read",
        })
        self.assertFalse(any("sha" in key or "evidence" in key for key in report))
        self.assertNotIn("postgresql://", json.dumps(report))
        self.assertNotIn("password", json.dumps(report).lower())

    def test_registered_target_identity_is_exact(self) -> None:
        target = self.plan["target"]
        self.assertEqual(target["expected_supabase_project_ref"], "xheyrgfagpoigpgakilu")
        self.assertEqual(target["expected_hostname"], "db.xheyrgfagpoigpgakilu.supabase.co")
        self.assertEqual(target["expected_database"], "postgres")
        self.assertEqual(target["allowed_readonly_roles"], ["stock_zero_codex_ro"])
        self.assertTrue(self.plan["activation_gate"]["target_identity_registered"])
        self.assertTrue(self.plan["activation_gate"]["productive_role_registered"])
        self.assertFalse(self.plan["activation_gate"]["gate_open"])

    def test_git_reference_contract_is_external_and_non_self_referential(self) -> None:
        self.assertNotIn("expected_repository_commit", self.plan)
        contract = self.plan["git_reference_contract"]
        self.assertEqual(contract["mode"], "CLI_EXACT_APPROVED_HEAD")
        self.assertEqual(contract["required_argument"], "--expected-plan-git-ref")
        self.assertIn("plan SHA256 is recorded before connection", contract["requirements"])

    def test_productive_modes_are_mutually_exclusive_and_arguments_required(self) -> None:
        runner = ROOT / "scripts" / "run_kpione_route_b_ingestion_v1.py"
        conflict = subprocess.run(
            [sys.executable, str(runner), "--apply-local", "--apply-productive"],
            capture_output=True, text=True,
        )
        self.assertEqual(conflict.returncode, 2)
        self.assertIn("not allowed with argument", conflict.stderr)

        env = os.environ.copy()
        env["DB_URL_KPIONE_ROUTE_B_PRODUCTIVE"] = "must-not-be-read"
        missing = subprocess.run(
            [sys.executable, str(runner), "--apply-productive"],
            capture_output=True, text=True, env=env,
        )
        report = json.loads(missing.stdout)
        self.assertEqual(missing.returncode, 2)
        self.assertIn("productive_arguments_required", report["error"])
        self.assertFalse(report["dsn_read"])
        self.assertFalse(report["connection_attempted"])
        self.assertFalse(report["writes_attempted"])
        self.assertNotIn(env["DB_URL_KPIONE_ROUTE_B_PRODUCTIVE"], missing.stdout)

    def test_git_guard_requires_exact_clean_tracked_head_blob_and_returns_plan_sha(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root, plan_path, head = self.init_git_fixture(folder)
            result = validate_productive_git_guard(plan_path, head, root)
            self.assertEqual(result["approved_git_sha"], head)
            self.assertEqual(result["plan_path"], "plan.json")
            self.assertEqual(result["plan_sha256"], hashlib.sha256(plan_path.read_bytes()).hexdigest())
            with self.assertRaisesRegex(RouteBError, "repository_head_mismatch"):
                validate_productive_git_guard(plan_path, "0" * 40, root)

        with tempfile.TemporaryDirectory() as folder:
            root, plan_path, head = self.init_git_fixture(folder)
            plan_path.write_text('{"document_type":"dirty"}\n', encoding="utf-8")
            with self.assertRaisesRegex(RouteBError, "repository_worktree_not_clean"):
                validate_productive_git_guard(plan_path, head, root)

        with tempfile.TemporaryDirectory() as folder:
            root, plan_path, head = self.init_git_fixture(folder)
            plan_path.write_text('{"document_type":"staged"}\n', encoding="utf-8")
            subprocess.run(["git", "add", "plan.json"], cwd=root, check=True)
            with self.assertRaisesRegex(RouteBError, "repository_index_not_clean"):
                validate_productive_git_guard(plan_path, head, root)

        with tempfile.TemporaryDirectory() as folder:
            root, plan_path, head = self.init_git_fixture(folder)
            subprocess.run(
                ["git", "update-index", "--assume-unchanged", "plan.json"],
                cwd=root, check=True,
            )
            plan_path.write_text('{"document_type":"hidden-drift"}\n', encoding="utf-8")
            with self.assertRaisesRegex(RouteBError, "approved_plan_worktree_blob_mismatch"):
                validate_productive_git_guard(plan_path, head, root)

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, capture_output=True)
            subprocess.run(["git", "config", "user.email", "tests@example.invalid"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.name", "Tests"], cwd=root, check=True)
            tracked = root / "tracked.txt"
            tracked.write_text("tracked\n", encoding="utf-8")
            subprocess.run(["git", "add", "tracked.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "fixture"], cwd=root, check=True, capture_output=True)
            head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
            untracked_plan = root / "plan.json"
            untracked_plan.write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(RouteBError, "repository_worktree_not_clean"):
                validate_productive_git_guard(untracked_plan, head, root)

    def test_productive_local_validation_blocks_sql_and_file_metric_drift(self) -> None:
        altered_sql = copy.deepcopy(self.plan)
        altered_sql["physical_contract"]["sql_sha256"] = "0" * 64
        with self.assertRaisesRegex(RouteBError, "sql_sha256_mismatch"):
            validate_productive_local_artifacts(altered_sql, ROOT)

        altered_size = copy.deepcopy(self.plan)
        altered_size["source_package"]["approved_files"][0]["file_size"] += 1
        with self.assertRaisesRegex(RouteBError, "observed_file_size_mismatch"):
            validate_productive_local_artifacts(altered_size, ROOT)

    def test_productive_apply_core_stages_activates_last_postchecks_and_redacts(self) -> None:
        plan, approved, dsn, git_guard = self.productive_unit_fixture()
        database = FakeProductiveDatabase(plan)
        with tempfile.TemporaryDirectory() as folder:
            evidence_path = Path(folder) / "apply.json"
            report = run_productive_apply(
                plan, approved, dsn, evidence_path,
                git_guard=git_guard, root=ROOT, connect_fn=database.connect,
            )
            persisted = json.loads(evidence_path.read_text(encoding="utf-8"))
        self.assertEqual(report["postcheck_verdict"], "PASS")
        self.assertTrue(report["committed"])
        self.assertTrue(report["downstream_use_allowed"])
        self.assertEqual(report, persisted)
        self.assertEqual(report["postcheck"]["declared_failures"], [])
        boolean_checks = {
            item["index"]: item["boolean_result"]
            for item in report["postcheck"]["declared_postchecks"]
            if item.get("result_type") == "boolean"
        }
        self.assertEqual(boolean_checks, {7: True, 10: True, 11: True})
        self.assertNotIn("boolean_result", report["postcheck"]["declared_postchecks"][4])
        self.assertEqual(report["postcheck"]["legacy_object"], "cg_raw.kpione2_raw")
        self.assertEqual(database.connection_count, 2)
        self.assertEqual(len(database.state["files"]), 2)
        self.assertEqual(len(database.state["staging"]), 2)
        self.assertEqual({item["status"] for item in database.state["batches"].values()}, {"ACTIVE"})
        staging_index = next(i for i, sql in enumerate(database.commands)
                             if sql.startswith("insert into cg_raw.kpione_raw_event_photo_staging_v1"))
        activation_index = next(i for i, sql in enumerate(database.commands)
                                if sql.startswith("update cg_raw.kpione_raw_ingest_batch_v1 set status='active'"))
        self.assertLess(staging_index, activation_index)
        rendered = json.dumps(report)
        self.assertNotIn(dsn, rendered)
        self.assertNotIn("synthetic-test-password", rendered)
        ddl_prefixes = ("create ", "alter ", "drop ", "grant ", "revoke ", "comment ")
        self.assertFalse(any(command.startswith(ddl_prefixes) for command in database.commands))

    def test_productive_apply_requires_preprovisioned_exact_objects(self) -> None:
        for case in ("objects_missing", "signature_mismatch"):
            with self.subTest(case=case):
                plan, approved, dsn, git_guard = self.productive_unit_fixture()
                database = FakeProductiveDatabase(plan)
                if case == "objects_missing":
                    database.state["objects_exist"] = False
                else:
                    database.state["signatures_match"] = False
                with tempfile.TemporaryDirectory() as folder:
                    with self.assertRaisesRegex(
                        RouteBError, "route_b_object_set_or_kind_mismatch"
                    ) as caught:
                        run_productive_apply(
                            plan, approved, dsn, Path(folder) / "evidence.json",
                            git_guard=git_guard, root=ROOT, connect_fn=database.connect,
                        )
                self.assertTrue(caught.exception.connection_attempted)
                self.assertFalse(caught.exception.writes_attempted)
                self.assertFalse(caught.exception.committed)
                self.assertEqual(database.state["batches"], {})

    def test_declared_boolean_and_legacy_postchecks_reject_failures(self) -> None:
        for case in ("event_view_false", "day_presence_false", "legacy_missing"):
            with self.subTest(case=case):
                plan, approved, dsn, git_guard = self.productive_unit_fixture()
                database = FakeProductiveDatabase(plan)
                if case == "event_view_false":
                    database.declared_boolean_failures.add(10)
                elif case == "day_presence_false":
                    database.declared_boolean_failures.add(11)
                else:
                    database.legacy_object = None
                with tempfile.TemporaryDirectory() as folder:
                    evidence_path = Path(folder) / "rejected.json"
                    with self.assertRaisesRegex(
                        RouteBError, "POSTCHECK_REJECTED_REQUIRES_EXPLICIT_ROLLBACK_AUTHORIZATION"
                    ):
                        run_productive_apply(
                            plan, approved, dsn, evidence_path,
                            git_guard=git_guard, root=ROOT, connect_fn=database.connect,
                        )
                    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
                self.assertEqual(evidence["postcheck_verdict"], "REJECTED")
                self.assertFalse(evidence["downstream_use_allowed"])
                if case == "legacy_missing":
                    self.assertIsNone(evidence["postcheck"]["legacy_object"])
                    self.assertEqual(evidence["postcheck"]["declared_failures"], [])
                else:
                    expected_index = 10 if case == "event_view_false" else 11
                    self.assertEqual(evidence["postcheck"]["declared_failures"], [expected_index])

    def test_postcommit_evidence_write_failure_preserves_structured_report(self) -> None:
        plan, approved, dsn, git_guard = self.productive_unit_fixture()
        database = FakeProductiveDatabase(plan)
        with tempfile.TemporaryDirectory() as folder, mock.patch.object(
            Path, "write_text", side_effect=OSError("synthetic write failure")
        ):
            with self.assertRaisesRegex(
                RouteBError, "productive_evidence_write_failed"
            ) as caught:
                run_productive_apply(
                    plan, approved, dsn, Path(folder) / "evidence.json",
                    git_guard=git_guard, root=ROOT, connect_fn=database.connect,
                )
        self.assertTrue(caught.exception.committed)
        report = productive_runner._blocked_error_report(caught.exception, False)
        for field in (
            "batch_id", "operation", "approved_git_sha", "plan_sha256", "committed",
            "postcheck_verdict", "downstream_use_allowed", "rollback_readiness",
        ):
            self.assertIn(field, report)
        self.assertEqual(report["error"], "productive_evidence_write_failed")
        self.assertEqual(report["outcome"], "COMMITTED_EVIDENCE_PERSISTENCE_FAILED")
        self.assertTrue(report["committed"])
        rendered = json.dumps(report)
        self.assertNotIn(dsn, rendered)
        self.assertNotIn("synthetic-test-password", rendered)

    def test_productive_apply_session_and_transaction_fail_closed(self) -> None:
        for case in ("wrong_role", "read_only"):
            with self.subTest(case=case):
                plan, approved, dsn, git_guard = self.productive_unit_fixture()
                database = FakeProductiveDatabase(plan)
                if case == "wrong_role":
                    database.role = "postgres"
                    expected = "productive_session_role_mismatch"
                else:
                    database.write_session_readonly = True
                    expected = "productive_apply_transaction_not_read_write"
                with tempfile.TemporaryDirectory() as folder:
                    with self.assertRaisesRegex(RouteBError, expected) as caught:
                        run_productive_apply(
                            plan, approved, dsn, Path(folder) / "evidence.json",
                            git_guard=git_guard, root=ROOT, connect_fn=database.connect,
                        )
                self.assertTrue(caught.exception.connection_attempted)
                self.assertFalse(caught.exception.writes_attempted)
                self.assertFalse(caught.exception.committed)
                self.assertEqual(database.state["batches"], {})

        plan, approved, dsn, git_guard = self.productive_unit_fixture()
        plan["source_package"]["expected_source_rows"] = 3
        database = FakeProductiveDatabase(plan)
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(RouteBError, "productive_staging_count_mismatch") as caught:
                run_productive_apply(
                    plan, approved, dsn, Path(folder) / "evidence.json",
                    git_guard=git_guard, root=ROOT, connect_fn=database.connect,
                )
        self.assertTrue(caught.exception.writes_attempted)
        self.assertFalse(caught.exception.committed)
        self.assertEqual(database.state["batches"], {})
        self.assertEqual(database.state["staging"], [])

    def test_postcheck_rejection_blocks_downstream_without_automatic_rollback(self) -> None:
        plan, approved, dsn, git_guard = self.productive_unit_fixture()
        database = FakeProductiveDatabase(plan)
        database.reject_postcheck = True
        with tempfile.TemporaryDirectory() as folder:
            evidence_path = Path(folder) / "rejected.json"
            with self.assertRaisesRegex(
                RouteBError, "POSTCHECK_REJECTED_REQUIRES_EXPLICIT_ROLLBACK_AUTHORIZATION"
            ) as caught:
                run_productive_apply(
                    plan, approved, dsn, evidence_path,
                    git_guard=git_guard, root=ROOT, connect_fn=database.connect,
                )
            evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        self.assertTrue(caught.exception.committed)
        self.assertEqual(evidence["postcheck_verdict"], "REJECTED")
        self.assertFalse(evidence["downstream_use_allowed"])
        self.assertEqual({item["status"] for item in database.state["batches"].values()}, {"ACTIVE"})

    def test_productive_logical_rollback_preserves_staging_and_is_transactional(self) -> None:
        plan, approved, dsn, git_guard = self.productive_unit_fixture()
        database = FakeProductiveDatabase(plan)
        with tempfile.TemporaryDirectory() as folder:
            applied = run_productive_apply(
                plan, approved, dsn, Path(folder) / "apply.json",
                git_guard=git_guard, root=ROOT, connect_fn=database.connect,
            )
            staged_before = copy.deepcopy(database.state["staging"])
            rolled_back = run_productive_rollback(
                plan, approved, applied["batch_id"], dsn, Path(folder) / "rollback.json",
                git_guard=git_guard, connect_fn=database.connect,
            )
        self.assertEqual(rolled_back["postcheck_verdict"], "PASS")
        self.assertEqual(database.state["staging"], staged_before)
        self.assertEqual(database.state["batches"][applied["batch_id"]]["status"], "ROLLED_BACK")

        plan, approved, dsn, git_guard = self.productive_unit_fixture()
        database = FakeProductiveDatabase(plan)
        with tempfile.TemporaryDirectory() as folder:
            applied = run_productive_apply(
                plan, approved, dsn, Path(folder) / "apply.json",
                git_guard=git_guard, root=ROOT, connect_fn=database.connect,
            )
            database.rollback_staging_mismatch = True
            with self.assertRaisesRegex(RouteBError, "rollback_mutated_staging"):
                run_productive_rollback(
                    plan, approved, applied["batch_id"], dsn,
                    Path(folder) / "rollback.json", git_guard=git_guard,
                    connect_fn=database.connect,
                )
        self.assertEqual(database.state["batches"][applied["batch_id"]]["status"], "ACTIVE")

    def test_productive_dsn_future_username_must_match_planned_role(self) -> None:
        role = "stock_zero_kpione_route_b_load"
        password = "synthetic-test-password"
        project_ref = "xheyrgfagpoigpgakilu"
        hostname = f"db.{project_ref}.supabase.co"
        synthetic_dsn = (
            "postgresql://"
            f"{role}:{password}@"
            f"{hostname}/postgres"
            "?sslmode=require"
        )
        result = validate_productive_dsn_target(synthetic_dsn, self.plan)
        self.assertEqual(result, {
            "hostname": hostname,
            "database": "postgres",
            "username": role,
            "project_ref": project_ref,
            "sslmode": "require",
        })
        self.assertNotIn(synthetic_dsn, json.dumps(result))
        self.assertNotIn(password, json.dumps(result))

        wrong_project_plan = copy.deepcopy(self.plan)
        wrong_project_plan["target"]["expected_supabase_project_ref"] = "wrong-project-ref"
        invalid_targets = {
            "role": (
                "productive_dsn_role_mismatch",
                synthetic_dsn.replace(role, "postgres", 1),
                self.plan,
            ),
            "hostname": (
                "productive_dsn_hostname_mismatch",
                synthetic_dsn.replace(hostname, "db.wrong.supabase.co", 1),
                self.plan,
            ),
            "project_ref": (
                "productive_dsn_project_ref_mismatch",
                synthetic_dsn,
                wrong_project_plan,
            ),
            "database": (
                "productive_dsn_database_mismatch",
                synthetic_dsn.replace("/postgres?", "/wrong?", 1),
                self.plan,
            ),
        }
        invalid_sslmodes = {
            "missing": synthetic_dsn.replace("?sslmode=require", ""),
            "empty": synthetic_dsn.replace("sslmode=require", "sslmode="),
            "disable": synthetic_dsn.replace("sslmode=require", "sslmode=disable"),
            "allow": synthetic_dsn.replace("sslmode=require", "sslmode=allow"),
            "prefer": synthetic_dsn.replace("sslmode=require", "sslmode=prefer"),
            "verify-ca": synthetic_dsn.replace("sslmode=require", "sslmode=verify-ca"),
            "verify-full": synthetic_dsn.replace("sslmode=require", "sslmode=verify-full"),
            "duplicate_conflict": synthetic_dsn.replace(
                "sslmode=require", "sslmode=require&sslmode=disable"
            ),
        }

        for case, (expected_error, invalid_dsn, plan) in invalid_targets.items():
            with self.subTest(case=case):
                with self.assertRaises(RouteBError) as caught:
                    validate_productive_dsn_target(invalid_dsn, plan)
                report = {
                    "error": str(caught.exception),
                    "connection_attempted": False,
                    "writes_attempted": False,
                }
                self.assertEqual(report["error"], expected_error)
                self.assertFalse(report["connection_attempted"])
                self.assertFalse(report["writes_attempted"])
                self.assertNotIn(invalid_dsn, json.dumps(report))
                self.assertNotIn(password, json.dumps(report))

        for case, invalid_dsn in invalid_sslmodes.items():
            with self.subTest(case=case):
                with self.assertRaises(RouteBError) as caught:
                    validate_productive_dsn_target(invalid_dsn, self.plan)
                report = {
                    "error": str(caught.exception),
                    "connection_attempted": False,
                    "writes_attempted": False,
                }
                self.assertEqual(report["error"], "productive_sslmode_require_required")
                self.assertFalse(report["connection_attempted"])
                self.assertFalse(report["writes_attempted"])
                self.assertNotIn(invalid_dsn, json.dumps(report))
                self.assertNotIn(password, json.dumps(report))

    def test_apply_productive_blocks_before_dsn_env_read_when_gate_closed(self) -> None:
        report = productive_blocked_report(
            self.provisioned_gate_closed_fixture(), "apply_productive",
        )
        self.assertEqual(report["verdict"], "BLOCKED")
        self.assertEqual(report["planned_productive_role"], PLANNED_PRODUCTIVE_ROLE)
        self.assertEqual(report["productive_role_status"], "PROVISIONED_AND_VERIFIED")
        self.assertEqual(report["allowed_productive_roles"], [PLANNED_PRODUCTIVE_ROLE])
        self.assertEqual(
            report["remaining_blockers"],
            PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED_BLOCKERS,
        )
        self.assertFalse(report["dsn_read"])
        self.assertFalse(report["connection_attempted"])
        self.assertFalse(report["writes_attempted"])
        self.assertFalse(report["committed"])

    def test_runner_returns_exit_3_for_deliberately_blocked_productive_gate(self) -> None:
        git_guard = {
            "approved_git_sha": "a" * 40,
            "plan_path": "plans/018_kpione_route_b_productive_apply_plan.json",
            "plan_sha256": "b" * 64,
        }
        with tempfile.TemporaryDirectory() as folder:
            environment_get = os.environ.get

            def guarded_environment_get(key: str, default=None):
                if key == "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE":
                    raise AssertionError("productive_dsn_read")
                return environment_get(key, default)

            argv = [
                "run_kpione_route_b_ingestion_v1.py",
                "--apply-productive",
                "--approved-plan", str(PLAN_PATH),
                "--expected-plan-git-ref", "a" * 40,
                "--expected-project-ref", "xheyrgfagpoigpgakilu",
                "--db-url-env", "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE",
                "--confirm-productive", "KPIONE_ROUTE_B_018_APPLY",
                "--postcheck-report-json", str(Path(folder) / "evidence.json"),
            ]
            output = io.StringIO()
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(productive_runner, "validate_productive_git_guard", return_value=git_guard),
                mock.patch.object(productive_runner, "load_approved_productive_plan", return_value=self.provisioned_gate_closed_fixture()),
                mock.patch.object(productive_runner, "build_approved_plan_from_manifest", return_value={}),
                mock.patch.object(productive_runner.os.environ, "get", side_effect=guarded_environment_get),
                redirect_stdout(output),
            ):
                returncode = productive_runner.main()
        report = json.loads(output.getvalue())
        self.assertEqual(returncode, 3)
        self.assertEqual(report["verdict"], "BLOCKED")
        self.assertFalse(report["dsn_read"])
        self.assertFalse(report["connection_attempted"])
        self.assertFalse(report["writes_attempted"])

    def test_provisioned_closed_runner_blocks_apply_and_rollback_before_dsn_read(self) -> None:
        plan = self.provisioned_gate_closed_fixture()
        git_guard = {
            "approved_git_sha": "a" * 40,
            "plan_path": "plans/018_kpione_route_b_productive_apply_plan.json",
            "plan_sha256": "b" * 64,
        }
        environment_get = os.environ.get

        def guarded_environment_get(key: str, default=None):
            if key == "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE":
                raise AssertionError("productive_dsn_read")
            return environment_get(key, default)

        cases = {
            "apply_productive": [
                "--apply-productive",
                "--confirm-productive", "KPIONE_ROUTE_B_018_APPLY",
            ],
            "rollback_productive": [
                "--rollback-productive",
                "--confirm-rollback", "KPIONE_ROUTE_B_018_ROLLBACK",
                "--rollback-batch-id", "00000000-0000-0000-0000-000000000020",
            ],
        }
        for mode, mode_arguments in cases.items():
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as folder:
                argv = [
                    "run_kpione_route_b_ingestion_v1.py",
                    *mode_arguments,
                    "--approved-plan", str(PLAN_PATH),
                    "--expected-plan-git-ref", "a" * 40,
                    "--expected-project-ref", "xheyrgfagpoigpgakilu",
                    "--db-url-env", "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE",
                    "--postcheck-report-json", str(Path(folder) / "evidence.json"),
                ]
                output = io.StringIO()
                with (
                    mock.patch.object(sys, "argv", argv),
                    mock.patch.object(
                        productive_runner,
                        "validate_productive_git_guard",
                        return_value=git_guard,
                    ),
                    mock.patch.object(
                        productive_runner,
                        "load_approved_productive_plan",
                        return_value=copy.deepcopy(plan),
                    ),
                    mock.patch.object(
                        productive_runner,
                        "build_approved_plan_from_manifest",
                        return_value={},
                    ),
                    mock.patch.object(
                        productive_runner.os.environ,
                        "get",
                        side_effect=guarded_environment_get,
                    ),
                    redirect_stdout(output),
                ):
                    returncode = productive_runner.main()
                report = json.loads(output.getvalue())
                self.assertEqual(returncode, 3)
                self.assertEqual(report["verdict"], "BLOCKED")
                self.assertEqual(report["mode"], mode)
                self.assertEqual(
                    report["status"],
                    PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED_STATUS,
                )
                self.assertEqual(
                    report["remaining_blockers"],
                    PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED_BLOCKERS,
                )
                self.assertFalse(report["productive_apply_authorized"])
                self.assertFalse(report["productive_rollback_authorized"])
                self.assertFalse(report["dsn_read"])
                self.assertFalse(report["connection_attempted"])
                self.assertFalse(report["writes_attempted"])
                rendered = json.dumps(report)
                self.assertNotIn("infrastructure_evidence", rendered)
                self.assertNotIn("readonly_precheck", rendered)
                for component in PRODUCTIVE_INFRASTRUCTURE_EVIDENCE_COMPONENTS:
                    self.assertNotIn(component, rendered)
                for evidence_hash in ("c" * 64, "d" * 64, "e" * 64, "f" * 64, "1" * 64):
                    self.assertNotIn(evidence_hash, rendered)
                self.assertNotIn("postgresql://", rendered)
                self.assertNotIn("password", rendered.lower())

    def test_apply_productive_rejects_wrong_project_ref_before_dsn_env_read(self) -> None:
        altered = copy.deepcopy(self.plan)
        altered["target"]["expected_supabase_project_ref"] = "wrong-ref"
        with self.assertRaisesRegex(RouteBError, "productive_target_expected_supabase_project_ref_mismatch"):
            validate_registered_productive_target(altered)

    def test_approved_manifest_ignores_known_truncated_fixture(self) -> None:
        package = self.plan["source_package"]
        truncated = [item for item in package["excluded_files"] if item["classification"] == "NEGATIVE_TEST_FIXTURE_TRUNCATED"]
        self.assertEqual([item["filename"] for item in truncated], ["photo-excel-admin_1781976368641.xlsx"])
        self.assertNotIn(truncated[0]["filename"], {item["filename"] for item in package["approved_files"]})
        summary = precheck.validate_local_artifacts(self.plan, ROOT)
        self.assertEqual(summary["expected_source_rows"], 239159)

    def test_truncated_fixture_cannot_enter_approved_list(self) -> None:
        altered = copy.deepcopy(self.plan)
        altered["source_package"]["approved_files"].append(
            {**altered["source_package"]["excluded_files"][0], "semantic_content_hash": "0" * 64}
        )
        with self.assertRaisesRegex(precheck.PrecheckBlock, "approved_excluded_filename_overlap"):
            precheck.validate_source_manifest(altered, ROOT)

    def test_two_day8_files_are_semantically_equivalent(self) -> None:
        folder = ROOT / self.plan["input_directory"]["repository_relative_value"]
        first = inspect_workbook(folder / "photo-excel-admin_1783219885210.xlsx")
        second = inspect_workbook(folder / "photo-excel-admin_1782012877303.xlsx")
        self.assertEqual(semantic_content_hash(first), semantic_content_hash(second))
        self.assertEqual((len(first.rows), first.event_count, first.day_presence_count),
                         (len(second.rows), second.event_count, second.day_presence_count))

    def test_day8_canonical_selection_is_deterministic(self) -> None:
        package = self.plan["source_package"]
        approved = next(item for item in package["approved_files"] if item["coverage_start"] == "2026-06-08")
        excluded = next(item for item in package["excluded_files"] if item["classification"] == "DUPLICATE_SEMANTIC_EXPORT")
        self.assertEqual(approved["sha256"], min(approved["sha256"], excluded["sha256"]))
        self.assertEqual(approved["semantic_content_hash"], excluded["semantic_content_hash"])

    def test_excluded_duplicate_does_not_affect_aggregate_counts(self) -> None:
        package = self.plan["source_package"]
        self.assertEqual(package["approved_file_count"], len(package["approved_files"]))
        self.assertEqual(package["approved_file_count"], 9)
        self.assertEqual(package["expected_source_rows"], 239159)
        self.assertEqual(package["expected_day_presence_rows"], 34996)

    def test_global_duplicate_inside_one_workbook(self) -> None:
        workbook = synthetic_workbook("a" * 64, "first.xlsx", [synthetic_row(2), synthetic_row(3)])
        classes = [row["duplicate_classification"] for row in classify_global_photo_duplicates([workbook])]
        self.assertEqual(classes, ["UNIQUE", "EXACT_DUPLICATE"])

    def test_global_duplicate_across_workbooks(self) -> None:
        first = synthetic_workbook("a" * 64, "first.xlsx", [synthetic_row(2)])
        second = synthetic_workbook("b" * 64, "second.xlsx", [synthetic_row(2)])
        classes = [row["duplicate_classification"] for row in classify_global_photo_duplicates([first, second])]
        self.assertEqual(classes, ["UNIQUE", "CROSS_FILE_DUPLICATE"])

    def test_global_canonical_order_is_input_order_independent(self) -> None:
        first = synthetic_workbook("a" * 64, "z.xlsx", [synthetic_row(2)])
        second = synthetic_workbook("b" * 64, "a.xlsx", [synthetic_row(2)])
        forward = classify_global_photo_duplicates([first, second])
        reverse = classify_global_photo_duplicates([second, first])
        signature = lambda rows: [(row["source_file_sha256"], row["duplicate_classification"]) for row in rows]
        self.assertEqual(signature(forward), signature(reverse))
        self.assertEqual(signature(forward)[0], ("a" * 64, "UNIQUE"))

    def test_global_classification_ignores_filename_and_path(self) -> None:
        rows = [synthetic_row(2)]
        left = synthetic_workbook("a" * 64, "folder-a/name-a.xlsx", rows)
        right = synthetic_workbook("a" * 64, "folder-b/name-b.xlsx", rows)
        self.assertEqual(
            [row["duplicate_classification"] for row in classify_global_photo_duplicates([left])],
            [row["duplicate_classification"] for row in classify_global_photo_duplicates([right])],
        )

    def test_global_classification_retains_rows_without_mutation(self) -> None:
        original = [synthetic_row(2), synthetic_row(3)]
        snapshot = copy.deepcopy(original)
        classified = classify_global_photo_duplicates([synthetic_workbook("a" * 64, "x.xlsx", original)])
        self.assertEqual(len(classified), len(original))
        self.assertEqual(original, snapshot)

    def test_global_classification_has_one_unique_per_identity(self) -> None:
        first = synthetic_workbook("a" * 64, "first.xlsx", [synthetic_row(2), synthetic_row(3)])
        second = synthetic_workbook("b" * 64, "second.xlsx", [synthetic_row(2)])
        classified = classify_global_photo_duplicates([second, first])
        self.assertEqual(sum(row["duplicate_classification"] == "UNIQUE" for row in classified), 1)

    def test_approved_june_global_duplicate_and_projection_counts(self) -> None:
        summary = precheck.validate_local_artifacts(self.plan, ROOT)
        self.assertEqual(summary["expected_duplicate_photo_rows"], 10089)
        self.assertEqual(summary["expected_exact_duplicate_rows"], 0)
        self.assertEqual(summary["expected_cross_file_duplicate_rows"], 10089)
        self.assertEqual(summary["expected_distinct_events"], 35287)
        self.assertEqual(summary["expected_day_presence_rows"], 34996)

    def test_unknown_matching_file_blocks(self) -> None:
        altered = copy.deepcopy(self.plan)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            altered["input_directory"]["repository_relative_value"] = "."
            (root / "photo-excel-admin_unknown.xlsx").write_bytes(b"unknown")
            with self.assertRaisesRegex(precheck.PrecheckBlock, "unknown_matching_files"):
                precheck.validate_source_manifest(altered, root)

    def test_missing_approved_file_blocks(self) -> None:
        altered = copy.deepcopy(self.plan)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            altered["input_directory"]["repository_relative_value"] = "."
            with self.assertRaisesRegex(precheck.PrecheckBlock, "missing_approved_files"):
                precheck.validate_source_manifest(altered, root)

    def test_altered_approved_hash_blocks(self) -> None:
        altered = copy.deepcopy(self.plan)
        approved = altered["source_package"]["approved_files"][0]
        altered["source_package"]["approved_files"] = [approved]
        altered["source_package"]["excluded_files"] = []
        altered["source_package"]["observed_directory_inventory"] = [
            {"filename": approved["filename"], "sha256": approved["sha256"], "manifest_role": "APPROVED"}
        ]
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            altered["input_directory"]["repository_relative_value"] = "."
            (root / approved["filename"]).write_bytes(b"altered")
            with self.assertRaisesRegex(precheck.PrecheckBlock, "observed_file_size_mismatch"):
                precheck.validate_source_manifest(altered, root)

    def test_folder_path_and_filename_do_not_affect_semantic_hash(self) -> None:
        source = ROOT / self.plan["input_directory"]["repository_relative_value"] / "photo-excel-admin_1783219885210.xlsx"
        with tempfile.TemporaryDirectory() as folder:
            renamed = Path(folder) / "photo-excel-admin_renamed.xlsx"
            renamed.write_bytes(source.read_bytes())
            self.assertEqual(semantic_content_hash(inspect_workbook(source)),
                             semantic_content_hash(inspect_workbook(renamed)))

    def test_approved_manifest_covers_every_june_date(self) -> None:
        coverage = self.plan["source_package"]["expected_coverage"]
        self.assertEqual((coverage["start"], coverage["end"], coverage["distinct_dates"]),
                         ("2026-06-01", "2026-06-30", 30))
        self.assertEqual(coverage["missing_dates"], [])

    def test_target_guard_requires_registered_exact_supabase_identity(self) -> None:
        with self.assertRaises(precheck.PrecheckBlock):
            precheck.validate_target("postgresql://u:p@localhost/db", "DB_URL_CODEX_RO", self.plan, "unknown")
        ready = copy.deepcopy(self.plan)
        ready["target"]["expected_supabase_project_ref"] = "project-ref"
        ready["target"]["expected_hostname"] = "db.project-ref.supabase.co"
        self.assertEqual(
            precheck.validate_target(
                "postgresql://stock_zero_codex_ro:synthetic@db.project-ref.supabase.co/postgres?sslmode=require",
                "DB_URL_CODEX_RO", ready, "project-ref",
            ),
            "db.project-ref.supabase.co",
        )

    def test_precheck_source_contains_no_mutating_sql(self) -> None:
        source = (ROOT / "scripts" / "precheck_kpione_route_b_018_read_only.py").read_text(encoding="utf-8").upper()
        self.assertIn("BEGIN READ ONLY", source)
        for token in ("INSERT INTO", "UPDATE ", "DELETE FROM", "DROP ", "ALTER TABLE", "CREATE TABLE", "TRUNCATE "):
            self.assertNotIn(token, source)

    def test_plan_has_exact_future_controls_and_postchecks(self) -> None:
        command = self.plan["future_productive_command"]["command"]
        for token in ("--apply-productive", "--approved-plan", "--expected-plan-git-ref", "--expected-project-ref", "--confirm-productive", "--postcheck-report-json"):
            self.assertIn(token, command)
        self.assertGreaterEqual(len(self.plan["postcheck_queries"]), 10)
        self.assertEqual(self.plan["target"]["future_productive_env"], "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE")

    def test_run_precheck_success_report_and_connection_lifecycle(self) -> None:
        ready = self.preparation_fixture()
        connection = FakeConnection()
        report = precheck.run_precheck(
            ready, "redacted", lambda _dsn: connection, run_id=str(uuid.uuid4()),
        )
        self.assertEqual(report["document_type"], "kpione_route_b_readonly_baseline_evidence_v1")
        self.assertEqual(report["verdict"], "PASS_READONLY_BASELINE")
        self.assertEqual(report["current_user"], "stock_zero_codex_ro")
        self.assertEqual(report["transaction_read_only"], "on")
        self.assertEqual(report["approved_source_bytes"], 16571976)
        self.assertEqual(report["excluded_source_bytes"], 4023244)
        self.assertEqual(report["observed_directory_bytes"], 20595220)
        self.assertEqual((report["approved_file_count"], report["excluded_file_count"], report["observed_file_count"]), (9, 2, 11))
        self.assertFalse(report["writes_attempted"])
        self.assertTrue(connection.rollback_called)
        self.assertTrue(connection.close_called)

    def reattestation_report_fixture(self) -> dict[str, object]:
        execution = self.plan["productive_execution"]
        sources = execution["source_evidence"]
        return {
            "document_type": "stock_zero_route_b_post_apply_reattestation_v1",
            "verdict": "PASS_ROUTE_B_POST_APPLY_REATTESTATION",
            "productive_run_id": execution["productive_run_id"],
            "approved_git_sha": execution["approved_git_sha"],
            "runner_execution_uuid": execution["runner_execution_uuid"],
            "batch_id": execution["batch_id"],
            "reattestation_execution_uuid": str(uuid.uuid4()),
            "observed_at_utc": "2026-07-19T17:00:00+00:00",
            "source_evidence": {
                "01": copy.deepcopy(sources["01_readonly_pre_apply_target_check"]),
                "02": copy.deepcopy(sources["02_route_b_june_productive_apply"]),
                "03": copy.deepcopy(sources["03_readonly_post_apply_verification"]),
            },
            "baseline_evidence_sha256": sources["01_readonly_pre_apply_target_check"]["sha256"],
            "database_execution": {
                "current_user": "stock_zero_codex_ro",
                "session_user": "stock_zero_codex_ro",
                "expected_role": "stock_zero_codex_ro",
                "transaction_read_only": "on",
                "begin_read_only": True,
                "rollback_completed": True,
                "writes_attempted": False,
                "write_statements_defined": False,
            },
            "current_productive_state": {
                "active_batch_count": 1,
                "active_batch_id": execution["batch_id"],
                "staged_rows": 239159,
                "events": 35287,
                "day_presence": 34996,
                "files": 9,
                "conflicts": 0,
            },
            "baseline_comparison": {
                "legacy_row_delta": 0,
                "legacy_relation_size_delta": 0,
                "public_acl_unchanged": True,
                "physical_signatures_unchanged": True,
                "observer_role_unchanged": True,
            },
        }

    def test_post_apply_states_and_forward_only_transitions(self) -> None:
        self.assertEqual(validate_productive_role_contract(self.plan), PLANNED_PRODUCTIVE_ROLE)
        validate_productive_state_transition(
            "READY_FOR_PRODUCTIVE_EXECUTION",
            PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_STATUS,
        )
        validate_productive_state_transition(
            PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_STATUS,
            PRODUCTIVE_APPLY_RECORDED_GATE_CLOSED_STATUS,
        )
        for current, requested in (
            (PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_STATUS, "READY_FOR_PRODUCTIVE_EXECUTION"),
            (PRODUCTIVE_APPLY_RECORDED_GATE_CLOSED_STATUS, PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_STATUS),
        ):
            with self.subTest(current=current, requested=requested), self.assertRaisesRegex(
                RouteBError, "productive_state_transition_not_allowed",
            ):
                validate_productive_state_transition(current, requested)

        recorded = copy.deepcopy(self.plan)
        recorded["status"] = PRODUCTIVE_APPLY_RECORDED_GATE_CLOSED_STATUS
        recorded["remaining_blockers"] = []
        recorded["evidence_closure_complete"] = True
        recorded["supplemental_reattestation_required"] = False
        recorded["productive_execution"]["reattestation"] = {
            "status": "PASSED",
            "verdict": "PASS_ROUTE_B_POST_APPLY_REATTESTATION",
            "sha256": "a" * 64,
        }
        recorded["productive_execution"]["evidence_bundle"] = {
            "status": "PASSED", "sha256": "b" * 64,
        }
        self.assertEqual(validate_productive_role_contract(recorded), PLANNED_PRODUCTIVE_ROLE)

    def test_closure_pending_state_is_fail_closed(self) -> None:
        required = {
            "productive_apply_executed": True,
            "productive_apply_authorized": False,
            "productive_rollback_authorized": False,
            "bridge_executed": False,
        }
        for field, value in required.items():
            self.assertIs(self.plan[field], value)
        self.assertFalse(self.plan["activation_gate"]["gate_open"])
        for mutation in (
            lambda plan: plan["activation_gate"].update({"gate_open": True}),
            lambda plan: plan.update({"productive_apply_authorized": True}),
            lambda plan: plan.update({"productive_apply_executed": False}),
            lambda plan: plan.update({"productive_rollback_authorized": True}),
            lambda plan: plan.update({"bridge_executed": True}),
        ):
            altered = copy.deepcopy(self.plan)
            mutation(altered)
            with self.assertRaisesRegex(RouteBError, "productive_execution_state_contract_mismatch"):
                validate_productive_role_contract(altered)

    def test_current_raw_evidence_matches_plan_and_remains_byte_identical(self) -> None:
        run_id = self.plan["productive_execution"]["productive_run_id"]
        loaded = precheck.load_post_apply_source_evidence(self.plan, ROOT, run_id)
        self.assertEqual(set(loaded["payloads"]), {"01", "02", "03"})
        self.assertNotIn("productive_run_id", loaded["payloads"]["02"])
        self.assertEqual(
            loaded["baseline_evidence_sha256"],
            "edee494b40187d01b9fc627f09a39f6ff9d0f46979c7a99f5cf82660d478c9fe",
        )
        for sequence, expected in {
            "01": "edee494b40187d01b9fc627f09a39f6ff9d0f46979c7a99f5cf82660d478c9fe",
            "02": "1ec47bd1ab62dafdcd7463292e033882f2bea57d3db7587a4f9f84385c485159",
            "03": "ad2823dd00d02b8b4c32e385a57c3516d14df834836c0790a3f3eb648733563b",
        }.items():
            path = ROOT / loaded["references"][sequence]["path"]
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), expected)

        altered = copy.deepcopy(self.plan)
        altered["productive_execution"]["source_evidence"][
            "01_readonly_pre_apply_target_check"
        ]["sha256"] = "0" * 64
        with self.assertRaisesRegex(precheck.PrecheckBlock, "source_evidence_sha256_mismatch:01"):
            precheck.load_post_apply_source_evidence(altered, ROOT, run_id)

    def test_reattestation_contract_requires_authority_and_readonly_invariants(self) -> None:
        valid = self.reattestation_report_fixture()
        precheck.validate_post_apply_reattestation(valid, self.plan)
        mutations = {
            "missing_run_id": lambda report: report.pop("productive_run_id"),
            "wrong_execution_uuid": lambda report: report.update({"runner_execution_uuid": str(uuid.uuid4())}),
            "wrong_batch_id": lambda report: report.update({"batch_id": str(uuid.uuid4())}),
            "missing_baseline_sha": lambda report: report.pop("baseline_evidence_sha256"),
            "wrong_source_hash": lambda report: report["source_evidence"]["01"].update({"sha256": "0" * 64}),
            "wrong_role": lambda report: report["database_execution"].update({"current_user": "postgres"}),
            "not_readonly": lambda report: report["database_execution"].update({"transaction_read_only": "off"}),
            "writes_attempted": lambda report: report["database_execution"].update({"writes_attempted": True}),
            "wrong_active_count": lambda report: report["current_productive_state"].update({"active_batch_count": 2}),
        }
        for name, mutation in mutations.items():
            report = copy.deepcopy(valid)
            mutation(report)
            with self.subTest(name=name), self.assertRaises(precheck.PrecheckBlock):
                precheck.validate_post_apply_reattestation(report, self.plan)


if __name__ == "__main__":
    unittest.main()
