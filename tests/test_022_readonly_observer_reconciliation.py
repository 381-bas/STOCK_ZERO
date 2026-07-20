from __future__ import annotations

import copy
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from scripts import reconcile_route_b_readonly_observer as observer
from scripts import precheck_kpione_route_b_018_read_only as precheck


ROOT = Path(__file__).resolve().parents[1]
PLAN = json.loads((ROOT / "plans" / "018_kpione_route_b_productive_apply_plan.json").read_text(encoding="utf-8"))


class RouteBSnapshotCursor:
    def __init__(self, plan: dict, columns: dict[str, list[str]] | None = None, *, can_select: bool = True) -> None:
        self.plan = plan
        self.columns = columns or {
            name: list(spec["columns"])
            for name, spec in plan["physical_contract"]["object_signatures"].items()
        }
        self.can_select = can_select
        self.rows: list[tuple] = []
        self.commands: list[str] = []

    def execute(self, statement: str, _params=None) -> None:
        normalized = " ".join(str(statement).split()).lower()
        self.commands.append(normalized)
        if "information_schema.columns" in normalized:
            raise AssertionError("information_schema.columns must not be used for Route B signatures")
        if "from pg_class c join pg_namespace n" in normalized and "relkind" in normalized:
            expected = self.plan["physical_contract"]["object_signatures"]
            self.rows = [(name, spec["relation_kind"]) for name, spec in sorted(expected.items())]
        elif "join pg_attribute" in normalized:
            self.rows = [
                (name, column)
                for name in sorted(self.columns)
                for column in self.columns[name]
            ]
        elif normalized.startswith("select count(*) from cg_raw.kpione_raw_ingest_batch_v1"):
            self.rows = [(0,)]
        elif "bool_and(has_table_privilege" in normalized:
            self.rows = [(self.can_select,)]
        elif "from pg_roles" in normalized:
            self.rows = [(True,)]
        elif "has_schema_privilege" in normalized:
            self.rows = [(False, False)]
        else:
            self.rows = [(None,)]

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class RecordingCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement, _params=None) -> None:
        self.statements.append(str(statement))


class MinimalCursor:
    def __init__(self, readonly: str = "on") -> None:
        self.readonly = readonly
        self.rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, _params=None) -> None:
        normalized = " ".join(str(statement).split()).lower()
        if normalized.startswith("select current_user,session_user,current_database"):
            self.rows = [("postgres", "postgres", "postgres", self.readonly)]
        else:
            self.rows = [(None,)]

    def fetchone(self):
        return self.rows[0]


class MinimalConnection:
    def __init__(self, readonly: str = "on") -> None:
        self.autocommit = True
        self.cursor_value = MinimalCursor(readonly)
        self.rollback_called = False
        self.commit_called = False
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def rollback(self) -> None:
        self.rollback_called = True

    def commit(self) -> None:
        self.commit_called = True

    def close(self) -> None:
        self.closed = True


def compliant_privileges(plan: dict) -> dict:
    return {
        "schemas": {
            "cg_raw": {"USAGE": True, "CREATE": False},
            "cg_core": {"USAGE": True, "CREATE": False},
        },
        "objects": {
            name: {
                privilege: privilege == "SELECT"
                for privilege in observer.TABLE_PRIVILEGES
            }
            for name in plan["physical_contract"]["object_signatures"]
        },
        "sequence": {
            "name": "cg_raw.kpione_raw_event_photo_staging_v1_staging_id_seq",
            "privileges": {privilege: False for privilege in observer.SEQUENCE_PRIVILEGES},
        },
    }


class ReadonlyObserverReconciliation022Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = copy.deepcopy(PLAN)

    def test_pg_attribute_exact_signature_matches(self) -> None:
        signatures, active_batches = precheck._route_b_snapshot(RouteBSnapshotCursor(self.plan), self.plan)
        self.assertEqual(active_batches, 0)
        self.assertEqual({item["object"] for item in signatures}, set(self.plan["physical_contract"]["object_signatures"]))

    def test_pg_attribute_order_mismatch_blocks(self) -> None:
        columns = {name: list(spec["columns"]) for name, spec in self.plan["physical_contract"]["object_signatures"].items()}
        first = sorted(columns)[0]
        columns[first] = list(reversed(columns[first]))
        with self.assertRaisesRegex(precheck.PrecheckBlock, "route_b_object_column_signature_mismatch"):
            precheck._route_b_snapshot(RouteBSnapshotCursor(self.plan, columns), self.plan)

    def test_pg_attribute_missing_column_blocks(self) -> None:
        columns = {name: list(spec["columns"]) for name, spec in self.plan["physical_contract"]["object_signatures"].items()}
        first = sorted(columns)[0]
        columns[first] = columns[first][:-1]
        with self.assertRaisesRegex(precheck.PrecheckBlock, "route_b_object_column_signature_mismatch"):
            precheck._route_b_snapshot(RouteBSnapshotCursor(self.plan, columns), self.plan)

    def test_pg_attribute_extra_column_blocks(self) -> None:
        columns = {name: list(spec["columns"]) for name, spec in self.plan["physical_contract"]["object_signatures"].items()}
        first = sorted(columns)[0]
        columns[first].append("unexpected_column")
        with self.assertRaisesRegex(precheck.PrecheckBlock, "route_b_object_column_signature_mismatch"):
            precheck._route_b_snapshot(RouteBSnapshotCursor(self.plan, columns), self.plan)

    def test_information_schema_hidden_columns_do_not_drive_false_mismatch(self) -> None:
        cursor = RouteBSnapshotCursor(self.plan)
        precheck._route_b_snapshot(cursor, self.plan)
        self.assertFalse(any("information_schema.columns" in command for command in cursor.commands))

    def test_missing_select_privilege_blocks_post_provision(self) -> None:
        cursor = RouteBSnapshotCursor(self.plan, can_select=False)
        signatures, active_batches = precheck._route_b_snapshot(cursor, self.plan)
        baseline = {
            "target_fingerprint": precheck.target_fingerprint(self.plan),
            "legacy": {},
            "public_acl": {},
        }
        with patch("scripts.precheck_kpione_route_b_018_read_only.assert_legacy_structural_invariance"), \
                self.assertRaisesRegex(precheck.PrecheckBlock, "readonly_observer_select_privilege_missing"):
            precheck._assert_post_provision(cursor, self.plan, signatures, active_batches, {}, {}, baseline)

    def test_reconciliation_grants_only_usage_and_select(self) -> None:
        cursor = RecordingCursor()
        observer._grant_observer_privileges(cursor, observer.EXPECTED_READONLY_ROLE, self.plan)
        rendered = "\n".join(cursor.statements).upper()
        self.assertIn("GRANT USAGE ON SCHEMA", rendered)
        self.assertIn("GRANT SELECT ON TABLE", rendered)
        self.assertNotIn("GRANT INSERT", rendered)
        self.assertNotIn("GRANT UPDATE", rendered)
        self.assertNotIn("GRANT DELETE", rendered)

    def test_compliance_rejects_dml_privileges(self) -> None:
        snapshot = compliant_privileges(self.plan)
        first = sorted(snapshot["objects"])[0]
        snapshot["objects"][first]["DELETE"] = True
        self.assertFalse(observer._is_compliant(snapshot, self.plan))

    def test_compliance_rejects_schema_create(self) -> None:
        snapshot = compliant_privileges(self.plan)
        snapshot["schemas"]["cg_raw"]["CREATE"] = True
        self.assertFalse(observer._is_compliant(snapshot, self.plan))

    def test_compliance_rejects_sequence_privileges(self) -> None:
        snapshot = compliant_privileges(self.plan)
        snapshot["sequence"]["privileges"]["USAGE"] = True
        self.assertFalse(observer._is_compliant(snapshot, self.plan))

    def test_public_acl_and_legacy_snapshots_are_compared_before_commit(self) -> None:
        source = Path("scripts/reconcile_route_b_readonly_observer.py").read_text(encoding="utf-8")
        self.assertIn("legacy_structural_identity(after_legacy)", source)
        self.assertIn("after_public_acl != before_public_acl", source)

    def test_idempotent_compliant_state_writes_evidence_without_db_commit(self) -> None:
        dsn = f"postgresql://postgres:synthetic@{self.plan['target']['expected_hostname']}/postgres?sslmode=require"
        connection = MinimalConnection()
        guard = {"approved_git_sha": "a" * 40, "plan_sha256": "b" * 64}
        with tempfile.TemporaryDirectory() as folder, \
                patch("scripts.reconcile_route_b_readonly_observer._physical_route_b_signatures", return_value={
                    name: {"relation_kind": spec["relation_kind"], "columns": spec["columns"]}
                    for name, spec in self.plan["physical_contract"]["object_signatures"].items()
                }), \
                patch("scripts.reconcile_route_b_readonly_observer._signature_diffs", return_value=[]), \
                patch("scripts.reconcile_route_b_readonly_observer._route_b_sequence", return_value="seq"), \
                patch("scripts.reconcile_route_b_readonly_observer._privilege_snapshot", return_value=compliant_privileges(self.plan)), \
                patch("scripts.reconcile_route_b_readonly_observer._legacy_snapshot", return_value={"object_identity": "cg_raw.kpione2_raw"}), \
                patch("scripts.reconcile_route_b_readonly_observer._public_acl_snapshot", return_value={"schemas": {}, "relations": {}}), \
                patch("scripts.reconcile_route_b_readonly_observer._role_attributes", return_value={"login": True}):
            evidence = Path(folder) / "evidence.json"
            report = observer.reconcile_readonly_observer(
                self.plan,
                dsn,
                evidence,
                maintenance_run_id=str(uuid.uuid4()),
                git_guard=guard,
                connect_fn=lambda _dsn: connection,
            )
        self.assertEqual(report["verdict"], "PASS_READONLY_OBSERVER_ALREADY_COMPLIANT")
        self.assertFalse(report["writes_attempted"])
        self.assertFalse(report["committed"])
        self.assertTrue(connection.rollback_called)
        self.assertFalse(connection.commit_called)

    def test_write_failure_before_commit_rolls_back(self) -> None:
        dsn = f"postgresql://postgres:synthetic@{self.plan['target']['expected_hostname']}/postgres?sslmode=require"
        diagnostic = MinimalConnection()
        writer = MinimalConnection(readonly="off")
        noncompliant = compliant_privileges(self.plan)
        first = sorted(noncompliant["objects"])[0]
        noncompliant["objects"][first]["SELECT"] = False
        guard = {"approved_git_sha": "a" * 40, "plan_sha256": "b" * 64}
        connections = iter((diagnostic, writer))
        with tempfile.TemporaryDirectory() as folder, \
                patch("scripts.reconcile_route_b_readonly_observer._physical_route_b_signatures", return_value={
                    name: {"relation_kind": spec["relation_kind"], "columns": spec["columns"]}
                    for name, spec in self.plan["physical_contract"]["object_signatures"].items()
                }), \
                patch("scripts.reconcile_route_b_readonly_observer._signature_diffs", return_value=[]), \
                patch("scripts.reconcile_route_b_readonly_observer._route_b_sequence", return_value="seq"), \
                patch("scripts.reconcile_route_b_readonly_observer._privilege_snapshot", return_value=noncompliant), \
                patch("scripts.reconcile_route_b_readonly_observer._legacy_snapshot", return_value={"object_identity": "cg_raw.kpione2_raw"}), \
                patch("scripts.reconcile_route_b_readonly_observer._public_acl_snapshot", return_value={"schemas": {}, "relations": {}}), \
                patch("scripts.reconcile_route_b_readonly_observer._role_attributes", return_value={"login": True}), \
                patch("scripts.reconcile_route_b_readonly_observer._grant_observer_privileges", side_effect=RuntimeError("synthetic")):
            with self.assertRaises(RuntimeError):
                observer.reconcile_readonly_observer(
                    self.plan,
                    dsn,
                    Path(folder) / "evidence.json",
                    maintenance_run_id=str(uuid.uuid4()),
                    git_guard=guard,
                    connect_fn=lambda _dsn: next(connections),
                )
        self.assertTrue(writer.rollback_called)
        self.assertFalse(writer.commit_called)

    def test_evidence_contains_no_dsn_password_or_secret_values(self) -> None:
        dsn = f"postgresql://postgres:synthetic-admin-password@{self.plan['target']['expected_hostname']}/postgres?sslmode=require"
        connection = MinimalConnection()
        guard = {"approved_git_sha": "a" * 40, "plan_sha256": "b" * 64}
        with tempfile.TemporaryDirectory() as folder, \
                patch("scripts.reconcile_route_b_readonly_observer._physical_route_b_signatures", return_value={
                    name: {"relation_kind": spec["relation_kind"], "columns": spec["columns"]}
                    for name, spec in self.plan["physical_contract"]["object_signatures"].items()
                }), \
                patch("scripts.reconcile_route_b_readonly_observer._signature_diffs", return_value=[]), \
                patch("scripts.reconcile_route_b_readonly_observer._route_b_sequence", return_value="seq"), \
                patch("scripts.reconcile_route_b_readonly_observer._privilege_snapshot", return_value=compliant_privileges(self.plan)), \
                patch("scripts.reconcile_route_b_readonly_observer._legacy_snapshot", return_value={"object_identity": "cg_raw.kpione2_raw"}), \
                patch("scripts.reconcile_route_b_readonly_observer._public_acl_snapshot", return_value={"schemas": {}, "relations": {}}), \
                patch("scripts.reconcile_route_b_readonly_observer._role_attributes", return_value={"login": True}):
            evidence = Path(folder) / "evidence.json"
            observer.reconcile_readonly_observer(
                self.plan,
                dsn,
                evidence,
                maintenance_run_id=str(uuid.uuid4()),
                git_guard=guard,
                connect_fn=lambda _dsn: connection,
            )
            rendered = evidence.read_text(encoding="utf-8")
        self.assertNotIn(dsn, rendered)
        self.assertNotIn("synthetic-admin-password", rendered)
        self.assertNotIn("STOCK_ZERO_DB_ADMIN", rendered)


if __name__ == "__main__":
    unittest.main()
