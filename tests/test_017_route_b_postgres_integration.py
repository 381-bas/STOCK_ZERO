from __future__ import annotations

import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import psycopg

from scripts.kpione_route_b_v1 import (
    ADVISORY_LOCK_KEY,
    PLANNED_PRODUCTIVE_ROLE,
    RouteBError,
    apply_local,
    build_plan,
    classify_global_photo_duplicates,
    rollback_local,
    run_productive_apply,
    run_productive_rollback,
)
from scripts.provision_kpione_route_b_role import (
    ProvisioningError,
    _blocked_report,
    provision_route_b_role,
)
from tests.test_017_route_b_runner import HEADERS, row, write_book


DSN = os.environ.get("DB_URL_CODEX_LOCAL")
DDL = Path(__file__).resolve().parents[1] / "sql" / "17_kpione_route_b_ingestion_v1.sql"
PRODUCTIVE_PLAN = Path(__file__).resolve().parents[1] / "plans" / "018_kpione_route_b_productive_apply_plan.json"


@unittest.skipUnless(DSN, "DB_URL_CODEX_LOCAL is required for the local PostgreSQL rehearsal")
class RouteBPostgresRehearsal(unittest.TestCase):
    def setUp(self) -> None:
        with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('cg_raw.kpione_raw_ingest_batch_v1')")
            if cursor.fetchone()[0]:
                cursor.execute(
                    "TRUNCATE cg_raw.kpione_raw_event_photo_staging_v1, "
                    "cg_raw.kpione_raw_ingest_batch_file_v1, "
                    "cg_raw.kpione_raw_ingest_batch_v1 RESTART IDENTITY CASCADE"
                )

    def counts(self) -> tuple[int, int, int]:
        with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_event_photo_staging_v1")
            staging = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM cg_core.kpione_event_normalized_v1")
            events = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM cg_core.kpione_day_presence_v1")
            presence = cursor.fetchone()[0]
        return staging, events, presence

    def active_batch_count(self) -> int:
        with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_ingest_batch_v1 WHERE status='ACTIVE'")
            return cursor.fetchone()[0]

    def test_full_rehearsal(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "photo-excel-admin_1.xlsx"
            write_book(source, [row(100, 900), row(101, 901, link="https://example.invalid/b.jpg")])
            first_plan = build_plan(root)

            first = apply_local(first_plan, DSN, DDL)
            self.assertEqual(first["outcome"], "ACTIVE")
            self.assertEqual(self.counts(), (2, 2, 1))

            same = apply_local(first_plan, DSN, DDL)
            self.assertEqual(same["outcome"], "NO_OP_ALREADY_REGISTERED")
            source.rename(root / "photo-excel-admin_renamed.xlsx")
            renamed_plan = build_plan(root)
            renamed = apply_local(renamed_plan, DSN, DDL)
            self.assertEqual(renamed["outcome"], "NO_OP_SAME_SOURCE_VERSION")

            malformed = root / "photo-excel-admin_bad.xlsx"
            write_book(malformed, [row()], HEADERS[:-1])
            with self.assertRaisesRegex(RouteBError, "missing_required_columns"):
                build_plan(root)
            malformed.unlink()

            conflict = root / "photo-excel-admin_conflict.xlsx"
            write_book(conflict, [row(200, 920), row(200, 921, link="https://example.invalid/c.jpg")])
            with self.assertRaisesRegex(RouteBError, "event_stability_conflict"):
                build_plan(root)
            conflict.unlink()

            corrected = root / "photo-excel-admin_corrected.xlsx"
            for path in root.glob("photo-excel-admin_*.xlsx"):
                path.unlink()
            write_book(corrected, [row(100, 900)])
            corrected_plan = build_plan(root)
            pending = apply_local(corrected_plan, DSN, DDL)
            self.assertEqual(pending["outcome"], "NEW_SOURCE_VERSION_PENDING_SUPERSESSION")
            successor = apply_local(corrected_plan, DSN, DDL, first["batch_id"])
            self.assertEqual(successor["outcome"], "ACTIVE")
            self.assertEqual(self.counts(), (3, 1, 1))

            restored = rollback_local(DSN, successor["batch_id"])
            self.assertEqual(restored["outcome"], "ROLLED_BACK")
            self.assertEqual(restored["restored_batch_id"], first["batch_id"])
            self.assertEqual(self.counts(), (3, 2, 1))

            replay = build_plan(root)
            self.assertEqual(replay["semantic_plan_hash"], corrected_plan["semantic_plan_hash"])
            self.assertEqual(replay["day_presence_count"], corrected_plan["day_presence_count"])

    def test_mixed_folders_classify_every_discovered_version(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source_a = root / "photo-excel-admin_a.xlsx"
            write_book(source_a, [row(100, 900, "2026-07-01")])
            first = apply_local(build_plan(root), DSN, DDL)

            source_b = root / "photo-excel-admin_b.xlsx"
            write_book(source_b, [row(200, 920, "2026-07-02")])
            mixed = apply_local(build_plan(root), DSN, DDL)
            self.assertEqual(mixed["outcome"], "ACTIVE")
            self.assertEqual(mixed["discovered_file_count"], 2)
            self.assertEqual(mixed["already_active_file_count"], 1)
            self.assertEqual(mixed["new_file_count"], 1)
            self.assertEqual(mixed["files_selected_for_staging"], 1)
            self.assertEqual(mixed["files_skipped_as_no_op"], 1)
            self.assertEqual(mixed["expected_inserts"], 1)
            self.assertEqual(self.counts(), (2, 2, 2))

            source_renamed = root / "photo-excel-admin_a_renamed.xlsx"
            source_renamed.write_bytes(source_a.read_bytes())
            all_known = apply_local(build_plan(root), DSN, DDL)
            self.assertEqual(all_known["outcome"], "NO_OP_SAME_SOURCE_VERSION")
            self.assertEqual(all_known["already_active_file_count"], 3)
            self.assertEqual(all_known["renamed_no_op_count"], 1)
            self.assertEqual(all_known["new_file_count"], 0)

            malformed = root / "photo-excel-admin_bad.xlsx"
            write_book(malformed, [row()], HEADERS[:-1])
            with self.assertRaisesRegex(RouteBError, "missing_required_columns"):
                build_plan(root)
            self.assertEqual(self.active_batch_count(), 2)
            self.assertEqual(self.counts(), (2, 2, 2))

    def test_mixed_corrected_content_requires_explicit_supersession(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source_a = root / "photo-excel-admin_a.xlsx"
            write_book(source_a, [row(100, 900, "2026-07-01")])
            first = apply_local(build_plan(root), DSN, DDL)

            corrected = root / "photo-excel-admin_corrected.xlsx"
            write_book(corrected, [row(200, 920, "2026-07-01")])
            corrected_plan = build_plan(root)
            pending = apply_local(corrected_plan, DSN, DDL)
            self.assertEqual(pending["outcome"], "NEW_SOURCE_VERSION_PENDING_SUPERSESSION")
            self.assertEqual(pending["already_active_file_count"], 1)
            self.assertEqual(pending["new_file_count"], 1)
            self.assertTrue(pending["expected_supersession_requirement"])
            self.assertIn("NEW_SOURCE_VERSION_REQUIRES_SUPERSESSION",
                          {item["classification"] for item in pending["files"]})

            successor = apply_local(corrected_plan, DSN, DDL, first["batch_id"])
            self.assertEqual(successor["outcome"], "ACTIVE")
            self.assertEqual(self.active_batch_count(), 1)
            self.assertEqual(self.counts(), (2, 1, 1))

    def test_concurrent_identical_plan_activates_once(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_book(root / "photo-excel-admin_a.xlsx", [row()])
            plan = build_plan(root)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _: apply_local(plan, DSN, DDL), range(2)))
            self.assertEqual(sorted(result["outcome"] for result in results),
                             ["ACTIVE", "NO_OP_ALREADY_REGISTERED"])
            self.assertEqual(self.active_batch_count(), 1)
            self.assertEqual(self.counts(), (1, 1, 1))

    def test_concurrent_overlapping_content_activates_once(self) -> None:
        with tempfile.TemporaryDirectory() as first_folder, tempfile.TemporaryDirectory() as second_folder:
            first_root, second_root = Path(first_folder), Path(second_folder)
            write_book(first_root / "photo-excel-admin_a.xlsx", [row(100, 900)])
            write_book(second_root / "photo-excel-admin_b.xlsx", [row(200, 920)])
            plans = [build_plan(first_root), build_plan(second_root)]
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda plan: apply_local(plan, DSN, DDL), plans))
            self.assertEqual(sorted(result["outcome"] for result in results),
                             ["ACTIVE", "NEW_SOURCE_VERSION_PENDING_SUPERSESSION"])
            self.assertEqual(self.active_batch_count(), 1)
            self.assertEqual(self.counts(), (1, 1, 1))

    def test_advisory_lock_and_active_coverage_constraint_exist(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_book(root / "photo-excel-admin_a.xlsx", [row()])
            apply_local(build_plan(root), DSN, DDL)
        with psycopg.connect(DSN) as first, psycopg.connect(DSN) as second:
            with first.transaction(), first.cursor() as first_cursor, second.transaction(), second.cursor() as second_cursor:
                first_cursor.execute("SELECT pg_advisory_xact_lock(%s)", (ADVISORY_LOCK_KEY,))
                second_cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", (ADVISORY_LOCK_KEY,))
                self.assertFalse(second_cursor.fetchone()[0])
                second_cursor.execute(
                    "SELECT count(*) FROM pg_constraint WHERE conname='kpione_one_active_coverage_v1' "
                    "AND contype='x'"
                )
                self.assertEqual(second_cursor.fetchone()[0], 1)

    def test_productive_core_apply_postcheck_and_logical_rollback_on_local_postgres(self) -> None:
        with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (PLANNED_PRODUCTIVE_ROLE,))
            if cursor.fetchone():
                cursor.execute("DROP OWNED BY stock_zero_kpione_route_b_load")
                cursor.execute("DROP ROLE stock_zero_kpione_route_b_load")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS cg_raw")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS cg_core")
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS cg_raw.kpione2_raw (legacy_evidence_id bigint)"
            )
            cursor.execute(
                "INSERT INTO cg_raw.kpione2_raw(legacy_evidence_id) "
                "SELECT 19 WHERE NOT EXISTS (SELECT 1 FROM cg_raw.kpione2_raw)"
            )
            cursor.execute("SELECT current_user,current_database(),to_regclass('cg_raw.kpione2_raw')::text")
            admin_role, database, legacy_before = cursor.fetchone()
            cursor.execute("SELECT count(*) FROM cg_raw.kpione2_raw")
            legacy_rows_before = cursor.fetchone()[0]
            cursor.execute(
                "SELECT n.nspname,COALESCE((SELECT string_agg(a.privilege_type,',' "
                "ORDER BY a.privilege_type) FROM aclexplode(COALESCE(n.nspacl,"
                "acldefault('n',n.nspowner))) a WHERE a.grantee=0),'') "
                "FROM pg_namespace n WHERE n.nspname IN ('cg_raw','cg_core') ORDER BY n.nspname"
            )
            public_schema_privileges_before = cursor.fetchall()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_book(root / "photo-excel-admin_productive_fixture.xlsx", [row()])
            local_plan = build_plan(root)
            workbook = local_plan["_workbooks"][0]
            classified = classify_global_photo_duplicates([workbook])
            plan = json.loads(PRODUCTIVE_PLAN.read_text(encoding="utf-8"))
            plan["status"] = "READY_FOR_PRODUCTIVE_EXECUTION"
            plan["remaining_blockers"] = []
            plan["readonly_precheck"] = {"status": "PASSED", "evidence_sha256": "c" * 64}
            plan["target"].update({
                "expected_database": database,
                "planned_productive_role": PLANNED_PRODUCTIVE_ROLE,
                "productive_role_status": "PROVISIONED_AND_VERIFIED",
                "allowed_productive_roles": [PLANNED_PRODUCTIVE_ROLE],
            })
            plan["activation_gate"].update({
                "productive_role_registered": True,
                "gate_open": True,
            })
            plan["productive_apply_authorized"] = True
            plan["productive_rollback_authorized"] = True
            plan["source_package"].update({
                "approved_file_count": 1,
                "expected_source_rows": 1,
                "expected_distinct_events": 1,
                "expected_duplicate_photo_rows": 0,
                "expected_exact_duplicate_rows": 0,
                "expected_cross_file_duplicate_rows": 0,
                "expected_event_conflicts": 0,
                "expected_day_presence_rows": 1,
                "semantic_plan_hash": local_plan["semantic_plan_hash"],
                "expected_coverage": {
                    "start": workbook.coverage_start,
                    "end": workbook.coverage_end,
                    "distinct_dates": 1,
                    "missing_dates": [],
                },
            })
            plan["postcheck_queries"] = [
                query.replace("count(*)=10089", "count(*)=0")
                .replace("THEN 35287", "THEN 1")
                .replace("THEN 34996", "THEN 1")
                for query in plan["postcheck_queries"]
            ]
            approved = {
                "files": [{"source_file_sha256": workbook.source_file_sha256}],
                "_workbooks": [workbook],
                "_classified_rows": classified,
            }
            admin_password = "synthetic-admin-password"
            role_password = "synthetic-role-password"
            expected_hostname = plan["target"]["expected_hostname"]
            synthetic_admin_dsn = (
                "postgresql://" + f"{admin_role}:{admin_password}@" +
                f"{expected_hostname}/{database}?sslmode=require"
            )
            synthetic_dsn = (
                "postgresql://" + f"{PLANNED_PRODUCTIVE_ROLE}:{role_password}@" +
                f"{expected_hostname}/{database}?sslmode=require"
            )
            git_guard = {
                "approved_git_sha": "a" * 40,
                "plan_path": PRODUCTIVE_PLAN.relative_to(Path(__file__).resolve().parents[1]).as_posix(),
                "plan_sha256": "b" * 64,
                "ddl_sha256": plan["physical_contract"]["sql_sha256"],
            }
            provision_report = provision_route_b_role(
                plan, synthetic_admin_dsn, role_password, root / "provision.json",
                root=Path(__file__).resolve().parents[1],
                connect_fn=lambda _dsn: psycopg.connect(DSN),
                expected_admin_username=admin_role,
                git_guard=git_guard,
            )
            reprovision_report = provision_route_b_role(
                plan, synthetic_admin_dsn, role_password, root / "reprovision.json",
                root=Path(__file__).resolve().parents[1],
                connect_fn=lambda _dsn: psycopg.connect(DSN),
                expected_admin_username=admin_role,
                git_guard=git_guard,
            )
            with patch(
                "scripts.provision_kpione_route_b_role._write_provisioning_evidence",
                side_effect=OSError("synthetic evidence failure"),
            ), self.assertRaisesRegex(
                ProvisioningError, "admin_provisioning_evidence_write_failed"
            ) as evidence_context:
                provision_route_b_role(
                    plan, synthetic_admin_dsn, role_password, root / "failed-evidence.json",
                    root=Path(__file__).resolve().parents[1],
                    connect_fn=lambda _dsn: psycopg.connect(DSN),
                    expected_admin_username=admin_role,
                    git_guard=git_guard,
                )
            evidence_failure = evidence_context.exception
            blocked_evidence_report = _blocked_report(evidence_failure)
            connect_fn = lambda _dsn: psycopg.connect(DSN, user=PLANNED_PRODUCTIVE_ROLE)
            apply_report = run_productive_apply(
                plan, approved, synthetic_dsn, root / "apply.json",
                git_guard=git_guard, root=Path(__file__).resolve().parents[1],
                connect_fn=connect_fn,
            )
            rollback_report = run_productive_rollback(
                plan, approved, apply_report["batch_id"], synthetic_dsn,
                root / "rollback.json", git_guard=git_guard, connect_fn=connect_fn,
            )

            with psycopg.connect(DSN, user=PLANNED_PRODUCTIVE_ROLE) as role_connection:
                with role_connection.cursor() as role_cursor:
                    role_cursor.execute(
                        "INSERT INTO cg_raw.kpione_raw_event_photo_staging_v1("
                        "batch_id,source_file_sha256,source_sheet,source_row_number,"
                        "source_row_identity,event_id,sp_item_id,source_payload,photo_row_hash,"
                        "event_stable_hash,event_date,location_key,cliente_norm,"
                        "duplicate_classification,conflict_classification) "
                        "SELECT batch_id,source_file_sha256,source_sheet,source_row_number+100000,"
                        "repeat('a',64),event_id||'-privilege-probe',sp_item_id,source_payload,"
                        "repeat('b',64),repeat('c',64),event_date,location_key,cliente_norm,"
                        "duplicate_classification,conflict_classification "
                        "FROM cg_raw.kpione_raw_event_photo_staging_v1 LIMIT 1"
                    )
                    self.assertEqual(role_cursor.rowcount, 1)
                role_connection.rollback()

            denied_statements = (
                "UPDATE cg_raw.kpione_raw_event_photo_staging_v1 SET cliente_norm=cliente_norm",
                "DELETE FROM cg_raw.kpione_raw_event_photo_staging_v1",
                "UPDATE cg_raw.kpione_raw_ingest_batch_file_v1 SET validation_status=validation_status",
                "DELETE FROM cg_raw.kpione_raw_ingest_batch_file_v1",
                "UPDATE cg_raw.kpione_raw_ingest_batch_v1 SET semantic_plan_hash=semantic_plan_hash",
                "UPDATE cg_raw.kpione_raw_ingest_batch_v1 SET coverage_start=coverage_start",
                "DELETE FROM cg_raw.kpione_raw_ingest_batch_v1",
            )
            for statement in denied_statements:
                with self.subTest(statement=statement), psycopg.connect(
                    DSN, user=PLANNED_PRODUCTIVE_ROLE
                ) as denied_connection:
                    with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                        denied_connection.execute(statement)
        with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_event_photo_staging_v1")
            staging_after = cursor.fetchone()[0]
            cursor.execute("SELECT to_regclass('cg_raw.kpione2_raw')::text")
            legacy_after = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM cg_raw.kpione2_raw")
            legacy_rows_after = cursor.fetchone()[0]
            cursor.execute(
                "SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,"
                "rolbypassrls,rolconnlimit FROM pg_roles WHERE rolname=%s",
                (PLANNED_PRODUCTIVE_ROLE,),
            )
            role_attributes = cursor.fetchone()
            cursor.execute(
                "SELECT has_schema_privilege(%s,'cg_raw','CREATE'),"
                "has_table_privilege(%s,'cg_raw.kpione2_raw','SELECT'),"
                "has_table_privilege(%s,'cg_raw.kpione_raw_ingest_batch_v1','SELECT,INSERT'),"
                "has_column_privilege(%s,'cg_raw.kpione_raw_ingest_batch_v1','status','UPDATE'),"
                "has_column_privilege(%s,'cg_raw.kpione_raw_ingest_batch_v1','semantic_plan_hash','UPDATE'),"
                "has_table_privilege(%s,'cg_raw.kpione_raw_ingest_batch_v1','DELETE')",
                (PLANNED_PRODUCTIVE_ROLE,) * 6,
            )
            (create_schema, legacy_select, route_b_select_insert, status_update,
             hash_update, route_b_delete) = cursor.fetchone()
            cursor.execute(
                "SELECT n.nspname,COALESCE((SELECT string_agg(a.privilege_type,',' "
                "ORDER BY a.privilege_type) FROM aclexplode(COALESCE(n.nspacl,"
                "acldefault('n',n.nspowner))) a WHERE a.grantee=0),'') "
                "FROM pg_namespace n WHERE n.nspname IN ('cg_raw','cg_core') ORDER BY n.nspname"
            )
            public_schema_privileges_after = cursor.fetchall()
            cursor.execute(
                "SELECT DISTINCT c.relowner::regrole::text FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname||'.'||c.relname = ANY(%s)",
                (plan["physical_contract"]["objects"],),
            )
            route_b_owners = {row[0] for row in cursor.fetchall()}
        self.assertEqual(provision_report["verdict"], "PASS_ADMIN_PROVISIONING")
        self.assertTrue(provision_report["role_created"])
        self.assertEqual(reprovision_report["verdict"], "PASS_ADMIN_PROVISIONING")
        self.assertFalse(reprovision_report["role_created"])
        self.assertTrue(evidence_failure.committed)
        self.assertTrue(evidence_failure.connection_attempted)
        self.assertTrue(evidence_failure.writes_attempted)
        self.assertEqual(evidence_failure.report["error"], "admin_provisioning_evidence_write_failed")
        self.assertTrue(evidence_failure.report["rollback_or_reconciliation_required"])
        for key in (
            "approved_git_sha", "plan_sha256", "ddl_sha256", "target_fingerprint",
            "planned_productive_role", "role_created", "legacy_object_unchanged",
        ):
            self.assertIn(key, evidence_failure.report)
        rendered_failure = json.dumps(evidence_failure.report)
        self.assertEqual(blocked_evidence_report["error"], "admin_provisioning_evidence_write_failed")
        self.assertTrue(blocked_evidence_report["committed"])
        self.assertTrue(blocked_evidence_report["rollback_or_reconciliation_required"])
        self.assertEqual(blocked_evidence_report["approved_git_sha"], "a" * 40)
        self.assertNotIn(synthetic_admin_dsn, rendered_failure)
        self.assertNotIn(admin_password, rendered_failure)
        self.assertNotIn(role_password, rendered_failure)
        self.assertNotIn(synthetic_admin_dsn, json.dumps(provision_report))
        self.assertNotIn(admin_password, json.dumps(provision_report))
        self.assertNotIn(role_password, json.dumps(provision_report))
        self.assertEqual(apply_report["postcheck_verdict"], "PASS")
        self.assertEqual(rollback_report["postcheck_verdict"], "PASS")
        self.assertEqual(apply_report["postcheck"]["declared_failures"], [])
        self.assertEqual(rollback_report["postcheck"]["declared_failures"], [])
        apply_booleans = {
            item["index"]: item["boolean_result"]
            for item in apply_report["postcheck"]["declared_postchecks"]
            if item.get("result_type") == "boolean"
        }
        rollback_booleans = {
            item["index"]: item["boolean_result"]
            for item in rollback_report["postcheck"]["declared_postchecks"]
            if item.get("result_type") == "boolean"
        }
        self.assertEqual(apply_booleans, {7: True, 10: True, 11: True})
        self.assertEqual(rollback_booleans, {7: True, 10: True, 11: True})
        self.assertEqual(apply_report["postcheck"]["legacy_object"], "cg_raw.kpione2_raw")
        self.assertEqual(rollback_report["postcheck"]["legacy_object"], "cg_raw.kpione2_raw")
        self.assertEqual(staging_after, 1)
        self.assertEqual(legacy_before, "cg_raw.kpione2_raw")
        self.assertEqual(legacy_after, legacy_before)
        self.assertEqual(legacy_rows_after, legacy_rows_before)
        self.assertEqual(role_attributes, (True, False, False, False, False, False, 5))
        self.assertFalse(create_schema)
        self.assertFalse(legacy_select)
        self.assertTrue(route_b_select_insert)
        self.assertTrue(status_update)
        self.assertFalse(hash_update)
        self.assertFalse(route_b_delete)
        self.assertEqual(route_b_owners, {admin_role})
        self.assertEqual(public_schema_privileges_after, public_schema_privileges_before)


if __name__ == "__main__":
    unittest.main()
