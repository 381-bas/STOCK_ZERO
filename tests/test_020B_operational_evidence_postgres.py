from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

import psycopg

from scripts.build_kpione_route_b_infrastructure_evidence import build_bundle
from scripts.kpione_route_b_v1 import PLANNED_PRODUCTIVE_ROLE
from scripts.precheck_kpione_route_b_018_read_only import (
    PrecheckBlock,
    run_precheck,
)
from scripts.provision_kpione_route_b_role import provision_route_b_role
from scripts.verify_kpione_route_b_productive_role import verify_productive_role


ROOT = Path(__file__).resolve().parents[1]
DSN = os.environ.get("DB_URL_CODEX_LOCAL")
PLAN_PATH = ROOT / "plans" / "018_kpione_route_b_productive_apply_plan.json"
DDL_PATH = ROOT / "sql" / "17_kpione_route_b_ingestion_v1.sql"
READONLY_ROLE = "stock_zero_codex_ro"


@unittest.skipUnless(DSN, "DB_URL_CODEX_LOCAL is required for the 020B PostgreSQL evidence rehearsal")
class OperationalEvidencePostgres020BTests(unittest.TestCase):
    def setUp(self) -> None:
        with psycopg.connect(DSN, autocommit=True) as connection, connection.cursor() as cursor:
            for role in (PLANNED_PRODUCTIVE_ROLE, READONLY_ROLE):
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
                if cursor.fetchone():
                    cursor.execute(f'DROP OWNED BY "{role}"')
                    cursor.execute(f'DROP ROLE "{role}"')
            cursor.execute("DROP SCHEMA IF EXISTS cg_core CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS cg_raw CASCADE")
            cursor.execute(f'CREATE ROLE "{READONLY_ROLE}" LOGIN')
            cursor.execute("CREATE SCHEMA cg_raw")
            cursor.execute("CREATE SCHEMA cg_core")
            cursor.execute("CREATE TABLE cg_raw.kpione2_raw(legacy_evidence_id bigint)")
            cursor.execute("INSERT INTO cg_raw.kpione2_raw VALUES (19)")
            cursor.execute(f'GRANT USAGE ON SCHEMA cg_raw,cg_core TO "{READONLY_ROLE}"')
            cursor.execute(f'GRANT SELECT ON cg_raw.kpione2_raw TO "{READONLY_ROLE}"')

    def tearDown(self) -> None:
        with psycopg.connect(DSN, autocommit=True) as connection, connection.cursor() as cursor:
            for role in (PLANNED_PRODUCTIVE_ROLE, READONLY_ROLE):
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
                if cursor.fetchone():
                    cursor.execute(f'DROP OWNED BY "{role}"')
            cursor.execute("DROP SCHEMA IF EXISTS cg_core CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS cg_raw CASCADE")
            for role in (PLANNED_PRODUCTIVE_ROLE, READONLY_ROLE):
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
                if cursor.fetchone():
                    cursor.execute(f'DROP ROLE "{role}"')

    @staticmethod
    def _write(path: Path, value: dict[str, object]) -> None:
        path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def test_gate_zero_full_evidence_cycle_uses_rollback_and_preserves_legacy(self) -> None:
        plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
        plan_sha = hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest()
        authority = {
            "approved_git_sha": "a" * 40,
            "plan_path": "plans/018_kpione_route_b_productive_apply_plan.json",
            "plan_sha256": plan_sha,
            "ddl_sha256": plan["physical_contract"]["sql_sha256"],
        }
        readonly_connect = lambda _dsn: psycopg.connect(DSN, user=READONLY_ROLE)
        baseline = run_precheck(
            plan,
            "synthetic-readonly-dsn",
            readonly_connect,
            check_stage="baseline",
            authority=authority,
        )
        self.assertEqual(baseline["verdict"], "PASS_READONLY_BASELINE")
        self.assertEqual(baseline["active_batch_count"], 0)
        self.assertEqual(baseline["legacy"]["row_count"], 1)
        self.assertEqual(baseline["route_b_signatures"], [])

        with tempfile.TemporaryDirectory() as folder:
            evidence_root = Path(folder)
            baseline_path = evidence_root / "baseline.json"
            self._write(baseline_path, baseline)
            baseline_sha = hashlib.sha256(baseline_path.read_bytes()).hexdigest()
            with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
                cursor.execute("SELECT current_user,current_database()")
                admin_role, database = cursor.fetchone()
            admin_dsn = (
                f"postgresql://{admin_role}:synthetic-admin@"
                f"{plan['target']['expected_hostname']}/{database}?sslmode=require"
            )
            admin_path = evidence_root / "admin.json"
            admin = provision_route_b_role(
                plan,
                admin_dsn,
                "synthetic-productive-password",
                admin_path,
                root=ROOT,
                connect_fn=lambda _dsn: psycopg.connect(DSN),
                expected_admin_username=admin_role,
                git_guard=authority,
            )
            self.assertEqual(admin["verdict"], "PASS_ADMIN_PROVISIONING")

            with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
                cursor.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA cg_raw,cg_core TO "{READONLY_ROLE}"')
            productive_dsn = (
                f"postgresql://{PLANNED_PRODUCTIVE_ROLE}:synthetic-productive@"
                f"{plan['target']['expected_hostname']}/{database}?sslmode=require"
            )
            verification = verify_productive_role(
                plan,
                productive_dsn,
                authority=authority,
                connect_fn=lambda _dsn: psycopg.connect(DSN, user=PLANNED_PRODUCTIVE_ROLE),
            )
            self.assertEqual(verification["verdict"], "PASS_PRODUCTIVE_ROLE_VERIFICATION")
            self.assertEqual(verification["transaction_outcome"], "ROLLED_BACK")
            self.assertEqual(verification["persistent_rows_written"], 0)
            self.assertTrue(all(item["sqlstate"] == "42501" for item in verification["negative_probes"]))
            verification_path = evidence_root / "verification.json"
            self._write(verification_path, verification)

            postcheck = run_precheck(
                plan,
                "synthetic-readonly-dsn",
                readonly_connect,
                check_stage="post-provision",
                baseline=baseline,
                baseline_sha256=baseline_sha,
                authority=authority,
            )
            self.assertEqual(postcheck["verdict"], "PASS_READONLY_POSTCHECK")
            self.assertEqual(len(postcheck["route_b_signatures"]), 5)
            self.assertEqual(postcheck["active_batch_count"], 0)
            postcheck_path = evidence_root / "postcheck.json"
            self._write(postcheck_path, postcheck)

            drifted_legacy = copy.deepcopy(baseline)
            drifted_legacy["legacy"]["row_count"] = 2
            with self.assertRaisesRegex(PrecheckBlock, "legacy_evidence_drift"):
                run_precheck(
                    plan,
                    "synthetic-readonly-dsn",
                    readonly_connect,
                    check_stage="post-provision",
                    baseline=drifted_legacy,
                    baseline_sha256=baseline_sha,
                    authority=authority,
                )
            drifted_public = copy.deepcopy(baseline)
            drifted_public["public_acl"]["schemas"]["cg_raw"] = ["CREATE"]
            with self.assertRaisesRegex(PrecheckBlock, "public_acl_drift"):
                run_precheck(
                    plan,
                    "synthetic-readonly-dsn",
                    readonly_connect,
                    check_stage="post-provision",
                    baseline=drifted_public,
                    baseline_sha256=baseline_sha,
                    authority=authority,
                )

            bundle = build_bundle({
                "readonly_baseline_precheck": baseline_path,
                "admin_provisioning": admin_path,
                "productive_role_verification": verification_path,
                "readonly_postcheck": postcheck_path,
            }, PLAN_PATH, authority["approved_git_sha"])
            self.assertEqual(bundle["status"], "PASSED")
            self.assertEqual(set(bundle["components"]), {
                "readonly_baseline_precheck",
                "admin_provisioning",
                "productive_role_verification",
                "readonly_postcheck",
            })

        with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM cg_raw.kpione2_raw")
            self.assertEqual(cursor.fetchone()[0], 1)
            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_ingest_batch_v1")
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_ingest_batch_file_v1")
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_event_photo_staging_v1")
            self.assertEqual(cursor.fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
