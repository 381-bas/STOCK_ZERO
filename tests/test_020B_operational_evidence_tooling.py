from __future__ import annotations

import copy
import hashlib
import inspect
import json
import os
import shutil
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

from scripts import precheck_kpione_route_b_018_read_only as precheck
from scripts.build_kpione_route_b_infrastructure_evidence import (
    COMPONENT_CONTRACT,
    EvidenceBundleError,
    build_bundle,
    validate_existing_bundle,
)
from scripts.kpione_route_b_evidence_v1 import (
    EVIDENCE_FILENAMES,
    EvidenceContractError,
    atomic_write_json,
    canonical_evidence_path,
    prepare_run_directory,
    require_canonical_evidence_path,
    validate_run_id,
)
from scripts.provision_kpione_route_b_role import (
    ProvisioningError,
    _validate_committed_provisioning_source_mapping,
    _validate_committed_provisioning_source_report,
    _validate_prior_failure_mapping,
    reconcile_existing_provisioned_state,
    reconcile_provisioning_evidence,
    provision_route_b_role,
)
from scripts.verify_kpione_route_b_productive_role import verify_productive_role


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "plans" / "018_kpione_route_b_productive_apply_plan.json"
WRAPPER = ROOT / "scripts" / "invoke_stock_zero_db_operation.ps1"
VAULT = ROOT / "scripts" / "manage_stock_zero_secret_vault.ps1"
BUILDER = ROOT / "scripts" / "build_kpione_route_b_infrastructure_evidence.py"
RUN_ID = str(uuid.uuid4())


class ExistingRoleCursor:
    def __init__(self) -> None:
        self.rows: list[tuple[object, ...]] = []
        self.commands: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, _params=None) -> None:
        rendered = str(statement)
        normalized = " ".join(rendered.split()).lower()
        self.commands.append(normalized)
        if "select current_user,session_user,current_database" in normalized:
            self.rows = [("postgres", "postgres", "postgres", "off")]
        elif "to_regclass('cg_raw.kpione2_raw')" in normalized:
            self.rows = [("cg_raw.kpione2_raw",)]
        elif "from pg_class c join pg_namespace n" in normalized and "kpione2_raw" in normalized:
            self.rows = []
        elif "from pg_namespace n" in normalized or "information_schema.table_privileges" in normalized:
            self.rows = []
        elif "from pg_roles where rolname" in normalized:
            self.rows = [(1,)]
        else:
            self.rows = [(None,)]

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class ExistingRoleConnection:
    def __init__(self) -> None:
        self.autocommit = True
        self.cursor_value = ExistingRoleCursor()
        self.rollback_called = False
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def rollback(self) -> None:
        self.rollback_called = True

    def close(self) -> None:
        self.closed = True


class OperationalEvidenceTooling020BTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))

    def test_baseline_accepts_empty_productive_allowlist_and_rejects_open_gate(self) -> None:
        self.assertEqual(self.plan["target"]["allowed_productive_roles"], [])
        precheck.validate_plan_readiness(self.plan, "baseline")
        altered = copy.deepcopy(self.plan)
        altered["activation_gate"]["gate_open"] = True
        with self.assertRaisesRegex(precheck.PrecheckBlock, "productive_gate_must_be_closed"):
            precheck.validate_plan_readiness(altered, "baseline")

    def test_baseline_target_requires_exact_readonly_role_database_and_ssl(self) -> None:
        ref = self.plan["target"]["expected_supabase_project_ref"]
        host = self.plan["target"]["expected_hostname"]
        valid = f"postgresql://stock_zero_codex_ro:synthetic@{host}/postgres?sslmode=require"
        self.assertEqual(precheck.validate_target(valid, "DB_URL_CODEX_RO", self.plan, ref), host)
        for dsn, error in (
            (valid.replace("stock_zero_codex_ro", "wrong_role"), "readonly_dsn_role_mismatch"),
            (valid.replace("/postgres?", "/wrong?"), "target_database_mismatch"),
            (valid.replace("sslmode=require", "sslmode=disable"), "readonly_sslmode_require_required"),
        ):
            with self.subTest(error=error), self.assertRaisesRegex(precheck.PrecheckBlock, error):
                precheck.validate_target(dsn, "DB_URL_CODEX_RO", self.plan, ref)

    def test_postcheck_requires_baseline_before_connection(self) -> None:
        def forbidden_connect(_dsn: str):
            raise AssertionError("postcheck connected before requiring baseline")

        with self.assertRaisesRegex(precheck.PrecheckBlock, "baseline_evidence_required"):
            precheck.run_precheck(
                self.plan,
                "synthetic",
                forbidden_connect,
                check_stage="post-provision",
                run_id=str(uuid.uuid4()),
            )

    def _bundle_fixture(self, root: Path) -> tuple[dict[str, Path], str]:
        run_directory = prepare_run_directory(root, RUN_ID)
        git_sha = "a" * 40
        plan_sha = hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest()
        sql_sha = self.plan["physical_contract"]["sql_sha256"]
        fingerprint = precheck.target_fingerprint(self.plan)
        common = {
            "run_id": RUN_ID,
            "target_fingerprint": fingerprint,
            "approved_git_sha": git_sha,
            "plan_sha256": plan_sha,
            "sql_sha256": sql_sha,
        }
        baseline = {
            "document_type": COMPONENT_CONTRACT["readonly_baseline_precheck"][0],
            "verdict": COMPONENT_CONTRACT["readonly_baseline_precheck"][1],
            "evidence_sequence_step": 1,
            "timestamp_utc": "2026-07-14T10:00:00+00:00",
            **common,
            "legacy": {
                "object_identity": "cg_raw.kpione2_raw", "oid": "19",
                "relation_kind": "r", "owner": "postgres", "acl": "",
                "column_signature_sha256": "c" * 64,
                "row_count": 1, "relation_size_bytes": 8192,
            },
            "public_acl": {"schemas": {"cg_raw": ["USAGE"]}, "relations": {}},
        }
        baseline_path = run_directory / EVIDENCE_FILENAMES["readonly_baseline_precheck"]
        baseline_path.write_text(json.dumps(baseline, sort_keys=True) + "\n", encoding="utf-8")
        components: dict[str, dict[str, object]] = {
            "admin_provisioning": {
                "document_type": COMPONENT_CONTRACT["admin_provisioning"][0],
                "verdict": COMPONENT_CONTRACT["admin_provisioning"][1],
                "evidence_sequence_step": 2,
                "timestamp_utc": "2026-07-14T10:01:00+00:00",
                "evidence_mode": "DIRECT_COMMITTED_EXECUTION",
                **common,
            },
            "productive_role_verification": {
                "document_type": COMPONENT_CONTRACT["productive_role_verification"][0],
                "verdict": COMPONENT_CONTRACT["productive_role_verification"][1],
                "evidence_sequence_step": 3,
                "timestamp_utc": "2026-07-14T10:02:00+00:00",
                **common,
            },
            "readonly_postcheck": {
                "document_type": COMPONENT_CONTRACT["readonly_postcheck"][0],
                "verdict": COMPONENT_CONTRACT["readonly_postcheck"][1],
                "evidence_sequence_step": 4,
                "timestamp_utc": "2026-07-14T10:03:00+00:00",
                **common,
                "baseline_evidence_sha256": hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
                "legacy": {**baseline["legacy"], "row_count": 2, "relation_size_bytes": 16384},
                "public_acl": baseline["public_acl"],
            },
        }
        paths = {"readonly_baseline_precheck": baseline_path}
        for name, evidence in components.items():
            path = run_directory / EVIDENCE_FILENAMES[name]
            path.write_text(json.dumps(evidence, sort_keys=True) + "\n", encoding="utf-8")
            paths[name] = path
        return paths, git_sha

    def _stored_bundle_fixture(
        self, root: Path,
    ) -> tuple[dict[str, Path], str, Path, dict[str, object]]:
        paths, git_sha = self._bundle_fixture(root)
        bundle = build_bundle(paths, PLAN_PATH, git_sha, RUN_ID, root=root)
        bundle_path = canonical_evidence_path(root, RUN_ID, "infrastructure_bundle")
        bundle_path.write_text(json.dumps(bundle, sort_keys=True) + "\n", encoding="utf-8")
        return paths, git_sha, bundle_path, bundle

    def test_bundle_is_deterministic_and_has_exact_four_components(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            paths, git_sha = self._bundle_fixture(Path(folder))
            first = build_bundle(paths, PLAN_PATH, git_sha, RUN_ID, root=Path(folder))
            second = build_bundle(paths, PLAN_PATH, git_sha, RUN_ID, root=Path(folder))
        self.assertEqual(first, second)
        self.assertEqual(first["document_type"], "kpione_route_b_infrastructure_evidence_bundle_v1")
        self.assertEqual(first["status"], "PASSED")
        self.assertEqual(first["run_id"], RUN_ID)
        self.assertEqual(set(first["components"]), set(COMPONENT_CONTRACT))
        self.assertRegex(first["bundle_sha256"], r"^[0-9a-f]{64}$")

    def test_bundle_rejects_missing_extra_tampered_target_baseline_and_secret(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            paths, git_sha = self._bundle_fixture(root)
            for changed, error in (
                ({key: value for key, value in paths.items() if key != "admin_provisioning"}, "component_name_set_mismatch"),
                ({**paths, "extra": root / "extra.json"}, "component_name_set_mismatch"),
            ):
                with self.subTest(error=error), self.assertRaisesRegex(EvidenceBundleError, error):
                    build_bundle(changed, PLAN_PATH, git_sha, RUN_ID, root=root)

            post = json.loads(paths["readonly_postcheck"].read_text(encoding="utf-8"))
            post["baseline_evidence_sha256"] = "0" * 64
            paths["readonly_postcheck"].write_text(json.dumps(post) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(EvidenceBundleError, "postcheck_baseline_reference_mismatch"):
                build_bundle(paths, PLAN_PATH, git_sha, RUN_ID, root=root)

            paths, git_sha = self._bundle_fixture(root)
            admin = json.loads(paths["admin_provisioning"].read_text(encoding="utf-8"))
            admin["target_fingerprint"] = "c" * 64
            paths["admin_provisioning"].write_text(json.dumps(admin) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(EvidenceBundleError, "target_fingerprint_mismatch"):
                build_bundle(paths, PLAN_PATH, git_sha, RUN_ID, root=root)

            paths, git_sha = self._bundle_fixture(root)
            verify = json.loads(paths["productive_role_verification"].read_text(encoding="utf-8"))
            verify["diagnostic"] = "postgresql://synthetic.invalid/postgres"
            paths["productive_role_verification"].write_text(json.dumps(verify) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(EvidenceBundleError, "suspicious_evidence_value"):
                build_bundle(paths, PLAN_PATH, git_sha, RUN_ID, root=root)

    def test_existing_bundle_validation_passes_exactly_and_modifies_no_file(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            paths, git_sha, bundle_path, bundle = self._stored_bundle_fixture(root)
            all_paths = [*paths.values(), bundle_path]
            before = {path: path.read_bytes() for path in all_paths}
            report = validate_existing_bundle(
                PLAN_PATH, git_sha, RUN_ID, bundle_path, root=root,
            )
            after = {path: path.read_bytes() for path in all_paths}
        self.assertEqual(report, {
            "verdict": "PASS_EXISTING_INFRASTRUCTURE_BUNDLE_VALIDATION",
            "run_id": RUN_ID,
            "bundle_sha256": bundle["bundle_sha256"],
        })
        self.assertEqual(before, after)

    def test_existing_bundle_validation_rejects_bundle_hash_and_semantic_drift(self) -> None:
        def update_component_hash(
            bundle_path: Path, component: str, component_path: Path,
        ) -> None:
            stored = json.loads(bundle_path.read_text(encoding="utf-8"))
            stored["components"][component] = hashlib.sha256(component_path.read_bytes()).hexdigest()
            bundle_path.write_text(json.dumps(stored, sort_keys=True) + "\n", encoding="utf-8")

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            paths, git_sha, bundle_path, _bundle = self._stored_bundle_fixture(root)
            altered = json.loads(bundle_path.read_text(encoding="utf-8"))
            altered["bundle_sha256"] = "0" * 64
            bundle_path.write_text(json.dumps(altered, sort_keys=True) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(EvidenceBundleError, "existing_bundle_exact_mismatch"):
                validate_existing_bundle(PLAN_PATH, git_sha, RUN_ID, bundle_path, root=root)

            paths, git_sha, bundle_path, _bundle = self._stored_bundle_fixture(root)
            stored = json.loads(bundle_path.read_text(encoding="utf-8"))
            for component, component_path in paths.items():
                item = json.loads(component_path.read_text(encoding="utf-8"))
                item["target_fingerprint"] = "d" * 64
                component_path.write_text(json.dumps(item, sort_keys=True) + "\n", encoding="utf-8")
                stored["components"][component] = hashlib.sha256(
                    component_path.read_bytes()
                ).hexdigest()
            bundle_path.write_text(json.dumps(stored, sort_keys=True) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(
                EvidenceBundleError, "target_fingerprint_not_registered_plan_target",
            ):
                validate_existing_bundle(PLAN_PATH, git_sha, RUN_ID, bundle_path, root=root)

            cases = (
                ("admin_provisioning", lambda item: item.update({"approved_git_sha": "d" * 40}), "approved_git_sha_mismatch"),
                ("admin_provisioning", lambda item: item.update({"plan_sha256": "d" * 64}), "plan_sha256_mismatch"),
                ("admin_provisioning", lambda item: item.update({"sql_sha256": "d" * 64}), "sql_sha256_mismatch"),
                ("productive_role_verification", lambda item: item.update({"timestamp_utc": "2026-07-14T09:00:00+00:00"}), "evidence_timestamps_not_nondecreasing"),
                ("readonly_postcheck", lambda item: item.update({"baseline_evidence_sha256": "d" * 64}), "postcheck_baseline_reference_mismatch"),
                ("readonly_postcheck", lambda item: item["legacy"].update({"oid": "20"}), "baseline_postcheck_legacy_structure_mismatch"),
                ("readonly_postcheck", lambda item: item["public_acl"]["schemas"].update({"cg_raw": ["CREATE"]}), "baseline_postcheck_public_acl_mismatch"),
                ("admin_provisioning", lambda item: item.update({"evidence_mode": "UNAPPROVED"}), "admin_evidence_mode_invalid"),
                ("productive_role_verification", lambda item: item.update({"diagnostic": "postgresql://synthetic.invalid/postgres"}), "suspicious_evidence_value"),
            )
            for component, mutate, error in cases:
                with self.subTest(error=error):
                    paths, git_sha, bundle_path, _bundle = self._stored_bundle_fixture(root)
                    component_path = paths[component]
                    item = json.loads(component_path.read_text(encoding="utf-8"))
                    mutate(item)
                    component_path.write_text(json.dumps(item, sort_keys=True) + "\n", encoding="utf-8")
                    update_component_hash(bundle_path, component, component_path)
                    with self.assertRaisesRegex(EvidenceBundleError, error):
                        validate_existing_bundle(
                            PLAN_PATH, git_sha, RUN_ID, bundle_path, root=root,
                        )

    def test_run_identity_rejects_invalid_mismatch_sequence_and_time_regression(self) -> None:
        real_run_id = str(uuid.uuid4())
        self.assertEqual(validate_run_id(real_run_id), real_run_id)
        for invalid in (
            real_run_id.upper(), str(uuid.uuid1()),
            str(uuid.UUID(int=0, version=4)), "not-a-uuid", "", None,
        ):
            with self.subTest(invalid=invalid), self.assertRaises(EvidenceContractError):
                validate_run_id(invalid)

        for producer in (precheck.run_precheck, provision_route_b_role, verify_productive_role):
            parameter = inspect.signature(producer).parameters["run_id"]
            self.assertIs(parameter.default, inspect.Parameter.empty)
        with self.assertRaises(TypeError):
            precheck.run_precheck(self.plan, "synthetic", lambda _dsn: None)

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            paths, git_sha = self._bundle_fixture(root)
            for component, field, value, error in (
                ("admin_provisioning", "run_id", str(uuid.uuid4()), "component_run_id_mismatch"),
                ("admin_provisioning", "evidence_sequence_step", 1, "component_sequence_step_mismatch"),
                ("productive_role_verification", "evidence_sequence_step", 2, "component_sequence_step_mismatch"),
                ("productive_role_verification", "timestamp_utc", "2026-07-14T09:59:00+00:00", "evidence_timestamps_not_nondecreasing"),
            ):
                paths, git_sha = self._bundle_fixture(root)
                payload = json.loads(paths[component].read_text(encoding="utf-8"))
                payload[field] = value
                paths[component].write_text(json.dumps(payload) + "\n", encoding="utf-8")
                with self.subTest(component=component, field=field), self.assertRaisesRegex(
                    EvidenceBundleError, error,
                ):
                    build_bundle(paths, PLAN_PATH, git_sha, RUN_ID, root=root)

    def test_canonical_paths_atomic_non_overwrite_and_git_ignore(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            directory = prepare_run_directory(root, RUN_ID)
            expected = canonical_evidence_path(root, RUN_ID, "readonly_baseline_precheck")
            self.assertEqual(
                require_canonical_evidence_path(expected, root, RUN_ID, "readonly_baseline_precheck"),
                expected,
            )
            with self.assertRaisesRegex(EvidenceContractError, "evidence_path_not_canonical"):
                require_canonical_evidence_path(
                    directory / "wrong.json", root, RUN_ID, "readonly_baseline_precheck",
                )
            atomic_write_json(expected, {"value": "first"})
            original = expected.read_bytes()
            with self.assertRaisesRegex(EvidenceContractError, "evidence_file_already_exists"):
                atomic_write_json(expected, {"value": "second"})
            self.assertEqual(expected.read_bytes(), original)
            self.assertEqual(list(directory.glob(".*.tmp")), [])

            failed = canonical_evidence_path(root, RUN_ID, "admin_provisioning")
            with mock.patch("scripts.kpione_route_b_evidence_v1.os.replace", side_effect=OSError("synthetic")):
                with self.assertRaises(OSError):
                    atomic_write_json(failed, {"value": "not-committed"})
            self.assertFalse(failed.exists())
            self.assertEqual(list(directory.glob(".*.tmp")), [])

        ignored = subprocess.run(
            ["git", "check-ignore", "evidence/runtime/020B/" + RUN_ID + "/01_readonly_baseline.json"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        self.assertEqual(ignored.returncode, 0, ignored.stderr)

    def test_legacy_structure_blocks_drift_but_activity_deltas_are_informative(self) -> None:
        baseline = {
            "object_identity": "cg_raw.kpione2_raw", "oid": "19",
            "relation_kind": "r", "owner": "postgres", "acl": "",
            "column_signature_sha256": "a" * 64,
            "row_count": 1, "relation_size_bytes": 8192,
        }
        current = {**baseline, "row_count": 3, "relation_size_bytes": 24576}
        precheck.assert_legacy_structural_invariance(baseline, current)
        self.assertEqual(precheck.legacy_activity_delta(baseline, current), {
            "row_count": {"before": 1, "after": 3, "delta": 2},
            "relation_size_bytes": {"before": 8192, "after": 24576, "delta": 16384},
        })
        for field, value in (
            ("oid", "20"), ("relation_kind", "v"),
            ("column_signature_sha256", "b" * 64),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(
                precheck.PrecheckBlock, f"legacy_structural_drift:{field}",
            ):
                precheck.assert_legacy_structural_invariance(baseline, {**current, field: value})

    def test_reconciliation_prior_authority_is_fail_closed_and_source_has_no_mutations(self) -> None:
        guard = {
            "approved_git_sha": "a" * 40,
            "plan_sha256": hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest(),
        }
        prior = {
            "verdict": "BLOCKED",
            "error": "admin_provisioning_evidence_write_failed",
            "run_id": RUN_ID,
            "evidence_sequence_step": 2,
            "committed": True,
            "rollback_or_reconciliation_required": True,
            "approved_git_sha": guard["approved_git_sha"],
            "plan_sha256": guard["plan_sha256"],
            "sql_sha256": self.plan["physical_contract"]["sql_sha256"],
            "target_fingerprint": precheck.target_fingerprint(self.plan),
            "legacy_structure_before": {"object_identity": "cg_raw.kpione2_raw"},
            "public_acl_before": {"schemas": {}, "relations": {}},
        }
        self.assertEqual(
            _validate_prior_failure_mapping(prior, RUN_ID, guard, self.plan), prior,
        )
        for field, value in (
            ("committed", False), ("approved_git_sha", "b" * 40),
            ("plan_sha256", "b" * 64), ("sql_sha256", "b" * 64),
            ("target_fingerprint", "b" * 64),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(
                ProvisioningError, f"prior_failure_report_mismatch:{field}",
            ):
                _validate_prior_failure_mapping({**prior, field: value}, RUN_ID, guard, self.plan)

        source = inspect.getsource(reconcile_provisioning_evidence).upper()
        self.assertIn("BEGIN READ ONLY", source)
        for mutation in ("CREATE ROLE", "ALTER ROLE", "GRANT ", "REVOKE "):
            self.assertNotIn(mutation, source)

    def test_existing_provisioned_state_source_requires_committed_pass_evidence(self) -> None:
        guard = {
            "approved_git_sha": "c" * 40,
            "plan_sha256": hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest(),
        }
        source = {
            "document_type": "kpione_route_b_role_provisioning_evidence_v1",
            "run_id": RUN_ID,
            "evidence_sequence_step": 2,
            "verdict": "PASS_ADMIN_PROVISIONING",
            "evidence_mode": "DIRECT_COMMITTED_EXECUTION",
            "approved_git_sha": "b" * 40,
            "plan_sha256": guard["plan_sha256"],
            "sql_sha256": self.plan["physical_contract"]["sql_sha256"],
            "target_fingerprint": precheck.target_fingerprint(self.plan),
            "committed": True,
            "role_created": True,
            "rollback_or_reconciliation_required": False,
            "role_attributes": {
                "login": True,
                "superuser": False,
                "createdb": False,
                "createrole": False,
                "replication": False,
                "bypassrls": False,
                "inherit": False,
                "connection_limit": 5,
            },
            "route_b_objects_validated": sorted(self.plan["physical_contract"]["objects"]),
            "route_b_sequence": "cg_raw.kpione_raw_event_photo_staging_v1_staging_id_seq",
            "legacy_structure_after": {"object_identity": "cg_raw.kpione2_raw"},
            "public_acl_after": {"schemas": {}, "relations": {}},
        }
        self.assertEqual(
            _validate_committed_provisioning_source_mapping(source, guard, self.plan),
            source,
        )
        for field, value in (
            ("verdict", "BLOCKED"),
            ("committed", False),
            ("role_created", False),
            ("rollback_or_reconciliation_required", True),
            ("plan_sha256", "b" * 64),
            ("sql_sha256", "b" * 64),
            ("target_fingerprint", "b" * 64),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(
                ProvisioningError, f"source_admin_provisioning_mismatch:{field}",
            ):
                _validate_committed_provisioning_source_mapping(
                    {**source, field: value}, guard, self.plan,
                )

        reconciler_source = inspect.getsource(reconcile_existing_provisioned_state).upper()
        self.assertIn("BEGIN READ ONLY", reconciler_source)
        self.assertIn("ROLLBACK", reconciler_source)
        for mutation in ("CREATE ROLE", "ALTER ROLE", "GRANT ", "REVOKE "):
            self.assertNotIn(mutation, reconciler_source)

    def test_existing_provisioned_state_source_path_is_repo_root_relative_and_canonical(self) -> None:
        guard = {
            "approved_git_sha": "c" * 40,
            "plan_sha256": hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest(),
        }
        source = {
            "document_type": "kpione_route_b_role_provisioning_evidence_v1",
            "run_id": RUN_ID,
            "evidence_sequence_step": 2,
            "verdict": "PASS_ADMIN_PROVISIONING",
            "evidence_mode": "DIRECT_COMMITTED_EXECUTION",
            "approved_git_sha": "b" * 40,
            "plan_sha256": guard["plan_sha256"],
            "sql_sha256": self.plan["physical_contract"]["sql_sha256"],
            "target_fingerprint": precheck.target_fingerprint(self.plan),
            "committed": True,
            "role_created": True,
            "rollback_or_reconciliation_required": False,
            "role_attributes": {
                "login": True,
                "superuser": False,
                "createdb": False,
                "createrole": False,
                "replication": False,
                "bypassrls": False,
                "inherit": False,
                "connection_limit": 5,
            },
            "route_b_objects_validated": sorted(self.plan["physical_contract"]["objects"]),
            "route_b_sequence": "cg_raw.kpione_raw_event_photo_staging_v1_staging_id_seq",
            "legacy_structure_after": {"object_identity": "cg_raw.kpione2_raw"},
            "public_acl_after": {"schemas": {}, "relations": {}},
        }
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source_run = str(uuid.uuid4())
            target_run = str(uuid.uuid4())
            source_dir = root / "evidence" / "runtime" / "020B" / source_run
            source_dir.mkdir(parents=True)
            source_path = source_dir / "02_admin_provisioning.json"
            source_path.write_text(json.dumps({**source, "run_id": source_run}) + "\n", encoding="utf-8")

            relative = Path("evidence") / "runtime" / "020B" / source_run / "02_admin_provisioning.json"
            relative_report, relative_sha = _validate_committed_provisioning_source_report(
                relative, guard, self.plan, root,
            )
            absolute_report, absolute_sha = _validate_committed_provisioning_source_report(
                source_path.resolve(), guard, self.plan, root,
            )
            self.assertEqual(relative_report["run_id"], source_run)
            self.assertEqual(absolute_report["run_id"], source_run)
            self.assertEqual(relative_sha, absolute_sha)

            same_run_dir = root / "evidence" / "runtime" / "020B" / target_run
            same_run_dir.mkdir(parents=True)
            same_run_path = same_run_dir / "02_admin_provisioning.json"
            same_run_path.write_text(json.dumps({**source, "run_id": target_run}) + "\n", encoding="utf-8")
            same_report, _same_sha = _validate_committed_provisioning_source_report(
                same_run_path, guard, self.plan, root,
            )
            self.assertEqual(same_report["run_id"], target_run)

            with self.assertRaisesRegex(ProvisioningError, "source_admin_provisioning_path_invalid"):
                _validate_committed_provisioning_source_report(
                    Path("evidence/runtime/020B") / str(uuid.uuid4()) / "02_admin_provisioning.json",
                    guard,
                    self.plan,
                    root,
                )
            wrong_name = source_dir / "not_02_admin_provisioning.json"
            wrong_name.write_text(json.dumps(source) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ProvisioningError, "source_admin_provisioning_filename_invalid"):
                _validate_committed_provisioning_source_report(wrong_name, guard, self.plan, root)
            outside = root / "outside.json"
            outside.write_text(json.dumps(source) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ProvisioningError, "source_admin_provisioning_path_invalid"):
                _validate_committed_provisioning_source_report(outside, guard, self.plan, root)
            invalid_json = source_dir / "02_admin_provisioning.json"
            invalid_json.write_text("{not-json", encoding="utf-8")
            with self.assertRaisesRegex(ProvisioningError, "source_admin_provisioning_unreadable"):
                _validate_committed_provisioning_source_report(invalid_json, guard, self.plan, root)

    def test_existing_role_blocks_before_password_rotation_or_other_write(self) -> None:
        connection = ExistingRoleConnection()
        dsn = (
            "postgresql://postgres:synthetic@"
            f"{self.plan['target']['expected_hostname']}/postgres?sslmode=require"
        )
        guard = {
            "approved_git_sha": "a" * 40,
            "plan_sha256": "b" * 64,
            "ddl_sha256": self.plan["physical_contract"]["sql_sha256"],
        }
        with self.assertRaisesRegex(
            ProvisioningError, "productive_role_exists_password_rotation_not_authorized"
        ) as context:
            provision_route_b_role(
                self.plan,
                dsn,
                "synthetic-password-long-enough",
                Path(tempfile.gettempdir()) / "never-written.json",
                connect_fn=lambda _dsn: connection,
                git_guard=guard,
                ddl="SELECT 1",
                run_id=str(uuid.uuid4()),
            )
        self.assertFalse(context.exception.writes_attempted)
        self.assertTrue(connection.rollback_called)
        self.assertTrue(connection.closed)
        rendered = "\n".join(connection.cursor_value.commands).upper()
        self.assertNotIn("ALTER ROLE", rendered)
        self.assertNotIn("CREATE ROLE", rendered)

    def test_wrapper_and_vault_have_only_typed_operations_and_safe_storage(self) -> None:
        wrapper = WRAPPER.read_text(encoding="utf-8")
        for operation in (
            "readonly-postcheck", "verify-route-b-role",
            "admin-reconcile-provisioning-evidence",
            "admin-reconcile-existing-provisioned-state",
        ):
            self.assertIn(f"'{operation}'", wrapper)
        self.assertIn("@('--check-stage', 'post-provision')", wrapper)
        self.assertIn("scripts/verify_kpione_route_b_productive_role.py", wrapper)
        self.assertNotIn("[string]$FilePath", wrapper)

        vault = VAULT.read_text(encoding="utf-8")
        for operation in (
            "bootstrap", "inventory", "set-readonly", "set-admin-temporary",
            "generate-role-password-temporary", "build-and-store-route-b-dsn",
            "remove-temporary",
        ):
            self.assertIn(f"'{operation}'", vault)
        self.assertIn("'1.1.2'", vault)
        self.assertIn("'1.0.6'", vault)
        self.assertIn("-Authentication Password", vault)
        self.assertNotIn("-Authentication None", vault)
        self.assertIn("-AsSecureString", vault)
        self.assertIn("[Security.Cryptography.RandomNumberGenerator]::Fill", vault)
        self.assertNotIn("[EnvironmentVariableTarget]::User", vault)
        self.assertNotIn("[EnvironmentVariableTarget]::Machine", vault)
        self.assertIn("[string]$EvidenceDirectory", vault)
        self.assertIn("[string]$RunId", vault)
        self.assertIn("[string]$ExpectedGitSha", vault)
        self.assertIn("--validate-existing", vault)
        self.assertNotIn("ProductiveRoleVerificationEvidence", vault)
        self.assertNotIn("ReadonlyPostcheckEvidence", vault)

    @unittest.skipUnless(shutil.which("pwsh"), "PowerShell 7 is required for the vault runtime probe")
    def test_vault_runtime_uses_secure_types_and_evidence_gated_removal(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            scripts = root / "scripts"
            scripts.mkdir()
            vault_copy = scripts / VAULT.name
            vault_copy.write_bytes(VAULT.read_bytes())
            for source in (
                BUILDER,
                ROOT / "scripts" / "kpione_route_b_evidence_v1.py",
                ROOT / "scripts" / "precheck_kpione_route_b_018_read_only.py",
                ROOT / "scripts" / "kpione_route_b_v1.py",
            ):
                (scripts / source.name).write_bytes(source.read_bytes())
            plans = root / "plans"
            plans.mkdir()
            (plans / PLAN_PATH.name).write_bytes(PLAN_PATH.read_bytes())
            log = root / "mock-log.jsonl"
            modules = root / "modules"
            management = modules / "Microsoft.PowerShell.SecretManagement" / "1.1.2"
            store = modules / "Microsoft.PowerShell.SecretStore" / "1.0.6"
            management.mkdir(parents=True)
            store.mkdir(parents=True)
            management.joinpath("Microsoft.PowerShell.SecretManagement.psd1").write_text(
                "@{RootModule='Microsoft.PowerShell.SecretManagement.psm1';ModuleVersion='1.1.2';"
                "GUID='11111111-1111-1111-1111-111111111112';FunctionsToExport='*'}\n",
                encoding="utf-8",
            )
            management.joinpath("Microsoft.PowerShell.SecretManagement.psm1").write_text(
                "function Add-MockEvent { param($Value) Add-Content -LiteralPath $env:SZ_MOCK_LOG "
                "-Value ($Value | ConvertTo-Json -Compress) }\n"
                "function Get-SecretVault { [CmdletBinding()]param([string]$Name) "
                "[pscustomobject]@{Name='STOCK_ZERO'} }\n"
                "function Register-SecretVault { [CmdletBinding()]param([string]$Name,[string]$ModuleName,[switch]$DefaultVault) "
                "Add-MockEvent @{action='register';name=$Name;module=$ModuleName} }\n"
                "function Get-SecretInfo { [CmdletBinding()]param([string]$Vault,[string]$Name) "
                "if($Name -eq 'STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE' -and $env:SZ_MOCK_PRODUCTIVE_PRESENT -ne '1'){return $null};"
                "[pscustomobject]@{Name=$Name;Type='SecureString'} }\n"
                "function Get-Secret { [CmdletBinding()]param([string]$Vault,[string]$Name) "
                "ConvertTo-SecureString 'synthetic-vault-secret' -AsPlainText -Force }\n"
                "function Set-Secret { [CmdletBinding()]param([string]$Vault,[string]$Name,$Secret) "
                "$event=@{action='set';name=$Name;type=$Secret.GetType().FullName}; "
                "if($Name -eq 'STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE'){"
                "$p=[Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secret);try{$v=[Runtime.InteropServices.Marshal]::PtrToStringBSTR($p);"
                "$event.role=$v.StartsWith('postgresql://stock_zero_kpione_route_b_load:');"
                "$event.host=$v.Contains('@db.xheyrgfagpoigpgakilu.supabase.co/');"
                "$event.database=$v.Contains('/postgres?');$event.ssl=$v.EndsWith('sslmode=require')}finally{[Runtime.InteropServices.Marshal]::ZeroFreeBSTR($p)}};"
                "Add-MockEvent $event }\n"
                "function Remove-Secret { [CmdletBinding()]param([string]$Vault,[string]$Name) "
                "Add-MockEvent @{action='remove';name=$Name} }\n"
                "Export-ModuleMember -Function *\n",
                encoding="utf-8",
            )
            store.joinpath("Microsoft.PowerShell.SecretStore.psd1").write_text(
                "@{RootModule='Microsoft.PowerShell.SecretStore.psm1';ModuleVersion='1.0.6';"
                "GUID='11111111-1111-1111-1111-111111111106';FunctionsToExport='*'}\n",
                encoding="utf-8",
            )
            store.joinpath("Microsoft.PowerShell.SecretStore.psm1").write_text(
                "function Set-SecretStoreConfiguration { [CmdletBinding()]param([string]$Authentication,"
                "[string]$Interaction,[int]$PasswordTimeout,[switch]$Confirm) "
                "Add-Content -LiteralPath $env:SZ_MOCK_LOG -Value (@{action='configuration';authentication=$Authentication;interaction=$Interaction}|ConvertTo-Json -Compress) }\n"
                "Export-ModuleMember -Function *\n",
                encoding="utf-8",
            )
            paths, git_sha = self._bundle_fixture(root)
            run_directory = paths["readonly_baseline_precheck"].parent
            bundle = build_bundle(paths, PLAN_PATH, git_sha, RUN_ID, root=root)
            bundle_path = run_directory / EVIDENCE_FILENAMES["infrastructure_bundle"]
            bundle_path.write_text(json.dumps(bundle, sort_keys=True) + "\n", encoding="utf-8")
            environment = os.environ.copy()
            environment["PSModulePath"] = str(modules) + os.pathsep + environment.get("PSModulePath", "")
            environment["SZ_MOCK_LOG"] = str(log)
            environment["SZ_MOCK_PRODUCTIVE_PRESENT"] = "1"

            def invoke(
                operation: str, *, prelude: str = "", arguments: list[str] | None = None,
                expected_success: bool = True, operation_environment: dict[str, str] | None = None,
            ):
                args = " ".join(
                    value if value.startswith("-") else f"'{value}'"
                    for value in (arguments or [])
                )
                command = f"{prelude}\n& '{vault_copy}' -Operation '{operation}' {args}"
                completed = subprocess.run(
                    ["pwsh", "-NoLogo", "-NoProfile", "-Command", command],
                    cwd=root,
                    env=operation_environment or environment,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if expected_success:
                    self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
                else:
                    self.assertNotEqual(completed.returncode, 0, completed.stdout + completed.stderr)
                self.assertNotIn("synthetic-vault-secret", completed.stdout)
                self.assertNotIn("synthetic-vault-secret", completed.stderr)
                return completed

            def removed_count() -> int:
                if not log.exists():
                    return 0
                return sum(
                    json.loads(line).get("action") == "remove"
                    for line in log.read_text(encoding="utf-8").splitlines()
                )

            def remove(arguments: list[str], *, env: dict[str, str] | None = None, success: bool) -> None:
                before = removed_count()
                invoke(
                    "remove-temporary", arguments=arguments,
                    expected_success=success, operation_environment=env,
                )
                self.assertEqual(removed_count() - before, 2 if success else 0)

            bootstrap_prelude = (
                "function global:Install-Module { param($Name,$RequiredVersion,$Scope,$Repository,"
                "[switch]$Force,[switch]$AllowClobber) Add-Content -LiteralPath $env:SZ_MOCK_LOG "
                "-Value (@{action='install';name=$Name;version=$RequiredVersion;scope=$Scope}|ConvertTo-Json -Compress) }"
            )
            invoke("bootstrap", prelude=bootstrap_prelude)
            read_prelude = (
                "function global:Read-Host { param($Prompt,[switch]$AsSecureString) "
                "ConvertTo-SecureString 'synthetic-vault-secret' -AsPlainText -Force }"
            )
            invoke("set-readonly", prelude=read_prelude)
            invoke("set-admin-temporary", prelude=read_prelude)
            invoke("generate-role-password-temporary")
            invoke("build-and-store-route-b-dsn")
            inventory = invoke("inventory")
            self.assertIn("STOCK_ZERO_DB_CODEX_RO", inventory.stdout)
            canonical_arguments = [
                "-EvidenceDirectory", str(run_directory), "-RunId", RUN_ID,
                "-ExpectedGitSha", git_sha,
            ]
            remove([], success=False)

            wrong_run = str(uuid.uuid4())
            remove([
                "-EvidenceDirectory", str(run_directory), "-RunId", wrong_run,
                "-ExpectedGitSha", git_sha,
            ], success=False)
            remove([
                "-EvidenceDirectory", str(run_directory), "-RunId", RUN_ID,
                "-ExpectedGitSha", "not-a-git-sha",
            ], success=False)

            original_verify = paths["productive_role_verification"].read_bytes()
            paths["productive_role_verification"].write_text(
                json.dumps({**json.loads(original_verify), "tampered": True}, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            remove(canonical_arguments, success=False)
            paths["productive_role_verification"].write_bytes(original_verify)

            missing_path = paths["readonly_postcheck"]
            missing_raw = missing_path.read_bytes()
            missing_path.unlink()
            remove(canonical_arguments, success=False)
            missing_path.write_bytes(missing_raw)

            original_bundle = bundle_path.read_bytes()
            altered_bundle = json.loads(original_bundle)
            altered_bundle["bundle_sha256"] = "0" * 64
            bundle_path.write_text(json.dumps(altered_bundle, sort_keys=True) + "\n", encoding="utf-8")
            remove(canonical_arguments, success=False)
            bundle_path.write_bytes(original_bundle)

            original_components = {name: path.read_bytes() for name, path in paths.items()}
            for name, path in paths.items():
                payload = json.loads(path.read_text(encoding="utf-8"))
                payload["target_fingerprint"] = "d" * 64
                path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
            post = json.loads(paths["readonly_postcheck"].read_text(encoding="utf-8"))
            post["baseline_evidence_sha256"] = hashlib.sha256(
                paths["readonly_baseline_precheck"].read_bytes()
            ).hexdigest()
            paths["readonly_postcheck"].write_text(
                json.dumps(post, sort_keys=True) + "\n", encoding="utf-8",
            )
            semantic_bundle = json.loads(original_bundle)
            semantic_bundle["status"] = "PASSED"
            semantic_bundle["components"] = {
                name: hashlib.sha256(path.read_bytes()).hexdigest()
                for name, path in paths.items()
            }
            bundle_path.write_text(
                json.dumps(semantic_bundle, sort_keys=True) + "\n", encoding="utf-8",
            )
            remove(canonical_arguments, success=False)
            for name, raw in original_components.items():
                paths[name].write_bytes(raw)
            bundle_path.write_bytes(original_bundle)

            validator_path = scripts / BUILDER.name
            validator_raw = validator_path.read_bytes()
            validator_path.write_text("raise SystemExit(9)\n", encoding="utf-8")
            remove(canonical_arguments, success=False)
            validator_path.write_bytes(validator_raw)

            no_dsn_environment = environment.copy()
            no_dsn_environment["SZ_MOCK_PRODUCTIVE_PRESENT"] = "0"
            remove(canonical_arguments, env=no_dsn_environment, success=False)
            remove(canonical_arguments, success=True)
            events = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
            installs = {event["name"]: event for event in events if event["action"] == "install"}
            self.assertEqual(installs["Microsoft.PowerShell.SecretManagement"]["version"], "1.1.2")
            self.assertEqual(installs["Microsoft.PowerShell.SecretStore"]["version"], "1.0.6")
            configuration = next(event for event in events if event["action"] == "configuration")
            self.assertEqual((configuration["authentication"], configuration["interaction"]), ("Password", "Prompt"))
            stored = [event for event in events if event["action"] == "set"]
            self.assertTrue(stored)
            self.assertTrue(all(event["type"] == "System.Security.SecureString" for event in stored))
            dsn_event = next(event for event in stored if event["name"] == "STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE")
            self.assertTrue(all(dsn_event[key] for key in ("role", "host", "database", "ssl")))
            removed = {event["name"] for event in events if event["action"] == "remove"}
            self.assertEqual(removed, {
                "STOCK_ZERO_DB_ADMIN", "STOCK_ZERO_DB_KPIONE_ROUTE_B_PASSWORD",
            })


if __name__ == "__main__":
    unittest.main()
