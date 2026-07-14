import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "contracts" / "control_gestion" / "kpione2_photo_export_contract_v1.json"
KERNEL_PATH = ROOT / "governance" / "kernel" / "current" / "01_project_kernel_stock_zero_v2026_06_16.json"
STATE_PATH = ROOT / "governance" / "kernel" / "current" / "02_project_state_stock_zero_v2026_06_30_011.json"
LEDGER_PATH = ROOT / "governance" / "kernel" / "current" / "03_project_ledger_stock_zero_v2026_06_30_011.json"
DIRECTIVE_PATH = ROOT / "governance" / "directives" / "KPIONE_DB_TRANSITION_016_019_LOCK_V1.json"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class Kpione016BIdentityIdempotencyContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract_payload = load_json(CONTRACT_PATH)
        self.contract = self.contract_payload["identity_idempotency_contract_016B"]
        self.kernel = load_json(KERNEL_PATH)
        self.state = load_json(STATE_PATH)
        self.ledger = load_json(LEDGER_PATH)
        self.directive = load_json(DIRECTIVE_PATH)

    def test_kernel_selects_route_b_as_future_authority_and_route_a_as_history(self):
        productive = self.kernel["source_contracts"]["productive_kpione2"]
        route_b = productive["future_productive_authority"]
        route_a = productive["historical_bootstrap"]

        self.assertEqual(productive["decision"], "PHOTO_EXPORT_SELECTED_AS_FUTURE_PRODUCTIVE_AUTHORITY")
        self.assertEqual(route_b["file_pattern"], "photo-excel-admin_*.xlsx")
        self.assertEqual(route_b["sheet"], "Fotos")
        self.assertEqual(route_b["source_grain"], "source_photo_row")
        self.assertEqual(route_b["authority_status"], "FUTURE_PRODUCTIVE_SOURCE_AUTHORITY")
        self.assertEqual(route_a["file"], "data/CUMPLIMIENTO_FRECUENCIA.xlsx")
        self.assertEqual(route_a["sheet"], "DB (KPIONE2.0)")
        self.assertEqual(route_a["status"], "HISTORICAL_BOOTSTRAP_AND_COMPATIBILITY_REFERENCE")
        self.assertFalse(route_a["future_ingestion_authority"])
        self.assertFalse(route_a["identity_authority"])
        self.assertFalse(productive["permanent_dual_source_authority"])
        self.assertNotIn("fixture_only", productive)
        self.assertEqual(
            productive["superseded_statement"]["status"],
            "SUPERSEDED_BY_ROUTE_B_FUTURE_PRODUCTIVE_AUTHORITY",
        )

    def test_contract_selects_source_and_persisted_grains(self):
        source_export = self.contract_payload["source_export"]
        self.assertEqual(source_export["authority_status"], "FUTURE_PRODUCTIVE_SOURCE_AUTHORITY")
        self.assertEqual(source_export["source_type"], "folder-delivered photo exports")
        self.assertEqual(source_export["file_pattern"], "photo-excel-admin_*.xlsx")
        self.assertEqual(source_export["sheet"], "Fotos")
        self.assertEqual(source_export["delivery_boundary"], "controlled input folder containing immutable source files")
        self.assertEqual(source_export["input_folder_path_role"], "runtime_configuration_not_identity")
        self.assertEqual(source_export["file_path_role"], "provenance_only")
        self.assertEqual(source_export["source_version_identity"], "source_file_sha256")
        self.assertEqual(self.contract["status"], "IDEMPOTENCY_CONTRACT_SELECTED")
        self.assertEqual(self.contract["source_grain"]["selected"], "source_photo_row")
        self.assertEqual(
            self.contract["persisted_grain"]["selected"],
            "immutable_event_photo_staging_row",
        )
        self.assertEqual(
            self.contract["persistence_boundary"]["selected"],
            "new_versioned_staging_boundary",
        )
        self.assertFalse(
            self.contract["persistence_boundary"]["legacy_database_state_is_migration_authority"]
        )

    def test_identity_hierarchy_keeps_traceability_separate_from_business_identity(self):
        hierarchy = self.contract["identity_hierarchy"]
        self.assertEqual(hierarchy["source_file_version"]["components"], ["source_file_sha256", "source_sheet"])
        self.assertIn("source_file_name", hierarchy["source_file_version"]["provenance_only"])
        self.assertIn("input_folder_path", hierarchy["source_file_version"]["provenance_only"])
        self.assertIn("input_folder_path", hierarchy["source_file_version"]["configuration_only"])
        self.assertEqual(
            hierarchy["source_row"]["components"],
            ["source_file_sha256", "source_sheet", "source_row_number"],
        )
        self.assertEqual(hierarchy["source_row"]["does_not_represent"], "business event identity")
        self.assertEqual(hierarchy["business_event"]["components"], ["event_id"])
        self.assertIn("event_stable_hash", hierarchy["business_event"]["stability_fields"])
        self.assertEqual(
            hierarchy["day_presence"]["location_key"],
            "cod_rt_norm when present, otherwise local_nombre_norm",
        )

    def test_idempotency_matrix_covers_required_cases_with_allowed_outcomes(self):
        rules = self.contract["idempotency_rules"]
        expected_cases = {
            "exact_rerun_same_file",
            "same_logical_file_different_filename",
            "corrected_file_replacing_previous_delivery",
            "appended_rows",
            "removed_rows",
            "partial_failed_ingestion",
            "resumed_ingestion",
            "duplicate_rows_inside_one_source",
            "same_event_multiple_source_files",
            "concurrent_or_overlapping_runs",
        }
        self.assertEqual(set(rules), expected_cases)
        allowed = {"no_op", "insert", "update", "supersession", "rejection", "quarantine", "new_source_version"}
        for case, spec in rules.items():
            self.assertIn(spec["outcome"], allowed, case)
            if "conflict_outcome" in spec:
                self.assertIn(spec["conflict_outcome"], allowed, case)

        self.assertEqual(rules["exact_rerun_same_file"]["outcome"], "no_op")
        self.assertEqual(rules["same_logical_file_different_filename"]["outcome"], "no_op")
        self.assertEqual(rules["corrected_file_replacing_previous_delivery"]["outcome"], "new_source_version")
        self.assertEqual(rules["removed_rows"]["outcome"], "supersession")
        self.assertEqual(rules["partial_failed_ingestion"]["outcome"], "quarantine")
        self.assertEqual(rules["concurrent_or_overlapping_runs"]["outcome"], "rejection")

    def test_state_preserves_016b_authority_and_keeps_018_productive_gates_closed(self):
        self.assertEqual(self.state["executive_progress"]["operating_model"], "V2_1_ACTIVE")
        self.assertEqual(self.state["current_work"]["unit"], "019")
        self.assertEqual(
            self.state["current_work"]["status"],
            "CREDENTIAL_ARCHITECTURE_READY_PROVISIONING_PENDING",
        )
        self.assertEqual(self.state["current_work"]["next_operation_status"], "NOT_AUTHORIZED")
        self.assertEqual(self.state["current_preparation"]["unit"], "018_PRODUCTIVE_APPLY")
        self.assertFalse(self.state["current_preparation"]["activation_gate"]["gate_open"])
        self.assertFalse(self.state["current_preparation"]["productive_apply_authorized"])
        self.assertTrue(self.state["authorization"]["016B_architecture_authorized"])
        self.assertTrue(self.state["authorization"]["017_authorized"])
        self.assertFalse(self.state["authorization"]["018_authorized"])

        productive_flags = [
            "supabase_access_authorized",
            "sql_execution_authorized",
            "db_reads_authorized",
            "db_writes_authorized",
            "apply_authorized",
            "cutover_authorized",
            "legacy_destructive_action_authorized",
            "app_runtime_modification_authorized",
            "loader_modification_authorized",
            "productive_contract_modification_authorized",
        ]
        self.assertTrue(all(self.state["authorization"][flag] is False for flag in productive_flags))

    def test_operating_model_v21_has_one_canonical_contract_and_one_ledger_decision(self):
        model = self.kernel["operating_model_v2"]
        contract = model["delivery_and_review_contract_v2_1"]
        self.assertEqual(contract["status"], "OPERATING_MODEL_V2_1_ACTIVE")
        self.assertFalse(contract["human_business_authority"]["technical_tests_substitute_for_approval"])
        self.assertEqual(json.dumps(self.kernel, ensure_ascii=True).count("delivery_and_review_contract_v2_1"), 1)
        self.assertIn("delivery_and_review_contract_v2_1", self.state["executive_progress"]["delivery_review_contract"])
        decisions = [entry for entry in self.ledger["entries"] if entry.get("id") == "ADR_OPERATING_MODEL_V2_1_PR_REVIEW_PRODUCTIVE_BOUNDARIES"]
        self.assertEqual(len(decisions), 1)
        self.assertIn("preserves its authority split and R1-R4 risk model", decisions[0]["compatibility"])
        self.assertEqual(self.state["current_preparation"]["status"], "TECHNICAL_BOUNDARY_READY_ROLE_PROVISIONING_PENDING")
        self.assertFalse(self.state["authorization"]["018_authorized"])
        self.assertTrue({"history", "commits", "pull_requests", "tests"}.isdisjoint(self.state))

    def test_directive_closes_017_and_advances_only_to_018_without_authorizing_it(self):
        self.assertEqual(self.directive["current_phase"], "017_COMPLETED")
        self.assertEqual(self.directive["allowed_next_phase"], "018_PRODUCTIVE_APPLY")
        self.assertEqual(self.directive["active_phase_count"], 0)
        self.assertEqual(self.directive["scope"]["016b_result"], "IDEMPOTENCY_CONTRACT_SELECTED")
        self.assertFalse(self.directive["scope"]["productive_authorization_granted"])

        phase_by_name = {phase["phase"]: phase for phase in self.directive["phases"]}
        self.assertEqual(
            phase_by_name["016B_IDENTITY_GRAIN_AND_IDEMPOTENCY_CONTRACT"]["status"],
            "CLOSED_SELECTED",
        )
        self.assertEqual(
            phase_by_name["016B_IDENTITY_GRAIN_AND_IDEMPOTENCY_CONTRACT"]["source_authority_result"],
            "PHOTO_EXPORT_SELECTED_AS_FUTURE_PRODUCTIVE_AUTHORITY",
        )
        self.assertEqual(
            phase_by_name["016B_IDENTITY_GRAIN_AND_IDEMPOTENCY_CONTRACT"]["route_b_authority"],
            "photo-excel-admin_*.xlsx / Fotos",
        )
        self.assertEqual(
            phase_by_name["016B_IDENTITY_GRAIN_AND_IDEMPOTENCY_CONTRACT"]["route_a_status"],
            "HISTORICAL_BOOTSTRAP_AND_COMPATIBILITY_REFERENCE",
        )
        self.assertEqual(phase_by_name["017_APPLY_RUNNER_AND_REHEARSAL"]["status"], "COMPLETED_READY")
        self.assertEqual(phase_by_name["018_PRODUCTIVE_APPLY"]["status"], "FUTURE_NOT_AUTHORIZED")

        amendment_requirements = self.directive["deviation_requires_directive_amendment"]["amendment_must_include"]
        self.assertIn("git_commit_reference_and_repository_path", amendment_requirements)
        self.assertNotIn("new_sha256", amendment_requirements)

    def test_ledger_has_single_durable_016b_adr(self):
        entries = self.ledger["entries"]
        matches = [
            entry
            for entry in entries
            if entry.get("id") == "ADR_016B_IDENTITY_GRAIN_IDEMPOTENCY_CONTRACT"
        ]
        self.assertEqual(len(matches), 1)
        adr = matches[0]
        self.assertEqual(adr["readiness_result"], "IDEMPOTENCY_CONTRACT_SELECTED")
        self.assertEqual(
            adr["selected_contract"]["source_authority_decision"],
            "PHOTO_EXPORT_SELECTED_AS_FUTURE_PRODUCTIVE_AUTHORITY",
        )
        self.assertEqual(
            adr["selected_contract"]["route_b"]["authority_status"],
            "FUTURE_PRODUCTIVE_SOURCE_AUTHORITY",
        )
        self.assertEqual(
            adr["selected_contract"]["route_a"]["status"],
            "HISTORICAL_BOOTSTRAP_AND_COMPATIBILITY_REFERENCE",
        )
        self.assertFalse(adr["selected_contract"]["route_a"]["future_ingestion_authority"])
        self.assertFalse(adr["selected_contract"]["permanent_dual_source_authority"])
        self.assertEqual(adr["selected_contract"]["source_grain"], "source_photo_row")
        self.assertFalse(adr["evidence"]["db_access"])
        self.assertFalse(adr["evidence"]["sql_executed"])
        self.assertFalse(adr["evidence"]["apply_executed"])
        self.assertIn("017 is the next permitted unit but remains unauthorized.", adr["implications_for_017"])
        self.assertIn(
            "The input folder path is runtime configuration and provenance, not identity authority.",
            adr["implications_for_017"],
        )


if __name__ == "__main__":
    unittest.main()
