from __future__ import annotations

import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "research" / "AI_LOAD_OBSERVATION_CONTRACT.json"
LEDGER_PATH = ROOT / "research" / "AI_LOAD_OBSERVATION_LEDGER.jsonl"
PROTOCOL_PATH = ROOT / "research" / "AI_RESEARCH_PROTOCOL.json"


REQUIRED_FIELDS = {
    "observation_id",
    "recorded_at",
    "source",
    "effective_week_start",
    "operation_type",
    "input_file_name",
    "input_file_sha256",
    "schema_signature",
    "input_rows",
    "accepted_rows",
    "rejected_rows",
    "exact_duplicate_rows",
    "grain_duplicate_rows",
    "missing_required_rows",
    "source_check_verdict",
    "loader_executed",
    "db_write_executed",
    "post_load_validation_status",
    "anomaly_label",
    "anomaly_reason",
    "evidence_refs",
    "recorded_by",
    "reviewed_by",
    "implementation_authorized",
}

ALLOWED_LABELS = {
    "UNREVIEWED",
    "CLEAN",
    "EXPECTED_CHANGE",
    "ANOMALOUS",
    "INVALID_INPUT",
    "LOAD_FAILURE",
    "POST_LOAD_REGRESSION",
}


class AiLoadObservationContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        cls.protocol = json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))
        cls.ledger_text = LEDGER_PATH.read_text(encoding="utf-8")

    def test_json_valid_and_contract_id(self) -> None:
        self.assertEqual(self.contract["contract_id"], "AI_LOAD_OBSERVATION_CONTRACT_V1")

    def test_status_and_authority_are_observe_only(self) -> None:
        self.assertEqual(self.contract["status"], "ACTIVE_OBSERVE_ONLY")
        self.assertEqual(self.contract["authority"], "Bastian")

    def test_model_fitting_and_production_are_not_authorized(self) -> None:
        self.assertFalse(self.contract["model_fitting_authorized"])
        self.assertFalse(self.contract["production_use_authorized"])

    def test_required_fields_complete(self) -> None:
        self.assertEqual(set(self.contract["required_fields"]), REQUIRED_FIELDS)

    def test_labels_allowed_and_default_unreviewed(self) -> None:
        label_contract = self.contract["label_contract"]
        self.assertEqual(set(label_contract["allowed_values"]), ALLOWED_LABELS)
        self.assertEqual(label_contract["default"], "UNREVIEWED")

    def test_clean_requires_post_load_validation(self) -> None:
        clean_rule = self.contract["label_contract"]["validation_rules"]["CLEAN"]
        self.assertTrue(clean_rule["requires_post_load_validation"])

    def test_anomalous_requires_reason_and_evidence(self) -> None:
        rule = self.contract["label_contract"]["validation_rules"]["ANOMALOUS"]
        self.assertTrue(rule["requires_anomaly_reason"])
        self.assertTrue(rule["requires_evidence_refs"])

    def test_unknown_fields_can_be_null(self) -> None:
        self.assertIn("Unknown fields must remain null.", self.contract["quality_rules"])

    def test_secret_and_dsn_storage_forbidden(self) -> None:
        privacy = " ".join(self.contract["privacy_rules"] + self.contract["forbidden_uses"])
        self.assertIn("No DSN.", privacy)
        self.assertIn("No credentials.", privacy)

    def test_no_pii_storage_forbidden(self) -> None:
        privacy = " ".join(self.contract["privacy_rules"] + self.contract["forbidden_uses"])
        self.assertIn("No personal names.", privacy)
        self.assertIn("No customer or store rows.", privacy)
        self.assertIn("Only aggregate metrics and technical references.", privacy)

    def test_reevaluation_triggers_are_heuristic(self) -> None:
        triggers = self.contract["reevaluation_triggers"]
        self.assertEqual(triggers["threshold_type"], "HEURISTIC_NOT_FORMAL_PROOF")
        self.assertEqual(triggers["review_after_total_observations"], 20)
        self.assertEqual(triggers["review_after_source_observations"], 10)
        self.assertEqual(triggers["review_after_confirmed_anomalies"], 3)
        self.assertEqual(triggers["first_comparison"], "RULES_ONLY_VS_DETERMINISTIC_SCORECARD")

    def test_ledger_initially_empty(self) -> None:
        self.assertEqual(self.ledger_text, "")

    def test_no_historical_observations_invented(self) -> None:
        self.assertIn("Do not backfill or invent historical observations.", self.contract["forbidden_uses"])
        self.assertEqual(self.ledger_text.splitlines(), [])

    def test_active_horizon_h1_not_modified(self) -> None:
        self.assertEqual(self.contract["active_operational_horizon"], "H1")
        self.assertEqual(self.contract["active_gate"], "CG-005")
        self.assertEqual(
            self.protocol["probabilistic_decision_support"]["active_operational_horizon"],
            "H1",
        )

    def test_protocol_requires_blind_routing(self) -> None:
        routing = self.protocol["blind_prompt_routing"]
        self.assertEqual(routing["status"], "ACTIVE")
        self.assertTrue(routing["assistant_behavior"]["recommendation_block_required_before_agent_prompts"])
        self.assertTrue(routing["assistant_behavior"]["recommendation_block_is_for_bastian_only"])

    def test_protocol_version_and_last_updated(self) -> None:
        self.assertEqual(self.protocol["schema_version"], 2)
        self.assertEqual(self.protocol["last_updated"], "2026-06-13")

    def test_recommendation_fields_are_complete(self) -> None:
        self.assertEqual(
            self.protocol["blind_prompt_routing"]["recommendation_fields"],
            [
                "agent",
                "task_class",
                "recommended_model",
                "reasoning_level",
                "expected_cost",
                "quality_target",
                "reason",
                "escalation_condition",
            ],
        )

    def test_quality_target_may_remain_inside_prompt(self) -> None:
        routing = self.protocol["blind_prompt_routing"]
        self.assertIn("quality_target", routing["prompt_may_include"])

    def test_model_and_reasoning_selection_excluded_by_default(self) -> None:
        routing = self.protocol["blind_prompt_routing"]
        self.assertIn("selected_model", routing["prompt_must_not_include_by_default"])
        self.assertIn("selected_reasoning_level", routing["prompt_must_not_include_by_default"])
        self.assertTrue(routing["assistant_behavior"]["do_not_copy_recommendation_into_prompt"])
        self.assertTrue(routing["assistant_behavior"]["bastian_selects_model_and_reasoning_in_ui"])

    def test_probabilistic_decision_support_is_observe_only(self) -> None:
        support = self.protocol["probabilistic_decision_support"]
        self.assertEqual(support["status"], "OBSERVE_ONLY")
        self.assertFalse(support["implementation_justified"])
        self.assertFalse(support["production_justified"])
        self.assertEqual(support["validated_candidate"], "UC-DL-01")
        self.assertFalse(support["pilot_ready"])
        self.assertFalse(support["causal_claims_authorized"])

    def test_protocol_routing_does_not_authorize_work(self) -> None:
        rules = " ".join(self.protocol["blind_prompt_routing"]["rules"])
        self.assertIn("does not authorize implementation, DB, commit, or push", rules)


if __name__ == "__main__":
    unittest.main()
