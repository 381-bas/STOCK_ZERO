"""Tests for scripts/sz_load_observation.py.

Synthetic fixtures only. No DB, no network, no real data.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "sz_load_observation.py"
CONTRACT_PATH = ROOT / "research" / "AI_LOAD_OBSERVATION_CONTRACT.json"
LEDGER_PATH = ROOT / "research" / "AI_LOAD_OBSERVATION_LEDGER.jsonl"
SKILL_PATH = ROOT / ".claude" / "skills" / "sz-load-observation" / "SKILL.md"


def _load_module():
    spec = importlib.util.spec_from_file_location("sz_load_observation", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MOD = _load_module()

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def shape_b(**overrides):
    base = {
        "source": "RUTA_RUTERO",
        "effective_week_start": "2026-06-08",
        "operation_type": "PREFLIGHT",
        "input_file_name": "WORKBOOK_XLSX",
        "input_file_sha256": SHA_A,
        "schema_signature": SHA_B,
        "input_rows": 100,
        "accepted_rows": 98,
        "rejected_rows": 2,
        "exact_duplicate_excess": 1,
        "grain_duplicate_groups": 1,
        "missing_required_rows": 0,
        "writes_executed": False,
        "source_check_verdict": "WARN",
        "loader_executed": False,
        "post_load_validation_status": None,
        "batch_id": 42,
        "raw_batch_ids": [19, 38],
        "affected_weeks": ["2026-06-08"],
    }
    base.update(overrides)
    return base


def shape_a(**overrides):
    draft = {
        "input_file_name": "WORKBOOK_XLSX",
        "input_file_sha256": SHA_A,
        "schema_signature": SHA_B,
        "input_rows": 100,
        "accepted_rows": 98,
        "rejected_rows": 2,
        "exact_duplicate_rows": 1,
        "grain_duplicate_rows": 1,
        "missing_required_rows": 0,
        "source_check_verdict": "WARN",
        "loader_executed": False,
        "db_write_executed": False,
        "post_load_validation_status": None,
        "batch_id": 7,
        "raw_batch_ids": [19],
        "affected_weeks": ["2026-06-08"],
    }
    draft.update(overrides)
    return {"observation_draft": draft}


def run(argv):
    buf = io.StringIO()
    err = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(err):
        code = MOD.run(argv)
    return code, buf.getvalue(), err.getvalue()


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        MOD.TEST_ALLOWED_INPUT_ROOTS.clear()
        MOD.register_test_input_root(self.tmp)

    def tearDown(self):
        MOD.TEST_ALLOWED_INPUT_ROOTS.clear()
        self._tmp.cleanup()

    def write(self, name, obj):
        path = self.tmp / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        return path

    def write_text(self, name, text):
        path = self.tmp / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def draft_path(self, path: Path, *extra, source="RUTA_RUTERO", week="2026-06-08",
                   op="PREFLIGHT", input_sha256=SHA_A, recorded_at="2026-06-13T15:00:00Z"):
        argv = [
            "draft", "--phase-json", str(path),
            "--source", source,
            "--effective-week-start", week,
            "--operation-type", op,
            "--input-file-sha256", input_sha256,
            "--recorded-at", recorded_at,
            "--recorded-by", "USER_AUTHORITY",
            *extra,
        ]
        code, out, err = run(argv)
        return code, json.loads(out), out, err

    def draft(self, phase_obj, *extra, name="phase.json", **kwargs):
        return self.draft_path(self.write(name, phase_obj), *extra, **kwargs)

    def valid_record(self, **kwargs):
        code, payload, _out, _err = self.draft(shape_b(**kwargs))
        self.assertEqual(code, 0, payload)
        return payload

    def validate(self, record, name="record.json"):
        path = self.write(name, record)
        code, out, err = run(["validate", "--record", str(path)])
        return code, json.loads(out), out, err

    def assert_error(self, result, error):
        code, payload, _out, err = result
        self.assertNotEqual(code, 0)
        self.assertEqual(err, "")
        self.assertEqual(payload["error"], error)


class ContractAndIntegrityTests(Base):
    def test_01_contract_required_fields_exact(self):
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(list(MOD.REQUIRED_FIELDS), contract["required_fields"])

    def test_02_contract_optional_fields_exact(self):
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(list(MOD.OPTIONAL_FIELDS), contract["optional_fields"])

    def test_03_contract_sources_exact(self):
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(list(MOD.ALLOWED_SOURCES), contract["sources"])

    def test_04_contract_labels_exact(self):
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(list(MOD.ALLOWED_LABELS), contract["label_contract"]["allowed_values"])

    def test_05_default_label_exact(self):
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(MOD.DEFAULT_LABEL, contract["label_contract"]["default"])

    def test_06_ledger_not_modified_by_draft(self):
        before = hashlib.sha256(LEDGER_PATH.read_bytes()).hexdigest()
        self.draft(shape_b())
        after = hashlib.sha256(LEDGER_PATH.read_bytes()).hexdigest()
        self.assertEqual(before, after)

    def test_07_ledger_not_modified_by_validate(self):
        rec = self.valid_record()
        before = hashlib.sha256(LEDGER_PATH.read_bytes()).hexdigest()
        self.validate(rec)
        after = hashlib.sha256(LEDGER_PATH.read_bytes()).hexdigest()
        self.assertEqual(before, after)

    def test_08_script_compiles_by_import(self):
        self.assertTrue(hasattr(MOD, "run"))


class OutputAndDeterminismTests(Base):
    def test_09_draft_from_shape_a(self):
        code, payload, _out, _err = self.draft(shape_a())
        self.assertEqual(code, 0, payload)
        self.assertEqual(payload["batch_id"], 7)

    def test_10_draft_from_shape_b_aliases(self):
        code, payload, _out, _err = self.draft(shape_b())
        self.assertEqual(code, 0, payload)
        self.assertEqual(payload["exact_duplicate_rows"], 1)
        self.assertEqual(payload["db_write_executed"], False)

    def test_11_all_required_fields_present(self):
        rec = self.valid_record()
        for field in MOD.REQUIRED_FIELDS:
            self.assertIn(field, rec)

    def test_12_unknowns_not_propagated(self):
        code, payload, _out, _err = self.draft(shape_b(unknown_metric=123))
        self.assertEqual(code, 0, payload)
        self.assertNotIn("unknown_metric", payload)

    def test_13_implementation_authorized_false(self):
        self.assertIs(self.valid_record()["implementation_authorized"], False)

    def test_14_default_label_unreviewed(self):
        self.assertEqual(self.valid_record()["anomaly_label"], "UNREVIEWED")

    def test_15_draft_compact_deterministic(self):
        path = self.write("phase.json", shape_b())
        self.assertEqual(self.draft_path(path)[2], self.draft_path(path)[2])

    def test_16_draft_pretty_deterministic(self):
        path = self.write("phase.json", shape_b())
        argv_extra = ("--pretty",)
        self.assertEqual(self.draft_path(path, *argv_extra)[2], self.draft_path(path, *argv_extra)[2])

    def test_17_validate_deterministic(self):
        rec = self.valid_record()
        self.assertEqual(self.validate(rec)[2], self.validate(rec)[2])

    def test_18_validate_generated_candidate(self):
        rec = self.valid_record()
        code, payload, _out, _err = self.validate(rec)
        self.assertEqual(code, 0, payload)
        self.assertEqual(payload["validate"], "ok")

    def test_19_validate_rejects_unknown_field(self):
        rec = self.valid_record()
        rec["surprise"] = 1
        self.assert_error(self.validate(rec), "unknown_fields")

    def test_20_observation_id_same_args_same_id(self):
        self.assertEqual(self.valid_record()["observation_id"], self.valid_record()["observation_id"])

    def test_21_observation_id_recorded_at_changes_id(self):
        one = self.draft(shape_b(), recorded_at="2026-06-13T15:00:00Z")[1]
        two = self.draft(shape_b(), recorded_at="2026-06-13T16:00:00Z")[1]
        self.assertNotEqual(one["observation_id"], two["observation_id"])

    def test_22_observation_id_matches_event_formula(self):
        rec = self.valid_record()
        expected = MOD.make_observation_id("RUTA_RUTERO", "2026-06-08", "PREFLIGHT", SHA_A, "2026-06-13T15:00:00Z")
        self.assertEqual(rec["observation_id"], expected)


class PathSafetyTests(Base):
    def test_23_external_absolute_path_rejected_without_injection(self):
        MOD.TEST_ALLOWED_INPUT_ROOTS.clear()
        path = self.write("external.json", shape_b())
        self.assert_error(self.draft_path(path), "unsafe_input_path")

    def test_24_registered_temp_path_allowed_for_tests(self):
        code, payload, _out, _err = self.draft(shape_b())
        self.assertEqual(code, 0, payload)

    def test_25_traversal_rejected(self):
        path = self.tmp / "sub" / ".." / "phase.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(shape_b()), encoding="utf-8")
        self.assert_error(self.draft_path(path), "unsafe_input_path")

    def test_26_dot_env_rejected_before_read(self):
        self.assert_error(self.draft(shape_b(), name=".env"), "unsafe_input_path")

    def test_27_dot_env_suffix_rejected(self):
        self.assert_error(self.draft(shape_b(), name=".env.local"), "unsafe_input_path")

    def test_28_local_secrets_rejected(self):
        self.assert_error(self.draft(shape_b(), name=".local_secrets/phase.json"), "unsafe_input_path")

    def test_29_credentials_json_rejected(self):
        self.assert_error(self.draft(shape_b(), name="credentials.json"), "unsafe_input_path")

    def test_30_data_path_rejected(self):
        self.assert_error(self.draft(shape_b(), name="data/phase.json"), "unsafe_input_path")

    def test_31_evidence_path_rejected(self):
        self.assert_error(self.draft(shape_b(), name="evidence/phase.json"), "unsafe_input_path")

    def test_32_symlink_rejected(self):
        target = self.write("target.json", shape_b())
        link = self.tmp / "link.json"
        try:
            os.symlink(target, link)
        except OSError:
            self.skipTest("symlink not available")
        self.assert_error(self.draft_path(link), "unsafe_input_path")

    def test_33_invalid_json_rejected(self):
        self.assert_error(self.draft_path(self.write_text("bad.json", "{ bad")), "invalid_json")

    def test_34_oversized_input_rejected(self):
        path = self.tmp / "big.json"
        path.write_bytes(b"a" * (MOD.MAX_INPUT_BYTES + 1))
        self.assert_error(self.draft_path(path), "oversized_input")


class TypeConsistencyTests(Base):
    def test_35_input_rows_text_rejected(self):
        self.assert_error(self.draft(shape_b(input_rows="100")), "invalid_count_type")

    def test_36_accepted_rows_negative_rejected(self):
        self.assert_error(self.draft(shape_b(accepted_rows=-1)), "invalid_count_negative")

    def test_37_accepted_rows_gt_input_rejected(self):
        self.assert_error(self.draft(shape_b(input_rows=10, accepted_rows=11)), "count_exceeds_input")

    def test_38_count_sum_gt_input_rejected(self):
        self.assert_error(self.draft(shape_b(input_rows=10, accepted_rows=9, rejected_rows=2)), "count_sum_exceeds_input")

    def test_39_rejected_rows_negative_rejected(self):
        self.assert_error(self.draft(shape_b(rejected_rows=-1)), "invalid_count_negative")

    def test_40_duplicate_negative_rejected(self):
        self.assert_error(self.draft(shape_b(exact_duplicate_excess=-1)), "invalid_count_negative")

    def test_41_loader_string_rejected(self):
        self.assert_error(self.draft(shape_b(loader_executed="false")), "invalid_boolean_type")

    def test_42_db_write_string_rejected(self):
        self.assert_error(self.draft(shape_b(writes_executed="false")), "invalid_boolean_type")

    def test_43_raw_batch_ids_invalid_rejected(self):
        self.assert_error(self.draft(shape_b(raw_batch_ids=["x"])), "invalid_list")

    def test_44_affected_weeks_not_list_rejected(self):
        self.assert_error(self.draft(shape_b(affected_weeks="2026-06-08")), "invalid_list")

    def test_45_affected_weeks_non_monday_rejected(self):
        self.assert_error(self.draft(shape_b(affected_weeks=["2026-06-09"])), "date_must_be_monday")

    def test_46_rollback_flag_string_rejected(self):
        self.assert_error(self.draft(shape_b(rollback_required="yes")), "invalid_boolean_type")

    def test_47_minimum_date_invalid_rejected(self):
        self.assert_error(self.draft(shape_b(minimum_date="bad-date")), "invalid_date_format")

    def test_48_sha_short_rejected(self):
        self.assert_error(self.draft(shape_b(input_file_sha256="abc")), "invalid_sha256")

    def test_49_sha_nonhex_rejected(self):
        self.assert_error(self.draft(shape_b(input_file_sha256="g" * 64)), "invalid_sha256")

    def test_50_sha_null_rejected(self):
        self.assert_error(self.draft(shape_b(input_file_sha256=None)), "missing_sha256")

    def test_51_validate_rejects_mutated_type(self):
        rec = self.valid_record()
        rec["input_rows"] = "100"
        self.assert_error(self.validate(rec), "invalid_count_type")

    def test_52_validate_rejects_mutated_hash_with_matching_id_missing(self):
        rec = self.valid_record()
        rec["input_file_sha256"] = "abc"
        self.assert_error(self.validate(rec), "invalid_sha256")


class CliJsonConsistencyAndShapeTests(Base):
    def test_53_phase_source_cli_mismatch_rejected(self):
        self.assert_error(self.draft(shape_b(source="KPIONE2")), "phase_cli_mismatch")

    def test_54_phase_week_cli_mismatch_rejected(self):
        self.assert_error(self.draft(shape_b(effective_week_start="2026-06-15")), "phase_cli_mismatch")

    def test_55_phase_operation_cli_mismatch_rejected(self):
        self.assert_error(self.draft(shape_b(operation_type="DRY_RUN")), "phase_cli_mismatch")

    def test_55b_phase_hash_cli_mismatch_rejected(self):
        self.assert_error(self.draft(shape_b(input_file_sha256=SHA_C)), "phase_cli_mismatch")

    def test_56_shape_a_source_cli_mismatch_rejected(self):
        self.assert_error(self.draft(shape_a(source="KPIONE2")), "phase_cli_mismatch")

    def test_57_shape_a_incomplete_rejected(self):
        self.assert_error(self.draft({"observation_draft": {"input_file_name": "WORKBOOK_XLSX"}}), "incomplete_input_shape")

    def test_58_shape_b_one_key_rejected(self):
        self.assert_error(self.draft({"input_rows": 10}), "incomplete_input_shape")

    def test_59_shape_b_accidental_rejected(self):
        self.assert_error(self.draft({"schema_signature": SHA_B}), "incomplete_input_shape")

    def test_60_alias_conflict_rejected(self):
        self.assert_error(self.draft(shape_b(db_write_executed=True, writes_executed=False)), "alias_conflict")

    def test_61_row_array_outside_rejected(self):
        obj = shape_b(rows=[{"x": 1}])
        self.assert_error(self.draft(obj), "privacy_violation_detected")

    def test_62_row_array_inside_shape_a_rejected(self):
        self.assert_error(self.draft(shape_a(rows=[{"x": 1}])), "privacy_violation_detected")

    def test_63_unknown_optional_nested_not_propagated(self):
        code, payload, _out, _err = self.draft(shape_b(metrics={"nested": 1}))
        self.assertEqual(code, 0, payload)
        self.assertNotIn("metrics", payload)


class PrivacyTests(Base):
    def test_64_dsn_rejected(self):
        self.assert_error(self.draft(shape_b(comment="postgresql://redacted.invalid/db")), "privacy_violation_detected")

    def test_65_email_rejected(self):
        self.assert_error(self.draft(shape_b(comment="person@example.invalid")), "privacy_violation_detected")

    def test_66_service_token_key_rejected(self):
        self.assert_error(self.draft(shape_b(service_token="redacted")), "privacy_violation_detected")

    def test_67_payload_json_rejected(self):
        self.assert_error(self.draft(shape_b(payload_json={"x": 1})), "privacy_violation_detected")

    def test_68_cliente_key_rejected(self):
        self.assert_error(self.draft(shape_b(cliente_id="redacted")), "privacy_violation_detected")

    def test_69_store_key_rejected(self):
        self.assert_error(self.draft(shape_b(store_id="redacted")), "privacy_violation_detected")

    def test_70_list_of_objects_rejected(self):
        self.assert_error(self.draft(shape_b(rows=[{"x": 1}])), "privacy_violation_detected")

    def test_71_nested_structure_safe(self):
        code, payload, _out, _err = self.draft(shape_b(metrics={"aggregate": 1}))
        self.assertEqual(code, 0, payload)

    def test_72_benign_secret_substring_value_allowed(self):
        code, payload, _out, _err = self.draft(shape_b(comment="secretariat"))
        self.assertEqual(code, 0, payload)

    def test_73_url_query_rejected(self):
        self.assert_error(self.draft(shape_b(report_url="https://example.invalid/a?token=x")), "privacy_violation_detected")

    def test_74_plain_url_unknown_not_propagated(self):
        code, payload, _out, _err = self.draft(shape_b(report_url="https://example.invalid/a"))
        self.assertEqual(code, 0, payload)
        self.assertNotIn("report_url", payload)

    def test_75_notes_free_text_rejected(self):
        self.assert_error(self.draft(shape_b(), "--notes", "Synthetic Person"), "invalid_technical_code")

    def test_76_notes_technical_code_allowed(self):
        code, payload, _out, _err = self.draft(shape_b(), "--notes", "SCHEMA_DRIFT")
        self.assertEqual(code, 0, payload)
        self.assertEqual(payload["notes"], "SCHEMA_DRIFT")

    def test_77_reason_free_text_rejected(self):
        self.assert_error(self.draft(shape_b(), "--label", "ANOMALOUS", "--anomaly-reason", "row count spike", "--evidence-ref", "commit:abcdef1"), "invalid_technical_code")

    def test_78_reason_technical_code_allowed(self):
        code, payload, _out, _err = self.draft(shape_b(), "--label", "ANOMALOUS", "--anomaly-reason", "POSTCHECK_ROW_COUNT_MISMATCH", "--evidence-ref", "commit:abcdef1")
        self.assertEqual(code, 0, payload)


class LabelMatrixTests(Base):
    def test_79_source_check_clean_valid(self):
        code, payload, _out, _err = self.draft(shape_b(operation_type="SOURCE_CHECK", source_check_verdict="OK"), "--label", "CLEAN", op="SOURCE_CHECK")
        self.assertEqual(code, 0, payload)

    def test_80_preflight_clean_rejected(self):
        self.assert_error(self.draft(shape_b(source_check_verdict="OK"), "--label", "CLEAN"), "operation_label_requirements_not_met")

    def test_81_dry_run_clean_valid(self):
        code, payload, _out, _err = self.draft(shape_b(operation_type="DRY_RUN", source_check_verdict="OK"), "--label", "CLEAN", op="DRY_RUN")
        self.assertEqual(code, 0, payload)

    def test_82_apply_unreviewed_loader_false_rejected(self):
        self.assert_error(self.draft(shape_b(operation_type="APPLY"), op="APPLY"), "operation_label_requirements_not_met")

    def test_83_apply_clean_without_postcheck_rejected(self):
        self.assert_error(self.draft(shape_b(operation_type="APPLY", loader_executed=True, writes_executed=True), "--label", "CLEAN", op="APPLY"), "operation_label_requirements_not_met")

    def test_84_apply_clean_valid(self):
        code, payload, _out, _err = self.draft(shape_b(operation_type="APPLY", loader_executed=True, writes_executed=True, post_load_validation_status="OK"), "--label", "CLEAN", op="APPLY")
        self.assertEqual(code, 0, payload)

    def test_85_load_failure_with_ok_status_rejected(self):
        self.assert_error(self.draft(shape_b(operation_type="APPLY", loader_executed=True, writes_executed=True, post_load_validation_status="OK"), "--label", "LOAD_FAILURE", "--anomaly-reason", "SCHEMA_DRIFT", "--evidence-ref", "commit:abcdef1", op="APPLY"), "operation_label_requirements_not_met")

    def test_86_load_failure_valid(self):
        code, payload, _out, _err = self.draft(shape_b(operation_type="APPLY", loader_executed=True, writes_executed=True, post_load_validation_status="FAILED"), "--label", "LOAD_FAILURE", "--anomaly-reason", "SCHEMA_DRIFT", "--evidence-ref", "commit:abcdef1", op="APPLY")
        self.assertEqual(code, 0, payload)

    def test_87_post_load_clean_without_db_rejected(self):
        self.assert_error(self.draft(shape_b(operation_type="POST_LOAD_VALIDATION", loader_executed=True, writes_executed=False, post_load_validation_status="OK"), "--label", "CLEAN", op="POST_LOAD_VALIDATION"), "operation_label_requirements_not_met")

    def test_88_post_load_clean_valid(self):
        code, payload, _out, _err = self.draft(shape_b(operation_type="POST_LOAD_VALIDATION", loader_executed=True, writes_executed=True, post_load_validation_status="OK"), "--label", "CLEAN", op="POST_LOAD_VALIDATION")
        self.assertEqual(code, 0, payload)

    def test_89_post_load_regression_valid(self):
        code, payload, _out, _err = self.draft(shape_b(operation_type="POST_LOAD_VALIDATION", loader_executed=True, writes_executed=True, post_load_validation_status="FAILED"), "--label", "POST_LOAD_REGRESSION", "--evidence-ref", "report:r1", op="POST_LOAD_VALIDATION")
        self.assertEqual(code, 0, payload)

    def test_90_rollback_clean_valid(self):
        code, payload, _out, _err = self.draft(shape_b(operation_type="ROLLBACK", loader_executed=True, writes_executed=True, post_load_validation_status="OK", rollback_required=True, rollback_executed=True), "--label", "CLEAN", op="ROLLBACK")
        self.assertEqual(code, 0, payload)

    def test_91_rollback_missing_bool_rejected(self):
        self.assert_error(self.draft(shape_b(operation_type="ROLLBACK", loader_executed=True, writes_executed=True, rollback_required=True), op="ROLLBACK"), "operation_label_requirements_not_met")


class EvidenceAndErrorTests(Base):
    def test_92_commit_ref_syntax_only_valid(self):
        code, payload, _out, _err = self.draft(shape_b(), "--label", "ANOMALOUS", "--anomaly-reason", "SCHEMA_DRIFT", "--evidence-ref", "commit:deadbeef")
        self.assertEqual(code, 0, payload)

    def test_93_phase_ref_syntax_only_valid(self):
        code, payload, _out, _err = self.draft(shape_b(), "--label", "ANOMALOUS", "--anomaly-reason", "SCHEMA_DRIFT", "--evidence-ref", "phase:DOES_NOT_EXIST")
        self.assertEqual(code, 0, payload)

    def test_94_research_existing_ref_valid(self):
        code, payload, _out, _err = self.draft(shape_b(), "--label", "ANOMALOUS", "--anomaly-reason", "SCHEMA_DRIFT", "--evidence-ref", "research/AI_LOAD_OBSERVATION_CONTRACT.json")
        self.assertEqual(code, 0, payload)

    def test_95_research_nonexistent_ref_rejected(self):
        self.assert_error(self.draft(shape_b(), "--label", "ANOMALOUS", "--anomaly-reason", "SCHEMA_DRIFT", "--evidence-ref", "research/DOES_NOT_EXIST.json"), "invalid_evidence_ref")

    def test_96_url_evidence_rejected(self):
        self.assert_error(self.draft(shape_b(), "--evidence-ref", "https://example.invalid/x"), "invalid_evidence_ref")

    def test_97_absolute_evidence_rejected(self):
        self.assert_error(self.draft(shape_b(), "--evidence-ref", "C:/tmp/x"), "invalid_evidence_ref")

    def test_98_traversal_evidence_rejected(self):
        self.assert_error(self.draft(shape_b(), "--evidence-ref", "research/../x"), "invalid_evidence_ref")

    def test_99_backslash_evidence_rejected(self):
        self.assert_error(self.draft(shape_b(), "--evidence-ref", r"research\\x.json"), "invalid_evidence_ref")

    def test_100_data_evidence_rejected(self):
        self.assert_error(self.draft(shape_b(), "--evidence-ref", "data/x.json"), "invalid_evidence_ref")

    def test_101_evidence_path_rejected(self):
        self.assert_error(self.draft(shape_b(), "--evidence-ref", "evidence/x.json"), "invalid_evidence_ref")

    def test_102_unicode_evidence_rejected(self):
        self.assert_error(self.draft(shape_b(), "--evidence-ref", "research/cafeé.json"), "invalid_evidence_ref")

    def test_103_long_evidence_rejected(self):
        self.assert_error(self.draft(shape_b(), "--evidence-ref", "research/" + "a" * 241), "invalid_evidence_ref")

    def test_104_argparse_missing_args_json(self):
        code, out, err = run(["draft"])
        payload = json.loads(out)
        self.assertNotEqual(code, 0)
        self.assertEqual(err, "")
        self.assertEqual(payload["error_category"], "ARGPARSE")

    def test_105_argparse_unknown_command_json(self):
        code, out, err = run(["not_a_command"])
        payload = json.loads(out)
        self.assertNotEqual(code, 0)
        self.assertEqual(err, "")
        self.assertEqual(payload["error_category"], "ARGPARSE")

    def test_106_input_not_found_json(self):
        self.assert_error(self.draft_path(self.tmp / "missing.json"), "input_not_found")

    def test_107_validate_reports_evidence_ref_scope(self):
        rec = self.valid_record()
        code, payload, _out, _err = self.validate(rec)
        self.assertEqual(code, 0, payload)
        self.assertEqual(payload["evidence_ref_validation"]["commit"], "syntax_only")


class SourceSafetyAndSkillTests(unittest.TestCase):
    def setUp(self):
        self.src = SCRIPT_PATH.read_text(encoding="utf-8")
        self.skill = SKILL_PATH.read_text(encoding="utf-8")

    def test_108_no_network_or_db_imports(self):
        for forbidden in ("import socket", "import requests", "http.client", "psycopg", "sqlite3", "sqlalchemy", "import ssl"):
            self.assertNotIn(forbidden, self.src)

    def test_109_no_subprocess(self):
        self.assertNotIn("import subprocess", self.src)
        self.assertNotIn("subprocess.", self.src)

    def test_110_no_shell_true(self):
        self.assertNotIn("shell=True", self.src)

    def test_111_no_file_writes(self):
        self.assertNotIn("write_text(", self.src)
        self.assertNotIn("write_bytes(", self.src)
        self.assertNotIn("open(", self.src)

    def test_112_skill_frontmatter_valid(self):
        self.assertTrue(self.skill.startswith("---"))
        self.assertIn("name: sz-load-observation", self.skill)

    def test_113_disable_model_invocation_true(self):
        self.assertIn("disable-model-invocation: true", self.skill)

    def test_114_allowed_tools_only_script(self):
        frontmatter = self.skill.split("---")[1]
        bullets = [ln for ln in frontmatter.splitlines() if ln.strip().startswith("- ")]
        self.assertEqual(len(bullets), 1)
        self.assertIn("Bash(python scripts/sz_load_observation.py *)", bullets[0])


if __name__ == "__main__":
    unittest.main()
