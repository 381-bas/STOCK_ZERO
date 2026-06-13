"""Tests for scripts/sz_load_observation.py.

Deterministic, fixture-based, no real or personal data. No DB, no network.
Confirms the load-observation candidate tool honors
research/AI_LOAD_OBSERVATION_CONTRACT.json and never writes the ledger.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
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

SHA_A = "A" * 64
SHA_B = "B" * 64


def shape_b(**overrides):
    base = {
        "source": "RUTA_RUTERO",
        "effective_week_start": "2026-06-08",
        "input_file_name": "WORKBOOK.xlsx",
        "input_file_sha256": SHA_A,
        "schema_signature": SHA_B,
        "input_rows": 3542,
        "accepted_rows": 3539,
        "rejected_rows": 3,
        "exact_duplicate_excess": 3,
        "grain_duplicate_groups": 1,
        "writes_executed": False,
        "source_check_verdict": "warn",
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
        "input_file_name": "WORKBOOK.xlsx",
        "input_file_sha256": SHA_A,
        "schema_signature": SHA_B,
        "input_rows": 3542,
        "accepted_rows": 3539,
        "rejected_rows": 3,
        "exact_duplicate_rows": 3,
        "grain_duplicate_rows": 1,
        "missing_required_rows": 0,
        "source_check_verdict": "warn",
        "loader_executed": False,
        "db_write_executed": False,
        "post_load_validation_status": None,
        "batch_id": 7,
        "raw_batch_ids": [19],
    }
    draft.update(overrides)
    return {"observation_draft": draft}


def run(argv):
    buf = io.StringIO()
    err = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(err):
        code = MOD.run(argv)
    return code, buf.getvalue()


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def write(self, name, obj):
        path = self.tmp / name
        path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        return str(path)

    def write_text(self, name, text):
        path = self.tmp / name
        path.write_text(text, encoding="utf-8")
        return str(path)

    def draft(self, phase_obj, *extra):
        p = self.write("phase.json", phase_obj)
        argv = [
            "draft", "--phase-json", p,
            "--source", "RUTA_RUTERO",
            "--effective-week-start", "2026-06-08",
            "--operation-type", "PREFLIGHT",
            "--recorded-at", "2026-06-13T15:00:00Z",
            "--recorded-by", "USER_AUTHORITY",
            *extra,
        ]
        code, out = run(argv)
        return code, out


class ContractTests(Base):
    def test_01_contract_loads_and_matches(self):
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(set(contract["required_fields"]), set(MOD.REQUIRED_FIELDS))
        self.assertEqual(set(contract["optional_fields"]), set(MOD.OPTIONAL_FIELDS))
        self.assertEqual(
            set(contract["label_contract"]["allowed_values"]), set(MOD.ALLOWED_LABELS)
        )

    def test_02_ledger_not_modified_by_runs(self):
        before = hashlib.sha256(LEDGER_PATH.read_bytes()).hexdigest()
        self.draft(shape_b())
        p = self.write("phase.json", shape_b())
        run(["draft", "--phase-json", p, "--source", "RUTA_RUTERO",
             "--effective-week-start", "2026-06-08", "--operation-type", "DRY_RUN",
             "--recorded-at", "2026-06-13T15:00:00Z", "--recorded-by", "USER_AUTHORITY"])
        after = hashlib.sha256(LEDGER_PATH.read_bytes()).hexdigest()
        self.assertEqual(before, after)

    def test_45_ledger_byte_identical(self):
        before = LEDGER_PATH.read_bytes()
        self.draft(shape_a())
        self.assertEqual(before, LEDGER_PATH.read_bytes())


class ShapeTests(Base):
    def test_03_draft_from_observation_draft(self):
        code, out = self.draft(shape_a())
        self.assertEqual(code, 0)
        rec = json.loads(out)
        self.assertEqual(rec["input_file_sha256"], SHA_A)
        self.assertEqual(rec["batch_id"], 7)

    def test_04_draft_from_supported_dry_run(self):
        code, out = self.draft(shape_b())
        self.assertEqual(code, 0)
        rec = json.loads(out)
        self.assertEqual(rec["exact_duplicate_rows"], 3)   # mapped from excess
        self.assertEqual(rec["grain_duplicate_rows"], 1)   # mapped from groups
        self.assertEqual(rec["db_write_executed"], False)  # mapped from writes_executed

    def test_05_unknown_shape_rejected(self):
        code, out = self.draft({"foo": 1, "bar": 2})
        self.assertEqual(code, 1)
        payload = json.loads(out)
        self.assertEqual(payload["error"], "unsupported_input_shape")
        self.assertIn("observation_draft", payload["supported_shapes"])

    def test_06_invalid_json_rejected(self):
        p = self.write_text("bad.json", "{ not json")
        code, out = run([
            "draft", "--phase-json", p, "--source", "RUTA_RUTERO",
            "--effective-week-start", "2026-06-08", "--operation-type", "PREFLIGHT",
            "--recorded-at", "2026-06-13T15:00:00Z", "--recorded-by", "USER_AUTHORITY",
        ])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "invalid_json")

    def test_07_oversized_input_rejected(self):
        p = self.tmp / "big.json"
        p.write_bytes(b"a" * (MOD.MAX_INPUT_BYTES + 10))
        code, out = run([
            "draft", "--phase-json", str(p), "--source", "RUTA_RUTERO",
            "--effective-week-start", "2026-06-08", "--operation-type", "PREFLIGHT",
            "--recorded-at", "2026-06-13T15:00:00Z", "--recorded-by", "USER_AUTHORITY",
        ])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "oversized_input")


class FieldValidationTests(Base):
    def test_08_invalid_source_rejected(self):
        p = self.write("phase.json", shape_b())
        code, out = run([
            "draft", "--phase-json", p, "--source", "NOT_A_SOURCE",
            "--effective-week-start", "2026-06-08", "--operation-type", "PREFLIGHT",
            "--recorded-at", "2026-06-13T15:00:00Z", "--recorded-by", "USER_AUTHORITY",
        ])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "invalid_source")

    def test_09_week_must_be_monday(self):
        p = self.write("phase.json", shape_b())
        code, out = run([
            "draft", "--phase-json", p, "--source", "RUTA_RUTERO",
            "--effective-week-start", "2026-06-09", "--operation-type", "PREFLIGHT",
            "--recorded-at", "2026-06-13T15:00:00Z", "--recorded-by", "USER_AUTHORITY",
        ])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "effective_week_start_must_be_monday")

    def test_10_recorded_at_requires_timezone(self):
        p = self.write("phase.json", shape_b())
        code, out = run([
            "draft", "--phase-json", p, "--source", "RUTA_RUTERO",
            "--effective-week-start", "2026-06-08", "--operation-type", "PREFLIGHT",
            "--recorded-at", "2026-06-13T15:00:00", "--recorded-by", "USER_AUTHORITY",
        ])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "recorded_at_requires_timezone")

    def test_24_role_allowed(self):
        code, out = self.draft(shape_b(), "--reviewed-by", "CODEX_VALIDATOR")
        self.assertEqual(code, 0)
        rec = json.loads(out)
        self.assertEqual(rec["recorded_by"], "USER_AUTHORITY")
        self.assertEqual(rec["reviewed_by"], "CODEX_VALIDATOR")

    def test_25_personal_name_rejected(self):
        p = self.write("phase.json", shape_b())
        code, out = run([
            "draft", "--phase-json", p, "--source", "RUTA_RUTERO",
            "--effective-week-start", "2026-06-08", "--operation-type", "PREFLIGHT",
            "--recorded-at", "2026-06-13T15:00:00Z", "--recorded-by", "John Doe",
        ])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "invalid_role")

    def test_26_email_rejected(self):
        p = self.write("phase.json", shape_b())
        code, out = run([
            "draft", "--phase-json", p, "--source", "RUTA_RUTERO",
            "--effective-week-start", "2026-06-08", "--operation-type", "PREFLIGHT",
            "--recorded-at", "2026-06-13T15:00:00Z", "--recorded-by", "a@b.com",
        ])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "invalid_role")


class OutputContractTests(Base):
    def test_11_observation_id_deterministic(self):
        code, out = self.draft(shape_b())
        rec = json.loads(out)
        expected = MOD.make_observation_id("RUTA_RUTERO", "2026-06-08", "PREFLIGHT", SHA_A)
        self.assertEqual(rec["observation_id"], expected)
        self.assertTrue(rec["observation_id"].startswith(
            "LOADOBS-RUTA_RUTERO-2026-06-08-PREFLIGHT-"))

    def test_12_rerun_byte_identical(self):
        _, out1 = self.draft(shape_b())
        _, out2 = self.draft(shape_b())
        self.assertEqual(out1, out2)

    def test_13_all_required_fields_present(self):
        _, out = self.draft(shape_b())
        rec = json.loads(out)
        for field in MOD.REQUIRED_FIELDS:
            self.assertIn(field, rec)

    def test_14_unknowns_stay_null(self):
        _, out = self.draft(shape_b())  # no post_load_validation/missing_required
        rec = json.loads(out)
        self.assertIsNone(rec["post_load_validation_status"])
        self.assertIsNone(rec["missing_required_rows"])

    def test_15_unrecognized_optionals_not_propagated(self):
        _, out = self.draft(shape_b(weird_unknown=123))
        rec = json.loads(out)
        self.assertNotIn("weird_unknown", rec)
        self.assertEqual(rec["batch_id"], 42)  # recognized optional kept

    def test_16_implementation_authorized_always_false(self):
        _, out = self.draft(shape_b())
        self.assertIs(json.loads(out)["implementation_authorized"], False)

    def test_17_default_label_unreviewed(self):
        _, out = self.draft(shape_b())
        self.assertEqual(json.loads(out)["anomaly_label"], "UNREVIEWED")

    def test_37_stdout_is_valid_json(self):
        _, out = self.draft(shape_b())
        json.loads(out)  # raises if invalid


class LabelTests(Base):
    def test_18_clean_without_postcheck_rejected(self):
        code, out = self.draft(
            shape_b(loader_executed=True, writes_executed=True),
            "--operation-type", "APPLY", "--label", "CLEAN",
        )
        # operation-type overridden by extra after base; argparse takes the last value
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "label_requirements_not_met")

    def test_19_clean_valid_accepted(self):
        code, out = self.draft(
            shape_b(loader_executed=True, writes_executed=True,
                    post_load_validation_status="OK"),
            "--operation-type", "APPLY", "--label", "CLEAN",
        )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["anomaly_label"], "CLEAN")

    def test_20_expected_change_requires_reason_and_evidence(self):
        code_bad, _ = self.draft(shape_b(), "--label", "EXPECTED_CHANGE",
                                 "--anomaly-reason", "expected baja")
        self.assertEqual(code_bad, 1)
        code_ok, out = self.draft(shape_b(), "--label", "EXPECTED_CHANGE",
                                  "--anomaly-reason", "expected baja",
                                  "--evidence-ref", "commit:abcdef1")
        self.assertEqual(code_ok, 0)
        self.assertEqual(json.loads(out)["anomaly_label"], "EXPECTED_CHANGE")

    def test_21_anomalous_requires_reason_and_evidence(self):
        code_bad, out_bad = self.draft(shape_b(), "--label", "ANOMALOUS",
                                       "--evidence-ref", "commit:abcdef1")
        self.assertEqual(code_bad, 1)  # no reason
        code_ok, _ = self.draft(shape_b(), "--label", "ANOMALOUS",
                                "--anomaly-reason", "row count spike",
                                "--evidence-ref", "commit:abcdef1")
        self.assertEqual(code_ok, 0)

    def test_22_load_failure_requires_loader_executed(self):
        code_bad, _ = self.draft(
            shape_b(loader_executed=False), "--operation-type", "APPLY",
            "--label", "LOAD_FAILURE", "--anomaly-reason", "crash",
            "--evidence-ref", "phase:CLAUDE_X",
        )
        self.assertEqual(code_bad, 1)
        code_ok, _ = self.draft(
            shape_b(loader_executed=True), "--operation-type", "APPLY",
            "--label", "LOAD_FAILURE", "--anomaly-reason", "crash",
            "--evidence-ref", "phase:CLAUDE_X",
        )
        self.assertEqual(code_ok, 0)

    def test_23_post_load_regression_requires_failed_postcheck(self):
        code_bad, _ = self.draft(
            shape_b(loader_executed=True, post_load_validation_status="OK"),
            "--operation-type", "APPLY", "--label", "POST_LOAD_REGRESSION",
            "--evidence-ref", "report:r1",
        )
        self.assertEqual(code_bad, 1)
        code_ok, _ = self.draft(
            shape_b(loader_executed=True, post_load_validation_status="FAILED"),
            "--operation-type", "APPLY", "--label", "POST_LOAD_REGRESSION",
            "--evidence-ref", "report:r1",
        )
        self.assertEqual(code_ok, 0)


class PrivacyTests(Base):
    def _expect_privacy(self, phase_obj, category):
        code, out = self.draft(phase_obj)
        self.assertEqual(code, 1)
        payload = json.loads(out)
        self.assertEqual(payload["error"], "privacy_violation_detected")
        self.assertEqual(payload["category"], category)

    def test_27_dsn_rejected(self):
        self._expect_privacy(shape_b(conn="postgresql://u:p@h/db"), "DSN")

    def test_28_secret_key_rejected(self):
        self._expect_privacy(shape_b(api_key="x"), "CREDENTIAL")

    def test_29_payload_json_rejected(self):
        self._expect_privacy(shape_b(payload_json={"x": 1}), "PAYLOAD")

    def test_30_person_client_store_fields_rejected(self):
        self._expect_privacy(shape_b(cliente="ANON"), "PERSONAL_FIELD")

    def test_31_row_arrays_rejected(self):
        self._expect_privacy(shape_b(rows=[{"a": 1}, {"a": 2}]), "ROW_COLLECTION")


class EvidenceRefTests(Base):
    def test_32_commit_ref_valid(self):
        code, out = self.draft(shape_b(), "--label", "ANOMALOUS",
                               "--anomaly-reason", "x",
                               "--evidence-ref", "commit:abcdef1234")
        self.assertEqual(code, 0)
        self.assertIn("commit:abcdef1234", json.loads(out)["evidence_refs"])

    def test_33_research_ref_valid(self):
        code, out = self.draft(
            shape_b(), "--label", "ANOMALOUS", "--anomaly-reason", "x",
            "--evidence-ref", "research/AI_LOAD_OBSERVATION_CONTRACT.json")
        self.assertEqual(code, 0)

    def test_34_absolute_path_rejected(self):
        code, out = self.draft(shape_b(), "--evidence-ref", "/etc/passwd")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "invalid_evidence_ref")

    def test_35_data_path_rejected(self):
        code, out = self.draft(shape_b(), "--evidence-ref", "data/secret.xlsx")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "invalid_evidence_ref")

    def test_36_url_rejected(self):
        code, out = self.draft(shape_b(), "--evidence-ref", "http://example.com")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "invalid_evidence_ref")


class ValidateSubcommandTests(Base):
    def test_validate_on_generated_candidate(self):
        _, out = self.draft(shape_b())
        rec = json.loads(out)
        rec_path = self.write("candidate.json", rec)
        code, vout = run(["validate", "--record", rec_path])
        self.assertEqual(code, 0)
        payload = json.loads(vout)
        self.assertEqual(payload["validate"], "ok")
        self.assertEqual(payload["record"]["observation_id"], rec["observation_id"])

    def test_validate_rejects_unknown_field(self):
        _, out = self.draft(shape_b())
        rec = json.loads(out)
        rec["surprise"] = 1
        rec_path = self.write("candidate.json", rec)
        code, vout = run(["validate", "--record", rec_path])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(vout)["error"], "unknown_fields")


class SourceSafetyTests(unittest.TestCase):
    def setUp(self):
        self.src = SCRIPT_PATH.read_text(encoding="utf-8")

    def test_38_no_network_or_db_imports(self):
        for forbidden in (
            "import socket", "import urllib", "import requests", "http.client",
            "import psycopg", "psycopg2", "import sqlite3", "sqlalchemy", "import ssl",
        ):
            self.assertNotIn(forbidden, self.src)

    def test_39_no_subprocess(self):
        self.assertNotIn("import subprocess", self.src)
        self.assertNotIn("subprocess.", self.src)

    def test_40_no_shell_true(self):
        self.assertNotIn("shell=True", self.src)

    def test_41_no_file_writes(self):
        self.assertNotIn("write_text(", self.src)
        self.assertNotIn("write_bytes(", self.src)
        self.assertNotIn("open(", self.src)


class SkillTests(unittest.TestCase):
    def setUp(self):
        self.text = SKILL_PATH.read_text(encoding="utf-8")

    def test_42_skill_frontmatter_valid(self):
        self.assertTrue(self.text.startswith("---"))
        self.assertIn("name: sz-load-observation", self.text)
        self.assertLessEqual(len(self.text.splitlines()), 50)

    def test_43_disable_model_invocation_true(self):
        self.assertIn("disable-model-invocation: true", self.text)

    def test_44_allowed_tools_only_the_script(self):
        frontmatter = self.text.split("---")[1]
        bullets = [ln for ln in frontmatter.splitlines() if ln.strip().startswith("- ")]
        self.assertEqual(len(bullets), 1)
        self.assertIn("Bash(python scripts/sz_load_observation.py *)", bullets[0])


if __name__ == "__main__":
    unittest.main()
