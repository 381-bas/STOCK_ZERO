#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Deterministic load-observation candidate preparation and validation.

This tool turns a SAFE technical result of a load phase into a normalized
observation candidate that conforms to research/AI_LOAD_OBSERVATION_CONTRACT.json.

Hard guarantees:
  * Standard library only. No network, no DB, no subprocess, no shell.
  * No file writes. Output goes to stdout only. The ledger is never touched.
  * Deterministic: same inputs -> byte-identical stdout. No now(), no randomness.
  * No anomaly label is ever inferred. Default label is UNREVIEWED.
  * Unknown input keys are not propagated. Unobserved fields stay null.
  * implementation_authorized is always false and cannot be overridden.

No observation produced here authorizes implementation, production, DB writes,
loaders, or ledger insertion. Ledger insertion is a separate, explicitly
authorized phase (Codex validation + Bastian authorization).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path

MAX_INPUT_BYTES = 5 * 1024 * 1024
DEFAULT_LABEL = "UNREVIEWED"
OBS_ID_HASH_PREFIX = 12

ALLOWED_SOURCES = ("RUTA_RUTERO", "KPIONE2", "POWER_APP", "KPIONE")
ALLOWED_OPERATION_TYPES = (
    "SOURCE_CHECK", "PREFLIGHT", "DRY_RUN", "APPLY", "POST_LOAD_VALIDATION", "ROLLBACK",
)
ALLOWED_LABELS = (
    "UNREVIEWED", "CLEAN", "EXPECTED_CHANGE", "ANOMALOUS",
    "INVALID_INPUT", "LOAD_FAILURE", "POST_LOAD_REGRESSION",
)
ALLOWED_ROLES = (
    "USER_AUTHORITY", "CLAUDE_RESEARCHER", "CODEX_EXECUTOR",
    "CODEX_VALIDATOR", "CHATGPT_GOVERNOR", "SYSTEM_TOOL",
)

# Exact mirror of AI_LOAD_OBSERVATION_CONTRACT.json (verified by the test suite).
REQUIRED_FIELDS = (
    "observation_id", "recorded_at", "source", "effective_week_start",
    "operation_type", "input_file_name", "input_file_sha256", "schema_signature",
    "input_rows", "accepted_rows", "rejected_rows", "exact_duplicate_rows",
    "grain_duplicate_rows", "missing_required_rows", "source_check_verdict",
    "loader_executed", "db_write_executed", "post_load_validation_status",
    "anomaly_label", "anomaly_reason", "evidence_refs", "recorded_by",
    "reviewed_by", "implementation_authorized",
)
OPTIONAL_FIELDS = (
    "batch_id", "route_batch_id", "raw_batch_ids", "minimum_date", "maximum_date",
    "affected_weeks", "new_key_count", "removed_key_count", "changed_key_count",
    "parity_missing_keys", "parity_extra_keys", "parity_value_differences",
    "rollback_required", "rollback_executed", "rework_rounds", "elapsed_minutes",
    "notes",
)

# Required fields populated from the phase JSON (technical metrics only).
TECHNICAL_FIELDS = (
    "input_file_name", "input_file_sha256", "schema_signature", "input_rows",
    "accepted_rows", "rejected_rows", "exact_duplicate_rows", "grain_duplicate_rows",
    "missing_required_rows", "source_check_verdict", "loader_executed",
    "db_write_executed", "post_load_validation_status",
)

# SHAPE B (flat dry-run) accepted aliases per canonical technical field.
SHAPE_B_ALIASES = {
    "exact_duplicate_rows": ("exact_duplicate_rows", "exact_duplicate_excess"),
    "grain_duplicate_rows": ("grain_duplicate_rows", "grain_duplicate_groups"),
    "db_write_executed": ("db_write_executed", "writes_executed"),
}

SHAPE_B_DETECT_KEYS = (
    "input_file_sha256", "schema_signature", "input_rows", "accepted_rows",
    "exact_duplicate_excess", "grain_duplicate_groups", "writes_executed",
    "exact_duplicate_rows", "grain_duplicate_rows", "db_write_executed",
    "source_check_verdict", "loader_executed",
)

APPROVED_STATUS = {"OK", "PASS", "PASSED", "APPROVED", "SUCCESS"}
FAILED_STATUS = {"FAIL", "FAILED", "ERROR", "REGRESSION", "BLOCK", "BLOCKED"}

# Privacy: substring categories scanned on raw input and serialized output.
PRIVACY_SUBSTRINGS = (
    ("DSN", ("postgresql://", "postgres://")),
    ("CREDENTIAL", (
        "password", "credential", "secret", "api_key", "apikey", "access_key",
        "accesskey", "refresh_token", "access_token", "auth_token", "private_key",
    )),
    ("ENV_PATH", (".env", ".local_secrets")),
    ("PAYLOAD", ("payload_json",)),
)
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
PERSONAL_KEY_RE = re.compile(
    r"^(cliente|local|tienda|gestor|rutero|reponedor|supervisor|persona|store)(s)?(_|$)",
    re.IGNORECASE,
)

EVIDENCE_COMMIT_RE = re.compile(r"^commit:[0-9a-fA-F]{7,40}$")
EVIDENCE_PHASE_RE = re.compile(r"^phase:[A-Za-z0-9_]+$")
EVIDENCE_RESEARCH_RE = re.compile(r"^research/[A-Za-z0-9_][A-Za-z0-9_./\-]*$")
EVIDENCE_REPORT_RE = re.compile(r"^report:[A-Za-z0-9_][A-Za-z0-9_.\-]*$")


class ObsError(Exception):
    def __init__(self, code: str, **extra):
        super().__init__(code)
        self.code = code
        self.extra = extra


# -----------------------------
# Privacy scanning
# -----------------------------
def _scan_text(text: str) -> None:
    low = text.lower()
    for category, needles in PRIVACY_SUBSTRINGS:
        for needle in needles:
            if needle in low:
                raise ObsError("privacy_violation_detected", category=category)
    if EMAIL_RE.search(text):
        raise ObsError("privacy_violation_detected", category="EMAIL")


def _scan_structure(obj) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            ks = str(key)
            if ks.lower() == "payload_json":
                raise ObsError("privacy_violation_detected", category="PAYLOAD")
            if PERSONAL_KEY_RE.match(ks):
                raise ObsError("privacy_violation_detected", category="PERSONAL_FIELD")
            _scan_structure(value)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                raise ObsError("privacy_violation_detected", category="ROW_COLLECTION")
        for item in obj:
            _scan_structure(item)


def privacy_scan(raw_text: str, obj) -> None:
    _scan_text(raw_text)
    _scan_structure(obj)


# -----------------------------
# Field validators
# -----------------------------
def validate_source(value: str) -> str:
    if value not in ALLOWED_SOURCES:
        raise ObsError("invalid_source", allowed=list(ALLOWED_SOURCES))
    return value


def validate_operation_type(value: str) -> str:
    if value not in ALLOWED_OPERATION_TYPES:
        raise ObsError("invalid_operation_type", allowed=list(ALLOWED_OPERATION_TYPES))
    return value


def validate_label(value: str) -> str:
    if value not in ALLOWED_LABELS:
        raise ObsError("invalid_label", allowed=list(ALLOWED_LABELS))
    return value


def validate_role(value, *, field: str, allow_null: bool):
    if value is None or value == "":
        if allow_null:
            return None
        raise ObsError("missing_role", field=field)
    if value not in ALLOWED_ROLES:
        raise ObsError("invalid_role", field=field, allowed=list(ALLOWED_ROLES))
    return value


def validate_effective_week_start(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ObsError("invalid_effective_week_start_format")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ObsError("invalid_effective_week_start") from exc
    if parsed.weekday() != 0:
        raise ObsError("effective_week_start_must_be_monday")
    return value


def validate_recorded_at(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ObsError("invalid_recorded_at")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ObsError("invalid_recorded_at") from exc
    if parsed.tzinfo is None:
        raise ObsError("recorded_at_requires_timezone")
    return value


def validate_evidence_ref(ref: str) -> str:
    if not isinstance(ref, str) or not ref:
        raise ObsError("invalid_evidence_ref")
    if any(ch.isspace() for ch in ref) or ".." in ref or "\\" in ref:
        raise ObsError("invalid_evidence_ref")
    if (
        EVIDENCE_COMMIT_RE.match(ref)
        or EVIDENCE_PHASE_RE.match(ref)
        or EVIDENCE_REPORT_RE.match(ref)
    ):
        return ref
    if EVIDENCE_RESEARCH_RE.match(ref):
        if ref.startswith("research/data/") or "/data/" in ref or "evidence/" in ref:
            raise ObsError("invalid_evidence_ref")
        return ref
    raise ObsError("invalid_evidence_ref")


def validate_evidence_refs(refs):
    if refs is None:
        return []
    if not isinstance(refs, list):
        raise ObsError("invalid_evidence_refs")
    return [validate_evidence_ref(r) for r in refs]


# -----------------------------
# Label requirement enforcement
# -----------------------------
def enforce_label_requirements(record) -> None:
    label = record["anomaly_label"]
    op = record["operation_type"]
    loader_executed = record["loader_executed"]
    db_write = record["db_write_executed"]
    status = record["post_load_validation_status"]
    reason = record["anomaly_reason"]
    notes = record.get("notes")
    evidence = record["evidence_refs"]
    status_up = str(status).upper() if status is not None else None

    def fail(detail: str) -> None:
        raise ObsError("label_requirements_not_met", label=label, detail=detail)

    if label == "UNREVIEWED":
        return
    if label == "CLEAN":
        if loader_executed is not True:
            fail("loader_executed must be true")
        if status is None or status_up not in APPROVED_STATUS:
            fail("post_load_validation_status must be approved")
        if op == "APPLY" and db_write is not True:
            fail("APPLY requires db_write_executed true")
        if op in ("SOURCE_CHECK", "PREFLIGHT", "DRY_RUN") and db_write is not False:
            fail("non-apply operation requires db_write_executed false")
        return
    if label == "EXPECTED_CHANGE":
        if not (reason or notes):
            fail("anomaly_reason or notes required")
        if not evidence:
            fail("evidence_refs required")
        return
    if label == "ANOMALOUS":
        if not reason:
            fail("anomaly_reason required")
        if not evidence:
            fail("evidence_refs required")
        return
    if label == "INVALID_INPUT":
        if not evidence:
            fail("evidence_refs required")
        if not (reason or notes):
            fail("anomaly_reason or notes required")
        return
    if label == "LOAD_FAILURE":
        if loader_executed is not True:
            fail("loader_executed must be true")
        if not evidence:
            fail("evidence_refs required")
        if not (reason or notes):
            fail("anomaly_reason or notes required")
        return
    if label == "POST_LOAD_REGRESSION":
        if loader_executed is not True:
            fail("loader_executed must be true")
        if status is None or status_up not in FAILED_STATUS:
            fail("post_load_validation_status must indicate failure")
        if not evidence:
            fail("evidence_refs required")
        return


# -----------------------------
# Identity
# -----------------------------
def make_observation_id(source: str, week: str, operation_type: str, sha: str | None) -> str:
    raw = "|".join([source, week, operation_type, str(sha or "")])
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:OBS_ID_HASH_PREFIX]
    return "LOADOBS-%s-%s-%s-%s" % (source, week, operation_type, digest)


# -----------------------------
# Input shape handling
# -----------------------------
def detect_shape(obj):
    if not isinstance(obj, dict):
        raise ObsError(
            "unsupported_input_shape",
            supported_shapes=["observation_draft", "dry_run_aggregate"],
        )
    draft = obj.get("observation_draft")
    if isinstance(draft, dict):
        return "A", draft
    if any(key in obj for key in SHAPE_B_DETECT_KEYS):
        return "B", obj
    raise ObsError(
        "unsupported_input_shape",
        supported_shapes=["observation_draft", "dry_run_aggregate"],
    )


def extract_technical(shape: str, src: dict) -> dict:
    tech = {}
    for field in TECHNICAL_FIELDS:
        if shape == "A":
            tech[field] = src.get(field)
            continue
        value = None
        for key in SHAPE_B_ALIASES.get(field, (field,)):
            if key in src and src[key] is not None:
                value = src[key]
                break
        tech[field] = value
    return tech


def extract_optionals(src: dict, notes: str | None) -> dict:
    opt = {}
    for field in OPTIONAL_FIELDS:
        if field == "notes":
            continue
        value = src.get(field)
        if value is not None:
            opt[field] = value
    if notes:
        opt["notes"] = notes
    return opt


def assemble_record(*, source, week, operation_type, recorded_at, recorded_by,
                    reviewed_by, label, anomaly_reason, evidence_refs, tech, optionals):
    obs_id = make_observation_id(source, week, operation_type, tech.get("input_file_sha256"))
    values = {
        "observation_id": obs_id,
        "recorded_at": recorded_at,
        "source": source,
        "effective_week_start": week,
        "operation_type": operation_type,
        "anomaly_label": label,
        "anomaly_reason": anomaly_reason,
        "evidence_refs": evidence_refs,
        "recorded_by": recorded_by,
        "reviewed_by": reviewed_by,
        "implementation_authorized": False,
    }
    values.update(tech)
    record = {}
    for field in REQUIRED_FIELDS:
        record[field] = values.get(field)
    record["implementation_authorized"] = False  # fixed, no override
    for field in OPTIONAL_FIELDS:
        if field in optionals and optionals[field] is not None:
            record[field] = optionals[field]
    return record


# -----------------------------
# Subcommands
# -----------------------------
def read_json_path(path_value: str) -> tuple[str, object]:
    path = Path(path_value)
    if not path.is_file():
        raise ObsError("input_not_found")
    if path.stat().st_size > MAX_INPUT_BYTES:
        raise ObsError("oversized_input", max_bytes=MAX_INPUT_BYTES)
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ObsError("invalid_json") from exc
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ObsError("invalid_json") from exc
    return text, obj


def cmd_draft(args) -> dict:
    source = validate_source(args.source)
    week = validate_effective_week_start(args.effective_week_start)
    operation_type = validate_operation_type(args.operation_type)
    recorded_at = validate_recorded_at(args.recorded_at)
    recorded_by = validate_role(args.recorded_by, field="recorded_by", allow_null=False)
    reviewed_by = validate_role(args.reviewed_by, field="reviewed_by", allow_null=True)
    label = validate_label(args.label) if args.label else DEFAULT_LABEL
    evidence_refs = validate_evidence_refs(list(args.evidence_ref or []))
    anomaly_reason = args.anomaly_reason if args.anomaly_reason else None

    raw_text, obj = read_json_path(args.phase_json)
    privacy_scan(raw_text, obj)

    shape, technical_src = detect_shape(obj)
    tech = extract_technical(shape, technical_src)
    optionals = extract_optionals(technical_src, args.notes if args.notes else None)

    record = assemble_record(
        source=source, week=week, operation_type=operation_type,
        recorded_at=recorded_at, recorded_by=recorded_by, reviewed_by=reviewed_by,
        label=label, anomaly_reason=anomaly_reason, evidence_refs=evidence_refs,
        tech=tech, optionals=optionals,
    )
    enforce_label_requirements(record)
    privacy_scan(json.dumps(record, ensure_ascii=False), record)
    return record


def validate_record(record) -> dict:
    if not isinstance(record, dict):
        raise ObsError("invalid_record")
    allowed = set(REQUIRED_FIELDS) | set(OPTIONAL_FIELDS)
    extra = sorted(set(record) - allowed)
    if extra:
        raise ObsError("unknown_fields", fields=extra)
    missing = sorted(set(REQUIRED_FIELDS) - set(record))
    if missing:
        raise ObsError("missing_required_fields", fields=missing)
    if record["implementation_authorized"] is not False:
        raise ObsError("implementation_authorized_must_be_false")

    source = validate_source(record["source"])
    week = validate_effective_week_start(record["effective_week_start"])
    operation_type = validate_operation_type(record["operation_type"])
    validate_recorded_at(record["recorded_at"])
    validate_role(record["recorded_by"], field="recorded_by", allow_null=False)
    validate_role(record["reviewed_by"], field="reviewed_by", allow_null=True)
    validate_label(record["anomaly_label"])
    validate_evidence_refs(record["evidence_refs"])

    expected_id = make_observation_id(
        source, week, operation_type, record["input_file_sha256"]
    )
    if record["observation_id"] != expected_id:
        raise ObsError("observation_id_mismatch")

    enforce_label_requirements(record)
    privacy_scan(json.dumps(record, ensure_ascii=False), record)
    return record


def cmd_validate(args) -> dict:
    _raw, obj = read_json_path(args.record)
    privacy_scan(_raw, obj)
    record = validate_record(obj)
    return {"validate": "ok", "observation_id": record["observation_id"], "record": record}


# -----------------------------
# CLI
# -----------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sz_load_observation", add_help=True)
    sub = parser.add_subparsers(dest="command", required=True)

    draft = sub.add_parser("draft")
    draft.add_argument("--phase-json", required=True)
    draft.add_argument("--source", required=True)
    draft.add_argument("--effective-week-start", required=True)
    draft.add_argument("--operation-type", required=True)
    draft.add_argument("--recorded-at", required=True)
    draft.add_argument("--recorded-by", required=True)
    draft.add_argument("--reviewed-by", default=None)
    draft.add_argument("--label", default=None)
    draft.add_argument("--anomaly-reason", default=None)
    draft.add_argument("--evidence-ref", action="append", default=None)
    draft.add_argument("--notes", default=None)
    draft.add_argument("--pretty", action="store_true")

    validate = sub.add_parser("validate")
    validate.add_argument("--record", required=True)
    validate.add_argument("--pretty", action="store_true")
    return parser


def emit(obj, pretty: bool) -> None:
    if pretty:
        text = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=False)
    else:
        text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
    sys.stdout.write(text + "\n")


def run(argv) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    pretty = bool(getattr(args, "pretty", False))
    try:
        if args.command == "draft":
            result = cmd_draft(args)
        else:
            result = cmd_validate(args)
    except ObsError as exc:
        payload = {"error": exc.code}
        payload.update(exc.extra)
        emit(payload, pretty)
        return 1
    emit(result, pretty)
    return 0


def main(argv=None) -> int:
    return run(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    sys.exit(main())
