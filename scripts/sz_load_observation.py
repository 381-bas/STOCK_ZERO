#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Deterministic load-observation candidate preparation and validation.

This tool turns a safe technical result of a load phase into a normalized
observation candidate that conforms to research/AI_LOAD_OBSERVATION_CONTRACT.json.

Hard guarantees:
  * Standard library only. No network, DB, subprocess, shell, or file writes.
  * Input paths are scope-checked before opening.
  * Output goes to stdout only. The ledger is never touched.
  * Deterministic: same inputs -> byte-identical stdout.
  * No anomaly label is inferred. Default label is UNREVIEWED.
  * Unknown input keys are not propagated. Unobserved fields stay null.
  * implementation_authorized is always false and cannot be overridden.

No observation produced here authorizes implementation, production, DB writes,
loaders, or ledger insertion.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


MAX_INPUT_BYTES = 5 * 1024 * 1024
MAX_EVIDENCE_REF_LEN = 240
DEFAULT_LABEL = "UNREVIEWED"
OBS_ID_HASH_PREFIX = 12

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_ALLOWED_INPUT_ROOTS: list[Path] = []

ALLOWED_SOURCES = ("RUTA_RUTERO", "KPIONE2", "POWER_APP", "KPIONE")
ALLOWED_OPERATION_TYPES = (
    "SOURCE_CHECK",
    "PREFLIGHT",
    "DRY_RUN",
    "APPLY",
    "POST_LOAD_VALIDATION",
    "ROLLBACK",
)
ALLOWED_LABELS = (
    "UNREVIEWED",
    "CLEAN",
    "EXPECTED_CHANGE",
    "ANOMALOUS",
    "INVALID_INPUT",
    "LOAD_FAILURE",
    "POST_LOAD_REGRESSION",
)
ALLOWED_ROLES = (
    "USER_AUTHORITY",
    "CLAUDE_RESEARCHER",
    "CODEX_EXECUTOR",
    "CODEX_VALIDATOR",
    "CHATGPT_GOVERNOR",
    "SYSTEM_TOOL",
)

REQUIRED_FIELDS = (
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
)
OPTIONAL_FIELDS = (
    "batch_id",
    "route_batch_id",
    "raw_batch_ids",
    "minimum_date",
    "maximum_date",
    "affected_weeks",
    "new_key_count",
    "removed_key_count",
    "changed_key_count",
    "parity_missing_keys",
    "parity_extra_keys",
    "parity_value_differences",
    "rollback_required",
    "rollback_executed",
    "rework_rounds",
    "elapsed_minutes",
    "notes",
)
TECHNICAL_FIELDS = (
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
)

SHAPE_A_REQUIRED = TECHNICAL_FIELDS
SHAPE_B_REQUIRED = (
    "source",
    "effective_week_start",
    "input_file_sha256",
    "schema_signature",
    "input_rows",
    "accepted_rows",
)
SHAPE_B_ALIASES = {
    "exact_duplicate_rows": ("exact_duplicate_rows", "exact_duplicate_excess"),
    "grain_duplicate_rows": ("grain_duplicate_rows", "grain_duplicate_groups"),
    "db_write_executed": ("db_write_executed", "writes_executed"),
}
SHAPE_B_DETECT_KEYS = (
    "source",
    "effective_week_start",
    "operation_type",
    "input_file_sha256",
    "schema_signature",
    "input_rows",
    "accepted_rows",
    "exact_duplicate_excess",
    "grain_duplicate_groups",
    "writes_executed",
    "exact_duplicate_rows",
    "grain_duplicate_rows",
    "db_write_executed",
    "source_check_verdict",
    "loader_executed",
)

APPROVED_STATUS = {"OK", "PASS", "PASSED", "APPROVED", "SUCCESS"}
FAILED_STATUS = {"FAIL", "FAILED", "ERROR", "REGRESSION", "BLOCK", "BLOCKED"}

TECH_CODE_RE = re.compile(r"^[A-Z][A-Z0-9_:-]{0,119}$")
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
DSN_RE = re.compile(r"\bpostgres(?:ql)?://[^\s\"'<>]+", re.IGNORECASE)
TOKEN_VALUE_RE = re.compile(
    r"\b(?:sk|pk|api|token|key|secret|bearer)[_-]?[A-Za-z0-9]{8,}\b",
    re.IGNORECASE,
)
EVIDENCE_COMMIT_RE = re.compile(r"^commit:[0-9a-fA-F]{7,40}$")
EVIDENCE_PHASE_RE = re.compile(r"^phase:[A-Za-z0-9_]+$")
EVIDENCE_RESEARCH_RE = re.compile(r"^research/[A-Za-z0-9_][A-Za-z0-9_./\-]*$")
EVIDENCE_REPORT_RE = re.compile(r"^report:[A-Za-z0-9_][A-Za-z0-9_.\-]*$")

PERSONAL_KEY_RE = re.compile(
    r"^(cliente|local|tienda|gestor|rutero|reponedor|supervisor|persona|store)(s)?(_|$)",
    re.IGNORECASE,
)
FORBIDDEN_KEY_TOKENS = {
    "password",
    "credential",
    "credentials",
    "secret",
    "secrets",
    "apikey",
    "accesskey",
    "refreshtoken",
    "accesstoken",
    "authtoken",
    "privkey",
    "privatekey",
    "token",
}
SENSITIVE_PATH_NAMES = {
    ".env",
    ".local_secrets",
    "credentials.json",
    "secrets",
    "data",
    "evidence",
}


class ObsError(Exception):
    def __init__(self, code: str, *, category: str = "VALIDATION", **extra):
        super().__init__(code)
        self.code = code
        self.category = category
        self.extra = extra


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ObsError("argument_error", category="ARGPARSE")

    def exit(self, status=0, message=None):
        if status:
            raise ObsError("argument_error", category="ARGPARSE")
        raise ObsError("help_not_supported", category="ARGPARSE")


def register_test_input_root(path: str | Path) -> None:
    TEST_ALLOWED_INPUT_ROOTS.append(Path(path).resolve())


def _normalize_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.lower())


def _contains_symlink(path: Path) -> bool:
    current = path
    parts = []
    while current != current.parent:
        parts.append(current)
        current = current.parent
    for part in reversed(parts):
        try:
            if part.exists() and part.is_symlink():
                return True
        except OSError:
            return True
    return False


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _path_has_sensitive_part(path: Path) -> bool:
    for part in path.parts:
        low = part.lower()
        if low in SENSITIVE_PATH_NAMES:
            return True
        if low.startswith(".env."):
            return True
    return False


def resolve_input_path(path_value: str) -> Path:
    raw = Path(path_value)
    if any(part == ".." for part in raw.parts):
        raise ObsError("unsafe_input_path", category="PATH", reason="traversal")
    if _path_has_sensitive_part(raw):
        raise ObsError("unsafe_input_path", category="PATH", reason="sensitive_path")

    if raw.is_absolute():
        candidate = raw
    else:
        if not raw.parts or raw.parts[0] not in {"research", "tests"}:
            raise ObsError("unsafe_input_path", category="PATH", reason="outside_allowed_roots")
        if raw.parts[0] == "tests" and (len(raw.parts) < 2 or raw.parts[1] != "fixtures"):
            raise ObsError("unsafe_input_path", category="PATH", reason="outside_allowed_roots")
        candidate = REPO_ROOT / raw

    try:
        resolved = candidate.resolve(strict=False)
    except OSError as exc:
        raise ObsError("unsafe_input_path", category="PATH", reason="resolve_failed") from exc

    allowed = False
    if _is_relative_to(resolved, REPO_ROOT):
        rel = resolved.relative_to(REPO_ROOT)
        if rel.parts and rel.parts[0] == "research":
            allowed = True
        if len(rel.parts) >= 2 and rel.parts[0] == "tests" and rel.parts[1] == "fixtures":
            allowed = True
    for root in TEST_ALLOWED_INPUT_ROOTS:
        if _is_relative_to(resolved, root):
            allowed = True
            break

    if not allowed:
        raise ObsError("unsafe_input_path", category="PATH", reason="outside_allowed_roots")
    if _path_has_sensitive_part(resolved):
        raise ObsError("unsafe_input_path", category="PATH", reason="sensitive_path")
    if _contains_symlink(resolved):
        raise ObsError("unsafe_input_path", category="PATH", reason="symlink")
    return resolved


def read_json_path(path_value: str) -> tuple[str, object]:
    path = resolve_input_path(path_value)
    if not path.is_file():
        raise ObsError("input_not_found", category="PATH")
    if path.stat().st_size > MAX_INPUT_BYTES:
        raise ObsError("oversized_input", category="PATH", max_bytes=MAX_INPUT_BYTES)
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ObsError("invalid_json", category="INPUT") from exc
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ObsError("invalid_json", category="INPUT") from exc
    return text, obj


def _scan_text(text: str) -> None:
    if DSN_RE.search(text):
        raise ObsError("privacy_violation_detected", category="PRIVACY", privacy_category="DSN")
    if EMAIL_RE.search(text):
        raise ObsError("privacy_violation_detected", category="PRIVACY", privacy_category="EMAIL")
    if TOKEN_VALUE_RE.search(text):
        raise ObsError("privacy_violation_detected", category="PRIVACY", privacy_category="CREDENTIAL")
    for match in re.finditer(r"https?://[^\s\"'<>]+", text, re.IGNORECASE):
        parsed = urlparse(match.group(0))
        if parsed.query or parsed.fragment:
            raise ObsError("privacy_violation_detected", category="PRIVACY", privacy_category="URL_QUERY")
    for needle in (".env", ".local_secrets", "credentials.json", "secrets/"):
        if needle in text.lower():
            raise ObsError("privacy_violation_detected", category="PRIVACY", privacy_category="SENSITIVE_PATH")


def _scan_structure(obj) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_text = str(key)
            norm = _normalize_key(key_text)
            if norm == "payloadjson" or "payload" == norm:
                raise ObsError("privacy_violation_detected", category="PRIVACY", privacy_category="PAYLOAD")
            if PERSONAL_KEY_RE.match(key_text):
                raise ObsError("privacy_violation_detected", category="PRIVACY", privacy_category="PERSONAL_FIELD")
            if any(token in norm for token in FORBIDDEN_KEY_TOKENS):
                raise ObsError("privacy_violation_detected", category="PRIVACY", privacy_category="CREDENTIAL_KEY")
            _scan_structure(value)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                raise ObsError("privacy_violation_detected", category="PRIVACY", privacy_category="ROW_COLLECTION")
        for item in obj:
            _scan_structure(item)
    elif isinstance(obj, str):
        _scan_text(obj)


def privacy_scan(raw_text: str, obj) -> None:
    _scan_text(raw_text)
    _scan_structure(obj)


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


def parse_iso_date(value, *, field: str, monday: bool = False, allow_null: bool = True):
    if value is None:
        if allow_null:
            return None
        raise ObsError("missing_date", field=field)
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ObsError("invalid_date_format", field=field)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ObsError("invalid_date", field=field) from exc
    if monday and parsed.weekday() != 0:
        raise ObsError("date_must_be_monday", field=field)
    return value


def validate_effective_week_start(value: str) -> str:
    return parse_iso_date(value, field="effective_week_start", monday=True, allow_null=False)


def parse_recorded_at(value: str) -> tuple[str, str]:
    if not isinstance(value, str) or not value:
        raise ObsError("invalid_recorded_at")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ObsError("invalid_recorded_at") from exc
    if parsed.tzinfo is None:
        raise ObsError("recorded_at_requires_timezone")
    normalized = parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value, normalized


def validate_count(value, *, field: str, allow_null: bool = True):
    if value is None:
        if allow_null:
            return None
        raise ObsError("missing_count", field=field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ObsError("invalid_count_type", field=field)
    if value < 0:
        raise ObsError("invalid_count_negative", field=field)
    return value


def validate_bool(value, *, field: str, allow_null: bool = True):
    if value is None:
        if allow_null:
            return None
        raise ObsError("missing_boolean", field=field)
    if not isinstance(value, bool):
        raise ObsError("invalid_boolean_type", field=field)
    return value


def validate_sha256(value, *, field: str, allow_null: bool = False):
    if value is None:
        if allow_null:
            return None
        raise ObsError("missing_sha256", field=field)
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ObsError("invalid_sha256", field=field)
    return value.lower()


def validate_optional_number(value, *, field: str):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ObsError("invalid_number", field=field)
    if value < 0:
        raise ObsError("invalid_number_negative", field=field)
    return value


def validate_technical_code(value, *, field: str, allow_null: bool = True):
    if value is None or value == "":
        if allow_null:
            return None
        raise ObsError("missing_technical_code", field=field)
    if not isinstance(value, str) or not TECH_CODE_RE.fullmatch(value):
        raise ObsError("invalid_technical_code", field=field)
    return value


def validate_evidence_ref(ref: str) -> str:
    if not isinstance(ref, str) or not ref:
        raise ObsError("invalid_evidence_ref")
    if len(ref) > MAX_EVIDENCE_REF_LEN:
        raise ObsError("invalid_evidence_ref", reason="too_long")
    if any(ch.isspace() for ch in ref) or ".." in ref or "\\" in ref:
        raise ObsError("invalid_evidence_ref")
    if EVIDENCE_COMMIT_RE.match(ref) or EVIDENCE_PHASE_RE.match(ref) or EVIDENCE_REPORT_RE.match(ref):
        return ref
    if EVIDENCE_RESEARCH_RE.match(ref):
        if ref.startswith("research/data/") or "/data/" in ref or "evidence/" in ref:
            raise ObsError("invalid_evidence_ref")
        path = (REPO_ROOT / ref).resolve(strict=False)
        if not _is_relative_to(path, REPO_ROOT):
            raise ObsError("invalid_evidence_ref")
        if not path.is_file():
            raise ObsError("invalid_evidence_ref", reason="research_path_not_found")
        return ref
    raise ObsError("invalid_evidence_ref")


def validate_evidence_refs(refs):
    if refs is None:
        return []
    if not isinstance(refs, list):
        raise ObsError("invalid_evidence_refs")
    return [validate_evidence_ref(r) for r in refs]


def _has_key(src: dict, key: str) -> bool:
    return key in src


def detect_shape(obj):
    if not isinstance(obj, dict):
        raise ObsError("unsupported_input_shape", supported_shapes=["observation_draft", "dry_run_aggregate"])
    draft = obj.get("observation_draft")
    if "observation_draft" in obj:
        if not isinstance(draft, dict):
            raise ObsError("unsupported_input_shape", supported_shapes=["observation_draft", "dry_run_aggregate"])
        return "A", draft, obj
    if any(key in obj for key in SHAPE_B_DETECT_KEYS):
        return "B", obj, obj
    raise ObsError("unsupported_input_shape", supported_shapes=["observation_draft", "dry_run_aggregate"])


def require_shape_complete(shape: str, src: dict) -> None:
    if shape == "A":
        missing = [field for field in SHAPE_A_REQUIRED if field not in src]
        if missing:
            raise ObsError("incomplete_input_shape", fields=missing)
        return
    missing = [field for field in SHAPE_B_REQUIRED if field not in src]
    if not any(alias in src for alias in ("writes_executed", "db_write_executed")):
        missing.append("writes_executed|db_write_executed")
    if missing:
        raise ObsError("incomplete_input_shape", fields=missing)


def assert_no_alias_conflicts(src: dict) -> None:
    for field, aliases in SHAPE_B_ALIASES.items():
        present = [(key, src[key]) for key in aliases if key in src and src[key] is not None]
        if len(present) > 1:
            values = {json.dumps(value, sort_keys=True) for _key, value in present}
            if len(values) > 1:
                raise ObsError("alias_conflict", field=field)


def assert_cli_json_consistency(
    shape: str,
    src: dict,
    *,
    source: str,
    week: str,
    operation_type: str,
) -> None:
    checks = {
        "source": source,
        "effective_week_start": week,
        "operation_type": operation_type,
    }
    for field, expected in checks.items():
        if field in src and src[field] is not None and src[field] != expected:
            raise ObsError("phase_cli_mismatch", field=field)


def extract_technical(shape: str, src: dict) -> dict:
    if shape == "B":
        assert_no_alias_conflicts(src)
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


def make_observation_id(
    source: str,
    week: str,
    operation_type: str,
    sha: str,
    recorded_at_normalized: str,
) -> str:
    raw = "|".join([source, week, operation_type, sha.lower(), recorded_at_normalized])
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:OBS_ID_HASH_PREFIX]
    return "LOADOBS-%s-%s-%s-%s" % (source, week, operation_type, digest)


def assemble_record(*, source, week, operation_type, recorded_at, recorded_at_normalized,
                    recorded_by, reviewed_by, label, anomaly_reason, evidence_refs, tech, optionals):
    obs_id = make_observation_id(
        source,
        week,
        operation_type,
        tech.get("input_file_sha256"),
        recorded_at_normalized,
    )
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
    record = {field: values.get(field) for field in REQUIRED_FIELDS}
    record["implementation_authorized"] = False
    for field in OPTIONAL_FIELDS:
        if field in optionals and optionals[field] is not None:
            record[field] = optionals[field]
    return record


def validate_record_fields(record: dict) -> dict:
    normalized = dict(record)
    normalized["source"] = validate_source(record["source"])
    normalized["effective_week_start"] = validate_effective_week_start(record["effective_week_start"])
    normalized["operation_type"] = validate_operation_type(record["operation_type"])
    _recorded_at, recorded_at_normalized = parse_recorded_at(record["recorded_at"])
    normalized["recorded_by"] = validate_role(record["recorded_by"], field="recorded_by", allow_null=False)
    normalized["reviewed_by"] = validate_role(record["reviewed_by"], field="reviewed_by", allow_null=True)
    normalized["anomaly_label"] = validate_label(record["anomaly_label"])
    normalized["anomaly_reason"] = validate_technical_code(record["anomaly_reason"], field="anomaly_reason", allow_null=True)
    normalized["evidence_refs"] = validate_evidence_refs(record["evidence_refs"])

    normalized["input_file_name"] = validate_technical_code(record["input_file_name"], field="input_file_name", allow_null=False)
    normalized["input_file_sha256"] = validate_sha256(record["input_file_sha256"], field="input_file_sha256")
    normalized["schema_signature"] = validate_sha256(record["schema_signature"], field="schema_signature")
    for field in (
        "input_rows",
        "accepted_rows",
        "rejected_rows",
        "exact_duplicate_rows",
        "grain_duplicate_rows",
        "missing_required_rows",
    ):
        normalized[field] = validate_count(record[field], field=field, allow_null=True)
    normalized["source_check_verdict"] = validate_technical_code(
        record["source_check_verdict"], field="source_check_verdict", allow_null=True
    )
    normalized["loader_executed"] = validate_bool(record["loader_executed"], field="loader_executed", allow_null=False)
    normalized["db_write_executed"] = validate_bool(record["db_write_executed"], field="db_write_executed", allow_null=False)
    normalized["post_load_validation_status"] = validate_technical_code(
        record["post_load_validation_status"], field="post_load_validation_status", allow_null=True
    )

    validate_count_consistency(normalized)

    for field in ("batch_id", "route_batch_id", "new_key_count", "removed_key_count", "changed_key_count",
                  "parity_missing_keys", "parity_extra_keys", "parity_value_differences", "rework_rounds"):
        if field in normalized:
            normalized[field] = validate_count(normalized[field], field=field, allow_null=True)
    if "raw_batch_ids" in normalized:
        value = normalized["raw_batch_ids"]
        if not isinstance(value, list) or any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in value):
            raise ObsError("invalid_list", field="raw_batch_ids")
    if "affected_weeks" in normalized:
        value = normalized["affected_weeks"]
        if not isinstance(value, list):
            raise ObsError("invalid_list", field="affected_weeks")
        normalized["affected_weeks"] = [
            parse_iso_date(v, field="affected_weeks", monday=True, allow_null=False) for v in value
        ]
    for field in ("minimum_date", "maximum_date"):
        if field in normalized:
            normalized[field] = parse_iso_date(normalized[field], field=field, allow_null=True)
    for field in ("rollback_required", "rollback_executed"):
        if field in normalized:
            normalized[field] = validate_bool(normalized[field], field=field, allow_null=True)
    if "elapsed_minutes" in normalized:
        normalized["elapsed_minutes"] = validate_optional_number(normalized["elapsed_minutes"], field="elapsed_minutes")
    if "notes" in normalized:
        normalized["notes"] = validate_technical_code(normalized["notes"], field="notes", allow_null=True)

    expected_id = make_observation_id(
        normalized["source"],
        normalized["effective_week_start"],
        normalized["operation_type"],
        normalized["input_file_sha256"],
        recorded_at_normalized,
    )
    if normalized["observation_id"] != expected_id:
        raise ObsError("observation_id_mismatch")
    return normalized


def validate_count_consistency(record) -> None:
    input_rows = record["input_rows"]
    accepted = record["accepted_rows"]
    rejected = record["rejected_rows"]
    if input_rows is not None:
        for field in ("accepted_rows", "rejected_rows"):
            value = record[field]
            if value is not None and value > input_rows:
                raise ObsError("count_exceeds_input", field=field)
        if accepted is not None and rejected is not None and accepted + rejected > input_rows:
            raise ObsError("count_sum_exceeds_input")


def enforce_operation_and_label(record) -> None:
    op = record["operation_type"]
    label = record["anomaly_label"]
    loader = record["loader_executed"]
    db_write = record["db_write_executed"]
    status = record["post_load_validation_status"]
    status_up = str(status).upper() if status else None
    source_status = record["source_check_verdict"]
    source_status_up = str(source_status).upper() if source_status else None
    reason = record["anomaly_reason"]
    notes = record.get("notes")
    evidence = record["evidence_refs"]
    rollback_required = record.get("rollback_required")
    rollback_executed = record.get("rollback_executed")

    matrix = {
        "SOURCE_CHECK": {"UNREVIEWED", "CLEAN", "EXPECTED_CHANGE", "INVALID_INPUT"},
        "PREFLIGHT": {"UNREVIEWED", "EXPECTED_CHANGE", "INVALID_INPUT", "ANOMALOUS"},
        "DRY_RUN": {"UNREVIEWED", "CLEAN", "EXPECTED_CHANGE", "INVALID_INPUT", "ANOMALOUS"},
        "APPLY": {"UNREVIEWED", "CLEAN", "EXPECTED_CHANGE", "INVALID_INPUT", "LOAD_FAILURE"},
        "POST_LOAD_VALIDATION": {"UNREVIEWED", "CLEAN", "EXPECTED_CHANGE", "ANOMALOUS", "POST_LOAD_REGRESSION"},
        "ROLLBACK": {"UNREVIEWED", "CLEAN", "LOAD_FAILURE", "POST_LOAD_REGRESSION"},
    }

    def fail(detail: str) -> None:
        raise ObsError("operation_label_requirements_not_met", label=label, operation_type=op, detail=detail)

    if label not in matrix[op]:
        fail("label not allowed for operation")
    if op in {"SOURCE_CHECK", "PREFLIGHT", "DRY_RUN"}:
        if loader is not False or db_write is not False:
            fail("pre-apply operations require loader_executed=false and db_write_executed=false")
    if op in {"APPLY", "POST_LOAD_VALIDATION"}:
        if loader is not True or db_write is not True:
            fail("apply/post-load operations require loader_executed=true and db_write_executed=true")
    if op == "ROLLBACK":
        if loader is not True:
            fail("rollback requires loader_executed=true")
        if rollback_required is not True:
            fail("rollback_required must be true")
        if rollback_executed is None:
            fail("rollback_executed must be boolean")

    if label in {"EXPECTED_CHANGE", "ANOMALOUS", "INVALID_INPUT"}:
        if not (reason or notes):
            fail("technical reason or notes required")
        if not evidence:
            fail("evidence_refs required")
    if label == "CLEAN":
        if op in {"SOURCE_CHECK", "DRY_RUN"}:
            if source_status_up not in APPROVED_STATUS:
                fail("source_check_verdict must be approved")
        if op in {"APPLY", "POST_LOAD_VALIDATION"}:
            if status_up not in APPROVED_STATUS:
                fail("post_load_validation_status must be approved")
        if op == "ROLLBACK":
            if rollback_executed is not True or status_up not in APPROVED_STATUS:
                fail("rollback clean requires executed rollback and approved validation")
    if label == "LOAD_FAILURE":
        if not evidence or not (reason or notes):
            fail("load failure requires technical reason and evidence")
        if status_up in APPROVED_STATUS:
            fail("load failure cannot have approved status")
    if label == "POST_LOAD_REGRESSION":
        if status_up not in FAILED_STATUS:
            fail("post-load regression requires failed status")
        if not evidence:
            fail("post-load regression requires evidence")


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
    normalized = validate_record_fields(record)
    enforce_operation_and_label(normalized)
    privacy_scan(json.dumps(normalized, ensure_ascii=False), normalized)
    return normalized


def cmd_draft(args) -> dict:
    source = validate_source(args.source)
    week = validate_effective_week_start(args.effective_week_start)
    operation_type = validate_operation_type(args.operation_type)
    cli_input_sha256 = validate_sha256(args.input_file_sha256, field="input_file_sha256")
    recorded_at, recorded_at_normalized = parse_recorded_at(args.recorded_at)
    recorded_by = validate_role(args.recorded_by, field="recorded_by", allow_null=False)
    reviewed_by = validate_role(args.reviewed_by, field="reviewed_by", allow_null=True)
    label = validate_label(args.label) if args.label else DEFAULT_LABEL
    evidence_refs = validate_evidence_refs(list(args.evidence_ref or []))
    anomaly_reason = validate_technical_code(args.anomaly_reason, field="anomaly_reason", allow_null=True)
    notes = validate_technical_code(args.notes, field="notes", allow_null=True)

    raw_text, obj = read_json_path(args.phase_json)
    privacy_scan(raw_text, obj)
    shape, technical_src, consistency_src = detect_shape(obj)
    require_shape_complete(shape, technical_src)
    assert_cli_json_consistency(
        shape,
        consistency_src if shape == "B" else technical_src,
        source=source,
        week=week,
        operation_type=operation_type,
    )
    tech = extract_technical(shape, technical_src)
    json_input_sha256 = validate_sha256(tech["input_file_sha256"], field="input_file_sha256")
    if json_input_sha256 != cli_input_sha256:
        raise ObsError("phase_cli_mismatch", field="input_file_sha256")
    tech["input_file_sha256"] = json_input_sha256
    optionals = extract_optionals(technical_src, notes)
    record = assemble_record(
        source=source,
        week=week,
        operation_type=operation_type,
        recorded_at=recorded_at,
        recorded_at_normalized=recorded_at_normalized,
        recorded_by=recorded_by,
        reviewed_by=reviewed_by,
        label=label,
        anomaly_reason=anomaly_reason,
        evidence_refs=evidence_refs,
        tech=tech,
        optionals=optionals,
    )
    normalized = validate_record(record)
    return normalized


def cmd_validate(args) -> dict:
    raw, obj = read_json_path(args.record)
    privacy_scan(raw, obj)
    record = validate_record(obj)
    return {
        "validate": "ok",
        "observation_id": record["observation_id"],
        "record": record,
        "evidence_ref_validation": {
            "commit": "syntax_only",
            "phase": "syntax_only",
            "report": "syntax_only",
            "research": "syntax_and_exists",
        },
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(prog="sz_load_observation", add_help=False)
    sub = parser.add_subparsers(dest="command", required=True)

    draft = sub.add_parser("draft", add_help=False)
    draft.add_argument("--phase-json", required=True)
    draft.add_argument("--source", required=True)
    draft.add_argument("--effective-week-start", required=True)
    draft.add_argument("--operation-type", required=True)
    draft.add_argument("--input-file-sha256", required=True)
    draft.add_argument("--recorded-at", required=True)
    draft.add_argument("--recorded-by", required=True)
    draft.add_argument("--reviewed-by", default=None)
    draft.add_argument("--label", default=None)
    draft.add_argument("--anomaly-reason", default=None)
    draft.add_argument("--evidence-ref", action="append", default=None)
    draft.add_argument("--notes", default=None)
    draft.add_argument("--pretty", action="store_true")

    validate = sub.add_parser("validate", add_help=False)
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
    pretty = "--pretty" in argv
    try:
        parser = build_arg_parser()
        args = parser.parse_args(argv)
        pretty = bool(getattr(args, "pretty", False))
        if args.command == "draft":
            result = cmd_draft(args)
        else:
            result = cmd_validate(args)
    except ObsError as exc:
        payload = {"error": exc.code, "error_category": exc.category}
        payload.update(exc.extra)
        emit(payload, pretty)
        return 1
    except Exception:
        emit({"error": "internal_error", "error_category": "INTERNAL"}, pretty)
        return 2
    emit(result, pretty)
    return 0


def main(argv=None) -> int:
    return run(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    sys.exit(main())
