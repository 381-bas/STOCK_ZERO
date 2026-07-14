from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any


RUN_ID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
EVIDENCE_FILENAMES = {
    "readonly_baseline_precheck": "01_readonly_baseline.json",
    "admin_provisioning": "02_admin_provisioning.json",
    "productive_role_verification": "03_productive_role_verification.json",
    "readonly_postcheck": "04_readonly_postcheck.json",
    "infrastructure_bundle": "05_infrastructure_bundle.json",
}


class EvidenceContractError(RuntimeError):
    pass


def validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise EvidenceContractError("run_id_must_be_canonical_lowercase_uuid4")
    try:
        parsed = uuid.UUID(run_id)
    except ValueError as exc:
        raise EvidenceContractError("run_id_must_be_canonical_lowercase_uuid4") from exc
    if parsed.version != 4 or str(parsed) != run_id:
        raise EvidenceContractError("run_id_must_be_canonical_lowercase_uuid4")
    return run_id


def canonical_run_directory(root: Path, run_id: str) -> Path:
    validate_run_id(run_id)
    return root.resolve() / "evidence" / "runtime" / "020B" / run_id


def prepare_run_directory(root: Path, run_id: str) -> Path:
    directory = canonical_run_directory(root, run_id)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def canonical_evidence_path(root: Path, run_id: str, component: str) -> Path:
    if component not in EVIDENCE_FILENAMES:
        raise EvidenceContractError("unknown_evidence_component")
    return canonical_run_directory(root, run_id) / EVIDENCE_FILENAMES[component]


def require_canonical_evidence_path(
    path: Path,
    root: Path,
    run_id: str,
    component: str,
) -> Path:
    expected = canonical_evidence_path(root, run_id, component)
    candidate = path if path.is_absolute() else root / path
    if candidate.resolve() != expected:
        raise EvidenceContractError(f"evidence_path_not_canonical:{component}")
    return expected


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise EvidenceContractError("evidence_file_already_exists")
    if not path.parent.is_dir():
        raise EvidenceContractError("evidence_run_directory_missing")
    rendered = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        if path.exists():
            raise EvidenceContractError("evidence_file_already_exists")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def parse_utc_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value.endswith("+00:00"):
        raise EvidenceContractError("evidence_timestamp_must_be_utc")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise EvidenceContractError("evidence_timestamp_must_be_utc") from exc
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise EvidenceContractError("evidence_timestamp_must_be_utc")
    return parsed
