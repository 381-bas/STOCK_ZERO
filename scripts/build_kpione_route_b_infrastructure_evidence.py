from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

try:
    from scripts.kpione_route_b_evidence_v1 import (
        EvidenceContractError,
        atomic_write_json,
        parse_utc_timestamp,
        prepare_run_directory,
        require_canonical_evidence_path,
        validate_run_id,
    )
    from scripts.precheck_kpione_route_b_018_read_only import (
        legacy_structural_identity,
        target_fingerprint,
    )
except ModuleNotFoundError:  # Direct execution from scripts/.
    from kpione_route_b_evidence_v1 import (
        EvidenceContractError,
        atomic_write_json,
        parse_utc_timestamp,
        prepare_run_directory,
        require_canonical_evidence_path,
        validate_run_id,
    )
    from precheck_kpione_route_b_018_read_only import (
        legacy_structural_identity,
        target_fingerprint,
    )


DOCUMENT_TYPE = "kpione_route_b_infrastructure_evidence_bundle_v1"
ROOT = Path(__file__).resolve().parents[1]
COMPONENT_CONTRACT = {
    "readonly_baseline_precheck": (
        "kpione_route_b_readonly_baseline_evidence_v1",
        "PASS_READONLY_BASELINE",
        1,
    ),
    "admin_provisioning": (
        "kpione_route_b_role_provisioning_evidence_v1",
        "PASS_ADMIN_PROVISIONING",
        2,
    ),
    "productive_role_verification": (
        "kpione_route_b_productive_role_verification_evidence_v1",
        "PASS_PRODUCTIVE_ROLE_VERIFICATION",
        3,
    ),
    "readonly_postcheck": (
        "kpione_route_b_readonly_postcheck_evidence_v1",
        "PASS_READONLY_POSTCHECK",
        4,
    ),
}
SUSPICIOUS_KEY = re.compile(r"(^|_)(dsn|password|secret|environment|credential_value)($|_)", re.I)
SUSPICIOUS_VALUE_TOKENS = (
    "postgresql://",
    "postgres://",
    "DB_URL_ADMIN",
    "DB_URL_CODEX_RO",
    "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE",
    "KPIONE_ROUTE_B_PRODUCTIVE_PASSWORD",
)


class EvidenceBundleError(RuntimeError):
    pass


def _reject_sensitive_content(value: Any, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if SUSPICIOUS_KEY.search(str(key)):
                raise EvidenceBundleError(f"suspicious_evidence_field:{path}.{key}")
            _reject_sensitive_content(nested, f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _reject_sensitive_content(nested, f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.lower()
        if any(token.lower() in lowered for token in SUSPICIOUS_VALUE_TOKENS):
            raise EvidenceBundleError(f"suspicious_evidence_value:{path}")


def _load_component(path: Path, name: str) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
        evidence = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceBundleError(f"component_unreadable:{name}") from exc
    expected_type, expected_verdict, expected_step = COMPONENT_CONTRACT[name]
    if evidence.get("document_type") != expected_type:
        raise EvidenceBundleError(f"component_document_type_mismatch:{name}")
    if evidence.get("verdict") != expected_verdict:
        raise EvidenceBundleError(f"component_verdict_mismatch:{name}")
    if evidence.get("evidence_sequence_step") != expected_step:
        raise EvidenceBundleError(f"component_sequence_step_mismatch:{name}")
    _reject_sensitive_content(evidence)
    return evidence, hashlib.sha256(raw).hexdigest()


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_bundle(
    component_paths: Mapping[str, Path],
    plan_path: Path,
    expected_git_sha: str,
    run_id: str,
    *,
    root: Path = ROOT,
) -> dict[str, Any]:
    try:
        validate_run_id(run_id)
    except EvidenceContractError as exc:
        raise EvidenceBundleError(str(exc)) from None
    if set(component_paths) != set(COMPONENT_CONTRACT):
        raise EvidenceBundleError("component_name_set_mismatch")
    if re.fullmatch(r"[0-9a-f]{40}", expected_git_sha) is None:
        raise EvidenceBundleError("expected_git_sha_invalid")
    try:
        plan_raw = plan_path.read_bytes()
        plan = json.loads(plan_raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceBundleError("plan_unreadable") from exc
    plan_sha256 = hashlib.sha256(plan_raw).hexdigest()
    sql_sha256 = plan.get("physical_contract", {}).get("sql_sha256")
    if re.fullmatch(r"[0-9a-f]{64}", str(sql_sha256)) is None:
        raise EvidenceBundleError("plan_sql_sha256_invalid")

    evidence: dict[str, dict[str, Any]] = {}
    component_hashes: dict[str, str] = {}
    for name in COMPONENT_CONTRACT:
        try:
            path = require_canonical_evidence_path(
                component_paths[name], root, run_id, name,
            )
        except EvidenceContractError as exc:
            raise EvidenceBundleError(str(exc)) from None
        evidence[name], component_hashes[name] = _load_component(path, name)

    if {item.get("run_id") for item in evidence.values()} != {run_id}:
        raise EvidenceBundleError("component_run_id_mismatch")
    timestamps = []
    for name, item in evidence.items():
        try:
            timestamps.append(parse_utc_timestamp(item.get("timestamp_utc")))
        except EvidenceContractError as exc:
            raise EvidenceBundleError(f"{name}:{exc}") from None
    if timestamps != sorted(timestamps):
        raise EvidenceBundleError("evidence_timestamps_not_nondecreasing")

    fingerprints = {item.get("target_fingerprint") for item in evidence.values()}
    if len(fingerprints) != 1 or None in fingerprints:
        raise EvidenceBundleError("target_fingerprint_mismatch")
    if fingerprints != {target_fingerprint(plan)}:
        raise EvidenceBundleError("target_fingerprint_not_registered_plan_target")
    for name, item in evidence.items():
        if item.get("approved_git_sha") != expected_git_sha:
            raise EvidenceBundleError(f"approved_git_sha_mismatch:{name}")
        if item.get("plan_sha256") != plan_sha256:
            raise EvidenceBundleError(f"plan_sha256_mismatch:{name}")
        if item.get("sql_sha256") != sql_sha256:
            raise EvidenceBundleError(f"sql_sha256_mismatch:{name}")

    baseline = evidence["readonly_baseline_precheck"]
    admin = evidence["admin_provisioning"]
    postcheck = evidence["readonly_postcheck"]
    if admin.get("evidence_mode") not in {
        "DIRECT_COMMITTED_EXECUTION", "RECONCILED_COMMITTED_STATE",
    }:
        raise EvidenceBundleError("admin_evidence_mode_invalid")
    if postcheck.get("baseline_evidence_sha256") != component_hashes["readonly_baseline_precheck"]:
        raise EvidenceBundleError("postcheck_baseline_reference_mismatch")
    if legacy_structural_identity(postcheck.get("legacy", {})) != legacy_structural_identity(
        baseline.get("legacy", {})
    ):
        raise EvidenceBundleError("baseline_postcheck_legacy_structure_mismatch")
    if postcheck.get("public_acl") != baseline.get("public_acl"):
        raise EvidenceBundleError("baseline_postcheck_public_acl_mismatch")

    canonical = {
        "document_type": DOCUMENT_TYPE,
        "run_id": run_id,
        "status": "PASSED",
        "components": component_hashes,
    }
    return {
        **canonical,
        "bundle_sha256": _canonical_sha256(canonical),
    }


def canonical_component_paths(root: Path, run_id: str) -> dict[str, Path]:
    return {
        name: require_canonical_evidence_path(
            root / "evidence" / "runtime" / "020B" / run_id / filename,
            root,
            run_id,
            name,
        )
        for name, filename in (
            ("readonly_baseline_precheck", "01_readonly_baseline.json"),
            ("admin_provisioning", "02_admin_provisioning.json"),
            ("productive_role_verification", "03_productive_role_verification.json"),
            ("readonly_postcheck", "04_readonly_postcheck.json"),
        )
    }


def validate_existing_bundle(
    plan_path: Path,
    expected_git_sha: str,
    run_id: str,
    output: Path,
    *,
    root: Path = ROOT,
) -> dict[str, str]:
    try:
        validate_run_id(run_id)
        bundle_path = require_canonical_evidence_path(
            output, root, run_id, "infrastructure_bundle",
        )
    except EvidenceContractError as exc:
        raise EvidenceBundleError(str(exc)) from None
    try:
        raw = bundle_path.read_bytes()
        stored = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceBundleError("existing_bundle_unreadable") from exc
    recalculated = build_bundle(
        canonical_component_paths(root, run_id),
        plan_path,
        expected_git_sha,
        run_id,
        root=root,
    )
    if stored != recalculated:
        raise EvidenceBundleError("existing_bundle_exact_mismatch")
    return {
        "verdict": "PASS_EXISTING_INFRASTRUCTURE_BUNDLE_VALIDATION",
        "run_id": run_id,
        "bundle_sha256": recalculated["bundle_sha256"],
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Build the KPIONE Route B infrastructure evidence bundle")
    result.add_argument("--baseline-evidence", type=Path)
    result.add_argument("--admin-provisioning-evidence", type=Path)
    result.add_argument("--productive-role-verification-evidence", type=Path)
    result.add_argument("--readonly-postcheck-evidence", type=Path)
    result.add_argument("--plan", type=Path, required=True)
    result.add_argument("--run-id", required=True)
    result.add_argument("--expected-git-sha", required=True)
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--validate-existing", action="store_true")
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        validate_run_id(args.run_id)
        output = require_canonical_evidence_path(
            args.output, ROOT, args.run_id, "infrastructure_bundle",
        )
        if args.validate_existing:
            if any((
                args.baseline_evidence,
                args.admin_provisioning_evidence,
                args.productive_role_verification_evidence,
                args.readonly_postcheck_evidence,
            )):
                raise EvidenceBundleError("component_arguments_not_allowed_for_existing_validation")
            print(json.dumps(validate_existing_bundle(
                args.plan, args.expected_git_sha, args.run_id, output,
            ), sort_keys=True))
            return 0
        if not all((
            args.baseline_evidence,
            args.admin_provisioning_evidence,
            args.productive_role_verification_evidence,
            args.readonly_postcheck_evidence,
        )):
            raise EvidenceBundleError("all_component_arguments_required_for_bundle_creation")
        prepare_run_directory(ROOT, args.run_id)
        if output.exists():
            raise EvidenceBundleError("evidence_file_already_exists")
        bundle = build_bundle({
            "readonly_baseline_precheck": args.baseline_evidence,
            "admin_provisioning": args.admin_provisioning_evidence,
            "productive_role_verification": args.productive_role_verification_evidence,
            "readonly_postcheck": args.readonly_postcheck_evidence,
        }, args.plan, args.expected_git_sha, args.run_id)
        rendered = json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True)
        atomic_write_json(output, bundle)
        print(rendered)
        return 0
    except (OSError, ValueError, EvidenceContractError, EvidenceBundleError) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
