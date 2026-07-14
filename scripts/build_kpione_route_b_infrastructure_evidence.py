from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping


DOCUMENT_TYPE = "kpione_route_b_infrastructure_evidence_bundle_v1"
COMPONENT_CONTRACT = {
    "readonly_baseline_precheck": (
        "kpione_route_b_readonly_baseline_evidence_v1",
        "PASS_READONLY_BASELINE",
    ),
    "admin_provisioning": (
        "kpione_route_b_role_provisioning_evidence_v1",
        "PASS_ADMIN_PROVISIONING",
    ),
    "productive_role_verification": (
        "kpione_route_b_productive_role_verification_evidence_v1",
        "PASS_PRODUCTIVE_ROLE_VERIFICATION",
    ),
    "readonly_postcheck": (
        "kpione_route_b_readonly_postcheck_evidence_v1",
        "PASS_READONLY_POSTCHECK",
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
    expected_type, expected_verdict = COMPONENT_CONTRACT[name]
    if evidence.get("document_type") != expected_type:
        raise EvidenceBundleError(f"component_document_type_mismatch:{name}")
    if evidence.get("verdict") != expected_verdict:
        raise EvidenceBundleError(f"component_verdict_mismatch:{name}")
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
) -> dict[str, Any]:
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
        evidence[name], component_hashes[name] = _load_component(component_paths[name], name)

    fingerprints = {item.get("target_fingerprint") for item in evidence.values()}
    if len(fingerprints) != 1 or None in fingerprints:
        raise EvidenceBundleError("target_fingerprint_mismatch")
    for name, item in evidence.items():
        if item.get("approved_git_sha") != expected_git_sha:
            raise EvidenceBundleError(f"approved_git_sha_mismatch:{name}")
        if item.get("plan_sha256") != plan_sha256:
            raise EvidenceBundleError(f"plan_sha256_mismatch:{name}")
        if item.get("sql_sha256") != sql_sha256:
            raise EvidenceBundleError(f"sql_sha256_mismatch:{name}")

    baseline = evidence["readonly_baseline_precheck"]
    postcheck = evidence["readonly_postcheck"]
    if postcheck.get("baseline_evidence_sha256") != component_hashes["readonly_baseline_precheck"]:
        raise EvidenceBundleError("postcheck_baseline_reference_mismatch")
    for field in ("legacy", "public_acl"):
        if postcheck.get(field) != baseline.get(field):
            raise EvidenceBundleError(f"baseline_postcheck_{field}_mismatch")

    canonical = {
        "document_type": DOCUMENT_TYPE,
        "status": "PASSED",
        "components": component_hashes,
    }
    return {
        **canonical,
        "bundle_sha256": _canonical_sha256(canonical),
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Build the KPIONE Route B infrastructure evidence bundle")
    result.add_argument("--baseline-evidence", type=Path, required=True)
    result.add_argument("--admin-provisioning-evidence", type=Path, required=True)
    result.add_argument("--productive-role-verification-evidence", type=Path, required=True)
    result.add_argument("--readonly-postcheck-evidence", type=Path, required=True)
    result.add_argument("--plan", type=Path, required=True)
    result.add_argument("--expected-git-sha", required=True)
    result.add_argument("--output", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        bundle = build_bundle({
            "readonly_baseline_precheck": args.baseline_evidence,
            "admin_provisioning": args.admin_provisioning_evidence,
            "productive_role_verification": args.productive_role_verification_evidence,
            "readonly_postcheck": args.readonly_postcheck_evidence,
        }, args.plan, args.expected_git_sha)
        rendered = json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
        return 0
    except (OSError, ValueError, EvidenceBundleError) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
