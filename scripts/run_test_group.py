from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "ci" / "test_groups.json"


def load_registry(path: Path = REGISTRY) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_registry(registry: dict, root: Path = ROOT) -> dict[str, list[str]]:
    categories = registry["categories"]
    groups = registry["modules"]
    expected_categories = [
        "CI_CORE",
        "CI_POSTGRESQL",
        "LOCAL_SOURCE_INTEGRATION",
        "LOCAL_ENVIRONMENT",
        "PRODUCTIVE_NEVER_CI",
    ]
    if categories != expected_categories or set(groups) != set(expected_categories):
        raise ValueError("test group categories do not match the fixed classification contract")

    classified = [module for category in categories for module in groups[category]]
    duplicates = sorted({module for module in classified if classified.count(module) > 1})
    tracked = sorted(path.relative_to(root).as_posix() for path in (root / "tests").glob("test_*.py"))
    missing = sorted(set(tracked) - set(classified))
    unknown = sorted(set(classified) - set(tracked))
    reasons = registry.get("reasons", {})
    non_core = {module for category in categories[1:] for module in groups[category]}
    missing_reasons = sorted(module for module in non_core if not reasons.get(module, "").strip())
    stale_reasons = sorted(set(reasons) - non_core)
    errors = {
        "duplicates": duplicates,
        "missing": missing,
        "unknown": unknown,
        "missing_reasons": missing_reasons,
        "stale_reasons": stale_reasons,
    }
    if any(errors.values()):
        raise ValueError(json.dumps(errors, sort_keys=True))
    return groups


def module_name(path: str) -> str:
    return Path(path).with_suffix("").as_posix().replace("/", ".")


def validate_local_requirements(registry: dict, group: str, root: Path = ROOT) -> None:
    failures = []
    for requirement in registry.get("local_requirements", {}).get(group, []):
        candidates = [root / relative for relative in requirement["any_of"]]
        existing = [path for path in candidates if path.is_file()]
        if not existing:
            failures.append(f"missing {requirement['description']}: {requirement['any_of']}")
            continue
        expected = requirement.get("sha256")
        if expected and not any(hashlib.sha256(path.read_bytes()).hexdigest().upper() == expected for path in existing):
            failures.append(f"hash mismatch for {requirement['description']}: {requirement['any_of']}")
    if failures:
        raise RuntimeError("local test prerequisites failed:\n" + "\n".join(failures))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one complete STOCK_ZERO test classification group.")
    parser.add_argument("--group", required=True)
    parser.add_argument("--list", action="store_true", help="Validate and list modules without running them.")
    args = parser.parse_args(argv)

    registry = load_registry()
    groups = validate_registry(registry)
    if args.group not in groups:
        parser.error(f"unknown group: {args.group}")
    selected = groups[args.group]
    print(json.dumps({"group": args.group, "module_count": len(selected), "modules": selected}, indent=2))
    if args.list:
        return 0
    validate_local_requirements(registry, args.group)
    if not selected:
        print("No modules classified in this group.")
        return 0
    command = [sys.executable, "-m", "unittest", *map(module_name, selected)]
    completed = subprocess.run(command, cwd=ROOT, check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
