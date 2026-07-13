from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import json5


ROOT = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {".json", ".jsonl", ".md", ".py", ".sql", ".toml", ".txt", ".yml", ".yaml", ".ps1", ".example"}
KERNEL_DIR = ROOT / "governance" / "kernel" / "current"
MANIFEST = KERNEL_DIR / "00_kernel_manifest_stock_zero_v2026_06_30_011.json"
WORKFLOW = ROOT / ".github" / "workflows" / "repository-quality.yml"
ENCODING_ALLOWLIST = ROOT / "ci" / "encoding_allowlist.json"
FORBIDDEN_WORKFLOW_TOKENS = (
    "DB_URL_CODEX_RO",
    "DB_URL_CODEX_LOAD",
    "DB_URL_LOAD",
    "DB_URL_APP",
    "SUPABASE_SERVICE_ROLE_KEY",
    "--apply-productive",
    "--apply-local",
    "refresh_control_gestion_v2",
    "load_fact_from_excel.py",
    "load_ruta_rutero_from_excel.py",
)


def tracked_files(root: Path = ROOT) -> list[Path]:
    output = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root
    )
    return [root / item.decode("utf-8") for item in output.split(b"\0") if item]


def check_json() -> None:
    paths = [path for path in tracked_files() if path.suffix.lower() in {".json", ".jsonl"}]
    for path in paths:
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".jsonl":
            for line_number, line in enumerate(text.splitlines(), 1):
                if line.strip():
                    json.loads(line)
        elif path.relative_to(ROOT).as_posix() == ".devcontainer/devcontainer.json":
            json5.loads(text)
        else:
            json.loads(text)
    print(f"validated_json_files={len(paths)}")


def check_encoding() -> None:
    allowlist = json.loads(ENCODING_ALLOWLIST.read_text(encoding="utf-8"))["utf8_bom"]
    if any(not reason.strip() for reason in allowlist.values()):
        raise ValueError("every UTF-8 BOM allowlist entry requires a reason")
    observed_bom = set()
    checked = 0
    for path in tracked_files():
        if path.suffix.lower() not in TEXT_SUFFIXES and path.name not in {".gitignore", ".gitattributes"}:
            continue
        raw = path.read_bytes()
        if raw.startswith(b"\xef\xbb\xbf"):
            relative = path.relative_to(ROOT).as_posix()
            observed_bom.add(relative)
            if relative not in allowlist:
                raise ValueError(f"unapproved UTF-8 BOM: {relative}")
        raw.decode("utf-8-sig")
        checked += 1
    stale = sorted(set(allowlist) - observed_bom)
    if stale:
        raise ValueError(f"stale UTF-8 BOM allowlist entries: {stale}")
    print(f"validated_utf8_text_files={checked}")


def check_manifest() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    expected = {"01_project_kernel", "02_project_state", "03_project_ledger"}
    if set(manifest["files"]) != expected:
        raise ValueError("current kernel manifest must contain exactly Kernel 01, State 02 and Ledger 03")
    retired = list(KERNEL_DIR.glob("04_project_technical_evidence*.json"))
    if retired:
        raise ValueError(f"retired Kernel 04 exists: {retired}")
    print("kernel_manifest=valid_three_member_model")


def check_safety() -> None:
    tracked = tracked_files()
    forbidden_names = [path for path in tracked if path.name.startswith(".env") and path.name != ".env.example"]
    if forbidden_names:
        raise ValueError(f"tracked environment files are forbidden: {forbidden_names}")
    private_key_markers = (
        b"-----BEGIN " + b"PRIVATE KEY-----",
        b"-----BEGIN " + b"RSA PRIVATE KEY-----",
    )
    leaked = []
    for path in tracked:
        raw = path.read_bytes()
        if any(marker in raw for marker in private_key_markers):
            leaked.append(path.relative_to(ROOT).as_posix())
    if leaked:
        raise ValueError(f"private key material detected: {leaked}")
    workflow = WORKFLOW.read_text(encoding="utf-8")
    present = [token for token in FORBIDDEN_WORKFLOW_TOKENS if token in workflow]
    if present:
        raise ValueError(f"productive token present in workflow: {present}")
    print(f"safety_static=pass tracked_files={len(tracked)}")


def check_diff(base: str, head: str, mode: str) -> None:
    separator = "..." if mode == "pull_request" else ".."
    completed = subprocess.run(["git", "diff", "--check", f"{base}{separator}{head}"], cwd=ROOT, check=False)
    if completed.returncode:
        raise SystemExit(completed.returncode)
    print(f"diff_check=pass range={base}{separator}{head}")


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("json", "encoding", "manifest", "safety"):
        subparsers.add_parser(name)
    diff = subparsers.add_parser("diff")
    diff.add_argument("--base", required=True)
    diff.add_argument("--head", required=True)
    diff.add_argument("--mode", choices=("pull_request", "push"), required=True)
    args = parser.parse_args()
    if args.command == "json":
        check_json()
    elif args.command == "encoding":
        check_encoding()
    elif args.command == "manifest":
        check_manifest()
    elif args.command == "safety":
        check_safety()
    else:
        check_diff(args.base, args.head, args.mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
