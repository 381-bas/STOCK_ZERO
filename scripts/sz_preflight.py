#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any


PREFLIGHT_VERSION = "9B14_5_P2B"
PHASES = ("scanner_only", "generic", "control_gestion_v2", "9B15")
SCANNER_SCOPES = ("active", "all")
CODex_RO_PHASES = {"control_gestion_v2", "9B15"}
EXPECTED_CODEX_RO_USER = "stock_zero_codex_ro"
KERNEL_NAMES = (
    "01_kernel_global_v1_4_1.json",
    "02_project_state_stock_zero_v1_6_4.json",
    "03_iterative_ledger_v1_6_3.json",
    "CONTROL_GESTION_IMPLEMENTATION_KERNEL_V2_2.json",
)
ARTIFACT_PATTERNS = (
    "CG_V2_BUSINESS_RULE_EVIDENCE_PACK_9B13_*.json",
    "CG_V2_BUSINESS_RULE_CONTRACT_9B14_*.json",
)
PHASE_NEXT_ACTION = {
    "scanner_only": "Scanner operativo; revisar warnings si aplica.",
    "generic": "Preflight genérico limpio; continuar con análisis o patch local acotado.",
    "control_gestion_v2": "CONTROL_GESTION v2 apto para diagnóstico/patch local si no hay blockers; usar DB solo por read-only.",
    "9B15": "Apto para abrir diseño de contrato export 9B15 si no hay blockers; no implementar export sin contrato.",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=PHASES, required=True)
    ap.add_argument("--root", default=".")
    ap.add_argument("--json-out", default="")
    ap.add_argument("--txt-out", default="")
    ap.add_argument("--skip-db", action="store_true")
    ap.add_argument("--require-clean-git", action="store_true")
    ap.add_argument("--expected-head", default="")
    ap.add_argument("--scanner-scope", choices=SCANNER_SCOPES, default="active")
    ap.add_argument("--fail-on-warnings", action="store_true")
    return ap.parse_args()


def now_local_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def run_cmd(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def temp_report_path(prefix: str, suffix: str) -> Path:
    return Path(tempfile.gettempdir()) / f"{prefix}_{suffix}"


def list_nonempty_lines(text: str) -> list[str]:
    return [line.rstrip() for line in str(text or "").splitlines() if line.strip()]


def add_warning(warnings: list[str], text: str) -> None:
    if text and text not in warnings:
        warnings.append(text)


def add_blocker(blockers: list[str], text: str) -> None:
    if text and text not in blockers:
        blockers.append(text)


def git_probe(root: Path, require_clean: bool, expected_head: str) -> tuple[dict[str, Any], list[str], list[str]]:
    warnings: list[str] = []
    blockers: list[str] = []

    status_cp = run_cmd(["git", "status", "--short"], root)
    head_cp = run_cmd(["git", "rev-parse", "--short", "HEAD"], root)
    branch_cp = run_cmd(["git", "branch", "--show-current"], root)

    status_short = status_cp.stdout.strip()
    head = head_cp.stdout.strip()
    branch = branch_cp.stdout.strip()
    clean = not status_short
    head_matches_expected = None
    expected_value = expected_head or None

    if require_clean and not clean:
        add_blocker(blockers, "git_worktree_not_clean")
    elif not clean:
        add_warning(warnings, "git_worktree_not_clean")

    if expected_head:
        head_matches_expected = head == expected_head
        if not head_matches_expected:
            add_blocker(blockers, "git_head_mismatch")

    payload = {
        "branch": branch,
        "head": head,
        "status_short": status_short,
        "clean": clean,
        "expected_head": expected_value,
        "head_matches_expected": head_matches_expected,
    }
    return payload, warnings, blockers


def scanner_probe(root: Path, scope: str) -> tuple[dict[str, Any], list[str], list[str]]:
    warnings: list[str] = []
    blockers: list[str] = []

    scanner_txt = temp_report_path("sz_preflight_scanner", f"{scope}.txt")
    scanner_json = temp_report_path("sz_preflight_scanner", f"{scope}.json")
    scanner_path = root / "scripts" / "scanner.py"
    cmd = [
        sys.executable,
        str(scanner_path),
        "--root",
        str(root),
        "--scope",
        scope,
        "--fail-on-syntax",
        "--out",
        str(scanner_txt),
        "--json-out",
        str(scanner_json),
        "--max-dump-lines",
        "0",
    ]
    cp = run_cmd(cmd, root)

    json_valid = False
    summary: dict[str, Any] = {}
    if cp.returncode != 0:
        add_blocker(blockers, "scanner_exit_nonzero")
    if scanner_json.exists():
        try:
            payload = json.loads(scanner_json.read_text(encoding="utf-8"))
            summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
            json_valid = isinstance(summary, dict)
        except Exception:
            json_valid = False
    if not json_valid:
        add_blocker(blockers, "scanner_json_invalid")

    syntax_errors = int(summary.get("syntax_errors", 0) or 0)
    read_errors = int(summary.get("read_errors", 0) or 0)
    bom_warnings = int(summary.get("bom_warnings", 0) or 0)

    if syntax_errors > 0:
        add_blocker(blockers, "scanner_syntax_errors_present")
    if read_errors > 0:
        add_blocker(blockers, "scanner_read_errors_present")
    if bom_warnings > 0:
        add_warning(warnings, "scanner_bom_warnings_present")

    verdict = "block" if blockers else "warn" if warnings else "ok"
    payload = {
        "command": " ".join(f'"{part}"' if " " in part else part for part in cmd),
        "scope": scope,
        "exit_code": cp.returncode,
        "json_valid": json_valid,
        "summary": summary,
        "report_paths": {
            "txt": str(scanner_txt),
            "json": str(scanner_json),
        },
        "verdict": verdict,
    }
    return payload, warnings, blockers


def kernel_search_roots(root: Path) -> list[Path]:
    candidates = [
        root / "Documentos" / "KERNEL BASE FINAL",
        root / "Documentos",
        root,
    ]
    seen: set[Path] = set()
    ordered: list[Path] = []
    for path in candidates:
        resolved = path.resolve()
        if resolved.exists() and resolved not in seen:
            seen.add(resolved)
            ordered.append(resolved)
    return ordered


def find_kernel(root: Path, filename: str) -> Path | None:
    for search_root in kernel_search_roots(root):
        matches = sorted(search_root.rglob(filename), key=lambda p: str(p).lower())
        if matches:
            return matches[0]
    return None


def load_json_file(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def kernel_probe(root: Path, phase: str, git_head: str) -> tuple[dict[str, Any], list[str], list[str]]:
    warnings: list[str] = []
    blockers: list[str] = []
    files_detected: dict[str, bool] = {}
    json_parse_ok: dict[str, bool] = {}

    head_expected_from_02 = ""
    head_expected_from_module = ""
    next_phase_from_02 = ""
    next_phase_from_module = ""
    open_watchlist_count: int | None = None
    meta_01_status = ""
    meta_01_last_updated = ""

    kernel_paths: dict[str, Path | None] = {}
    for filename in KERNEL_NAMES:
        path = find_kernel(root, filename)
        kernel_paths[filename] = path
        files_detected[filename] = bool(path)

    for filename in KERNEL_NAMES:
        path = kernel_paths[filename]
        if not path:
            json_parse_ok[filename] = False
            if filename == "02_project_state_stock_zero_v1_6_4.json":
                if phase == "9B15":
                    add_blocker(blockers, "kernel_02_missing")
                else:
                    add_warning(warnings, "kernel_02_missing")
            else:
                add_warning(warnings, f"{filename}_missing")
            continue

        data = load_json_file(path)
        json_parse_ok[filename] = data is not None
        if data is None:
            if filename == "02_project_state_stock_zero_v1_6_4.json" and phase == "9B15":
                add_blocker(blockers, "kernel_02_json_invalid")
            else:
                add_warning(warnings, f"{filename}_json_invalid")
            continue

        if filename == "01_kernel_global_v1_4_1.json":
            meta = data.get("meta", {}) if isinstance(data.get("meta"), dict) else {}
            meta_01_status = str(meta.get("status") or "")
            meta_01_last_updated = str(meta.get("last_updated") or "")
        elif filename == "02_project_state_stock_zero_v1_6_4.json":
            current_checkpoint = data.get("current_checkpoint", {}) if isinstance(data.get("current_checkpoint"), dict) else {}
            head_expected_from_02 = str(current_checkpoint.get("head_commit") or "")
            next_phase_from_02 = str(current_checkpoint.get("next_recommended_phase") or "")
            if head_expected_from_02 and head_expected_from_02 != git_head:
                add_warning(warnings, "kernel_02_head_mismatch")
        elif filename == "03_iterative_ledger_v1_6_3.json":
            open_watchlist = data.get("open_watchlist")
            if isinstance(open_watchlist, list):
                open_watchlist_count = len(open_watchlist)
        else:
            current_stage = data.get("current_stage", {}) if isinstance(data.get("current_stage"), dict) else {}
            head_expected_from_module = str(current_stage.get("head_commit") or "")
            next_phase_from_module = str(data.get("next_phase") or current_stage.get("next_phase") or "")
            if head_expected_from_module and head_expected_from_module != git_head:
                add_warning(warnings, "module_kernel_head_mismatch")

    verdict = "block" if blockers else "warn" if warnings else "ok"
    payload = {
        "files_detected": files_detected,
        "json_parse_ok": json_parse_ok,
        "head_expected_from_02": head_expected_from_02,
        "head_expected_from_module_kernel": head_expected_from_module,
        "next_phase_from_02": next_phase_from_02,
        "next_phase_from_module_kernel": next_phase_from_module,
        "open_watchlist_count": open_watchlist_count,
        "kernel_01_status": meta_01_status,
        "kernel_01_last_updated": meta_01_last_updated,
        "warnings": warnings,
        "verdict": verdict,
    }
    return payload, warnings, blockers


def detect_codex_ro_source(root: Path) -> tuple[bool, str]:
    if os.getenv("DB_URL_CODEX_RO", "").strip():
        return True, "env"
    secret_file = root / ".local_secrets" / "codex_ro.env"
    if secret_file.exists():
        return True, "file"
    return False, "none"


def parse_key_value_stdout(text: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for line in str(text or "").splitlines():
        raw = line.strip()
        if not raw or "=" not in raw:
            if raw == "NO_DB_URL_AVAILABLE":
                parsed["NO_DB_URL_AVAILABLE"] = "true"
            continue
        key, value = raw.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def codex_ro_probe(root: Path, phase: str, skip_db: bool) -> tuple[dict[str, Any], list[str], list[str]]:
    warnings: list[str] = []
    blockers: list[str] = []
    available, detected_source = detect_codex_ro_source(root)
    payload: dict[str, Any] = {
        "available": available,
        "executed": False,
        "source_used": detected_source,
        "current_user": None,
        "readonly_state": None,
        "statement_timeout": None,
        "mv_count": None,
        "warnings": [],
        "verdict": "skip",
    }

    if phase not in CODex_RO_PHASES:
        payload["warnings"] = []
        return payload, warnings, blockers

    if skip_db:
        payload["warnings"] = ["codex_ro_skipped_by_flag"]
        payload["verdict"] = "skip"
        return payload, warnings, blockers

    helper_path = root / "scripts" / "codex_ro_env_check.py"
    if not helper_path.exists():
        add_warning(warnings, "codex_ro_helper_missing")
        payload["warnings"] = list(warnings)
        payload["verdict"] = "warn"
        return payload, warnings, blockers

    if not available:
        add_warning(warnings, "codex_ro_not_available")
        payload["warnings"] = list(warnings)
        payload["verdict"] = "warn"
        return payload, warnings, blockers

    cp = run_cmd([sys.executable, str(helper_path)], root)
    payload["executed"] = True
    kv = parse_key_value_stdout(cp.stdout)
    payload["source_used"] = kv.get("SOURCE_USED", detected_source or "unknown")
    payload["current_user"] = kv.get("CURRENT_USER")
    payload["readonly_state"] = kv.get("DEFAULT_TRANSACTION_READ_ONLY")
    payload["statement_timeout"] = kv.get("STATEMENT_TIMEOUT")
    mv_count_raw = kv.get("MV_COUNT")
    try:
        payload["mv_count"] = int(mv_count_raw) if mv_count_raw is not None else None
    except Exception:
        payload["mv_count"] = None

    if kv.get("NO_DB_URL_AVAILABLE") == "true" or cp.returncode != 0 and not kv.get("CURRENT_USER"):
        add_warning(warnings, "codex_ro_unavailable_or_failed")

    current_user = payload["current_user"]
    readonly_state = str(payload["readonly_state"] or "").strip().lower()
    if current_user and current_user != EXPECTED_CODEX_RO_USER:
        add_blocker(blockers, "codex_ro_user_mismatch")
    if payload["readonly_state"] is not None and readonly_state != "on":
        add_blocker(blockers, "codex_ro_not_readonly")
    if payload["readonly_state"] is None and payload["executed"]:
        add_warning(warnings, "codex_ro_missing_readonly_state")

    payload["warnings"] = list(warnings)
    payload["verdict"] = "block" if blockers else "warn" if warnings else "ok"
    return payload, warnings, blockers


def detect_artifacts(root: Path) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    artifact_dir = root / "Supabase" / "artifacts"
    detected: list[str] = []
    if artifact_dir.exists():
        for pattern in ARTIFACT_PATTERNS:
            matches = sorted(artifact_dir.glob(pattern), key=lambda p: str(p).lower())
            if matches:
                detected.append(str(matches[-1]))
    payload = {"detected": detected, "warnings": warnings}
    return payload, warnings


def build_next_safe_action(phase: str, has_blockers: bool) -> str:
    if has_blockers:
        return f"Resolver blockers del preflight antes de continuar con la fase {phase}."
    return PHASE_NEXT_ACTION[phase]


def write_txt_summary(path: Path, payload: dict[str, Any]) -> None:
    blockers = payload.get("blockers", [])
    warnings = payload.get("warnings", [])
    scanner_paths = payload.get("scanner", {}).get("report_paths", {})
    lines = [
        f"phase: {payload.get('phase')}",
        f"final_verdict: {payload.get('final_verdict')}",
        "blockers:",
    ]
    if blockers:
        lines.extend(f"- {item}" for item in blockers)
    else:
        lines.append("- <none>")
    lines.append("warnings:")
    if warnings:
        lines.extend(f"- {item}" for item in warnings)
    else:
        lines.append("- <none>")
    lines.extend(
        [
            f"next_safe_action: {payload.get('next_safe_action')}",
            f"scanner_txt: {scanner_paths.get('txt', '')}",
            f"scanner_json: {scanner_paths.get('json', '')}",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()

    git_data, git_warnings, git_blockers = git_probe(
        root,
        require_clean=bool(args.require_clean_git),
        expected_head=str(args.expected_head or ""),
    )
    scanner_data, scanner_warnings, scanner_blockers = scanner_probe(root, args.scanner_scope)
    kernels_data, kernel_warnings, kernel_blockers = kernel_probe(root, args.phase, git_data["head"])
    codex_ro_data, codex_ro_warnings, codex_ro_blockers = codex_ro_probe(root, args.phase, bool(args.skip_db))
    artifacts_data, artifact_warnings = detect_artifacts(root)

    db_skipped = args.skip_db or args.phase not in CODex_RO_PHASES
    if args.skip_db:
        db_reason = "skip_db_requested"
    elif args.phase not in CODex_RO_PHASES:
        db_reason = "phase_not_db_enabled"
    else:
        db_reason = ""

    blockers: list[str] = []
    warnings: list[str] = []
    for group in (git_blockers, scanner_blockers, kernel_blockers, codex_ro_blockers):
        for item in group:
            add_blocker(blockers, item)
    for group in (git_warnings, scanner_warnings, kernel_warnings, codex_ro_warnings, artifact_warnings):
        for item in group:
            add_warning(warnings, item)

    final_verdict = "block" if blockers else "warn" if warnings else "ok"
    next_safe_action = build_next_safe_action(args.phase, bool(blockers))

    payload = {
        "preflight_version": PREFLIGHT_VERSION,
        "phase": args.phase,
        "timestamp_local": now_local_iso(),
        "root": str(root),
        "git": git_data,
        "scanner": scanner_data,
        "kernels": kernels_data,
        "codex_ro": codex_ro_data,
        "db_optional": {
            "skipped": bool(db_skipped),
            "reason": db_reason,
        },
        "artifacts": artifacts_data,
        "blockers": blockers,
        "warnings": warnings,
        "next_safe_action": next_safe_action,
        "final_verdict": final_verdict,
    }

    if args.json_out:
        json_path = Path(args.json_out)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.txt_out:
        txt_path = Path(args.txt_out)
        txt_path.parent.mkdir(parents=True, exist_ok=True)
        write_txt_summary(txt_path, payload)

    print(json.dumps(payload, ensure_ascii=False, indent=2))

    if final_verdict == "block":
        return 1
    if args.fail_on_warnings and warnings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
