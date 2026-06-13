#!/usr/bin/env python3
"""Deterministic STOCK_ZERO context bundle.

Reads the four versioned research contracts and emits a compact, deterministic
JSON context for one domain. No DB, Docker, network, temp files, or writes.
Standard library only.

Usage:
    python scripts/sz_context_bundle.py --domain control_gestion
    python scripts/sz_context_bundle.py --domain inventory --pretty --max-items 8

Two consecutive runs with the same files and the same Git state produce byte
-identical stdout.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BUNDLE_VERSION = "SZ_CONTEXT_BUNDLE_V1"

VALID_DOMAINS: Tuple[str, ...] = (
    "control_gestion",
    "inventory",
    "route",
    "database",
    "runtime",
    "research",
)

SOURCE_FILES: Tuple[str, ...] = (
    "research/AI_PROJECT_HORIZON.json",
    "research/AI_CAPABILITY_MAP.json",
    "research/AI_SHARED_MEMORY.json",
    "research/AI_BACKLOG.json",
)

MAX_ITEMS_DEFAULT = 12
MAX_ITEMS_MIN = 1
MAX_ITEMS_MAX = 25

# Stable status ordering for listing (priority first, then id alphabetical).
STATUS_RANK: Dict[str, int] = {
    "READY": 0,
    "INVESTIGATE": 1,
    "VALIDATED": 2,
    "VALIDATED_LOCAL": 2,
    "VALIDATED_WITH_CORRECTIONS": 2,
    "SHADOW_READY": 2,
    "SHADOW_READY_NO_PROD_WRITES": 2,
    "PARTIAL": 3,
    "DISPUTED": 4,
    "REJECTED": 5,
    "BLOCKED": 6,
}
DEFAULT_RANK = 9

# Recommendation only considers actionable, non-settled work; BLOCKED is never chosen.
RECOMMENDATION_RANK: Dict[str, int] = {"READY": 0, "INVESTIGATE": 1, "PARTIAL": 2}

VALIDATED_STATUSES = {"VALIDATED", "VALIDATED_LOCAL", "VALIDATED_WITH_CORRECTIONS"}
PARTIAL_STATUSES = {"PARTIAL", "DISPUTED"}

BASE_FORBIDDEN_ACTIONS: Tuple[str, ...] = (
    "No Supabase writes",
    "No production apply",
    "No cleanup",
    "No retention",
    "No payload_json removal",
    "No DB or Docker execution",
    "No loader, refresh, or SQL execution without explicit authorization",
    "No commit or push unless the task explicitly authorizes the exact files",
    "Never treat 'latest' raw or 'latest' route as a temporal contract",
)

# Validated route policy token. When present in shared memory or backlog, the
# route-snapshot question is resolved and must not surface as a pending decision.
ROUTE_POLICY_VALIDATED_TOKEN = "ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1"
ROUTE_POLICY_DECISION_ID = "BDP-ROUTE-POLICY"

# Operational gates that replace the legacy H0 parity gates once the canonical
# builder is shadow-ready (control_gestion only). Stable order.
OPERATIONAL_CG_GATES: Tuple[str, ...] = (
    "CG-005 requiere una fase explícitamente autorizada de dual-run shadow sobre una semana real.",
    "CG-006 build registry y forward apply permanece en investigación y depende del shadow.",
    "CG-007 production canary permanece bloqueado hasta completar shadow, diseño y autorización explícita.",
    "DB-002 mantiene bloqueados cleanup, retention, payload_json removal y Supabase writes.",
)

HORIZON_LAG_WARNING = (
    "AI_PROJECT_HORIZON H0 precedes C006/C007 shadow readiness; "
    "backlog and shared memory contain the newer operational gate."
)


# Explicit, deterministic per-domain configuration. No semantic/model search.
DomainConfig = Dict[str, Any]

DOMAIN_CONFIG: Dict[str, DomainConfig] = {
    "control_gestion": {
        "backlog_categories": {"CONTROL_GESTION"},
        "backlog_id_prefixes": ("CG-",),
        "extra_backlog_ids": {"DB-002"},
        "data_title_tokens": set(),
        "capability_domains": {
            "CONTROL_GESTION",
            "CONTROL_GESTION_TEMPORAL_CONTRACT",
            "SHARED_ROUTE_MASTER_AND_CONTROL_GESTION",
        },
        "capability_categories": set(),
        "capability_name_tokens": {
            "daily", "weekly", "precedence", "route", "snapshot",
            "parity", "canonical", "alert",
        },
        "shared_memory_sections": [
            "control_gestion",
            "c002_b0_parity_rootcause",
            "c006_c007_canonical_builder",
        ],
        "runtime_files": [
            "app/screens/control_gestion.py",
            "app/db.py",
            "scripts/refresh_control_gestion_v2_incremental.py",
            "scripts/cg_canonical_build_local.py",
            "scripts/load_control_gestion_raw_v17.py",
        ],
    },
    "inventory": {
        "backlog_categories": {"INVENTORY"},
        "backlog_id_prefixes": (),
        "extra_backlog_ids": set(),
        # DATA backlog items are included only when their title is inventory/stock-related.
        "data_title_tokens": {"INVENTARIO", "HISTORICO", "STOCK", "B2B"},
        "capability_domains": {"INVENTORY"},
        "capability_categories": set(),
        "capability_name_tokens": {"inventory", "stock", "b2b", "fact_stock"},
        "shared_memory_sections": ["data_lineage_c001"],
        "runtime_files": [
            "app/screens/reposicion.py",
            "app/screens/cliente.py",
            "app/services/stock.py",
            "scripts/load_fact_from_excel.py",
        ],
    },
    "route": {
        "backlog_categories": {"ROUTE"},
        "backlog_id_prefixes": ("ROUTE-",),
        "extra_backlog_ids": {"ARCH-002"},
        "data_title_tokens": set(),
        "capability_domains": {
            "SHARED_ROUTE_MASTER",
            "SHARED_ROUTE_MASTER_AND_CONTROL_GESTION",
        },
        "capability_categories": {"SHARED_REFERENCE_DATA"},
        "capability_name_tokens": {"route", "ruta", "snapshot"},
        "shared_memory_sections": [],
        "runtime_files": [
            "scripts/load_ruta_rutero_from_excel.py",
            "scripts/cg_canonical_build_local.py",
        ],
    },
    "database": {
        "backlog_categories": {"DB"},
        "backlog_id_prefixes": ("DB-",),
        "extra_backlog_ids": set(),
        "data_title_tokens": set(),
        "capability_domains": set(),
        "capability_categories": {"PERSISTENCE_ADAPTER"},
        "capability_name_tokens": {"payload", "mart", "persistence", "catalog", "raw", "storage", "extract"},
        "shared_memory_sections": [],
        "runtime_files": [
            "app/db.py",
            "scripts/cg_readonly_extract.py",
        ],
    },
    "runtime": {
        "backlog_categories": {"CODE", "RUNTIME", "ARCHITECTURE"},
        "backlog_id_prefixes": (),
        "extra_backlog_ids": set(),
        "data_title_tokens": set(),
        "capability_domains": {"STOCK_ZERO_LEGACY_APP", "RUNTIME_OPERATIONS", "APP_GOVERNANCE"},
        "capability_categories": {"PRESENTATION"},
        "capability_name_tokens": set(),
        "shared_memory_sections": [
            "runtime_map",
            "reposicion",
            "cliente",
            "possibly_unused_static_only",
        ],
        "runtime_files": [
            "streamlit_app.py",
            "app/Home.py",
            "app/screens/control_gestion.py",
            "app/screens/cliente.py",
            "app/screens/reposicion.py",
            "app/services/stock.py",
            "app/db.py",
        ],
    },
    "research": {
        "backlog_categories": {"MEMORY", "RESEARCH", "GOVERNANCE"},
        "backlog_id_prefixes": (),
        "extra_backlog_ids": set(),
        "data_title_tokens": set(),
        "capability_domains": {"RESEARCH_AND_VALIDATION", "CONTRACT_GOVERNANCE", "REPRODUCIBILITY_LAB"},
        "capability_categories": {"INTERNAL_TOOLING", "LABORATORY"},
        "capability_name_tokens": set(),
        "shared_memory_sections": ["c001_validated_research", "data_lineage_c001"],
        "runtime_files": [
            "research/AI_PROJECT_HORIZON.json",
            "research/AI_CAPABILITY_MAP.json",
            "research/AI_SHARED_MEMORY.json",
            "research/AI_BACKLOG.json",
            "research/AI_FINDINGS_LEDGER.jsonl",
        ],
    },
}


class DomainError(Exception):
    """Raised for an invalid or missing domain."""


def _status_sort_key(status: Optional[str], ident: str) -> Tuple[int, str]:
    return (STATUS_RANK.get(status or "", DEFAULT_RANK), ident)


def read_json(path: Path, warnings: List[str]) -> Dict[str, Any]:
    """Read a JSON file, returning {} and a warning on any failure."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        warnings.append("source_missing:" + path.name)
        return {}
    except OSError as exc:
        warnings.append("source_read_error:" + path.name + ":" + exc.__class__.__name__)
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        warnings.append("source_parse_error:" + path.name)
        return {}
    if not isinstance(data, dict):
        warnings.append("source_not_object:" + path.name)
        return {}
    return data


def sha256_of(path: Path) -> Optional[str]:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def run_git(repo_root: Path, args: List[str]) -> Tuple[Optional[str], bool]:
    """Run a git command read-only. Returns (stdout_or_None, ok)."""
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None, False
    if proc.returncode != 0:
        return None, False
    return proc.stdout, True


def collect_git(repo_root: Path) -> Dict[str, Any]:
    warnings: List[str] = []
    head, ok = run_git(repo_root, ["rev-parse", "HEAD"])
    head_val = head.strip() if (ok and head) else None
    if head_val is None:
        warnings.append("git_head_unavailable")

    status_out, ok = run_git(repo_root, ["status", "--short"])
    status_lines: List[str] = []
    if ok and status_out is not None:
        status_lines = [ln for ln in status_out.splitlines() if ln.strip()]
    else:
        warnings.append("git_status_unavailable")

    log_out, ok = run_git(repo_root, ["log", "--oneline", "-5"])
    commits: List[str] = []
    if ok and log_out is not None:
        commits = [ln for ln in log_out.splitlines() if ln.strip()]
    else:
        warnings.append("git_log_unavailable")

    return {
        "head": head_val,
        "status": status_lines,
        "recent_commits": commits,
        "warnings": warnings,
    }


def _backlog_item_status(backlog_doc: Dict[str, Any], item_id: str) -> Optional[str]:
    items = backlog_doc.get("items")
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict) and it.get("id") == item_id:
                return str(it.get("status")) if it.get("status") is not None else None
    return None


def is_shadow_ready(memory_doc: Dict[str, Any], backlog_doc: Dict[str, Any]) -> bool:
    """Shadow-ready when the canonical builder reports it or CG-005 is READY."""
    builder = memory_doc.get("c006_c007_canonical_builder")
    if isinstance(builder, dict) and builder.get("status") == "SHADOW_READY_NO_PROD_WRITES":
        return True
    return _backlog_item_status(backlog_doc, "CG-005") == "READY"


def route_policy_validated(memory_doc: Dict[str, Any], backlog_doc: Dict[str, Any]) -> bool:
    """True when the validated route policy token appears in either contract."""
    for doc in (memory_doc, backlog_doc):
        try:
            if ROUTE_POLICY_VALIDATED_TOKEN in json.dumps(doc, ensure_ascii=False):
                return True
        except (TypeError, ValueError):
            continue
    return False


def horizon_is_h0_parity(horizon_doc: Dict[str, Any]) -> bool:
    ah = active_horizon(horizon_doc)
    if ah.get("id") == "H0":
        return True
    return "Parity Closure" in str(ah.get("name") or "")


def active_horizon(horizon_doc: Dict[str, Any]) -> Dict[str, Any]:
    horizons = horizon_doc.get("horizons")
    if not isinstance(horizons, list):
        return {}
    for item in horizons:
        if isinstance(item, dict) and item.get("status") == "ACTIVE":
            return {
                "id": item.get("id"),
                "name": item.get("name"),
                "status": item.get("status"),
                "objective": item.get("objective"),
            }
    return {}


def active_horizon_gates(horizon_doc: Dict[str, Any]) -> List[str]:
    horizons = horizon_doc.get("horizons")
    if not isinstance(horizons, list):
        return []
    for item in horizons:
        if isinstance(item, dict) and item.get("status") == "ACTIVE":
            gates = item.get("gates")
            if isinstance(gates, list):
                return [str(g) for g in gates]
    return []


def bastian_unknowns(
    horizon_doc: Dict[str, Any], max_items: int, drop_ids: Tuple[str, ...] = ()
) -> List[Dict[str, Any]]:
    points = horizon_doc.get("bastian_decision_points")
    out: List[Dict[str, Any]] = []
    if isinstance(points, list):
        for p in points:
            if isinstance(p, dict) and p.get("id") not in drop_ids:
                out.append({"id": p.get("id"), "question": p.get("question")})
    out.sort(key=lambda d: str(d.get("id") or ""))
    return out[:max_items]


def _backlog_item_in_domain(item: Dict[str, Any], cfg: DomainConfig) -> bool:
    ident = str(item.get("id") or "")
    category = item.get("category")
    title = str(item.get("title") or "").upper()

    if ident in cfg["extra_backlog_ids"]:
        return True
    if category in cfg["backlog_categories"]:
        # DATA items only count when their title matches the configured tokens.
        if category == "DATA" and cfg["data_title_tokens"]:
            return any(tok in title for tok in cfg["data_title_tokens"])
        return True
    for prefix in cfg["backlog_id_prefixes"]:
        if ident.startswith(prefix):
            return True
    # DATA-linked-to-stock case for inventory: category DATA with token match.
    if category == "DATA" and cfg["data_title_tokens"]:
        return any(tok in title for tok in cfg["data_title_tokens"])
    return False


def project_backlog_item(item: Dict[str, Any]) -> Dict[str, Any]:
    proj: Dict[str, Any] = {
        "id": item.get("id"),
        "title": item.get("title"),
        "status": item.get("status"),
    }
    if "required_next_action" in item:
        proj["required_next_action"] = item.get("required_next_action")
    if "implementation_authorized" in item:
        proj["implementation_authorized"] = item.get("implementation_authorized")
    return proj


def select_domain_backlog(backlog_doc: Dict[str, Any], cfg: DomainConfig) -> List[Dict[str, Any]]:
    items = backlog_doc.get("items")
    if not isinstance(items, list):
        return []
    selected = [it for it in items if isinstance(it, dict) and _backlog_item_in_domain(it, cfg)]
    selected.sort(key=lambda it: _status_sort_key(it.get("status"), str(it.get("id") or "")))
    return selected


def capability_in_domain(cap: Dict[str, Any], cfg: DomainConfig) -> bool:
    domain = cap.get("domain")
    category = cap.get("category")
    name = str(cap.get("capability") or "").lower()
    if domain in cfg["capability_domains"]:
        return True
    if category in cfg["capability_categories"]:
        return True
    if cfg["capability_name_tokens"] and any(tok in name for tok in cfg["capability_name_tokens"]):
        return True
    return False


def project_capability(cap: Dict[str, Any]) -> Dict[str, Any]:
    proj: Dict[str, Any] = {
        "capability": cap.get("capability"),
        "domain": cap.get("domain"),
        "target_state": cap.get("target_state"),
        "validation_status": cap.get("validation_status"),
    }
    if "implementation_authorized" in cap:
        proj["implementation_authorized"] = cap.get("implementation_authorized")
    return proj


def select_capabilities(cap_doc: Dict[str, Any], cfg: DomainConfig) -> List[Dict[str, Any]]:
    caps = cap_doc.get("capabilities")
    if not isinstance(caps, list):
        return []
    selected = [c for c in caps if isinstance(c, dict) and capability_in_domain(c, cfg)]
    selected.sort(key=lambda c: _status_sort_key(c.get("validation_status"), str(c.get("capability") or "")))
    return selected


def _short(text: Any, limit: int = 240) -> Optional[str]:
    if text is None:
        return None
    s = str(text).strip()
    if len(s) > limit:
        s = s[: limit - 1].rstrip() + "…"
    return s


def shared_memory_facts(
    memory_doc: Dict[str, Any], cfg: DomainConfig
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (validated_facts, partial_or_disputed) drawn from mapped sections."""
    validated: List[Dict[str, Any]] = []
    partial: List[Dict[str, Any]] = []
    for key in cfg["shared_memory_sections"]:
        section = memory_doc.get(key)
        if not isinstance(section, dict):
            continue
        status = section.get("status")
        statement = (
            section.get("finding")
            or section.get("normal_path")
            or section.get("root_cause")
            or section.get("meaning")
        )
        if statement is None:
            vfs = section.get("validated_facts") or section.get("validated_findings")
            if isinstance(vfs, list) and vfs:
                first = vfs[0]
                if isinstance(first, dict):
                    statement = first.get("statement") or first.get("claim")
                else:
                    statement = first
        fact = {"id": key, "statement": _short(statement) or "see section", "status": status}
        rank = STATUS_RANK.get(str(status or ""), DEFAULT_RANK)
        if rank <= 2:
            validated.append(fact)
        elif str(status or "") in PARTIAL_STATUSES:
            partial.append(fact)
        else:
            validated.append(fact)
    validated.sort(key=lambda d: str(d.get("id") or ""))
    partial.sort(key=lambda d: str(d.get("id") or ""))
    return validated, partial


def build_validated_and_partial(
    domain_backlog: List[Dict[str, Any]],
    memory_doc: Dict[str, Any],
    cfg: DomainConfig,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    validated: List[Dict[str, Any]] = []
    partial: List[Dict[str, Any]] = []
    for it in domain_backlog:
        status = str(it.get("status") or "")
        fact = {"id": it.get("id"), "statement": _short(it.get("title")), "status": status}
        if status in VALIDATED_STATUSES:
            validated.append(fact)
        elif status in PARTIAL_STATUSES:
            partial.append(fact)
    sm_validated, sm_partial = shared_memory_facts(memory_doc, cfg)
    validated.extend(sm_validated)
    partial.extend(sm_partial)
    validated.sort(key=lambda d: str(d.get("id") or ""))
    partial.sort(key=lambda d: str(d.get("id") or ""))
    return validated, partial


def recommend_next_task(domain_backlog: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Derive the next task from backlog only. BLOCKED is never chosen."""
    candidates = [
        it for it in domain_backlog if str(it.get("status") or "") in RECOMMENDATION_RANK
    ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda it: (
            RECOMMENDATION_RANK[str(it.get("status"))],
            str(it.get("id") or ""),
        )
    )
    chosen = candidates[0]
    ident = chosen.get("id")
    title = chosen.get("title")
    status = str(chosen.get("status"))
    authorized = bool(chosen.get("implementation_authorized", False))

    if status == "READY":
        action = "Preparar una fase explicitamente autorizada para " + str(ident) + ": " + str(title) + "."
    elif status == "INVESTIGATE":
        action = "Disenar (design-only) " + str(ident) + ": " + str(title) + "."
    else:  # PARTIAL
        action = "Continuar la investigacion de " + str(ident) + ": " + str(title) + "."

    return {
        "id": ident,
        "action": action,
        "status": status,
        "implementation_authorized": authorized,
    }


def build_current_gates(
    domain: str,
    horizon_doc: Dict[str, Any],
    domain_backlog: List[Dict[str, Any]],
    max_items: int,
    shadow_ready: bool,
) -> List[str]:
    # Once the canonical builder is shadow-ready, control_gestion reports the
    # operational gates (CG-005..CG-007, DB-002), not the legacy H0 parity gates.
    if domain == "control_gestion" and shadow_ready:
        return list(OPERATIONAL_CG_GATES)[:max_items]

    gates = list(active_horizon_gates(horizon_doc))
    for it in domain_backlog:
        if it.get("cleanup_blocked") is True:
            gates.append(
                "Cleanup/retention blocked on " + str(it.get("id")) + " until G0 closes and explicit authorization."
            )
    # De-duplicate while preserving order.
    seen: set = set()
    unique: List[str] = []
    for g in gates:
        if g not in seen:
            seen.add(g)
            unique.append(g)
    return unique[:max_items]


def build_runtime_files(repo_root: Path, cfg: DomainConfig) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rel in cfg["runtime_files"]:
        out.append({"path": rel, "exists": (repo_root / rel).exists()})
    return out


def build_bundle(domain: str, repo_root: Path, max_items: int) -> Dict[str, Any]:
    warnings: List[str] = []
    cfg = DOMAIN_CONFIG[domain]

    sources: List[Dict[str, Any]] = []
    docs: Dict[str, Dict[str, Any]] = {}
    for rel in SOURCE_FILES:
        path = repo_root / rel
        sources.append({"path": rel, "sha256": sha256_of(path)})
        docs[rel] = read_json(path, warnings)

    horizon_doc = docs["research/AI_PROJECT_HORIZON.json"]
    capability_doc = docs["research/AI_CAPABILITY_MAP.json"]
    memory_doc = docs["research/AI_SHARED_MEMORY.json"]
    backlog_doc = docs["research/AI_BACKLOG.json"]

    git_info = collect_git(repo_root)

    shadow_ready = is_shadow_ready(memory_doc, backlog_doc)
    route_resolved = route_policy_validated(memory_doc, backlog_doc)

    domain_backlog = select_domain_backlog(backlog_doc, cfg)
    validated, partial = build_validated_and_partial(domain_backlog, memory_doc, cfg)
    capabilities = select_capabilities(capability_doc, cfg)
    recommended = recommend_next_task(domain_backlog)
    gates = build_current_gates(domain, horizon_doc, domain_backlog, max_items, shadow_ready)

    # Resolved route policy must not appear as a pending Bastian decision.
    drop_ids: Tuple[str, ...] = (ROUTE_POLICY_DECISION_ID,) if route_resolved else ()

    # Flag a horizon that still leads with H0 parity while the backlog already
    # carries the newer operational gate (CG-005 READY).
    if horizon_is_h0_parity(horizon_doc) and _backlog_item_status(backlog_doc, "CG-005") == "READY":
        warnings.append(HORIZON_LAG_WARNING)

    bundle: Dict[str, Any] = {
        "bundle_version": BUNDLE_VERSION,
        "domain": domain,
        "sources": sources,
        "git": git_info,
        "active_horizon": active_horizon(horizon_doc),
        "validated_facts": validated[:max_items],
        "partial_or_disputed": partial[:max_items],
        "capabilities": [project_capability(c) for c in capabilities[:max_items]],
        "backlog": [project_backlog_item(it) for it in domain_backlog[:max_items]],
        "runtime_files": build_runtime_files(repo_root, cfg),
        "current_gates": gates,
        "forbidden_actions": list(BASE_FORBIDDEN_ACTIONS),
        "recommended_next_task": recommended,
        "unknowns": bastian_unknowns(horizon_doc, max_items, drop_ids),
        "warnings": warnings,
    }
    return bundle


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sz_context_bundle",
        description="Deterministic STOCK_ZERO context bundle for one domain.",
        add_help=True,
    )
    parser.add_argument("--domain", dest="domain", default=None)
    parser.add_argument("--max-items", dest="max_items", type=int, default=MAX_ITEMS_DEFAULT)
    parser.add_argument("--pretty", dest="pretty", action="store_true")
    parser.add_argument("--repo-root", dest="repo_root", default=None)
    return parser


def _compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def _emit(bundle: Dict[str, Any], pretty: bool) -> None:
    """Emit JSON to stdout.

    Pretty mode keeps one top-level key per line and renders each collection
    element as a single compact line. This stays valid JSON, deterministic, and
    well under the 150-line budget while remaining readable.
    """
    if not pretty:
        sys.stdout.write(_compact(bundle) + "\n")
        return

    lines: List[str] = ["{"]
    keys = list(bundle.keys())
    for ki, key in enumerate(keys):
        value = bundle[key]
        key_comma = "," if ki < len(keys) - 1 else ""
        prefix = "  " + json.dumps(key, ensure_ascii=False) + ": "
        if isinstance(value, list):
            if not value:
                lines.append(prefix + "[]" + key_comma)
            else:
                lines.append(prefix + "[")
                for ei, element in enumerate(value):
                    el_comma = "," if ei < len(value) - 1 else ""
                    lines.append("    " + _compact(element) + el_comma)
                lines.append("  ]" + key_comma)
        else:
            lines.append(prefix + _compact(value) + key_comma)
    lines.append("}")
    sys.stdout.write("\n".join(lines) + "\n")


def _emit_error(domain: Optional[str], message: str, pretty: bool) -> None:
    err = {
        "bundle_version": BUNDLE_VERSION,
        "error": "invalid_domain",
        "domain": domain,
        "valid_domains": list(VALID_DOMAINS),
        "message": message,
    }
    _emit(err, pretty)


def _ensure_utf8_stdout() -> None:
    """Force UTF-8 stdout so accented gate text survives a non-UTF-8 console.

    No-op when stdout has no reconfigure (e.g. io.StringIO under tests).
    """
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        try:
            reconfigure(encoding="utf-8")
        except (ValueError, OSError):
            pass


def run(argv: List[str]) -> int:
    _ensure_utf8_stdout()
    parser = build_parser()
    # argparse writes usage to stderr on parse errors; for this tool we keep
    # stderr empty and report argument problems as a JSON error on stdout.
    try:
        args, _unknown = parser.parse_known_args(argv)
    except SystemExit:
        _emit_error(None, "argument parsing failed", pretty=True)
        return 2

    pretty = bool(getattr(args, "pretty", False))

    domain = args.domain
    if domain is None or domain not in VALID_DOMAINS:
        _emit_error(domain, "domain must be one of: " + ", ".join(VALID_DOMAINS), pretty)
        return 2

    max_items = args.max_items
    if not isinstance(max_items, int):
        max_items = MAX_ITEMS_DEFAULT
    clamped = max(MAX_ITEMS_MIN, min(MAX_ITEMS_MAX, max_items))

    if args.repo_root is not None:
        repo_root = Path(args.repo_root).resolve()
    else:
        repo_root = default_repo_root()

    try:
        bundle = build_bundle(domain, repo_root, clamped)
    except Exception as exc:  # never leak a traceback to the user
        _emit_error(domain, "internal_error:" + exc.__class__.__name__, pretty)
        return 1

    if clamped != args.max_items:
        bundle["warnings"].append(
            "max_items_clamped:" + str(args.max_items) + "->" + str(clamped)
        )

    _emit(bundle, pretty)
    return 0


def main() -> int:
    return run(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
