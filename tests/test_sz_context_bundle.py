"""Tests for scripts/sz_context_bundle.py.

Deterministic, fixture-based. No production JSON is read as the only strategy:
every behavioural test builds a minimal repo in a TemporaryDirectory.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List, Tuple

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "sz_context_bundle.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("sz_context_bundle", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MOD = _load_module()


def run_bundle(args: List[str]) -> Tuple[int, str]:
    """Invoke run() in-process, capturing stdout and the exit code."""
    buf = io.StringIO()
    err = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(err):
        code = MOD.run(args)
    # stderr must stay empty for this tool.
    assert err.getvalue() == "", "stderr was not empty: " + err.getvalue()
    return code, buf.getvalue()


def write_repo(
    root: Path,
    horizon: Dict[str, Any] | None = None,
    capabilities: Dict[str, Any] | None = None,
    memory: Dict[str, Any] | None = None,
    backlog: Dict[str, Any] | None = None,
    omit: Tuple[str, ...] = (),
) -> None:
    research = root / "research"
    research.mkdir(parents=True, exist_ok=True)
    files = {
        "AI_PROJECT_HORIZON.json": horizon if horizon is not None else {"horizons": []},
        "AI_CAPABILITY_MAP.json": capabilities if capabilities is not None else {"capabilities": []},
        "AI_SHARED_MEMORY.json": memory if memory is not None else {},
        "AI_BACKLOG.json": backlog if backlog is not None else {"items": []},
    }
    for name, content in files.items():
        if name in omit:
            continue
        (research / name).write_text(json.dumps(content, ensure_ascii=False), encoding="utf-8")


def default_horizon() -> Dict[str, Any]:
    return {
        "horizons": [
            {
                "id": "H0",
                "name": "Parity Closure",
                "status": "ACTIVE",
                "objective": "Close parity.",
                "gates": ["Daily parity exact", "Weekly from staged daily"],
            },
            {"id": "H1", "name": "Next", "status": "NEXT_DIRECTION", "gates": ["later"]},
        ],
        "bastian_decision_points": [
            {"id": "BDP-ROUTE-POLICY", "question": "Which route policy?"},
        ],
    }


def cg_backlog() -> Dict[str, Any]:
    return {
        "items": [
            {"id": "CG-002", "title": "Temporal contract", "category": "CONTROL_GESTION",
             "status": "VALIDATED", "implementation_authorized": False,
             "required_next_action": "Use canonical contract."},
            {"id": "CG-003", "title": "Route snapshot policy", "category": "CONTROL_GESTION",
             "status": "VALIDATED_LOCAL", "implementation_authorized": False},
            {"id": "CG-004", "title": "Weekly from daily staged", "category": "CONTROL_GESTION",
             "status": "VALIDATED_LOCAL", "implementation_authorized": False},
            {"id": "CG-005", "title": "Dual-run shadow real week", "category": "CONTROL_GESTION",
             "status": "READY", "implementation_authorized": False,
             "required_next_action": "Run authorized real-week shadow."},
            {"id": "CG-006", "title": "Build registry design", "category": "CONTROL_GESTION",
             "status": "INVESTIGATE", "implementation_authorized": False},
            {"id": "CG-007", "title": "Production canary", "category": "CONTROL_GESTION",
             "status": "BLOCKED", "implementation_authorized": False},
            {"id": "DB-002", "title": "Raw duplication justification", "category": "DB",
             "status": "BLOCKED", "implementation_authorized": False, "cleanup_blocked": True},
            {"id": "UNREL-1", "title": "Unrelated", "category": "PRODUCT",
             "status": "INVESTIGATE", "implementation_authorized": False},
        ]
    }


def shadow_ready_memory() -> Dict[str, Any]:
    """Shared memory mirroring the post-C006/C007 shadow-ready regime."""
    return {
        "control_gestion": {"status": "PARTIAL", "normal_path": "CONTROL_GESTION v2"},
        "c006_c007_canonical_builder": {
            "status": "SHADOW_READY_NO_PROD_WRITES",
            "canonical_contract": {"route_policy_version": "ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1"},
        },
    }


def route_policy_horizon() -> Dict[str, Any]:
    """Horizon still on H0, with the route-policy decision point still listed."""
    h = default_horizon()
    h["bastian_decision_points"] = [
        {"id": "BDP-ROUTE-POLICY", "question": "Which route policy?"},
        {"id": "BDP-DOMAIN-SEAM", "question": "Approve domain seam?"},
    ]
    return h


class InvalidDomainTests(unittest.TestCase):
    def test_invalid_domain_exit_2_json_stdout(self) -> None:
        code, out = run_bundle(["--domain", "nope"])
        self.assertEqual(code, 2)
        payload = json.loads(out)
        self.assertEqual(payload["error"], "invalid_domain")
        self.assertEqual(payload["domain"], "nope")
        self.assertIn("control_gestion", payload["valid_domains"])

    def test_missing_domain_exit_2(self) -> None:
        code, out = run_bundle([])
        self.assertEqual(code, 2)
        payload = json.loads(out)
        self.assertEqual(payload["error"], "invalid_domain")
        self.assertIsNone(payload["domain"])


class ValidDomainTests(unittest.TestCase):
    def test_valid_domain_ok(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=cg_backlog())
            code, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
            self.assertEqual(code, 0)
            payload = json.loads(out)
            self.assertEqual(payload["bundle_version"], "SZ_CONTEXT_BUNDLE_V1")
            self.assertEqual(payload["domain"], "control_gestion")
            self.assertEqual(payload["active_horizon"]["id"], "H0")
            self.assertTrue(payload["current_gates"])

    def test_legacy_gates_present_without_shadow(self) -> None:
        # No CG-005 READY and no canonical-builder memory -> legacy horizon gates.
        backlog = {"items": [{"id": "CG-001", "title": "x", "category": "CONTROL_GESTION", "status": "VALIDATED"}]}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=backlog)
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
            self.assertEqual(json.loads(out)["current_gates"][0], "Daily parity exact")

    def test_missing_source_files_warn_not_crash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, omit=("AI_BACKLOG.json", "AI_CAPABILITY_MAP.json"))
            code, out = run_bundle(["--domain", "inventory", "--repo-root", str(root)])
            self.assertEqual(code, 0)
            payload = json.loads(out)
            self.assertTrue(any(w.startswith("source_missing:AI_BACKLOG.json") for w in payload["warnings"]))
            # sha256 for the missing file is null
            shas = {s["path"]: s["sha256"] for s in payload["sources"]}
            self.assertIsNone(shas["research/AI_BACKLOG.json"])
            self.assertEqual(payload["backlog"], [])


class ExtractionTests(unittest.TestCase):
    def test_extraction_by_category_and_id(self) -> None:
        backlog = {
            "items": [
                {"id": "X1", "title": "by category", "category": "CONTROL_GESTION", "status": "VALIDATED"},
                {"id": "CG-99", "title": "by id prefix", "category": "OTHER", "status": "PARTIAL"},
                {"id": "DB-002", "title": "extra id", "category": "DB", "status": "BLOCKED"},
                {"id": "ZZ-1", "title": "unrelated", "category": "PRODUCT", "status": "INVESTIGATE"},
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=backlog)
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
            ids = {b["id"] for b in json.loads(out)["backlog"]}
            self.assertIn("X1", ids)        # category match
            self.assertIn("CG-99", ids)     # id-prefix match
            self.assertIn("DB-002", ids)    # extra_backlog_ids
            self.assertNotIn("ZZ-1", ids)   # unrelated excluded

    def test_inventory_data_token_filtering(self) -> None:
        backlog = {
            "items": [
                {"id": "DATA-001", "title": "Linaje de DB_GLOBAL_INVENTARIO", "category": "DATA", "status": "PARTIAL"},
                {"id": "DATA-003", "title": "Linaje de CUMPLIMIENTO_FRECUENCIA", "category": "DATA", "status": "VALIDATED"},
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=backlog)
            _, out = run_bundle(["--domain", "inventory", "--repo-root", str(root)])
            ids = {b["id"] for b in json.loads(out)["backlog"]}
            self.assertIn("DATA-001", ids)      # INVENTARIO token
            self.assertNotIn("DATA-003", ids)   # CG workbook, excluded

    def test_max_items_limit(self) -> None:
        items = [
            {"id": "CG-%02d" % i, "title": "t%d" % i, "category": "CONTROL_GESTION", "status": "VALIDATED"}
            for i in range(20)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog={"items": items})
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root), "--max-items", "5"])
            payload = json.loads(out)
            self.assertEqual(len(payload["backlog"]), 5)
            self.assertLessEqual(len(payload["validated_facts"]), 5)

    def test_max_items_clamped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=cg_backlog())
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root), "--max-items", "999"])
            payload = json.loads(out)
            self.assertTrue(any(w.startswith("max_items_clamped:999->25") for w in payload["warnings"]))

    def test_deterministic_order(self) -> None:
        # READY should sort before VALIDATED before PARTIAL before BLOCKED; ties by id.
        items = [
            {"id": "CG-200", "title": "b", "category": "CONTROL_GESTION", "status": "PARTIAL"},
            {"id": "CG-100", "title": "a", "category": "CONTROL_GESTION", "status": "READY"},
            {"id": "CG-150", "title": "c", "category": "CONTROL_GESTION", "status": "READY"},
            {"id": "CG-050", "title": "d", "category": "CONTROL_GESTION", "status": "BLOCKED"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog={"items": items})
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
            order = [b["id"] for b in json.loads(out)["backlog"]]
            self.assertEqual(order, ["CG-100", "CG-150", "CG-200", "CG-050"])

    def test_sha256_of_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=cg_backlog())
            _, out = run_bundle(["--domain", "research", "--repo-root", str(root)])
            shas = {s["path"]: s["sha256"] for s in json.loads(out)["sources"]}
            expected = hashlib.sha256((root / "research" / "AI_BACKLOG.json").read_bytes()).hexdigest()
            self.assertEqual(shas["research/AI_BACKLOG.json"], expected)


class GitTests(unittest.TestCase):
    def test_git_failing_does_not_abort(self) -> None:
        # A TemporaryDirectory is not a git repo, so git commands fail gracefully.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=cg_backlog())
            code, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
            self.assertEqual(code, 0)
            git = json.loads(out)["git"]
            self.assertIsNone(git["head"])
            self.assertEqual(git["status"], [])
            self.assertEqual(git["recent_commits"], [])
            self.assertTrue(git["warnings"])


class RecommendationTests(unittest.TestCase):
    def test_cg005_ready_before_cg006_investigate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=cg_backlog())
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
            rec = json.loads(out)["recommended_next_task"]
            self.assertEqual(rec["id"], "CG-005")
            self.assertEqual(rec["status"], "READY")
            self.assertIn("shadow", rec["action"].lower())
            self.assertIn("autorizada", rec["action"].lower())

    def test_blocked_never_recommended(self) -> None:
        # Only BLOCKED candidates -> no recommendation; BLOCKED is never chosen.
        items = [
            {"id": "CG-007", "title": "canary", "category": "CONTROL_GESTION", "status": "BLOCKED"},
            {"id": "DB-002", "title": "raw", "category": "DB", "status": "BLOCKED", "cleanup_blocked": True},
            {"id": "CG-002", "title": "done", "category": "CONTROL_GESTION", "status": "VALIDATED"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog={"items": items})
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
            self.assertIsNone(json.loads(out)["recommended_next_task"])

    def test_implementation_authorized_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=cg_backlog())
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
            backlog = {b["id"]: b for b in json.loads(out)["backlog"]}
            self.assertIn("implementation_authorized", backlog["CG-005"])
            self.assertFalse(backlog["CG-005"]["implementation_authorized"])


class ForbiddenActionsTests(unittest.TestCase):
    def test_forbidden_actions_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=cg_backlog())
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
            blob = " ".join(json.loads(out)["forbidden_actions"]).lower()
            self.assertIn("supabase writes", blob)
            self.assertIn("production apply", blob)
            self.assertIn("cleanup", blob)
            self.assertIn("retention", blob)


class DeterminismTests(unittest.TestCase):
    def test_two_identical_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), capabilities={"capabilities": []}, backlog=cg_backlog())
            _, out1 = run_bundle(["--domain", "control_gestion", "--repo-root", str(root), "--pretty"])
            _, out2 = run_bundle(["--domain", "control_gestion", "--repo-root", str(root), "--pretty"])
            self.assertEqual(out1, out2)

    def test_output_under_40kb(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=cg_backlog())
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root), "--pretty"])
            self.assertLess(len(out.encode("utf-8")), 40_000)

    def test_pretty_under_150_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=default_horizon(), backlog=cg_backlog())
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root), "--pretty"])
            self.assertLessEqual(len(out.splitlines()), 150)


class ShadowReadyRegimeTests(unittest.TestCase):
    """Corrections introduced in CLAUDE_PLATFORM_002B."""

    def _run(self) -> Dict[str, Any]:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        write_repo(root, horizon=route_policy_horizon(), memory=shadow_ready_memory(), backlog=cg_backlog())
        _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
        return json.loads(out)

    def tearDown(self) -> None:
        tmp = getattr(self, "_tmp", None)
        if tmp is not None:
            tmp.cleanup()

    def test_cg005_ready_replaces_legacy_g0_gates(self) -> None:
        gates = self._run()["current_gates"]
        self.assertTrue(gates[0].startswith("CG-005 requiere una fase"))
        blob = " ".join(gates).lower()
        for legacy in ("daily parity", "weekly parity", "g0 parity", "route policy decision"):
            self.assertNotIn(legacy, blob)

    def test_cg006_appears_as_downstream_dependency(self) -> None:
        gates = self._run()["current_gates"]
        cg006 = [g for g in gates if g.startswith("CG-006")]
        self.assertEqual(len(cg006), 1)
        self.assertIn("depende del shadow", cg006[0])

    def test_cg007_remains_blocked_gate(self) -> None:
        gates = self._run()["current_gates"]
        self.assertTrue(any(g.startswith("CG-007") and "bloqueado" in g for g in gates))

    def test_db002_remains_blocked_gate(self) -> None:
        gates = self._run()["current_gates"]
        self.assertTrue(any(g.startswith("DB-002") and "Supabase writes" in g for g in gates))

    def test_route_policy_unknown_removed_when_validated(self) -> None:
        unknown_ids = {u["id"] for u in self._run()["unknowns"]}
        self.assertNotIn("BDP-ROUTE-POLICY", unknown_ids)
        self.assertIn("BDP-DOMAIN-SEAM", unknown_ids)  # other decisions remain

    def test_route_policy_unknown_kept_when_not_validated(self) -> None:
        # Without the validated token, the route decision is still pending.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=route_policy_horizon(), memory={}, backlog={
                "items": [{"id": "CG-001", "title": "x", "category": "CONTROL_GESTION", "status": "VALIDATED"}]
            })
            _, out = run_bundle(["--domain", "control_gestion", "--repo-root", str(root)])
            unknown_ids = {u["id"] for u in json.loads(out)["unknowns"]}
            self.assertIn("BDP-ROUTE-POLICY", unknown_ids)

    def test_horizon_lag_warning_present(self) -> None:
        warnings = self._run()["warnings"]
        self.assertIn(
            "AI_PROJECT_HORIZON H0 precedes C006/C007 shadow readiness; "
            "backlog and shared memory contain the newer operational gate.",
            warnings,
        )

    def test_recommended_task_still_cg005(self) -> None:
        rec = self._run()["recommended_next_task"]
        self.assertEqual(rec["id"], "CG-005")
        self.assertEqual(rec["status"], "READY")

    def test_two_identical_runs_shadow_regime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_repo(root, horizon=route_policy_horizon(), memory=shadow_ready_memory(), backlog=cg_backlog())
            _, out1 = run_bundle(["--domain", "control_gestion", "--repo-root", str(root), "--pretty"])
            _, out2 = run_bundle(["--domain", "control_gestion", "--repo-root", str(root), "--pretty"])
            self.assertEqual(out1, out2)


class SafetyTests(unittest.TestCase):
    def test_no_heavy_imports(self) -> None:
        source = SCRIPT_PATH.read_text(encoding="utf-8")
        for forbidden in ("psycopg", "pandas", "sqlalchemy", "requests"):
            self.assertNotIn("import " + forbidden, source)
            self.assertNotIn("from " + forbidden, source)


if __name__ == "__main__":
    unittest.main()
