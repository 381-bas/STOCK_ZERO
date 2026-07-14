from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
KERNEL_DIR = ROOT / "governance" / "kernel" / "current"
MANIFEST = KERNEL_DIR / "00_kernel_manifest_stock_zero_v2026_06_30_011.json"
RETIRED = KERNEL_DIR / "04_project_technical_evidence_stock_zero_v2026_06_30_011.json"
PRESERVED = ROOT / "research" / "governance" / "kernel04_retirement_preserved_evidence_v1.json"
PROTECTED = {
    "01_project_kernel_stock_zero_v2026_06_16.json": "324741809e920b12ed8a34b35996014cae95af5968512c6042c362dbbfca5b92",
    "02_project_state_stock_zero_v2026_06_30_011.json": "29b522bd15f8e1d86b51059edff80401bb8d01c8fcf1b6aee66631236702b903",
    "03_project_ledger_stock_zero_v2026_06_30_011.json": "c54b6d63d871ebcbb9d3a8c3131cabeb4cf15da5c063907ccee19669b1fd6bbc",
}
SECTION_DIGESTS = {
    "cg005n_lab": "d4e4399085bc4d0556da2b45d241bfd29fc593793be199f4c77c7f8b75208005",
    "productive_drift": "eb3c2e6ef13c3d3a810678573742b4409c60ee99ffc0b54901fc485a3aac58cf",
    "control_gestion": "0bee001daa990020e568716491652580e9ce94ee8d35590bd0c880b96de1f7ae",
}


def canonical_digest(value: object) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def lf_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


class Kernel04RetirementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        cls.preserved = json.loads(PRESERVED.read_text(encoding="utf-8"))

    def test_manifest_has_only_three_current_kernel_members(self) -> None:
        self.assertEqual(set(self.manifest["files"]), {"01_project_kernel", "02_project_state", "03_project_ledger"})
        self.assertNotIn("04_project_technical_evidence", self.manifest["files"])

    def test_promotion_scope_does_not_restore_kernel04(self) -> None:
        for entry in self.manifest["promotion_scope"]:
            normalized = entry.casefold()
            self.assertNotIn("kernel 04", normalized)
            self.assertNotIn("04_project_technical_evidence", normalized)
            if "fourth kernel" in normalized:
                self.assertIn("do not maintain", normalized)
                self.assertNotIn("update a fourth kernel", normalized)

    def test_retired_file_absent_and_preservation_artifact_present(self) -> None:
        self.assertFalse(RETIRED.exists())
        self.assertTrue(PRESERVED.is_file())

    def test_required_sections_are_exactly_preserved(self) -> None:
        sections = self.preserved["preserved_sections"]
        self.assertEqual(set(sections), set(SECTION_DIGESTS))
        self.assertEqual(self.preserved["section_sha256"], SECTION_DIGESTS)
        self.assertEqual({key: canonical_digest(value) for key, value in sections.items()}, SECTION_DIGESTS)

    def test_preservation_is_explicitly_non_authoritative(self) -> None:
        self.assertEqual(
            set(self.preserved["authority_status"]),
            {"HISTORICAL_EVIDENCE_ONLY", "NOT_CURRENT_PROJECT_STATE", "NOT_PRODUCTIVE_AUTHORIZATION"},
        )
        self.assertIn("not a live authority", self.preserved["current_truth_statement"])

    def test_protected_governance_files_are_unchanged(self) -> None:
        self.assertEqual({name: lf_digest(KERNEL_DIR / name) for name in PROTECTED}, PROTECTED)

    def test_productive_authorization_remains_false(self) -> None:
        state = json.loads((KERNEL_DIR / "02_project_state_stock_zero_v2026_06_30_011.json").read_text(encoding="utf-8"))
        self.assertFalse(state["authorization"]["018_authorized"])
        self.assertFalse(state["authorization"]["apply_authorized"])
        self.assertFalse(state["authorization"]["db_writes_authorized"])


if __name__ == "__main__":
    unittest.main()
