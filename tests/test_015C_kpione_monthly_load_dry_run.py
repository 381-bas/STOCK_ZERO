import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import load_kpione_monthly_input_015C_dry_run_no_apply as loader


EXPECTED_REAL_HASHES = {
    "load_plan_sha256": "e634b4720b66db7a3036302e2553c53d7a93475257bb27410804f6b9efe8f7cd",
    "dry_run_batch_id": "015C_e634b4720b66db7a3036302e",
    "deterministic_payload_sha256": "1f99a2b3a34fc7f721a0ea5d5836e340def688516edd871f2eeb710d496b34e4",
}

EXPECTED_REAL_COUNTS = {
    "exact_duplicate_rows_detected": 20178,
    "exact_duplicate_rows_removed": 10089,
    "same_id_same_hash_count": 1579,
    "same_id_diff_hash_count": 0,
    "event_stable_hash_conflict_count": 0,
    "cross_file_exact_photo_row_count": 10089,
}


PHOTO_COLUMNS = [
    "ID",
    "SP Item ID",
    "Holding",
    "Subcadena",
    "Codigo Local",
    "Marca",
    "Local",
    "Direccion",
    "Reponedor",
    "Fecha",
    "Fecha de subida",
    "Hora",
    "Tipo de Tarea",
    "Foto Nº/Total",
    "Comentarios",
    "Link Foto",
]


def photo_row(event_id: str, fecha: str, *, link: str | None = None) -> dict:
    default_link = "https" + f"://example.invalid/photo/{event_id}"
    return {
        "ID": event_id,
        "SP Item ID": f"SP-{event_id}",
        "Holding": "H",
        "Subcadena": "S",
        "Codigo Local": "L001",
        "Marca": "M",
        "Local": "LOCAL UNO",
        "Direccion": "D",
        "Reponedor": "R",
        "Fecha": fecha,
        "Fecha de subida": fecha,
        "Hora": "10:00",
        "Tipo de Tarea": "FOTO",
        "Foto Nº/Total": "1/1",
        "Comentarios": "",
        "Link Foto": link or default_link,
    }


def write_photo(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=PHOTO_COLUMNS)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="Fotos", index=False)


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_contracts(base: Path) -> None:
    contract_dir = base / "contracts" / "control_gestion"
    write_json(
        contract_dir / "kpione2_photo_export_contract_v1.json",
        {
            "artifact": "kpione2_photo_export_contract_v1",
            "grain_contract": {
                "input_grain": "photo_row",
                "normalized_grain": "event_row",
                "compliance_grain": "day_presence",
            },
            "event_identity": ["ID", "SP Item ID"],
        },
    )
    write_json(
        contract_dir / "operational_calendar_contract_v1.json",
        {
            "contract_id": "operational_calendar_contract_v1",
            "rules": {
                "week_shape": "monday_sunday",
                "minimum_days_in_month": 4,
            },
        },
    )


def make_photo_entry(
    base: Path,
    source_file_id: str,
    rows: list[dict],
    *,
    role: str = "include_candidate",
    invalid_date_rows: int = 0,
) -> dict:
    path = (
        base
        / "data"
        / "kpione_photo_reports"
        / "2026-06"
        / f"photo-excel-admin_{source_file_id}.xlsx"
    )
    write_photo(path, rows)
    return {
        "source_file_id": source_file_id,
        "source_file_name": path.name,
        "relative_path": path.relative_to(base).as_posix(),
        "sha256": loader.sha256_file(path),
        "size_bytes": path.stat().st_size,
        "row_count": len(rows),
        "fecha_min": rows[0]["Fecha"] if rows else None,
        "fecha_max": rows[-1]["Fecha"] if rows else None,
        "role": role,
        "invalid_date_rows": invalid_date_rows,
    }


def write_validation(
    base: Path,
    entries: list[dict],
    *,
    month_id: str = "2026-06",
    validation_mode: str = "close",
    as_of_date: str = "2026-07-11",
    verdict: str = "PASS",
    blockers: list[str] | None = None,
    warnings: list[str] | None = None,
    legacy_seen: list[str] | None = None,
    reference_sha_override: str | None = None,
) -> tuple[Path, Path]:
    token = month_id.replace("-", "_")
    reference_path = (
        base
        / "research"
        / "015_INPUT_LAYOUT_TRACEABILITY_NO_APPLY"
        / f"015_monthly_input_layout_manifest_{token}.json"
    )
    reference = {
        "phase_id": "015A_MONTHLY_INPUT_LAYOUT_TRACEABILITY_NO_APPLY",
        "month_id": month_id,
        "photo_report_files": {"files": entries},
        "ruta_rutero_reference": {"files": [], "transition_week": {}},
    }
    write_json(reference_path, reference)
    reference_sha = reference_sha_override or loader.sha256_file(reference_path)
    validation_path = (
        base
        / "research"
        / "015B_KPIONE_MONTHLY_INPUT_VALIDATOR_NO_APPLY"
        / f"015B_kpione_monthly_input_validation_{token}.json"
    )
    validation = {
        "phase_id": "015B_KPIONE_MONTHLY_INPUT_VALIDATOR_NO_DB_APPLY",
        "month_id": month_id,
        "validation_mode": validation_mode,
        "as_of_date": as_of_date,
        "verdict": verdict,
        "blockers": blockers or [],
        "warnings": warnings or [],
        "contract_reference": {
            "path": reference_path.relative_to(base).as_posix(),
            "present": True,
            "required": validation_mode == "close",
            "sha256": reference_sha,
        },
        "input_contract": {
            "legacy_and_monthly_layouts_are_not_merged": True,
            "month_semantics": "operational_month",
        },
        "discovery": {
            "legacy_files_detected_not_used": legacy_seen or [],
            "monthly_photo_dir": "data/kpione_photo_reports/2026-06",
        },
        "photo_reports": {"files": entries},
        "operational_coverage": {
            "month_id": month_id,
            "operational_coverage_start": "2026-06-01",
            "operational_coverage_end": "2026-06-28",
        },
        "operational_period_status": "CLOSED_ELIGIBLE",
    }
    write_json(validation_path, validation)
    return validation_path, reference_path


class KpioneMonthlyLoadDryRun015CTests(unittest.TestCase):
    def build_payload(self, entries: list[dict], **kwargs):
        write_validation(Path(kwargs.pop("base")), entries, **kwargs)

    def test_cli_rejects_invalid_month(self):
        self.assertEqual(loader.main(["--month", "2026-13", "--soft-exit"]), 2)

    def test_blocks_validation_evidence_blocked(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry], verdict="BLOCKED", blockers=["x"])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(payload["verdict"], "BLOCKED")
            self.assertIn("input_validation_blocked", payload["blockers"])

    def test_blocks_validation_mode_mismatch(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry], validation_mode="close")
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="open", as_of_date="2026-07-11"
            )
            self.assertIn("input_validation_mode_mismatch:close!=open", payload["blockers"])

    def test_blocks_manifest_hash_mismatch(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry], reference_sha_override="0" * 64)
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertIn("reference_manifest_sha256_mismatch", payload["blockers"])

    def test_selects_only_include_candidate(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            include = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            compare = make_photo_entry(base, "2", [photo_row("2", "2026-06-01")], role="compare_only")
            quarantine = make_photo_entry(
                base, "3", [photo_row("3", "2026-06-01")], role="quarantine_truncation"
            )
            write_validation(base, [include, compare, quarantine])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(payload["row_accounting"]["source_rows_selected_files"], 1)
            self.assertEqual(len(payload["selection"]["include_candidate_files"]), 1)

    def test_excludes_compare_only(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            include = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            compare = make_photo_entry(base, "2", [photo_row("2", "2026-06-01")], role="compare_only")
            write_validation(base, [include, compare])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(payload["selection"]["compare_only_files"][0]["source_file_id"], "2")
            self.assertEqual(payload["row_accounting"]["would_stage_rows"], 1)

    def test_excludes_quarantine(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            include = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            quarantine = make_photo_entry(
                base, "3", [photo_row("3", "2026-06-01")], role="quarantine_truncation"
            )
            write_validation(base, [include, quarantine])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(payload["selection"]["quarantine_files"][0]["source_file_id"], "3")
            self.assertEqual(payload["row_accounting"]["would_stage_rows"], 1)

    def test_unclassified_blocks_close(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")], role="unclassified")
            write_validation(base, [entry])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertIn("unclassified_role_in_close:1", payload["blockers"])

    def test_legacy_layout_mix_blocks(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry], legacy_seen=["data/photo-excel-admin_1.xlsx"])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertIn("legacy_layout_detected_alongside_monthly_layout", payload["blockers"])

    def test_normalization_compatible_with_014e_payload_columns(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(payload["semantic_plan"]["payload_layer"], "raw_candidate_photo_rows")
            self.assertIn("photo_row_hash", payload["semantic_plan"]["payload_columns"])
            self.assertEqual(payload["row_accounting"]["normalized_candidate_rows"], 1)

    def test_dedupe_exact_event_id_and_photo_row_hash(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            row = photo_row("1", "2026-06-01")
            first = make_photo_entry(base, "1", [row])
            second = make_photo_entry(base, "2", [row])
            write_validation(base, [first, second])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(payload["row_accounting"]["exact_duplicate_rows_removed"], 1)
            self.assertEqual(payload["row_accounting"]["would_stage_rows"], 1)

    def test_dedupe_keeps_first_source_identity(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            row = photo_row("1", "2026-06-01")
            first = make_photo_entry(base, "1", [row])
            second = make_photo_entry(base, "2", [row])
            write_validation(base, [second, first])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            identity = payload["batch_plan"]["batches"][0]["first_source_row_identity"]
            self.assertEqual(identity, {"source_file_id": "1", "source_row_number": 2})

    def test_preserves_different_event_ids_same_day(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            rows = [photo_row("1", "2026-06-01"), photo_row("2", "2026-06-01")]
            entry = make_photo_entry(base, "1", rows)
            write_validation(base, [entry])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(payload["row_accounting"]["would_stage_rows"], 2)
            self.assertEqual(payload["row_accounting"]["distinct_event_ids_would_stage"], 2)

    def test_dedupes_before_operational_partition(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            row = photo_row("9", "2026-06-29")
            first = make_photo_entry(base, "1", [row])
            second = make_photo_entry(base, "2", [row])
            write_validation(base, [first, second])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(payload["row_accounting"]["exact_duplicate_rows_removed"], 1)
            self.assertEqual(payload["row_accounting"]["carry_forward_out_rows"], 1)

    def test_june_excludes_29_30_from_target_batch(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(
                base,
                "1",
                [photo_row("1", "2026-06-29"), photo_row("2", "2026-06-30")],
            )
            write_validation(base, [entry])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(payload["row_accounting"]["would_stage_rows"], 0)
            self.assertEqual(payload["row_accounting"]["carry_forward_out_rows"], 2)

    def test_june_reports_carry_forward_to_july(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-29")])
            write_validation(base, [entry])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(
                payload["operational_partition"]["carry_forward_out"][0]["assigned_operational_month"],
                "2026-07",
            )

    def test_does_not_embed_historical_june_arithmetic_constants(self):
        source = (SCRIPTS / "load_kpione_monthly_input_015C_dry_run_no_apply.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("229070 - 15889", source)

    def test_quarantine_invalid_date_warning_not_eligible_count(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            include = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            quarantine = make_photo_entry(
                base,
                "2",
                [photo_row("2", "not-a-date")],
                role="quarantine_truncation",
                invalid_date_rows=1,
            )
            write_validation(
                base,
                [include, quarantine],
                verdict="WARN",
                warnings=["photo_invalid_date_rows:photo-excel-admin_2.xlsx:1"],
            )
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(payload["row_accounting"]["invalid_date_rows_eligible"], 0)
            self.assertIn("excluded_invalid_date_rows:photo-excel-admin_2.xlsx:1", payload["excluded_file_warnings"])

    def test_load_plan_hash_stable_ignores_generated_at(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry])
            first = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            time.sleep(1)
            second = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(first["load_plan_sha256"], second["load_plan_sha256"])

    def test_load_plan_hash_independent_of_discovery_order(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            first = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            second = make_photo_entry(base, "2", [photo_row("2", "2026-06-02")])
            write_validation(base, [first, second])
            a = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            write_validation(base, [second, first])
            b = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(a["load_plan_sha256"], b["load_plan_sha256"])

    def test_no_absolute_paths_in_semantic_plan(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertNotIn(str(base), json.dumps(payload["semantic_plan"]))

    def test_no_row_level_payload_written(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            secret_link = "https" + "://example.invalid/private/full/link"
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01", link=secret_link)])
            write_validation(base, [entry])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            outputs = loader.write_outputs(base=base, payload=payload, canonical=True)
            text = (base / outputs["json_path"]).read_text(encoding="utf-8")
            text += (base / outputs["md_path"]).read_text(encoding="utf-8")
            self.assertNotIn(secret_link, text)

    def test_no_url_full_links_in_outputs(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            secret_link = "https" + "://example.invalid/private/second/link"
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01", link=secret_link)])
            write_validation(base, [entry])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            dumped = json.dumps(payload, ensure_ascii=False)
            self.assertNotIn(secret_link, dumped)

    def test_no_db_client_imports_static(self):
        source = (SCRIPTS / "load_kpione_monthly_input_015C_dry_run_no_apply.py").read_text(
            encoding="utf-8"
        )
        forbidden = [
            "import " + "psy" + "copg",
            "import " + "sqlal" + "chemy",
            "from " + "sqlal" + "chemy",
            "DB" + "_URL",
        ]
        for token in forbidden:
            self.assertNotIn(token, source)

    def test_no_productive_loader_imports_static(self):
        source = (SCRIPTS / "load_kpione_monthly_input_015C_dry_run_no_apply.py").read_text(
            encoding="utf-8"
        )
        forbidden = [
            "load_control" + "_gestion_raw",
            "refresh_control" + "_gestion",
            "load_fact" + "_from_excel",
            "load_ruta" + "_rutero_from_excel",
        ]
        for token in forbidden:
            self.assertNotIn(token, source)

    def test_no_network_imports_static(self):
        source = (SCRIPTS / "load_kpione_monthly_input_015C_dry_run_no_apply.py").read_text(
            encoding="utf-8"
        )
        for token in [
            "import " + "requ" + "ests",
            "import " + "ht" + "tpx",
            "import " + "url" + "lib",
            "import " + "sock" + "et",
        ]:
            self.assertNotIn(token, source)

    def test_same_inputs_same_hashes_and_counts(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry])
            first = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            second = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertEqual(first["row_accounting"], second["row_accounting"])
            self.assertEqual(first["batch_plan"]["batches"][0]["deterministic_payload_sha256"], second["batch_plan"]["batches"][0]["deterministic_payload_sha256"])

    def test_metric_definitions_exist_with_units_universe_and_definition(self):
        definitions = loader.duplicate_metric_definitions()
        expected_units = {
            "exact_duplicate_rows_detected": "rows",
            "exact_duplicate_rows_removed": "rows",
            "same_id_same_hash_count": "distinct_event_ids",
            "same_id_diff_hash_count": "distinct_event_ids",
            "event_stable_hash_conflict_count": "distinct_event_ids",
            "cross_file_exact_photo_row_count": "distinct_duplicate_keys",
        }
        self.assertEqual(set(definitions), set(expected_units))
        for metric, expected_unit in expected_units.items():
            with self.subTest(metric=metric):
                self.assertEqual(definitions[metric]["unit"], expected_unit)
                self.assertTrue(definitions[metric]["universe"])
                self.assertTrue(definitions[metric]["definition"])

    def test_markdown_contains_duplicate_metric_glossary(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            row = photo_row("1", "2026-06-01")
            first = make_photo_entry(base, "1", [row])
            second = make_photo_entry(base, "2", [row])
            write_validation(base, [first, second])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            md = loader.report_markdown(payload)
            self.assertIn("## Duplicate metric definitions", md)
            self.assertIn("| Metric | Unit | Exact meaning |", md)
            for metric in loader.duplicate_metric_definitions():
                self.assertIn(metric, md)
            self.assertIn("Rows participating in exact duplicate groups:", md)
            self.assertIn("Distinct event IDs with same hash fingerprint across files:", md)
            self.assertIn("Universe:", md)

    def test_real_evidence_metric_definitions_counts_and_hashes_are_versioned(self):
        path = (
            ROOT
            / "research"
            / "015C_KPIONE_MONTHLY_LOAD_DRY_RUN_NO_APPLY"
            / "015C_kpione_monthly_load_dry_run_2026_06.json"
        )
        self.assertTrue(path.exists())
        payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertTrue(loader.has_required_metric_definitions(payload))
        definitions = payload["metric_definitions"]
        self.assertEqual(definitions["exact_duplicate_rows_detected"]["unit"], "rows")
        self.assertEqual(definitions["exact_duplicate_rows_removed"]["unit"], "rows")
        self.assertEqual(definitions["same_id_same_hash_count"]["unit"], "distinct_event_ids")
        self.assertEqual(definitions["same_id_diff_hash_count"]["unit"], "distinct_event_ids")
        self.assertEqual(definitions["cross_file_exact_photo_row_count"]["unit"], "distinct_duplicate_keys")
        self.assertEqual(
            payload["row_accounting"]["exact_duplicate_rows_detected"],
            EXPECTED_REAL_COUNTS["exact_duplicate_rows_detected"],
        )
        self.assertEqual(
            payload["row_accounting"]["exact_duplicate_rows_removed"],
            EXPECTED_REAL_COUNTS["exact_duplicate_rows_removed"],
        )
        for metric in [
            "same_id_same_hash_count",
            "same_id_diff_hash_count",
            "event_stable_hash_conflict_count",
            "cross_file_exact_photo_row_count",
        ]:
            self.assertEqual(payload["dedupe_summary"][metric], EXPECTED_REAL_COUNTS[metric])
        self.assertEqual(payload["load_plan_sha256"], EXPECTED_REAL_HASHES["load_plan_sha256"])
        self.assertEqual(payload["batch_plan"]["dry_run_batch_id"], EXPECTED_REAL_HASHES["dry_run_batch_id"])
        self.assertEqual(
            payload["batch_plan"]["batches"][0]["deterministic_payload_sha256"],
            EXPECTED_REAL_HASHES["deterministic_payload_sha256"],
        )

    def test_real_markdown_glossary_exposes_units_without_reading_code(self):
        path = (
            ROOT
            / "research"
            / "015C_KPIONE_MONTHLY_LOAD_DRY_RUN_NO_APPLY"
            / "015C_kpione_monthly_load_dry_run_2026_06.md"
        )
        self.assertTrue(path.exists())
        text = path.read_text(encoding="utf-8")
        self.assertIn("## Duplicate metric definitions", text)
        self.assertIn("| Metric | Unit | Exact meaning |", text)
        self.assertIn("| exact_duplicate_rows_detected | rows |", text)
        self.assertIn("| same_id_same_hash_count | distinct_event_ids |", text)
        self.assertIn("| cross_file_exact_photo_row_count | distinct_duplicate_keys |", text)

    def test_canonical_identical_run_does_not_rewrite_evidence(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11", canonical=True
            )
            first = loader.write_outputs(base=base, payload=payload, canonical=True)
            second_payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11", canonical=True
            )
            second = loader.write_outputs(base=base, payload=second_payload, canonical=True)
            self.assertTrue(first["json_changed"])
            self.assertFalse(second["json_changed"])
            self.assertFalse(second["md_changed"])

    def test_open_close_respect_015b_validation_mode(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry], validation_mode="open")
            open_payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="open", as_of_date="2026-07-11"
            )
            close_payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertNotIn("input_validation_mode_mismatch:open!=open", open_payload["blockers"])
            self.assertIn("input_validation_mode_mismatch:open!=close", close_payload["blockers"])

    def test_guardrails_no_apply_are_false(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            write_contracts(base)
            entry = make_photo_entry(base, "1", [photo_row("1", "2026-06-01")])
            write_validation(base, [entry])
            payload = loader.build_dry_run_payload(
                base=base, month_id="2026-06", validation_mode="close", as_of_date="2026-07-11"
            )
            self.assertFalse(payload["guardrails"]["db_access"])
            self.assertFalse(payload["guardrails"]["supabase_access"])
            self.assertFalse(payload["guardrails"]["sql_apply"])
            self.assertFalse(payload["guardrails"]["ddl"])
            self.assertFalse(payload["guardrails"]["data_movement"])
            self.assertFalse(payload["guardrails"]["payload_rows_persisted"])


if __name__ == "__main__":
    unittest.main()
