import hashlib
import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "sql" / "proposed" / "014F_kpione_raw_staging_design_NO_APPLY.sql"
VALIDATION_SQL_PATH = (
    ROOT / "sql" / "proposed" / "014F_validation_queries_NO_APPLY.sql"
)
MANIFEST_PATH = (
    ROOT
    / "research"
    / "014F_KPIONE_RAW_DB_STAGING_DESIGN_NO_APPLY"
    / "014F_db_staging_design_manifest.json"
)
INPUT_014E_PATH = (
    ROOT
    / "research"
    / "014E_KPIONE_RAW_DRY_RUN_LOADER_NO_APPLY"
    / "014E_dry_run_loader_manifest.json"
)
GOVERNANCE_PATH = (
    ROOT / "docs" / "governance" / "014F_kpione_raw_db_staging_design_no_apply.md"
)
REPORT_PATH = (
    ROOT
    / "research"
    / "014F_KPIONE_RAW_DB_STAGING_DESIGN_NO_APPLY"
    / "014F_db_staging_design_report.md"
)


class KpioneRawStaging014FStaticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = SQL_PATH.read_text(encoding="utf-8")
        cls.validation_sql = VALIDATION_SQL_PATH.read_text(encoding="utf-8")
        cls.manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    def test_sql_has_no_apply_header_and_rollback_terminal(self):
        lines = self.sql.splitlines()
        self.assertEqual(lines[0], "-- 014F NO-APPLY PROPOSAL ONLY")
        self.assertEqual(lines[1], "-- DO NOT RUN IN PRODUCTION")
        self.assertEqual(lines[2], "-- Supabase apply not authorized")
        self.assertRegex(self.sql, r"(?im)^BEGIN;\s*$")
        self.assertRegex(self.sql, r"(?im)^ROLLBACK;\s*$")
        self.assertNotRegex(self.sql, r"(?im)^COMMIT;\s*$")

    def test_proposed_tables_and_patched_columns_exist(self):
        self.assertIn("cg_raw.kpione_raw_ingest_batch_v1", self.sql)
        self.assertIn("cg_raw.kpione_raw_ingest_batch_file_v1", self.sql)
        self.assertIn("cg_raw.kpione_raw_event_photo_staging_v1", self.sql)
        for column in (
            "batch_id",
            "event_id",
            "source_file_id",
            "source_row_number",
            "fecha",
            "week_start",
            "cod_rt",
            "local_nombre",
            "cliente_norm",
            "reponedor",
            "tipo_tarea",
            "n_fotos_raw",
            "photo_sequence",
            "photo_total",
            "link_foto",
            "event_stable_hash",
            "photo_row_hash",
            "loaded_at",
            "loader_version",
        ):
            self.assertRegex(self.sql, rf"(?im)^\s*{re.escape(column)}\s+")
        for column in ("status", "rolled_back_at", "rolled_back_by"):
            self.assertRegex(self.sql, rf"(?im)^\s*{column}\s+")

        self.assertNotRegex(self.sql, r"(?im)^\s*n_fotos\s+integer\b")
        self.assertRegex(self.sql, r"(?im)^\s*n_fotos_raw\s+text\b")
        self.assertRegex(self.sql, r"(?im)^\s*photo_sequence\s+integer\b")
        self.assertRegex(self.sql, r"(?im)^\s*photo_total\s+integer\b")

    def test_batch_file_and_composite_fk_cover_source_contract(self):
        compact = re.sub(r"\s+", " ", self.sql.lower())
        staging_ddl = self.sql.split(
            "CREATE TABLE cg_raw.kpione_raw_event_photo_staging_v1",
            maxsplit=1,
        )[1].split(");", maxsplit=1)[0]
        self.assertIn("primary key (batch_id, source_file_id)", compact)
        self.assertIn(
            "foreign key (batch_id, source_file_id) references "
            "cg_raw.kpione_raw_ingest_batch_file_v1 ( batch_id, source_file_id )",
            compact,
        )
        self.assertIn("on delete restrict", compact)
        self.assertIn("row_count >= 0", compact)
        self.assertIn("fecha_min <= fecha_max", compact)
        self.assertNotRegex(staging_ddl, r"(?im)^\s*source_file_name\s+")
        self.assertNotRegex(staging_ddl, r"(?im)^\s*source_file_sha256\s+")
        for role in (
            "include_candidate",
            "quarantine_truncation",
            "compare_only",
            "rejected_schema",
        ):
            self.assertIn(role, compact)

    def test_dedupe_and_photo_sequence_constraints_are_lossless(self):
        compact = re.sub(r"\s+", " ", self.sql.lower())
        self.assertIn("unique (batch_id, photo_row_hash)", compact)
        self.assertIn(
            "unique (batch_id, source_file_id, source_row_number)",
            compact,
        )
        self.assertIn("photo_sequence <= photo_total", compact)
        self.assertNotRegex(compact, r"check\s*\([^)]*photo_total\s*>\s*0")
        self.assertNotRegex(compact, r"check\s*\([^)]*n_fotos\s*>\s*0")
        self.assertIn("0/0", self.sql)
        self.assertIn("(event_id, photo_row_hash)", self.sql)
        self.assertIn("photo_row_hash includes event_id", self.sql)

    def test_status_tombstone_and_partial_uniques_exist(self):
        compact = re.sub(r"\s+", " ", self.sql.lower())
        self.assertIn("status text not null default 'staged'", compact)
        self.assertIn("status in ('staged', 'rolled_back')", compact)
        self.assertIn("status = 'rolled_back'", compact)
        self.assertIn("rolled_back_at is not null", compact)
        self.assertIn("nullif(btrim(rolled_back_by), '') is not null", compact)
        self.assertNotRegex(compact, r"verdict\s+in\s*\(")
        self.assertRegex(
            compact,
            r"create unique index \S+ on "
            r"cg_raw\.kpione_raw_ingest_batch_v1 "
            r"\(candidate_manifest_sha256\) where status = 'staged'",
        )
        self.assertRegex(
            compact,
            r"create unique index \S+ on "
            r"cg_raw\.kpione_raw_ingest_batch_v1 "
            r"\(month\) where status = 'staged'",
        )

    def test_index_set_is_reduced_and_batch_scoped(self):
        compact = re.sub(r"\s+", " ", self.sql.lower())
        self.assertIn(
            "on cg_raw.kpione_raw_event_photo_staging_v1 (batch_id, fecha)",
            compact,
        )
        self.assertIn(
            "on cg_raw.kpione_raw_event_photo_staging_v1 (batch_id, event_id)",
            compact,
        )
        for redundant_name in (
            "kpione_raw_event_photo_batch_id_idx",
            "kpione_raw_event_photo_source_file_id_idx",
            "kpione_raw_ingest_batch_verdict_idx",
        ):
            self.assertNotIn(redundant_name, compact)
        self.assertNotRegex(
            compact,
            r"create (?:unique )?index \S+ on "
            r"cg_raw\.kpione_raw_event_photo_staging_v1 \(batch_id\)",
        )
        self.assertNotRegex(
            compact,
            r"create (?:unique )?index \S+ on "
            r"cg_raw\.kpione_raw_event_photo_staging_v1 \(source_file_id\)",
        )
        self.assertNotRegex(
            compact,
            r"create (?:unique )?index \S+ on "
            r"cg_raw\.kpione_raw_ingest_batch_v1 \(verdict\)",
        )

    def test_validation_query_pack_is_select_only_and_complete(self):
        lines = self.validation_sql.splitlines()
        self.assertEqual(lines[0], "-- 014F VALIDATION QUERIES NO-APPLY ONLY")
        self.assertEqual(
            lines[1],
            "-- DO NOT RUN WITHOUT AUTHORIZED FUTURE PHASE",
        )
        self.assertEqual(lines[2], "-- Supabase apply not authorized")
        executable = "\n".join(
            line
            for line in lines
            if not line.lstrip().startswith("--")
        )
        self.assertRegex(executable, r"(?im)^\s*SELECT\b")
        self.assertNotRegex(
            executable,
            r"(?i)\b(CREATE|ALTER|DROP|TRUNCATE|INSERT|UPDATE|DELETE|COMMIT)\b",
        )
        for token in (
            "count(DISTINCT event_stable_hash)",
            "coverage_start",
            "count_matches_registry",
            "f.source_file_id IS NULL",
            "date_trunc('week', fecha)",
            "candidate_file_count_matches",
            "manifest row_count/distinct_event_ids and daily totals",
        ):
            self.assertIn(token, self.validation_sql)

    def test_sql_contains_no_destructive_execution_or_productive_apply(self):
        executable = "\n".join(
            line for line in self.sql.splitlines() if not line.lstrip().startswith("--")
        )
        self.assertNotRegex(executable, r"(?i)\bDROP\b")
        self.assertNotRegex(executable, r"(?i)\bTRUNCATE\b")
        self.assertNotRegex(executable, r"(?i)\bDELETE\s+FROM\b")
        self.assertNotRegex(executable, r"(?i)\bINSERT\s+INTO\b")

    def test_manifest_is_tied_to_exact_014e_input(self):
        actual_sha = hashlib.sha256(INPUT_014E_PATH.read_bytes()).hexdigest()
        declared = self.manifest["input_014E_manifest"]
        self.assertEqual(declared["sha256"], actual_sha)
        self.assertEqual(declared["verdict"], "DRY_RUN_READY_WITH_WARNINGS")
        self.assertEqual(declared["would_stage_rows"], 229070)
        self.assertEqual(
            self.manifest["verdict"],
            "STAGING_DESIGN_PATCHED_READY_FOR_014G",
        )
        self.assertEqual(self.manifest["blockers"], [])

    def test_manifest_grain_and_dedupe_are_unambiguous(self):
        grain = self.manifest["grain_definition"]["staging"]
        dedupe = self.manifest["dedupe_contract"]
        self.assertIn("event-photo", grain)
        self.assertIn("batch_id", grain)
        self.assertEqual(
            dedupe["dry_run_dedupe_key"],
            ["event_id", "photo_row_hash"],
        )
        self.assertEqual(
            dedupe["staging_operational_unique"],
            ["batch_id", "photo_row_hash"],
        )
        self.assertIn("includes event_id", dedupe["hash_equivalence"])
        self.assertIn("validation query", dedupe["same_event_id_different_hash"])
        self.assertIn("valid distinct events", dedupe["same_local_date_brand_different_event_id"])

    def test_docs_preserve_no_apply_and_mart_boundary(self):
        combined = (
            GOVERNANCE_PATH.read_text(encoding="utf-8")
            + "\n"
            + REPORT_PATH.read_text(encoding="utf-8")
        )
        self.assertIn("STAGING_DESIGN_PATCHED_READY_FOR_014G", combined)
        self.assertIn("No DB connection", combined)
        self.assertIn("no SQL/DDL apply", combined)
        self.assertIn("no Supabase", combined)
        self.assertIn("n_fotos_raw", combined)
        self.assertIn("batch_file", combined)
        self.assertIn("tombstone", combined.lower())
        self.assertIn("014F_validation_queries_NO_APPLY.sql", combined)
        self.assertIn("mart", combined.lower())
        self.assertIn("batch_id", combined)


if __name__ == "__main__":
    unittest.main()
