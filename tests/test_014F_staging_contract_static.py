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

PAYLOAD_TO_DDL = {
    "event_id": ("staging.event_id",),
    "source_file_id": (
        "staging.source_file_id",
        "batch_file.source_file_id",
    ),
    "source_file_name": ("batch_file.source_file_name",),
    "source_file_sha256": ("batch_file.source_file_sha256",),
    "source_row_number": ("staging.source_row_number",),
    "fecha": ("staging.fecha",),
    "week_start": ("staging.week_start",),
    "cod_rt": ("staging.cod_rt",),
    "local_nombre": ("staging.local_nombre",),
    "cliente_norm": ("staging.cliente_norm",),
    "reponedor": ("staging.reponedor",),
    "tipo_tarea": ("staging.tipo_tarea",),
    "n_fotos": (
        "staging.n_fotos_raw",
        "staging.photo_sequence",
        "staging.photo_total",
    ),
    "link_foto": ("staging.link_foto",),
    "event_stable_hash": ("staging.event_stable_hash",),
    "photo_row_hash": ("staging.photo_row_hash",),
    "dry_run_batch_id": (
        "staging.batch_id",
        "registry.batch_id",
    ),
}

STAGING_COLUMN_ORIGINS = {
    "batch_id": "payload.dry_run_batch_id",
    "event_id": "payload.event_id",
    "source_file_id": "payload.source_file_id",
    "source_row_number": "payload.source_row_number",
    "fecha": "payload.fecha",
    "week_start": "payload.week_start",
    "cod_rt": "payload.cod_rt",
    "local_nombre": "payload.local_nombre",
    "cliente_norm": "payload.cliente_norm",
    "reponedor": "payload.reponedor",
    "tipo_tarea": "payload.tipo_tarea",
    "n_fotos_raw": "payload.n_fotos preserved verbatim",
    "photo_sequence": "nullable parse of payload.n_fotos",
    "photo_total": "nullable parse of payload.n_fotos",
    "link_foto": "payload.link_foto",
    "event_stable_hash": "payload.event_stable_hash",
    "photo_row_hash": "payload.photo_row_hash",
    "loaded_at": "generated insertion timestamp",
    "loader_version": "generated future loader metadata",
}

INTENTIONAL_DIVERGENCES = {
    "source_file_name": "moved to batch_file authority",
    "source_file_sha256": "moved to batch_file authority",
    "n_fotos": "lossless raw plus nullable parsed transformation",
    "dry_run_batch_id": "renamed to batch_id in registry and staging",
    "loaded_at": "generated metadata, absent from 014E payload",
    "loader_version": "generated metadata, absent from 014E payload",
}


class KpioneRawStaging014FStaticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = SQL_PATH.read_text(encoding="utf-8")
        cls.validation_sql = VALIDATION_SQL_PATH.read_text(encoding="utf-8")
        cls.manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        cls.input_014e = json.loads(INPUT_014E_PATH.read_text(encoding="utf-8"))
        cls.docs = (
            GOVERNANCE_PATH.read_text(encoding="utf-8")
            + "\n"
            + REPORT_PATH.read_text(encoding="utf-8")
        )

    @staticmethod
    def _ddl_columns(sql, table_name):
        body = sql.split(f"CREATE TABLE {table_name}", maxsplit=1)[1].split(
            "\n);",
            maxsplit=1,
        )[0]
        column_pattern = re.compile(
            r"(?im)^\s*([a-z][a-z0-9_]*)\s+"
            r"(?:text|date|integer|bigint|timestamptz)\b"
        )
        return set(column_pattern.findall(body))

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
        self.assertRegex(self.sql, r"(?im)^\s*schema_signature\s+text\b")
        self.assertRegex(self.sql, r"(?im)^\s*staged_row_count\s+bigint\b")

    def test_batch_file_and_composite_fk_cover_source_contract(self):
        compact = re.sub(r"\s+", " ", self.sql.lower())
        batch_file_ddl = self.sql.split(
            "CREATE TABLE cg_raw.kpione_raw_ingest_batch_file_v1",
            maxsplit=1,
        )[1].split(");", maxsplit=1)[0]
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
        self.assertIn("row_count >= staged_row_count", compact)
        self.assertIn("staged_row_count >= 0", compact)
        self.assertIn("schema_signature ~ '^[0-9a-f]{64}$'", compact)
        self.assertNotRegex(
            batch_file_ddl,
            r"(?im)^\s*schema_signature\s+text\s+not null",
        )
        self.assertNotIn("schema_signature", json.dumps(self.input_014e))
        self.assertIn(
            "future apply gate must require both",
            self.docs,
        )
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
        self.assertIn("3/2", self.sql)
        self.assertIn("set both parsed fields to NULL", self.sql)
        self.assertIn("Parsed NULL with raw present is the anomaly signal", self.sql)
        self.assertIn("(event_id, photo_row_hash)", self.sql)
        self.assertIn("photo_row_hash includes event_id", self.sql)

    def test_month_coverage_and_candidate_file_array_contract(self):
        compact = re.sub(r"\s+", " ", self.sql.lower())
        self.assertIn("kpione_raw_ingest_batch_coverage_month_ck", self.sql)
        self.assertIn(
            "date_trunc('month', coverage_start)::date = month",
            compact,
        )
        self.assertIn(
            "date_trunc('month', coverage_end)::date = month",
            compact,
        )
        self.assertRegex(
            self.sql,
            r"(?im)^\s*candidate_source_file_ids\s+text\[\]\s+not null",
        )
        self.assertIn(
            "cardinality(candidate_source_file_ids) = source_files_count",
            compact,
        )
        for content in (
            self.sql,
            self.validation_sql,
            self.docs,
            json.dumps(self.manifest),
        ):
            self.assertNotRegex(content, r"\bsource_file_ids\b")
        self.assertIn("batch_file is the", self.validation_sql)
        self.assertIn("per-file authority", self.validation_sql)
        self.assertIn("Partial incremental and multi-month coverage are outside v1", self.sql)

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
            "count(DISTINCT s.event_stable_hash)",
            "coverage_start",
            "count_matches_registry",
            "f.source_file_id IS NULL",
            "date_trunc('week', s.fecha)",
            "candidate_file_count_matches",
            "per_file_count_matches",
            "raw_present_with_parsed_null",
            "event_row_count_differs_from_photo_total",
            "FUTURE APPLY BLOCKER CONDITION",
        ):
            self.assertIn(token, self.validation_sql)
        self.assertGreaterEqual(
            self.validation_sql.count("status = 'STAGED'"),
            9,
        )
        self.assertIn("ROLLED_BACK batches are intentionally excluded", self.validation_sql)
        self.assertIn("DEFENSE IN DEPTH ONLY", self.validation_sql)
        self.assertIn("not primary evidence", self.validation_sql)
        self.assertIn(
            "f.fecha_min NOT BETWEEN b.coverage_start AND b.coverage_end",
            self.validation_sql,
        )
        self.assertIn(
            "f.fecha_max NOT BETWEEN b.coverage_start AND b.coverage_end",
            self.validation_sql,
        )
        self.assertIn("staged_row_count", self.validation_sql)
        self.assertIn("No future apply may proceed", self.validation_sql)
        self.assertIn("-- 8a. PRIMARY EVIDENCE.", self.validation_sql)
        self.assertIn("f.role = 'include_candidate'", self.validation_sql)
        self.assertIn("-- 8b. PRIMARY EVIDENCE.", self.validation_sql)
        self.assertIn("f.role <> 'include_candidate'", self.validation_sql)
        self.assertIn(
            "non_candidate_has_zero_staged_rows",
            self.validation_sql,
        )
        self.assertIn(
            "Non-candidates may exist in",
            self.validation_sql,
        )
        self.assertNotIn(
            "Expected: four rows with affected_rows_or_events = 0",
            self.validation_sql,
        )
        self.assertIn(
            "PHOTO ANOMALY PROFILE / REVIEW REQUIRED",
            self.validation_sql,
        )
        self.assertIn(
            "PARSED INVARIANT VIOLATION / Expected zero",
            self.validation_sql,
        )
        self.assertIn(
            "they block\n-- mart/compliance activation",
            self.validation_sql,
        )

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
            "014G_SQL_REVIEW_MICRO_PATCH_APPLIED_READY_FOR_PR_CLOSEOUT",
        )
        self.assertEqual(self.manifest["blockers"], [])
        self.assertEqual(
            self.manifest["warnings"],
            [
                "SQL proposed and validation query pack were not executed",
                "immutable manifest projection reconciliation remains a future apply blocker",
                "cross-batch activation and precedence remain a future contract",
                "photo anomaly profile requires future review before mart activation",
            ],
        )

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

    def test_014e_payload_to_ddl_mapping_has_full_declared_parity(self):
        payload_columns = set(
            self.input_014e["dry_run_payload_summary"]["payload_columns"]
        )
        self.assertEqual(payload_columns, set(PAYLOAD_TO_DDL))

        table_columns = {
            "registry": self._ddl_columns(
                self.sql,
                "cg_raw.kpione_raw_ingest_batch_v1",
            ),
            "batch_file": self._ddl_columns(
                self.sql,
                "cg_raw.kpione_raw_ingest_batch_file_v1",
            ),
            "staging": self._ddl_columns(
                self.sql,
                "cg_raw.kpione_raw_event_photo_staging_v1",
            ),
        }
        for payload_column, targets in PAYLOAD_TO_DDL.items():
            with self.subTest(payload_column=payload_column):
                self.assertTrue(targets)
                for target in targets:
                    table, column = target.split(".", maxsplit=1)
                    self.assertIn(column, table_columns[table])

        self.assertEqual(
            table_columns["staging"],
            set(STAGING_COLUMN_ORIGINS),
        )
        self.assertEqual(
            set(INTENTIONAL_DIVERGENCES),
            {
                "source_file_name",
                "source_file_sha256",
                "n_fotos",
                "dry_run_batch_id",
                "loaded_at",
                "loader_version",
            },
        )
        for token in ("filename/SHA relocation", "photo parsing", "generated loader metadata"):
            self.assertIn(token, self.docs)

    def test_docs_preserve_no_apply_and_mart_boundary(self):
        self.assertIn(
            "014G_SQL_REVIEW_MICRO_PATCH_APPLIED_READY_FOR_PR_CLOSEOUT",
            self.docs,
        )
        self.assertIn("No DB connection", self.docs)
        self.assertIn("no SQL/DDL apply", self.docs)
        self.assertIn("no Supabase", self.docs)
        self.assertIn("n_fotos_raw", self.docs)
        self.assertIn("batch_file", self.docs)
        self.assertIn("tombstone", self.docs.lower())
        self.assertIn("014F_validation_queries_NO_APPLY.sql", self.docs)
        self.assertIn("mart", self.docs.lower())
        self.assertIn("batch_id", self.docs)
        self.assertIn("schema_signature", self.docs)
        self.assertIn("staged_row_count", self.docs)
        self.assertIn("future apply blocker", self.docs.lower())
        self.assertIn("Photo anomaly profile requires future review", self.docs)

    def test_sql_and_docs_have_no_mojibake_tokens(self):
        combined = self.sql + "\n" + self.validation_sql + "\n" + self.docs
        for token in ("\ufffd", "Ã", "Â", "â€"):
            with self.subTest(token=token):
                self.assertNotIn(token, combined)


if __name__ == "__main__":
    unittest.main()
