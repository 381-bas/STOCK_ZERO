from __future__ import annotations

import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg

from scripts.kpione_route_b_v1 import (
    ADVISORY_LOCK_KEY,
    RouteBError,
    apply_local,
    build_plan,
    rollback_local,
)
from tests.test_017_route_b_runner import HEADERS, row, write_book


DSN = os.environ.get("DB_URL_CODEX_LOCAL")
DDL = Path(__file__).resolve().parents[1] / "sql" / "17_kpione_route_b_ingestion_v1.sql"


@unittest.skipUnless(DSN, "DB_URL_CODEX_LOCAL is required for the local PostgreSQL rehearsal")
class RouteBPostgresRehearsal(unittest.TestCase):
    def setUp(self) -> None:
        with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('cg_raw.kpione_raw_ingest_batch_v1')")
            if cursor.fetchone()[0]:
                cursor.execute(
                    "TRUNCATE cg_raw.kpione_raw_event_photo_staging_v1, "
                    "cg_raw.kpione_raw_ingest_batch_file_v1, "
                    "cg_raw.kpione_raw_ingest_batch_v1 RESTART IDENTITY CASCADE"
                )

    def counts(self) -> tuple[int, int, int]:
        with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_event_photo_staging_v1")
            staging = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM cg_core.kpione_event_normalized_v1")
            events = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM cg_core.kpione_day_presence_v1")
            presence = cursor.fetchone()[0]
        return staging, events, presence

    def active_batch_count(self) -> int:
        with psycopg.connect(DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_ingest_batch_v1 WHERE status='ACTIVE'")
            return cursor.fetchone()[0]

    def test_full_rehearsal(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "photo-excel-admin_1.xlsx"
            write_book(source, [row(100, 900), row(101, 901, link="https://example.invalid/b.jpg")])
            first_plan = build_plan(root)

            first = apply_local(first_plan, DSN, DDL)
            self.assertEqual(first["outcome"], "ACTIVE")
            self.assertEqual(self.counts(), (2, 2, 1))

            same = apply_local(first_plan, DSN, DDL)
            self.assertEqual(same["outcome"], "NO_OP_ALREADY_REGISTERED")
            source.rename(root / "photo-excel-admin_renamed.xlsx")
            renamed_plan = build_plan(root)
            renamed = apply_local(renamed_plan, DSN, DDL)
            self.assertEqual(renamed["outcome"], "NO_OP_SAME_SOURCE_VERSION")

            malformed = root / "photo-excel-admin_bad.xlsx"
            write_book(malformed, [row()], HEADERS[:-1])
            with self.assertRaisesRegex(RouteBError, "missing_required_columns"):
                build_plan(root)
            malformed.unlink()

            conflict = root / "photo-excel-admin_conflict.xlsx"
            write_book(conflict, [row(200, 920), row(200, 921, link="https://example.invalid/c.jpg")])
            with self.assertRaisesRegex(RouteBError, "event_stability_conflict"):
                build_plan(root)
            conflict.unlink()

            corrected = root / "photo-excel-admin_corrected.xlsx"
            for path in root.glob("photo-excel-admin_*.xlsx"):
                path.unlink()
            write_book(corrected, [row(100, 900)])
            corrected_plan = build_plan(root)
            pending = apply_local(corrected_plan, DSN, DDL)
            self.assertEqual(pending["outcome"], "NEW_SOURCE_VERSION_PENDING_SUPERSESSION")
            successor = apply_local(corrected_plan, DSN, DDL, first["batch_id"])
            self.assertEqual(successor["outcome"], "ACTIVE")
            self.assertEqual(self.counts(), (3, 1, 1))

            restored = rollback_local(DSN, successor["batch_id"])
            self.assertEqual(restored["outcome"], "ROLLED_BACK")
            self.assertEqual(restored["restored_batch_id"], first["batch_id"])
            self.assertEqual(self.counts(), (3, 2, 1))

            replay = build_plan(root)
            self.assertEqual(replay["semantic_plan_hash"], corrected_plan["semantic_plan_hash"])
            self.assertEqual(replay["day_presence_count"], corrected_plan["day_presence_count"])

    def test_mixed_folders_classify_every_discovered_version(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source_a = root / "photo-excel-admin_a.xlsx"
            write_book(source_a, [row(100, 900, "2026-07-01")])
            first = apply_local(build_plan(root), DSN, DDL)

            source_b = root / "photo-excel-admin_b.xlsx"
            write_book(source_b, [row(200, 920, "2026-07-02")])
            mixed = apply_local(build_plan(root), DSN, DDL)
            self.assertEqual(mixed["outcome"], "ACTIVE")
            self.assertEqual(mixed["discovered_file_count"], 2)
            self.assertEqual(mixed["already_active_file_count"], 1)
            self.assertEqual(mixed["new_file_count"], 1)
            self.assertEqual(mixed["files_selected_for_staging"], 1)
            self.assertEqual(mixed["files_skipped_as_no_op"], 1)
            self.assertEqual(mixed["expected_inserts"], 1)
            self.assertEqual(self.counts(), (2, 2, 2))

            source_renamed = root / "photo-excel-admin_a_renamed.xlsx"
            source_renamed.write_bytes(source_a.read_bytes())
            all_known = apply_local(build_plan(root), DSN, DDL)
            self.assertEqual(all_known["outcome"], "NO_OP_SAME_SOURCE_VERSION")
            self.assertEqual(all_known["already_active_file_count"], 3)
            self.assertEqual(all_known["renamed_no_op_count"], 1)
            self.assertEqual(all_known["new_file_count"], 0)

            malformed = root / "photo-excel-admin_bad.xlsx"
            write_book(malformed, [row()], HEADERS[:-1])
            with self.assertRaisesRegex(RouteBError, "missing_required_columns"):
                build_plan(root)
            self.assertEqual(self.active_batch_count(), 2)
            self.assertEqual(self.counts(), (2, 2, 2))

    def test_mixed_corrected_content_requires_explicit_supersession(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source_a = root / "photo-excel-admin_a.xlsx"
            write_book(source_a, [row(100, 900, "2026-07-01")])
            first = apply_local(build_plan(root), DSN, DDL)

            corrected = root / "photo-excel-admin_corrected.xlsx"
            write_book(corrected, [row(200, 920, "2026-07-01")])
            corrected_plan = build_plan(root)
            pending = apply_local(corrected_plan, DSN, DDL)
            self.assertEqual(pending["outcome"], "NEW_SOURCE_VERSION_PENDING_SUPERSESSION")
            self.assertEqual(pending["already_active_file_count"], 1)
            self.assertEqual(pending["new_file_count"], 1)
            self.assertTrue(pending["expected_supersession_requirement"])
            self.assertIn("NEW_SOURCE_VERSION_REQUIRES_SUPERSESSION",
                          {item["classification"] for item in pending["files"]})

            successor = apply_local(corrected_plan, DSN, DDL, first["batch_id"])
            self.assertEqual(successor["outcome"], "ACTIVE")
            self.assertEqual(self.active_batch_count(), 1)
            self.assertEqual(self.counts(), (2, 1, 1))

    def test_concurrent_identical_plan_activates_once(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_book(root / "photo-excel-admin_a.xlsx", [row()])
            plan = build_plan(root)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _: apply_local(plan, DSN, DDL), range(2)))
            self.assertEqual(sorted(result["outcome"] for result in results),
                             ["ACTIVE", "NO_OP_ALREADY_REGISTERED"])
            self.assertEqual(self.active_batch_count(), 1)
            self.assertEqual(self.counts(), (1, 1, 1))

    def test_concurrent_overlapping_content_activates_once(self) -> None:
        with tempfile.TemporaryDirectory() as first_folder, tempfile.TemporaryDirectory() as second_folder:
            first_root, second_root = Path(first_folder), Path(second_folder)
            write_book(first_root / "photo-excel-admin_a.xlsx", [row(100, 900)])
            write_book(second_root / "photo-excel-admin_b.xlsx", [row(200, 920)])
            plans = [build_plan(first_root), build_plan(second_root)]
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda plan: apply_local(plan, DSN, DDL), plans))
            self.assertEqual(sorted(result["outcome"] for result in results),
                             ["ACTIVE", "NEW_SOURCE_VERSION_PENDING_SUPERSESSION"])
            self.assertEqual(self.active_batch_count(), 1)
            self.assertEqual(self.counts(), (1, 1, 1))

    def test_advisory_lock_and_active_coverage_constraint_exist(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_book(root / "photo-excel-admin_a.xlsx", [row()])
            apply_local(build_plan(root), DSN, DDL)
        with psycopg.connect(DSN) as first, psycopg.connect(DSN) as second:
            with first.transaction(), first.cursor() as first_cursor, second.transaction(), second.cursor() as second_cursor:
                first_cursor.execute("SELECT pg_advisory_xact_lock(%s)", (ADVISORY_LOCK_KEY,))
                second_cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", (ADVISORY_LOCK_KEY,))
                self.assertFalse(second_cursor.fetchone()[0])
                second_cursor.execute(
                    "SELECT count(*) FROM pg_constraint WHERE conname='kpione_one_active_coverage_v1' "
                    "AND contype='x'"
                )
                self.assertEqual(second_cursor.fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
