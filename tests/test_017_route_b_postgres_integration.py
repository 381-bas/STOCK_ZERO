from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import psycopg

from scripts.kpione_route_b_v1 import RouteBError, apply_local, build_plan, rollback_local
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


if __name__ == "__main__":
    unittest.main()
