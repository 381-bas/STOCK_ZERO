from __future__ import annotations

import argparse
from contextlib import contextmanager
import os
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from scripts import cg_readonly_extract as ext


class CgReadonlyExtractTests(unittest.TestCase):
    @contextmanager
    def patched_repo_roots(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            evidence = root / "evidence"
            phase_root = evidence / "supabase_cleanup_9C7A4"
            evidence.mkdir(parents=True)
            with (
                mock.patch.object(ext, "ROOT", root),
                mock.patch.object(ext, "EVIDENCE_ROOT", evidence),
                mock.patch.object(ext, "PHASE_ROOT", phase_root),
            ):
                yield root, evidence, phase_root

    def test_mutation_tokens_are_blocked(self) -> None:
        for token in (
            "INSERT INTO x VALUES (1)",
            "UPDATE x SET y = 1",
            "DELETE FROM x",
            "TRUNCATE x",
            "CREATE TABLE x(id int)",
            "ALTER TABLE x ADD COLUMN y int",
            "DROP TABLE x",
            "GRANT SELECT ON x TO y",
            "REVOKE SELECT ON x FROM y",
            "REFRESH MATERIALIZED VIEW x",
            "VACUUM x",
            "ANALYZE x",
            "CALL do_work()",
            "DO $$ BEGIN NULL; END $$",
            "COPY x TO PROGRAM 'cmd'",
        ):
            with self.subTest(token=token):
                with self.assertRaises(ext.ExtractorBlock):
                    ext.validate_static_sql("blocked", token)

    def test_no_free_form_sql_arguments_exist(self) -> None:
        parser = ext.build_parser()
        help_text = parser.format_help()
        for forbidden in ("--sql", "--query", "--execute", "--statement"):
            self.assertNotIn(forbidden, help_text)

    def test_unknown_subcommand_rejected(self) -> None:
        parser = ext.build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["not-allowed"])

    def test_role_mismatch_blocks(self) -> None:
        status = ext.RoleStatus(
            current_user="postgres",
            session_user="postgres",
            transaction_read_only="on",
            default_transaction_read_only="on",
        )
        with self.assertRaises(ext.ExtractorBlock):
            ext.require_role_status(status)

    def test_non_readonly_transaction_blocks(self) -> None:
        status = ext.RoleStatus(
            current_user=ext.EXPECTED_ROLE,
            session_user=ext.EXPECTED_ROLE,
            transaction_read_only="off",
            default_transaction_read_only="on",
        )
        with self.assertRaises(ext.ExtractorBlock):
            ext.require_role_status(status)

    def test_object_allowlist_blocks_other_schema(self) -> None:
        with self.assertRaises(ext.ExtractorBlock):
            ext.split_qualified_name("auth.users")

    def test_dsn_redaction(self) -> None:
        message = "postgresql://user:secret@example.supabase.co:5432/db password=secret host=example.supabase.co port=5432"
        redacted = ext.redact_secret(message)
        self.assertNotIn("secret", redacted)
        self.assertNotIn("example.supabase.co", redacted)
        self.assertNotIn("5432", redacted)

    def test_output_path_must_stay_inside_evidence_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ext.ExtractorBlock):
                ext.ensure_output_root(Path(tmp))

    def test_allowed_output_path_creates_required_subdirs(self) -> None:
        with self.patched_repo_roots() as (_root, _evidence, phase_root):
            out_root = ext.ensure_output_root(phase_root)
            for subdir in ext.REQUIRED_SUBDIRS:
                self.assertTrue((out_root / subdir).exists())

    def test_default_output_root_is_preserved(self) -> None:
        with self.patched_repo_roots() as (_root, _evidence, phase_root):
            parser = ext.build_parser()
            args = parser.parse_args(["role-audit"])
            self.assertEqual(Path(args.output_root), Path("evidence") / "supabase_cleanup_9C7A4")
            self.assertEqual(ext.ensure_output_root(args.output_root), phase_root.resolve())

    def test_output_root_allows_valid_path_inside_evidence(self) -> None:
        with self.patched_repo_roots() as (_root, evidence, _phase_root):
            out_root = ext.ensure_output_root(Path("evidence") / "extractor_run")
            self.assertEqual(out_root, (evidence / "extractor_run").resolve())

    def test_output_root_allows_valid_subfolder_inside_evidence(self) -> None:
        with self.patched_repo_roots() as (_root, evidence, _phase_root):
            out_root = ext.ensure_output_root(Path("evidence") / "extractor_run" / "nested")
            self.assertEqual(out_root, (evidence / "extractor_run" / "nested").resolve())

    def test_output_root_blocks_relative_external_path(self) -> None:
        with self.patched_repo_roots():
            with self.assertRaises(ext.ExtractorBlock):
                ext.ensure_output_root("outside_evidence")

    def test_output_root_blocks_absolute_external_path(self) -> None:
        with self.patched_repo_roots(), tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ext.ExtractorBlock):
                ext.ensure_output_root(Path(tmp) / "external")

    def test_output_root_blocks_path_traversal(self) -> None:
        with self.patched_repo_roots():
            with self.assertRaises(ext.ExtractorBlock):
                ext.ensure_output_root(Path("evidence") / ".." / "outside")

    def test_output_root_blocks_evidence_fake_prefix(self) -> None:
        with self.patched_repo_roots():
            with self.assertRaises(ext.ExtractorBlock):
                ext.ensure_output_root(Path("evidence_fake") / "run")

    def test_output_root_blocks_symlink_escape_when_available(self) -> None:
        with self.patched_repo_roots() as (_root, evidence, _phase_root):
            outside = evidence.parent / "outside_target"
            outside.mkdir()
            link = evidence / "link_out"
            try:
                os.symlink(outside, link, target_is_directory=True)
            except (OSError, NotImplementedError) as exc:
                self.skipTest(f"symlink creation unavailable: {exc}")
            with self.assertRaises(ext.ExtractorBlock):
                ext.ensure_output_root(link / "nested")

    def test_baseline_modes_have_split_hints(self) -> None:
        daily = [{"column_name": "fecha", "data_type": "date"}]
        weekly = [{"column_name": "semana", "data_type": "text"}]
        self.assertEqual(ext.choose_split_column(daily, "daily"), "fecha")
        self.assertEqual(ext.choose_split_column(weekly, "weekly"), "semana")

    def test_allowlisted_subcommands_are_explicit(self) -> None:
        self.assertEqual(
            set(ext.ALLOWLISTED_SUBCOMMANDS),
            {
                "role-audit",
                "catalog",
                "dependencies",
                "ddl",
                "baseline-daily",
                "baseline-weekly",
                "baseline-audit",
                "c001-profile",
                "route-preflight",
                "all",
            },
        )

    def test_c001_profile_does_not_require_output_root(self) -> None:
        with (
            mock.patch.object(ext, "ensure_output_root", side_effect=AssertionError("output root should not be used")),
            mock.patch.object(ext, "run_command", return_value={"verdict": "OK"}) as run_command,
            mock.patch("sys.stdout"),
        ):
            self.assertEqual(ext.main(["--output-root", "outside_evidence", "c001-profile"]), 0)
        run_command.assert_called_once_with("c001-profile", None, None, None)

    def test_route_preflight_does_not_require_output_root(self) -> None:
        with (
            mock.patch.object(ext, "ensure_output_root", side_effect=AssertionError("output root should not be used")),
            mock.patch.object(ext, "run_command", return_value={"verdict": "OK"}) as run_command,
            mock.patch("sys.stdout"),
        ):
            self.assertEqual(ext.main(["--output-root", "outside_evidence", "route-preflight"]), 0)
        run_command.assert_called_once_with("route-preflight", None, None, None)


if __name__ == "__main__":
    unittest.main()
