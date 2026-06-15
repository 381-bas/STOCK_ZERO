import argparse
import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import apply_cg005n_ddl as runner
import cg005n_catalog_contract as contract
import cg005n_prestage_catalog as prestage


class FakeCursor:
    def __init__(self, fetchone=None):
        self.fetchone_values = list(fetchone or [])
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        if self.fetchone_values:
            return self.fetchone_values.pop(0)
        return (0,)

    def fetchall(self):
        return []

    @property
    def description(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.autocommit = None
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class SyntheticCatalogCursor:
    def __init__(self):
        self.executed = []
        self._rows = []
        self._one = None
        self._description = []
        self.objects = self._build_objects()
        self.direct_rows = [
            {
                "target_object": "cg_core.v_ruta_rutero_load_batch_week_v2",
                "referenced_schema": "cg_core",
                "referenced_name": "ruta_rutero_load_batch",
                "referenced_relkind": "r",
            }
        ]
        self.reverse_rows = [
            {
                "target_object": "cg_core.v_ruta_rutero_load_batch_week_v2",
                "dependent_schema": "cg_mart",
                "dependent_name": "fact_cg_out_weekly_v2",
                "dependent_relkind": "m",
            }
        ]

    def _build_objects(self):
        objects = {}
        for idx, object_name in enumerate(contract.TARGET_OBJECTS, start=1):
            schema, name = object_name.split(".", 1)
            exists = object_name != "cg_core.ruta_rutero_week_assignment"
            relkind = "v" if object_name in contract.VIEW_TARGETS else "r"
            objects[object_name] = {
                "oid": idx,
                "schema": schema,
                "relation": name,
                "exists": exists,
                "relkind": relkind if exists else None,
                "owner": "postgres" if exists else None,
                "comment": None,
                "reloptions": ["security_barrier=true"] if object_name == "cg_core.v_ruta_rutero_load_batch_week_v2" else [],
                "relacl": ["stock_zero_readonly=r/postgres"] if exists else None,
                "columns": [
                    {
                        "ordinal_position": 1,
                        "column_name": "id",
                        "data_type": "bigint",
                        "udt_name": "int8",
                        "is_nullable": "NO",
                        "column_default_present": False,
                    }
                ] if exists else [],
                "constraints": [{"constraint_name": f"{name}_pkey", "constraint_type": "p", "definition": "PRIMARY KEY (id)"}] if exists and relkind == "r" else [],
                "indexes": [{"index_name": f"{name}_pkey", "definition": f"CREATE UNIQUE INDEX {name}_pkey ON {schema}.{name} (id)"}] if exists and relkind == "r" else [],
                "acl": [{"grantor": "postgres", "grantee": "stock_zero_readonly", "privilege_type": "SELECT", "is_grantable": "NO"}] if exists else [],
                "view_definition": "select 1::bigint as id" if object_name in contract.VIEW_TARGETS else None,
                "view_options": {"security_barrier": object_name == "cg_core.v_ruta_rutero_load_batch_week_v2", "check_option": None} if object_name in contract.VIEW_TARGETS else None,
            }
        return objects

    @property
    def description(self):
        return [(name,) for name in self._description]

    def fetchone(self):
        return self._one

    def fetchall(self):
        return [tuple(row.get(col) for col in self._description) for row in self._rows]

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        compact = " ".join(sql.split())
        self._rows = []
        self._one = None
        self._description = []
        if compact.startswith("SAVEPOINT") or compact.startswith("RELEASE") or compact.startswith("ROLLBACK TO"):
            return
        if "FROM unnest(%s::text[]) AS wanted" in sql:
            self._description = ["qualified_name", "schema", "relation", "relkind", "owner", "comment", "reloptions", "relacl"]
            for object_name in params[0]:
                obj = self.objects[object_name]
                self._rows.append({
                    "qualified_name": object_name,
                    "schema": obj["schema"] if obj["exists"] else None,
                    "relation": obj["relation"] if obj["exists"] else None,
                    "relkind": obj["relkind"],
                    "owner": obj["owner"],
                    "comment": obj["comment"],
                    "reloptions": obj["reloptions"] if obj["exists"] else None,
                    "relacl": obj["relacl"],
                })
            return
        if "SELECT c.oid," in sql and "obj_description" in sql:
            schema, relation = params
            obj = self.objects[f"{schema}.{relation}"]
            if obj["exists"]:
                self._one = (obj["oid"], obj["relkind"], obj["owner"], obj["comment"], obj["reloptions"], obj["relacl"])
            return
        if "FROM information_schema.columns" in sql:
            if "table_schema || '.' || table_name = ANY" in sql:
                self._description = ["schema", "relation", "ordinal_position", "column_name", "data_type", "udt_name", "is_nullable", "column_default"]
                for object_name in params[0]:
                    obj = self.objects[object_name]
                    for column in obj["columns"]:
                        self._rows.append({**column, "schema": obj["schema"], "relation": obj["relation"], "column_default": None})
            else:
                schema, relation = params
                obj = self.objects[f"{schema}.{relation}"]
                self._description = ["ordinal_position", "column_name", "data_type", "udt_name", "is_nullable", "column_default_present"]
                self._rows = obj["columns"]
            return
        if "FROM pg_catalog.pg_constraint" in sql or "FROM pg_constraint" in sql:
            if "n.nspname::text AS schema" in sql:
                self._description = ["schema", "relation", "constraint_name", "constraint_type", "definition"]
                object_names = params[0]
                for object_name in object_names:
                    obj = self.objects[object_name]
                    for row in obj["constraints"]:
                        self._rows.append({**row, "schema": obj["schema"], "relation": obj["relation"]})
            else:
                object_name = params[0]
                self._description = ["constraint_name", "constraint_type", "definition"]
                self._rows = self.objects[object_name]["constraints"]
            return
        if "FROM pg_catalog.pg_index" in sql or "FROM pg_indexes" in sql:
            if "n.nspname::text AS schema" in sql:
                self._description = ["schema", "relation", "index_name", "definition"]
                for object_name in params[0]:
                    obj = self.objects[object_name]
                    for row in obj["indexes"]:
                        self._rows.append({**row, "schema": obj["schema"], "relation": obj["relation"]})
            else:
                if len(params) == 2:
                    schema, relation = params
                    object_name = f"{schema}.{relation}"
                else:
                    object_name = params[0]
                self._description = ["index_name", "definition"]
                self._rows = self.objects[object_name]["indexes"]
            return
        if "aclexplode" in sql:
            if "n.nspname::text AS schema" in sql:
                self._description = ["schema", "relation", "grantor", "grantee", "privilege_type", "is_grantable"]
                for object_name in params[0]:
                    obj = self.objects[object_name]
                    for row in obj["acl"]:
                        self._rows.append({**row, "schema": obj["schema"], "relation": obj["relation"]})
            else:
                schema, relation = params
                self._description = ["grantor", "grantee", "privilege_type", "is_grantable"]
                self._rows = self.objects[f"{schema}.{relation}"]["acl"]
            return
        if "pg_get_viewdef" in sql:
            if "n.nspname::text AS schema" in sql:
                self._description = ["schema", "relation", "view_definition", "security_barrier", "check_option"]
                for object_name in params[0]:
                    obj = self.objects[object_name]
                    if object_name in contract.VIEW_TARGETS:
                        self._rows.append({
                            "schema": obj["schema"],
                            "relation": obj["relation"],
                            "view_definition": obj["view_definition"],
                            "security_barrier": obj["view_options"]["security_barrier"],
                            "check_option": obj["view_options"]["check_option"],
                        })
            else:
                self._one = (self.objects[params[0]]["view_definition"],)
            return
        if "referenced_ns.nspname AS referenced_schema" in sql:
            self._description = ["referenced_schema", "referenced_name", "referenced_kind"]
            target = params[0]
            self._rows = [
                {
                    "referenced_schema": row["referenced_schema"],
                    "referenced_name": row["referenced_name"],
                    "referenced_kind": row["referenced_relkind"],
                }
                for row in self.direct_rows
                if row["target_object"] == target
            ]
            return
        if "dependent_ns.nspname AS dependent_schema" in sql:
            self._description = ["dependent_schema", "dependent_name", "dependent_kind"]
            target = params[0]
            self._rows = [
                {
                    "dependent_schema": row["dependent_schema"],
                    "dependent_name": row["dependent_name"],
                    "dependent_kind": row["dependent_relkind"],
                }
                for row in self.reverse_rows
                if row["target_object"] == target
            ]
            return
        if "referenced_relkind" in sql:
            self._description = ["target_object", "referenced_schema", "referenced_name", "referenced_relkind"]
            self._rows = self.direct_rows
            return
        if "dependent_relkind" in sql:
            self._description = ["target_object", "dependent_schema", "dependent_name", "dependent_relkind"]
            self._rows = self.reverse_rows
            return
        if "count(*)::bigint, max(ruta_batch_id)::bigint" in sql:
            self._one = (3, 9)
            return
        if "to_regclass('cg_core.ruta_rutero_week_assignment') IS NOT NULL" in sql:
            self._one = (False,)
            self._description = ["value"]
            self._rows = [{"value": False}]
            return
        raise AssertionError(f"Unhandled SQL: {compact[:160]}")


def strict_column(column, ordinal):
    payload = {
        "ordinal_position": ordinal,
        "column_name": column["column_name"],
        "data_type": column["data_type"],
        "udt_name": column["udt_name"],
        "is_nullable": column["is_nullable"],
        "column_default_present": column.get("column_default_present", False),
    }
    return payload


class ApplyCg005nDdlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prestate = runner.load_prestate_catalog()
        cls.file_hash = cls.prestate["_prestate_file_sha256"]
        cls.tech_hash = cls.prestate["technical_catalog_fingerprint_sha256"]
        cls.rollback_hash = cls.prestate["rollback_baseline_sha256"]

    def _synthetic_prestage_catalog(self, cursor=None):
        cur = cursor or SyntheticCatalogCursor()
        objects = prestage.fetch_relation_objects(cur)
        prestage.attach_columns(cur, objects)
        prestage.attach_constraints(cur, objects)
        prestage.attach_indexes(cur, objects)
        prestage.attach_acl(cur, objects)
        prestage.attach_view_definitions(cur, objects)
        reverse_dependencies = prestage.fetch_reverse_dependencies(cur)
        direct_dependencies = prestage.fetch_direct_dependencies(cur)
        return contract.normalize_catalog(
            {
                "phase": prestage.PHASE,
                "target_objects": objects,
                "direct_dependencies": direct_dependencies,
                "reverse_dependencies": reverse_dependencies,
                "material_dependency_unknowns": [],
                "rollback_baseline": {
                    "route_batch_count": 3,
                    "max_ruta_batch_id": 9,
                    "assignment_table_exists": False,
                    "assignment_row_count": 0,
                    "measurement_available": True,
                },
                "completion": {"unknown_dependencies": 999},
            },
            prestate=True,
        )

    def _synthetic_runner_catalog(self, cursor=None):
        return runner.fetch_catalog(cursor or SyntheticCatalogCursor())

    def _post_catalog(self):
        catalog = copy.deepcopy(self.prestate)
        assignment = catalog["target_objects"]["cg_core.ruta_rutero_week_assignment"]
        assignment.update(
            {
                "exists": True,
                "schema": "cg_core",
                "name": "ruta_rutero_week_assignment",
                "relkind": "r",
                "owner": "postgres",
                "comment": None,
                "comment_captured": True,
                "reloptions": [],
                "acl": [
                    {
                        "grantor": "postgres",
                        "grantee": "stock_zero_readonly",
                        "privilege_type": "SELECT",
                        "is_grantable": "NO",
                    }
                ],
                "columns": [
                    strict_column(column, idx + 1)
                    for idx, column in enumerate(runner.ASSIGNMENT_COLUMNS)
                ],
                "constraints": [
                    {"constraint_name": "ruta_rutero_week_assignment_pkey", "definition": "PRIMARY KEY (assignment_id)"},
                    {"constraint_name": "ruta_rutero_week_assignment_batch_fk", "definition": "FOREIGN KEY (ruta_batch_id) REFERENCES ruta_rutero_load_batch(ruta_batch_id)"},
                    {"constraint_name": "ruta_rutero_week_assignment_rollback_fk", "definition": "FOREIGN KEY (rollback_of_assignment_id) REFERENCES ruta_rutero_week_assignment(assignment_id)"},
                    {"constraint_name": "ruta_rutero_week_assignment_monday_check", "definition": "CHECK (effective_week_start IS NOT NULL)"},
                    {"constraint_name": "ruta_rutero_week_assignment_status_check", "definition": "CHECK (assignment_status IN ('ACTIVE','ROLLED_BACK'))"},
                    {"constraint_name": "ruta_rutero_week_assignment_policy_check", "definition": "CHECK (route_policy_version <> '')"},
                    {"constraint_name": "ruta_rutero_week_assignment_current_hash_check", "definition": "CHECK (length(current_surface_hash) = 64)"},
                    {"constraint_name": "ruta_rutero_week_assignment_resolved_hash_check", "definition": "CHECK (length(resolved_surface_hash) = 64)"},
                ],
                "indexes": [
                    {"index_name": "ruta_rutero_week_assignment_pkey", "definition": "CREATE UNIQUE INDEX ruta_rutero_week_assignment_pkey ON cg_core.ruta_rutero_week_assignment (assignment_id)"},
                    {"index_name": "ix_ruta_rutero_week_assignment_week", "definition": "CREATE INDEX ix_ruta_rutero_week_assignment_week ON cg_core.ruta_rutero_week_assignment (effective_week_start)"},
                    {"index_name": "ux_ruta_rutero_week_assignment_active", "definition": "CREATE UNIQUE INDEX ux_ruta_rutero_week_assignment_active ON cg_core.ruta_rutero_week_assignment (effective_week_start) WHERE assignment_status = 'ACTIVE'"},
                ],
            }
        )
        for view_name, appended in runner.APPENDED_SIGNATURES.items():
            columns = catalog["target_objects"][view_name]["columns"]
            start = len(columns)
            columns.extend(strict_column(column, start + idx + 1) for idx, column in enumerate(appended))
        return catalog

    def test_prestate_catalog_is_strictly_complete(self):
        runner.validate_catalog_complete(self.prestate, prestate=True)
        self.assertEqual(self.prestate["material_dependency_unknowns"], [])

    def test_apply_and_rollback_do_not_accept_sql_argument(self):
        source = runner.Path(runner.__file__).read_text(encoding="utf-8")
        self.assertNotIn('add_argument("--sql"', source)
        self.assertNotIn('add_argument("--db-url"', source)

    def test_sql13_sql14_paths_are_fixed_in_plan(self):
        args = argparse.Namespace(
            expected_sql13_sha256="A" * 64,
            expected_sql14_sha256="B" * 64,
            expected_prestate_file_sha256=self.file_hash,
            expected_prestate_technical_fingerprint=self.tech_hash,
        )
        with mock.patch.object(runner, "read_sql_artifact", return_value="-- NO APPLY\nBEGIN;\nROLLBACK;") as read_sql:
            payload = runner.run_plan(args)
        read_sql.assert_any_call(runner.SQL13_PATH, "A" * 64)
        read_sql.assert_any_call(runner.SQL14_PATH, "B" * 64)
        self.assertFalse(payload["free_sql_cli"])

    def test_file_sha_and_technical_fingerprint_are_not_interchangeable(self):
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_prestate_integrity(self.prestate, self.tech_hash, self.file_hash)
        self.assertEqual(ctx.exception.code, "prestate_file_sha256_mismatch")

    def test_expected_post_fingerprint_required_for_rollback(self):
        args = argparse.Namespace(
            apply=True,
            expected_sql14_sha256="A" * 64,
            expected_post_technical_fingerprint="",
            expected_rollback_technical_fingerprint="R" * 64,
            expected_prestate_file_sha256=self.file_hash,
            expected_prestate_technical_fingerprint=self.tech_hash,
        )
        with self.assertRaises(runner.RunnerBlock) as ctx:
            with mock.patch.object(runner, "read_sql_artifact", return_value="-- NO APPLY\nBEGIN;\nROLLBACK;"):
                runner.run_rollback(args)
        self.assertEqual(ctx.exception.code, "expected_post_technical_fingerprint_required")

    def test_expected_rollback_fingerprint_required_for_rollback(self):
        args = argparse.Namespace(
            apply=True,
            expected_sql14_sha256="A" * 64,
            expected_post_technical_fingerprint="P" * 64,
            expected_rollback_technical_fingerprint="",
            expected_prestate_file_sha256=self.file_hash,
            expected_prestate_technical_fingerprint=self.tech_hash,
        )
        with self.assertRaises(runner.RunnerBlock) as ctx:
            with mock.patch.object(runner, "read_sql_artifact", return_value="-- NO APPLY\nBEGIN;\nROLLBACK;"):
                runner.run_rollback(args)
        self.assertEqual(ctx.exception.code, "expected_rollback_technical_fingerprint_required")

    def test_apply_without_confirmation_does_not_connect_or_write(self):
        args = argparse.Namespace(apply=False)
        with mock.patch.object(runner, "connect_db_from_env") as connect, \
            mock.patch.object(runner, "begin_runner_transaction") as begin, \
            mock.patch.object(runner, "execute_static_sql") as execute:
            with self.assertRaises(runner.RunnerBlock) as ctx:
                runner.run_apply(args)
        self.assertEqual(ctx.exception.code, "apply_confirmation_required")
        self.assertEqual(
            ctx.exception.telemetry,
            {
                "writes_attempted": False,
                "ddl_statements_executed": 0,
                "committed": False,
                "rolled_back": False,
                "postcheck_passed": False,
            },
        )
        connect.assert_not_called()
        begin.assert_not_called()
        execute.assert_not_called()

    def test_rollback_without_confirmation_does_not_connect_or_write(self):
        args = argparse.Namespace(apply=False)
        with mock.patch.object(runner, "connect_db_from_env") as connect, \
            mock.patch.object(runner, "begin_runner_transaction") as begin, \
            mock.patch.object(runner, "execute_static_sql") as execute:
            with self.assertRaises(runner.RunnerBlock) as ctx:
                runner.run_rollback(args)
        self.assertEqual(ctx.exception.code, "rollback_confirmation_required")
        self.assertEqual(
            ctx.exception.telemetry,
            {
                "writes_attempted": False,
                "ddl_statements_executed": 0,
                "committed": False,
                "rolled_back": False,
                "postcheck_passed": False,
            },
        )
        connect.assert_not_called()
        begin.assert_not_called()
        execute.assert_not_called()

    def test_wrong_ddl_role_blocks(self):
        status = runner.RoleStatus("other", "other", "off", "off", "postgres", "F" * 64)
        args = argparse.Namespace(expected_current_user="ddl_user", expected_database="postgres", expected_environment_fingerprint="F" * 64)
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_write_identity(status, args)
        self.assertEqual(ctx.exception.code, "ddl_role_mismatch")

    def test_db_url_ddl_absent_blocks(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(runner.RunnerBlock) as ctx:
                runner.connect_db_from_env()
        self.assertEqual(ctx.exception.code, "db_url_ddl_required")

    def test_wrong_database_blocks(self):
        status = runner.RoleStatus("ddl_user", "ddl_user", "off", "off", "wrong_db", "F" * 64)
        args = argparse.Namespace(expected_current_user="ddl_user", expected_database="postgres", expected_environment_fingerprint="F" * 64)
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_write_identity(status, args)
        self.assertEqual(ctx.exception.code, "database_mismatch")

    def test_readonly_role_blocks_writes(self):
        status = runner.RoleStatus("ddl_user", "ddl_user", "on", "on", "postgres", "F" * 64)
        args = argparse.Namespace(expected_current_user="ddl_user", expected_database="postgres", expected_environment_fingerprint="F" * 64)
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_write_identity(status, args)
        self.assertEqual(ctx.exception.code, "readonly_role_rejected")

    def test_catalog_without_acl_blocks(self):
        catalog = copy.deepcopy(self.prestate)
        del catalog["target_objects"]["cg_core.v_ruta_rutero_load_batch_week_v2"]["acl"]
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_catalog_complete(catalog, prestate=True)
        self.assertTrue(ctx.exception.code.startswith("catalog_incomplete:acl."))

    def test_catalog_without_view_definition_blocks(self):
        catalog = copy.deepcopy(self.prestate)
        catalog["target_objects"]["cg_core.v_rr_frecuencia_base_resuelta_v2"]["view_definition"] = ""
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_catalog_complete(catalog, prestate=True)
        self.assertIn("view_definition", ctx.exception.code)

    def test_appended_columns_wrong_order_or_type_block(self):
        post = self._post_catalog()
        columns = post["target_objects"]["cg_core.v_rr_frecuencia_base_resuelta_v2"]["columns"]
        columns[-1]["data_type"] = "bigint"
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_post_catalog(post, self.prestate)
        self.assertEqual(ctx.exception.code, "postcheck_failed")
        self.assertIn("appended_signature", str(ctx.exception))

    def test_assignment_table_incomplete_blocks(self):
        post = self._post_catalog()
        post["target_objects"]["cg_core.ruta_rutero_week_assignment"]["indexes"] = []
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_post_catalog(post, self.prestate)
        self.assertEqual(ctx.exception.code, "postcheck_failed")
        self.assertIn("assignment_index", str(ctx.exception))

    def test_assignment_unexpected_index_blocks(self):
        post = self._post_catalog()
        post["target_objects"]["cg_core.ruta_rutero_week_assignment"]["indexes"].append(
            {"index_name": "ix_unexpected", "definition": "CREATE INDEX ix_unexpected ON cg_core.ruta_rutero_week_assignment (assigned_at)"}
        )
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_post_catalog(post, self.prestate)
        self.assertIn("assignment_indexes_exact", str(ctx.exception))

    def test_assignment_owner_must_match_contract(self):
        post = self._post_catalog()
        post["target_objects"]["cg_core.ruta_rutero_week_assignment"]["owner"] = "other_owner"
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_post_catalog(post, self.prestate)
        self.assertIn("assignment_owner", str(ctx.exception))

    def test_assignment_acl_must_include_readonly_select(self):
        post = self._post_catalog()
        post["target_objects"]["cg_core.ruta_rutero_week_assignment"]["acl"] = []
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_post_catalog(post, self.prestate)
        self.assertIn("assignment_acl", str(ctx.exception))

    def test_post_catalog_exact_contract_passes(self):
        result = runner.validate_post_catalog(self._post_catalog(), self.prestate)
        self.assertTrue(result["postcheck_passed"])

    def test_assignment_assigned_at_default_is_part_of_contract(self):
        assigned_at = next(
            column for column in runner.ASSIGNMENT_COLUMNS if column["column_name"] == "assigned_at"
        )
        self.assertTrue(assigned_at["column_default_present"])

    def test_error_after_ddl_reports_attempted_and_rolled_back(self):
        conn = FakeConnection()
        args = argparse.Namespace(
            apply=True,
            expected_sql13_sha256="A" * 64,
            expected_prestate_file_sha256=self.file_hash,
            expected_prestate_technical_fingerprint=self.tech_hash,
            expected_current_user="ddl_user",
            expected_database="postgres",
            expected_environment_fingerprint="F" * 64,
        )
        status = runner.RoleStatus("ddl_user", "ddl_user", "off", "off", "postgres", "F" * 64)
        sql = "-- NO APPLY\nBEGIN;\nCREATE TABLE x(id integer);\nROLLBACK;"
        with mock.patch.object(runner, "connect_db_from_env", return_value=conn), \
            mock.patch.object(runner, "begin_runner_transaction", return_value=status), \
            mock.patch.object(runner, "read_sql_artifact", return_value=sql), \
            mock.patch.object(runner, "validate_apply_baseline"), \
            mock.patch.object(runner, "fetch_catalog", return_value=self.prestate), \
            mock.patch.object(runner, "execute_static_sql", side_effect=RuntimeError("boom")):
            with self.assertRaises(runner.RunnerBlock) as ctx:
                runner.run_apply(args)
        self.assertTrue(ctx.exception.telemetry["writes_attempted"])
        self.assertTrue(ctx.exception.telemetry["rolled_back"])
        self.assertFalse(ctx.exception.telemetry["committed"])
        self.assertTrue(conn.rolled_back)

    def _baseline_catalog(self):
        catalog = copy.deepcopy(self.prestate)
        catalog["rollback_baseline"] = {
            "route_batch_count": 19,
            "max_ruta_batch_id": 25,
            "assignment_table_exists": False,
            "assignment_row_count": 0,
        }
        return catalog

    def test_apply_baseline_identical_allows_continue(self):
        cur = FakeCursor(fetchone=[(19, 25), (False,)])
        runner.validate_apply_baseline(cur, self._baseline_catalog())

    def test_apply_route_batch_count_changed_blocks(self):
        cur = FakeCursor(fetchone=[(20, 25), (False,)])
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_apply_baseline(cur, self._baseline_catalog())
        self.assertEqual(ctx.exception.code, "apply_route_batch_count_changed")

    def test_apply_high_water_changed_blocks(self):
        cur = FakeCursor(fetchone=[(19, 26), (False,)])
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_apply_baseline(cur, self._baseline_catalog())
        self.assertEqual(ctx.exception.code, "apply_max_ruta_batch_id_changed")

    def test_apply_assignment_table_appearance_blocks(self):
        cur = FakeCursor(fetchone=[(19, 25), (True,), (0,)])
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_apply_baseline(cur, self._baseline_catalog())
        self.assertEqual(ctx.exception.code, "apply_assignment_table_state_changed")

    def test_apply_assignment_rows_block(self):
        cur = FakeCursor(fetchone=[(19, 25), (True,), (1,)])
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_apply_baseline(cur, self._baseline_catalog())
        self.assertEqual(ctx.exception.code, "apply_assignment_row_count_changed")

    def test_historical_prestate_batches_do_not_block_rollback(self):
        baseline = copy.deepcopy(self.prestate)
        baseline["rollback_baseline"] = {"route_batch_count": 19, "max_ruta_batch_id": 25, "assignment_table_exists": False, "assignment_row_count": 0}
        cur = FakeCursor(fetchone=[(19, 25), (False,)])
        runner.validate_rollback_safety(cur, baseline, "P" * 64, "P" * 64, "R" * 64)

    def test_batch_after_baseline_blocks_rollback(self):
        baseline = copy.deepcopy(self.prestate)
        baseline["rollback_baseline"] = {"route_batch_count": 19, "max_ruta_batch_id": 25, "assignment_table_exists": False, "assignment_row_count": 0}
        cur = FakeCursor(fetchone=[(20, 26), (False,)])
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_rollback_safety(cur, baseline, "P" * 64, "P" * 64, "R" * 64)
        self.assertTrue(ctx.exception.code.startswith("rollback_batch_baseline_changed"))

    def test_assignment_rows_block_rollback(self):
        baseline = copy.deepcopy(self.prestate)
        baseline["rollback_baseline"] = {"route_batch_count": 19, "max_ruta_batch_id": 25, "assignment_table_exists": False, "assignment_row_count": 0}
        cur = FakeCursor(fetchone=[(19, 25), (True,), (1,)])
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_rollback_safety(cur, baseline, "P" * 64, "P" * 64, "R" * 64)
        self.assertEqual(ctx.exception.code, "rollback_assignment_rows_present")

    def test_json_out_path_escape_blocks(self):
        outside = ROOT.parent / "outside_runner_output.json"
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.safe_json_out_path(str(outside), None)
        self.assertEqual(ctx.exception.code, "json_out_path_escape_blocked")

    def test_json_out_temp_allowed(self):
        out = Path(tempfile.gettempdir()) / "cg005n_runner_test.json"
        self.assertEqual(runner.safe_json_out_path(str(out), None), out.resolve(strict=False))

    def test_json_redaction_does_not_leak_dsn(self):
        fake_dsn = "postgres" + "ql://user:secret@" + "db.example.com/app?" + "pass" + "word=hidden"
        payload = {"error": runner.redact_secret(fake_dsn)}
        text = json.dumps(payload)
        self.assertNotIn("secret", text)
        self.assertNotIn("hidden", text)
        self.assertNotIn("db.example.com", text)

    def test_sql13_and_sql14_keep_no_apply_and_no_drop_view(self):
        for path in (runner.SQL13_PATH, runner.SQL14_PATH):
            text = path.read_text(encoding="utf-8")
            self.assertTrue(text.startswith("-- NO APPLY"))
            self.assertIn("rollback", text.lower())
            self.assertNotRegex(text.upper(), r"\bDROP\s+VIEW\b")

    def test_read_sql_artifact_uses_raw_bytes_lf_and_crlf(self):
        lf = b"-- no apply\nbegin;\nselect 1;\nrollback;\n"
        crlf = b"-- no apply\r\nbegin;\r\nselect 1;\r\nrollback;\r\n"
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            lf_path = Path(tmp) / "lf.sql"
            crlf_path = Path(tmp) / "crlf.sql"
            lf_path.write_bytes(lf)
            crlf_path.write_bytes(crlf)
            self.assertNotEqual(runner.file_sha256(lf_path), runner.file_sha256(crlf_path))
            self.assertEqual(runner.read_sql_artifact(lf_path, runner.file_sha256(lf_path)), lf.decode("utf-8"))
            self.assertEqual(runner.read_sql_artifact(crlf_path, runner.file_sha256(crlf_path)), crlf.decode("utf-8"))

    def test_read_sql_artifact_wrong_hash_blocks(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            path = Path(tmp) / "wrong_hash.sql"
            path.write_bytes(b"-- NO APPLY\nBEGIN;\nselect 1;\nROLLBACK;\n")
            with self.assertRaises(runner.RunnerBlock) as ctx:
                runner.read_sql_artifact(path, "0" * 64)
        self.assertEqual(ctx.exception.code, "sql_sha256_mismatch")

    def test_real_sql_artifacts_read_and_extract_without_mocks(self):
        sql13 = runner.read_sql_artifact(runner.SQL13_PATH, runner.file_sha256(runner.SQL13_PATH))
        sql14 = runner.read_sql_artifact(runner.SQL14_PATH, runner.file_sha256(runner.SQL14_PATH))
        self.assertTrue(runner.extract_apply_body(sql13))
        self.assertTrue(runner.extract_apply_body(sql14))

    def test_base_plan_hashes_match_read_sql_artifact_hashes(self):
        payload = runner.base_plan_payload(self.prestate)
        self.assertEqual(payload["sql13_sha256"], runner.file_sha256(runner.SQL13_PATH))
        self.assertEqual(payload["sql14_sha256"], runner.file_sha256(runner.SQL14_PATH))
        runner.read_sql_artifact(runner.SQL13_PATH, payload["sql13_sha256"])
        runner.read_sql_artifact(runner.SQL14_PATH, payload["sql14_sha256"])

    def test_read_sql_artifact_markers_are_case_insensitive(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            path = Path(tmp) / "case.sql"
            path.write_text("-- no apply\nbegin;\nselect 1;\nrollback;\n", encoding="utf-8")
            self.assertIn("select 1", runner.read_sql_artifact(path, runner.file_sha256(path)))

    def test_no_shell_true(self):
        source = runner.Path(runner.__file__).read_text(encoding="utf-8")
        self.assertNotIn("shell" + "=True", source)

    def test_no_free_sql_cli(self):
        parser = runner.build_parser()
        help_text = parser.format_help()
        self.assertNotIn("--sql", help_text)
        self.assertNotIn("--query", help_text)
        self.assertNotIn("--stdin", help_text)

    def test_technical_fingerprint_excludes_rollback_baseline(self):
        changed = copy.deepcopy(self.prestate)
        changed["rollback_baseline"]["route_batch_count"] = 999999
        self.assertEqual(runner.technical_fingerprint(changed), self.tech_hash)
        self.assertNotEqual(runner.sha256_text(runner.canonical_json(changed["rollback_baseline"])), self.rollback_hash)

    def test_prestage_and_runner_share_technical_payload(self):
        self.assertEqual(prestage.technical_catalog_payload(self.prestate), runner.technical_payload(self.prestate))
        self.assertEqual(prestage.technical_catalog_payload(self.prestate), contract.technical_payload(self.prestate))

    def test_prestage_and_runner_share_technical_fingerprint(self):
        self.assertEqual(prestage.attach_separate_hashes.__globals__["catalog_contract"].technical_fingerprint(self.prestate), runner.technical_fingerprint(self.prestate))
        self.assertEqual(runner.technical_fingerprint(self.prestate), contract.technical_fingerprint(self.prestate))

    def test_canonical_order_is_deterministic_for_acl_and_dependencies(self):
        catalog = copy.deepcopy(self.prestate)
        view = "cg_core.v_ruta_rutero_load_batch_week_v2"
        obj = catalog["target_objects"][view]
        obj["acl"] = list(reversed(obj["acl"]))
        catalog["direct_dependencies"][view] = list(reversed(catalog["direct_dependencies"][view]))
        first = contract.technical_payload(catalog)
        second = contract.technical_payload(copy.deepcopy(catalog))
        self.assertEqual(first, second)

    def test_acl_comments_reloptions_view_options_normalize_shared(self):
        view = "cg_core.v_ruta_rutero_load_batch_week_v2"
        obj = self.prestate["target_objects"][view]
        self.assertEqual(runner.acl_identity(obj), contract.acl_identity(obj))
        normalized = contract.normalize_object(view, obj)
        self.assertIn("comment", normalized)
        self.assertIn("reloptions", normalized)
        self.assertIn("view_options", normalized)

    def test_dependency_model_shared_direct_and_reverse(self):
        normalized = contract.normalize_catalog(self.prestate, prestate=True)
        self.assertEqual(set(normalized["direct_dependencies"]), set(contract.VIEW_TARGETS))
        self.assertEqual(set(normalized["reverse_dependencies"]), set(contract.VIEW_TARGETS))
        self.assertEqual(normalized["material_dependency_unknowns"], [])

    def test_unknown_material_dependency_blocks_runner_and_prestage(self):
        catalog = copy.deepcopy(self.prestate)
        view = "cg_core.v_ruta_rutero_load_batch_week_v2"
        catalog["reverse_dependencies"][view].append(
            {
                "target_object": view,
                "dependent_schema": "unknown_schema",
                "dependent_name": "mystery_view",
                "dependent_relkind": "v",
            }
        )
        normalized = contract.normalize_catalog(catalog, prestate=True)
        self.assertEqual(normalized["completion"]["catalog_gap"], "material_dependency_unknowns")
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_catalog_complete(normalized, prestate=True)
        self.assertEqual(ctx.exception.code, "catalog_incomplete:material_dependency_unknowns")
        self.assertEqual(prestage.find_catalog_gap(normalized), "material_dependency_unknowns")

    def test_incomplete_catalog_blocks_same_classification(self):
        catalog = copy.deepcopy(self.prestate)
        view = "cg_core.v_rr_frecuencia_base_resuelta_v2"
        catalog["target_objects"][view]["view_definition"] = ""
        normalized = contract.normalize_catalog(catalog, prestate=True)
        with self.assertRaises(runner.RunnerBlock) as ctx:
            runner.validate_catalog_complete(normalized, prestate=True)
        self.assertEqual(ctx.exception.code, f"catalog_incomplete:views.{view}.view_definition")
        self.assertEqual(prestage.find_catalog_gap(normalized), f"views.{view}.view_definition")

    def test_real_acquisition_paths_share_normalized_catalog(self):
        prestage_catalog = self._synthetic_prestage_catalog()
        runner_catalog = self._synthetic_runner_catalog()
        self.assertEqual(prestage_catalog["target_objects"], runner_catalog["target_objects"])
        self.assertEqual(prestage_catalog["direct_dependencies"], runner_catalog["direct_dependencies"])
        self.assertEqual(prestage_catalog["reverse_dependencies"], runner_catalog["reverse_dependencies"])
        self.assertEqual(prestage_catalog["completion"], runner_catalog["completion"])

    def test_real_acquisition_paths_share_payload_and_fingerprint(self):
        prestage_catalog = self._synthetic_prestage_catalog()
        runner_catalog = self._synthetic_runner_catalog()
        self.assertEqual(contract.technical_payload(prestage_catalog), contract.technical_payload(runner_catalog))
        self.assertEqual(contract.technical_fingerprint(prestage_catalog), contract.technical_fingerprint(runner_catalog))

    def test_dependency_aliases_are_removed_from_canonical_output(self):
        view = "cg_core.v_ruta_rutero_load_batch_week_v2"
        direct_alias = {view: [{"target_object": view, "referenced_schema": "cg_core", "referenced_name": "ruta_rutero_load_batch", "referenced_kind": "r"}]}
        direct_canonical = {view: [{"target_object": view, "referenced_schema": "cg_core", "referenced_name": "ruta_rutero_load_batch", "referenced_relkind": "r"}]}
        reverse_alias = {view: [{"target_object": view, "dependent_schema": "cg_mart", "dependent_name": "fact_cg_out_weekly_v2", "dependent_kind": "m"}]}
        reverse_canonical = {view: [{"target_object": view, "dependent_schema": "cg_mart", "dependent_name": "fact_cg_out_weekly_v2", "dependent_relkind": "m"}]}
        normalized_direct = contract.normalize_dependency_section(direct_alias, direction="direct")
        normalized_reverse = contract.normalize_dependency_section(reverse_alias, direction="reverse")
        self.assertEqual(normalized_direct, contract.normalize_dependency_section(direct_canonical, direction="direct"))
        self.assertEqual(normalized_reverse, contract.normalize_dependency_section(reverse_canonical, direction="reverse"))
        self.assertNotIn("referenced_kind", normalized_direct[view][0])
        self.assertNotIn("dependent_kind", normalized_reverse[view][0])

    def test_real_path_order_variation_does_not_change_fingerprint(self):
        baseline = self._synthetic_runner_catalog()
        shuffled = copy.deepcopy(baseline)
        view = "cg_core.v_ruta_rutero_load_batch_week_v2"
        shuffled["target_objects"][view]["acl"] = list(reversed(shuffled["target_objects"][view]["acl"]))
        shuffled["target_objects"]["cg_core.ruta_rutero_load_batch"]["constraints"] = list(
            reversed(shuffled["target_objects"]["cg_core.ruta_rutero_load_batch"]["constraints"])
        )
        shuffled["target_objects"]["cg_core.ruta_rutero_load_batch"]["indexes"] = list(
            reversed(shuffled["target_objects"]["cg_core.ruta_rutero_load_batch"]["indexes"])
        )
        shuffled["direct_dependencies"][view] = list(reversed(shuffled["direct_dependencies"][view]))
        shuffled["reverse_dependencies"][view] = list(reversed(shuffled["reverse_dependencies"][view]))
        self.assertEqual(contract.technical_fingerprint(baseline), contract.technical_fingerprint(shuffled))

    def test_real_path_auxiliary_hashes_and_relacl_do_not_diverge(self):
        prestage_catalog = self._synthetic_prestage_catalog()
        runner_catalog = self._synthetic_runner_catalog()
        view = "cg_core.v_ruta_rutero_load_batch_week_v2"
        prestage_obj = prestage_catalog["target_objects"][view]
        runner_obj = runner_catalog["target_objects"][view]
        self.assertEqual(prestage_obj["relacl"], runner_obj["relacl"])
        self.assertEqual(prestage_obj["view_definition_sha256"], runner_obj["view_definition_sha256"])
        self.assertEqual(prestage_obj["column_signature_sha256"], runner_obj["column_signature_sha256"])

    def test_completion_is_rebuilt_canonically_for_fingerprint(self):
        catalog = self._synthetic_runner_catalog()
        tampered = copy.deepcopy(catalog)
        tampered["completion"] = {
            "unknown_dependencies": 99,
            "catalog_complete": False,
            "technical_catalog_complete": False,
            "catalog_gap": "manual",
            "stop_required": True,
        }
        self.assertEqual(contract.technical_payload(catalog), contract.technical_payload(tampered))
        self.assertEqual(contract.technical_fingerprint(catalog), contract.technical_fingerprint(tampered))

    def test_real_path_rollback_baseline_excluded_from_fingerprint(self):
        catalog = self._synthetic_runner_catalog()
        changed = copy.deepcopy(catalog)
        changed["rollback_baseline"]["route_batch_count"] = 123456
        self.assertEqual(contract.technical_fingerprint(catalog), contract.technical_fingerprint(changed))
        self.assertNotEqual(
            contract.rollback_baseline_fingerprint(catalog),
            contract.rollback_baseline_fingerprint(changed),
        )

    def test_real_path_technical_field_change_changes_fingerprint(self):
        catalog = self._synthetic_runner_catalog()
        changed = copy.deepcopy(catalog)
        changed["target_objects"]["cg_core.v_ruta_rutero_load_batch_week_v2"]["columns"][0]["data_type"] = "numeric"
        self.assertNotEqual(contract.technical_fingerprint(catalog), contract.technical_fingerprint(changed))

    def test_sql_artifacts_do_not_contain_obsolete_fingerprint_comments(self):
        obsolete = "1B45468E28453EE29A5A1BB9530D746B" + "CCA208A8083FA9386A398BF738A1B5C8"
        for path in (runner.SQL13_PATH, runner.SQL14_PATH):
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("catalog fingerprint", text.lower())
            self.assertNotIn(obsolete, text)


if __name__ == "__main__":
    unittest.main()
