#!/usr/bin/env python
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

try:
    import cg005n_catalog_contract as catalog_contract
except ImportError:  # pragma: no cover - package import path
    from scripts import cg005n_catalog_contract as catalog_contract


ROOT = Path(__file__).resolve().parents[1]
PRESTATE_CATALOG_PATH = ROOT / "research" / "CG005N_PRESTATE_CATALOG.json"
SQL13_PATH = ROOT / "sql" / "13_control_gestion_route_week_replacement_signature_preserving.sql"
SQL14_PATH = ROOT / "sql" / "14_control_gestion_route_week_replacement_signature_preserving_rollback.sql"
DDL_LOCK_KEY = 55005
APPLICATION_NAME = "stock_zero_cg005n_ddl_runner"
PHASE = "CG005NQ_PACKAGE_CORRECTION_NO_SUPABASE_WRITE"
ASSIGNMENT_EXPECTED_OWNER = "postgres"

TARGET_OBJECTS = catalog_contract.TARGET_OBJECTS
VIEW_TARGETS = catalog_contract.VIEW_TARGETS

APPENDED_SIGNATURES = {
    "cg_core.v_ruta_rutero_load_batch_week_v2": (
        {"column_name": "route_policy_version", "data_type": "text", "udt_name": "text", "is_nullable": "YES"},
        {"column_name": "route_week_source", "data_type": "text", "udt_name": "text", "is_nullable": "YES"},
        {"column_name": "assignment_id", "data_type": "bigint", "udt_name": "int8", "is_nullable": "YES"},
        {
            "column_name": "assigned_at",
            "data_type": "timestamp with time zone",
            "udt_name": "timestamptz",
            "is_nullable": "YES",
        },
    ),
    "cg_core.v_ruta_rutero_latest_week_batch_v2": (
        {"column_name": "route_policy_version", "data_type": "text", "udt_name": "text", "is_nullable": "YES"},
        {"column_name": "route_week_source", "data_type": "text", "udt_name": "text", "is_nullable": "YES"},
        {"column_name": "assignment_id", "data_type": "bigint", "udt_name": "int8", "is_nullable": "YES"},
    ),
    "cg_core.v_rr_frecuencia_base_resuelta_v2": (
        {"column_name": "route_policy_version", "data_type": "text", "udt_name": "text", "is_nullable": "YES"},
        {"column_name": "route_week_source", "data_type": "text", "udt_name": "text", "is_nullable": "YES"},
        {"column_name": "cod_rt_norm", "data_type": "text", "udt_name": "text", "is_nullable": "YES"},
        {"column_name": "ruta_person_conflict_flag", "data_type": "integer", "udt_name": "int4", "is_nullable": "YES"},
    ),
}

ASSIGNMENT_COLUMNS = (
    {"column_name": "assignment_id", "data_type": "bigint", "udt_name": "int8", "is_nullable": "NO"},
    {"column_name": "effective_week_start", "data_type": "date", "udt_name": "date", "is_nullable": "NO"},
    {"column_name": "route_policy_version", "data_type": "text", "udt_name": "text", "is_nullable": "NO"},
    {"column_name": "ruta_batch_id", "data_type": "bigint", "udt_name": "int8", "is_nullable": "NO"},
    {"column_name": "assignment_status", "data_type": "text", "udt_name": "text", "is_nullable": "NO"},
    {"column_name": "input_file_name", "data_type": "text", "udt_name": "text", "is_nullable": "NO"},
    {"column_name": "input_file_sha256", "data_type": "text", "udt_name": "text", "is_nullable": "NO"},
    {"column_name": "schema_signature", "data_type": "text", "udt_name": "text", "is_nullable": "NO"},
    {"column_name": "current_surface_hash", "data_type": "text", "udt_name": "text", "is_nullable": "NO"},
    {"column_name": "resolved_surface_hash", "data_type": "text", "udt_name": "text", "is_nullable": "NO"},
    {
        "column_name": "assigned_at",
        "data_type": "timestamp with time zone",
        "udt_name": "timestamptz",
        "is_nullable": "NO",
        "column_default_present": True,
    },
    {"column_name": "assigned_by", "data_type": "text", "udt_name": "text", "is_nullable": "NO"},
    {"column_name": "replaces_ruta_batch_id", "data_type": "bigint", "udt_name": "int8", "is_nullable": "YES"},
    {"column_name": "rollback_of_assignment_id", "data_type": "bigint", "udt_name": "int8", "is_nullable": "YES"},
    {"column_name": "notes", "data_type": "text", "udt_name": "text", "is_nullable": "YES"},
)
ASSIGNMENT_REQUIRED_CONSTRAINT_FRAGMENTS = (
    "PRIMARY KEY",
    "FOREIGN KEY (ruta_batch_id)",
    "FOREIGN KEY (rollback_of_assignment_id)",
    "effective_week_start",
    "assignment_status",
    "route_policy_version",
    "current_surface_hash",
    "resolved_surface_hash",
)
ASSIGNMENT_REQUIRED_INDEXES = (
    "ruta_rutero_week_assignment_pkey",
    "ix_ruta_rutero_week_assignment_week",
    "ux_ruta_rutero_week_assignment_active",
)

POSTGRES_URL_RE = re.compile(r"postgres(?:ql)?://[^\s'\"<>]+", re.IGNORECASE)
PASSWORD_RE = re.compile(r"(?i)(password\s*=\s*)[^\s;&]+")
HOST_RE = re.compile(r"(?i)(host\s*=\s*)[^\s;]+")
USERINFO_RE = re.compile(r"(postgres(?:ql)?://)[^/@\s]+@", re.IGNORECASE)


class RunnerBlock(RuntimeError):
    def __init__(self, code: str, detail: str | None = None, telemetry: dict[str, Any] | None = None):
        super().__init__(detail or code)
        self.code = code
        self.telemetry = telemetry or {}


@dataclass(frozen=True)
class RoleStatus:
    current_user: str
    session_user: str
    transaction_read_only: str
    default_transaction_read_only: str
    database: str
    environment_fingerprint: str


def redact_secret(value: object) -> str:
    text = str(value)
    text = POSTGRES_URL_RE.sub("postgresql://<redacted>", text)
    text = USERINFO_RE.sub(r"\1<redacted>@", text)
    text = PASSWORD_RE.sub(r"\1<redacted>", text)
    text = HOST_RE.sub(r"\1<redacted>", text)
    return text


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    return value


def canonical_json(value: Any) -> str:
    return catalog_contract.canonical_json(value)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest().upper()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def file_sha256(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def resolve_repo_file(path: Path) -> Path:
    if path.is_symlink():
        raise RunnerBlock("path_symlink_blocked", str(path))
    resolved = path.resolve(strict=True)
    root = ROOT.resolve(strict=True)
    if root != resolved and root not in resolved.parents:
        raise RunnerBlock("path_escape_blocked", str(path))
    if resolved.is_symlink() or not resolved.is_file():
        raise RunnerBlock("path_invalid", str(path))
    return resolved


def read_sql_artifact(path: Path, expected_sha256: str) -> str:
    resolved = resolve_repo_file(path)
    raw = resolved.read_bytes()
    actual = sha256_bytes(raw)
    if actual != expected_sha256.upper():
        raise RunnerBlock("sql_sha256_mismatch", f"{resolved.name}: {actual}")
    text = raw.decode("utf-8")
    if not re.search(r"NO\s+APPLY", text, re.IGNORECASE):
        raise RunnerBlock("sql_contract_marker_missing", resolved.name)
    if not re.search(r"^\s*BEGIN\s*;\s*$", text, re.IGNORECASE | re.MULTILINE):
        raise RunnerBlock("sql_contract_marker_missing", resolved.name)
    if not re.search(r"^\s*ROLLBACK\s*;\s*$", text, re.IGNORECASE | re.MULTILINE):
        raise RunnerBlock("sql_contract_marker_missing", resolved.name)
    if re.search(r"\bDROP\s+VIEW\b", text, re.IGNORECASE):
        raise RunnerBlock("drop_view_forbidden", resolved.name)
    return text


def extract_apply_body(sql_text: str) -> str:
    start = re.search(r"^\s*BEGIN\s*;\s*$", sql_text, re.IGNORECASE | re.MULTILINE)
    end = re.search(r"^\s*ROLLBACK\s*;\s*$", sql_text, re.IGNORECASE | re.MULTILINE)
    if not start or not end or start.end() >= end.start():
        raise RunnerBlock("sql_artifact_not_no_apply_transaction")
    return sql_text[start.end() : end.start()].strip()


def count_ddl_statements(sql_text: str) -> int:
    matches = re.findall(
        r"\b(CREATE|ALTER|DROP|COMMENT|GRANT|REVOKE)\b",
        re.sub(r"--.*?$", "", sql_text, flags=re.MULTILINE),
        flags=re.IGNORECASE,
    )
    return len(matches)


def technical_payload(catalog: dict[str, Any]) -> dict[str, Any]:
    return catalog_contract.technical_payload(catalog)


def technical_fingerprint(catalog: dict[str, Any]) -> str:
    return catalog_contract.technical_fingerprint(catalog)


def load_prestate_catalog(path: Path = PRESTATE_CATALOG_PATH) -> dict[str, Any]:
    resolved = resolve_repo_file(path)
    raw = resolved.read_bytes()
    catalog = json.loads(raw.decode("utf-8"))
    catalog["_prestate_file_sha256"] = sha256_bytes(raw)
    catalog["technical_catalog_fingerprint_sha256"] = technical_fingerprint(catalog)
    catalog["rollback_baseline_sha256"] = catalog_contract.rollback_baseline_fingerprint(catalog)
    return catalog


def validate_prestate_integrity(
    catalog: dict[str, Any],
    expected_prestate_file_sha256: str,
    expected_prestate_technical_fingerprint: str,
) -> None:
    file_hash = catalog.get("_prestate_file_sha256")
    tech_hash = catalog.get("technical_catalog_fingerprint_sha256") or technical_fingerprint(catalog)
    if file_hash != expected_prestate_file_sha256.upper():
        raise RunnerBlock("prestate_file_sha256_mismatch", str(file_hash))
    if tech_hash != expected_prestate_technical_fingerprint.upper():
        raise RunnerBlock("prestate_technical_fingerprint_mismatch", str(tech_hash))


def validate_catalog_complete(catalog: dict[str, Any], *, prestate: bool) -> None:
    missing = catalog_contract.find_catalog_gap(catalog, prestate=prestate)
    if missing:
        raise RunnerBlock(f"catalog_incomplete:{missing}")


def column_identity(column: dict[str, Any], *, include_position: bool = True) -> dict[str, Any]:
    return catalog_contract.column_identity(column, include_position=include_position, include_default=True)


def expected_column_identity(column: dict[str, Any], *, include_position: bool = False) -> dict[str, Any]:
    expected = dict(column)
    expected.setdefault("column_default_present", False)
    return column_identity(expected, include_position=include_position)


def exact_prefix(columns: Sequence[dict[str, Any]], length: int) -> list[dict[str, Any]]:
    return [column_identity(column, include_position=True) for column in columns[:length]]


def appended_identity(columns: Sequence[dict[str, Any]], start: int) -> list[dict[str, Any]]:
    return [column_identity(column, include_position=False) for column in columns[start:]]


def acl_identity(obj: dict[str, Any]) -> list[dict[str, Any]]:
    return catalog_contract.acl_identity(obj)


def assignment_table_contract(obj: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if obj.get("exists") is not True:
        return ["assignment_missing"]
    columns = [column_identity(column, include_position=False) for column in obj.get("columns", [])]
    expected_columns = [expected_column_identity(column, include_position=False) for column in ASSIGNMENT_COLUMNS]
    if columns != expected_columns:
        errors.append("assignment_columns")
    constraints = {row.get("constraint_name"): str(row.get("definition", "")) for row in obj.get("constraints", [])}
    expected_constraints = {
        "ruta_rutero_week_assignment_pkey": "PRIMARY KEY (assignment_id)",
        "ruta_rutero_week_assignment_batch_fk": "FOREIGN KEY (ruta_batch_id)",
        "ruta_rutero_week_assignment_rollback_fk": "FOREIGN KEY (rollback_of_assignment_id)",
        "ruta_rutero_week_assignment_monday_check": "effective_week_start",
        "ruta_rutero_week_assignment_status_check": "assignment_status",
        "ruta_rutero_week_assignment_policy_check": "route_policy_version",
        "ruta_rutero_week_assignment_current_hash_check": "current_surface_hash",
        "ruta_rutero_week_assignment_resolved_hash_check": "resolved_surface_hash",
    }
    if set(constraints) != set(expected_constraints):
        errors.append("assignment_constraints_exact")
    for constraint_name, fragment in expected_constraints.items():
        if fragment not in constraints.get(constraint_name, ""):
            errors.append(f"assignment_constraint:{constraint_name}")
    definitions = " ".join(constraints.values())
    for fragment in ASSIGNMENT_REQUIRED_CONSTRAINT_FRAGMENTS:
        if fragment not in definitions:
            errors.append(f"assignment_constraint_fragment:{fragment}")
    indexes = {row.get("index_name"): str(row.get("definition", "")) for row in obj.get("indexes", [])}
    if set(indexes) != set(ASSIGNMENT_REQUIRED_INDEXES):
        errors.append("assignment_indexes_exact")
    for index_name in ASSIGNMENT_REQUIRED_INDEXES:
        if index_name not in indexes:
            errors.append(f"assignment_index:{index_name}")
    unique_def = indexes.get("ux_ruta_rutero_week_assignment_active", "")
    if not re.search(r"\bUNIQUE\b", unique_def, re.IGNORECASE) or not re.search(
        r"WHERE\s+.*assignment_status\s*=\s*'ACTIVE'", unique_def, re.IGNORECASE
    ):
        errors.append("assignment_index:ux_active_condition")
    if obj.get("owner") != ASSIGNMENT_EXPECTED_OWNER:
        errors.append("assignment_owner")
    acl = obj.get("acl")
    if not isinstance(acl, list) or not any(
        row.get("grantee") == "stock_zero_readonly" and row.get("privilege_type") == "SELECT"
        for row in acl
    ):
        errors.append("assignment_acl")
    return errors


def validate_post_catalog(current: dict[str, Any], prestate: dict[str, Any]) -> dict[str, Any]:
    validate_catalog_complete(current, prestate=False)
    validate_catalog_complete(prestate, prestate=True)
    failures: list[str] = []
    for view_name, expected_appended in APPENDED_SIGNATURES.items():
        before = prestate["target_objects"][view_name]["columns"]
        after = current["target_objects"][view_name]["columns"]
        if exact_prefix(after, len(before)) != exact_prefix(before, len(before)):
            failures.append(f"{view_name}:prefix")
        appended = appended_identity(after, len(before))
        if appended != [expected_column_identity(item, include_position=False) for item in expected_appended]:
            failures.append(f"{view_name}:appended_signature")
        if len(after) != len(before) + len(expected_appended):
            failures.append(f"{view_name}:unexpected_columns")
        before_obj = prestate["target_objects"][view_name]
        after_obj = current["target_objects"][view_name]
        for key in ("owner", "comment", "reloptions", "view_options"):
            if after_obj.get(key) != before_obj.get(key):
                failures.append(f"{view_name}:{key}")
        if acl_identity(after_obj) != acl_identity(before_obj):
            failures.append(f"{view_name}:acl")
    failures.extend(assignment_table_contract(current["target_objects"]["cg_core.ruta_rutero_week_assignment"]))
    if failures:
        raise RunnerBlock("postcheck_failed", ",".join(failures))
    return {"postcheck_passed": True, "views_checked": len(APPENDED_SIGNATURES)}


def connect_db_from_env():
    db_url = os.getenv("DB_URL_DDL")
    if not db_url:
        raise RunnerBlock("db_url_ddl_required")
    try:
        import psycopg2
    except Exception as exc:  # pragma: no cover
        raise RunnerBlock("psycopg2_unavailable", redact_secret(exc)) from exc
    return psycopg2.connect(db_url)


def environment_fingerprint(cur) -> tuple[str, str]:
    cur.execute(
        """
        SELECT current_database(),
               COALESCE(inet_server_addr()::text, '<local>'),
               COALESCE(inet_server_port(), 0)::text,
               current_setting('server_version_num')
        """
    )
    database, host, port, version = cur.fetchone()
    return database, sha256_text(canonical_json({"database": database, "host": host, "port": port, "version": version}))


def begin_runner_transaction(conn, *, write: bool) -> RoleStatus:
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("BEGIN READ WRITE" if write else "BEGIN READ ONLY")
        cur.execute("SET LOCAL lock_timeout = '5s'")
        cur.execute("SET LOCAL statement_timeout = '30s'")
        cur.execute("SET LOCAL application_name = %s", (APPLICATION_NAME,))
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (DDL_LOCK_KEY,))
        cur.execute(
            """
            SELECT current_user,
                   session_user,
                   current_setting('transaction_read_only'),
                   current_setting('default_transaction_read_only')
            """
        )
        current_user, session_user, transaction_read_only, default_transaction_read_only = cur.fetchone()
        database, env_fp = environment_fingerprint(cur)
    return RoleStatus(
        current_user=current_user,
        session_user=session_user,
        transaction_read_only=transaction_read_only,
        default_transaction_read_only=default_transaction_read_only,
        database=database,
        environment_fingerprint=env_fp,
    )


def validate_write_identity(status: RoleStatus, args: argparse.Namespace) -> None:
    if status.transaction_read_only == "on":
        raise RunnerBlock("readonly_role_rejected")
    if status.current_user != args.expected_current_user:
        raise RunnerBlock("ddl_role_mismatch")
    if status.database != args.expected_database:
        raise RunnerBlock("database_mismatch")
    if status.environment_fingerprint != args.expected_environment_fingerprint.upper():
        raise RunnerBlock("environment_fingerprint_mismatch")


def rows_to_dicts(cur) -> list[dict[str, Any]]:
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def split_object_name(object_name: str) -> tuple[str, str]:
    schema, relation = object_name.split(".", 1)
    return schema, relation


def fetch_catalog(cur) -> dict[str, Any]:
    catalog: dict[str, Any] = {
        "phase": PHASE,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "target_objects": {},
        "direct_dependencies": {},
        "reverse_dependencies": {},
        "material_dependency_unknowns": [],
        "completion": {},
    }
    for object_name in TARGET_OBJECTS:
        schema, relation = split_object_name(object_name)
        cur.execute(
            """
            SELECT c.oid,
                   c.relkind,
                   pg_get_userbyid(c.relowner) AS owner,
                   obj_description(c.oid, 'pg_class') AS comment,
                   COALESCE(c.reloptions, ARRAY[]::text[]) AS reloptions,
                   c.relacl::text[] AS relacl
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = %s
               AND c.relname = %s
            """,
            (schema, relation),
        )
        row = cur.fetchone()
        exists = row is not None
        obj: dict[str, Any] = {
            "schema": schema,
            "name": relation,
            "exists": exists,
            "columns": [],
            "constraints": [],
            "indexes": [],
            "acl": [],
            "grants": [],
            "comment": None,
            "comment_captured": exists,
            "description_present": False,
            "reloptions": [],
            "relacl": None,
            "view_definition": None,
            "view_definition_sha256": None,
            "column_signature_sha256": None,
        }
        if exists:
            oid, relkind, owner, comment, reloptions, relacl = row
            obj.update(
                {
                    "relkind": relkind,
                    "owner": owner,
                    "comment": comment,
                    "comment_captured": True,
                    "description_present": comment is not None,
                    "reloptions": list(reloptions or []),
                    "relacl": list(relacl or []),
                }
            )
            cur.execute(
                """
                SELECT ordinal_position,
                       column_name,
                       data_type,
                       udt_name,
                       is_nullable,
                       column_default IS NOT NULL AS column_default_present
                  FROM information_schema.columns
                 WHERE table_schema = %s
                   AND table_name = %s
                 ORDER BY ordinal_position
                """,
                (schema, relation),
            )
            obj["columns"] = rows_to_dicts(cur)
            obj["column_signature_sha256"] = catalog_contract.sha256_canonical(obj["columns"])
            if object_name in VIEW_TARGETS:
                cur.execute(
                    "SELECT pg_get_viewdef(%s::regclass, true)",
                    (object_name,),
                )
                obj["view_definition"] = cur.fetchone()[0]
                obj["view_definition_sha256"] = catalog_contract.sha256_text(obj["view_definition"] or "")
                obj["view_options"] = {
                    "security_barrier": "security_barrier=true" in obj["reloptions"],
                    "check_option": next(
                        (opt.split("=", 1)[1] for opt in obj["reloptions"] if opt.startswith("check_option=")),
                        None,
                    ),
                }
            cur.execute(
                """
                SELECT conname AS constraint_name,
                       contype::text AS constraint_type,
                       pg_get_constraintdef(oid, true) AS definition
                  FROM pg_constraint
                 WHERE conrelid = %s::regclass
                 ORDER BY conname
                """,
                (object_name,),
            )
            obj["constraints"] = rows_to_dicts(cur)
            cur.execute(
                """
                SELECT indexname AS index_name,
                       indexdef AS definition
                  FROM pg_indexes
                 WHERE schemaname = %s
                   AND tablename = %s
                 ORDER BY indexname
                """,
                (schema, relation),
            )
            obj["indexes"] = rows_to_dicts(cur)
            cur.execute(
                """
                SELECT COALESCE((a).grantor::regrole::text, '') AS grantor,
                       COALESCE((a).grantee::regrole::text, '') AS grantee,
                       COALESCE((a).privilege_type::text, '') AS privilege_type,
                       CASE WHEN COALESCE((a).is_grantable, false) THEN 'YES' ELSE 'NO' END AS is_grantable
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  LEFT JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) a ON true
                 WHERE n.nspname = %s
                   AND c.relname = %s
                 ORDER BY grantee, privilege_type, grantor
                """,
                (schema, relation),
            )
            obj["acl"] = rows_to_dicts(cur)
            obj["grants"] = obj["acl"]
        catalog["target_objects"][object_name] = obj
    attach_dependency_sections(cur, catalog)
    attach_rollback_baseline(cur, catalog)
    return catalog_contract.normalize_catalog(
        catalog,
        prestate=not catalog["target_objects"]["cg_core.ruta_rutero_week_assignment"]["exists"],
    )


def attach_dependency_sections(cur, catalog: dict[str, Any]) -> None:
    # Direct and reverse dependencies are intentionally stored as structured rows, not text-only summaries.
    for view_name in VIEW_TARGETS:
        cur.execute(
            """
            SELECT DISTINCT
                   referenced_ns.nspname AS referenced_schema,
                   referenced.relname AS referenced_name,
                   referenced.relkind AS referenced_kind
              FROM pg_rewrite rw
              JOIN pg_depend dep ON dep.objid = rw.oid
              JOIN pg_class view_rel ON view_rel.oid = rw.ev_class
              JOIN pg_namespace view_ns ON view_ns.oid = view_rel.relnamespace
              JOIN pg_class referenced ON referenced.oid = dep.refobjid
              JOIN pg_namespace referenced_ns ON referenced_ns.oid = referenced.relnamespace
             WHERE view_ns.nspname || '.' || view_rel.relname = %s
               AND referenced_ns.nspname || '.' || referenced.relname <> %s
             ORDER BY referenced_schema, referenced_name, referenced_kind
            """,
            (view_name, view_name),
        )
        catalog["direct_dependencies"][view_name] = rows_to_dicts(cur)
        cur.execute(
            """
            SELECT DISTINCT
                   dependent_ns.nspname AS dependent_schema,
                   dependent.relname AS dependent_name,
                   dependent.relkind AS dependent_kind
              FROM pg_rewrite rw
              JOIN pg_depend dep ON dep.objid = rw.oid
              JOIN pg_class dependent ON dependent.oid = rw.ev_class
              JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.relnamespace
              JOIN pg_class target ON target.oid = dep.refobjid
              JOIN pg_namespace target_ns ON target_ns.oid = target.relnamespace
             WHERE target_ns.nspname || '.' || target.relname = %s
               AND dependent_ns.nspname || '.' || dependent.relname <> %s
             ORDER BY dependent_schema, dependent_name, dependent_kind
            """,
            (view_name, view_name),
        )
        catalog["reverse_dependencies"][view_name] = rows_to_dicts(cur)


def attach_rollback_baseline(cur, catalog: dict[str, Any]) -> None:
    cur.execute("SELECT count(*)::bigint, max(ruta_batch_id)::bigint FROM cg_core.ruta_rutero_load_batch")
    route_batch_count, max_ruta_batch_id = cur.fetchone()
    cur.execute("SELECT to_regclass('cg_core.ruta_rutero_week_assignment') IS NOT NULL")
    assignment_table_exists = bool(cur.fetchone()[0])
    assignment_row_count = 0
    if assignment_table_exists:
        cur.execute("SELECT count(*)::bigint FROM cg_core.ruta_rutero_week_assignment")
        assignment_row_count = int(cur.fetchone()[0])
    catalog["rollback_baseline"] = {
        "route_batch_count": int(route_batch_count or 0),
        "max_ruta_batch_id": int(max_ruta_batch_id) if max_ruta_batch_id is not None else None,
        "assignment_table_exists": assignment_table_exists,
        "assignment_row_count": assignment_row_count,
    }


def execute_static_sql(cur, sql_body: str) -> None:
    if "%(" in sql_body or "${" in sql_body:
        raise RunnerBlock("sql_template_forbidden")
    cur.execute(sql_body)


def current_rollback_counts(cur) -> dict[str, Any]:
    cur.execute("SELECT count(*)::bigint, max(ruta_batch_id)::bigint FROM cg_core.ruta_rutero_load_batch")
    route_batch_count, max_ruta_batch_id = cur.fetchone()
    cur.execute("SELECT to_regclass('cg_core.ruta_rutero_week_assignment') IS NOT NULL")
    assignment_table_exists = bool(cur.fetchone()[0])
    assignment_row_count = 0
    if assignment_table_exists:
        cur.execute("SELECT count(*)::bigint FROM cg_core.ruta_rutero_week_assignment")
        assignment_row_count = int(cur.fetchone()[0])
    return {
        "route_batch_count": int(route_batch_count or 0),
        "max_ruta_batch_id": int(max_ruta_batch_id) if max_ruta_batch_id is not None else None,
        "assignment_table_exists": assignment_table_exists,
        "assignment_row_count": assignment_row_count,
    }


def validate_apply_baseline(cur, prestate: dict[str, Any]) -> None:
    current = current_rollback_counts(cur)
    baseline = prestate["rollback_baseline"]
    if current.get("route_batch_count") != baseline.get("route_batch_count"):
        raise RunnerBlock("apply_route_batch_count_changed")
    if current.get("max_ruta_batch_id") != baseline.get("max_ruta_batch_id"):
        raise RunnerBlock("apply_max_ruta_batch_id_changed")
    if current.get("assignment_row_count") != baseline.get("assignment_row_count"):
        raise RunnerBlock("apply_assignment_row_count_changed")
    if current.get("assignment_table_exists") != baseline.get("assignment_table_exists"):
        raise RunnerBlock("apply_assignment_table_state_changed")


def validate_rollback_safety(
    cur,
    prestate: dict[str, Any],
    current_post_technical_fingerprint: str,
    expected_post_technical_fingerprint: str,
    expected_rollback_technical_fingerprint: str,
) -> None:
    if not expected_post_technical_fingerprint:
        raise RunnerBlock("expected_post_technical_fingerprint_required")
    if not expected_rollback_technical_fingerprint:
        raise RunnerBlock("expected_rollback_technical_fingerprint_required")
    if current_post_technical_fingerprint != expected_post_technical_fingerprint.upper():
        raise RunnerBlock("post_technical_fingerprint_mismatch")
    current = current_rollback_counts(cur)
    baseline = prestate["rollback_baseline"]
    if current["assignment_row_count"] > 0:
        raise RunnerBlock("rollback_assignment_rows_present")
    for key in ("route_batch_count", "max_ruta_batch_id"):
        if current.get(key) != baseline.get(key):
            raise RunnerBlock(f"rollback_batch_baseline_changed:{key}")


def base_plan_payload(catalog: dict[str, Any]) -> dict[str, Any]:
    return {
        "phase": PHASE,
        "prestate_file_sha256": catalog["_prestate_file_sha256"],
        "prestate_technical_fingerprint": catalog["technical_catalog_fingerprint_sha256"],
        "sql13_path": str(SQL13_PATH.relative_to(ROOT)),
        "sql13_sha256": file_sha256(SQL13_PATH),
        "sql14_path": str(SQL14_PATH.relative_to(ROOT)),
        "sql14_sha256": file_sha256(SQL14_PATH),
        "default_no_write": True,
        "free_sql_cli": False,
    }


def run_plan(args: argparse.Namespace) -> dict[str, Any]:
    catalog = load_prestate_catalog()
    validate_catalog_complete(catalog, prestate=True)
    validate_prestate_integrity(
        catalog,
        args.expected_prestate_file_sha256,
        args.expected_prestate_technical_fingerprint,
    )
    read_sql_artifact(SQL13_PATH, args.expected_sql13_sha256)
    read_sql_artifact(SQL14_PATH, args.expected_sql14_sha256)
    payload = base_plan_payload(catalog)
    payload["action"] = "plan"
    return payload


def run_inspect(args: argparse.Namespace) -> dict[str, Any]:
    conn = connect_db_from_env()
    try:
        begin_runner_transaction(conn, write=False)
        with conn.cursor() as cur:
            catalog = fetch_catalog(cur)
            validate_catalog_complete(catalog, prestate=not catalog["target_objects"]["cg_core.ruta_rutero_week_assignment"]["exists"])
        conn.rollback()
        return {
            "phase": PHASE,
            "action": "inspect",
            "technical_catalog_complete": True,
            "technical_catalog_fingerprint_sha256": catalog["technical_catalog_fingerprint_sha256"],
            "catalog": catalog,
            "writes_attempted": False,
        }
    finally:
        conn.close()


def run_postcheck(args: argparse.Namespace) -> dict[str, Any]:
    prestate = load_prestate_catalog()
    validate_prestate_integrity(
        prestate,
        args.expected_prestate_file_sha256,
        args.expected_prestate_technical_fingerprint,
    )
    conn = connect_db_from_env()
    try:
        begin_runner_transaction(conn, write=False)
        with conn.cursor() as cur:
            current = fetch_catalog(cur)
        conn.rollback()
        result = validate_post_catalog(current, prestate)
        return {"phase": PHASE, "action": "postcheck", **result, "writes_attempted": False}
    finally:
        conn.close()


def run_apply(args: argparse.Namespace) -> dict[str, Any]:
    telemetry = {
        "writes_attempted": False,
        "ddl_statements_executed": 0,
        "committed": False,
        "rolled_back": False,
        "postcheck_passed": False,
    }
    if not args.apply:
        raise RunnerBlock("apply_confirmation_required", telemetry=telemetry)
    prestate = load_prestate_catalog()
    validate_catalog_complete(prestate, prestate=True)
    validate_prestate_integrity(
        prestate,
        args.expected_prestate_file_sha256,
        args.expected_prestate_technical_fingerprint,
    )
    sql_text = read_sql_artifact(SQL13_PATH, args.expected_sql13_sha256)
    sql_body = extract_apply_body(sql_text)
    conn = connect_db_from_env()
    try:
        try:
            status = begin_runner_transaction(conn, write=True)
            validate_write_identity(status, args)
            with conn.cursor() as cur:
                validate_apply_baseline(cur, prestate)
                current = fetch_catalog(cur)
                if technical_fingerprint(current) != prestate["technical_catalog_fingerprint_sha256"]:
                    raise RunnerBlock("prestate_technical_fingerprint_mismatch")
                telemetry["writes_attempted"] = True
                telemetry["ddl_statements_executed"] = count_ddl_statements(sql_body)
                execute_static_sql(cur, sql_body)
                post_catalog = fetch_catalog(cur)
                validate_post_catalog(post_catalog, prestate)
                telemetry["postcheck_passed"] = True
            conn.commit()
            telemetry["committed"] = True
            return {"phase": PHASE, "action": "apply", **telemetry}
        except RunnerBlock as exc:
            conn.rollback()
            telemetry["rolled_back"] = True
            exc.telemetry = {**telemetry, **exc.telemetry}
            raise
        except Exception as exc:
            conn.rollback()
            telemetry["rolled_back"] = True
            raise RunnerBlock("apply_failed", redact_secret(exc), telemetry=telemetry) from exc
    finally:
        conn.close()


def run_rollback(args: argparse.Namespace) -> dict[str, Any]:
    telemetry = {
        "writes_attempted": False,
        "ddl_statements_executed": 0,
        "committed": False,
        "rolled_back": False,
        "postcheck_passed": False,
    }
    if not args.apply:
        raise RunnerBlock("rollback_confirmation_required", telemetry=telemetry)
    prestate = load_prestate_catalog()
    validate_catalog_complete(prestate, prestate=True)
    validate_prestate_integrity(
        prestate,
        args.expected_prestate_file_sha256,
        args.expected_prestate_technical_fingerprint,
    )
    if not args.expected_post_technical_fingerprint:
        raise RunnerBlock("expected_post_technical_fingerprint_required")
    if not args.expected_rollback_technical_fingerprint:
        raise RunnerBlock("expected_rollback_technical_fingerprint_required")
    sql_text = read_sql_artifact(SQL14_PATH, args.expected_sql14_sha256)
    sql_body = extract_apply_body(sql_text)
    conn = connect_db_from_env()
    try:
        try:
            status = begin_runner_transaction(conn, write=True)
            validate_write_identity(status, args)
            with conn.cursor() as cur:
                post_catalog = fetch_catalog(cur)
                post_fp = technical_fingerprint(post_catalog)
                validate_rollback_safety(
                    cur,
                    prestate,
                    post_fp,
                    args.expected_post_technical_fingerprint,
                    args.expected_rollback_technical_fingerprint,
                )
                telemetry["writes_attempted"] = True
                telemetry["ddl_statements_executed"] = count_ddl_statements(sql_body)
                execute_static_sql(cur, sql_body)
                restored = fetch_catalog(cur)
                restored_fp = technical_fingerprint(restored)
                if restored_fp != args.expected_rollback_technical_fingerprint.upper():
                    raise RunnerBlock("rollback_technical_fingerprint_mismatch")
                telemetry["postcheck_passed"] = True
            conn.commit()
            telemetry["committed"] = True
            return {"phase": PHASE, "action": "rollback", **telemetry}
        except RunnerBlock as exc:
            conn.rollback()
            telemetry["rolled_back"] = True
            exc.telemetry = {**telemetry, **exc.telemetry}
            raise
        except Exception as exc:
            conn.rollback()
            telemetry["rolled_back"] = True
            raise RunnerBlock("rollback_failed", redact_secret(exc), telemetry=telemetry) from exc
    finally:
        conn.close()


def run_week_baseline(args: argparse.Namespace) -> dict[str, Any]:
    conn = connect_db_from_env()
    try:
        begin_runner_transaction(conn, write=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT effective_week_start,
                       count(*)::bigint AS rows,
                       count(DISTINCT ruta_batch_id)::bigint AS batches
                  FROM cg_core.v_rr_frecuencia_base_resuelta_v2
                 GROUP BY effective_week_start
                 ORDER BY effective_week_start DESC
                 LIMIT 12
                """
            )
            rows = rows_to_dicts(cur)
        conn.rollback()
        return {"phase": PHASE, "action": "week-baseline", "rows": rows, "writes_attempted": False}
    finally:
        conn.close()


def safe_json_out_path(path_text: str | None, output_root: str | None) -> Path | None:
    if not path_text:
        return None
    path = Path(path_text)
    if path.is_symlink():
        raise RunnerBlock("json_out_symlink_blocked")
    resolved = path.resolve(strict=False)
    temp_root = Path(tempfile.gettempdir()).resolve(strict=True)
    allowed_roots = [temp_root]
    if output_root:
        root = Path(output_root).resolve(strict=True)
        if root.is_symlink():
            raise RunnerBlock("json_output_root_symlink_blocked")
        worktree_root = ROOT.resolve(strict=True)
        if worktree_root != root and worktree_root not in root.parents:
            raise RunnerBlock("json_output_root_escape_blocked")
        allowed_roots.append(root)
    if not any(root == resolved or root in resolved.parents for root in allowed_roots):
        raise RunnerBlock("json_out_path_escape_blocked")
    parent = resolved.parent
    if parent.exists() and parent.is_symlink():
        raise RunnerBlock("json_out_symlink_blocked")
    return resolved


def emit_json(payload: dict[str, Any], json_out: str | None, output_root: str | None) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True, default=json_default)
    out_path = safe_json_out_path(json_out, output_root)
    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text + "\n", encoding="utf-8")
    print(text)


def add_integrity_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--expected-prestate-file-sha256", required=True)
    parser.add_argument("--expected-prestate-technical-fingerprint", required=True)


def add_write_identity_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--expected-current-user", required=True)
    parser.add_argument("--expected-database", required=True)
    parser.add_argument("--expected-environment-fingerprint", required=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CG005N fixed-artifact DDL runner")
    parser.add_argument("--json-out", default="")
    parser.add_argument("--output-root", default="")
    sub = parser.add_subparsers(dest="command", required=True)

    inspect_parser = sub.add_parser("inspect")
    inspect_parser.set_defaults(func=run_inspect)

    plan_parser = sub.add_parser("plan")
    plan_parser.add_argument("--expected-sql13-sha256", required=True)
    plan_parser.add_argument("--expected-sql14-sha256", required=True)
    add_integrity_args(plan_parser)
    plan_parser.set_defaults(func=run_plan)

    apply_parser = sub.add_parser("apply")
    apply_parser.add_argument("--apply", action="store_true")
    apply_parser.add_argument("--expected-sql13-sha256", required=True)
    add_integrity_args(apply_parser)
    add_write_identity_args(apply_parser)
    apply_parser.set_defaults(func=run_apply)

    rollback_parser = sub.add_parser("rollback")
    rollback_parser.add_argument("--apply", action="store_true")
    rollback_parser.add_argument("--expected-sql14-sha256", required=True)
    rollback_parser.add_argument("--expected-post-technical-fingerprint", required=True)
    rollback_parser.add_argument("--expected-rollback-technical-fingerprint", required=True)
    add_integrity_args(rollback_parser)
    add_write_identity_args(rollback_parser)
    rollback_parser.set_defaults(func=run_rollback)

    postcheck_parser = sub.add_parser("postcheck")
    add_integrity_args(postcheck_parser)
    postcheck_parser.set_defaults(func=run_postcheck)

    week_parser = sub.add_parser("week-baseline")
    week_parser.set_defaults(func=run_week_baseline)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        payload = args.func(args)
        emit_json(payload, args.json_out, args.output_root)
        return 0
    except RunnerBlock as exc:
        payload = {
            "phase": PHASE,
            "action": getattr(args, "command", "unknown"),
            "status": "BLOCK",
            "blocker": exc.code,
            "detail": redact_secret(exc),
            "writes_attempted": False,
            "ddl_statements_executed": 0,
            "committed": False,
            "rolled_back": False,
            "postcheck_passed": False,
            **exc.telemetry,
        }
        emit_json(payload, getattr(args, "json_out", ""), getattr(args, "output_root", ""))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
