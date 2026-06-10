#!/usr/bin/env python
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.codex_ro_env_check import resolve_db_url


EVIDENCE_ROOT = ROOT / "evidence"
PHASE_ROOT = EVIDENCE_ROOT / "supabase_cleanup_9C7A4"
REQUIRED_SUBDIRS = ("ddl", "baseline", "reports", "logs")
EXPECTED_ROLE = "stock_zero_codex_ro"
ALLOWED_SCHEMAS = ("cg_raw", "cg_core", "cg_mart", "cg_audit", "public", "pg_catalog", "information_schema")
DDL_SCHEMAS = ("cg_raw", "cg_core", "cg_mart", "cg_audit", "public")
MUTATION_TOKEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|TRUNCATE|CREATE|ALTER|DROP|GRANT|REVOKE|REFRESH|VACUUM|ANALYZE|CALL|DO)\b"
    r"|\bCOPY\b\s+[^;]*\bTO\b\s+\bPROGRAM\b",
    re.IGNORECASE | re.DOTALL,
)
POSTGRES_URL_RE = re.compile(r"postgres(?:ql)?://[^\s'\"<>]+", re.IGNORECASE)
PASSWORD_RE = re.compile(r"(?i)(password\s*=\s*)[^\s;]+")
HOST_RE = re.compile(r"(?i)(host\s*=\s*)[^\s;]+")
PORT_RE = re.compile(r"(?i)(port\s*=\s*)[^\s;]+")


ALLOWLISTED_SUBCOMMANDS = (
    "role-audit",
    "catalog",
    "dependencies",
    "ddl",
    "baseline-daily",
    "baseline-weekly",
    "baseline-audit",
    "all",
)

TARGET_OBJECTS = (
    "cg_core.v_cg_visita_dia_precedencia_v2",
    "cg_core.v_cg_visita_dia_resuelta_v2",
    "cg_core.v_rr_frecuencia_base_resuelta_v2",
    "cg_core.v_local_context_latest",
    "cg_mart.fact_cg_visita_dia_resuelta_v2",
    "cg_mart.fact_cg_out_weekly_v2",
    "cg_mart.v_cg_marcaje_multifuente_dia_v2",
    "cg_mart.v_cg_fuera_cruce_real_v2",
    "cg_mart.v_cg_sin_batch_ruta_semana_v2",
    "cg_mart.v_cg_ruta_duplicados_auditoria_v2",
)

BASELINE_CANDIDATES = {
    "daily": (
        "cg_mart.fact_cg_visita_dia_resuelta_v2",
        "cg_mart.mv_cg_visita_dia_resuelta_v2",
        "cg_core.v_cg_visita_dia_resuelta_v2",
    ),
    "weekly": (
        "cg_mart.fact_cg_out_weekly_v2",
        "cg_mart.mv_cg_out_weekly_v2",
        "public.v_cg_cumplimiento_semana_local_parity",
    ),
    "audit_multimarcaje": ("cg_mart.v_cg_marcaje_multifuente_dia_v2",),
    "audit_fuera_cruce": ("cg_mart.v_cg_fuera_cruce_real_v2",),
    "audit_sin_batch_ruta": ("cg_mart.v_cg_sin_batch_ruta_semana_v2",),
    "audit_ruta_duplicados": ("cg_mart.v_cg_ruta_duplicados_auditoria_v2",),
}

DAILY_SPLIT_HINTS = ("fecha", "dia", "date", "created_at", "ingested_at")
WEEKLY_SPLIT_HINTS = ("semana", "week", "week_start", "periodo")


class ExtractorBlock(RuntimeError):
    pass


@dataclass(frozen=True)
class RoleStatus:
    current_user: str
    session_user: str
    transaction_read_only: str
    default_transaction_read_only: str


def redact_secret(value: object) -> str:
    text = str(value)
    text = POSTGRES_URL_RE.sub("postgresql://<redacted>", text)
    text = PASSWORD_RE.sub(r"\1<redacted>", text)
    text = HOST_RE.sub(r"\1<redacted>", text)
    text = PORT_RE.sub(r"\1<redacted>", text)
    return text


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=json_default) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def aggregate_sha256(rows: Sequence[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, sort_keys=True, ensure_ascii=False, default=json_default).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def validate_static_sql(name: str, sql: str) -> None:
    match = MUTATION_TOKEN_RE.search(sql)
    if match:
        raise ExtractorBlock(f"SQL_TEMPLATE_MUTATION_TOKEN_BLOCKED:{name}:{match.group(0).upper()}")


def execute_static(cur: Any, name: str, sql: str, params: Sequence[Any] | None = None) -> None:
    validate_static_sql(name, sql)
    cur.execute(sql, params or ())


def require_role_status(status: RoleStatus) -> None:
    if status.current_user != EXPECTED_ROLE:
        raise ExtractorBlock("ROLE_MISMATCH_BLOCK")
    if str(status.transaction_read_only).lower() != "on":
        raise ExtractorBlock("TRANSACTION_NOT_READ_ONLY_BLOCK")


def split_qualified_name(name: str) -> tuple[str, str]:
    parts = name.split(".")
    if len(parts) != 2 or not all(parts):
        raise ExtractorBlock("INVALID_QUALIFIED_NAME")
    schema, relation = parts
    if schema not in ALLOWED_SCHEMAS:
        raise ExtractorBlock("SCHEMA_NOT_ALLOWLISTED")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation):
        raise ExtractorBlock("UNSAFE_IDENTIFIER")
    return schema, relation


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def quote_qualified(name: str) -> str:
    schema, relation = split_qualified_name(name)
    return f"{quote_ident(schema)}.{quote_ident(relation)}"


def default_output_root() -> str:
    return str(PHASE_ROOT.relative_to(ROOT))


def ensure_output_root(output_root: str | Path) -> Path:
    candidate = Path(output_root)
    if any(part == ".." for part in candidate.parts):
        raise ExtractorBlock("OUTPUT_ROOT_PATH_TRAVERSAL_BLOCK")
    if not candidate.is_absolute():
        candidate = ROOT / candidate
    resolved = candidate.resolve(strict=False)
    allowed = EVIDENCE_ROOT.resolve(strict=False)
    if resolved != allowed and allowed not in resolved.parents:
        raise ExtractorBlock("OUTPUT_ROOT_OUTSIDE_EVIDENCE_BLOCK")
    for subdir in REQUIRED_SUBDIRS:
        (resolved / subdir).mkdir(parents=True, exist_ok=True)
    return resolved


def import_psycopg2():
    try:
        import psycopg2  # type: ignore

        return psycopg2
    except ModuleNotFoundError:
        venv_python = ROOT / ".venv" / "Scripts" / "python.exe"
        if venv_python.exists() and Path(sys.executable).resolve() != venv_python.resolve():
            os.execv(str(venv_python), [str(venv_python), str(Path(__file__).resolve()), *sys.argv[1:]])
        raise


def connect_readonly():
    db_url, source = resolve_db_url()
    if not db_url:
        raise ExtractorBlock("DB_URL_CODEX_RO_NOT_AVAILABLE")
    psycopg2 = import_psycopg2()
    try:
        conn = psycopg2.connect(db_url, connect_timeout=10)
    except Exception as exc:  # pragma: no cover - exercised only with live DB failures
        raise ExtractorBlock(redact_secret(exc)) from exc
    conn.autocommit = True
    return conn, source


def begin_guarded_readonly(conn: Any) -> RoleStatus:
    with conn.cursor() as cur:
        execute_static(
            cur,
            "role_check_initial",
            """
            SELECT
                current_user::text,
                session_user::text,
                current_setting('transaction_read_only')::text,
                current_setting('default_transaction_read_only')::text
            """,
        )
        current_user, session_user, tx_readonly, default_readonly = cur.fetchone()
        if current_user != EXPECTED_ROLE:
            raise ExtractorBlock("ROLE_MISMATCH_BLOCK")

        execute_static(cur, "begin_read_only", "BEGIN READ ONLY")
        execute_static(cur, "statement_timeout", "SET LOCAL statement_timeout = '300s'")
        execute_static(cur, "lock_timeout", "SET LOCAL lock_timeout = '5s'")
        execute_static(cur, "idle_timeout", "SET LOCAL idle_in_transaction_session_timeout = '60s'")
        execute_static(cur, "readonly_check", "SELECT current_setting('transaction_read_only')::text")
        tx_readonly = cur.fetchone()[0]
        status = RoleStatus(
            current_user=str(current_user),
            session_user=str(session_user),
            transaction_read_only=str(tx_readonly),
            default_transaction_read_only=str(default_readonly),
        )
        require_role_status(status)
        return status


def fetch_all_dicts(cur: Any) -> list[dict[str, Any]]:
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def relation_exists(cur: Any, qualified_name: str) -> bool:
    split_qualified_name(qualified_name)
    execute_static(cur, "relation_exists", "SELECT to_regclass(%s)::text", (qualified_name,))
    return cur.fetchone()[0] is not None


def resolve_baseline_relation(cur: Any, alias: str) -> str:
    candidates = BASELINE_CANDIDATES[alias]
    for candidate in candidates:
        if relation_exists(cur, candidate):
            return candidate
    raise ExtractorBlock(f"BASELINE_RELATION_NOT_FOUND:{alias}")


def get_columns(cur: Any, qualified_name: str) -> list[dict[str, Any]]:
    schema, relation = split_qualified_name(qualified_name)
    execute_static(
        cur,
        "columns_for_relation",
        """
        SELECT
            column_name::text,
            data_type::text,
            udt_name::text,
            ordinal_position::int
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, relation),
    )
    return fetch_all_dicts(cur)


def choose_split_column(columns: Sequence[dict[str, Any]], mode: str) -> str | None:
    hints = DAILY_SPLIT_HINTS if mode == "daily" else WEEKLY_SPLIT_HINTS
    lower_names = {str(column["column_name"]).lower(): str(column["column_name"]) for column in columns}
    for hint in hints:
        for lower_name, original in lower_names.items():
            if hint == lower_name or hint in lower_name:
                return original
    for column in columns:
        data_type = str(column["data_type"]).lower()
        if "date" in data_type or "timestamp" in data_type:
            return str(column["column_name"])
    return None


def rows_to_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    import pandas as pd

    frame = pd.DataFrame(rows)
    frame.to_parquet(path, index=False)


def export_query_to_parquet(
    cur: Any,
    name: str,
    sql: str,
    params: Sequence[Any],
    path: Path,
    out_root: Path,
) -> dict[str, Any]:
    execute_static(cur, name, sql, params)
    rows = fetch_all_dicts(cur)
    rows_to_parquet(rows, path)
    file_hash = sha256_file(path)
    return {
        "path": str(path.relative_to(out_root)),
        "rows": len(rows),
        "columns": list(rows[0].keys()) if rows else [desc[0] for desc in cur.description],
        "aggregate_sha256": aggregate_sha256(rows),
        "file_sha256": file_hash,
    }


def export_baseline(conn: Any, out_root: Path, alias: str, mode: str, sample_limit: int | None, max_buckets: int | None) -> dict[str, Any]:
    with conn.cursor() as cur:
        relation = resolve_baseline_relation(cur, alias)
        columns = get_columns(cur, relation)
        split_col = choose_split_column(columns, mode)
        qualified = quote_qualified(relation)
        exports: list[dict[str, Any]] = []

        if sample_limit is not None:
            limit = max(1, min(int(sample_limit), 1000))
            order_sql = f" ORDER BY {quote_ident(split_col)} NULLS LAST" if split_col else ""
            sql = f"SELECT * FROM {qualified}{order_sql} LIMIT %s"
            path = out_root / "baseline" / f"{alias}_sample.parquet"
            exports.append(export_query_to_parquet(cur, f"{alias}_sample", sql, (limit,), path, out_root))
        elif split_col:
            bucket_sql = f"SELECT DISTINCT {quote_ident(split_col)} AS bucket FROM {qualified} WHERE {quote_ident(split_col)} IS NOT NULL ORDER BY 1"
            execute_static(cur, f"{alias}_buckets", bucket_sql)
            buckets = [row[0] for row in cur.fetchall()]
            if max_buckets is not None:
                buckets = buckets[: max(1, int(max_buckets))]
            for index, bucket in enumerate(buckets, start=1):
                path = out_root / "baseline" / f"{alias}_bucket_{index:04d}.parquet"
                sql = f"SELECT * FROM {qualified} WHERE {quote_ident(split_col)} = %s ORDER BY {quote_ident(split_col)}"
                exports.append(export_query_to_parquet(cur, f"{alias}_bucket", sql, (bucket,), path, out_root))
        else:
            sql = f"SELECT * FROM {qualified}"
            path = out_root / "baseline" / f"{alias}.parquet"
            exports.append(export_query_to_parquet(cur, alias, sql, (), path, out_root))

        manifest = {
            "alias": alias,
            "relation": relation,
            "mode": mode,
            "split_column": split_col,
            "sample_limit": sample_limit,
            "exports": exports,
            "total_rows": sum(item["rows"] for item in exports),
        }
        write_json(out_root / "baseline" / f"{alias}_manifest.json", manifest)
        return manifest


def role_audit(conn: Any, out_root: Path, status: RoleStatus) -> dict[str, Any]:
    with conn.cursor() as cur:
        execute_static(
            cur,
            "role_attrs",
            """
            SELECT
                rolname::text,
                rolsuper::boolean,
                rolcreatedb::boolean,
                rolcreaterole::boolean,
                rolreplication::boolean,
                rolbypassrls::boolean
            FROM pg_catalog.pg_roles
            WHERE rolname = current_user
            """,
        )
        role_attrs = fetch_all_dicts(cur)
        execute_static(
            cur,
            "role_table_privileges",
            """
            SELECT
                table_schema::text,
                table_name::text,
                privilege_type::text
            FROM information_schema.role_table_grants
            WHERE grantee = current_user
              AND table_schema = ANY(%s)
            ORDER BY table_schema, table_name, privilege_type
            """,
            (list(ALLOWED_SCHEMAS),),
        )
        privileges = fetch_all_dicts(cur)
    write_privileges = [row for row in privileges if row["privilege_type"] not in ("SELECT", "REFERENCES", "TRIGGER")]
    payload = {
        "current_user": status.current_user,
        "session_user": status.session_user,
        "transaction_read_only": status.transaction_read_only,
        "default_transaction_read_only": status.default_transaction_read_only,
        "role_attrs": role_attrs,
        "unexpected_write_privileges": write_privileges,
        "select_privileges_count": sum(1 for row in privileges if row["privilege_type"] == "SELECT"),
        "dsn_printed": False,
        "writes_attempted": False,
        "verdict": "OK" if not write_privileges else "WARN",
    }
    write_json(out_root / "reports" / "role_audit.json", payload)
    return payload


def catalog(conn: Any, out_root: Path) -> dict[str, Any]:
    with conn.cursor() as cur:
        queries = {
            "server": """
                SELECT
                    current_setting('server_version')::text AS server_version,
                    current_setting('server_version_num')::text AS server_version_num
            """,
            "extensions": """
                SELECT e.extname::text, e.extversion::text, n.nspname::text AS schema
                FROM pg_catalog.pg_extension e
                JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
                ORDER BY e.extname
            """,
            "schemas": """
                SELECT schema_name::text
                FROM information_schema.schemata
                WHERE schema_name = ANY(%s)
                ORDER BY schema_name
            """,
            "relations": """
                SELECT
                    n.nspname::text AS schema,
                    c.relname::text AS name,
                    c.relkind::text AS kind,
                    c.reltuples::bigint AS estimated_rows,
                    pg_catalog.pg_total_relation_size(c.oid)::bigint AS total_bytes
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY(%s)
                  AND c.relkind IN ('r','p','v','m','f')
                ORDER BY n.nspname, c.relname
            """,
            "columns": """
                SELECT
                    table_schema::text,
                    table_name::text,
                    ordinal_position::int,
                    column_name::text,
                    data_type::text,
                    udt_name::text,
                    is_nullable::text
                FROM information_schema.columns
                WHERE table_schema = ANY(%s)
                ORDER BY table_schema, table_name, ordinal_position
            """,
            "constraints": """
                SELECT
                    n.nspname::text AS schema,
                    c.relname::text AS relation,
                    con.conname::text AS constraint_name,
                    con.contype::text AS constraint_type,
                    pg_catalog.pg_get_constraintdef(con.oid, true)::text AS definition
                FROM pg_catalog.pg_constraint con
                JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY(%s)
                ORDER BY n.nspname, c.relname, con.conname
            """,
            "indexes": """
                SELECT
                    n.nspname::text AS schema,
                    c.relname::text AS relation,
                    i.relname::text AS index_name,
                    pg_catalog.pg_get_indexdef(i.oid)::text AS definition
                FROM pg_catalog.pg_index ix
                JOIN pg_catalog.pg_class c ON c.oid = ix.indrelid
                JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY(%s)
                ORDER BY n.nspname, c.relname, i.relname
            """,
        }
        payload: dict[str, Any] = {}
        for name, sql in queries.items():
            params: Sequence[Any] = (list(ALLOWED_SCHEMAS),) if name not in ("server", "extensions") else ()
            execute_static(cur, f"catalog_{name}", sql, params)
            rows = fetch_all_dicts(cur)
            payload[name] = rows[0] if name == "server" and rows else rows
    write_json(out_root / "reports" / "catalog.json", payload)
    return payload


def dependencies(conn: Any, out_root: Path) -> dict[str, Any]:
    with conn.cursor() as cur:
        execute_static(
            cur,
            "dependency_edges",
            """
            SELECT DISTINCT
                src_ns.nspname::text AS source_schema,
                src.relname::text AS source_name,
                src.relkind::text AS source_kind,
                COALESCE(dep_ns.nspname::text, ext.extname::text, proc_ns.nspname::text) AS dependency_schema,
                COALESCE(dep.relname::text, ext.extname::text, proc.proname::text) AS dependency_name,
                CASE
                    WHEN dep.oid IS NOT NULL THEN dep.relkind::text
                    WHEN ext.oid IS NOT NULL THEN 'extension'
                    WHEN proc.oid IS NOT NULL THEN 'function'
                    ELSE 'unknown'
                END AS dependency_kind
            FROM pg_catalog.pg_rewrite rw
            JOIN pg_catalog.pg_class src ON src.oid = rw.ev_class
            JOIN pg_catalog.pg_namespace src_ns ON src_ns.oid = src.relnamespace
            JOIN pg_catalog.pg_depend d ON d.objid = rw.oid
            LEFT JOIN pg_catalog.pg_class dep ON dep.oid = d.refobjid
            LEFT JOIN pg_catalog.pg_namespace dep_ns ON dep_ns.oid = dep.relnamespace
            LEFT JOIN pg_catalog.pg_extension ext ON ext.oid = d.refobjid
            LEFT JOIN pg_catalog.pg_proc proc ON proc.oid = d.refobjid
            LEFT JOIN pg_catalog.pg_namespace proc_ns ON proc_ns.oid = proc.pronamespace
            WHERE src_ns.nspname = ANY(%s)
              AND (dep_ns.nspname = ANY(%s) OR proc_ns.nspname = ANY(%s) OR ext.oid IS NOT NULL)
              AND COALESCE(dep.oid, 0) <> src.oid
            ORDER BY source_schema, source_name, dependency_schema, dependency_name
            """,
            (list(ALLOWED_SCHEMAS), list(ALLOWED_SCHEMAS), list(ALLOWED_SCHEMAS)),
        )
        edges = fetch_all_dicts(cur)
        execute_static(
            cur,
            "target_presence",
            """
            SELECT
                requested.object_name::text,
                pg_catalog.to_regclass(requested.object_name)::text AS resolved_name
            FROM unnest(%s::text[]) AS requested(object_name)
            ORDER BY requested.object_name
            """,
            (list(TARGET_OBJECTS),),
        )
        targets = fetch_all_dicts(cur)
    present = {row["resolved_name"] for row in targets if row["resolved_name"]}
    required = sorted(present | {f"{row['dependency_schema']}.{row['dependency_name']}" for row in edges if row["dependency_schema"] and row["dependency_name"]})
    unknown = [row for row in edges if row["dependency_kind"] == "unknown"]
    payload = {
        "targets": targets,
        "edges": edges,
        "required_objects": required,
        "unknown_objects": unknown,
        "verdict": "OK" if not unknown else "BLOCK",
    }
    write_json(out_root / "reports" / "dependency_graph.json", payload)
    return payload


def ddl(conn: Any, out_root: Path) -> dict[str, Any]:
    with conn.cursor() as cur:
        execute_static(
            cur,
            "view_definitions",
            """
            SELECT
                n.nspname::text AS schema,
                c.relname::text AS name,
                c.relkind::text AS kind,
                pg_catalog.pg_get_viewdef(c.oid, true)::text AS definition
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s)
              AND c.relkind IN ('v','m')
            ORDER BY n.nspname, c.relname
            """,
            (list(DDL_SCHEMAS),),
        )
        views = fetch_all_dicts(cur)
        execute_static(
            cur,
            "function_definitions",
            """
            SELECT
                n.nspname::text AS schema,
                p.proname::text AS name,
                pg_catalog.pg_get_function_identity_arguments(p.oid)::text AS args,
                pg_catalog.pg_get_functiondef(p.oid)::text AS definition
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = ANY(%s)
              AND p.prokind IN ('f','p','w')
            ORDER BY n.nspname, p.proname, args
            """,
            (list(DDL_SCHEMAS),),
        )
        functions = fetch_all_dicts(cur)
        execute_static(
            cur,
            "constraint_index_definitions",
            """
            SELECT 'constraint'::text AS object_type, n.nspname::text AS schema, c.relname::text AS relation,
                   con.conname::text AS name, pg_catalog.pg_get_constraintdef(con.oid, true)::text AS definition
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s)
            UNION ALL
            SELECT 'index'::text AS object_type, n.nspname::text AS schema, c.relname::text AS relation,
                   i.relname::text AS name, pg_catalog.pg_get_indexdef(i.oid)::text AS definition
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class c ON c.oid = ix.indrelid
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s)
            ORDER BY schema, relation, name
            """,
            (list(DDL_SCHEMAS), list(DDL_SCHEMAS)),
        )
        constraints_indexes = fetch_all_dicts(cur)

    views_sql = "\n\n".join(f"-- {row['schema']}.{row['name']}\n{row['definition']};" for row in views)
    functions_sql = "\n\n".join(str(row["definition"]).rstrip() + ";" for row in functions)
    ci_sql = "\n\n".join(f"-- {row['object_type']} {row['schema']}.{row['relation']}.{row['name']}\n{row['definition']};" for row in constraints_indexes)
    (out_root / "ddl" / "03_views.sql").write_text(views_sql + "\n", encoding="utf-8")
    (out_root / "ddl" / "04_functions.sql").write_text(functions_sql + "\n", encoding="utf-8")
    (out_root / "ddl" / "05_constraints_indexes.sql").write_text(ci_sql + "\n", encoding="utf-8")
    payload = {
        "views": len(views),
        "functions": len(functions),
        "constraints_indexes": len(constraints_indexes),
        "files": [
            "ddl/03_views.sql",
            "ddl/04_functions.sql",
            "ddl/05_constraints_indexes.sql",
        ],
    }
    write_json(out_root / "reports" / "ddl_manifest.json", payload)
    return payload


def baseline_audit(conn: Any, out_root: Path, sample_limit: int | None, max_buckets: int | None) -> dict[str, Any]:
    payload = {}
    for alias in ("audit_multimarcaje", "audit_fuera_cruce", "audit_sin_batch_ruta", "audit_ruta_duplicados"):
        payload[alias] = export_baseline(conn, out_root, alias, "daily", sample_limit, max_buckets)
    write_json(out_root / "baseline" / "audit_manifest.json", payload)
    return payload


def run_command(command: str, out_root: Path, sample_limit: int | None, max_buckets: int | None) -> dict[str, Any]:
    conn, source = connect_readonly()
    try:
        status = begin_guarded_readonly(conn)
        result: dict[str, Any] = {
            "command": command,
            "source_used": source,
            "role": status.current_user,
            "transaction_read_only": status.transaction_read_only,
            "writes_attempted": False,
            "dsn_printed": False,
        }
        if command == "role-audit":
            result["role_audit"] = role_audit(conn, out_root, status)
        elif command == "catalog":
            result["catalog"] = catalog(conn, out_root)
        elif command == "dependencies":
            result["dependencies"] = dependencies(conn, out_root)
        elif command == "ddl":
            result["ddl"] = ddl(conn, out_root)
        elif command == "baseline-daily":
            result["baseline_daily"] = export_baseline(conn, out_root, "daily", "daily", sample_limit, max_buckets)
        elif command == "baseline-weekly":
            result["baseline_weekly"] = export_baseline(conn, out_root, "weekly", "weekly", sample_limit, max_buckets)
        elif command == "baseline-audit":
            result["baseline_audit"] = baseline_audit(conn, out_root, sample_limit, max_buckets)
        elif command == "all":
            result["role_audit"] = role_audit(conn, out_root, status)
            result["catalog"] = catalog(conn, out_root)
            result["dependencies"] = dependencies(conn, out_root)
            result["ddl"] = ddl(conn, out_root)
            result["baseline_daily"] = export_baseline(conn, out_root, "daily", "daily", sample_limit, max_buckets)
            result["baseline_weekly"] = export_baseline(conn, out_root, "weekly", "weekly", sample_limit, max_buckets)
            result["baseline_audit"] = baseline_audit(conn, out_root, sample_limit, max_buckets)
        else:
            raise ExtractorBlock("UNKNOWN_SUBCOMMAND_BLOCK")
        result["verdict"] = "OK"
        return result
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Control Gestion repo-managed read-only extractor")
    parser.add_argument(
        "--output-root",
        "--out-dir",
        dest="output_root",
        default=default_output_root(),
        help="Output directory under evidence/; defaults to evidence/supabase_cleanup_9C7A4",
    )
    parser.add_argument("--sample-limit", type=int, default=None, help="Optional bounded sample size for baseline smoke exports")
    parser.add_argument("--max-buckets", type=int, default=None, help="Optional bounded number of date/week buckets for baseline exports")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ALLOWLISTED_SUBCOMMANDS:
        subparsers.add_parser(command)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        out_root = ensure_output_root(args.output_root)
        result = run_command(args.command, out_root, args.sample_limit, args.max_buckets)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))
        return 0
    except ExtractorBlock as exc:
        print(json.dumps({"verdict": "BLOCK", "error": redact_secret(exc), "dsn_printed": False}, ensure_ascii=False, indent=2))
        return 2
    except Exception as exc:  # pragma: no cover - defensive final redaction
        print(json.dumps({"verdict": "BLOCK", "error": redact_secret(exc), "dsn_printed": False}, ensure_ascii=False, indent=2))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
