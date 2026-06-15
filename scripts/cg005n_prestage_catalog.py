#!/usr/bin/env python
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.cg_readonly_extract import (  # noqa: E402
    begin_guarded_readonly,
    connect_readonly,
    execute_static,
    fetch_all_dicts,
    json_default,
)
from scripts import cg005n_catalog_contract as catalog_contract  # noqa: E402


PHASE = "CG005NQ_PACKAGE_CORRECTION_NO_SUPABASE_WRITE"
TARGET_OBJECTS = catalog_contract.TARGET_OBJECTS
VIEW_TARGETS = catalog_contract.VIEW_TARGETS
TARGET_WEEK_START = "2026-06-08"


class CatalogBlock(RuntimeError):
    pass


def canonical_json(value: Any) -> str:
    return catalog_contract.canonical_json(value)


def sha256_canonical(value: Any) -> str:
    return catalog_contract.sha256_canonical(value)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest().upper()


def technical_catalog_payload(catalog: dict[str, Any]) -> dict[str, Any]:
    return catalog_contract.technical_payload(catalog)


def attach_separate_hashes(catalog: dict[str, Any]) -> None:
    catalog["technical_catalog_fingerprint_sha256"] = catalog_contract.technical_fingerprint(catalog)
    catalog["rollback_baseline_sha256"] = catalog_contract.rollback_baseline_fingerprint(catalog)


def split_name(qualified_name: str) -> tuple[str, str]:
    schema, name = qualified_name.split(".", 1)
    return schema, name


def rows_by_object(rows: Sequence[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        object_name = f"{row.pop('schema')}.{row.pop('relation')}"
        grouped.setdefault(object_name, []).append(row)
    return grouped


def fetch_relation_objects(cur: Any) -> dict[str, dict[str, Any]]:
    execute_static(
        cur,
        "cg005n_target_relations",
        """
        SELECT
            wanted.qualified_name::text,
            n.nspname::text AS schema,
            c.relname::text AS relation,
            c.relkind::text AS relkind,
            pg_catalog.pg_get_userbyid(c.relowner)::text AS owner,
            pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment,
            c.reloptions::text[] AS reloptions,
            c.relacl::text[] AS relacl
        FROM unnest(%s::text[]) AS wanted(qualified_name)
        LEFT JOIN pg_catalog.pg_class c
          ON c.oid = to_regclass(wanted.qualified_name)
        LEFT JOIN pg_catalog.pg_namespace n
          ON n.oid = c.relnamespace
        ORDER BY wanted.qualified_name
        """,
        (list(TARGET_OBJECTS),),
    )
    objects: dict[str, dict[str, Any]] = {}
    for row in fetch_all_dicts(cur):
        qualified_name = row.pop("qualified_name")
        exists = row["schema"] is not None
        objects[qualified_name] = {
            "exists": exists,
            "schema": row["schema"],
            "name": row["relation"],
            "relkind": row["relkind"],
            "owner": row["owner"],
            "comment": row["comment"],
            "comment_captured": exists,
            "description_present": row["comment"] is not None,
            "reloptions": row["reloptions"] if row["reloptions"] is not None else [],
            "relacl": row["relacl"],
            "columns": [],
            "constraints": [],
            "indexes": [],
            "grants": [],
            "acl": [],
            "view_definition_sha256": None,
            "view_definition": None,
            "view_options": None,
            "column_signature_sha256": None,
        }
    return objects


def attach_columns(cur: Any, objects: dict[str, dict[str, Any]]) -> None:
    execute_static(
        cur,
        "cg005n_columns",
        """
        SELECT
            table_schema::text AS schema,
            table_name::text AS relation,
            ordinal_position::int,
            column_name::text,
            data_type::text,
            udt_name::text,
            is_nullable::text,
            column_default::text
        FROM information_schema.columns
        WHERE table_schema || '.' || table_name = ANY(%s)
        ORDER BY table_schema, table_name, ordinal_position
        """,
        (list(TARGET_OBJECTS),),
    )
    for object_name, rows in rows_by_object(fetch_all_dicts(cur)).items():
        for row in rows:
            row["column_default_present"] = row.pop("column_default") is not None
        objects[object_name]["columns"] = rows
        objects[object_name]["column_signature_sha256"] = sha256_canonical(rows)


def attach_constraints(cur: Any, objects: dict[str, dict[str, Any]]) -> None:
    execute_static(
        cur,
        "cg005n_constraints",
        """
        SELECT
            n.nspname::text AS schema,
            c.relname::text AS relation,
            con.conname::text AS constraint_name,
            con.contype::text AS constraint_type,
            pg_catalog.pg_get_constraintdef(con.oid, true)::text AS definition
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname || '.' || c.relname = ANY(%s)
        ORDER BY n.nspname, c.relname, con.conname
        """,
        (list(TARGET_OBJECTS),),
    )
    for object_name, rows in rows_by_object(fetch_all_dicts(cur)).items():
        objects[object_name]["constraints"] = rows


def attach_indexes(cur: Any, objects: dict[str, dict[str, Any]]) -> None:
    execute_static(
        cur,
        "cg005n_indexes",
        """
        SELECT
            n.nspname::text AS schema,
            c.relname::text AS relation,
            i.relname::text AS index_name,
            pg_catalog.pg_get_indexdef(i.oid)::text AS definition
        FROM pg_catalog.pg_index ix
        JOIN pg_catalog.pg_class c ON c.oid = ix.indrelid
        JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname || '.' || c.relname = ANY(%s)
        ORDER BY n.nspname, c.relname, i.relname
        """,
        (list(TARGET_OBJECTS),),
    )
    for object_name, rows in rows_by_object(fetch_all_dicts(cur)).items():
        objects[object_name]["indexes"] = rows


def attach_acl(cur: Any, objects: dict[str, dict[str, Any]]) -> None:
    execute_static(
        cur,
        "cg005n_acl",
        """
        SELECT
            n.nspname::text AS schema,
            c.relname::text AS relation,
            (acl).grantor::regrole::text AS grantor,
            (acl).grantee::regrole::text AS grantee,
            (acl).privilege_type::text AS privilege_type,
            CASE WHEN (acl).is_grantable THEN 'YES' ELSE 'NO' END AS is_grantable
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(c.relacl, pg_catalog.acldefault('r', c.relowner))
        ) AS acl ON true
        WHERE n.nspname || '.' || c.relname = ANY(%s)
        ORDER BY n.nspname, c.relname, grantee, privilege_type, grantor
        """,
        (list(TARGET_OBJECTS),),
    )
    for object_name, rows in rows_by_object(fetch_all_dicts(cur)).items():
        objects[object_name]["acl"] = rows
        objects[object_name]["grants"] = rows


def attach_view_definitions(cur: Any, objects: dict[str, dict[str, Any]]) -> None:
    execute_static(
        cur,
        "cg005n_view_definitions",
        """
        SELECT
            n.nspname::text AS schema,
            c.relname::text AS relation,
            pg_catalog.pg_get_viewdef(c.oid, true)::text AS view_definition,
            EXISTS (
              SELECT 1
              FROM unnest(COALESCE(c.reloptions, ARRAY[]::text[])) opt
              WHERE opt = 'security_barrier=true'
            ) AS security_barrier,
            (
              SELECT split_part(opt, '=', 2)
              FROM unnest(COALESCE(c.reloptions, ARRAY[]::text[])) opt
              WHERE opt LIKE 'check_option=%%'
              LIMIT 1
            )::text AS check_option
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname || '.' || c.relname = ANY(%s)
          AND c.relkind IN ('v', 'm')
        ORDER BY n.nspname, c.relname
        """,
        (list(VIEW_TARGETS),),
    )
    for row in fetch_all_dicts(cur):
        object_name = f"{row['schema']}.{row['relation']}"
        definition = row["view_definition"] or ""
        objects[object_name]["view_definition_sha256"] = hashlib.sha256(definition.encode("utf-8")).hexdigest().upper()
        objects[object_name]["view_definition"] = definition
        objects[object_name]["view_options"] = {
            "security_barrier": row["security_barrier"],
            "check_option": row["check_option"],
        }


def classify_dependency(schema: str | None, relkind: str | None) -> str:
    return catalog_contract.classify_dependency(schema, relkind)


def group_dependencies(rows: list[dict[str, Any]], *, direction: str) -> dict[str, list[dict[str, Any]]]:
    return catalog_contract.normalize_dependency_section(rows, direction=direction)


def flatten_dependencies(grouped: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return catalog_contract.flatten_dependency_section(grouped)


def fetch_reverse_dependencies(cur: Any) -> dict[str, list[dict[str, Any]]]:
    execute_static(
        cur,
        "cg005n_reverse_dependencies",
        """
        WITH target AS (
          SELECT c.oid AS target_oid, n.nspname AS target_schema, c.relname AS target_name
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname || '.' || c.relname = ANY(%s)
        ), dep AS (
          SELECT DISTINCT
            t.target_schema || '.' || t.target_name AS target_object,
            dn.nspname::text AS dependent_schema,
            dc.relname::text AS dependent_name,
            dc.relkind::text AS dependent_relkind
          FROM target t
          JOIN pg_catalog.pg_depend d ON d.refobjid = t.target_oid
          JOIN pg_catalog.pg_rewrite rw ON rw.oid = d.objid
          JOIN pg_catalog.pg_class dc ON dc.oid = rw.ev_class
          JOIN pg_catalog.pg_namespace dn ON dn.oid = dc.relnamespace
          WHERE dc.oid <> t.target_oid
        )
        SELECT *
        FROM dep
        ORDER BY target_object, dependent_schema, dependent_name
        """,
        (list(VIEW_TARGETS),),
    )
    dependencies = fetch_all_dicts(cur)
    for row in dependencies:
        row["dependent_object"] = f"{row['dependent_schema']}.{row['dependent_name']}"
        row["material_dependency_status"] = classify_dependency(row["dependent_schema"], row["dependent_relkind"])
    return group_dependencies(dependencies, direction="reverse")


def fetch_direct_dependencies(cur: Any) -> dict[str, list[dict[str, Any]]]:
    execute_static(
        cur,
        "cg005n_direct_dependencies",
        """
        WITH target AS (
          SELECT c.oid AS target_oid, n.nspname AS target_schema, c.relname AS target_name
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname || '.' || c.relname = ANY(%s)
        )
        SELECT DISTINCT
          t.target_schema || '.' || t.target_name AS target_object,
          rn.nspname::text AS referenced_schema,
          rc.relname::text AS referenced_name,
          rc.relkind::text AS referenced_relkind
        FROM target t
        JOIN pg_catalog.pg_rewrite rw ON rw.ev_class = t.target_oid
        JOIN pg_catalog.pg_depend d ON d.objid = rw.oid
        JOIN pg_catalog.pg_class rc ON rc.oid = d.refobjid
        JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
        WHERE rc.oid <> t.target_oid
        ORDER BY target_object, referenced_schema, referenced_name
        """,
        (list(VIEW_TARGETS),),
    )
    dependencies = fetch_all_dicts(cur)
    for row in dependencies:
        row["referenced_object"] = f"{row['referenced_schema']}.{row['referenced_name']}"
        row["material_dependency_status"] = classify_dependency(row["referenced_schema"], row["referenced_relkind"])
    return group_dependencies(dependencies, direction="direct")


def fetch_rollback_baseline(cur: Any) -> dict[str, Any]:
    route_count = safe_scalar(
        cur,
        "cg005n_rollback_route_batch_count",
        "SELECT COUNT(*)::bigint FROM cg_core.ruta_rutero_load_batch",
    )
    max_batch = safe_scalar(
        cur,
        "cg005n_rollback_max_ruta_batch_id",
        "SELECT MAX(ruta_batch_id)::bigint FROM cg_core.ruta_rutero_load_batch",
    )
    assignment_exists = safe_scalar(
        cur,
        "cg005n_rollback_assignment_regclass",
        "SELECT to_regclass('cg_core.ruta_rutero_week_assignment') IS NOT NULL",
    )
    assignment_row_count: dict[str, Any] = {"available": True, "value": 0}
    if assignment_exists.get("available") and assignment_exists.get("value"):
        assignment_row_count = safe_scalar(
            cur,
            "cg005n_rollback_assignment_rows",
            "SELECT COUNT(*)::bigint FROM cg_core.ruta_rutero_week_assignment",
        )
    return {
        "route_batch_count": route_count.get("value") if route_count.get("available") else None,
        "max_ruta_batch_id": max_batch.get("value") if max_batch.get("available") else None,
        "assignment_table_exists": assignment_exists.get("value") if assignment_exists.get("available") else None,
        "assignment_row_count": assignment_row_count.get("value") if assignment_row_count.get("available") else None,
        "measurement_available": all(
            item.get("available")
            for item in (route_count, max_batch, assignment_exists, assignment_row_count)
        ),
    }


def find_catalog_gap(catalog: dict[str, Any]) -> str | None:
    gap = catalog_contract.find_catalog_gap(catalog, prestate=True)
    if gap:
        return gap
    baseline = catalog.get("rollback_baseline")
    if not isinstance(baseline, dict) or not baseline.get("measurement_available"):
        return "rollback_baseline"
    for key in ("route_batch_count", "max_ruta_batch_id", "assignment_table_exists", "assignment_row_count"):
        if key not in baseline:
            return f"rollback_baseline:{key}"
    return None


def safe_query_rows(cur: Any, name: str, sql: str, params: Sequence[Any] = ()) -> dict[str, Any]:
    savepoint = f"sp_{name}"
    cur.execute(f"SAVEPOINT {savepoint}")
    try:
        execute_static(cur, name, sql, params)
        rows = fetch_all_dicts(cur)
        cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        return {"available": True, "rows": rows}
    except Exception as exc:  # pragma: no cover - live DB timeout path
        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        return {"available": False, "error": type(exc).__name__}


def safe_scalar(cur: Any, name: str, sql: str, params: Sequence[Any] = ()) -> dict[str, Any]:
    result = safe_query_rows(cur, name, sql, params)
    if not result["available"]:
        return result
    rows = result["rows"]
    if not rows:
        return {"available": True, "value": None}
    return {"available": True, "value": next(iter(rows[0].values()))}


def fetch_aggregates(cur: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    payload["route_counts"] = {
        "public_ruta_rows": safe_scalar(cur, "cg005n_count_public_ruta", "SELECT COUNT(*)::bigint FROM public.ruta_rutero"),
        "route_batches": safe_scalar(cur, "cg005n_count_route_batches", "SELECT COUNT(*)::bigint FROM cg_core.ruta_rutero_load_batch"),
        "route_history_rows": safe_scalar(cur, "cg005n_count_route_history", "SELECT COUNT(*)::bigint FROM cg_core.ruta_rutero_load_rows"),
        "week_view_rows": safe_scalar(cur, "cg005n_count_week_view", "SELECT COUNT(*)::bigint FROM cg_core.v_ruta_rutero_load_batch_week_v2"),
        "latest_week_view_rows": safe_scalar(cur, "cg005n_count_latest_week", "SELECT COUNT(*)::bigint FROM cg_core.v_ruta_rutero_latest_week_batch_v2"),
        "resolved_view_rows": safe_scalar(cur, "cg005n_count_resolved", "SELECT COUNT(*)::bigint FROM cg_core.v_rr_frecuencia_base_resuelta_v2"),
    }
    payload["target_week"] = {
        "effective_week_start": TARGET_WEEK_START,
        "week_view_rows": safe_scalar(
            cur,
            "cg005n_target_week_view",
            "SELECT COUNT(*)::bigint FROM cg_core.v_ruta_rutero_load_batch_week_v2 WHERE effective_week_start::text = %s",
            (TARGET_WEEK_START,),
        ),
        "latest_week_rows": safe_scalar(
            cur,
            "cg005n_target_latest_week",
            "SELECT COUNT(*)::bigint FROM cg_core.v_ruta_rutero_latest_week_batch_v2 WHERE effective_week_start::text = %s",
            (TARGET_WEEK_START,),
        ),
        "resolved_rows": safe_scalar(
            cur,
            "cg005n_target_resolved",
            "SELECT COUNT(*)::bigint FROM cg_core.v_rr_frecuencia_base_resuelta_v2 WHERE effective_week_start::text = %s",
            (TARGET_WEEK_START,),
        ),
    }

    payload["weekly_baseline_recent"] = safe_query_rows(
        cur,
        "cg005n_weekly_baseline",
        """
        SELECT
            "SEMANA_INICIO"::text AS semana_inicio,
            COUNT(*)::bigint AS rows,
            COUNT(*) FILTER (WHERE "ALERTA" = 'CUMPLE')::bigint AS cumple_rows,
            COUNT(*) FILTER (WHERE "ALERTA" = 'INCUMPLE')::bigint AS incumple_rows
        FROM cg_mart.v_cg_out_weekly_v2
        GROUP BY "SEMANA_INICIO"::text
        ORDER BY semana_inicio DESC
        LIMIT 12
        """,
    )
    payload["privacy"] = "aggregate_only_no_customer_store_person_payload_or_secret_values"
    return payload


def build_signature_plan(objects: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "strategy": "signature_preserving_create_or_replace",
        "drop_view_required": False,
        "rules": [
            "preserve every existing column name, order and type for target views",
            "append only new compatibility columns at the end of each target view",
            "block if catalog fingerprint differs before DDL",
        ],
        "target_view_plan": {
            "cg_core.v_ruta_rutero_load_batch_week_v2": {
                "prestate_column_count": len(objects["cg_core.v_ruta_rutero_load_batch_week_v2"]["columns"]),
                "preserve_existing_prefix": True,
                "append_columns": ["route_policy_version", "route_week_source", "assignment_id", "assigned_at"],
            },
            "cg_core.v_ruta_rutero_latest_week_batch_v2": {
                "prestate_column_count": len(objects["cg_core.v_ruta_rutero_latest_week_batch_v2"]["columns"]),
                "preserve_existing_prefix": True,
                "append_columns": ["route_policy_version", "route_week_source", "assignment_id"],
            },
            "cg_core.v_rr_frecuencia_base_resuelta_v2": {
                "prestate_column_count": len(objects["cg_core.v_rr_frecuencia_base_resuelta_v2"]["columns"]),
                "preserve_existing_prefix": True,
                "append_columns": ["route_policy_version", "route_week_source", "cod_rt_norm", "ruta_person_conflict_flag"],
            },
        },
    }


def build_catalog() -> dict[str, Any]:
    conn, source = connect_readonly()
    try:
        status = begin_guarded_readonly(conn)
        with conn.cursor() as cur:
            execute_static(cur, "cg005n_short_timeout", "SET LOCAL statement_timeout = '15s'")
            objects = fetch_relation_objects(cur)
            attach_columns(cur, objects)
            attach_constraints(cur, objects)
            attach_indexes(cur, objects)
            attach_acl(cur, objects)
            attach_view_definitions(cur, objects)
            reverse_dependencies = fetch_reverse_dependencies(cur)
            direct_dependencies = fetch_direct_dependencies(cur)
            rollback_baseline = fetch_rollback_baseline(cur)
            aggregates = fetch_aggregates(cur)
        catalog = {
            "phase": PHASE,
            "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source_used": source,
            "db_access": {
                "current_user": status.current_user,
                "transaction_read_only": status.transaction_read_only,
                "default_transaction_read_only": status.default_transaction_read_only,
                "writes_attempted": False,
                "dsn_printed": False,
            },
            "target_objects": objects,
            "direct_dependencies": direct_dependencies,
            "reverse_dependencies": reverse_dependencies,
            "material_dependency_unknowns": [],
            "rollback_baseline": rollback_baseline,
            "aggregates": aggregates,
            "signature_preserving_plan": build_signature_plan(objects),
            "completion": {},
        }
        catalog = catalog_contract.normalize_catalog(catalog, prestate=True)
        catalog_gap = find_catalog_gap(catalog)
        if catalog_gap:
            catalog["completion"]["catalog_gap"] = catalog_gap
            catalog["completion"]["catalog_complete"] = False
            catalog["completion"]["technical_catalog_complete"] = False
            catalog["completion"]["stop_required"] = True
        attach_separate_hashes(catalog)
        return catalog
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True, default=json_default) + "\n", encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture CG005N prestate catalog using DB_URL_CODEX_RO only.")
    parser.add_argument("--json-out", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    catalog = build_catalog()
    output_path = Path(args.json_out)
    write_json(output_path, catalog)
    prestate_file_sha256 = sha256_bytes(output_path.read_bytes())
    print(
        json.dumps(
            {
                "verdict": "OK" if catalog["completion"]["catalog_complete"] else "BLOCK",
                "catalog_complete": catalog["completion"]["catalog_complete"],
                "catalog_gap": catalog["completion"]["catalog_gap"],
                "material_dependency_unknowns": catalog["completion"]["unknown_dependencies"],
                "prestate_file_sha256": prestate_file_sha256,
                "technical_catalog_fingerprint_sha256": catalog["technical_catalog_fingerprint_sha256"],
                "rollback_baseline_sha256": catalog["rollback_baseline_sha256"],
                "source_used": catalog["source_used"],
                "current_user": catalog["db_access"]["current_user"],
                "transaction_read_only": catalog["db_access"]["transaction_read_only"],
                "acl_from_pg_class": True,
                "comments_captured": True,
                "reloptions_captured": True,
                "view_options_captured": True,
                "rollback_baseline_measured": catalog["rollback_baseline"]["measurement_available"],
                "max_ruta_batch_id_measured": catalog["rollback_baseline"]["max_ruta_batch_id"] is not None,
                "writes_attempted": False,
                "dsn_printed": False,
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0 if catalog["completion"]["catalog_complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
