#!/usr/bin/env python
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Sequence


TARGET_OBJECTS = (
    "cg_core.ruta_rutero_week_assignment",
    "cg_core.v_ruta_rutero_load_batch_week_v2",
    "cg_core.v_ruta_rutero_latest_week_batch_v2",
    "cg_core.v_rr_frecuencia_base_resuelta_v2",
    "cg_core.ruta_rutero_load_batch",
    "cg_core.ruta_rutero_load_rows",
    "public.ruta_rutero",
)

VIEW_TARGETS = (
    "cg_core.v_ruta_rutero_load_batch_week_v2",
    "cg_core.v_ruta_rutero_latest_week_batch_v2",
    "cg_core.v_rr_frecuencia_base_resuelta_v2",
)

KNOWN_MATERIAL_SCHEMAS = {"cg_core", "cg_mart", "public"}
KNOWN_MATERIAL_KINDS = {"r", "p", "v", "m"}

TECHNICAL_FINGERPRINT_KEYS = (
    "target_objects",
    "direct_dependencies",
    "reverse_dependencies",
    "material_dependency_unknowns",
    "completion",
)

COLUMN_IDENTITY_KEYS = (
    "ordinal_position",
    "column_name",
    "data_type",
    "udt_name",
    "is_nullable",
    "column_default_present",
)


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=json_default)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest().upper()


def sha256_canonical(value: Any) -> str:
    return sha256_text(canonical_json(value))


def normalize_scalar(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def column_identity(
    column: dict[str, Any],
    *,
    include_position: bool = True,
    include_default: bool = True,
) -> dict[str, Any]:
    keys = list(COLUMN_IDENTITY_KEYS)
    if not include_position:
        keys.remove("ordinal_position")
    if not include_default:
        keys.remove("column_default_present")
    return {key: normalize_scalar(column.get(key)) for key in keys}


def normalize_columns(columns: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [column_identity(column) for column in sorted(columns, key=lambda row: row.get("ordinal_position") or 0)]


def normalize_acl_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {
            "grantor": row.get("grantor"),
            "grantee": row.get("grantee"),
            "privilege_type": row.get("privilege_type"),
            "is_grantable": str(row.get("is_grantable", "NO")).upper(),
        }
        for row in rows
    ]
    return sorted(normalized, key=canonical_json)


def acl_identity(obj: dict[str, Any]) -> list[dict[str, Any]]:
    return normalize_acl_rows(obj.get("acl", []))


def normalize_named_rows(rows: Iterable[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return sorted(
        [{field: normalize_scalar(value) for field, value in row.items()} for row in rows],
        key=lambda row: str(row.get(key, "")) + canonical_json(row),
    )


def normalize_view_options(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "security_barrier": bool(value.get("security_barrier")),
        "check_option": value.get("check_option"),
    }


def normalize_object(object_name: str, obj: dict[str, Any]) -> dict[str, Any]:
    target_schema, target_name = object_name.split(".", 1)
    columns = normalize_columns(obj.get("columns", []))
    view_definition = obj.get("view_definition")
    normalized = {
        "exists": obj.get("exists"),
        "schema": obj.get("schema") or target_schema,
        "name": obj.get("name") or target_name,
        "relkind": obj.get("relkind"),
        "owner": obj.get("owner"),
        "comment": obj.get("comment"),
        "comment_captured": obj.get("comment_captured", "comment" in obj),
        "description_present": obj.get("description_present"),
        "reloptions": sorted(obj.get("reloptions") or []),
        "relacl": sorted(obj.get("relacl") or []),
        "columns": columns,
        "constraints": normalize_named_rows(obj.get("constraints", []), "constraint_name"),
        "indexes": normalize_named_rows(obj.get("indexes", []), "index_name"),
        "acl": normalize_acl_rows(obj.get("acl", [])),
        "grants": normalize_acl_rows(obj.get("grants", obj.get("acl", []))),
        "view_definition": view_definition,
        "view_definition_sha256": sha256_text(view_definition or "") if view_definition is not None else None,
        "column_signature_sha256": sha256_canonical(columns) if columns else None,
    }
    if object_name in VIEW_TARGETS or obj.get("view_options") is not None:
        normalized["view_options"] = normalize_view_options(obj.get("view_options"))
    return normalized


def classify_dependency(schema: str | None, relkind: str | None) -> str:
    if schema in KNOWN_MATERIAL_SCHEMAS and relkind in KNOWN_MATERIAL_KINDS:
        return "KNOWN_MATERIAL_RELATION"
    return "UNKNOWN"


def normalize_dependency_row(row: dict[str, Any], *, direction: str) -> dict[str, Any]:
    raw = {key: normalize_scalar(value) for key, value in row.items()}
    target_object = raw.get("target_object")
    if direction == "direct":
        schema = raw.get("referenced_schema")
        name = raw.get("referenced_name")
        relkind = raw.get("referenced_relkind") or raw.get("referenced_kind")
        referenced_object = raw.get("referenced_object") or (f"{schema}.{name}" if schema and name else None)
        normalized = {
            "target_object": target_object,
            "referenced_schema": schema,
            "referenced_name": name,
            "referenced_relkind": relkind,
            "referenced_object": referenced_object,
            "material_dependency_status": raw.get("material_dependency_status") or classify_dependency(schema, relkind),
        }
    else:
        schema = raw.get("dependent_schema")
        name = raw.get("dependent_name")
        relkind = raw.get("dependent_relkind") or raw.get("dependent_kind")
        dependent_object = raw.get("dependent_object") or (f"{schema}.{name}" if schema and name else None)
        normalized = {
            "target_object": target_object,
            "dependent_schema": schema,
            "dependent_name": name,
            "dependent_relkind": relkind,
            "dependent_object": dependent_object,
            "material_dependency_status": raw.get("material_dependency_status") or classify_dependency(schema, relkind),
        }
    return normalized


def normalize_dependency_section(value: Any, *, direction: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {view_name: [] for view_name in VIEW_TARGETS}
    if isinstance(value, dict):
        for target, rows in value.items():
            grouped[target] = [
                normalize_dependency_row({**row, "target_object": row.get("target_object", target)}, direction=direction)
                for row in (rows or [])
            ]
    elif isinstance(value, list):
        for row in value:
            target = row.get("target_object")
            if target:
                grouped.setdefault(target, []).append(normalize_dependency_row(row, direction=direction))
    for target, rows in grouped.items():
        grouped[target] = sorted(rows, key=canonical_json)
    return grouped


def flatten_dependency_section(section: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for target in sorted(section):
        rows.extend(section[target])
    return rows


def material_dependency_unknowns(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    rows = flatten_dependency_section(catalog.get("direct_dependencies", {}))
    rows.extend(flatten_dependency_section(catalog.get("reverse_dependencies", {})))
    return [row for row in rows if row.get("material_dependency_status") == "UNKNOWN"]


def find_catalog_gap(catalog: dict[str, Any], *, prestate: bool) -> str | None:
    target_objects = catalog.get("target_objects")
    if not isinstance(target_objects, dict):
        return "target_objects"
    for object_name in TARGET_OBJECTS:
        if object_name not in target_objects:
            return f"target_objects.{object_name}"
    if prestate and target_objects["cg_core.ruta_rutero_week_assignment"].get("exists") is not False:
        return "prestate.assignment_absent"
    for view_name in VIEW_TARGETS:
        obj = target_objects.get(view_name, {})
        if obj.get("exists") is not True:
            return f"views.{view_name}.missing"
        if not obj.get("view_definition"):
            return f"views.{view_name}.view_definition"
    for object_name, obj in target_objects.items():
        if not obj.get("exists"):
            continue
        if not obj.get("owner"):
            return f"owner.{object_name}"
        if "comment" not in obj or not obj.get("comment_captured", "comment" in obj):
            return f"comments.{object_name}"
        if "reloptions" not in obj:
            return f"reloptions.{object_name}"
        if "acl" not in obj or not isinstance(obj.get("acl"), list):
            return f"acl.{object_name}"
        columns = obj.get("columns")
        if not isinstance(columns, list) or not columns:
            return f"columns.{object_name}"
        for column in columns:
            for key in COLUMN_IDENTITY_KEYS:
                if key not in column:
                    return f"columns.{object_name}.{key}"
        if "constraints" not in obj or not isinstance(obj.get("constraints"), list):
            return f"constraints.{object_name}"
        if "indexes" not in obj or not isinstance(obj.get("indexes"), list):
            return f"indexes.{object_name}"
        if object_name in VIEW_TARGETS and not isinstance(obj.get("view_options"), dict):
            return f"view_options.{object_name}"
    for section in ("direct_dependencies", "reverse_dependencies"):
        if not isinstance(catalog.get(section), dict):
            return section
        for view_name in VIEW_TARGETS:
            if view_name not in catalog[section]:
                return f"{section}.{view_name}"
    if catalog.get("material_dependency_unknowns"):
        return "material_dependency_unknowns"
    return None


def build_completion(catalog: dict[str, Any], *, prestate: bool) -> dict[str, Any]:
    gap = find_catalog_gap(catalog, prestate=prestate)
    unknown_count = len(catalog.get("material_dependency_unknowns") or [])
    complete = gap is None and unknown_count == 0
    return {
        "unknown_dependencies": unknown_count,
        "material_dependencies_unknown": unknown_count,
        "catalog_complete": complete,
        "technical_catalog_complete": complete,
        "catalog_gap": gap,
        "stop_required": not complete,
    }


def _normalize_technical_catalog(catalog: dict[str, Any], *, prestate: bool) -> dict[str, Any]:
    normalized = dict(catalog)
    normalized["target_objects"] = {
        object_name: normalize_object(object_name, catalog.get("target_objects", {}).get(object_name, {}))
        for object_name in TARGET_OBJECTS
    }
    normalized["direct_dependencies"] = normalize_dependency_section(catalog.get("direct_dependencies", {}), direction="direct")
    normalized["reverse_dependencies"] = normalize_dependency_section(catalog.get("reverse_dependencies", {}), direction="reverse")
    normalized["material_dependency_unknowns"] = material_dependency_unknowns(normalized)
    normalized["completion"] = build_completion(normalized, prestate=prestate)
    return normalized


def normalize_catalog(catalog: dict[str, Any], *, prestate: bool) -> dict[str, Any]:
    normalized = _normalize_technical_catalog(catalog, prestate=prestate)
    normalized["technical_catalog_fingerprint_sha256"] = technical_fingerprint(normalized)
    if "rollback_baseline" in normalized:
        normalized["rollback_baseline_sha256"] = rollback_baseline_fingerprint(normalized)
    return normalized


def technical_payload(catalog: dict[str, Any]) -> dict[str, Any]:
    assignment = catalog.get("target_objects", {}).get("cg_core.ruta_rutero_week_assignment", {})
    prestate = assignment.get("exists") is not True
    normalized = _normalize_technical_catalog(catalog, prestate=prestate)
    return {key: normalized.get(key) for key in TECHNICAL_FINGERPRINT_KEYS}


def technical_fingerprint(catalog: dict[str, Any]) -> str:
    return sha256_canonical(technical_payload(catalog))


def rollback_baseline_fingerprint(catalog: dict[str, Any]) -> str:
    return sha256_canonical(catalog.get("rollback_baseline"))


def validate_catalog_complete(catalog: dict[str, Any], *, prestate: bool) -> None:
    gap = find_catalog_gap(catalog, prestate=prestate)
    if gap:
        raise ValueError(f"catalog_incomplete:{gap}")
