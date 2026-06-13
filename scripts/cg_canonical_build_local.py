#!/usr/bin/env python
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse


PHASE = "FASE_C007_CANONICAL_BUILDER_PACKAGING_AND_SEMANTIC_PARITY_NO_SUPABASE_WRITE"
FORBIDDEN_CONTEXT_TOKENS = ("latest", "current", "implicit", "auto winner")
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}

DAILY_KEY_COLUMNS = ["fecha_visita", "cod_rt", "cliente_norm"]
DAILY_TECHNICAL_FULL_ROW_COLUMNS = [
    "semana_inicio",
    "fecha_visita",
    "cod_rt",
    "cod_b2b",
    "cliente",
    "cliente_norm",
    "local_nombre",
    "gestor",
    "gestor_norm",
    "rutero",
    "reponedor_scope",
    "reponedor_scope_norm",
    "supervisor",
    "jefe_operaciones",
    "modalidad",
    "semana_iso",
    "fuente_ganadora",
    "fuentes_presentes",
    "tiene_kpione2",
    "tiene_power_app",
    "tiene_kpione1",
    "power_app_fallback",
    "kpione1_audit_only",
    "useful_day",
    "raw_evidence_count",
    "same_source_multimark",
    "multisource_overlap",
    "kpione_rows_dia",
    "kpione2_rows_dia",
    "power_app_rows_dia",
    "persona_conflicto_rows_dia",
    "match_quality",
    "registro_fuera_cruce",
]
DAILY_BUSINESS_SEMANTIC_COLUMNS = [
    column
    for column in DAILY_TECHNICAL_FULL_ROW_COLUMNS
    if column not in {"match_quality", "registro_fuera_cruce"}
]

WEEKLY_KEY_COLUMNS = ["SEMANA_INICIO", "COD_RT", "CLIENTE_NORM_FILTER"]
WEEKLY_TECHNICAL_FULL_ROW_COLUMNS = [
    "COD_RT",
    "COD_B2B",
    "LOCAL",
    "CLIENTE",
    "GESTOR",
    "RUTERO",
    "REPONEDOR",
    "SUPERVISOR",
    "MODALIDAD",
    "SEMANA_INICIO",
    "SEMANA_ISO",
    "LUNES_FLAG",
    "MARTES_FLAG",
    "MIERCOLES_FLAG",
    "JUEVES_FLAG",
    "VIERNES_FLAG",
    "SABADO_FLAG",
    "DOMINGO_FLAG",
    "LUNES_PLAN",
    "MARTES_PLAN",
    "MIERCOLES_PLAN",
    "JUEVES_PLAN",
    "VIERNES_PLAN",
    "SABADO_PLAN",
    "DOMINGO_PLAN",
    "VISITA",
    "VISITA_REALIZADA",
    "DIFERENCIA",
    "ALERTA",
    "DIAS_KPIONE",
    "DIAS_KPIONE2",
    "DIAS_POWER_APP",
    "DIAS_DOBLE_MARCAJE",
    "DIAS_TRIPLE_MARCAJE",
    "FUENTES_REPORTADAS_SEMANA",
    "PERSONA_CONFLICTO_ROWS",
    "VISITA_REALIZADA_RAW",
    "VISITA_REALIZADA_CAP",
    "SOBRE_CUMPLIMIENTO",
    "RUTA_DUPLICADA_FLAG",
    "RUTA_DUPLICADA_ROWS",
    "SEMANA_INICIO_KEY",
    "GESTOR_NORM_FILTER",
    "RUTERO_NORM_FILTER",
    "LOCAL_NORM_FILTER",
    "CLIENTE_NORM_FILTER",
    "ALERTA_NORM_FILTER",
    "GESTION_COMPARTIDA_FLAG_CALC",
    "VISITAS_PENDIENTES_CALC",
]
WEEKLY_BUSINESS_SEMANTIC_COLUMNS = [
    column
    for column in WEEKLY_TECHNICAL_FULL_ROW_COLUMNS
    if column
    not in {
        "SEMANA_INICIO_KEY",
        "GESTOR_NORM_FILTER",
        "RUTERO_NORM_FILTER",
        "LOCAL_NORM_FILTER",
        "CLIENTE_NORM_FILTER",
        "ALERTA_NORM_FILTER",
    }
]


class BuilderBlock(RuntimeError):
    pass


@dataclass(frozen=True)
class RawLineageItem:
    source_key: str
    batch_id: int
    source_sheet: str
    loaded_rows: int
    status: str
    loader_name: str
    source_type: str
    snapshot_hash: str
    selection_reason: str


@dataclass(frozen=True)
class RouteWeekItem:
    week_start: str
    source_ruta_batch_ids: tuple[int, ...]
    route_week_snapshot_version_id: str
    expected_surface_hash: str | None


@dataclass(frozen=True)
class BuildContext:
    build_id: str
    affected_date_start: str
    affected_date_end: str
    affected_weeks: tuple[str, ...]
    raw_lineage: tuple[RawLineageItem, ...]
    route_weeks: tuple[RouteWeekItem, ...]
    source_precedence_version: str
    daily_builder_version: str
    weekly_builder_version: str
    input_manifest_hash: str | None


def _walk_strings(value: Any, path: str = "$") -> list[tuple[str, str]]:
    if isinstance(value, str):
        return [(path, value)]
    if isinstance(value, dict):
        pairs: list[tuple[str, str]] = []
        for key, item in value.items():
            pairs.extend(_walk_strings(item, f"{path}.{key}"))
        return pairs
    if isinstance(value, list):
        pairs = []
        for index, item in enumerate(value):
            pairs.extend(_walk_strings(item, f"{path}[{index}]"))
        return pairs
    return []


def validate_no_forbidden_context_tokens(value: Any) -> None:
    hits = []
    for path, text in _walk_strings(value):
        lower = text.lower()
        for token in FORBIDDEN_CONTEXT_TOKENS:
            if token in lower:
                hits.append(f"{path}:{token}")
    if hits:
        raise BuilderBlock("forbidden_context_token:" + ",".join(hits))


def load_build_context(path: Path) -> BuildContext:
    document = json.loads(path.read_text(encoding="utf-8"))
    core = {
        "build_context": document.get("build_context", {}),
        "raw_lineage": document.get("raw_lineage", []),
        "route_lineage": document.get("route_lineage", {}),
    }
    validate_no_forbidden_context_tokens(core)

    build = core["build_context"]
    route_lineage = core["route_lineage"]
    raw_items = []
    for item in core["raw_lineage"]:
        raw_items.append(
            RawLineageItem(
                source_key=str(item["source_key"]),
                batch_id=int(item["batch_id"]),
                source_sheet=str(item["source_sheet"]),
                loaded_rows=int(item["loaded_rows"]),
                status=str(item["status"]),
                loader_name=str(item["loader_name"]),
                source_type=str(item["source_type"]),
                snapshot_hash=str(item["snapshot_hash"]),
                selection_reason=str(item.get("selection_reason", "")),
            )
        )

    route_items = []
    for item in route_lineage.get("weeks", []):
        route_items.append(
            RouteWeekItem(
                week_start=str(item["week_start"]),
                source_ruta_batch_ids=tuple(int(value) for value in item["source_ruta_batch_ids"]),
                route_week_snapshot_version_id=str(item["route_week_snapshot_version_id"]),
                expected_surface_hash=item.get("surface_hash"),
            )
        )

    required = [
        "build_id",
        "affected_date_start",
        "affected_date_end",
        "affected_weeks",
        "source_precedence_version",
        "daily_builder_version",
        "weekly_builder_version",
    ]
    missing = [key for key in required if key not in build]
    if missing:
        raise BuilderBlock("missing_build_context_keys:" + ",".join(missing))
    if not raw_items:
        raise BuilderBlock("raw_lineage_empty")
    if not route_items:
        raise BuilderBlock("route_lineage_empty")

    return BuildContext(
        build_id=str(build["build_id"]),
        affected_date_start=str(build["affected_date_start"]),
        affected_date_end=str(build["affected_date_end"]),
        affected_weeks=tuple(str(value) for value in build["affected_weeks"]),
        raw_lineage=tuple(raw_items),
        route_weeks=tuple(route_items),
        source_precedence_version=str(build["source_precedence_version"]),
        daily_builder_version=str(build["daily_builder_version"]),
        weekly_builder_version=str(build["weekly_builder_version"]),
        input_manifest_hash=build.get("input_manifest_hash"),
    )


def assert_local_postgres_dsn(dsn: str) -> None:
    parsed = urlparse(dsn)
    host = (parsed.hostname or "").lower()
    if not dsn:
        raise BuilderBlock("missing_dsn")
    if "supabase" in dsn.lower() or "pooler" in dsn.lower():
        raise BuilderBlock("supabase_dsn_rejected")
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise BuilderBlock("unsupported_dsn_scheme")
    if host not in LOCAL_HOSTS:
        raise BuilderBlock(f"non_local_dsn_host:{host or 'missing'}")


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return str(int(value))
        return format(value.normalize(), "f")
    return str(value).strip()


def stable_md5_parts(values: list[Any]) -> str:
    raw = "\x1f".join(normalize_text(value) for value in values)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def deterministic_rows_hash(rows: list[dict[str, Any]], columns: list[str], order_columns: list[str]) -> str:
    ordered = sorted(rows, key=lambda row: tuple(normalize_text(row.get(column)) for column in order_columns))
    raw = "".join(stable_md5_parts([row.get(column) for column in columns]) for row in ordered)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def summarize_source_day(evidence: list[dict[str, Any]], route_reponedor_norm: str = "") -> dict[str, Any]:
    counts = {
        "KPIONE": 0,
        "KPIONE2": 0,
        "POWER_APP": 0,
    }
    persona_conflicts = 0
    outside = False
    for item in evidence:
        source = str(item.get("fuente") or item.get("source") or "")
        if source not in counts:
            continue
        counts[source] += 1
        if source == "KPIONE2" and normalize_text(item.get("persona_norm")) != normalize_text(route_reponedor_norm):
            persona_conflicts += 1
        if normalize_text(item.get("registro_fuera_cruce")).upper() not in {"", "N/A"}:
            outside = True

    source_count = sum(1 for value in counts.values() if value > 0)
    winning_source = "KPIONE2" if counts["KPIONE2"] else "POWER_APP" if counts["POWER_APP"] else None
    return {
        "included": bool(evidence),
        "fuente_ganadora": winning_source,
        "fuentes_presentes": "+".join(source for source in ("KPIONE2", "POWER_APP", "KPIONE") if counts[source] > 0),
        "tiene_kpione2": int(counts["KPIONE2"] > 0),
        "tiene_power_app": int(counts["POWER_APP"] > 0),
        "tiene_kpione1": int(counts["KPIONE"] > 0),
        "power_app_fallback": int(counts["KPIONE2"] == 0 and counts["POWER_APP"] > 0),
        "kpione1_audit_only": int(counts["KPIONE"] > 0),
        "useful_day": int(winning_source is not None),
        "raw_evidence_count": sum(counts.values()),
        "same_source_multimark": int(max(counts.values()) > 1),
        "multisource_overlap": int(source_count > 1),
        "triple_source_overlap": int(source_count >= 3),
        "kpione_rows_dia": counts["KPIONE"],
        "kpione2_rows_dia": counts["KPIONE2"],
        "power_app_rows_dia": counts["POWER_APP"],
        "persona_conflicto_rows_dia": persona_conflicts,
        "registro_fuera_cruce": "FUERA_CRUCE" if outside else "N/A",
    }


def summarize_week(plan_visits: int, realized_visits: int) -> dict[str, int | str]:
    cap = min(realized_visits, plan_visits)
    return {
        "VISITA": plan_visits,
        "VISITA_REALIZADA": realized_visits,
        "VISITA_REALIZADA_RAW": realized_visits,
        "VISITA_REALIZADA_CAP": cap,
        "DIFERENCIA": realized_visits - plan_visits,
        "SOBRE_CUMPLIMIENTO": max(realized_visits - plan_visits, 0),
        "VISITAS_PENDIENTES_CALC": max(plan_visits - cap, 0),
        "ALERTA": "CUMPLE" if realized_visits >= plan_visits else "INCUMPLE",
    }


def select_route_winners(rows: list[dict[str, Any]], week_start: str, source_batch_ids: list[int]) -> list[dict[str, Any]]:
    selected = [
        row
        for row in rows
        if int(row["ruta_batch_id"]) in set(source_batch_ids)
        and normalize_text(row.get("cod_rt"))
        and normalize_text(row.get("cliente_norm"))
    ]
    winners: dict[tuple[str, str], int] = {}
    for row in selected:
        key = (normalize_text(row["cod_rt"]), normalize_text(row["cliente_norm"]).upper())
        winners[key] = max(winners.get(key, -1), int(row["ruta_batch_id"]))
    result = []
    for row in selected:
        key = (normalize_text(row["cod_rt"]), normalize_text(row["cliente_norm"]).upper())
        if int(row["ruta_batch_id"]) == winners[key]:
            enriched = dict(row)
            enriched["week_start"] = week_start
            result.append(enriched)
    return result


def quote_ident(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise BuilderBlock(f"invalid_identifier:{identifier}")
    return '"' + identifier.replace('"', '""') + '"'


def schema_sql_name(schema: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", schema):
        raise BuilderBlock(f"invalid_schema:{schema}")
    return quote_ident(schema)


def _column_expr(column: str) -> str:
    if column.isupper():
        return f'coalesce("{column}"::text, \'\')'
    return f"coalesce({quote_ident(column)}::text, '')"


def _order_expr(column: str) -> str:
    return f'"{column}"' if column.isupper() else quote_ident(column)


def _hash_sql(table: str, columns: list[str], order_columns: list[str], run_no: int) -> str:
    exprs = ", ".join(_column_expr(column) for column in columns)
    order = ", ".join(_order_expr(column) for column in order_columns)
    return f"""
        SELECT md5(coalesce(string_agg(md5(concat_ws(chr(31), {exprs})), '' ORDER BY {order}), ''))
        FROM {table}
        WHERE run_no = {int(run_no)}
    """


def connect_local(dsn: str):
    assert_local_postgres_dsn(dsn)
    import psycopg

    return psycopg.connect(dsn, connect_timeout=10)


def validate_raw_lineage(cur: Any, context: BuildContext) -> list[dict[str, Any]]:
    validated = []
    for item in context.raw_lineage:
        cur.execute(
            """
            SELECT batch_id, source_sheet, loader_name, loaded_rows, status, notes
            FROM cg_audit.batch_registry
            WHERE batch_id = %s
            """,
            (item.batch_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise BuilderBlock(f"missing_raw_batch:{item.source_key}:{item.batch_id}")
        batch_id, source_sheet, loader_name, loaded_rows, status, notes = row
        if source_sheet != item.source_sheet:
            raise BuilderBlock(f"raw_batch_source_sheet_mismatch:{item.source_key}:{batch_id}")
        if int(loaded_rows) != item.loaded_rows:
            raise BuilderBlock(f"raw_batch_loaded_rows_mismatch:{item.source_key}:{batch_id}")
        if status != item.status:
            raise BuilderBlock(f"raw_batch_status_mismatch:{item.source_key}:{batch_id}")
        if loader_name != item.loader_name:
            raise BuilderBlock(f"raw_batch_loader_mismatch:{item.source_key}:{batch_id}")
        if item.snapshot_hash not in str(notes):
            raise BuilderBlock(f"raw_batch_snapshot_hash_mismatch:{item.source_key}:{batch_id}")
        validated.append(
            {
                "source_key": item.source_key,
                "batch_id": item.batch_id,
                "loaded_rows": item.loaded_rows,
                "snapshot_hash_validated": True,
            }
        )
    return validated


def create_schema(cur: Any, schema: str, replace_schema: bool) -> None:
    q_schema = schema_sql_name(schema)
    if replace_schema:
        if not (schema.startswith("c007_") or schema.startswith("tmp_")):
            raise BuilderBlock(f"replace_schema_not_allowed:{schema}")
        cur.execute(f"DROP SCHEMA IF EXISTS {q_schema} CASCADE")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {q_schema}")


def create_base_tables(cur: Any, schema: str, context: BuildContext) -> None:
    q_schema = schema_sql_name(schema)
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {q_schema}.build_context (
            build_id text primary key,
            build_context_json jsonb not null,
            created_at timestamptz not null default now()
        );
        CREATE TABLE IF NOT EXISTS {q_schema}.raw_lineage (
            source_key text primary key,
            batch_id bigint not null,
            source_sheet text not null,
            loaded_rows integer not null,
            status text not null,
            loader_name text not null,
            source_type text not null,
            snapshot_hash text not null,
            selection_reason text not null,
            selected_window_rows integer
        );
        CREATE TABLE IF NOT EXISTS {q_schema}.route_source_plan (
            week_start date not null,
            route_week_snapshot_version_id text not null,
            source_ruta_batch_id bigint not null,
            expected_surface_hash text
        );
        """
    )
    cur.execute(f"DELETE FROM {q_schema}.build_context")
    cur.execute(f"DELETE FROM {q_schema}.raw_lineage")
    cur.execute(f"DELETE FROM {q_schema}.route_source_plan")
    cur.execute(
        f"INSERT INTO {q_schema}.build_context (build_id, build_context_json) VALUES (%s, %s::jsonb)",
        (context.build_id, json.dumps(context_to_json(context), sort_keys=True)),
    )
    for item in context.raw_lineage:
        cur.execute(
            f"""
            INSERT INTO {q_schema}.raw_lineage (
                source_key, batch_id, source_sheet, loaded_rows, status, loader_name,
                source_type, snapshot_hash, selection_reason
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                item.source_key,
                item.batch_id,
                item.source_sheet,
                item.loaded_rows,
                item.status,
                item.loader_name,
                item.source_type,
                item.snapshot_hash,
                item.selection_reason,
            ),
        )
    for item in context.route_weeks:
        for batch_id in item.source_ruta_batch_ids:
            cur.execute(
                f"""
                INSERT INTO {q_schema}.route_source_plan (
                    week_start, route_week_snapshot_version_id, source_ruta_batch_id, expected_surface_hash
                ) VALUES (%s,%s,%s,%s)
                """,
                (item.week_start, item.route_week_snapshot_version_id, batch_id, item.expected_surface_hash),
            )


def context_to_json(context: BuildContext) -> dict[str, Any]:
    return {
        "build_id": context.build_id,
        "affected_date_start": context.affected_date_start,
        "affected_date_end": context.affected_date_end,
        "affected_weeks": list(context.affected_weeks),
        "source_precedence_version": context.source_precedence_version,
        "daily_builder_version": context.daily_builder_version,
        "weekly_builder_version": context.weekly_builder_version,
        "input_manifest_hash": context.input_manifest_hash,
        "raw_lineage": [item.__dict__ for item in context.raw_lineage],
        "route_weeks": [
            {
                "week_start": item.week_start,
                "source_ruta_batch_ids": list(item.source_ruta_batch_ids),
                "route_week_snapshot_version_id": item.route_week_snapshot_version_id,
                "expected_surface_hash": item.expected_surface_hash,
            }
            for item in context.route_weeks
        ],
    }


def build_route_snapshot(cur: Any, schema: str) -> None:
    q_schema = schema_sql_name(schema)
    cur.execute(f"DROP TABLE IF EXISTS {q_schema}.route_week_snapshot")
    cur.execute(
        f"""
        CREATE TABLE {q_schema}.route_week_snapshot AS
        WITH normalized AS (
            SELECT
                p.week_start,
                p.route_week_snapshot_version_id,
                r.ruta_batch_id,
                NULLIF(TRIM(COALESCE(r.cod_rt_norm, r.cod_rt)), '') AS cod_rt,
                NULLIF(TRIM(COALESCE(r.cod_b2b_norm, r.cod_b2b)), '') AS cod_b2b,
                NULLIF(TRIM(r.local_nombre), '') AS local_nombre,
                NULLIF(TRIM(r.cliente), '') AS cliente,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(r.cliente_norm), ''), NULLIF(TRIM(r.cliente), ''), ''))) AS cliente_norm,
                NULLIF(TRIM(r.gestores), '') AS gestor_value,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(r.gestor_norm), ''), NULLIF(TRIM(r.gestores), ''), ''))) AS gestor_norm_value,
                NULLIF(TRIM(r.supervisor), '') AS supervisor_value,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(r.supervisor_norm), ''), NULLIF(TRIM(r.supervisor), ''), ''))) AS supervisor_norm_value,
                NULLIF(TRIM(r.rutero), '') AS rutero_value,
                NULLIF(TRIM(r.reponedor), '') AS reponedor_value,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(r.reponedor_norm), ''), NULLIF(TRIM(r.reponedor), ''), ''))) AS reponedor_norm_value,
                NULLIF(TRIM(r.jefe_operaciones), '') AS jefe_operaciones_value,
                UPPER(TRIM(COALESCE(NULLIF(TRIM(r.jefe_operaciones), ''), ''))) AS jefe_operaciones_norm_value,
                NULLIF(TRIM(r.modalidad), '') AS modalidad_value,
                COALESCE(r.veces_por_semana, 0) AS visitas_exigidas_semana,
                COALESCE(r.lunes, 0) AS lunes,
                COALESCE(r.martes, 0) AS martes,
                COALESCE(r.miercoles, 0) AS miercoles,
                COALESCE(r.jueves, 0) AS jueves,
                COALESCE(r.viernes, 0) AS viernes,
                COALESCE(r.sabado, 0) AS sabado,
                COALESCE(r.domingo, 0) AS domingo
            FROM {q_schema}.route_source_plan p
            JOIN cg_core.ruta_rutero_load_rows r
              ON r.ruta_batch_id = p.source_ruta_batch_id
            WHERE NULLIF(TRIM(COALESCE(r.cod_rt_norm, r.cod_rt)), '') IS NOT NULL
              AND NULLIF(TRIM(COALESCE(r.cliente_norm, r.cliente)), '') IS NOT NULL
        ),
        winning AS (
            SELECT week_start, cod_rt, cliente_norm, MAX(ruta_batch_id) AS ruta_batch_id
            FROM normalized
            GROUP BY week_start, cod_rt, cliente_norm
        ),
        aggregated AS (
            SELECT
                n.route_week_snapshot_version_id,
                n.week_start,
                EXTRACT(week FROM n.week_start)::integer AS week_iso,
                n.ruta_batch_id AS source_ruta_batch_id,
                n.cod_rt,
                MIN(n.cod_b2b) FILTER (WHERE n.cod_b2b IS NOT NULL) AS cod_b2b,
                MIN(n.local_nombre) FILTER (WHERE n.local_nombre IS NOT NULL) AS local,
                MIN(n.cliente) FILTER (WHERE n.cliente IS NOT NULL) AS cliente,
                n.cliente_norm,
                STRING_AGG(DISTINCT n.gestor_value, ' | ' ORDER BY n.gestor_value) FILTER (WHERE n.gestor_value IS NOT NULL) AS gestor,
                STRING_AGG(DISTINCT n.gestor_norm_value, ' | ' ORDER BY n.gestor_norm_value) FILTER (WHERE n.gestor_norm_value IS NOT NULL) AS gestor_norm,
                STRING_AGG(DISTINCT n.supervisor_value, ' | ' ORDER BY n.supervisor_value) FILTER (WHERE n.supervisor_value IS NOT NULL) AS supervisor,
                STRING_AGG(DISTINCT n.supervisor_norm_value, ' | ' ORDER BY n.supervisor_norm_value) FILTER (WHERE n.supervisor_norm_value IS NOT NULL) AS supervisor_norm,
                STRING_AGG(DISTINCT n.rutero_value, ' | ' ORDER BY n.rutero_value) FILTER (WHERE n.rutero_value IS NOT NULL) AS rutero,
                STRING_AGG(DISTINCT n.reponedor_value, ' | ' ORDER BY n.reponedor_value) FILTER (WHERE n.reponedor_value IS NOT NULL) AS reponedor,
                STRING_AGG(DISTINCT n.reponedor_norm_value, ' | ' ORDER BY n.reponedor_norm_value) FILTER (WHERE n.reponedor_norm_value IS NOT NULL) AS reponedor_norm,
                STRING_AGG(DISTINCT n.jefe_operaciones_value, ' | ' ORDER BY n.jefe_operaciones_value) FILTER (WHERE n.jefe_operaciones_value IS NOT NULL) AS jefe_operaciones,
                STRING_AGG(DISTINCT n.jefe_operaciones_norm_value, ' | ' ORDER BY n.jefe_operaciones_norm_value) FILTER (WHERE n.jefe_operaciones_norm_value IS NOT NULL) AS jefe_operaciones_norm,
                STRING_AGG(DISTINCT n.modalidad_value, ' | ' ORDER BY n.modalidad_value) FILTER (WHERE n.modalidad_value IS NOT NULL) AS modalidad,
                MAX(n.visitas_exigidas_semana)::integer AS frecuencia,
                MAX(n.lunes)::integer AS lunes,
                MAX(n.martes)::integer AS martes,
                MAX(n.miercoles)::integer AS miercoles,
                MAX(n.jueves)::integer AS jueves,
                MAX(n.viernes)::integer AS viernes,
                MAX(n.sabado)::integer AS sabado,
                MAX(n.domingo)::integer AS domingo,
                CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END::integer AS ruta_duplicada_flag,
                COUNT(*)::integer AS ruta_duplicada_rows
            FROM normalized n
            JOIN winning w USING (week_start, cod_rt, cliente_norm, ruta_batch_id)
            GROUP BY n.route_week_snapshot_version_id, n.week_start, n.ruta_batch_id, n.cod_rt, n.cliente_norm
        )
        SELECT
            *,
            'ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1'::text AS lineage_reason,
            md5(concat_ws(chr(31),
                route_week_snapshot_version_id, week_start::text, week_iso::text, source_ruta_batch_id::text,
                cod_rt, cod_b2b, local, cliente, cliente_norm, gestor, gestor_norm, supervisor, supervisor_norm,
                rutero, reponedor, reponedor_norm, jefe_operaciones, jefe_operaciones_norm, modalidad,
                frecuencia::text, lunes::text, martes::text, miercoles::text, jueves::text, viernes::text,
                sabado::text, domingo::text, ruta_duplicada_flag::text, ruta_duplicada_rows::text
            )) AS row_hash
        FROM aggregated
        """
    )
    cur.execute(f"CREATE INDEX ON {q_schema}.route_week_snapshot (week_start, cod_rt, cliente_norm)")
    cur.execute(f"DROP TABLE IF EXISTS {q_schema}.route_week_snapshot_manifest")
    cur.execute(
        f"""
        CREATE TABLE {q_schema}.route_week_snapshot_manifest AS
        SELECT
            r.week_start,
            COUNT(*)::integer AS rows,
            ARRAY_AGG(DISTINCT r.source_ruta_batch_id ORDER BY r.source_ruta_batch_id) AS source_ruta_batch_ids,
            MIN(r.route_week_snapshot_version_id) AS route_week_snapshot_version_id,
            md5(string_agg(r.row_hash, '' ORDER BY r.cod_rt, r.cliente_norm, r.source_ruta_batch_id)) AS surface_hash,
            MIN(p.expected_surface_hash) AS expected_c006_surface_hash
        FROM {q_schema}.route_week_snapshot r
        LEFT JOIN {q_schema}.route_source_plan p
          ON p.week_start = r.week_start
         AND p.source_ruta_batch_id = r.source_ruta_batch_id
        GROUP BY r.week_start
        ORDER BY r.week_start
        """
    )


def create_output_tables(cur: Any, schema: str) -> None:
    q_schema = schema_sql_name(schema)
    cur.execute(
        f"""
        DROP TABLE IF EXISTS {q_schema}.daily_canonical;
        CREATE TABLE {q_schema}.daily_canonical (
            run_no integer not null,
            semana_inicio date,
            fecha_visita date,
            cod_rt text,
            cod_b2b text,
            cliente text,
            cliente_norm text,
            local_nombre text,
            gestor text,
            gestor_norm text,
            rutero text,
            reponedor_scope text,
            reponedor_scope_norm text,
            supervisor text,
            jefe_operaciones text,
            modalidad text,
            semana_iso integer,
            fuente_ganadora text,
            fuentes_presentes text,
            tiene_kpione2 integer,
            tiene_power_app integer,
            tiene_kpione1 integer,
            power_app_fallback integer,
            kpione1_audit_only integer,
            useful_day integer,
            raw_evidence_count integer,
            same_source_multimark integer,
            multisource_overlap integer,
            kpione_rows_dia integer,
            kpione2_rows_dia integer,
            power_app_rows_dia integer,
            persona_conflicto_rows_dia integer,
            match_quality text,
            registro_fuera_cruce text
        );
        DROP TABLE IF EXISTS {q_schema}.weekly_canonical;
        CREATE TABLE {q_schema}.weekly_canonical (
            run_no integer not null,
            "COD_RT" text,
            "COD_B2B" text,
            "LOCAL" text,
            "CLIENTE" text,
            "GESTOR" text,
            "RUTERO" text,
            "REPONEDOR" text,
            "SUPERVISOR" text,
            "MODALIDAD" text,
            "SEMANA_INICIO" date,
            "SEMANA_ISO" integer,
            "LUNES_FLAG" integer,
            "MARTES_FLAG" integer,
            "MIERCOLES_FLAG" integer,
            "JUEVES_FLAG" integer,
            "VIERNES_FLAG" integer,
            "SABADO_FLAG" integer,
            "DOMINGO_FLAG" integer,
            "LUNES_PLAN" integer,
            "MARTES_PLAN" integer,
            "MIERCOLES_PLAN" integer,
            "JUEVES_PLAN" integer,
            "VIERNES_PLAN" integer,
            "SABADO_PLAN" integer,
            "DOMINGO_PLAN" integer,
            "VISITA" integer,
            "VISITA_REALIZADA" integer,
            "DIFERENCIA" integer,
            "ALERTA" text,
            "DIAS_KPIONE" integer,
            "DIAS_KPIONE2" integer,
            "DIAS_POWER_APP" integer,
            "DIAS_DOBLE_MARCAJE" integer,
            "DIAS_TRIPLE_MARCAJE" integer,
            "FUENTES_REPORTADAS_SEMANA" text,
            "PERSONA_CONFLICTO_ROWS" integer,
            "VISITA_REALIZADA_RAW" integer,
            "VISITA_REALIZADA_CAP" integer,
            "SOBRE_CUMPLIMIENTO" integer,
            "RUTA_DUPLICADA_FLAG" integer,
            "RUTA_DUPLICADA_ROWS" integer,
            "SEMANA_INICIO_KEY" date,
            "GESTOR_NORM_FILTER" text,
            "RUTERO_NORM_FILTER" text,
            "LOCAL_NORM_FILTER" text,
            "CLIENTE_NORM_FILTER" text,
            "ALERTA_NORM_FILTER" text,
            "GESTION_COMPARTIDA_FLAG_CALC" integer,
            "VISITAS_PENDIENTES_CALC" integer
        );
        DROP TABLE IF EXISTS {q_schema}.build_run_metrics;
        CREATE TABLE {q_schema}.build_run_metrics (
            run_no integer primary key,
            daily_rows integer,
            daily_duplicate_keys integer,
            daily_key_hash text,
            daily_business_semantic_hash text,
            daily_technical_full_row_hash text,
            weekly_rows integer,
            weekly_duplicate_keys integer,
            weekly_key_hash text,
            weekly_business_semantic_hash text,
            weekly_technical_full_row_hash text,
            internal_warnings jsonb not null default '[]'::jsonb
        );
        """
    )


def insert_daily_run(cur: Any, schema: str, run_no: int, context: BuildContext) -> None:
    q_schema = schema_sql_name(schema)
    cur.execute(
        f"""
        INSERT INTO {q_schema}.daily_canonical
        WITH raw AS (
            SELECT e.*
            FROM cg_core.v_cg_evidencia_unificada_v2 e
            JOIN {q_schema}.raw_lineage r
              ON r.source_key = e.fuente
             AND r.batch_id = e.batch_id
            WHERE e.fecha_visita BETWEEN %s::date AND %s::date
              AND COALESCE(e.visita_value, 0) > 0
        ),
        matched AS (
            SELECT
                rw.week_start AS semana_inicio,
                raw.fecha_visita,
                rw.cod_rt,
                rw.cod_b2b,
                rw.cliente,
                rw.cliente_norm,
                rw.local AS local_nombre,
                rw.gestor,
                rw.gestor_norm,
                rw.rutero,
                rw.reponedor AS reponedor_scope,
                rw.reponedor_norm AS reponedor_scope_norm,
                rw.supervisor,
                rw.jefe_operaciones,
                rw.modalidad,
                rw.week_iso AS semana_iso,
                raw.fuente,
                raw.persona_norm,
                raw.registro_fuera_cruce
            FROM raw
            JOIN {q_schema}.route_week_snapshot rw
              ON rw.week_start = raw.report_week_start
             AND rw.cod_rt = raw.cod_rt_candidate
             AND rw.cliente_norm = raw.cliente_norm
        ),
        agg AS (
            SELECT
                semana_inicio,
                fecha_visita,
                cod_rt, cod_b2b, cliente, cliente_norm, local_nombre,
                gestor, gestor_norm, rutero, reponedor_scope, reponedor_scope_norm,
                supervisor, jefe_operaciones, modalidad, semana_iso,
                COUNT(*) FILTER (WHERE fuente = 'KPIONE2')::integer AS kpione2_rows,
                COUNT(*) FILTER (WHERE fuente = 'POWER_APP')::integer AS power_rows,
                COUNT(*) FILTER (WHERE fuente = 'KPIONE')::integer AS kpione_rows,
                COUNT(*)::integer AS raw_count,
                COUNT(*) FILTER (
                    WHERE fuente = 'KPIONE2'
                      AND COALESCE(persona_norm, '') <> COALESCE(reponedor_scope_norm, '')
                )::integer AS kpione2_persona_conflict,
                BOOL_OR(registro_fuera_cruce <> 'N/A') AS outside
            FROM matched
            GROUP BY
                semana_inicio, fecha_visita, cod_rt, cod_b2b, cliente, cliente_norm, local_nombre,
                gestor, gestor_norm, rutero, reponedor_scope, reponedor_scope_norm,
                supervisor, jefe_operaciones, modalidad, semana_iso
        )
        SELECT
            %s::integer AS run_no,
            semana_inicio,
            fecha_visita,
            cod_rt, cod_b2b, cliente, cliente_norm, local_nombre,
            gestor, gestor_norm, rutero, reponedor_scope, reponedor_scope_norm,
            supervisor, jefe_operaciones, modalidad, semana_iso,
            CASE WHEN kpione2_rows > 0 THEN 'KPIONE2'
                 WHEN power_rows > 0 THEN 'POWER_APP'
                 ELSE NULL END AS fuente_ganadora,
            concat_ws('+',
                CASE WHEN kpione2_rows > 0 THEN 'KPIONE2' END,
                CASE WHEN power_rows > 0 THEN 'POWER_APP' END,
                CASE WHEN kpione_rows > 0 THEN 'KPIONE' END
            ) AS fuentes_presentes,
            CASE WHEN kpione2_rows > 0 THEN 1 ELSE 0 END AS tiene_kpione2,
            CASE WHEN power_rows > 0 THEN 1 ELSE 0 END AS tiene_power_app,
            CASE WHEN kpione_rows > 0 THEN 1 ELSE 0 END AS tiene_kpione1,
            CASE WHEN kpione2_rows = 0 AND power_rows > 0 THEN 1 ELSE 0 END AS power_app_fallback,
            CASE WHEN kpione_rows > 0 THEN 1 ELSE 0 END AS kpione1_audit_only,
            CASE WHEN kpione2_rows > 0 OR power_rows > 0 THEN 1 ELSE 0 END AS useful_day,
            raw_count AS raw_evidence_count,
            CASE WHEN GREATEST(kpione2_rows, power_rows, kpione_rows) > 1 THEN 1 ELSE 0 END AS same_source_multimark,
            CASE WHEN (
                CASE WHEN kpione2_rows > 0 THEN 1 ELSE 0 END
              + CASE WHEN power_rows > 0 THEN 1 ELSE 0 END
              + CASE WHEN kpione_rows > 0 THEN 1 ELSE 0 END
            ) > 1 THEN 1 ELSE 0 END AS multisource_overlap,
            kpione_rows AS kpione_rows_dia,
            kpione2_rows AS kpione2_rows_dia,
            power_rows AS power_app_rows_dia,
            kpione2_persona_conflict AS persona_conflicto_rows_dia,
            'week_cod_rt_cliente' AS match_quality,
            CASE WHEN outside THEN 'FUERA_CRUCE' ELSE 'N/A' END AS registro_fuera_cruce
        FROM agg
        """,
        (context.affected_date_start, context.affected_date_end, run_no),
    )


def insert_weekly_run(cur: Any, schema: str, run_no: int) -> None:
    q_schema = schema_sql_name(schema)
    cur.execute(
        f"""
        INSERT INTO {q_schema}.weekly_canonical
        WITH d AS (
            SELECT * FROM {q_schema}.daily_canonical WHERE run_no = %s
        ),
        agg AS (
            SELECT
                r.*,
                COUNT(d.*)::integer AS realized,
                COUNT(d.*) FILTER (WHERE EXTRACT(isodow FROM d.fecha_visita) = 1)::integer AS lunes_flag,
                COUNT(d.*) FILTER (WHERE EXTRACT(isodow FROM d.fecha_visita) = 2)::integer AS martes_flag,
                COUNT(d.*) FILTER (WHERE EXTRACT(isodow FROM d.fecha_visita) = 3)::integer AS miercoles_flag,
                COUNT(d.*) FILTER (WHERE EXTRACT(isodow FROM d.fecha_visita) = 4)::integer AS jueves_flag,
                COUNT(d.*) FILTER (WHERE EXTRACT(isodow FROM d.fecha_visita) = 5)::integer AS viernes_flag,
                COUNT(d.*) FILTER (WHERE EXTRACT(isodow FROM d.fecha_visita) = 6)::integer AS sabado_flag,
                COUNT(d.*) FILTER (WHERE EXTRACT(isodow FROM d.fecha_visita) = 7)::integer AS domingo_flag,
                COALESCE(SUM(d.tiene_kpione1), 0)::integer AS dias_kpione,
                COALESCE(SUM(d.tiene_kpione2), 0)::integer AS dias_kpione2,
                COALESCE(SUM(d.tiene_power_app), 0)::integer AS dias_power_app,
                COALESCE(SUM(CASE
                    WHEN array_length(string_to_array(d.fuentes_presentes, '+'), 1) = 2 THEN 1 ELSE 0 END), 0)::integer AS dias_doble_marcaje,
                COALESCE(SUM(CASE
                    WHEN array_length(string_to_array(d.fuentes_presentes, '+'), 1) >= 3 THEN 1 ELSE 0 END), 0)::integer AS dias_triple_marcaje,
                COALESCE(SUM(d.persona_conflicto_rows_dia), 0)::integer AS persona_conflicto_rows,
                COALESCE(SUM(d.useful_day), 0)::integer AS visita_realizada_raw,
                LEAST(COALESCE(SUM(d.useful_day), 0)::integer, COALESCE(r.frecuencia, 0)) AS visita_realizada_cap,
                GREATEST(COALESCE(SUM(d.useful_day), 0)::integer - COALESCE(r.frecuencia, 0), 0) AS sobre_cumplimiento,
                GREATEST(
                    COALESCE(r.frecuencia, 0) - LEAST(COALESCE(SUM(d.useful_day), 0)::integer, COALESCE(r.frecuencia, 0)),
                    0
                ) AS visitas_pendientes_calc,
                STRING_AGG(DISTINCT d.fuente_ganadora, '+' ORDER BY d.fuente_ganadora)
                    FILTER (WHERE d.fuente_ganadora IS NOT NULL) AS fuentes
            FROM {q_schema}.route_week_snapshot r
            LEFT JOIN d
              ON d.semana_inicio = r.week_start
             AND d.cod_rt = r.cod_rt
             AND d.cliente_norm = r.cliente_norm
            GROUP BY
                r.route_week_snapshot_version_id, r.week_start, r.week_iso, r.source_ruta_batch_id,
                r.cod_rt, r.cod_b2b, r.local, r.cliente, r.cliente_norm, r.gestor, r.gestor_norm,
                r.supervisor, r.supervisor_norm, r.rutero, r.reponedor, r.reponedor_norm,
                r.jefe_operaciones, r.jefe_operaciones_norm, r.modalidad, r.frecuencia, r.lunes,
                r.martes, r.miercoles, r.jueves, r.viernes, r.sabado, r.domingo,
                r.ruta_duplicada_flag, r.ruta_duplicada_rows, r.lineage_reason, r.row_hash
        )
        SELECT
            %s::integer AS run_no,
            cod_rt AS "COD_RT",
            cod_b2b AS "COD_B2B",
            local AS "LOCAL",
            cliente AS "CLIENTE",
            gestor AS "GESTOR",
            rutero AS "RUTERO",
            reponedor AS "REPONEDOR",
            supervisor AS "SUPERVISOR",
            modalidad AS "MODALIDAD",
            week_start AS "SEMANA_INICIO",
            week_iso AS "SEMANA_ISO",
            lunes_flag AS "LUNES_FLAG",
            martes_flag AS "MARTES_FLAG",
            miercoles_flag AS "MIERCOLES_FLAG",
            jueves_flag AS "JUEVES_FLAG",
            viernes_flag AS "VIERNES_FLAG",
            sabado_flag AS "SABADO_FLAG",
            domingo_flag AS "DOMINGO_FLAG",
            lunes AS "LUNES_PLAN",
            martes AS "MARTES_PLAN",
            miercoles AS "MIERCOLES_PLAN",
            jueves AS "JUEVES_PLAN",
            viernes AS "VIERNES_PLAN",
            sabado AS "SABADO_PLAN",
            domingo AS "DOMINGO_PLAN",
            frecuencia AS "VISITA",
            realized AS "VISITA_REALIZADA",
            realized - frecuencia AS "DIFERENCIA",
            CASE WHEN realized >= frecuencia THEN 'CUMPLE' ELSE 'INCUMPLE' END AS "ALERTA",
            dias_kpione AS "DIAS_KPIONE",
            dias_kpione2 AS "DIAS_KPIONE2",
            dias_power_app AS "DIAS_POWER_APP",
            dias_doble_marcaje AS "DIAS_DOBLE_MARCAJE",
            dias_triple_marcaje AS "DIAS_TRIPLE_MARCAJE",
            COALESCE(fuentes, '') AS "FUENTES_REPORTADAS_SEMANA",
            persona_conflicto_rows AS "PERSONA_CONFLICTO_ROWS",
            visita_realizada_raw AS "VISITA_REALIZADA_RAW",
            visita_realizada_cap AS "VISITA_REALIZADA_CAP",
            sobre_cumplimiento AS "SOBRE_CUMPLIMIENTO",
            ruta_duplicada_flag AS "RUTA_DUPLICADA_FLAG",
            ruta_duplicada_rows AS "RUTA_DUPLICADA_ROWS",
            week_start AS "SEMANA_INICIO_KEY",
            gestor_norm AS "GESTOR_NORM_FILTER",
            rutero AS "RUTERO_NORM_FILTER",
            UPPER(TRIM(COALESCE(local, ''))) AS "LOCAL_NORM_FILTER",
            cliente_norm AS "CLIENTE_NORM_FILTER",
            CASE WHEN realized >= frecuencia THEN 'CUMPLE' ELSE 'INCUMPLE' END AS "ALERTA_NORM_FILTER",
            CASE
                WHEN ruta_duplicada_flag = 1
                  OR ruta_duplicada_rows > 1
                  OR COALESCE(gestor, '') LIKE '%%|%%'
                  OR COALESCE(rutero, '') LIKE '%%|%%'
                THEN 1 ELSE 0
            END AS "GESTION_COMPARTIDA_FLAG_CALC",
            visitas_pendientes_calc AS "VISITAS_PENDIENTES_CALC"
        FROM agg
        """,
        (run_no, run_no),
    )


def insert_run_metrics(cur: Any, schema: str, run_no: int) -> dict[str, Any]:
    q_schema = schema_sql_name(schema)
    daily_table = f"{q_schema}.daily_canonical"
    weekly_table = f"{q_schema}.weekly_canonical"

    cur.execute(f"SELECT COUNT(*) FROM {daily_table} WHERE run_no = %s", (run_no,))
    daily_rows = int(cur.fetchone()[0])
    cur.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT fecha_visita, cod_rt, cliente_norm
            FROM {daily_table}
            WHERE run_no = %s
            GROUP BY 1,2,3
            HAVING COUNT(*) > 1
        ) d
        """,
        (run_no,),
    )
    daily_duplicate_keys = int(cur.fetchone()[0])
    cur.execute(f"SELECT COUNT(*) FROM {weekly_table} WHERE run_no = %s", (run_no,))
    weekly_rows = int(cur.fetchone()[0])
    cur.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT "SEMANA_INICIO", "COD_RT", "CLIENTE_NORM_FILTER"
            FROM {weekly_table}
            WHERE run_no = %s
            GROUP BY 1,2,3
            HAVING COUNT(*) > 1
        ) d
        """,
        (run_no,),
    )
    weekly_duplicate_keys = int(cur.fetchone()[0])

    hashes = {}
    for name, table, columns, order_columns in (
        ("daily_key_hash", daily_table, DAILY_KEY_COLUMNS, DAILY_KEY_COLUMNS),
        (
            "daily_business_semantic_hash",
            daily_table,
            DAILY_BUSINESS_SEMANTIC_COLUMNS,
            DAILY_KEY_COLUMNS,
        ),
        (
            "daily_technical_full_row_hash",
            daily_table,
            DAILY_TECHNICAL_FULL_ROW_COLUMNS,
            DAILY_KEY_COLUMNS,
        ),
        ("weekly_key_hash", weekly_table, WEEKLY_KEY_COLUMNS, WEEKLY_KEY_COLUMNS),
        (
            "weekly_business_semantic_hash",
            weekly_table,
            WEEKLY_BUSINESS_SEMANTIC_COLUMNS,
            WEEKLY_KEY_COLUMNS,
        ),
        (
            "weekly_technical_full_row_hash",
            weekly_table,
            WEEKLY_TECHNICAL_FULL_ROW_COLUMNS,
            WEEKLY_KEY_COLUMNS,
        ),
    ):
        cur.execute(_hash_sql(table, columns, order_columns, run_no))
        hashes[name] = cur.fetchone()[0]

    warnings = []
    if daily_duplicate_keys:
        warnings.append("daily_duplicate_keys")
    if weekly_duplicate_keys:
        warnings.append("weekly_duplicate_keys")

    row = {
        "run_no": run_no,
        "daily_rows": daily_rows,
        "daily_duplicate_keys": daily_duplicate_keys,
        "weekly_rows": weekly_rows,
        "weekly_duplicate_keys": weekly_duplicate_keys,
        **hashes,
        "internal_warnings": warnings,
    }
    cur.execute(
        f"""
        INSERT INTO {q_schema}.build_run_metrics (
            run_no,
            daily_rows,
            daily_duplicate_keys,
            daily_key_hash,
            daily_business_semantic_hash,
            daily_technical_full_row_hash,
            weekly_rows,
            weekly_duplicate_keys,
            weekly_key_hash,
            weekly_business_semantic_hash,
            weekly_technical_full_row_hash,
            internal_warnings
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        """,
        (
            run_no,
            row["daily_rows"],
            row["daily_duplicate_keys"],
            row["daily_key_hash"],
            row["daily_business_semantic_hash"],
            row["daily_technical_full_row_hash"],
            row["weekly_rows"],
            row["weekly_duplicate_keys"],
            row["weekly_key_hash"],
            row["weekly_business_semantic_hash"],
            row["weekly_technical_full_row_hash"],
            json.dumps(warnings),
        ),
    )
    return row


def compare_schema_outputs(cur: Any, schema: str, compare_schema: str) -> dict[str, Any]:
    q_schema = schema_sql_name(schema)
    q_compare = schema_sql_name(compare_schema)
    cur.execute(
        f"""
        WITH
        daily_left AS (SELECT * FROM {q_schema}.daily_canonical WHERE run_no = 1),
        daily_right AS (SELECT * FROM {q_compare}.daily_canonical WHERE run_no = 1),
        weekly_left AS (SELECT * FROM {q_schema}.weekly_canonical WHERE run_no = 1),
        weekly_right AS (SELECT * FROM {q_compare}.weekly_canonical WHERE run_no = 1)
        SELECT
            (SELECT COUNT(*) FROM (SELECT * FROM daily_left EXCEPT SELECT * FROM daily_right) x)::integer AS daily_left_minus_right,
            (SELECT COUNT(*) FROM (SELECT * FROM daily_right EXCEPT SELECT * FROM daily_left) x)::integer AS daily_right_minus_left,
            (SELECT COUNT(*) FROM (SELECT * FROM weekly_left EXCEPT SELECT * FROM weekly_right) x)::integer AS weekly_left_minus_right,
            (SELECT COUNT(*) FROM (SELECT * FROM weekly_right EXCEPT SELECT * FROM weekly_left) x)::integer AS weekly_right_minus_left
        """
    )
    row = cur.fetchone()
    result = {
        "compare_schema": compare_schema,
        "daily_left_minus_right": int(row[0]),
        "daily_right_minus_left": int(row[1]),
        "weekly_left_minus_right": int(row[2]),
        "weekly_right_minus_left": int(row[3]),
    }
    result["matches"] = all(value == 0 for key, value in result.items() if key != "compare_schema")
    return result


def fetch_metrics(cur: Any, schema: str) -> list[dict[str, Any]]:
    q_schema = schema_sql_name(schema)
    cur.execute(
        f"""
        SELECT run_no, daily_rows, daily_duplicate_keys, daily_key_hash,
               daily_business_semantic_hash, daily_technical_full_row_hash,
               weekly_rows, weekly_duplicate_keys, weekly_key_hash,
               weekly_business_semantic_hash, weekly_technical_full_row_hash,
               internal_warnings::text
        FROM {q_schema}.build_run_metrics
        ORDER BY run_no
        """
    )
    rows = []
    for row in cur.fetchall():
        rows.append(
            {
                "run_no": int(row[0]),
                "daily_rows": int(row[1]),
                "daily_duplicate_keys": int(row[2]),
                "daily_key_hash": row[3],
                "daily_business_semantic_hash": row[4],
                "daily_technical_full_row_hash": row[5],
                "weekly_rows": int(row[6]),
                "weekly_duplicate_keys": int(row[7]),
                "weekly_key_hash": row[8],
                "weekly_business_semantic_hash": row[9],
                "weekly_technical_full_row_hash": row[10],
                "internal_warnings": json.loads(row[11]),
            }
        )
    return rows


def fetch_route_manifest(cur: Any, schema: str) -> list[dict[str, Any]]:
    q_schema = schema_sql_name(schema)
    cur.execute(
        f"""
        SELECT week_start::text, rows, source_ruta_batch_ids::text,
               route_week_snapshot_version_id, surface_hash, expected_c006_surface_hash
        FROM {q_schema}.route_week_snapshot_manifest
        ORDER BY week_start
        """
    )
    return [
        {
            "week_start": row[0],
            "rows": int(row[1]),
            "source_ruta_batch_ids": row[2],
            "route_week_snapshot_version_id": row[3],
            "surface_hash": row[4],
            "expected_c006_surface_hash": row[5],
        }
        for row in cur.fetchall()
    ]


def selected_window_rows(cur: Any, schema: str, context: BuildContext) -> None:
    q_schema = schema_sql_name(schema)
    for item in context.raw_lineage:
        cur.execute(
            """
            SELECT COUNT(*)::integer
            FROM cg_core.v_cg_evidencia_unificada_v2
            WHERE fuente = %s
              AND batch_id = %s
              AND fecha_visita BETWEEN %s::date AND %s::date
              AND COALESCE(visita_value, 0) > 0
            """,
            (item.source_key, item.batch_id, context.affected_date_start, context.affected_date_end),
        )
        rows = int(cur.fetchone()[0])
        cur.execute(
            f"UPDATE {q_schema}.raw_lineage SET selected_window_rows = %s WHERE source_key = %s",
            (rows, item.source_key),
        )


def run_builder(
    *,
    dsn: str,
    context: BuildContext,
    schema: str,
    runs: int,
    replace_schema: bool,
    compare_schema: str | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "phase": PHASE,
        "schema": schema,
        "runs_requested": runs,
        "local_only_enforced": True,
        "supabase_writes": False,
        "warnings": [],
        "blockers": [],
    }
    with connect_local(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '10min'")
            create_schema(cur, schema, replace_schema)
            validate_raw_lineage(cur, context)
            create_base_tables(cur, schema, context)
            selected_window_rows(cur, schema, context)
            build_route_snapshot(cur, schema)
            create_output_tables(cur, schema)
            run_metrics = []
            for run_no in range(1, runs + 1):
                insert_daily_run(cur, schema, run_no, context)
                insert_weekly_run(cur, schema, run_no)
                run_metrics.append(insert_run_metrics(cur, schema, run_no))
            compare = compare_schema_outputs(cur, schema, compare_schema) if compare_schema else None
            result["route_manifest"] = fetch_route_manifest(cur, schema)
            result["metrics"] = fetch_metrics(cur, schema)
            result["compare"] = compare
            conn.commit()
    metrics = result["metrics"]
    result["deterministic"] = {
        "daily_rows_variants": len({row["daily_rows"] for row in metrics}),
        "daily_key_hash_variants": len({row["daily_key_hash"] for row in metrics}),
        "daily_business_semantic_hash_variants": len({row["daily_business_semantic_hash"] for row in metrics}),
        "daily_technical_full_row_hash_variants": len({row["daily_technical_full_row_hash"] for row in metrics}),
        "weekly_rows_variants": len({row["weekly_rows"] for row in metrics}),
        "weekly_key_hash_variants": len({row["weekly_key_hash"] for row in metrics}),
        "weekly_business_semantic_hash_variants": len({row["weekly_business_semantic_hash"] for row in metrics}),
        "weekly_technical_full_row_hash_variants": len({row["weekly_technical_full_row_hash"] for row in metrics}),
    }
    result["status"] = "ok"
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local-only canonical Control Gestion builder.")
    parser.add_argument("--context", required=True, type=Path, help="Build context JSON, for example C006 output.")
    parser.add_argument("--schema", required=True, help="Local PostgreSQL schema to write.")
    parser.add_argument("--runs", type=int, default=1, help="Number of repeat builds to materialize.")
    parser.add_argument("--replace-schema", action="store_true", help="Drop and recreate the target local schema.")
    parser.add_argument("--dsn-env", default="C007_LOCAL_PG_DSN", help="Environment variable containing local Postgres DSN.")
    parser.add_argument("--compare-schema", default="", help="Optional schema to compare run 1 outputs against.")
    parser.add_argument("--json-out", type=Path, default=None, help="Optional JSON output path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.runs < 1:
        parser.error("--runs must be >= 1")
    dsn = os.getenv(args.dsn_env, "")
    try:
        context = load_build_context(args.context)
        result = run_builder(
            dsn=dsn,
            context=context,
            schema=args.schema,
            runs=args.runs,
            replace_schema=args.replace_schema,
            compare_schema=args.compare_schema or None,
        )
        text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
        if args.json_out:
            args.json_out.write_text(text + "\n", encoding="utf-8")
        print(text)
        return 0
    except Exception as exc:
        blocked = {
            "phase": PHASE,
            "status": "blocked",
            "error": str(exc),
            "supabase_writes": False,
        }
        text = json.dumps(blocked, ensure_ascii=False, indent=2)
        if args.json_out:
            args.json_out.write_text(text + "\n", encoding="utf-8")
        print(text)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
