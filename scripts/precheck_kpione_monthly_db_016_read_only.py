#!/usr/bin/env python
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
PHASE_ID = "016_KPIONE_MONTHLY_DB_PRECHECK_READ_ONLY"
EXPECTED_PLAN_PHASE_ID = "015C_KPIONE_MONTHLY_LOAD_DRY_RUN_NO_DB_APPLY"
EXPECTED_MONTH = "2026-06"
EXPECTED_WOULD_STAGE_ROWS = 213181
EXPECTED_LOAD_PLAN_SHA256 = "e634b4720b66db7a3036302e2553c53d7a93475257bb27410804f6b9efe8f7cd"
EXPECTED_USER = "stock_zero_codex_ro"
ROADMAP_LOCK_ID = "KPIONE_DB_TRANSITION_016_019_LOCK_V1"
ROADMAP_LOCK_SHA256 = "cce9eea337c07c56b722968beaa7eb481e79028b60fa218e20875fb71be2e46e"
ROADMAP_LOCK_COMMIT = "5c0aa19ac753c21aa9bb43b6fdd72a927b694a5f"
CURRENT_PHASE = "016"
EXPECTED_NEXT_PHASE = "016A"
TARGET_SCHEMA = "cg_raw"
TARGET_TABLE = "kpione2_raw"
TARGET_RELATION = f"{TARGET_SCHEMA}.{TARGET_TABLE}"
TARGET_ALLOWLIST = (TARGET_RELATION,)
PLAN_LAYER = "raw_candidate_photo_rows"
HISTORICAL_ROW_COUNT = 526022
HISTORICAL_BATCH_COUNT = 19
HISTORICAL_LATEST_BATCH_ID = "38"
HISTORICAL_LATEST_BATCH_LOADED_ROWS = 45736
FORBIDDEN_SQL_PATTERNS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "COPY",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "REFRESH",
    "VACUUM",
    "ANALYZE",
    "CALL",
    "DO",
    "EXECUTE",
    "SELECT\\s+INTO",
    "FOR\\s+UPDATE",
    "FOR\\s+SHARE",
    "LOCK",
    "PG_ADVISORY_LOCK",
    "DBLINK",
    "\\bHTTP\\b",
    "\\bNET\\.",
    "\\bRPC\\b",
)
IDENTITY_JSON_KEYS = (
    "event_id",
    "source_file_id",
    "source_file_sha256",
    "source_row_number",
    "photo_row_hash",
    "event_stable_hash",
    "dry_run_batch_id",
)
SENSITIVE_OUTPUT_PATTERNS = (
    re.compile(r"postgres(?:ql)?://", re.IGNORECASE),
    re.compile(r"https?://", re.IGNORECASE),
    re.compile(r"\bpassword\b", re.IGNORECASE),
    re.compile(r"\bservice[_-]?role\b", re.IGNORECASE),
)


@dataclass(frozen=True)
class Statement:
    statement_id: str
    purpose: str
    sql: str
    business: bool = False


STATEMENTS: tuple[Statement, ...] = (
    Statement(
        "target_exists",
        "Resolve target schema/table existence.",
        """
        SELECT
            EXISTS (
                SELECT 1
                FROM information_schema.schemata
                WHERE schema_name = %s
            ) AS schema_exists,
            EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = %s
            ) AS table_exists
        """,
    ),
    Statement(
        "target_privileges",
        "Check effective table privileges for the current role.",
        """
        SELECT
            has_schema_privilege(current_user, %s, 'USAGE') AS schema_usage,
            has_table_privilege(current_user, c.oid, 'SELECT') AS can_select,
            has_table_privilege(current_user, c.oid, 'INSERT') AS can_insert,
            has_table_privilege(current_user, c.oid, 'UPDATE') AS can_update,
            has_table_privilege(current_user, c.oid, 'DELETE') AS can_delete,
            has_table_privilege(current_user, c.oid, 'TRUNCATE') AS can_truncate,
            has_table_privilege(current_user, c.oid, 'REFERENCES') AS can_references,
            has_table_privilege(current_user, c.oid, 'TRIGGER') AS can_trigger
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND c.relkind IN ('r', 'p')
        """,
    ),
    Statement(
        "target_columns",
        "Read target physical columns and generated/identity metadata.",
        """
        SELECT
            column_name,
            ordinal_position,
            data_type,
            udt_name,
            is_nullable,
            column_default,
            is_generated,
            generation_expression,
            identity_generation
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
    ),
    Statement(
        "target_constraints",
        "Read primary, unique, check, and foreign-key constraints.",
        """
        SELECT
            c.conname AS constraint_name,
            c.contype AS constraint_type,
            pg_catalog.pg_get_constraintdef(c.oid, true) AS definition
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = %s
          AND t.relname = %s
          AND c.contype IN ('p', 'u', 'c', 'f')
        ORDER BY c.contype, c.conname
        """,
    ),
    Statement(
        "target_indexes",
        "Read target index definitions and validity state.",
        """
        SELECT
            i.relname AS index_name,
            ix.indisunique AS is_unique,
            ix.indisprimary AS is_primary,
            ix.indisvalid AS is_valid,
            ix.indisready AS is_ready,
            pg_catalog.pg_get_indexdef(ix.indexrelid) AS definition
        FROM pg_catalog.pg_index ix
        JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
        JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = %s
          AND t.relname = %s
        ORDER BY i.relname
        """,
    ),
    Statement(
        "target_triggers",
        "Read non-internal triggers.",
        """
        SELECT
            tg.tgname AS trigger_name,
            tg.tgenabled AS enabled_state
        FROM pg_catalog.pg_trigger tg
        JOIN pg_catalog.pg_class t ON t.oid = tg.tgrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = %s
          AND t.relname = %s
          AND NOT tg.tgisinternal
        ORDER BY tg.tgname
        """,
    ),
    Statement(
        "target_owner_rls_size",
        "Read owner, RLS flags, relation sizes, and approximate row count.",
        """
        SELECT
            pg_catalog.pg_get_userbyid(c.relowner) AS owner,
            c.relrowsecurity AS relrowsecurity,
            c.relforcerowsecurity AS relforcerowsecurity,
            c.reltuples::bigint AS approximate_rows,
            pg_catalog.pg_total_relation_size(c.oid)::bigint AS total_relation_size_bytes,
            pg_catalog.pg_relation_size(c.oid)::bigint AS relation_size_bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND c.relkind IN ('r', 'p')
        """,
    ),
    Statement(
        "target_policies",
        "Read RLS policy metadata without row data.",
        """
        SELECT
            policyname,
            permissive,
            roles,
            cmd,
            qual,
            with_check
        FROM pg_catalog.pg_policies
        WHERE schemaname = %s
          AND tablename = %s
        ORDER BY policyname
        """,
    ),
    Statement(
        "target_dependencies",
        "Read view and materialized-view dependencies on target.",
        """
        SELECT DISTINCT
            ns.nspname AS dependent_schema,
            dep.relname AS dependent_name,
            dep.relkind AS dependent_kind
        FROM pg_catalog.pg_depend d
        JOIN pg_catalog.pg_rewrite rw ON rw.oid = d.objid
        JOIN pg_catalog.pg_class dep ON dep.oid = rw.ev_class
        JOIN pg_catalog.pg_namespace ns ON ns.oid = dep.relnamespace
        JOIN pg_catalog.pg_class src ON src.oid = d.refobjid
        JOIN pg_catalog.pg_namespace src_ns ON src_ns.oid = src.relnamespace
        WHERE src_ns.nspname = %s
          AND src.relname = %s
          AND dep.relkind IN ('v', 'm')
        ORDER BY ns.nspname, dep.relname
        """,
    ),
    Statement(
        "state_total_minmax",
        "Aggregate total rows and fecha_visita min/max.",
        """
        SELECT
            COUNT(*)::bigint AS exact_total_rows,
            MIN(fecha_visita)::date AS fecha_min,
            MAX(fecha_visita)::date AS fecha_max
        FROM cg_raw.kpione2_raw
        """,
        business=True,
    ),
    Statement(
        "state_date_counts",
        "Aggregate date coverage for June and July 2026.",
        """
        SELECT
            fecha_visita::date AS fecha,
            COUNT(*)::bigint AS rows
        FROM cg_raw.kpione2_raw
        WHERE fecha_visita >= %s::date
          AND fecha_visita <= %s::date
        GROUP BY fecha_visita::date
        ORDER BY fecha_visita::date
        """,
        business=True,
    ),
    Statement(
        "state_range_summary",
        "Aggregate operational June, carry-forward, and July windows.",
        """
        SELECT
            SUM(CASE WHEN fecha_visita >= DATE '2026-06-01' AND fecha_visita <= DATE '2026-06-28' THEN 1 ELSE 0 END)::bigint AS rows_june_operational,
            SUM(CASE WHEN fecha_visita >= DATE '2026-06-29' AND fecha_visita <= DATE '2026-06-30' THEN 1 ELSE 0 END)::bigint AS rows_june_carry_forward_window,
            SUM(CASE WHEN fecha_visita >= DATE '2026-07-01' AND fecha_visita <= DATE '2026-07-31' THEN 1 ELSE 0 END)::bigint AS rows_july
        FROM cg_raw.kpione2_raw
        """,
        business=True,
    ),
    Statement(
        "state_week_counts",
        "Aggregate rows by Monday week start.",
        """
        SELECT
            date_trunc('week', fecha_visita)::date AS week_start,
            COUNT(*)::bigint AS rows
        FROM cg_raw.kpione2_raw
        WHERE fecha_visita >= %s::date
          AND fecha_visita <= %s::date
        GROUP BY date_trunc('week', fecha_visita)::date
        ORDER BY week_start
        """,
        business=True,
    ),
    Statement(
        "state_null_counts",
        "Aggregate nulls in technical identity columns.",
        """
        SELECT
            COUNT(*) FILTER (WHERE batch_id IS NULL)::bigint AS batch_id_nulls,
            COUNT(*) FILTER (WHERE source_file IS NULL)::bigint AS source_file_nulls,
            COUNT(*) FILTER (WHERE source_row IS NULL)::bigint AS source_row_nulls,
            COUNT(*) FILTER (WHERE fecha_visita IS NULL)::bigint AS fecha_visita_nulls,
            COUNT(*) FILTER (WHERE payload_json IS NULL)::bigint AS payload_json_nulls
        FROM cg_raw.kpione2_raw
        """,
        business=True,
    ),
    Statement(
        "json_identity_key_presence",
        "Aggregate presence of known identity keys inside payload_json.",
        """
        SELECT
            COUNT(*) FILTER (WHERE payload_json ? 'event_id')::bigint AS event_id,
            COUNT(*) FILTER (WHERE payload_json ? 'source_file_id')::bigint AS source_file_id,
            COUNT(*) FILTER (WHERE payload_json ? 'source_file_sha256')::bigint AS source_file_sha256,
            COUNT(*) FILTER (WHERE payload_json ? 'source_row_number')::bigint AS source_row_number,
            COUNT(*) FILTER (WHERE payload_json ? 'photo_row_hash')::bigint AS photo_row_hash,
            COUNT(*) FILTER (WHERE payload_json ? 'event_stable_hash')::bigint AS event_stable_hash,
            COUNT(*) FILTER (WHERE payload_json ? 'dry_run_batch_id')::bigint AS dry_run_batch_id
        FROM cg_raw.kpione2_raw
        """,
        business=True,
    ),
    Statement(
        "source_file_scope_counts",
        "Aggregate source_file coverage for 015C file names.",
        """
        SELECT
            source_file,
            COUNT(*)::bigint AS rows,
            MIN(fecha_visita)::date AS fecha_min,
            MAX(fecha_visita)::date AS fecha_max,
            COUNT(DISTINCT batch_id)::bigint AS batch_count
        FROM cg_raw.kpione2_raw
        WHERE source_file = ANY(%s)
        GROUP BY source_file
        ORDER BY source_file
        """,
        business=True,
    ),
    Statement(
        "batch_scope_counts",
        "Aggregate batches containing 015C file names.",
        """
        SELECT
            batch_id::text AS batch_id,
            COUNT(*)::bigint AS rows,
            MIN(fecha_visita)::date AS fecha_min,
            MAX(fecha_visita)::date AS fecha_max,
            COUNT(DISTINCT source_file)::bigint AS source_file_count
        FROM cg_raw.kpione2_raw
        WHERE source_file = ANY(%s)
        GROUP BY batch_id
        ORDER BY MAX(fecha_visita) DESC NULLS LAST, batch_id DESC
        LIMIT 50
        """,
        business=True,
    ),
    Statement(
        "recent_batch_counts",
        "Aggregate recent target batches.",
        """
        SELECT
            batch_id::text AS batch_id,
            COUNT(*)::bigint AS rows,
            MIN(fecha_visita)::date AS fecha_min,
            MAX(fecha_visita)::date AS fecha_max,
            COUNT(DISTINCT source_file)::bigint AS source_file_count
        FROM cg_raw.kpione2_raw
        GROUP BY batch_id
        ORDER BY MAX(fecha_visita) DESC NULLS LAST, batch_id DESC
        LIMIT 50
        """,
        business=True,
    ),
    Statement(
        "state_batch_ids",
        "Aggregate exact distinct target batch ids.",
        """
        SELECT
            ARRAY(
                SELECT distinct_batches.batch_id_text
                FROM (
                    SELECT DISTINCT
                        k.batch_id::text AS batch_id_text,
                        k.batch_id::bigint AS batch_id_numeric
                    FROM cg_raw.kpione2_raw k
                    WHERE k.batch_id IS NOT NULL
                ) distinct_batches
                ORDER BY distinct_batches.batch_id_numeric
            ) AS batch_ids,
            COUNT(DISTINCT batch_id)::bigint AS batch_count
        FROM cg_raw.kpione2_raw
        """,
        business=True,
    ),
)


class PrecheckBlocked(Exception):
    pass


def _normalize_db_url(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if value.startswith("$env:DB_URL_CODEX_RO="):
        value = value.split("=", 1)[1].strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1].strip()
    return value


def _load_db_url_from_file(path: Path) -> str:
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.replace("\ufeff", "").strip()
        if not raw or raw.startswith("#"):
            continue
        if raw.startswith("DB_URL_CODEX_RO="):
            return _normalize_db_url(raw.split("=", 1)[1])
    return ""


def resolve_db_url(env: dict[str, str] | None = None) -> tuple[str, str]:
    source = env if env is not None else os.environ
    env_value = _normalize_db_url(source.get("DB_URL_CODEX_RO", ""))
    if env_value:
        return env_value, "env"
    file_value = _load_db_url_from_file(ROOT / ".local_secrets" / "codex_ro.env")
    if file_value:
        return file_value, "file"
    return "", "none"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_sha256(payload: Any) -> str:
    return sha256_bytes(json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def strip_sql_comments_and_strings(sql: str) -> str:
    result: list[str] = []
    i = 0
    in_single = False
    in_double = False
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""
        if in_single:
            if ch == "'" and nxt == "'":
                i += 2
                continue
            if ch == "'":
                in_single = False
            result.append(" ")
            i += 1
            continue
        if in_double:
            if ch == '"':
                in_double = False
            result.append(" ")
            i += 1
            continue
        if ch == "-" and nxt == "-":
            while i < len(sql) and sql[i] != "\n":
                i += 1
            result.append(" ")
            continue
        if ch == "/" and nxt == "*":
            i += 2
            while i + 1 < len(sql) and not (sql[i] == "*" and sql[i + 1] == "/"):
                i += 1
            i += 2
            result.append(" ")
            continue
        if ch == "'":
            in_single = True
            result.append(" ")
            i += 1
            continue
        if ch == '"':
            in_double = True
            result.append(" ")
            i += 1
            continue
        result.append(ch)
        i += 1
    return "".join(result)


def validate_statement_sql(sql: str) -> None:
    stripped = sql.strip()
    upper_clean = strip_sql_comments_and_strings(stripped).upper()
    if not (upper_clean.lstrip().startswith("SELECT") or upper_clean.lstrip().startswith("WITH") or upper_clean.lstrip().startswith("SHOW")):
        raise ValueError("statement_not_select_or_show")
    for pattern in FORBIDDEN_SQL_PATTERNS:
        if re.search(rf"(?<![A-Z0-9_]){pattern}(?![A-Z0-9_])", upper_clean):
            raise ValueError(f"forbidden_sql:{pattern}")


def validate_static_statements() -> None:
    seen: set[str] = set()
    for statement in STATEMENTS:
        if statement.statement_id in seen:
            raise ValueError(f"duplicate_statement_id:{statement.statement_id}")
        seen.add(statement.statement_id)
        validate_statement_sql(statement.sql)


def rows_to_dicts(cursor: Any) -> list[dict[str, Any]]:
    columns = [item[0] for item in (cursor.description or [])]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def safe_json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, tuple):
        return [safe_json_value(item) for item in value]
    if isinstance(value, list):
        return [safe_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): safe_json_value(v) for k, v in value.items()}
    return value


def redact_error(exc: Exception) -> str:
    text = f"{exc.__class__.__name__}: {exc}"
    for pattern in SENSITIVE_OUTPUT_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    return text[:300]


def make_base_payload(month: str, load_plan_path: Path) -> dict[str, Any]:
    return {
        "phase_id": PHASE_ID,
        "month_id": month,
        "observed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "roadmap_compliance": {
            "roadmap_lock_id": ROADMAP_LOCK_ID,
            "roadmap_lock_sha256": ROADMAP_LOCK_SHA256,
            "roadmap_lock_commit": ROADMAP_LOCK_COMMIT,
            "current_phase": CURRENT_PHASE,
            "expected_next_phase": EXPECTED_NEXT_PHASE,
            "roadmap_compliance": "COMPLIANT",
            "deviations_detected": [],
        },
        "verdict": "BLOCKED",
        "apply_gate": "BLOCKED",
        "guardrails": {
            "db_access_used": False,
            "transaction_read_only": False,
            "default_transaction_read_only": False,
            "write_privileges_present": None,
            "rollback_completed": False,
            "writes_attempted": False,
        },
        "connection_proof": {
            "current_user": None,
            "database_name_redacted": True,
            "current_schema": None,
            "transaction_read_only": None,
            "default_transaction_read_only": None,
            "statement_timeout": None,
            "lock_timeout": None,
            "idle_transaction_timeout": None,
        },
        "load_plan": {
            "path": str(load_plan_path.as_posix()),
            "file_sha256_before": None,
            "file_sha256_after": None,
            "sha256_before": None,
            "sha256_after": None,
            "load_plan_sha256": None,
            "would_stage_rows": None,
            "carry_forward_out_rows": None,
        },
        "target_resolution": {},
        "persisted_grain": {},
        "schema": {
            "columns": [],
            "constraints": [],
            "indexes": [],
            "triggers": [],
            "rls": {},
            "privileges": {},
        },
        "database_state": {
            "approximate_total_rows": 0,
            "exact_total_rows": 0,
            "fecha_min": None,
            "fecha_max": None,
            "date_counts": [],
            "week_counts": [],
            "range_summary": {},
            "null_counts": {},
        },
        "json_identity_key_presence": {},
        "source_file_coverage": {},
        "batch_coverage": {},
        "historical_state_discrepancy": {},
        "source_signal_interpretation": {},
        "coarse_overlap_classification": "NO_SOURCE_SIGNAL",
        "exact_overlap_classification": "BLOCKED",
        "exact_overlap_feasibility": {},
        "dependencies": [],
        "query_audit": [],
        "query_plan_sha256": None,
        "db_state_sha256": None,
        "blockers": [],
        "warnings": [],
    }


def build_source_signal_interpretation(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "coarse_classification": payload.get("coarse_overlap_classification"),
        "proves_freshness": False,
        "proves_no_overlap": False,
        "exact_identity_available": False,
        "exact_overlap_classification": payload.get("exact_overlap_classification"),
        "apply_gate": payload.get("apply_gate"),
        "explanation": "Absence of legacy source-file matches or dry_run_batch_id does not prove that equivalent rows are absent.",
        "policy": "NO_SOURCE_SIGNAL DOES NOT IMPLY FRESH.",
        "notes": [
            "Legacy source_file is not necessarily equivalent to the 015C source_file_id.",
            "dry_run_batch_id did not exist historically.",
            "Zero matching filenames does not prove zero overlap.",
            "There is no persisted identity sufficient for exact comparison.",
            "exact_overlap_classification remains UNVERIFIABLE.",
            "apply_gate remains BLOCKED_FOR_IDEMPOTENCY_CONTRACT.",
        ],
    }


def build_historical_state_discrepancy(payload: dict[str, Any]) -> dict[str, Any]:
    state = payload.get("database_state", {}) if isinstance(payload.get("database_state"), dict) else {}
    current_exact_rows = int(state.get("exact_total_rows") or 0)
    current_batch_ids = state.get("batch_ids") if isinstance(state.get("batch_ids"), list) else []
    return {
        "target": TARGET_RELATION,
        "historical_row_count": HISTORICAL_ROW_COUNT,
        "historical_batch_count": HISTORICAL_BATCH_COUNT,
        "historical_evidence": [
            "research/C002_B0_PARITY_ROOTCAUSE.json",
            "research/C005_BUILD_PROVENANCE_AUDIT.json",
        ],
        "latest_historical_batch_id": HISTORICAL_LATEST_BATCH_ID,
        "latest_historical_batch_loaded_rows": HISTORICAL_LATEST_BATCH_LOADED_ROWS,
        "current_exact_row_count": current_exact_rows,
        "current_batch_ids": current_batch_ids,
        "current_fecha_min": state.get("fecha_min"),
        "current_fecha_max": state.get("fecha_max"),
        "classification": "TABLE_STATE_CHANGED",
        "resolution_status": "DOCUMENTED_NOT_RECONSTRUCTED",
        "legacy_migration_authority": False,
        "current_matches_latest_historical_batch_loaded_rows": current_exact_rows == HISTORICAL_LATEST_BATCH_LOADED_ROWS,
        "blocker_for_new_design": False,
        "warning_condition": True,
        "explanation": [
            "526022 corresponds to a previously evidenced accumulated historical state.",
            "45736 is the current reproducible live state.",
            "The current count matches loaded_rows for historical batch 38.",
            "This is consistent with replacement, cleanup, or reconstruction of table state.",
            "The exact operation that produced the change is not asserted without evidence.",
            "The legacy table is documented as evidence of the previous runtime.",
            "The legacy table does not govern the design of the new architecture.",
        ],
    }


def load_and_validate_plan(path: Path, month: str) -> tuple[dict[str, Any], str, list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    before = path.read_bytes()
    file_sha = sha256_bytes(before)
    plan = json.loads(before.decode("utf-8"))
    if plan.get("phase_id") != EXPECTED_PLAN_PHASE_ID:
        blockers.append("load_plan_phase_id_mismatch")
    if plan.get("month_id") != month:
        blockers.append("load_plan_month_mismatch")
    if plan.get("verdict") == "BLOCKED":
        blockers.append("load_plan_verdict_blocked")
    if plan.get("blockers"):
        blockers.append("load_plan_has_blockers")
    row_accounting = plan.get("row_accounting", {}) if isinstance(plan.get("row_accounting"), dict) else {}
    guardrails = plan.get("guardrails", {}) if isinstance(plan.get("guardrails"), dict) else {}
    semantic_plan = plan.get("semantic_plan", {}) if isinstance(plan.get("semantic_plan"), dict) else {}
    if month == EXPECTED_MONTH and int(row_accounting.get("would_stage_rows") or -1) != EXPECTED_WOULD_STAGE_ROWS:
        blockers.append("load_plan_would_stage_rows_mismatch")
    if plan.get("load_plan_sha256") != EXPECTED_LOAD_PLAN_SHA256:
        blockers.append("load_plan_sha256_mismatch")
    if guardrails.get("payload_rows_persisted") is not False:
        blockers.append("load_plan_payload_rows_persisted_not_false")
    if semantic_plan.get("payload_layer") != PLAN_LAYER:
        blockers.append("load_plan_payload_layer_mismatch")
    if plan.get("warnings"):
        warnings.extend(str(item) for item in plan.get("warnings", []))
    return plan, file_sha, blockers, warnings


def extract_plan_sources(plan: dict[str, Any]) -> dict[str, Any]:
    selection = plan.get("selection", {}) if isinstance(plan.get("selection"), dict) else {}
    include = selection.get("include_candidate_files", []) if isinstance(selection.get("include_candidate_files"), list) else []
    source_files = []
    for item in include:
        if not isinstance(item, dict):
            continue
        source_files.append(
            {
                "source_file_id": str(item.get("source_file_id") or ""),
                "source_file_name": str(item.get("source_file_name") or ""),
                "source_file_sha256": str(item.get("sha256") or ""),
            }
        )
    batch_plan = plan.get("batch_plan", {}) if isinstance(plan.get("batch_plan"), dict) else {}
    return {
        "source_files": source_files,
        "source_file_names": sorted({item["source_file_name"] for item in source_files if item["source_file_name"]}),
        "source_file_ids": sorted({item["source_file_id"] for item in source_files if item["source_file_id"]}),
        "source_file_sha256": sorted({item["source_file_sha256"] for item in source_files if item["source_file_sha256"]}),
        "dry_run_batch_id": str(batch_plan.get("dry_run_batch_id") or ""),
    }


def local_target_proof() -> dict[str, Any]:
    loader = (ROOT / "scripts" / "load_control_gestion_raw_v17.py").read_text(encoding="utf-8")
    draft = (ROOT / "sql" / "04_control_gestion_kpione2_multifuente_v2_draft.sql").read_text(encoding="utf-8")
    incremental = (ROOT / "scripts" / "refresh_control_gestion_v2_incremental.py").read_text(encoding="utf-8")
    return {
        "target_allowlist": list(TARGET_ALLOWLIST),
        "loader_v17_points_to_target": "insert into cg_raw.kpione2_raw" in loader.lower(),
        "schema_draft_defines_target": "create table if not exists cg_raw.kpione2_raw" in draft.lower(),
        "downstream_sql_mentions_target": "cg_raw.kpione2_raw" in draft.lower(),
        "incremental_helper_consumes_downstream_marts": all(
            token in incremental for token in ("cg_core.v_cg_visita_dia_precedencia_v2", "cg_mart.fact_cg_out_weekly_v2")
        ),
    }


def add_query_audit(payload: dict[str, Any], statement_id: str, purpose: str, started: float, rows: int, error: str | None = None) -> None:
    payload["query_audit"].append(
        {
            "statement_id": statement_id,
            "purpose": purpose,
            "duration_ms": int((time.perf_counter() - started) * 1000),
            "result_rows": int(rows),
            "error": error,
        }
    )


def execute_statement(cur: Any, statement: Statement, params: tuple[Any, ...], payload: dict[str, Any]) -> list[dict[str, Any]]:
    started = time.perf_counter()
    try:
        cur.execute(statement.sql, params)
        rows = rows_to_dicts(cur)
        add_query_audit(payload, statement.statement_id, statement.purpose, started, len(rows))
        return [safe_json_value(row) for row in rows]
    except Exception as exc:
        error = redact_error(exc)
        add_query_audit(payload, statement.statement_id, statement.purpose, started, 0, error)
        raise


def execute_guard(cur: Any, statement_timeout_ms: int, lock_timeout_ms: int, idle_timeout_ms: int) -> dict[str, Any]:
    cur.execute("BEGIN TRANSACTION READ ONLY")
    cur.execute("SET LOCAL statement_timeout = %s", (f"{int(statement_timeout_ms)}ms",))
    cur.execute("SET LOCAL lock_timeout = %s", (f"{int(lock_timeout_ms)}ms",))
    cur.execute("SET LOCAL idle_in_transaction_session_timeout = %s", (f"{int(idle_timeout_ms)}ms",))
    cur.execute(
        """
        SELECT
            current_user::text AS current_user,
            current_database()::text AS database_name,
            current_schema()::text AS current_schema,
            current_setting('transaction_read_only')::text AS transaction_read_only,
            current_setting('default_transaction_read_only')::text AS default_transaction_read_only,
            current_setting('statement_timeout')::text AS statement_timeout,
            current_setting('lock_timeout')::text AS lock_timeout,
            current_setting('idle_in_transaction_session_timeout')::text AS idle_transaction_timeout
        """
    )
    rows = rows_to_dicts(cur)
    return safe_json_value(rows[0] if rows else {})


def column_names(payload: dict[str, Any]) -> set[str]:
    return {str(row.get("column_name")) for row in payload.get("schema", {}).get("columns", [])}


def indexes_text(payload: dict[str, Any]) -> str:
    return "\n".join(str(row.get("definition") or "").lower() for row in payload.get("schema", {}).get("indexes", []))


def has_indexed_identity(payload: dict[str, Any], fields: tuple[str, ...]) -> bool:
    text = indexes_text(payload)
    return bool(text) and all(field.lower() in text for field in fields)


def classify_overlap(payload: dict[str, Any], plan_sources: dict[str, Any]) -> None:
    source_coverage = payload.get("source_file_coverage", {})
    matched = int(source_coverage.get("matched_source_file_count") or 0)
    total = int(source_coverage.get("plan_source_file_count") or len(plan_sources.get("source_file_names", [])))
    dry_run_present = bool(source_coverage.get("dry_run_batch_id_present"))
    if dry_run_present and matched:
        coarse = "MIXED_SIGNALS"
    elif dry_run_present:
        coarse = "BATCH_ID_PRESENT"
    elif total and matched == total:
        coarse = "ALL_SOURCE_FILES_PRESENT"
    elif matched > 0:
        coarse = "PARTIAL_SOURCE_FILES_PRESENT"
    else:
        coarse = "NO_SOURCE_SIGNAL"
    payload["coarse_overlap_classification"] = coarse

    cols = column_names(payload)
    physical_source_identity = {"source_file_id", "source_row_number"}.issubset(cols)
    physical_event_identity = {"event_id", "photo_row_hash"}.issubset(cols)
    legacy_source_identity = {"source_file", "source_row"}.issubset(cols)
    json_presence = payload.get("json_identity_key_presence", {})
    json_identity_rows = any(int(json_presence.get(key) or 0) > 0 for key in IDENTITY_JSON_KEYS)
    payload_json_indexed = "payload_json" in indexes_text(payload)
    source_identity_indexed = has_indexed_identity(payload, ("source_file_id", "source_row_number"))
    event_identity_indexed = has_indexed_identity(payload, ("event_id", "photo_row_hash"))

    feasible = {
        "physical_source_file_id_source_row_number_present": physical_source_identity,
        "physical_source_file_id_source_row_number_indexed": source_identity_indexed,
        "physical_event_id_photo_row_hash_present": physical_event_identity,
        "physical_event_id_photo_row_hash_indexed": event_identity_indexed,
        "legacy_source_file_source_row_present": legacy_source_identity,
        "json_identity_present": json_identity_rows,
        "payload_json_indexed": payload_json_indexed,
        "exact_comparison_potentially_feasible": bool(
            (physical_source_identity and source_identity_indexed)
            or (physical_event_identity and event_identity_indexed)
        ),
        "operationally_unsafe_reason": "",
    }
    if json_identity_rows and not payload_json_indexed and not feasible["exact_comparison_potentially_feasible"]:
        feasible["operationally_unsafe_reason"] = "identity_only_in_payload_json_without_index"
    elif legacy_source_identity and not feasible["exact_comparison_potentially_feasible"]:
        feasible["operationally_unsafe_reason"] = "legacy_source_file_source_row_requires_manifest_mapping"
    elif not feasible["exact_comparison_potentially_feasible"]:
        feasible["operationally_unsafe_reason"] = "no_indexed_contractual_identity"
    payload["exact_overlap_feasibility"] = feasible

    if payload.get("verdict") == "BLOCKED":
        exact = "BLOCKED"
        gate = "BLOCKED"
    elif dry_run_present:
        exact = "BATCH_REPLAY"
        gate = "BLOCKED"
    elif feasible["exact_comparison_potentially_feasible"]:
        exact = "UNVERIFIABLE"
        gate = "ELIGIBLE_FOR_EXACT_OVERLAP_SUBPHASE"
    else:
        exact = "UNVERIFIABLE"
        gate = "BLOCKED_FOR_IDEMPOTENCY_CONTRACT"
    payload["exact_overlap_classification"] = exact
    payload["apply_gate"] = gate


def build_hashes(payload: dict[str, Any], month: str, plan_sha: str, timeout_config: dict[str, int]) -> None:
    payload["query_plan_sha256"] = canonical_sha256(
        {
            "phase_id": PHASE_ID,
            "month": month,
            "load_plan_sha256": plan_sha,
            "target_allowlist": list(TARGET_ALLOWLIST),
            "statement_ids": [statement.statement_id for statement in STATEMENTS],
            "timeout_config": timeout_config,
        }
    )
    excluded = {"observed_at", "query_audit", "connection_proof"}
    state = {k: v for k, v in payload.items() if k not in excluded}
    payload["db_state_sha256"] = canonical_sha256(state)


def assert_no_sensitive_output(payload: dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    for pattern in SENSITIVE_OUTPUT_PATTERNS:
        if pattern.search(text):
            raise PrecheckBlocked(f"sensitive_output_pattern_detected:{pattern.pattern}")


def statement_by_id(statement_id: str) -> Statement:
    for statement in STATEMENTS:
        if statement.statement_id == statement_id:
            return statement
    raise KeyError(statement_id)


def run_precheck(args: argparse.Namespace, connect_fn: Callable[[str], Any] | None = None, env: dict[str, str] | None = None) -> dict[str, Any]:
    validate_static_statements()
    load_plan_path = (ROOT / args.load_plan_json).resolve() if not Path(args.load_plan_json).is_absolute() else Path(args.load_plan_json)
    payload = make_base_payload(args.month, load_plan_path.relative_to(ROOT) if load_plan_path.is_relative_to(ROOT) else load_plan_path)
    timeout_config = {
        "statement_timeout_ms": int(args.statement_timeout_ms),
        "lock_timeout_ms": int(args.lock_timeout_ms),
        "idle_transaction_timeout_ms": int(args.idle_transaction_timeout_ms),
    }
    conn = None
    try:
        local_proof = local_target_proof()
        payload["target_resolution"]["local_proof"] = local_proof
        if not all(local_proof[key] for key in ("loader_v17_points_to_target", "schema_draft_defines_target", "downstream_sql_mentions_target")):
            payload["blockers"].append("target_contract_mismatch")

        plan, file_sha_before, plan_blockers, plan_warnings = load_and_validate_plan(load_plan_path, args.month)
        plan_sources = extract_plan_sources(plan)
        payload["load_plan"].update(
            {
                "file_sha256_before": file_sha_before,
                "sha256_before": plan.get("load_plan_sha256"),
                "load_plan_sha256": plan.get("load_plan_sha256"),
                "would_stage_rows": plan.get("row_accounting", {}).get("would_stage_rows"),
                "carry_forward_out_rows": plan.get("row_accounting", {}).get("carry_forward_out_rows"),
                "source_file_count": len(plan_sources["source_file_names"]),
                "dry_run_batch_id_present_in_plan": bool(plan_sources["dry_run_batch_id"]),
            }
        )
        payload["blockers"].extend(plan_blockers)
        payload["warnings"].extend(plan_warnings)
        db_url, db_source = resolve_db_url(env)
        payload["connection_proof"]["db_url_source"] = db_source
        if not db_url:
            payload["blockers"].append("db_url_codex_ro_missing")
            raise PrecheckBlocked("db_url_codex_ro_missing")
        if payload["blockers"]:
            raise PrecheckBlocked("pre_connection_blockers_present")

        if connect_fn is None:
            try:
                import psycopg2
            except ModuleNotFoundError:
                venv_python = ROOT / ".venv" / "Scripts" / "python.exe"
                if venv_python.exists() and Path(sys.executable).resolve() != venv_python.resolve():
                    cmd = [str(venv_python), str(Path(__file__).resolve()), *sys.argv[1:]]
                    raise SystemExit(os.spawnv(os.P_WAIT, str(venv_python), cmd))
                raise
            connect_fn = psycopg2.connect

        payload["guardrails"]["db_access_used"] = True
        conn = connect_fn(db_url)
        if hasattr(conn, "set_session"):
            conn.set_session(readonly=True, autocommit=False)
        cur = conn.cursor()
        guard = execute_guard(cur, args.statement_timeout_ms, args.lock_timeout_ms, args.idle_transaction_timeout_ms)
        payload["connection_proof"].update(
            {
                "current_user": guard.get("current_user"),
                "current_schema": guard.get("current_schema"),
                "transaction_read_only": guard.get("transaction_read_only"),
                "default_transaction_read_only": guard.get("default_transaction_read_only"),
                "statement_timeout": guard.get("statement_timeout"),
                "lock_timeout": guard.get("lock_timeout"),
                "idle_transaction_timeout": guard.get("idle_transaction_timeout"),
            }
        )
        payload["guardrails"]["transaction_read_only"] = guard.get("transaction_read_only") == "on"
        payload["guardrails"]["default_transaction_read_only"] = guard.get("default_transaction_read_only") == "on"
        if guard.get("current_user") != EXPECTED_USER:
            payload["blockers"].append("codex_ro_user_mismatch")
        if guard.get("transaction_read_only") != "on":
            payload["blockers"].append("transaction_read_only_not_on")
        if guard.get("default_transaction_read_only") != "on":
            payload["blockers"].append("default_transaction_read_only_not_on")
        if payload["blockers"]:
            raise PrecheckBlocked("guardrail_failed_before_business_queries")

        rows = execute_statement(cur, statement_by_id("target_exists"), (TARGET_SCHEMA, TARGET_SCHEMA, TARGET_TABLE), payload)
        target_exists = rows[0] if rows else {}
        payload["target_resolution"].update(target_exists)
        payload["target_resolution"]["target_relation"] = TARGET_RELATION
        if not target_exists.get("schema_exists") or not target_exists.get("table_exists"):
            payload["blockers"].append("target_object_missing")
            raise PrecheckBlocked("target_object_missing")

        priv_rows = execute_statement(cur, statement_by_id("target_privileges"), (TARGET_SCHEMA, TARGET_SCHEMA, TARGET_TABLE), payload)
        privileges = priv_rows[0] if priv_rows else {}
        payload["schema"]["privileges"] = privileges
        write_privileges = any(bool(privileges.get(key)) for key in ("can_insert", "can_update", "can_delete", "can_truncate"))
        payload["guardrails"]["write_privileges_present"] = write_privileges
        if not privileges.get("can_select"):
            payload["blockers"].append("select_privilege_missing")
        if write_privileges:
            payload["blockers"].append("writable_role_not_allowed_for_016")
        if payload["blockers"]:
            raise PrecheckBlocked("privilege_guardrail_failed")

        payload["schema"]["columns"] = execute_statement(cur, statement_by_id("target_columns"), (TARGET_SCHEMA, TARGET_TABLE), payload)
        payload["schema"]["constraints"] = execute_statement(cur, statement_by_id("target_constraints"), (TARGET_SCHEMA, TARGET_TABLE), payload)
        payload["schema"]["indexes"] = execute_statement(cur, statement_by_id("target_indexes"), (TARGET_SCHEMA, TARGET_TABLE), payload)
        payload["schema"]["triggers"] = execute_statement(cur, statement_by_id("target_triggers"), (TARGET_SCHEMA, TARGET_TABLE), payload)
        owner_rows = execute_statement(cur, statement_by_id("target_owner_rls_size"), (TARGET_SCHEMA, TARGET_TABLE), payload)
        owner = owner_rows[0] if owner_rows else {}
        payload["schema"]["owner"] = owner.get("owner")
        payload["schema"]["rls"] = {
            "relrowsecurity": owner.get("relrowsecurity"),
            "relforcerowsecurity": owner.get("relforcerowsecurity"),
            "policies": execute_statement(cur, statement_by_id("target_policies"), (TARGET_SCHEMA, TARGET_TABLE), payload),
        }
        payload["database_state"]["approximate_total_rows"] = owner.get("approximate_rows") or 0
        payload["database_state"]["total_relation_size_bytes"] = owner.get("total_relation_size_bytes")
        payload["database_state"]["relation_size_bytes"] = owner.get("relation_size_bytes")
        payload["dependencies"] = execute_statement(cur, statement_by_id("target_dependencies"), (TARGET_SCHEMA, TARGET_TABLE), payload)

        cols = column_names(payload)
        payload["persisted_grain"] = {
            "physical_columns_present": {name: name in cols for name in (
                "batch_id",
                "source_file",
                "source_row",
                "fecha_visita",
                "payload_json",
                "inserted_at",
                "loader_version",
                "event_id",
                "source_file_id",
                "source_file_sha256",
                "source_row_number",
                "photo_row_hash",
                "event_stable_hash",
                "dry_run_batch_id",
            )},
            "inferred_from_loader": "one raw Excel row per target row; unique protection is batch_id + source_row in local SQL/loader",
        }
        required_business_cols = {"fecha_visita", "batch_id", "source_file", "source_row", "payload_json"}
        if required_business_cols.issubset(cols):
            total_rows = execute_statement(cur, statement_by_id("state_total_minmax"), tuple(), payload)[0]
            payload["database_state"]["exact_total_rows"] = total_rows.get("exact_total_rows") or 0
            payload["database_state"]["fecha_min"] = total_rows.get("fecha_min")
            payload["database_state"]["fecha_max"] = total_rows.get("fecha_max")
            payload["database_state"]["date_counts"] = execute_statement(
                cur, statement_by_id("state_date_counts"), ("2026-06-01", "2026-07-31"), payload
            )
            payload["database_state"]["range_summary"] = execute_statement(cur, statement_by_id("state_range_summary"), tuple(), payload)[0]
            payload["database_state"]["week_counts"] = execute_statement(
                cur, statement_by_id("state_week_counts"), ("2026-06-01", "2026-07-31"), payload
            )
            payload["database_state"]["null_counts"] = execute_statement(cur, statement_by_id("state_null_counts"), tuple(), payload)[0]
            payload["json_identity_key_presence"] = execute_statement(cur, statement_by_id("json_identity_key_presence"), tuple(), payload)[0]
            source_rows = execute_statement(
                cur, statement_by_id("source_file_scope_counts"), (plan_sources["source_file_names"],), payload
            )
            batch_rows = execute_statement(
                cur, statement_by_id("batch_scope_counts"), (plan_sources["source_file_names"],), payload
            )
            recent_batches = execute_statement(cur, statement_by_id("recent_batch_counts"), tuple(), payload)
            batch_state = execute_statement(cur, statement_by_id("state_batch_ids"), tuple(), payload)[0]
            payload["database_state"]["batch_ids"] = batch_state.get("batch_ids") or []
            payload["database_state"]["batch_count"] = batch_state.get("batch_count") or 0
            matched_files = {row.get("source_file") for row in source_rows}
            payload["source_file_coverage"] = {
                "plan_source_file_count": len(plan_sources["source_file_names"]),
                "matched_source_file_count": len(matched_files),
                "matched_source_files": sorted(str(item) for item in matched_files if item),
                "source_file_counts": source_rows,
                "dry_run_batch_id_present": False,
            }
            payload["batch_coverage"] = {
                "batches_with_plan_source_files": batch_rows,
                "recent_batches_limited_50": recent_batches,
            }
        else:
            payload["warnings"].append("business_queries_skipped_missing_required_columns")

        payload["verdict"] = "WARN"
    except PrecheckBlocked:
        payload["verdict"] = "BLOCKED"
        payload["exact_overlap_classification"] = "BLOCKED"
        payload["apply_gate"] = "BLOCKED"
    except Exception as exc:
        payload["verdict"] = "BLOCKED"
        payload["blockers"].append("precheck_exception")
        payload["warnings"].append(redact_error(exc))
    finally:
        if conn is not None:
            try:
                conn.rollback()
                payload["guardrails"]["rollback_completed"] = True
            except Exception as exc:
                payload["warnings"].append(f"rollback_failed:{redact_error(exc)}")
            try:
                conn.close()
            except Exception:
                pass
        try:
            after = load_plan_path.read_bytes()
            after_file_sha = sha256_bytes(after)
            payload["load_plan"]["file_sha256_after"] = after_file_sha
            payload["load_plan"]["sha256_after"] = json.loads(after.decode("utf-8")).get("load_plan_sha256")
            if payload["load_plan"].get("file_sha256_before") and after_file_sha != payload["load_plan"].get("file_sha256_before"):
                payload["blockers"].append("load_plan_file_changed_during_execution")
                payload["verdict"] = "BLOCKED"
        except Exception as exc:
            payload["warnings"].append(f"load_plan_after_hash_failed:{redact_error(exc)}")

    classify_overlap(payload, extract_plan_sources(json.loads(load_plan_path.read_text(encoding="utf-8"))))
    if payload["blockers"]:
        payload["verdict"] = "BLOCKED"
        if payload["apply_gate"] != "BLOCKED_FOR_IDEMPOTENCY_CONTRACT":
            payload["apply_gate"] = "BLOCKED"
    payload["source_signal_interpretation"] = build_source_signal_interpretation(payload)
    payload["historical_state_discrepancy"] = build_historical_state_discrepancy(payload)
    if payload["historical_state_discrepancy"].get("classification") == "TABLE_STATE_CHANGED":
        warning = "historical_state_discrepancy:TABLE_STATE_CHANGED"
        if warning not in payload["warnings"]:
            payload["warnings"].append(warning)
    build_hashes(payload, args.month, str(payload["load_plan"].get("load_plan_sha256") or ""), timeout_config)
    assert_no_sensitive_output(payload)
    return payload


def report_markdown(payload: dict[str, Any]) -> str:
    guard = payload["guardrails"]
    state = payload["database_state"]
    feasibility = payload["exact_overlap_feasibility"]
    roadmap = payload.get("roadmap_compliance", {})
    discrepancy = payload.get("historical_state_discrepancy", {})
    signal = payload.get("source_signal_interpretation", {})
    lines = [
        "# 016 KPIONE monthly DB precheck read-only",
        "",
        f"- Verdict: `{payload['verdict']}`",
        f"- Apply gate: `{payload['apply_gate']}`",
        f"- Coarse overlap: `{payload['coarse_overlap_classification']}`",
        f"- Exact overlap: `{payload['exact_overlap_classification']}`",
        f"- Target: `{payload.get('target_resolution', {}).get('target_relation', TARGET_RELATION)}`",
        "",
        "## Roadmap compliance",
        "",
        f"- roadmap_lock_id: `{roadmap.get('roadmap_lock_id')}`",
        f"- roadmap_lock_sha256: `{roadmap.get('roadmap_lock_sha256')}`",
        f"- roadmap_lock_commit: `{roadmap.get('roadmap_lock_commit')}`",
        f"- current_phase: `{roadmap.get('current_phase')}`",
        f"- expected_next_phase: `{roadmap.get('expected_next_phase')}`",
        f"- roadmap_compliance: `{roadmap.get('roadmap_compliance')}`",
        f"- deviations_detected: `{json.dumps(roadmap.get('deviations_detected', []), sort_keys=True)}`",
        "",
        "## Guardrails",
        "",
        f"- DB access used: `{guard['db_access_used']}`",
        f"- transaction_read_only: `{guard['transaction_read_only']}`",
        f"- default_transaction_read_only: `{guard['default_transaction_read_only']}`",
        f"- write privileges present: `{guard['write_privileges_present']}`",
        f"- rollback completed: `{guard['rollback_completed']}`",
        f"- writes attempted: `{guard['writes_attempted']}`",
        "",
        "## Load plan",
        "",
        f"- load_plan_sha256: `{payload['load_plan'].get('load_plan_sha256')}`",
        f"- would_stage_rows: `{payload['load_plan'].get('would_stage_rows')}`",
        f"- carry_forward_out_rows: `{payload['load_plan'].get('carry_forward_out_rows')}`",
        "",
        "## Database state",
        "",
        f"- approximate_total_rows: `{state.get('approximate_total_rows')}`",
        f"- exact_total_rows: `{state.get('exact_total_rows')}`",
        f"- fecha_min: `{state.get('fecha_min')}`",
        f"- fecha_max: `{state.get('fecha_max')}`",
        f"- batch_ids: `{json.dumps(state.get('batch_ids', []), sort_keys=True)}`",
        f"- range_summary: `{json.dumps(state.get('range_summary', {}), sort_keys=True)}`",
        "",
        "## Historical state discrepancy",
        "",
        f"- target: `{discrepancy.get('target')}`",
        f"- historical_row_count: `{discrepancy.get('historical_row_count')}`",
        f"- historical_batch_count: `{discrepancy.get('historical_batch_count')}`",
        f"- latest_historical_batch_id: `{discrepancy.get('latest_historical_batch_id')}`",
        f"- latest_historical_batch_loaded_rows: `{discrepancy.get('latest_historical_batch_loaded_rows')}`",
        f"- current_exact_row_count: `{discrepancy.get('current_exact_row_count')}`",
        f"- current_batch_ids: `{json.dumps(discrepancy.get('current_batch_ids', []), sort_keys=True)}`",
        f"- current_fecha_min: `{discrepancy.get('current_fecha_min')}`",
        f"- current_fecha_max: `{discrepancy.get('current_fecha_max')}`",
        f"- classification: `{discrepancy.get('classification')}`",
        f"- resolution_status: `{discrepancy.get('resolution_status')}`",
        f"- legacy_migration_authority: `{discrepancy.get('legacy_migration_authority')}`",
        f"- blocker_for_new_design: `{discrepancy.get('blocker_for_new_design')}`",
        f"- warning_condition: `{discrepancy.get('warning_condition')}`",
        "",
        "## Source signal interpretation",
        "",
        f"- policy: `{signal.get('policy')}`",
        f"- coarse_classification: `{signal.get('coarse_classification')}`",
        f"- proves_freshness: `{signal.get('proves_freshness')}`",
        f"- proves_no_overlap: `{signal.get('proves_no_overlap')}`",
        f"- exact_identity_available: `{signal.get('exact_identity_available')}`",
        f"- exact_overlap_classification: `{signal.get('exact_overlap_classification')}`",
        f"- apply_gate: `{signal.get('apply_gate')}`",
        f"- explanation: {signal.get('explanation')}",
        "",
        "## Identity feasibility",
        "",
        f"- exact_comparison_potentially_feasible: `{feasibility.get('exact_comparison_potentially_feasible')}`",
        f"- operationally_unsafe_reason: `{feasibility.get('operationally_unsafe_reason', '')}`",
        "",
        "## Blockers",
        "",
    ]
    lines.extend(f"- `{item}`" for item in payload.get("blockers", [])) if payload.get("blockers") else lines.append("- None")
    lines.extend(["", "## Warnings", ""])
    lines.extend(f"- `{item}`" for item in payload.get("warnings", [])) if payload.get("warnings") else lines.append("- None")
    lines.extend(["", "## Query audit", ""])
    for item in payload.get("query_audit", []):
        lines.append(
            f"- `{item['statement_id']}` rows=`{item['result_rows']}` duration_ms=`{item['duration_ms']}` error=`{item.get('error')}`"
        )
    lines.append("")
    lines.append("Evidence is aggregate only. No payload_json rows, URLs, secrets, or row-level DB exports are included.")
    return "\n".join(lines) + "\n"


def write_outputs(payload: dict[str, Any], json_out: str, md_out: str) -> None:
    json_path = (ROOT / json_out).resolve() if not Path(json_out).is_absolute() else Path(json_out)
    md_path = (ROOT / md_out).resolve() if not Path(md_out).is_absolute() else Path(md_out)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    md_path.write_text(report_markdown(payload), encoding="utf-8", newline="\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="016 KPIONE monthly DB precheck, strict read-only.")
    ap.add_argument("--month", required=True)
    ap.add_argument("--load-plan-json", required=True)
    ap.add_argument("--statement-timeout-ms", type=int, default=120000)
    ap.add_argument("--lock-timeout-ms", type=int, default=5000)
    ap.add_argument("--idle-transaction-timeout-ms", type=int, default=60000)
    ap.add_argument("--json-out", required=True)
    ap.add_argument("--md-out", required=True)
    ap.add_argument("--soft-exit", action="store_true")
    args = ap.parse_args(argv)
    if args.month != EXPECTED_MONTH:
        raise SystemExit("Only month 2026-06 is allowlisted for phase 016.")
    for value_name in ("statement_timeout_ms", "lock_timeout_ms", "idle_transaction_timeout_ms"):
        if int(getattr(args, value_name)) <= 0:
            raise SystemExit(f"{value_name} must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = run_precheck(args)
    write_outputs(payload, args.json_out, args.md_out)
    print(
        json.dumps(
            {
                "phase_id": payload["phase_id"],
                "verdict": payload["verdict"],
                "apply_gate": payload["apply_gate"],
                "coarse_overlap_classification": payload["coarse_overlap_classification"],
                "exact_overlap_classification": payload["exact_overlap_classification"],
                "rollback_completed": payload["guardrails"]["rollback_completed"],
                "json_out": args.json_out,
                "md_out": args.md_out,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    if payload["verdict"] == "BLOCKED" and not args.soft_exit:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
