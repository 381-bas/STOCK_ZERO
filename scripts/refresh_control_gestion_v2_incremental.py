#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
from typing import Any
import uuid
from urllib.parse import parse_qs, urlparse


PHASE = "FASE_9C5N_N4B_3A_CONTROL_GESTION_INITIAL_SEED_APPLY_TYPE_FIX"
DEFAULT_STATEMENT_TIMEOUT_SECONDS = 1800
ROOT = Path(__file__).resolve().parents[1]
CODEX_RO_SECRET_FILE = ROOT / ".local_secrets" / "codex_ro.env"
PLAN_023_DEFAULT = ROOT / "plans" / "023_control_gestion_route_b_bridge_refresh_plan.json"
PLAN_023_TYPE = "stock_zero_control_gestion_route_b_bridge_refresh_plan_v1"
PLAN_023_REFRESH_STATE = "BRIDGE_COMMITTED_REFRESH_PENDING_GATE_CLOSED"
MART_REFRESH_ROLE = "stock_zero_cg_mart_refresh"
MART_REFRESH_ENV = "DB_URL_CG_MART_REFRESH"
MART_REFRESH_PROFILE = "cg-mart-refresh"
MART_REFRESH_DRY_RUN_OPERATION = "dry-run-june-refresh-023"
MART_REFRESH_APPLY_OPERATION = "apply-june-refresh-023"
EXPECTED_PROJECT_REF = "xheyrgfagpoigpgakilu"
EXPECTED_HOST = "db.xheyrgfagpoigpgakilu.supabase.co"
EXPECTED_DATABASE = "postgres"
DRY_RUN_CONFIRM_TOKEN = "STOCK_ZERO_023_DRY_RUN_JUNE_REFRESH"
APPLY_CONFIRM_TOKEN = "STOCK_ZERO_023_APPLY_JUNE_REFRESH"
FINGERPRINT_SCHEMA_VERSION = "stock_zero_cg_content_fingerprint_v1"
FINGERPRINT_KEYS = (
    "target_daily_sha256",
    "target_weekly_sha256",
    "source_daily_stage_sha256",
    "source_weekly_stage_sha256",
)
PRECOMMIT = "PRECOMMIT"
COMMITTED_EVIDENCE_PENDING = "COMMITTED_EVIDENCE_PENDING"
COMMITTED_EVIDENCE_RECORDED = "COMMITTED_EVIDENCE_RECORDED"
COMMITTED_EVIDENCE_RECOVERY_REQUIRED = "COMMITTED_EVIDENCE_RECOVERY_REQUIRED"
_INTERNAL_APPLY_MARKER = object()

DAILY_SOURCE = "cg_core.v_cg_visita_dia_precedencia_route_b_v1"
DAILY_FACT = "cg_mart.fact_cg_visita_dia_resuelta_v2"
WEEKLY_FREQ = "cg_core.v_rr_frecuencia_base_resuelta_v2"
WEEKLY_FACT = "cg_mart.fact_cg_out_weekly_v2"
WEEKLY_MV = "cg_mart.mv_cg_out_weekly_v2"

DAILY_FACT_COLUMNS = [
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
    "mart_loaded_at",
]

WEEKLY_FACT_COLUMNS = [
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


class RefreshContractError(RuntimeError):
    pass


def _canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _git(*args: str, text: bool = True) -> subprocess.CompletedProcess[Any]:
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=text, check=False,
    )


def _validate_git_and_plan(plan_path: Path, plan: dict[str, Any]) -> dict[str, str]:
    approved = plan.get("approved_git_sha")
    if not isinstance(approved, str) or re.fullmatch(r"[0-9a-f]{40}", approved) is None:
        raise RefreshContractError("approved_git_sha_required")
    head = _git("rev-parse", "HEAD")
    if head.returncode != 0 or head.stdout.strip() != approved:
        raise RefreshContractError("repository_head_mismatch")
    if _git("diff", "--quiet").returncode != 0:
        raise RefreshContractError("repository_worktree_not_clean")
    if _git("diff", "--cached", "--quiet").returncode != 0:
        raise RefreshContractError("repository_index_not_clean")
    untracked = _git("ls-files", "--others", "--exclude-standard")
    if untracked.returncode != 0 or untracked.stdout.strip():
        raise RefreshContractError("repository_untracked_files_present")
    try:
        relative = plan_path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise RefreshContractError("plan_outside_repository") from exc
    blob = _git("show", f"HEAD:{relative}", text=False)
    if blob.returncode != 0:
        raise RefreshContractError("plan_not_tracked_at_head")
    if plan_path.read_bytes() != blob.stdout:
        raise RefreshContractError("plan_worktree_blob_mismatch")
    return {
        "approved_git_sha": approved,
        "plan_path": relative,
        "plan_raw_sha256": _sha256_bytes(blob.stdout),
    }


def _load_plan_023(plan_path: Path, *, operation: str) -> tuple[dict[str, Any], dict[str, str]]:
    try:
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RefreshContractError("plan_023_unavailable_or_invalid") from exc
    if plan.get("document_type") != PLAN_023_TYPE:
        raise RefreshContractError("plan_023_document_type_mismatch")
    if plan.get("status") != PLAN_023_REFRESH_STATE:
        raise RefreshContractError("plan_023_refresh_state_mismatch")
    authorizations = plan.get("authorizations")
    if not isinstance(authorizations, dict):
        raise RefreshContractError("plan_023_authorizations_missing")
    expected_names = {
        "provision_refresh_role_authorized",
        "apply_bridge_authorized",
        "apply_june_refresh_authorized",
        "runtime_app_validation_authorized",
    }
    if set(authorizations) != expected_names:
        raise RefreshContractError("plan_023_authorization_set_mismatch")
    active = [name for name, enabled in authorizations.items() if enabled is True]
    if len(active) > 1:
        raise RefreshContractError("multiple_productive_authorizations_active")
    if operation == "apply":
        if active != ["apply_june_refresh_authorized"]:
            raise RefreshContractError("june_refresh_not_exclusively_authorized")
        if plan.get("gate_open") is not True or plan.get("productive_actions_authorized") is not True:
            raise RefreshContractError("june_refresh_gate_closed")
    elif operation == "dry-run":
        if active:
            raise RefreshContractError("dry_run_requires_all_authorizations_closed")
        if plan.get("gate_open") is not False or plan.get("productive_actions_authorized") is not False:
            raise RefreshContractError("dry_run_requires_closed_gate")
    else:
        raise RefreshContractError("unsupported_plan_operation")
    scope = plan.get("scope", {})
    payload = scope.get("canonical_payload")
    if not isinstance(payload, dict):
        raise RefreshContractError("scope_payload_missing")
    calculated = _sha256_bytes(_canonical_json_bytes(payload))
    if calculated != scope.get("scope_sha256"):
        raise RefreshContractError("scope_sha256_mismatch")
    dates = payload.get("affected_dates")
    weeks = payload.get("affected_weeks")
    expected_dates = [
        (date(2026, 6, 1) + timedelta(days=offset)).isoformat()
        for offset in range(30)
    ]
    expected_weeks = [
        "2026-06-01", "2026-06-08", "2026-06-15", "2026-06-22", "2026-06-29",
    ]
    if dates != expected_dates or weeks != expected_weeks:
        raise RefreshContractError("june_scope_exact_match_required")
    target = plan.get("target", {})
    if target != {
        "project_ref": EXPECTED_PROJECT_REF,
        "hostname": EXPECTED_HOST,
        "database": EXPECTED_DATABASE,
        "sslmode": "require",
    }:
        raise RefreshContractError("registered_target_contract_mismatch")
    authority = _validate_git_and_plan(plan_path, plan)
    authority["scope_sha256"] = calculated
    return plan, authority


def _validate_wrapper_marker(expected_operation: str) -> None:
    if os.getenv("STOCK_ZERO_OPERATION_PROFILE") != MART_REFRESH_PROFILE:
        raise RefreshContractError("cg_mart_refresh_wrapper_profile_required")
    if os.getenv("STOCK_ZERO_OPERATION") != expected_operation:
        raise RefreshContractError("cg_mart_refresh_wrapper_operation_required")


def _validate_productive_dsn(dsn: str, plan: dict[str, Any]) -> None:
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise RefreshContractError("unsupported_db_scheme")
    if parsed.username != MART_REFRESH_ROLE:
        raise RefreshContractError("mart_refresh_dsn_role_mismatch")
    target = plan["target"]
    if (parsed.hostname or "").lower() != target["hostname"]:
        raise RefreshContractError("mart_refresh_hostname_mismatch")
    if target["project_ref"] not in (parsed.hostname or ""):
        raise RefreshContractError("mart_refresh_project_mismatch")
    if (parsed.path or "").lstrip("/") != target["database"]:
        raise RefreshContractError("mart_refresh_database_mismatch")
    sslmodes = parse_qs(parsed.query, keep_blank_values=True).get("sslmode", [])
    if sslmodes != [target["sslmode"]]:
        raise RefreshContractError("mart_refresh_sslmode_require_required")


def _canonical_evidence_path(path: Path, run_id: str, filename: str) -> Path:
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
        run_id or "",
    ) is None:
        raise RefreshContractError("canonical_run_id_required")
    expected = (ROOT / "evidence" / "runtime" / "023" / run_id / filename).resolve()
    actual = (ROOT / path).resolve() if not path.is_absolute() else path.resolve()
    if actual != expected:
        raise RefreshContractError("canonical_evidence_path_required")
    if actual.exists():
        raise RefreshContractError("evidence_output_already_exists")
    if not actual.parent.is_dir():
        raise RefreshContractError("evidence_parent_missing")
    return actual


def _temporary_report_path(path: Path) -> Path:
    actual = path.resolve()
    temporary_root = Path(tempfile.gettempdir()).resolve()
    try:
        actual.relative_to(temporary_root)
    except ValueError as exc:
        raise RefreshContractError("dry_run_report_must_be_in_temp") from exc
    if actual.exists() or not actual.parent.is_dir():
        raise RefreshContractError("unused_dry_run_report_path_required")
    return actual


def write_json_exclusive(path: Path, report: dict[str, Any]) -> None:
    """Publish JSON without ever replacing an existing final path."""
    if path.exists():
        raise RefreshContractError("evidence_output_already_exists")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    fd: int | None = None
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            fd = None
            json.dump(report, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        # Hard-link publication is atomic and fails if the destination exists.
        os.link(temporary, path)
    except FileExistsError as exc:
        raise RefreshContractError("evidence_output_already_exists") from exc
    finally:
        if fd is not None:
            os.close(fd)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _write_json_once(path: Path, report: dict[str, Any]) -> None:
    write_json_exclusive(path, report)


def write_committed_recovery_receipt(report: dict[str, Any]) -> Path:
    receipt = Path(tempfile.gettempdir()) / (
        "stock_zero_023_committed_evidence_recovery_"
        f"{report['run_id']}_{uuid.uuid4().hex}.json"
    )
    write_json_exclusive(receipt, report)
    return receipt


def _canonical_scalar(value: Any) -> dict[str, Any]:
    if value is None:
        return {"type": "null", "value": None}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, (Decimal, float)):
        number = Decimal(str(value))
        normalized = format(number.normalize(), "f")
        return {"type": "number", "value": "0" if normalized == "-0" else normalized}
    if isinstance(value, datetime):
        if value.tzinfo is None:
            rendered = value.replace(tzinfo=timezone.utc).isoformat()
        else:
            rendered = value.astimezone(timezone.utc).isoformat()
        return {"type": "timestamp", "value": rendered.replace("+00:00", "Z")}
    if isinstance(value, date):
        return {"type": "date", "value": value.isoformat()}
    if isinstance(value, bytes):
        return {"type": "bytes", "value": value.hex()}
    return {"type": "text", "value": str(value)}


def _canonical_row_bytes(row: Any, columns: list[str]) -> bytes:
    values = list(row)
    if len(values) != len(columns):
        raise RefreshContractError("fingerprint_column_count_mismatch")
    return _canonical_json_bytes({
        "schema_version": FINGERPRINT_SCHEMA_VERSION,
        "columns": columns,
        "values": [_canonical_scalar(value) for value in values],
    })


def fingerprint_rows(rows: Any, columns: list[str]) -> dict[str, Any]:
    encoded = sorted(_canonical_row_bytes(row, columns) for row in rows)
    digest = hashlib.sha256()
    digest.update(_canonical_json_bytes({
        "schema_version": FINGERPRINT_SCHEMA_VERSION,
        "columns": columns,
    }))
    for payload in encoded:
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return {
        "schema_version": FINGERPRINT_SCHEMA_VERSION,
        "row_count": len(encoded),
        "sha256": digest.hexdigest(),
    }


def _fingerprint_query(cur, query: str, params: tuple[Any, ...], columns: list[str]) -> dict[str, Any]:
    cur.execute(query, params)
    digest = hashlib.sha256()
    digest.update(_canonical_json_bytes({
        "schema_version": FINGERPRINT_SCHEMA_VERSION,
        "columns": columns,
    }))
    row_count = 0
    while True:
        rows = cur.fetchmany(1000)
        if not rows:
            break
        for row in rows:
            payload = _canonical_row_bytes(row, columns)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
            row_count += 1
    return {
        "schema_version": FINGERPRINT_SCHEMA_VERSION,
        "row_count": row_count,
        "sha256": digest.hexdigest(),
    }


def _stage_select_query(create_query: str, cte_name: str) -> str:
    marker = f"WITH {cte_name} AS"
    offset = create_query.find(marker)
    if offset < 0:
        raise RefreshContractError("stage_select_contract_missing")
    return create_query[offset:].strip()


def _compute_content_fingerprints(cur, affected_dates: list[str], affected_weeks: list[str]) -> dict[str, Any]:
    daily_columns = _quoted_ident_list(DAILY_FACT_COLUMNS)
    weekly_columns = _quoted_ident_list(WEEKLY_FACT_COLUMNS)
    daily_source = _stage_select_query(_create_daily_stage_query(), "affected_dates").replace(
        "now()::timestamptz AS mart_loaded_at",
        "NULL::timestamptz AS mart_loaded_at",
    )
    weekly_source = _stage_select_query(_create_weekly_stage_query(), "affected_weeks")
    return {
        "target_daily_sha256": _fingerprint_query(
            cur,
            f"SELECT {daily_columns} FROM {DAILY_FACT} "
            "WHERE fecha_visita = ANY(%s::date[]) "
            "ORDER BY fecha_visita,cod_rt,cliente_norm",
            (affected_dates,), DAILY_FACT_COLUMNS,
        ),
        "target_weekly_sha256": _fingerprint_query(
            cur,
            f"SELECT {weekly_columns} FROM {WEEKLY_FACT} "
            'WHERE "SEMANA_INICIO" = ANY(%s::date[]) '
            'ORDER BY "SEMANA_INICIO","COD_RT","CLIENTE_NORM_FILTER"',
            (affected_weeks,), WEEKLY_FACT_COLUMNS,
        ),
        "source_daily_stage_sha256": _fingerprint_query(
            cur,
            f"SELECT {daily_columns} FROM ({daily_source}) source_daily "
            "ORDER BY fecha_visita,cod_rt,cliente_norm",
            (affected_dates,), DAILY_FACT_COLUMNS,
        ),
        "source_weekly_stage_sha256": _fingerprint_query(
            cur,
            f"SELECT {weekly_columns} FROM ({weekly_source}) source_weekly "
            'ORDER BY "SEMANA_INICIO","COD_RT","CLIENTE_NORM_FILTER"',
            (affected_weeks,), WEEKLY_FACT_COLUMNS,
        ),
    }


def _assert_fingerprints_match(expected: Any, observed: Any) -> None:
    if not isinstance(expected, dict) or set(expected) != set(FINGERPRINT_KEYS):
        raise RefreshContractError("authorized_content_fingerprints_missing")
    for value in expected.values():
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != FINGERPRINT_SCHEMA_VERSION
            or not isinstance(value.get("row_count"), int)
            or value["row_count"] < 0
            or re.fullmatch(r"[0-9a-f]{64}", str(value.get("sha256", ""))) is None
        ):
            raise RefreshContractError("authorized_content_fingerprint_invalid")
    if expected != observed:
        raise RefreshContractError("STOP_023_JUNE_REFRESH_PRESTATE_OR_SOURCE_DRIFT")


def _now_ms() -> int:
    return int(time.perf_counter() * 1000)


def ensure_sslmode(db_url: str) -> str:
    if not db_url:
        return db_url
    if "sslmode=" in db_url:
        return db_url
    return db_url + ("&sslmode=require" if "?" in db_url else "?sslmode=require")


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


def week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def sorted_iso(values: set[date]) -> list[str]:
    return [value.isoformat() for value in sorted(values)]


def build_week_scope(
    affected_dates: set[date],
    explicit_weeks: set[date],
    safety_window_weeks: int,
) -> dict[str, set[date]]:
    derived_weeks = {week_start(value) for value in affected_dates}
    explicit_affected_weeks = {week_start(value) for value in explicit_weeks}
    strict_weeks = derived_weeks | explicit_affected_weeks

    safety_weeks: set[date] = set()
    for base_week in strict_weeks:
        for offset in range(1, safety_window_weeks + 1):
            safety_weeks.add(base_week - timedelta(days=7 * offset))
    safety_weeks -= strict_weeks

    return {
        "requested_affected_dates": affected_dates,
        "explicit_affected_weeks": explicit_affected_weeks,
        "derived_affected_weeks": derived_weeks,
        "strict_affected_weeks": strict_weeks,
        "safety_weeks": safety_weeks,
        "validation_weeks": strict_weeks | safety_weeks,
    }


def build_week_origin_by_week(week_scope: dict[str, set[date]]) -> dict[str, str]:
    origins: dict[str, str] = {}
    for week_value in sorted(week_scope["validation_weeks"]):
        labels = []
        if week_value in week_scope["derived_affected_weeks"]:
            labels.append("derived")
        if week_value in week_scope["explicit_affected_weeks"]:
            labels.append("explicit")
        if not labels and week_value in week_scope["safety_weeks"]:
            labels.append("safety")
        origins[week_value.isoformat()] = "+".join(labels) if labels else "unknown"
    return origins


def build_apply_scope(week_scope: dict[str, set[date]]) -> dict[str, Any]:
    apply_daily_dates = sorted_iso(week_scope["requested_affected_dates"])
    apply_weekly_weeks = sorted_iso(week_scope["strict_affected_weeks"])
    explicit_weeks = sorted_iso(week_scope["explicit_affected_weeks"])
    return {
        "apply_daily_dates": apply_daily_dates,
        "apply_weekly_weeks": apply_weekly_weeks,
        "explicit_weeks_included_in_apply": all(value in apply_weekly_weeks for value in explicit_weeks),
        "safety_weeks_included_in_apply": False,
        "weekly_stage_order": "after_daily_apply",
        "apply_order": {
            "daily_fact": "delete_insert_validate_first",
            "weekly_fact": "stage_delete_insert_validate_after_daily",
            "transaction": "single_transaction_until_fact_validations_pass",
            "analyze": "after_commit",
        },
    }


def _statement_timeout_ms(statement_timeout_seconds: int) -> int:
    seconds = int(statement_timeout_seconds)
    if seconds <= 0:
        return 0
    return seconds * 1000


def _normalize_db_url(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if value.startswith("$env:DB_URL_CODEX_RO="):
        value = value.split("=", 1)[1].strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1].strip()
    return value


def _load_codex_ro_db_url_from_file(path: Path) -> str:
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.replace("\ufeff", "").strip()
        if not raw or raw.startswith("#"):
            continue
        if raw.startswith("DB_URL_CODEX_RO="):
            _, value = raw.split("=", 1)
            return _normalize_db_url(value)
    return ""


def resolve_readonly_db_url_for_dry_run(args: argparse.Namespace) -> tuple[str, str]:
    explicit = _normalize_db_url(args.db_url)
    if explicit:
        return explicit, "explicit"
    if not args.dry_run or args.apply or args.confirm_real_apply:
        return "", "missing"
    env_value = _normalize_db_url(os.getenv("DB_URL_CODEX_RO", ""))
    if env_value:
        return env_value, "env:DB_URL_CODEX_RO"
    file_value = _load_codex_ro_db_url_from_file(CODEX_RO_SECRET_FILE)
    if file_value:
        return file_value, "file:.local_secrets/codex_ro.env"
    return "", "missing"


def _fetch_relation_exists(cur, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation_name,))
    row = cur.fetchone()
    return bool(row and row[0])


def _metric_row(row: dict[str, Any], metrics: list[str]) -> dict[str, int]:
    return {metric: int(row.get(metric) or 0) for metric in metrics}


def _sum_metric_rows(rows: list[dict[str, Any]], metrics: list[str]) -> dict[str, int]:
    totals = {metric: 0 for metric in metrics}
    for row in rows:
        for metric in metrics:
            totals[metric] += int(row.get(metric) or 0)
    return totals


def _diff_rows(
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    *,
    key_name: str,
    metrics: list[str],
) -> dict[str, Any]:
    left_by_key = {str(row[key_name]): _metric_row(row, metrics) for row in left_rows}
    right_by_key = {str(row[key_name]): _metric_row(row, metrics) for row in right_rows}
    keys = sorted(set(left_by_key) | set(right_by_key))

    diffs: list[dict[str, Any]] = []
    totals = {f"{metric}_diff": 0 for metric in metrics}
    max_abs_diff = 0
    for key in keys:
        item: dict[str, Any] = {key_name: key}
        has_diff = False
        for metric in metrics:
            diff_value = right_by_key.get(key, {}).get(metric, 0) - left_by_key.get(key, {}).get(metric, 0)
            item[f"{metric}_diff"] = diff_value
            totals[f"{metric}_diff"] += diff_value
            if diff_value != 0:
                has_diff = True
                max_abs_diff = max(max_abs_diff, abs(diff_value))
        if has_diff:
            diffs.append(item)

    return {
        "ok": not diffs and all(value == 0 for value in totals.values()),
        "diff_rows_count": len(diffs),
        "max_abs_diff": max_abs_diff,
        "total_diffs": totals,
        "diff_rows": diffs[:20],
    }


def _sql_ident_list(columns: list[str]) -> str:
    return ", ".join(columns)


def _quoted_ident_list(columns: list[str]) -> str:
    return ", ".join(f'"{column}"' for column in columns)


def _stage_select_list(columns: list[str], alias: str = "s") -> str:
    return ", ".join(f'{alias}."{column}"' for column in columns)


def _daily_stats_query(source_relation: str) -> str:
    return f"""
    WITH affected_dates AS (
        SELECT unnest(%s::date[]) AS fecha_visita
    )
    SELECT
        v.fecha_visita::date AS fecha_visita,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(v.useful_day), 0)::bigint AS useful_day,
        COALESCE(SUM(v.tiene_kpione2), 0)::bigint AS tiene_kpione2,
        COALESCE(SUM(v.tiene_power_app), 0)::bigint AS tiene_power_app,
        COALESCE(SUM(v.kpione1_audit_only), 0)::bigint AS kpione1_audit_only,
        COALESCE(SUM(v.power_app_fallback), 0)::bigint AS power_app_fallback,
        COALESCE(SUM(v.raw_evidence_count), 0)::bigint AS raw_evidence_count,
        COUNT(*) FILTER (WHERE v.fuente_ganadora = 'KPIONE2')::bigint AS kpione2_winner_rows,
        COUNT(*) FILTER (WHERE v.fuente_ganadora = 'POWER_APP')::bigint AS power_app_winner_rows,
        COUNT(*) FILTER (WHERE v.fuente_ganadora IS NULL)::bigint AS no_winner_rows
    FROM {source_relation} v
    JOIN affected_dates ad
      ON ad.fecha_visita = v.fecha_visita
    GROUP BY v.fecha_visita::date
    ORDER BY v.fecha_visita::date
    """


def _daily_week_coverage_query(source_relation: str) -> str:
    return f"""
    WITH affected_weeks AS (
        SELECT unnest(%s::date[]) AS semana_inicio
    )
    SELECT
        date_trunc('week', v.fecha_visita)::date AS semana_inicio,
        COUNT(*)::bigint AS rows
    FROM {source_relation} v
    JOIN affected_weeks aw
      ON aw.semana_inicio = date_trunc('week', v.fecha_visita)::date
    GROUP BY date_trunc('week', v.fecha_visita)::date
    ORDER BY date_trunc('week', v.fecha_visita)::date
    """


def _route_scope_query() -> str:
    return f"""
    WITH affected_weeks AS (
        SELECT unnest(%s::date[]) AS semana_inicio
    )
    SELECT
        f.effective_week_start::date AS semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(COALESCE(f.visitas_exigidas_semana, 0)), 0)::bigint AS visita
    FROM {WEEKLY_FREQ} f
    JOIN affected_weeks aw
      ON aw.semana_inicio = f.effective_week_start
    GROUP BY f.effective_week_start::date
    ORDER BY f.effective_week_start::date
    """


def _weekly_candidate_query() -> str:
    return f"""
    WITH affected_weeks AS (
        SELECT unnest(%s::date[]) AS semana_inicio
    ),
    base AS MATERIALIZED (
        SELECT
            f.effective_week_start,
            f.effective_week_iso,
            f.ruta_batch_id,
            f.cod_rt,
            f.cod_b2b,
            f.local_nombre,
            f.cliente,
            f.cliente_norm,
            f.gestor,
            f.supervisor,
            f.rutero,
            f.reponedor_scope,
            f.modalidad,
            f.visitas_exigidas_semana,
            f.ruta_duplicada_flag,
            f.ruta_duplicada_rows
        FROM {WEEKLY_FREQ} f
        JOIN affected_weeks aw
          ON aw.semana_inicio = f.effective_week_start
    ),
    row_level AS (
        SELECT
            b.effective_week_start::date AS semana_inicio,
            COALESCE(b.visitas_exigidas_semana, 0)::bigint AS visita,
            COALESCE(SUM(d.useful_day), 0)::bigint AS visita_realizada_raw,
            LEAST(
                COALESCE(SUM(d.useful_day), 0)::bigint,
                COALESCE(b.visitas_exigidas_semana, 0)::bigint
            ) AS visita_realizada_cap,
            COALESCE(b.ruta_duplicada_flag, 0)::integer AS ruta_duplicada_flag,
            COALESCE(b.ruta_duplicada_rows, 0)::integer AS ruta_duplicada_rows,
            b.gestor,
            b.rutero
        FROM base b
        LEFT JOIN {DAILY_FACT} d
          ON d.cod_rt = b.cod_rt
         AND d.cliente_norm = b.cliente_norm
         AND d.semana_inicio = b.effective_week_start
        GROUP BY
            b.effective_week_start,
            b.effective_week_iso,
            b.ruta_batch_id,
            b.cod_rt,
            b.cod_b2b,
            b.local_nombre,
            b.cliente,
            b.cliente_norm,
            b.gestor,
            b.supervisor,
            b.rutero,
            b.reponedor_scope,
            b.modalidad,
            b.visitas_exigidas_semana,
            b.ruta_duplicada_flag,
            b.ruta_duplicada_rows
    )
    SELECT
        semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(visita), 0)::bigint AS visita,
        COALESCE(SUM(visita_realizada_raw), 0)::bigint AS visita_realizada_raw,
        COALESCE(SUM(visita_realizada_cap), 0)::bigint AS visita_realizada_cap,
        COALESCE(SUM(GREATEST(visita - visita_realizada_cap, 0)), 0)::bigint AS visitas_pendientes_calc,
        COUNT(*) FILTER (WHERE visita_realizada_raw >= visita)::bigint AS cumple_rows,
        COUNT(*) FILTER (WHERE visita_realizada_raw < visita)::bigint AS incumple_rows,
        COALESCE(SUM(CASE
            WHEN ruta_duplicada_flag = 1
              OR ruta_duplicada_rows > 1
              OR CAST(gestor AS text) LIKE '%%|%%'
              OR CAST(rutero AS text) LIKE '%%|%%'
            THEN 1 ELSE 0 END), 0)::bigint AS gestion_compartida_rows
    FROM row_level
    GROUP BY semana_inicio
    ORDER BY semana_inicio
    """


def _weekly_mv_query() -> str:
    return f"""
    WITH affected_weeks AS (
        SELECT unnest(%s::date[]) AS semana_inicio
    )
    SELECT
        m."SEMANA_INICIO"::date AS semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(COALESCE(m."VISITA", 0)), 0)::bigint AS visita,
        COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
        COALESCE(SUM(COALESCE(m."VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
        COALESCE(SUM(COALESCE(
            m."VISITAS_PENDIENTES_CALC",
            GREATEST(COALESCE(m."VISITA", 0) - COALESCE(m."VISITA_REALIZADA_CAP", 0), 0)
        )), 0)::bigint AS visitas_pendientes_calc,
        COUNT(*) FILTER (
            WHERE COALESCE(m."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(m."ALERTA" AS text), '')))) = 'CUMPLE'
        )::bigint AS cumple_rows,
        COUNT(*) FILTER (
            WHERE COALESCE(m."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(m."ALERTA" AS text), '')))) = 'INCUMPLE'
        )::bigint AS incumple_rows,
        COALESCE(SUM(COALESCE(
            m."GESTION_COMPARTIDA_FLAG_CALC",
            CASE
                WHEN COALESCE(m."RUTA_DUPLICADA_FLAG", 0) = 1
                  OR COALESCE(m."RUTA_DUPLICADA_ROWS", 0) > 1
                  OR CAST(m."GESTOR" AS text) LIKE '%%|%%'
                  OR CAST(m."RUTERO" AS text) LIKE '%%|%%'
                THEN 1 ELSE 0
            END
        )), 0)::bigint AS gestion_compartida_rows
    FROM {WEEKLY_MV} m
    JOIN affected_weeks aw
      ON aw.semana_inicio = m."SEMANA_INICIO"
    GROUP BY m."SEMANA_INICIO"::date
    ORDER BY m."SEMANA_INICIO"::date
    """


def _weekly_relation_query(relation_name: str) -> str:
    return f"""
    WITH affected_weeks AS (
        SELECT unnest(%s::date[]) AS semana_inicio
    )
    SELECT
        r."SEMANA_INICIO"::date AS semana_inicio,
        COUNT(*)::bigint AS rows,
        COALESCE(SUM(COALESCE(r."VISITA", 0)), 0)::bigint AS visita,
        COALESCE(SUM(COALESCE(r."VISITA_REALIZADA_RAW", 0)), 0)::bigint AS visita_realizada_raw,
        COALESCE(SUM(COALESCE(r."VISITA_REALIZADA_CAP", 0)), 0)::bigint AS visita_realizada_cap,
        COALESCE(SUM(COALESCE(
            r."VISITAS_PENDIENTES_CALC",
            GREATEST(COALESCE(r."VISITA", 0) - COALESCE(r."VISITA_REALIZADA_CAP", 0), 0)
        )), 0)::bigint AS visitas_pendientes_calc,
        COUNT(*) FILTER (
            WHERE COALESCE(r."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(r."ALERTA" AS text), '')))) = 'CUMPLE'
        )::bigint AS cumple_rows,
        COUNT(*) FILTER (
            WHERE COALESCE(r."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(CAST(r."ALERTA" AS text), '')))) = 'INCUMPLE'
        )::bigint AS incumple_rows,
        COALESCE(SUM(COALESCE(
            r."GESTION_COMPARTIDA_FLAG_CALC",
            CASE
                WHEN COALESCE(r."RUTA_DUPLICADA_FLAG", 0) = 1
                  OR COALESCE(r."RUTA_DUPLICADA_ROWS", 0) > 1
                  OR CAST(r."GESTOR" AS text) LIKE '%%|%%'
                  OR CAST(r."RUTERO" AS text) LIKE '%%|%%'
                THEN 1 ELSE 0
            END
        )), 0)::bigint AS gestion_compartida_rows
    FROM {relation_name} r
    JOIN affected_weeks aw
      ON aw.semana_inicio = r."SEMANA_INICIO"
    GROUP BY r."SEMANA_INICIO"::date
    ORDER BY r."SEMANA_INICIO"::date
    """


def _create_daily_stage_query() -> str:
    return f"""
    CREATE TEMP TABLE _cg_daily_stage
    ON COMMIT DROP AS
    WITH affected_dates AS (
        SELECT unnest(%s::date[]) AS fecha_visita
    )
    SELECT
        date_trunc('week', v.fecha_visita)::date AS semana_inicio,
        v.fecha_visita::date AS fecha_visita,
        v.cod_rt,
        v.cod_b2b,
        v.cliente,
        v.cliente_norm,
        v.local_nombre,
        v.gestor,
        v.gestor_norm,
        v.rutero,
        v.reponedor_scope,
        v.reponedor_scope_norm,
        v.supervisor,
        v.jefe_operaciones,
        v.modalidad,
        v.semana_iso,
        v.fuente_ganadora,
        v.fuentes_presentes,
        COALESCE(v.tiene_kpione2, 0)::integer AS tiene_kpione2,
        COALESCE(v.tiene_power_app, 0)::integer AS tiene_power_app,
        COALESCE(v.tiene_kpione1, 0)::integer AS tiene_kpione1,
        COALESCE(v.power_app_fallback, 0)::integer AS power_app_fallback,
        COALESCE(v.kpione1_audit_only, 0)::integer AS kpione1_audit_only,
        COALESCE(v.useful_day, 0)::integer AS useful_day,
        COALESCE(v.raw_evidence_count, 0)::integer AS raw_evidence_count,
        COALESCE(v.same_source_multimark, 0)::integer AS same_source_multimark,
        COALESCE(v.multisource_overlap, 0)::integer AS multisource_overlap,
        COALESCE(v.kpione_rows_dia, 0)::integer AS kpione_rows_dia,
        COALESCE(v.kpione2_rows_dia, 0)::integer AS kpione2_rows_dia,
        COALESCE(v.power_app_rows_dia, 0)::integer AS power_app_rows_dia,
        COALESCE(v.persona_conflicto_rows_dia, 0)::integer AS persona_conflicto_rows_dia,
        v.match_quality,
        COALESCE(v.registro_fuera_cruce, '')::text AS registro_fuera_cruce,
        now()::timestamptz AS mart_loaded_at
    FROM {DAILY_SOURCE} v
    JOIN affected_dates ad
      ON ad.fecha_visita = v.fecha_visita
    """


def _delete_daily_query() -> str:
    return f"""
    DELETE FROM {DAILY_FACT} f
    USING (
        SELECT unnest(%s::date[]) AS fecha_visita
    ) ad
    WHERE f.fecha_visita = ad.fecha_visita
    """


def _insert_daily_query() -> str:
    return f"""
    INSERT INTO {DAILY_FACT} (
        {_sql_ident_list(DAILY_FACT_COLUMNS)}
    )
    SELECT
        {_sql_ident_list(DAILY_FACT_COLUMNS)}
    FROM _cg_daily_stage
    """


def _create_weekly_stage_query() -> str:
    return f"""
    CREATE TEMP TABLE _cg_weekly_stage
    ON COMMIT DROP AS
    WITH affected_weeks AS (
        SELECT unnest(%s::date[]) AS semana_inicio
    ),
    base AS MATERIALIZED (
        SELECT
            f.effective_week_start,
            f.effective_week_iso,
            f.ruta_batch_id,
            f.cod_rt,
            f.cod_b2b,
            f.local_nombre,
            f.cliente,
            f.cliente_norm,
            f.gestor,
            f.supervisor,
            f.rutero,
            f.reponedor_scope,
            f.modalidad,
            f.visitas_exigidas_semana,
            f.lunes,
            f.martes,
            f.miercoles,
            f.jueves,
            f.viernes,
            f.sabado,
            f.domingo,
            f.ruta_duplicada_flag,
            f.ruta_duplicada_rows
        FROM {WEEKLY_FREQ} f
        JOIN affected_weeks aw
          ON aw.semana_inicio = f.effective_week_start
    ),
    agg AS (
        SELECT
            b.effective_week_start,
            b.effective_week_iso,
            b.ruta_batch_id,
            b.cod_rt,
            b.cod_b2b,
            b.local_nombre,
            b.cliente,
            b.cliente_norm,
            b.gestor,
            b.supervisor,
            b.rutero,
            b.reponedor_scope,
            b.modalidad,
            max(CASE WHEN extract(isodow FROM d.fecha_visita) = 1 THEN d.useful_day ELSE 0 END)::integer AS lunes_flag,
            max(CASE WHEN extract(isodow FROM d.fecha_visita) = 2 THEN d.useful_day ELSE 0 END)::integer AS martes_flag,
            max(CASE WHEN extract(isodow FROM d.fecha_visita) = 3 THEN d.useful_day ELSE 0 END)::integer AS miercoles_flag,
            max(CASE WHEN extract(isodow FROM d.fecha_visita) = 4 THEN d.useful_day ELSE 0 END)::integer AS jueves_flag,
            max(CASE WHEN extract(isodow FROM d.fecha_visita) = 5 THEN d.useful_day ELSE 0 END)::integer AS viernes_flag,
            max(CASE WHEN extract(isodow FROM d.fecha_visita) = 6 THEN d.useful_day ELSE 0 END)::integer AS sabado_flag,
            max(CASE WHEN extract(isodow FROM d.fecha_visita) = 7 THEN d.useful_day ELSE 0 END)::integer AS domingo_flag,
            max(COALESCE(b.lunes, 0))::integer AS lunes_plan,
            max(COALESCE(b.martes, 0))::integer AS martes_plan,
            max(COALESCE(b.miercoles, 0))::integer AS miercoles_plan,
            max(COALESCE(b.jueves, 0))::integer AS jueves_plan,
            max(COALESCE(b.viernes, 0))::integer AS viernes_plan,
            max(COALESCE(b.sabado, 0))::integer AS sabado_plan,
            max(COALESCE(b.domingo, 0))::integer AS domingo_plan,
            max(COALESCE(b.visitas_exigidas_semana, 0))::integer AS visita,
            sum(COALESCE(d.useful_day, 0))::integer AS visita_realizada_raw,
            least(
                sum(COALESCE(d.useful_day, 0))::integer,
                max(COALESCE(b.visitas_exigidas_semana, 0))::integer
            )::integer AS visita_realizada_cap,
            greatest(
                sum(COALESCE(d.useful_day, 0))::integer
                - max(COALESCE(b.visitas_exigidas_semana, 0))::integer,
                0
            )::integer AS sobre_cumplimiento,
            sum(COALESCE(d.tiene_kpione1, 0))::integer AS dias_kpione,
            sum(COALESCE(d.tiene_kpione2, 0))::integer AS dias_kpione2,
            sum(COALESCE(d.tiene_power_app, 0))::integer AS dias_power_app,
            sum(
                CASE
                    WHEN (
                        COALESCE(d.tiene_kpione1, 0)
                      + COALESCE(d.tiene_kpione2, 0)
                      + COALESCE(d.tiene_power_app, 0)
                    ) = 2 THEN 1
                    ELSE 0
                END
            )::integer AS dias_doble_marcaje,
            sum(
                CASE
                    WHEN (
                        COALESCE(d.tiene_kpione1, 0)
                      + COALESCE(d.tiene_kpione2, 0)
                      + COALESCE(d.tiene_power_app, 0)
                    ) = 3 THEN 1
                    ELSE 0
                END
            )::integer AS dias_triple_marcaje,
            sum(COALESCE(d.persona_conflicto_rows_dia, 0))::integer AS persona_conflicto_rows,
            max(COALESCE(b.ruta_duplicada_flag, 0))::integer AS ruta_duplicada_flag,
            max(COALESCE(b.ruta_duplicada_rows, 0))::integer AS ruta_duplicada_rows,
            concat_ws(
                ' | ',
                CASE WHEN max(CASE WHEN COALESCE(d.tiene_kpione1, 0) = 1 THEN 1 ELSE 0 END) = 1 THEN 'KPIONE' END,
                CASE WHEN max(CASE WHEN COALESCE(d.tiene_kpione2, 0) = 1 THEN 1 ELSE 0 END) = 1 THEN 'KPIONE2' END,
                CASE WHEN max(CASE WHEN COALESCE(d.tiene_power_app, 0) = 1 THEN 1 ELSE 0 END) = 1 THEN 'POWER_APP' END
            ) AS fuentes_reportadas_semana
        FROM base b
        LEFT JOIN {DAILY_FACT} d
          ON d.cod_rt = b.cod_rt
         AND d.cliente_norm = b.cliente_norm
         AND d.semana_inicio = b.effective_week_start
        GROUP BY
            b.effective_week_start,
            b.effective_week_iso,
            b.ruta_batch_id,
            b.cod_rt,
            b.cod_b2b,
            b.local_nombre,
            b.cliente,
            b.cliente_norm,
            b.gestor,
            b.supervisor,
            b.rutero,
            b.reponedor_scope,
            b.modalidad
    )
    SELECT
        cod_rt AS "COD_RT",
        cod_b2b AS "COD_B2B",
        local_nombre AS "LOCAL",
        cliente AS "CLIENTE",
        gestor AS "GESTOR",
        rutero AS "RUTERO",
        reponedor_scope AS "REPONEDOR",
        supervisor AS "SUPERVISOR",
        modalidad AS "MODALIDAD",
        effective_week_start::date AS "SEMANA_INICIO",
        effective_week_iso AS "SEMANA_ISO",
        lunes_flag AS "LUNES_FLAG",
        martes_flag AS "MARTES_FLAG",
        miercoles_flag AS "MIERCOLES_FLAG",
        jueves_flag AS "JUEVES_FLAG",
        viernes_flag AS "VIERNES_FLAG",
        sabado_flag AS "SABADO_FLAG",
        domingo_flag AS "DOMINGO_FLAG",
        lunes_plan AS "LUNES_PLAN",
        martes_plan AS "MARTES_PLAN",
        miercoles_plan AS "MIERCOLES_PLAN",
        jueves_plan AS "JUEVES_PLAN",
        viernes_plan AS "VIERNES_PLAN",
        sabado_plan AS "SABADO_PLAN",
        domingo_plan AS "DOMINGO_PLAN",
        visita AS "VISITA",
        visita_realizada_raw AS "VISITA_REALIZADA",
        (visita_realizada_raw - visita)::integer AS "DIFERENCIA",
        CASE WHEN visita_realizada_raw >= visita THEN 'CUMPLE' ELSE 'INCUMPLE' END AS "ALERTA",
        dias_kpione AS "DIAS_KPIONE",
        dias_kpione2 AS "DIAS_KPIONE2",
        dias_power_app AS "DIAS_POWER_APP",
        dias_doble_marcaje AS "DIAS_DOBLE_MARCAJE",
        dias_triple_marcaje AS "DIAS_TRIPLE_MARCAJE",
        fuentes_reportadas_semana AS "FUENTES_REPORTADAS_SEMANA",
        persona_conflicto_rows AS "PERSONA_CONFLICTO_ROWS",
        visita_realizada_raw AS "VISITA_REALIZADA_RAW",
        visita_realizada_cap AS "VISITA_REALIZADA_CAP",
        sobre_cumplimiento AS "SOBRE_CUMPLIMIENTO",
        ruta_duplicada_flag AS "RUTA_DUPLICADA_FLAG",
        ruta_duplicada_rows AS "RUTA_DUPLICADA_ROWS",
        effective_week_start::date AS "SEMANA_INICIO_KEY",
        UPPER(TRIM(COALESCE(CAST(gestor AS text), ''))) AS "GESTOR_NORM_FILTER",
        UPPER(TRIM(COALESCE(CAST(rutero AS text), ''))) AS "RUTERO_NORM_FILTER",
        UPPER(TRIM(COALESCE(CAST(local_nombre AS text), ''))) AS "LOCAL_NORM_FILTER",
        UPPER(TRIM(COALESCE(CAST(cliente AS text), ''))) AS "CLIENTE_NORM_FILTER",
        CASE WHEN visita_realizada_raw >= visita THEN 'CUMPLE' ELSE 'INCUMPLE' END AS "ALERTA_NORM_FILTER",
        CASE
            WHEN COALESCE(ruta_duplicada_flag, 0) = 1
              OR COALESCE(ruta_duplicada_rows, 0) > 1
              OR CAST(gestor AS text) LIKE '%%|%%'
              OR CAST(rutero AS text) LIKE '%%|%%'
            THEN 1
            ELSE 0
        END::integer AS "GESTION_COMPARTIDA_FLAG_CALC",
        GREATEST(COALESCE(visita, 0) - COALESCE(visita_realizada_cap, 0), 0)::integer AS "VISITAS_PENDIENTES_CALC"
    FROM agg
    """


def _delete_weekly_query() -> str:
    return f"""
    DELETE FROM {WEEKLY_FACT} f
    USING (
        SELECT unnest(%s::date[]) AS semana_inicio
    ) aw
    WHERE f."SEMANA_INICIO" = aw.semana_inicio
    """


def _insert_weekly_query() -> str:
    return f"""
    INSERT INTO {WEEKLY_FACT} (
        {_quoted_ident_list(WEEKLY_FACT_COLUMNS)}
    )
    SELECT
        {_stage_select_list(WEEKLY_FACT_COLUMNS)}
    FROM _cg_weekly_stage s
    """


def _fetch_dicts(cur, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cur.execute(query, params)
    keys = [desc[0] for desc in cur.description]
    return [dict(zip(keys, row)) for row in cur.fetchall()]


def _rows_by_week(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {str(row["semana_inicio"]): int(row.get("rows") or 0) for row in rows}


def _route_scope_by_week(cur, validation_weeks: list[str], week_origin_by_week: dict[str, str]) -> list[dict[str, Any]]:
    if not validation_weeks:
        return []
    route_rows = {
        str(row["semana_inicio"]): row
        for row in _fetch_dicts(cur, _route_scope_query(), (validation_weeks,))
    }
    scoped: list[dict[str, Any]] = []
    for week_value in validation_weeks:
        row = route_rows.get(week_value, {})
        rows = int(row.get("rows") or 0)
        visita = int(row.get("visita") or 0)
        scoped.append({
            "semana_inicio": week_value,
            "rows": rows,
            "visita": visita,
            "scope_status": "present" if rows > 0 else "missing",
            "week_origin": week_origin_by_week.get(week_value, "unknown"),
        })
    return scoped


def _run_daily_check(cur, affected_dates: list[str], validate: bool) -> dict[str, Any]:
    metrics = [
        "rows",
        "useful_day",
        "tiene_kpione2",
        "tiene_power_app",
        "kpione1_audit_only",
        "power_app_fallback",
        "raw_evidence_count",
        "kpione2_winner_rows",
        "power_app_winner_rows",
        "no_winner_rows",
    ]
    source_started_ms = _now_ms()
    source_rows = _fetch_dicts(cur, _daily_stats_query(DAILY_SOURCE), (affected_dates,))
    source_elapsed_ms = _now_ms() - source_started_ms
    source_totals = _sum_metric_rows(source_rows, metrics)

    result: dict[str, Any] = {
        "source": DAILY_SOURCE,
        "fact": DAILY_FACT,
        "source_rows_by_date": source_rows,
        "source_totals": source_totals,
        "source_elapsed_ms": source_elapsed_ms,
        "validation_status": "skipped" if not validate else "pending",
    }
    if not validate:
        return result

    if not _fetch_relation_exists(cur, DAILY_FACT):
        result["validation_status"] = "fact_missing"
        result["fact_available"] = False
        return result

    fact_started_ms = _now_ms()
    fact_rows = _fetch_dicts(cur, _daily_stats_query(DAILY_FACT), (affected_dates,))
    fact_elapsed_ms = _now_ms() - fact_started_ms
    result["fact_available"] = True
    result["fact_rows_by_date"] = fact_rows
    result["fact_totals"] = _sum_metric_rows(fact_rows, metrics)
    result["fact_elapsed_ms"] = fact_elapsed_ms
    result["diff"] = _diff_rows(source_rows, fact_rows, key_name="fecha_visita", metrics=metrics)
    result["validation_status"] = "ok" if result["diff"]["ok"] else "diff"
    return result


def _daily_fact_coverage_by_week(
    cur,
    validation_weeks: list[str],
    week_origin_by_week: dict[str, str],
) -> tuple[list[dict[str, Any]], bool]:
    if not validation_weeks:
        return [], _fetch_relation_exists(cur, DAILY_FACT)

    fact_available = _fetch_relation_exists(cur, DAILY_FACT)
    source_rows = _rows_by_week(_fetch_dicts(cur, _daily_week_coverage_query(DAILY_SOURCE), (validation_weeks,)))
    fact_rows: dict[str, int] = {}
    if fact_available:
        fact_rows = _rows_by_week(_fetch_dicts(cur, _daily_week_coverage_query(DAILY_FACT), (validation_weeks,)))

    coverage: list[dict[str, Any]] = []
    for week_value in validation_weeks:
        source_count = int(source_rows.get(week_value, 0))
        fact_count = int(fact_rows.get(week_value, 0))
        if not fact_available:
            coverage_status = "unknown"
        elif fact_count == 0:
            coverage_status = "empty"
        elif source_count == fact_count:
            coverage_status = "complete"
        else:
            coverage_status = "partial"
        coverage.append({
            "semana_inicio": week_value,
            "source_rows": source_count,
            "daily_fact_rows": fact_count,
            "coverage_status": coverage_status,
            "week_origin": week_origin_by_week.get(week_value, "unknown"),
        })
    return coverage, fact_available


def _run_weekly_check(
    cur,
    validation_weeks: list[str],
    derived_weeks: set[date],
    explicit_weeks: set[date],
    safety_weeks: set[date],
    week_origin_by_week: dict[str, str],
    validate: bool,
    require_complete_safety_window: bool,
    post_apply_validate: bool,
) -> dict[str, Any]:
    metrics = [
        "rows",
        "visita",
        "visita_realizada_raw",
        "visita_realizada_cap",
        "visitas_pendientes_calc",
        "cumple_rows",
        "incumple_rows",
        "gestion_compartida_rows",
    ]
    result: dict[str, Any] = {
        "candidate_source": f"{WEEKLY_FREQ} + {DAILY_FACT}",
        "comparison_target": WEEKLY_MV,
        "validation_status": "skipped" if not validate else "pending",
        "derived_weeks_ok": True,
        "explicit_weeks_ok": True,
        "safety_weeks_skipped": [],
        "safety_weeks_with_warnings": [],
        "expected_pre_apply_diff_weeks": [],
        "warnings": [],
    }

    coverage, fact_available = _daily_fact_coverage_by_week(cur, validation_weeks, week_origin_by_week)
    result["daily_fact_available"] = fact_available
    result["daily_fact_coverage_by_week"] = coverage
    if not fact_available:
        result["validation_status"] = "daily_fact_missing"
        result["derived_weeks_ok"] = False
        result["blocking"] = True
        return result

    safety_week_values = {value.isoformat() for value in safety_weeks}
    derived_week_values = {value.isoformat() for value in derived_weeks}
    explicit_week_values = {value.isoformat() for value in explicit_weeks}
    coverage_by_week = {row["semana_inicio"]: row for row in coverage}
    incomplete_derived = [
        week_value for week_value in sorted(derived_week_values)
        if coverage_by_week.get(week_value, {}).get("coverage_status") != "complete"
    ]
    incomplete_safety = [
        week_value for week_value in sorted(safety_week_values)
        if coverage_by_week.get(week_value, {}).get("coverage_status") != "complete"
    ]

    result["safety_weeks_skipped"] = [] if require_complete_safety_window else incomplete_safety
    if incomplete_derived:
        result["warnings"].append("derived_week_incomplete_daily_fact:" + ",".join(incomplete_derived))
        if post_apply_validate:
            result["derived_weeks_ok"] = False
    if incomplete_safety:
        warning_key = "safety_week_incomplete_daily_fact:" + ",".join(incomplete_safety)
        result["warnings"].append(warning_key)
        if require_complete_safety_window:
            result["safety_weeks_with_warnings"] = incomplete_safety

    route_scope = _route_scope_by_week(cur, validation_weeks, week_origin_by_week)
    result["route_scope_by_week"] = route_scope
    route_scope_by_week = {row["semana_inicio"]: row for row in route_scope}
    missing_explicit_route = [
        week_value for week_value in sorted(explicit_week_values)
        if route_scope_by_week.get(week_value, {}).get("scope_status") != "present"
    ]
    if missing_explicit_route:
        result["explicit_weeks_ok"] = False
        result["warnings"].append("explicit_week_no_route_scope:" + ",".join(missing_explicit_route))

    weeks_to_compare = set(derived_week_values | explicit_week_values)
    complete_safety = sorted(safety_week_values - set(incomplete_safety))
    weeks_to_compare.update(complete_safety)
    compare_week_values = sorted(weeks_to_compare)
    result["compared_weeks"] = compare_week_values

    if not validate:
        result["validation_status"] = "skipped"
        result["blocking"] = bool(missing_explicit_route)
        result["warning_only"] = bool(incomplete_safety and not require_complete_safety_window)
        return result

    if missing_explicit_route or (post_apply_validate and incomplete_derived) or (incomplete_safety and require_complete_safety_window):
        result["validation_status"] = "error"
        result["blocking"] = True
        return result

    if not compare_week_values:
        result["validation_status"] = "skipped_incomplete_daily_fact"
        result["blocking"] = False
        result["warning_only"] = bool(incomplete_safety)
        return result

    candidate_started_ms = _now_ms()
    candidate_rows = _fetch_dicts(cur, _weekly_candidate_query(), (compare_week_values,))
    candidate_elapsed_ms = _now_ms() - candidate_started_ms
    result["candidate_rows_by_week"] = candidate_rows
    result["candidate_totals"] = _sum_metric_rows(candidate_rows, metrics)
    result["candidate_elapsed_ms"] = candidate_elapsed_ms

    mv_started_ms = _now_ms()
    mv_rows = _fetch_dicts(cur, _weekly_mv_query(), (compare_week_values,))
    mv_elapsed_ms = _now_ms() - mv_started_ms
    result["mv_rows_by_week"] = mv_rows
    result["mv_totals"] = _sum_metric_rows(mv_rows, metrics)
    result["mv_elapsed_ms"] = mv_elapsed_ms
    result["diff"] = _diff_rows(mv_rows, candidate_rows, key_name="semana_inicio", metrics=metrics)
    diff_weeks = {str(row["semana_inicio"]) for row in result["diff"]["diff_rows"]}
    derived_diff_weeks = sorted(diff_weeks & derived_week_values)
    explicit_diff_weeks = sorted(diff_weeks & explicit_week_values)
    safety_diff_weeks = sorted(diff_weeks & safety_week_values)
    if derived_diff_weeks:
        result["warnings"].append("derived_week_diff:" + ",".join(derived_diff_weeks))
        if post_apply_validate:
            result["derived_weeks_ok"] = False
    if explicit_diff_weeks:
        result["warnings"].append("explicit_week_diff:" + ",".join(explicit_diff_weeks))
        if post_apply_validate:
            result["explicit_weeks_ok"] = False
    if safety_diff_weeks:
        result["safety_weeks_with_warnings"] = safety_diff_weeks
        result["warnings"].append("safety_week_diff:" + ",".join(safety_diff_weeks))

    pre_apply_diff_weeks = sorted(set(incomplete_derived) | set(derived_diff_weeks) | set(explicit_diff_weeks))
    result["expected_pre_apply_diff_weeks"] = [] if post_apply_validate else pre_apply_diff_weeks

    if (
        missing_explicit_route
        or (post_apply_validate and (derived_diff_weeks or explicit_diff_weeks))
        or (safety_diff_weeks and require_complete_safety_window)
    ):
        result["validation_status"] = "diff"
        result["blocking"] = True
    elif pre_apply_diff_weeks and not post_apply_validate:
        result["validation_status"] = "would_update"
        result["blocking"] = False
        result["warning_only"] = True
    elif incomplete_safety:
        result["validation_status"] = "ok_with_skipped_safety_weeks"
        result["blocking"] = False
        result["warning_only"] = True
    elif safety_diff_weeks:
        result["validation_status"] = "diff"
        result["blocking"] = False
        result["warning_only"] = True
    else:
        result["validation_status"] = "ok"
        result["blocking"] = False
        result["warning_only"] = False
    return result


def run_incremental_dry_run(
    *,
    db_url: str,
    week_scope: dict[str, set[date]],
    validate: bool,
    require_complete_safety_window: bool,
    post_apply_validate: bool,
    statement_timeout_seconds: int,
    contract_023: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not db_url:
        raise RuntimeError("NO_DB_URL_AVAILABLE")
    affected_dates = week_scope["requested_affected_dates"]
    validation_weeks = week_scope["validation_weeks"]
    if not affected_dates and not validation_weeks:
        raise RuntimeError("NO_AFFECTED_DATES_OR_WEEKS")

    import psycopg2

    db_url = ensure_sslmode(db_url)
    timeout_ms = _statement_timeout_ms(statement_timeout_seconds)
    requested_affected_date_values = sorted_iso(week_scope["requested_affected_dates"])
    explicit_affected_week_values = sorted_iso(week_scope["explicit_affected_weeks"])
    derived_affected_week_values = sorted_iso(week_scope["derived_affected_weeks"])
    safety_week_values = sorted_iso(week_scope["safety_weeks"])
    validation_week_values = sorted_iso(week_scope["validation_weeks"])
    week_origin_by_week = build_week_origin_by_week(week_scope)
    apply_scope = build_apply_scope(week_scope)

    started_ms = _now_ms()
    result: dict[str, Any] = {
        "phase": PHASE,
        "status": "started",
        "dry_run": True,
        "affected_dates": requested_affected_date_values,
        "affected_weeks": validation_week_values,
        "requested_affected_dates": requested_affected_date_values,
        "explicit_affected_weeks": explicit_affected_week_values,
        "derived_affected_weeks": derived_affected_week_values,
        "safety_weeks": safety_week_values,
        "validation_weeks": validation_week_values,
        "week_origin_by_week": week_origin_by_week,
        **apply_scope,
        "skipped_weeks": [],
        "warnings": [],
        "dry_run_projection": {
            "current_candidate_from_existing_daily": True,
            "projected_candidate_after_daily_apply": False,
            "warning": None,
        },
        "route_scope_by_week": [],
        "daily_fact_coverage_by_week": [],
        "pre_apply_diffs": [],
        "expected_updates": {
            "daily_fact_dates": [],
            "weekly_fact_weeks": [],
        },
        "daily_check": {},
        "weekly_check": {},
        "would_update": {
            "daily_fact": bool(requested_affected_date_values),
            "weekly_fact": bool(validation_week_values),
        },
        "validate": bool(validate),
        "post_apply_validate": bool(post_apply_validate),
        "require_complete_safety_window": bool(require_complete_safety_window),
        "statement_timeout_seconds": int(statement_timeout_seconds),
        "final_status": "dry_run_started",
    }

    with psycopg2.connect(db_url) as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = %s", (timeout_ms,))
            if contract_023 is not None:
                cur.execute(
                    "SELECT current_user,session_user,current_database(),"
                    "current_setting('role'),current_setting('transaction_read_only')"
                )
                current_user, session_user, database, active_role, readonly = cur.fetchone()
                if (
                    current_user != MART_REFRESH_ROLE
                    or session_user != MART_REFRESH_ROLE
                    or current_user != session_user
                ):
                    raise RefreshContractError("mart_refresh_session_role_mismatch")
                if database != EXPECTED_DATABASE or active_role != "none" or readonly != "on":
                    raise RefreshContractError("mart_refresh_dry_run_session_mismatch")
                cur.execute(
                    "SELECT pg_advisory_xact_lock_shared(%s)",
                    (int(contract_023["advisory_lock_key"]),),
                )
                result["content_fingerprints"] = _compute_content_fingerprints(
                    cur, requested_affected_date_values, apply_scope["apply_weekly_weeks"]
                )
            if requested_affected_date_values:
                result["daily_check"] = _run_daily_check(cur, requested_affected_date_values, validate)
            else:
                result["daily_check"] = {
                    "validation_status": "skipped",
                    "reason": "no_affected_dates",
                }
            if validation_week_values:
                result["weekly_check"] = _run_weekly_check(
                    cur,
                    validation_week_values,
                    week_scope["derived_affected_weeks"],
                    week_scope["explicit_affected_weeks"],
                    week_scope["safety_weeks"],
                    week_origin_by_week,
                    validate,
                    require_complete_safety_window,
                    post_apply_validate,
                )
            else:
                result["weekly_check"] = {
                    "validation_status": "skipped",
                    "reason": "no_affected_weeks",
                    "derived_weeks_ok": True,
                    "explicit_weeks_ok": True,
                    "safety_weeks_skipped": [],
                }
            conn.rollback()

    daily_status = result["daily_check"].get("validation_status")
    weekly_check = result["weekly_check"]
    weekly_status = weekly_check.get("validation_status")
    result["route_scope_by_week"] = weekly_check.get("route_scope_by_week", [])
    result["daily_fact_coverage_by_week"] = weekly_check.get("daily_fact_coverage_by_week", [])
    result["skipped_weeks"] = list(weekly_check.get("safety_weeks_skipped", []))
    result["warnings"].extend(result["daily_check"].get("warnings", []))
    result["warnings"].extend(weekly_check.get("warnings", []))

    daily_diff_dates = []
    daily_diff = result["daily_check"].get("diff")
    if isinstance(daily_diff, dict):
        daily_diff_dates = [str(row.get("fecha_visita")) for row in daily_diff.get("diff_rows", [])]
    if daily_status == "diff" and not post_apply_validate:
        result["daily_check"]["validation_status"] = "would_update"
        result["pre_apply_diffs"].append({
            "scope": "daily_source_vs_fact",
            "diff_dates": daily_diff_dates,
            "diff": daily_diff,
        })
        result["expected_updates"]["daily_fact_dates"] = daily_diff_dates or requested_affected_date_values
    if daily_status == "fact_missing":
        result["expected_updates"]["daily_fact_dates"] = requested_affected_date_values

    weekly_diff = weekly_check.get("diff")
    weekly_expected_weeks = list(weekly_check.get("expected_pre_apply_diff_weeks", []))
    if result["expected_updates"]["daily_fact_dates"]:
        projection_warning = "dry_run_weekly_candidate_uses_current_daily_fact_not_projected_stage"
        result["dry_run_projection"]["warning"] = projection_warning
        result["warnings"].append(projection_warning)
    if weekly_expected_weeks:
        result["pre_apply_diffs"].append({
            "scope": "weekly_candidate_vs_mv",
            "diff_weeks": weekly_expected_weeks,
            "diff": weekly_diff,
        })
        result["expected_updates"]["weekly_fact_weeks"] = weekly_expected_weeks

    blocking_statuses = {"fact_missing", "daily_fact_missing"}
    if post_apply_validate:
        blocking_statuses.add("diff")
    weekly_blocking = bool(weekly_check.get("blocking"))
    if daily_status in blocking_statuses or weekly_blocking or weekly_status == "error":
        result["status"] = "error"
        result["final_status"] = "dry_run_error"
    elif result["expected_updates"]["daily_fact_dates"] or result["expected_updates"]["weekly_fact_weeks"]:
        result["status"] = "warn"
        result["final_status"] = "dry_run_would_update"
    elif result["skipped_weeks"] or weekly_check.get("warning_only"):
        result["status"] = "warn"
        result["final_status"] = "dry_run_ok_with_skipped_safety_weeks"
    else:
        result["status"] = "ok"
        result["final_status"] = "dry_run_ok"
    result["elapsed_ms"] = _now_ms() - started_ms
    return result


def _count_temp_rows(cur, relation_name: str) -> int:
    cur.execute(f"SELECT COUNT(*)::bigint AS rows FROM {relation_name}")
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _run_weekly_fact_validation(cur, affected_weeks: list[str]) -> dict[str, Any]:
    metrics = [
        "rows",
        "visita",
        "visita_realizada_raw",
        "visita_realizada_cap",
        "visitas_pendientes_calc",
        "cumple_rows",
        "incumple_rows",
        "gestion_compartida_rows",
    ]
    stage_started_ms = _now_ms()
    stage_rows = _fetch_dicts(cur, _weekly_relation_query("_cg_weekly_stage"), (affected_weeks,))
    stage_elapsed_ms = _now_ms() - stage_started_ms

    fact_started_ms = _now_ms()
    fact_rows = _fetch_dicts(cur, _weekly_relation_query(WEEKLY_FACT), (affected_weeks,))
    fact_elapsed_ms = _now_ms() - fact_started_ms

    diff = _diff_rows(stage_rows, fact_rows, key_name="semana_inicio", metrics=metrics)
    return {
        "source": "_cg_weekly_stage",
        "fact": WEEKLY_FACT,
        "stage_rows_by_week": stage_rows,
        "stage_totals": _sum_metric_rows(stage_rows, metrics),
        "stage_elapsed_ms": stage_elapsed_ms,
        "fact_rows_by_week": fact_rows,
        "fact_totals": _sum_metric_rows(fact_rows, metrics),
        "fact_elapsed_ms": fact_elapsed_ms,
        "diff": diff,
        "validation_status": "ok" if diff["ok"] else "diff",
    }


def _run_incremental_transaction(
    *,
    db_url: str,
    week_scope: dict[str, set[date]],
    statement_timeout_seconds: int,
    post_apply_validate: bool,
    advisory_lock_key: int,
    expected_content_fingerprints: dict[str, Any],
    _marker: object,
    _connect_fn: Any = None,
) -> dict[str, Any]:
    if _marker is not _INTERNAL_APPLY_MARKER:
        raise RefreshContractError("internal_apply_authority_required")
    if not db_url:
        raise RuntimeError("NO_DB_URL_AVAILABLE")

    requested_affected_date_values = sorted_iso(week_scope["requested_affected_dates"])
    derived_affected_week_values = sorted_iso(week_scope["derived_affected_weeks"])
    explicit_affected_week_values = sorted_iso(week_scope["explicit_affected_weeks"])
    safety_week_values = sorted_iso(week_scope["safety_weeks"])
    affected_week_values = sorted_iso(week_scope["strict_affected_weeks"])
    validation_week_values = sorted_iso(week_scope["validation_weeks"])
    week_origin_by_week = build_week_origin_by_week(week_scope)
    apply_scope = build_apply_scope(week_scope)

    if not requested_affected_date_values and not affected_week_values:
        raise RuntimeError("NO_AFFECTED_DATES_OR_WEEKS")

    if _connect_fn is None:
        import psycopg2
        _connect_fn = psycopg2.connect

    db_url = ensure_sslmode(db_url)
    timeout_ms = _statement_timeout_ms(statement_timeout_seconds)
    started_ms = _now_ms()
    result: dict[str, Any] = {
        "phase": PHASE,
        "status": "started",
        "dry_run": False,
        "apply": True,
        "affected_dates": requested_affected_date_values,
        "affected_weeks": affected_week_values,
        "requested_affected_dates": requested_affected_date_values,
        "derived_affected_weeks": derived_affected_week_values,
        "explicit_affected_weeks": explicit_affected_week_values,
        "safety_weeks": safety_week_values,
        "validation_weeks": validation_week_values,
        "week_origin_by_week": week_origin_by_week,
        **apply_scope,
        "warnings": [],
        "route_scope_by_week": [],
        "daily_apply": {
            "deleted_rows": 0,
            "inserted_rows": 0,
            "stage_rows": 0,
            "validation_status": "skipped",
        },
        "weekly_apply": {
            "deleted_rows": 0,
            "inserted_rows": 0,
            "stage_rows": 0,
            "validation_status": "skipped",
        },
        "post_apply_validate": bool(post_apply_validate),
        "statement_timeout_seconds": int(statement_timeout_seconds),
        "commit_state": PRECOMMIT,
        "committed": False,
        "rolled_back": False,
        "final_status": "apply_started",
    }

    conn = None
    committed = False
    try:
        conn = _connect_fn(db_url)
        conn.set_session(readonly=False, autocommit=False)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = %s", (timeout_ms,))
            cur.execute(
                "SELECT current_user,session_user,current_database(),"
                "current_setting('role'),current_setting('transaction_read_only')"
            )
            current_user, session_user, database, active_role, readonly = cur.fetchone()
            if (
                current_user != MART_REFRESH_ROLE
                or session_user != MART_REFRESH_ROLE
                or current_user != session_user
            ):
                raise RefreshContractError("mart_refresh_session_role_mismatch")
            if database != EXPECTED_DATABASE or active_role != "none" or readonly != "off":
                raise RefreshContractError("mart_refresh_apply_session_mismatch")
            cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (int(advisory_lock_key),),
            )
            observed_prestate = _compute_content_fingerprints(
                cur, requested_affected_date_values, affected_week_values
            )
            _assert_fingerprints_match(expected_content_fingerprints, observed_prestate)
            result["content_fingerprints"] = observed_prestate

            if affected_week_values:
                route_scope = _route_scope_by_week(cur, affected_week_values, week_origin_by_week)
                result["route_scope_by_week"] = route_scope
                route_scope_by_week = {row["semana_inicio"]: row for row in route_scope}
                missing_explicit_route = [
                    week_value for week_value in explicit_affected_week_values
                    if route_scope_by_week.get(week_value, {}).get("scope_status") != "present"
                ]
                if missing_explicit_route:
                    raise RuntimeError("explicit_week_no_route_scope:" + ",".join(missing_explicit_route))

            if requested_affected_date_values:
                cur.execute(_create_daily_stage_query(), (requested_affected_date_values,))
                daily_stage_rows = _count_temp_rows(cur, "_cg_daily_stage")
                result["daily_apply"]["stage_rows"] = daily_stage_rows
                if daily_stage_rows == 0:
                    raise RuntimeError("daily_stage_empty_for_affected_dates")

                cur.execute(_delete_daily_query(), (requested_affected_date_values,))
                result["daily_apply"]["deleted_rows"] = int(cur.rowcount or 0)
                cur.execute(_insert_daily_query())
                result["daily_apply"]["inserted_rows"] = int(cur.rowcount or 0)

                daily_validation = _run_daily_check(cur, requested_affected_date_values, True)
                result["daily_apply"]["validation_status"] = daily_validation.get("validation_status", "unknown")
                result["daily_apply"]["validation"] = daily_validation
                if daily_validation.get("validation_status") != "ok":
                    raise RuntimeError("daily_validation_diff")

            if affected_week_values:
                cur.execute(_create_weekly_stage_query(), (affected_week_values,))
                weekly_stage_rows = _count_temp_rows(cur, "_cg_weekly_stage")
                result["weekly_apply"]["stage_rows"] = weekly_stage_rows
                if weekly_stage_rows == 0:
                    raise RuntimeError("weekly_stage_empty_for_affected_weeks")

                cur.execute(_delete_weekly_query(), (affected_week_values,))
                result["weekly_apply"]["deleted_rows"] = int(cur.rowcount or 0)
                cur.execute(_insert_weekly_query())
                result["weekly_apply"]["inserted_rows"] = int(cur.rowcount or 0)

                weekly_validation = _run_weekly_fact_validation(cur, affected_week_values)
                result["weekly_apply"]["validation_status"] = weekly_validation.get("validation_status", "unknown")
                result["weekly_apply"]["validation"] = weekly_validation
                if weekly_validation.get("validation_status") != "ok":
                    raise RuntimeError("weekly_validation_diff")

            conn.commit()
            committed = True
            result["committed"] = True
            result["commit_state"] = COMMITTED_EVIDENCE_PENDING
        result["status"] = "ok"
        result["final_status"] = "apply_ok"
        result["elapsed_ms"] = _now_ms() - started_ms
        return result
    except Exception as exc:
        if conn is not None and not committed:
            conn.rollback()
            result["rolled_back"] = True
        result["status"] = "error"
        result["error"] = str(exc)
        result["final_status"] = "apply_failed_rolled_back" if not committed else "apply_committed_with_post_commit_error"
        result["elapsed_ms"] = _now_ms() - started_ms
        return result
    finally:
        if conn is not None:
            conn.close()


def run_incremental_apply(
    *,
    plan_path: Path,
    confirm: str,
    run_id: str,
    evidence_json: Path,
    dry_run_report_json: Path,
    statement_timeout_seconds: int,
    post_apply_validate: bool,
) -> dict[str, Any]:
    """The only productive entrypoint; authority is reloaded from canonical state."""
    if plan_path.resolve() != PLAN_023_DEFAULT.resolve():
        raise RefreshContractError("canonical_plan_023_path_required")
    plan, authority = _load_plan_023(plan_path, operation="apply")
    _validate_wrapper_marker(MART_REFRESH_APPLY_OPERATION)
    if confirm != APPLY_CONFIRM_TOKEN:
        raise RefreshContractError("plan_023_confirmation_token_mismatch")
    dsn = os.getenv(MART_REFRESH_ENV, "")
    if not dsn:
        raise RefreshContractError("cg_mart_refresh_env_required")
    _validate_productive_dsn(dsn, plan)
    evidence_path = _canonical_evidence_path(
        evidence_json, run_id, "04_june_mart_refresh_apply.json"
    )
    dry_path = dry_run_report_json.resolve()
    try:
        dry_path.relative_to(Path(tempfile.gettempdir()).resolve())
    except ValueError as exc:
        raise RefreshContractError("authorized_dry_run_report_must_be_in_temp") from exc
    dry_bytes = dry_path.read_bytes()
    dry_report = json.loads(dry_bytes.decode("utf-8"))
    expected_fingerprints = dry_report.get("content_fingerprints")
    _assert_fingerprints_match(expected_fingerprints, expected_fingerprints)
    dry_authority = plan.get("dry_run_authorization")
    required_dry = {
        "raw_sha256": _sha256_bytes(dry_bytes),
        "scope_sha256": authority["scope_sha256"],
        "content_fingerprints": expected_fingerprints,
    }
    if not isinstance(dry_authority, dict) or any(
        dry_authority.get(key) != value for key, value in required_dry.items()
    ):
        raise RefreshContractError("dry_run_authorization_link_mismatch")
    payload = plan["scope"]["canonical_payload"]
    dates = {date.fromisoformat(value) for value in payload["affected_dates"]}
    weeks = {date.fromisoformat(value) for value in payload["affected_weeks"]}
    week_scope = build_week_scope(dates, weeks, 0)
    result = _run_incremental_transaction(
        db_url=dsn,
        week_scope=week_scope,
        statement_timeout_seconds=statement_timeout_seconds,
        post_apply_validate=post_apply_validate,
        advisory_lock_key=int(plan["advisory_lock_key"]),
        expected_content_fingerprints=expected_fingerprints,
        _marker=_INTERNAL_APPLY_MARKER,
    )
    if result.get("final_status") != "apply_ok":
        return result
    result.update(authority)
    result.update({
        "document_type": "stock_zero_cg_june_mart_refresh_apply_v1",
        "schema_version": 1,
        "verdict": "PASS_023_JUNE_MART_REFRESH_APPLY",
        "run_id": run_id,
        "scope_sha256": authority["scope_sha256"],
        "dry_run_raw_sha256": required_dry["raw_sha256"],
        "writes_attempted": True,
        "writes_executed": True,
        "transaction_outcome": "COMMITTED",
        "commit_state": COMMITTED_EVIDENCE_PENDING,
        "committed": True,
        "rolled_back": False,
    })
    result["commit_state"] = COMMITTED_EVIDENCE_RECORDED
    try:
        write_json_exclusive(evidence_path, result)
    except Exception as exc:
        result["commit_state"] = COMMITTED_EVIDENCE_PENDING
        receipt = {
            "verdict": COMMITTED_EVIDENCE_RECOVERY_REQUIRED,
            "run_id": run_id,
            "commit_state": COMMITTED_EVIDENCE_PENDING,
            "committed": True,
            "rolled_back": False,
            "target_evidence_path": evidence_path.relative_to(ROOT).as_posix(),
            "approved_git_sha": authority["approved_git_sha"],
            "content_fingerprints": result["content_fingerprints"],
            "evidence_error": type(exc).__name__,
        }
        receipt_path = write_committed_recovery_receipt(receipt)
        receipt["receipt_path"] = str(receipt_path)
        result.update(receipt)
        result["status"] = "error"
        result["final_status"] = "committed_evidence_recovery_required"
        return result
    result["evidence_path"] = evidence_path.relative_to(ROOT).as_posix()
    return result


def _operator_next_step(result: dict[str, Any]) -> str:
    final_status = str(result.get("final_status") or "")
    return {
        "dry_run_ok": "dry_run_clean_no_action_required",
        "dry_run_would_update": "review_expected_updates_then_run_apply_with_confirm_if_approved",
        "dry_run_ok_with_skipped_safety_weeks": "review_skipped_safety_weeks_before_apply",
        "dry_run_error": "fix_blockers_before_apply",
        "apply_ok": "apply_completed_validate_app_or_exports_if_needed",
        "committed_evidence_recovery_required": "recover_evidence_only_do_not_reapply",
        "apply_failed_rolled_back": "review_error_no_commit_expected",
        "real_apply_requires_apply_and_confirm_flags": "rerun_with_dry_run_or_explicit_apply_confirm",
    }.get(final_status, "review_final_status_and_warnings")


def blocked_real_apply_result(
    args: argparse.Namespace,
    week_scope: dict[str, set[date]],
    elapsed_ms: int,
) -> dict[str, Any]:
    raw_dates = set(args.affected_date or [])
    apply_scope = build_apply_scope(week_scope)
    result = {
        "phase": PHASE,
        "status": "error",
        "error": "real_apply_requires_apply_and_confirm_flags",
        "dry_run": False,
        "apply": bool(args.apply),
        "affected_dates": sorted_iso(raw_dates),
        "affected_weeks": sorted_iso(week_scope["validation_weeks"]),
        "requested_affected_dates": sorted_iso(week_scope["requested_affected_dates"]),
        "explicit_affected_weeks": sorted_iso(week_scope["explicit_affected_weeks"]),
        "derived_affected_weeks": sorted_iso(week_scope["derived_affected_weeks"]),
        "safety_weeks": sorted_iso(week_scope["safety_weeks"]),
        "validation_weeks": sorted_iso(week_scope["validation_weeks"]),
        "week_origin_by_week": build_week_origin_by_week(week_scope),
        **apply_scope,
        "skipped_weeks": [],
        "warnings": [],
        "route_scope_by_week": [],
        "daily_fact_coverage_by_week": [],
        "pre_apply_diffs": [],
        "expected_updates": {
            "daily_fact_dates": [],
            "weekly_fact_weeks": [],
        },
        "daily_check": {},
        "weekly_check": {},
        "would_update": {
            "daily_fact": bool(raw_dates),
            "weekly_fact": bool(week_scope["validation_weeks"]),
        },
        "final_status": "real_apply_requires_apply_and_confirm_flags",
        "elapsed_ms": elapsed_ms,
    }
    result["operator_next_step"] = _operator_next_step(result)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Incremental Control Gestion v2 helper. Use dry-run first to inspect "
            "daily dates, weekly scope, safety weeks, and expected updates before "
            "any confirmed apply."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Safe examples:
  python scripts/refresh_control_gestion_v2_incremental.py --help
  python scripts/refresh_control_gestion_v2_incremental.py --dry-run --affected-date 2026-05-18 --validate
  python scripts/refresh_control_gestion_v2_incremental.py --dry-run --affected-week 2026-05-18 --safety-window-weeks 1 --validate

Legacy dry-run can resolve DB_URL_CODEX_RO from env or the local read-only file.
Productive June apply is available only through plan 023 and the credential wrapper;
it never accepts a DSN argument.""",
    )
    parser.add_argument(
        "--db-url",
        default="",
        help=(
            "Legacy dry-run DSN only. Productive plan-023 apply rejects this option."
        ),
    )
    parser.add_argument(
        "--affected-date",
        action="append",
        type=parse_iso_date,
        default=[],
        help="Affected daily date in YYYY-MM-DD format. Repeat for multiple dates.",
    )
    parser.add_argument(
        "--affected-week",
        action="append",
        type=parse_iso_date,
        default=[],
        help="Affected week anchor in YYYY-MM-DD format. Normalized to Monday week start.",
    )
    parser.add_argument(
        "--safety-window-weeks",
        type=int,
        default=1,
        help="Number of prior weeks to inspect as safety context. Use 0 to disable.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Compare sources and targets and report validation_status fields.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run read-only inspection and rollback; does not write fact tables.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Request real apply mode. Must be paired with --confirm-real-apply.",
    )
    parser.add_argument(
        "--confirm-real-apply",
        action="store_true",
        help="Legacy flag retained for CLI compatibility; it cannot authorize writes.",
    )
    parser.add_argument("--plan-023", type=Path)
    parser.add_argument("--confirm", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--report-json", type=Path)
    parser.add_argument("--evidence-json", type=Path)
    parser.add_argument("--dry-run-report-json", type=Path)
    parser.add_argument(
        "--post-apply-validate",
        action="store_true",
        help="Treat remaining validation diffs as blockers after apply-style validation.",
    )
    parser.add_argument(
        "--require-complete-safety-window",
        action="store_true",
        help="Block when safety weeks have incomplete daily fact coverage.",
    )
    parser.add_argument(
        "--statement-timeout-seconds",
        type=int,
        default=DEFAULT_STATEMENT_TIMEOUT_SECONDS,
        help="Statement timeout for DB operations, in seconds. Use 0 for no timeout.",
    )
    return parser


def main() -> int:
    started_ms = _now_ms()
    parser = build_parser()
    args = parser.parse_args()

    if args.safety_window_weeks < 0:
        parser.error("--safety-window-weeks must be >= 0")

    contract_mode = args.plan_023 is not None
    raw_dates = set(args.affected_date or [])
    raw_weeks = set(args.affected_week or [])
    plan: dict[str, Any] | None = None
    authority: dict[str, str] = {}
    contract_runtime: dict[str, Any] | None = None
    evidence_path: Path | None = None

    if not args.dry_run and not args.apply:
        week_scope = build_week_scope(raw_dates, raw_weeks, args.safety_window_weeks)
        print(json.dumps(
            blocked_real_apply_result(args, week_scope, _now_ms() - started_ms),
            ensure_ascii=False,
            indent=2,
            default=str,
        ))
        return 1

    try:
        if args.apply and not contract_mode:
            raise RefreshContractError("plan_023_required_for_any_apply")
        if contract_mode:
            if raw_dates or raw_weeks or args.safety_window_weeks != 0:
                raise RefreshContractError("plan_023_scope_override_forbidden")
            operation = "dry-run" if args.dry_run and not args.apply else "apply"
            plan, authority = _load_plan_023(args.plan_023, operation=operation)
            payload = plan["scope"]["canonical_payload"]
            raw_dates = {date.fromisoformat(value) for value in payload["affected_dates"]}
            raw_weeks = {date.fromisoformat(value) for value in payload["affected_weeks"]}
            week_scope = build_week_scope(raw_dates, raw_weeks, 0)
            expected_operation = (
                MART_REFRESH_DRY_RUN_OPERATION if operation == "dry-run"
                else MART_REFRESH_APPLY_OPERATION
            )
            _validate_wrapper_marker(expected_operation)
            expected_token = DRY_RUN_CONFIRM_TOKEN if operation == "dry-run" else APPLY_CONFIRM_TOKEN
            if args.confirm != expected_token:
                raise RefreshContractError("plan_023_confirmation_token_mismatch")
            if args.db_url:
                raise RefreshContractError("explicit_db_url_forbidden_for_plan_023")
            dsn = os.getenv(MART_REFRESH_ENV, "")
            if not dsn:
                raise RefreshContractError("cg_mart_refresh_env_required")
            _validate_productive_dsn(dsn, plan)
            contract_runtime = {
                "advisory_lock_key": int(plan["advisory_lock_key"]),
            }
            if operation == "dry-run":
                if args.report_json is None:
                    raise RefreshContractError("dry_run_report_json_required")
                report_path = _temporary_report_path(args.report_json)
                resolved_db_url, db_url_source = dsn, "wrapper:DB_URL_CG_MART_REFRESH"
            else:
                if args.dry_run or args.confirm_real_apply:
                    raise RefreshContractError("legacy_apply_flags_forbidden")
                if args.dry_run_report_json is None or not args.dry_run_report_json.is_file():
                    raise RefreshContractError("authorized_dry_run_report_required")
                dry_run_report_path = args.dry_run_report_json.resolve()
                temporary_root = Path(tempfile.gettempdir()).resolve()
                try:
                    dry_run_report_path.relative_to(temporary_root)
                except ValueError as exc:
                    raise RefreshContractError("authorized_dry_run_report_must_be_in_temp") from exc
                if args.evidence_json is None:
                    raise RefreshContractError("june_refresh_evidence_path_required")
                evidence_path = _canonical_evidence_path(
                    args.evidence_json, args.run_id, "04_june_mart_refresh_apply.json"
                )
                dry_bytes = dry_run_report_path.read_bytes()
                dry_hash = _sha256_bytes(dry_bytes)
                dry_report = json.loads(dry_bytes.decode("utf-8"))
                dry_authority = plan.get("dry_run_authorization")
                if not isinstance(dry_authority, dict):
                    raise RefreshContractError("dry_run_authorization_missing")
                required_dry = {
                    "raw_sha256": dry_hash,
                    "scope_sha256": authority["scope_sha256"],
                    "content_fingerprints": dry_report.get("content_fingerprints"),
                }
                if any(dry_authority.get(key) != value for key, value in required_dry.items()):
                    raise RefreshContractError("dry_run_authorization_link_mismatch")
                contract_runtime["expected_content_fingerprints"] = required_dry["content_fingerprints"]
                contract_runtime["dry_run_raw_sha256"] = dry_hash
                resolved_db_url, db_url_source = dsn, "wrapper:DB_URL_CG_MART_REFRESH"
        else:
            week_scope = build_week_scope(raw_dates, raw_weeks, args.safety_window_weeks)
            resolved_db_url, db_url_source = resolve_readonly_db_url_for_dry_run(args)

        if args.dry_run:
            result = run_incremental_dry_run(
                db_url=resolved_db_url,
                week_scope=week_scope,
                validate=args.validate,
                require_complete_safety_window=args.require_complete_safety_window,
                post_apply_validate=args.post_apply_validate,
                statement_timeout_seconds=args.statement_timeout_seconds,
                contract_023=contract_runtime,
            )
            if contract_mode:
                result.update(authority)
                result.update({
                    "document_type": "stock_zero_cg_june_refresh_dry_run_v1",
                    "verdict": "PASS_023_JUNE_REFRESH_DRY_RUN"
                    if result.get("status") in {"ok", "warn"} else "BLOCKED",
                    "scope_sha256": authority["scope_sha256"],
                    "writes_attempted": False,
                    "writes_executed": False,
                    "transaction_outcome": "ROLLED_BACK_READ_ONLY",
                })
                _write_json_once(report_path, result)
        else:
            result = run_incremental_apply(
                plan_path=args.plan_023,
                confirm=args.confirm,
                run_id=args.run_id,
                evidence_json=args.evidence_json,
                dry_run_report_json=args.dry_run_report_json,
                statement_timeout_seconds=args.statement_timeout_seconds,
                post_apply_validate=args.post_apply_validate,
            )
        result["db_url_source"] = db_url_source
        result["operator_next_step"] = _operator_next_step(result)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        if result.get("final_status") == "committed_evidence_recovery_required":
            return 2
        return 0 if result.get("status") in {"ok", "warn"} else 1
    except Exception as exc:
        if not contract_mode:
            _, db_url_source = resolve_readonly_db_url_for_dry_run(args)
        else:
            db_url_source = "wrapper" if os.getenv(MART_REFRESH_ENV) else "missing"
        if 'week_scope' not in locals():
            week_scope = build_week_scope(raw_dates, raw_weeks, 0)
        error_result = {
            "phase": PHASE,
            "status": "error",
            "dry_run": bool(args.dry_run),
            "apply": bool(args.apply),
            "affected_dates": sorted_iso(week_scope["requested_affected_dates"]),
            "affected_weeks": sorted_iso(week_scope["validation_weeks"]),
            "requested_affected_dates": sorted_iso(week_scope["requested_affected_dates"]),
            "explicit_affected_weeks": sorted_iso(week_scope["explicit_affected_weeks"]),
            "derived_affected_weeks": sorted_iso(week_scope["derived_affected_weeks"]),
            "safety_weeks": sorted_iso(week_scope["safety_weeks"]),
            "validation_weeks": sorted_iso(week_scope["validation_weeks"]),
            "week_origin_by_week": build_week_origin_by_week(week_scope),
            **build_apply_scope(week_scope),
            "skipped_weeks": [],
            "warnings": [],
            "route_scope_by_week": [],
            "daily_fact_coverage_by_week": [],
            "pre_apply_diffs": [],
            "expected_updates": {
                "daily_fact_dates": [],
                "weekly_fact_weeks": [],
            },
            "daily_check": {},
            "weekly_check": {},
            "would_update": {
                "daily_fact": bool(raw_dates),
                "weekly_fact": bool(week_scope["validation_weeks"]),
            },
            "db_url_source": db_url_source,
            "error": str(exc) if isinstance(exc, RefreshContractError) else type(exc).__name__,
            "final_status": "dry_run_error" if args.dry_run else "apply_failed_rolled_back",
            "elapsed_ms": _now_ms() - started_ms,
        }
        error_result["operator_next_step"] = _operator_next_step(error_result)
        print(json.dumps(error_result, ensure_ascii=False, indent=2, default=str))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
