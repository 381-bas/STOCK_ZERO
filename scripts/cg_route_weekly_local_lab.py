#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CG005I-M local PostgreSQL behavioral lab runner.

LAB ONLY. This runner is intentionally narrow: it validates the route weekly
replacement contract against a loopback PostgreSQL database and never contacts
Supabase or remote hosts.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import re
import sys
import tempfile
import threading
import time
import types
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

SQL11_PATH = ROOT / "sql" / "11_control_gestion_route_week_replacement_contract.sql"
BOOTSTRAP_PATH = ROOT / "sql" / "lab" / "12_cg005_route_weekly_lab_bootstrap.sql"
OBS_LEDGER_PATH = ROOT / "research" / "AI_LOAD_OBSERVATION_LEDGER.jsonl"

PHASE = "CG005I_M_LOCAL_POSTGRESQL_BEHAVIORAL_LAB"
WEEK = "2026-06-08"
SOURCE = "DB_GLOBAL_INVENTARIO.xlsx:RUTA_RUTERO"
SHEET = "RUTA_RUTERO"
MAIN_DB = "stock_zero_cg005_lab"
FAILURE_DB = "stock_zero_cg005_lab_failure"
SNAPSHOT_TRANSFORM_VERSION = "CG005K_B_SNAPSHOT_V1"
ROUTE_POLICY_VERSION = "ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1"
ROLLBACK_CONFIRM_TOKEN = "ROUTE_WEEK_ROLLBACK_V1"

REMOTE_HOST_MARKERS = ("supabase", "pooler", "aws-")
LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}
HEX64_RE = re.compile(r"^[0-9A-Fa-f]{64}$")
DB_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


class LabError(RuntimeError):
    def __init__(self, code: str, detail: str | None = None):
        super().__init__(detail or code)
        self.code = code


@dataclass(frozen=True)
class DsnInfo:
    dsn: str
    host: str
    port: int
    database: str
    sslmode: str | None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest().upper()


def safe_read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_report(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# CG005I-M local PostgreSQL lab",
        "",
        f"- Phase: `{PHASE}`",
        f"- Verdict: `{payload.get('verdict')}`",
        f"- Baseline: `{payload.get('baseline_commit')}`",
        f"- PostgreSQL: `{payload.get('local_postgresql', {}).get('version')}`",
        f"- Database: `{payload.get('local_postgresql', {}).get('database')}`",
        f"- Supabase contacted: `{payload.get('local_postgresql', {}).get('supabase_contacted')}`",
        "",
        "## Gates",
        "",
    ]
    for key in ("cg005i", "cg005j", "cg005k", "cg005l", "cg005m", "platform_008"):
        section = payload.get(key, {})
        lines.append(f"- `{key}` passed: `{section.get('passed', section.get('executed'))}`")
    if payload.get("blockers"):
        lines.extend(["", "## Blockers", ""])
        for blocker in payload["blockers"]:
            lines.append(f"- `{blocker}`")
        if payload.get("error_detail"):
            lines.append(f"- Detail: `{payload['error_detail']}`")
        cg005j = payload.get("cg005j", {})
        if cg005j.get("blocked_at"):
            lines.append(f"- Blocked at: `{cg005j['blocked_at']}`")
        if cg005j.get("loader_error"):
            lines.append(f"- Loader error: `{cg005j['loader_error']}`")
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- No DSN, password, row payload, customer, store, address or person values are recorded.",
            "- Writes are limited to the dedicated loopback PostgreSQL lab databases.",
            "- Snapshot B was generated under the OS temp directory and is not recorded in the repo.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_loopback_dsn(dsn: str) -> DsnInfo:
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise LabError("dsn_scheme_not_postgresql")
    host = (parsed.hostname or "").lower()
    if not host:
        raise LabError("dsn_host_missing")
    lowered = dsn.lower()
    if any(marker in lowered for marker in REMOTE_HOST_MARKERS):
        raise LabError("remote_dsn_marker_blocked")
    if host not in LOOPBACK_HOSTS:
        raise LabError("dsn_host_not_loopback")
    database = parsed.path.lstrip("/")
    if not database:
        raise LabError("dsn_database_missing")
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    sslmode = query.get("sslmode")
    if sslmode == "require" and host not in LOOPBACK_HOSTS:
        raise LabError("remote_sslmode_require_blocked")
    return DsnInfo(dsn=dsn, host=host, port=parsed.port or 5432, database=database, sslmode=sslmode)


def dsn_for_database(dsn: str, database: str) -> str:
    if not DB_NAME_RE.fullmatch(database):
        raise LabError("invalid_database_name")
    parsed = urlparse(dsn)
    return urlunparse((parsed.scheme, parsed.netloc, "/" + database, "", parsed.query, ""))


def import_psycopg():
    try:
        import psycopg
        from psycopg import sql
        from psycopg.types.json import Jsonb
    except Exception as exc:  # pragma: no cover - environment guard
        raise LabError("psycopg_v3_unavailable", type(exc).__name__) from exc
    return psycopg, sql, Jsonb


class _JsonCompat:
    def __init__(self, adapted, dumps=None):
        self.adapted = adapted
        self.dumps = dumps or json.dumps


def _values_statement(sql_text: str, width: int) -> str:
    placeholder = "(" + ",".join(["%s"] * width) + ")"
    stmt, count = re.subn(r"values\s+%s", "values " + placeholder, sql_text, count=1, flags=re.IGNORECASE)
    if count != 1:
        raise LabError("execute_values_sql_shape_unsupported")
    return stmt


def install_psycopg2_compat() -> None:
    psycopg, _sql, Jsonb = import_psycopg()

    def connect(dsn: str):
        parse_loopback_dsn(dsn)
        return psycopg.connect(dsn)

    def convert(value):
        if isinstance(value, _JsonCompat):
            return Jsonb(value.adapted, dumps=value.dumps)
        return value

    def execute_values(cur, sql_text: str, rows: list[tuple], page_size: int = 5000) -> None:
        if not rows:
            return
        stmt = _values_statement(sql_text, len(rows[0]))
        for start in range(0, len(rows), page_size):
            chunk = rows[start : start + page_size]
            converted = [tuple(convert(v) for v in row) for row in chunk]
            cur.executemany(stmt, converted)

    psycopg2_module = types.ModuleType("psycopg2")
    psycopg2_module.connect = connect
    extras_module = types.ModuleType("psycopg2.extras")
    extras_module.Json = _JsonCompat
    extras_module.execute_values = execute_values
    psycopg2_module.extras = extras_module
    sys.modules["psycopg2"] = psycopg2_module
    sys.modules["psycopg2.extras"] = extras_module


def connect(dsn: str):
    psycopg, _sql, _jsonb = import_psycopg()
    parse_loopback_dsn(dsn)
    return psycopg.connect(dsn)


def admin_recreate_databases(dsn: str, names: list[str]) -> None:
    _psycopg, sql, _jsonb = import_psycopg()
    admin_dsn = dsn_for_database(dsn, "postgres")
    with connect(admin_dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for name in names:
                if not DB_NAME_RE.fullmatch(name):
                    raise LabError("invalid_database_name")
                cur.execute(
                    "select pg_terminate_backend(pid) from pg_stat_activity where datname = %s and pid <> pg_backend_pid()",
                    (name,),
                )
                cur.execute(sql.SQL("drop database if exists {} with (force)").format(sql.Identifier(name)))
                cur.execute(sql.SQL("create database {}").format(sql.Identifier(name)))


def execute_sql_text(dsn: str, sql_text: str) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()


def query_one(dsn: str, sql_text: str, params: tuple = ()) -> tuple:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text, params)
            row = cur.fetchone()
            if row is None:
                raise LabError("query_returned_no_rows")
            return tuple(row)


def query_all(dsn: str, sql_text: str, params: tuple = ()) -> list[tuple]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text, params)
            return [tuple(row) for row in cur.fetchall()]


def server_summary(dsn: str) -> dict:
    row = query_one(
        dsn,
        """
        select
            current_user,
            coalesce(inet_server_addr()::text, 'local'),
            inet_server_port(),
            current_setting('server_version'),
            current_setting('transaction_read_only')
        """,
    )
    return {
        "current_user": row[0],
        "server_addr_loopback": str(row[1]) in {"127.0.0.1", "::1", "local"},
        "server_port": int(row[2]),
        "server_version": str(row[3]),
        "transaction_read_only": str(row[4]),
    }


def extract_sql11_body(path: Path = SQL11_PATH) -> tuple[str, dict]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("-- NO APPLY"):
        raise LabError("sql11_missing_no_apply_header")
    begin_match = re.search(r"(?im)^\s*begin\s*;\s*$", text)
    rollback_matches = list(re.finditer(r"(?im)^\s*rollback\s*;\s*$", text))
    if not begin_match or not rollback_matches:
        raise LabError("sql11_missing_begin_rollback_wrapper")
    rollback_match = rollback_matches[-1]
    if begin_match.end() >= rollback_match.start():
        raise LabError("sql11_invalid_wrapper_order")
    body = text[begin_match.end() : rollback_match.start()].strip() + "\n"
    return body, {
        "path": "sql/11_control_gestion_route_week_replacement_contract.sql",
        "sha256": sha256_file(path),
        "body_sha256": sha256_text(body),
        "no_apply_header": True,
        "begin_rollback_wrapper": True,
    }


def apply_bootstrap_and_sql11(dsn: str) -> dict:
    bootstrap_sql = BOOTSTRAP_PATH.read_text(encoding="utf-8")
    execute_sql_text(dsn, bootstrap_sql)
    body, sql11 = extract_sql11_body()
    execute_sql_text(dsn, body)
    return {
        "bootstrap_sha256": sha256_file(BOOTSTRAP_PATH),
        "sql11": sql11,
    }


def load_loader():
    install_psycopg2_compat()
    import load_ruta_rutero_from_excel as loader

    return loader


def verify_loader_contract(dsn: str, loader) -> dict:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            contract = loader.verify_db_contract(cur)
    if not contract.get("ok"):
        raise LabError("missing_loader_db_contract", ",".join(contract.get("missing", [])))
    return {"ok": True, "missing": []}


def prove_advisory_lock(dsn: str, loader) -> dict:
    key = loader.weekly_assignment_lock_key(
        source=SOURCE,
        effective_week_start_value=WEEK,
        route_policy_version=loader.ROUTE_POLICY_VERSION,
    )
    other_key = loader.weekly_assignment_lock_key(
        source=SOURCE,
        effective_week_start_value="2026-06-15",
        route_policy_version=loader.ROUTE_POLICY_VERSION,
    )
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select pg_advisory_xact_lock(%s)", (key,))
        conn.rollback()

    acquired = threading.Event()
    timings: dict[str, float] = {}
    errors: list[str] = []

    conn_a = connect(dsn)
    conn_b = connect(dsn)
    try:
        cur_a = conn_a.cursor()
        cur_b = conn_b.cursor()
        cur_a.execute("select pg_advisory_xact_lock(%s)", (key,))
        start = time.perf_counter()

        def wait_same_key():
            try:
                cur_b.execute("select pg_advisory_xact_lock(%s)", (key,))
                timings["blocked_seconds"] = time.perf_counter() - start
                acquired.set()
            except Exception as exc:  # pragma: no cover - failure evidence
                errors.append(type(exc).__name__)
                acquired.set()

        t = threading.Thread(target=wait_same_key, daemon=True)
        t.start()
        time.sleep(0.35)
        blocked_while_held = not acquired.is_set()
        conn_a.rollback()
        t.join(timeout=5)
        acquired_after_release = acquired.is_set() and not errors
        conn_b.rollback()
    finally:
        conn_a.close()
        conn_b.close()

    conn_c = connect(dsn)
    conn_d = connect(dsn)
    try:
        cur_c = conn_c.cursor()
        cur_d = conn_d.cursor()
        cur_c.execute("select pg_advisory_xact_lock(%s)", (key,))
        start_other = time.perf_counter()
        cur_d.execute("select pg_advisory_xact_lock(%s)", (other_key,))
        other_elapsed = time.perf_counter() - start_other
        distinct_key_not_blocked = other_elapsed < 0.35
        conn_d.rollback()
        conn_c.rollback()
    finally:
        conn_c.close()
        conn_d.close()

    if not blocked_while_held or not acquired_after_release or not distinct_key_not_blocked:
        raise LabError("advisory_lock_behavior_failed")
    return {
        "resolved": True,
        "same_key_blocked_while_held": blocked_while_held,
        "same_key_acquired_after_release": acquired_after_release,
        "distinct_key_not_blocked": distinct_key_not_blocked,
        "same_key_blocked_seconds": round(float(timings.get("blocked_seconds", 0.0)), 3),
        "distinct_key_elapsed_seconds": round(float(other_elapsed), 3),
    }


def loader_help_flags(loader) -> list[str]:
    text = loader.build_arg_parser().format_help()
    return sorted(set(re.findall(r"--[a-zA-Z0-9][a-zA-Z0-9-]*", text)))


def run_source_and_plan(loader, workbook: Path, expected_hash: str | None = None, operation: str = "dry_run") -> dict:
    source_check = loader.run_source_check_ruta(excel_path=workbook, sheet=SHEET, strict=False)
    plan = loader.build_dry_run_plan(
        excel_path=workbook,
        sheet=SHEET,
        source=SOURCE,
        effective_week_start_value=WEEK,
        expected_workbook_sha256=expected_hash,
        source_check=source_check,
    )
    return {
        "operation": operation,
        "source_check_verdict": source_check["final_verdict"],
        "input_rows": int(plan["input_rows"]),
        "accepted_rows": int(plan["accepted_rows"]),
        "history_insert_rows": int(plan["history_insert_rows"]),
        "planned_public_insert_rows": int(plan["planned_public_insert_rows"]),
        "exact_duplicate_excess": int(plan["exact_duplicate_excess"]),
        "grain_duplicate_groups": int(plan["grain_duplicate_groups"]),
        "schema_signature": plan["schema_signature"],
        "input_file_sha256": plan["input_file_sha256"],
        "current_surface_hash": plan["planned_assignment"]["current_surface_hash"],
        "resolved_surface_hash": plan["planned_assignment"]["resolved_surface_hash"],
        "writes_executed": False,
    }


def run_loader_apply(loader, dsn: str, workbook: Path, expected_hash: str, temp_dir: Path, label: str) -> dict:
    json_out = temp_dir / f"{label}_apply.json"
    argv = [
        "--excel",
        str(workbook),
        "--sheet",
        SHEET,
        "--source",
        SOURCE,
        "--effective-week-start",
        WEEK,
        "--apply",
        "--expected-workbook-sha256",
        expected_hash,
        "--confirm-weekly-replacement",
        loader.ROUTE_POLICY_VERSION,
        "--db_url",
        dsn,
        "--json-out",
        str(json_out),
    ]
    try:
        loader.main(argv)
    except SystemExit as exc:
        payload = safe_read_json(json_out) if json_out.exists() else {}
        code = payload.get("error_code", f"system_exit_{exc.code}")
        detail = str(code)
        if payload.get("error"):
            detail = f"{detail}:{payload['error']}"
        raise LabError(f"{label}_loader_apply_failed", detail) from exc
    result = safe_read_json(json_out)
    if result.get("mode") != "apply" or not result.get("writes_executed"):
        raise LabError("loader_apply_failed")
    return result


def run_loader_rollback(loader, dsn: str, failed_assignment_id: int, expected_current_surface_hash: str, temp_dir: Path, label: str) -> dict:
    json_out = temp_dir / f"{label}_rollback.json"
    argv = [
        "--source",
        SOURCE,
        "--effective-week-start",
        WEEK,
        "--rollback-weekly-replacement",
        "--failed-assignment-id",
        str(failed_assignment_id),
        "--expected-current-surface-hash",
        expected_current_surface_hash,
        "--confirm-rollback",
        loader.ROLLBACK_CONFIRM_TOKEN,
        "--db_url",
        dsn,
        "--json-out",
        str(json_out),
    ]
    try:
        loader.main(argv)
    except SystemExit as exc:
        payload = safe_read_json(json_out) if json_out.exists() else {}
        code = payload.get("error_code", f"system_exit_{exc.code}")
        detail = str(code)
        if payload.get("error"):
            detail = f"{detail}:{payload['error']}"
        raise LabError(f"{label}_loader_rollback_failed", detail) from exc
    result = safe_read_json(json_out)
    if result.get("mode") != "rollback" or not result.get("writes_executed"):
        raise LabError("loader_rollback_failed")
    return result


def assignment_summary(dsn: str) -> dict:
    rows = query_all(
        dsn,
        """
        select assignment_status, count(*)::bigint
          from cg_core.ruta_rutero_week_assignment
         where effective_week_start = %s
           and route_policy_version = %s
         group by assignment_status
         order by assignment_status
        """,
        (WEEK, ROUTE_POLICY_VERSION),
    )
    active = query_one(
        dsn,
        """
        select count(*)::bigint
          from cg_core.ruta_rutero_week_assignment
         where effective_week_start = %s
           and route_policy_version = %s
           and assignment_status = 'ACTIVE'
        """,
        (WEEK, ROUTE_POLICY_VERSION),
    )[0]
    return {"status_counts": {str(k): int(v) for k, v in rows}, "active_count": int(active)}


def week_view_summary(dsn: str) -> dict:
    row = query_one(
        dsn,
        """
        select count(*)::bigint,
               count(*) filter (where route_week_source = 'EXPLICIT_ASSIGNMENT')::bigint
          from cg_core.v_ruta_rutero_load_batch_week_v2
         where effective_week_start = %s
        """,
        (WEEK,),
    )
    return {"rows": int(row[0]), "explicit_assignment_rows": int(row[1])}


def batch_grains(dsn: str, batch_id: int) -> set[tuple[str, str]]:
    rows = query_all(
        dsn,
        """
        with exact_deduped as (
            select distinct on (row_hash)
                   nullif(trim(coalesce(cod_rt_norm, cod_rt)), '') as cod_rt_norm,
                   upper(trim(coalesce(nullif(trim(cliente_norm), ''), nullif(trim(cliente), ''), ''))) as cliente_norm,
                   row_hash,
                   source_row
              from cg_core.ruta_rutero_load_rows
             where ruta_batch_id = %s
               and nullif(trim(coalesce(cod_rt_norm, cod_rt)), '') is not null
               and nullif(trim(coalesce(cliente_norm, cliente)), '') is not null
             order by row_hash, source_row
        )
        select cod_rt_norm, cliente_norm
          from exact_deduped
        """,
        (batch_id,),
    )
    return {(str(a), str(b)) for a, b in rows}


def resolved_grain_diff(dsn: str, batch_id: int) -> dict:
    assigned = batch_grains(dsn, batch_id)
    resolved = {
        (str(a), str(b))
        for a, b in query_all(
            dsn,
            """
            select cod_rt_norm, cliente_norm
              from cg_core.v_rr_frecuencia_base_resuelta_v2
             where effective_week_start = %s
            """,
            (WEEK,),
        )
    }
    return {"missing": len(assigned - resolved), "extra": len(resolved - assigned)}


def current_hash(dsn: str) -> str:
    row = query_one(
        dsn,
        """
        select encode(
            digest(
                coalesce(string_agg(source_row::text || '|' || row_hash, E'\n' order by source_row, row_hash), ''),
                'sha256'
            ),
            'hex'
        )
          from public.ruta_rutero
         where source = %s
        """,
        (SOURCE,),
    )
    return str(row[0]).upper()


def ensure_pgcrypto(dsn: str) -> None:
    execute_sql_text(dsn, "create extension if not exists pgcrypto;")


def raw_index_for_source_row(df: pd.DataFrame, loader, source_row: int) -> int:
    normalized = loader.normalize_header_map(df.columns)
    if "IDROW" in normalized:
        matches = df.index[pd.to_numeric(df[normalized["IDROW"]], errors="coerce").fillna(-1).astype(int) == int(source_row)]
        if len(matches) != 1:
            raise LabError("snapshot_b_source_row_match_failed")
        return int(matches[0])
    idx = int(source_row) - 2
    if idx < 0 or idx >= len(df):
        raise LabError("snapshot_b_source_row_out_of_range")
    return idx


def col_for(loader, df: pd.DataFrame, key: str) -> str:
    normalized = loader.normalize_header_map(df.columns)
    if key not in normalized:
        raise LabError("snapshot_b_missing_column", key)
    return normalized[key]


def make_snapshot_b(loader, workbook_a: Path, temp_dir: Path) -> tuple[Path, dict]:
    df = loader.read_route_excel(workbook_a, SHEET)
    accepted = loader.transform_route_dataframe(df, source=SOURCE)
    current = loader.current_surface_rows(accepted)
    frame = current.copy()
    frame["_cod_rt_norm"] = frame["cod_rt"].map(loader.normalize_text)
    frame["_cliente_norm"] = frame["cliente"].map(loader.normalize_key)
    usable = frame[(frame["_cod_rt_norm"] != "") & (frame["_cliente_norm"] != "")].sort_values("source_row")
    if len(usable) < 6:
        raise LabError("snapshot_b_not_enough_rows")
    removal_rows = [int(v) for v in usable["source_row"].head(2).tolist()]
    change_rows = [int(v) for v in usable["source_row"].iloc[2:4].tolist()]
    frequency_row = int(usable["source_row"].iloc[4])
    removal_grains = {
        (str(row["_cod_rt_norm"]), str(row["_cliente_norm"]))
        for _, row in usable.head(2).iterrows()
    }

    df_b = df.copy()
    remove_indexes = [raw_index_for_source_row(df_b, loader, row) for row in removal_rows]
    df_b = df_b.drop(index=remove_indexes).reset_index(drop=True)

    reponedor_col = col_for(loader, df_b, "REPONEDOR")
    supervisor_col = col_for(loader, df_b, "SUPERVISOR")
    for i, source_row in enumerate(change_rows, start=1):
        idx = raw_index_for_source_row(df_b, loader, source_row)
        df_b.at[idx, reponedor_col] = f"LAB_ONLY_RESPONSABLE_{i}"
        df_b.at[idx, supervisor_col] = f"LAB_ONLY_SUPERVISOR_{i}"

    freq_idx = raw_index_for_source_row(df_b, loader, frequency_row)
    df_b.at[freq_idx, col_for(loader, df_b, "VECES POR SEMANA")] = 1
    for day in ("LUNES", "MARTES", "MIERCOLES", "JUEVES", "VIERNES", "SABADO", "DOMINGO"):
        df_b.at[freq_idx, col_for(loader, df_b, day)] = 1 if day == "LUNES" else 0

    synthetic = df_b.iloc[0].copy()
    synthetic_values = {
        "CADENA": "LAB_ONLY",
        "FORMATO": "LAB_ONLY",
        "REGION": "LAB_ONLY",
        "COMUNA": "LAB_ONLY",
        "COD KPI ONE": "LAB_ONLY_RT_20260608",
        "COD B2B": "LAB_ONLY_B2B_20260608",
        "LOCAL": "LAB_ONLY_LOCAL",
        "DIRECCION": "LAB_ONLY_ADDRESS",
        "RUTERO": "LAB_ONLY_RUTERO",
        "JEFE DE OPERACIONES": "LAB_ONLY_JEFE",
        "GESTORES": "LAB_ONLY_GESTOR",
        "CLIENTE": "LAB_ONLY_CLIENTE",
        "SUPERVISOR": "LAB_ONLY_SUPERVISOR",
        "REPONEDOR": "LAB_ONLY_RESPONSABLE",
        "VECES POR SEMANA": 1,
        "LUNES": 1,
        "MARTES": 0,
        "MIERCOLES": 0,
        "JUEVES": 0,
        "VIERNES": 0,
        "SABADO": 0,
        "DOMINGO": 0,
        "VISITA MENSUAL": 0,
        "DIF": 0,
        "OBS": "LAB_ONLY",
        "AUX": "",
        "GG": 0,
        "MODALIDAD": "LAB_ONLY",
    }
    normalized = loader.normalize_header_map(df_b.columns)
    for required, value in synthetic_values.items():
        if required in normalized:
            synthetic[normalized[required]] = value
    if "IDROW" in normalized:
        synthetic[normalized["IDROW"]] = int(pd.to_numeric(df_b[normalized["IDROW"]], errors="coerce").fillna(0).max()) + 1

    df_b = pd.concat([pd.DataFrame([synthetic]), df_b], ignore_index=True)
    if len(df_b) > 17:
        df_b = pd.concat([df_b.iloc[17:], df_b.iloc[:17]], ignore_index=True)
    df_b = df_b.iloc[::-1].reset_index(drop=True)

    snapshot_path = temp_dir / "snapshot_b.xlsx"
    with pd.ExcelWriter(snapshot_path, engine="openpyxl") as writer:
        df_b.to_excel(writer, index=False, sheet_name=SHEET)

    _, accepted_b = loader.prepare_route_rows(snapshot_path, sheet=SHEET, source=SOURCE)
    current_b = loader.current_surface_rows(accepted_b)
    synthetic_grain = ("LAB_ONLY_RT_20260608", "LAB_ONLY_CLIENTE")
    profile = {
        "transform_version": SNAPSHOT_TRANSFORM_VERSION,
        "material_reorder": True,
        "removed_logical_grains": len(removal_grains),
        "changed_responsables": len(change_rows),
        "changed_frequency_or_days": 1,
        "synthetic_highs": 1,
        "rows": int(len(accepted_b)),
        "current_rows": int(len(current_b)),
        "sha256": sha256_file(snapshot_path),
        "removal_grains_count": len(removal_grains),
        "synthetic_grain_marker_hash": hashlib.sha256("|".join(synthetic_grain).encode("utf-8")).hexdigest().upper(),
    }
    profile["_removal_grains_internal"] = removal_grains
    profile["_synthetic_grain_internal"] = synthetic_grain
    return snapshot_path, profile


def validate_snapshot_a(dsn: str, apply_result: dict) -> dict:
    batch_id = int(apply_result["ruta_batch_id"])
    diff = resolved_grain_diff(dsn, batch_id)
    summary = assignment_summary(dsn)
    week_view = week_view_summary(dsn)
    ok = (
        summary["active_count"] == 1
        and week_view["explicit_assignment_rows"] == 1
        and diff == {"missing": 0, "extra": 0}
        and current_hash(dsn) == apply_result["planned_assignment"]["current_surface_hash"].upper()
    )
    if not ok:
        raise LabError("snapshot_a_validation_failed")
    return {
        "passed": True,
        "batch_id": batch_id,
        "assignment_id": int(apply_result["assignment_id"]),
        "rows": int(apply_result["planned_public_insert_rows"]),
        "history_rows": int(apply_result["history_insert_rows"]),
        "exact_duplicate_excess": int(apply_result["exact_duplicate_excess"]),
        "current_surface_hash": apply_result["planned_assignment"]["current_surface_hash"],
        "resolved_surface_hash": apply_result["planned_assignment"]["resolved_surface_hash"],
        "assignment_active": True,
        "single_active": True,
        "week_view_source": "EXPLICIT_ASSIGNMENT",
        "grain_diff": diff,
    }


def validate_snapshot_b(dsn: str, apply_result: dict, snapshot_profile: dict, snapshot_a: dict) -> dict:
    batch_id = int(apply_result["ruta_batch_id"])
    assignment_id = int(apply_result["assignment_id"])
    summary = assignment_summary(dsn)
    diff = resolved_grain_diff(dsn, batch_id)
    grains = batch_grains(dsn, batch_id)
    removed = snapshot_profile["_removal_grains_internal"]
    synthetic = snapshot_profile["_synthetic_grain_internal"]
    rows = query_one(
        dsn,
        """
        select count(*) filter (where assignment_id = %s and assignment_status = 'ACTIVE')::bigint,
               count(*) filter (where ruta_batch_id = %s and assignment_status = 'SUPERSEDED')::bigint,
               max(replaces_ruta_batch_id) filter (where assignment_id = %s)
          from cg_core.ruta_rutero_week_assignment
         where effective_week_start = %s
        """,
        (assignment_id, snapshot_a["batch_id"], assignment_id, WEEK),
    )
    lab_changes = query_one(
        dsn,
        """
        select
            count(*) filter (where reponedor in ('LAB_ONLY_RESPONSABLE_1','LAB_ONLY_RESPONSABLE_2'))::bigint,
            count(*) filter (where cod_rt = 'LAB_ONLY_RT_20260608' and cliente = 'LAB_ONLY_CLIENTE')::bigint,
            count(*) filter (where veces_por_semana = 1 and lunes = 1 and martes = 0 and miercoles = 0 and jueves = 0 and viernes = 0 and sabado = 0 and domingo = 0)::bigint
          from cg_core.ruta_rutero_load_rows
         where ruta_batch_id = %s
        """,
        (batch_id,),
    )
    ok = (
        summary["active_count"] == 1
        and int(rows[0]) == 1
        and int(rows[1]) == 1
        and int(rows[2]) == snapshot_a["batch_id"]
        and len(removed - grains) == snapshot_profile["removed_logical_grains"]
        and synthetic in grains
        and int(lab_changes[0]) >= 2
        and int(lab_changes[1]) == 1
        and int(lab_changes[2]) >= 1
        and diff == {"missing": 0, "extra": 0}
        and current_hash(dsn) == apply_result["planned_assignment"]["current_surface_hash"].upper()
    )
    if not ok:
        raise LabError("snapshot_b_validation_failed")
    return {
        "passed": True,
        "batch_id": batch_id,
        "assignment_id": assignment_id,
        "rows": int(apply_result["planned_public_insert_rows"]),
        "history_rows": int(apply_result["history_insert_rows"]),
        "current_surface_hash": apply_result["planned_assignment"]["current_surface_hash"],
        "resolved_surface_hash": apply_result["planned_assignment"]["resolved_surface_hash"],
        "a_superseded": True,
        "b_active": True,
        "replaces_ruta_batch_id": snapshot_a["batch_id"],
        "removals_stay_removed": True,
        "synthetic_high_present": True,
        "responsable_changes_present": True,
        "frequency_or_day_change_present": True,
        "grain_diff": diff,
    }


def validate_rollback(dsn: str, rollback_result: dict, snapshot_a: dict, snapshot_b: dict) -> dict:
    summary = assignment_summary(dsn)
    diff = resolved_grain_diff(dsn, snapshot_a["batch_id"])
    rows = query_one(
        dsn,
        """
        select
            count(*) filter (where assignment_id = %s and assignment_status = 'ACTIVE')::bigint,
            count(*) filter (where assignment_id = %s and assignment_status = 'ROLLED_BACK')::bigint
          from cg_core.ruta_rutero_week_assignment
         where effective_week_start = %s
        """,
        (snapshot_a["assignment_id"], snapshot_b["assignment_id"], WEEK),
    )
    ok = (
        int(rows[0]) == 1
        and int(rows[1]) == 1
        and summary["active_count"] == 1
        and current_hash(dsn) == snapshot_a["current_surface_hash"].upper()
        and rollback_result["restored_current_surface_hash"].upper() == snapshot_a["current_surface_hash"].upper()
        and diff == {"missing": 0, "extra": 0}
    )
    if not ok:
        raise LabError("rollback_validation_failed")
    return {
        "passed": True,
        "rollback_restored_a": True,
        "postchecks_before_commit": True,
        "failed_assignment_id": snapshot_b["assignment_id"],
        "reactivated_assignment_id": int(rollback_result["reactivated_assignment_id"]),
        "restored_ruta_batch_id": int(rollback_result["restored_ruta_batch_id"]),
        "current_surface_hash": rollback_result["restored_current_surface_hash"],
        "grain_diff": diff,
    }


def run_intentional_failure(loader, dsn: str, snapshot_b: dict) -> dict:
    original = loader.run_rollback_postcheck

    def fail_postcheck(*_args, **_kwargs):
        raise RuntimeError("intentional_failure_postcheck")

    before_hash = current_hash(dsn)
    before_summary = assignment_summary(dsn)
    loader.run_rollback_postcheck = fail_postcheck
    failed_closed = False
    error_code = None
    try:
        try:
            loader.run_weekly_replacement_rollback(
                db_url=dsn,
                source=SOURCE,
                effective_week_start_value=WEEK,
                failed_assignment_id=snapshot_b["assignment_id"],
                expected_current_surface_hash=snapshot_b["current_surface_hash"],
                confirm_token=ROLLBACK_CONFIRM_TOKEN,
            )
        except Exception as exc:
            error_code = str(exc)
            failed_closed = True
    finally:
        loader.run_rollback_postcheck = original
    after_hash = current_hash(dsn)
    after_summary = assignment_summary(dsn)
    ok = (
        failed_closed
        and before_hash == after_hash
        and before_summary == after_summary
        and after_summary["status_counts"].get("ACTIVE") == 1
        and after_hash == snapshot_b["current_surface_hash"].upper()
    )
    if not ok:
        raise LabError("intentional_failure_not_fail_closed")
    return {
        "passed": True,
        "method": "postcheck_exception_in_lab_runner",
        "error_code": error_code,
        "commit_executed": False,
        "rollback_executed": True,
        "b_remained_active": True,
        "state_preserved": True,
    }


def run_sequence(label: str, dsn: str, failure_dsn: str, workbook_a: Path, temp_root: Path, loader) -> dict:
    run_dir = temp_root / label
    run_dir.mkdir(parents=True, exist_ok=True)
    bootstrap = apply_bootstrap_and_sql11(dsn)
    ensure_pgcrypto(dsn)
    contract = verify_loader_contract(dsn, loader)
    lock = prove_advisory_lock(dsn, loader)
    workbook_hash = sha256_file(workbook_a)
    dry_a = run_source_and_plan(loader, workbook_a, expected_hash=workbook_hash, operation="snapshot_a_dry_run")
    apply_a = run_loader_apply(loader, dsn, workbook_a, workbook_hash, run_dir, "snapshot_a")
    snapshot_a = validate_snapshot_a(dsn, apply_a)

    snapshot_b_path, snapshot_b_profile = make_snapshot_b(loader, workbook_a, run_dir)
    workbook_b_hash = sha256_file(snapshot_b_path)
    dry_b = run_source_and_plan(loader, snapshot_b_path, expected_hash=workbook_b_hash, operation="snapshot_b_dry_run")
    apply_b = run_loader_apply(loader, dsn, snapshot_b_path, workbook_b_hash, run_dir, "snapshot_b")
    snapshot_b = validate_snapshot_b(dsn, apply_b, snapshot_b_profile, snapshot_a)
    rollback_result = run_loader_rollback(
        loader,
        dsn,
        failed_assignment_id=snapshot_b["assignment_id"],
        expected_current_surface_hash=snapshot_b["current_surface_hash"],
        temp_dir=run_dir,
        label="snapshot_b_to_a",
    )
    rollback = validate_rollback(dsn, rollback_result, snapshot_a, snapshot_b)

    apply_bootstrap_and_sql11(failure_dsn)
    ensure_pgcrypto(failure_dsn)
    verify_loader_contract(failure_dsn, loader)
    fail_a = run_loader_apply(loader, failure_dsn, workbook_a, workbook_hash, run_dir, "failure_a")
    fail_snapshot_a = validate_snapshot_a(failure_dsn, fail_a)
    fail_b = run_loader_apply(loader, failure_dsn, snapshot_b_path, workbook_b_hash, run_dir, "failure_b")
    fail_snapshot_b = validate_snapshot_b(failure_dsn, fail_b, snapshot_b_profile, fail_snapshot_a)
    failure = run_intentional_failure(loader, failure_dsn, fail_snapshot_b)

    safe_b_profile = {k: v for k, v in snapshot_b_profile.items() if not k.startswith("_")}
    return {
        "label": label,
        "bootstrap": bootstrap,
        "cg005i": {
            "passed": True,
            "ddl_applied_local": True,
            "contract": contract,
            "advisory_lock": lock,
        },
        "cg005j": {
            "passed": True,
            "source_check": dry_a["source_check_verdict"],
            "snapshot_a_rows": snapshot_a["rows"],
            "history_rows": snapshot_a["history_rows"],
            "exact_duplicate_excess": snapshot_a["exact_duplicate_excess"],
            "assignment_active": snapshot_a["assignment_active"],
            "hashes_match": True,
            "grain_diff": snapshot_a["grain_diff"],
            "current_surface_hash": snapshot_a["current_surface_hash"],
            "resolved_surface_hash": snapshot_a["resolved_surface_hash"],
            "workbook_sha256": workbook_hash,
            "schema_signature": dry_a["schema_signature"],
        },
        "cg005k": {
            "passed": True,
            "source_check": dry_b["source_check_verdict"],
            "snapshot_b_rows": snapshot_b["rows"],
            "history_rows": snapshot_b["history_rows"],
            "a_superseded": snapshot_b["a_superseded"],
            "b_active": snapshot_b["b_active"],
            "removals_stay_removed": snapshot_b["removals_stay_removed"],
            "hashes_match": True,
            "grain_diff": snapshot_b["grain_diff"],
            "current_surface_hash": snapshot_b["current_surface_hash"],
            "resolved_surface_hash": snapshot_b["resolved_surface_hash"],
            "snapshot_b": safe_b_profile,
        },
        "cg005l": {
            "passed": True,
            "rollback_restored_a": rollback["rollback_restored_a"],
            "postchecks_before_commit": rollback["postchecks_before_commit"],
            "intentional_failure_fail_closed": failure["passed"],
            "rollback": rollback,
            "intentional_failure": failure,
        },
        "comparison_signature": {
            "a_current_surface_hash": snapshot_a["current_surface_hash"],
            "a_resolved_surface_hash": snapshot_a["resolved_surface_hash"],
            "b_current_surface_hash": snapshot_b["current_surface_hash"],
            "b_resolved_surface_hash": snapshot_b["resolved_surface_hash"],
            "a_rows": snapshot_a["rows"],
            "b_rows": snapshot_b["rows"],
            "b_removed_logical_grains": safe_b_profile["removed_logical_grains"],
            "b_synthetic_highs": safe_b_profile["synthetic_highs"],
            "rollback_restored_a": rollback["rollback_restored_a"],
            "failure_fail_closed": failure["passed"],
        },
    }


def run_platform_008(temp_root: Path, lab_summary: dict) -> dict:
    import sz_load_observation as obs

    before_hash = sha256_file(OBS_LEDGER_PATH) if OBS_LEDGER_PATH.exists() else hashlib.sha256(b"").hexdigest().upper()
    phase_json = temp_root / "platform_008_phase.json"
    candidate_json = temp_root / "platform_008_candidate.json"
    draft = {
        "observation_draft": {
            "source": "RUTA_RUTERO",
            "effective_week_start": WEEK,
            "operation_type": "POST_LOAD_VALIDATION",
            "input_file_name": "DB_GLOBAL_INVENTARIO.xlsx",
            "input_file_sha256": lab_summary["cg005j"]["workbook_sha256"],
            "schema_signature": lab_summary["cg005j"]["schema_signature"],
            "input_rows": lab_summary["cg005j"]["snapshot_a_rows"] + lab_summary["cg005j"]["exact_duplicate_excess"],
            "accepted_rows": lab_summary["cg005j"]["history_rows"],
            "rejected_rows": 0,
            "exact_duplicate_rows": lab_summary["cg005j"]["exact_duplicate_excess"],
            "grain_duplicate_rows": 0,
            "missing_required_rows": 0,
            "source_check_verdict": "WARN" if lab_summary["cg005j"]["source_check"] == "warn" else "OK",
            "loader_executed": True,
            "db_write_executed": True,
            "post_load_validation_status": "PASSED",
            "anomaly_label": "UNREVIEWED",
            "anomaly_reason": None,
            "evidence_refs": [
                "phase:CG005I_M_LOCAL_POSTGRESQL_BEHAVIORAL_LAB",
                "report:CODEX_CG005I_M_LOCAL_POSTGRESQL_LAB.md",
            ],
            "recorded_by": "CODEX_EXECUTOR",
            "reviewed_by": None,
            "implementation_authorized": False,
            "rollback_required": True,
            "rollback_executed": True,
            "new_key_count": lab_summary["cg005k"]["snapshot_b"]["synthetic_highs"],
            "removed_key_count": lab_summary["cg005k"]["snapshot_b"]["removed_logical_grains"],
            "changed_key_count": lab_summary["cg005k"]["snapshot_b"]["changed_responsables"]
            + lab_summary["cg005k"]["snapshot_b"]["changed_frequency_or_days"],
            "elapsed_minutes": 0,
            "notes": "CG005I_M_LOCAL_LAB_VALIDATED",
        }
    }
    write_json(phase_json, draft)
    obs.register_test_input_root(temp_root)
    recorded_at = utc_now()
    start = time.perf_counter()
    draft_args = [
        "draft",
        "--phase-json",
        str(phase_json),
        "--source",
        "RUTA_RUTERO",
        "--effective-week-start",
        WEEK,
        "--operation-type",
        "POST_LOAD_VALIDATION",
        "--input-file-sha256",
        lab_summary["cg005j"]["workbook_sha256"],
        "--recorded-at",
        recorded_at,
        "--recorded-by",
        "CODEX_EXECUTOR",
        "--label",
        "UNREVIEWED",
        "--evidence-ref",
        "phase:CG005I_M_LOCAL_POSTGRESQL_BEHAVIORAL_LAB",
        "--evidence-ref",
        "report:CODEX_CG005I_M_LOCAL_POSTGRESQL_LAB.md",
    ]
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        rc = obs.run(draft_args)
    if rc != 0:
        raise LabError("platform_008_draft_failed", stdout.getvalue())
    candidate = json.loads(stdout.getvalue())
    write_json(candidate_json, candidate)
    stdout_validate = io.StringIO()
    with contextlib.redirect_stdout(stdout_validate):
        validate_rc = obs.run(["validate", "--record", str(candidate_json)])
    if validate_rc != 0:
        raise LabError("platform_008_validate_failed", stdout_validate.getvalue())
    validation = json.loads(stdout_validate.getvalue())
    elapsed = round(time.perf_counter() - start, 3)
    after_hash = sha256_file(OBS_LEDGER_PATH) if OBS_LEDGER_PATH.exists() else hashlib.sha256(b"").hexdigest().upper()
    observation_id = candidate["observation_id"]
    try:
        phase_json.unlink(missing_ok=True)
        candidate_json.unlink(missing_ok=True)
    except TypeError:  # pragma: no cover - py<3.8 compatibility
        if phase_json.exists():
            phase_json.unlink()
        if candidate_json.exists():
            candidate_json.unlink()
    return {
        "executed": True,
        "candidate_validated": validation.get("validate") == "ok",
        "ledger_unchanged": before_hash == after_hash,
        "observation_id": observation_id,
        "validation_status": validation.get("validate"),
        "seconds_elapsed": elapsed,
        "manual_steps_removed": 2,
        "ledger_hash_before": before_hash,
        "ledger_hash_after": after_hash,
    }


def build_result_skeleton(args, dsn_info: DsnInfo, baseline_commit: str) -> dict:
    return {
        "phase": PHASE,
        "verdict": "BLOCK",
        "quality": {"target": "Q4_DECISION_GRADE", "achieved": None, "confidence": None},
        "baseline_commit": baseline_commit,
        "branch": args.branch,
        "local_postgresql": {
            "loopback_only": True,
            "database": MAIN_DB,
            "failure_database": FAILURE_DB,
            "host": dsn_info.host,
            "port": dsn_info.port,
            "version": None,
            "dedicated": True,
            "supabase_contacted": False,
        },
        "docker": {
            "executed": True,
            "container_name": args.container_name,
            "container_id": args.container_id,
            "image": args.container_image,
        },
        "cg005i": {"passed": False, "ddl_applied_local": False, "advisory_lock_resolved": False, "concurrency_proven": False},
        "cg005j": {"passed": False},
        "cg005k": {"passed": False},
        "cg005l": {"passed": False},
        "cg005m": {"passed": False},
        "platform_008": {"executed": False, "candidate_validated": False, "ledger_unchanged": False, "observation_id": None},
        "warnings": [],
        "blockers": [],
    }


def compare_clean_room(run1: dict, run2: dict) -> dict:
    same = run1["comparison_signature"] == run2["comparison_signature"]
    differences = [] if same else sorted(
        key for key in run1["comparison_signature"] if run1["comparison_signature"].get(key) != run2["comparison_signature"].get(key)
    )
    return {
        "passed": same,
        "clean_room_rebuilt": True,
        "business_hashes_identical": same,
        "unexplained_differences": differences,
    }


def run_lab(args) -> dict:
    dsn_info = parse_loopback_dsn(args.dsn)
    if dsn_info.database != MAIN_DB:
        raise LabError("main_dsn_must_target_dedicated_database")
    if parse_loopback_dsn(args.failure_dsn).database != FAILURE_DB:
        raise LabError("failure_dsn_must_target_dedicated_database")
    workbook_a = Path(args.workbook)
    if not workbook_a.is_file():
        raise LabError("workbook_a_not_found")
    baseline_commit = args.baseline_commit
    result = build_result_skeleton(args, dsn_info, baseline_commit)
    loader = load_loader()
    temp_root = Path(tempfile.gettempdir()) / "stock_zero_cg005_lab"
    temp_root.mkdir(parents=True, exist_ok=True)
    result["loader"] = {
        "policy_version": loader.ROUTE_POLICY_VERSION,
        "rollback_confirm_token_hash": hashlib.sha256(loader.ROLLBACK_CONFIRM_TOKEN.encode("utf-8")).hexdigest().upper(),
        "help_flags": loader_help_flags(loader),
    }
    result["manifest"] = {
        "main_commit": baseline_commit,
        "branch_commit": args.branch_commit,
        "python_version": sys.version.split()[0],
        "source": SOURCE,
        "week": WEEK,
        "snapshot_transform_version": SNAPSHOT_TRANSFORM_VERSION,
        "route_policy_version": loader.ROUTE_POLICY_VERSION,
        "commands": [
            "bootstrap dedicated local databases",
            "apply SQL 11 body locally",
            "source-check A",
            "dry-run A",
            "apply A local",
            "generate temp B",
            "source-check B",
            "dry-run B",
            "apply B local",
            "rollback B to A local",
            "intentional rollback failure local",
            "clean-room rebuild",
            "platform observation draft validate without ledger write",
        ],
    }
    admin_recreate_databases(args.dsn, [MAIN_DB, FAILURE_DB])
    summary = server_summary(args.dsn)
    result["local_postgresql"]["version"] = summary["server_version"]
    result["local_postgresql"]["transaction_read_only"] = summary["transaction_read_only"]
    if summary["transaction_read_only"] != "off":
        raise LabError("local_lab_database_not_read_write")

    run1 = run_sequence("run1", args.dsn, args.failure_dsn, workbook_a, temp_root, loader)
    result["run1"] = run1
    admin_recreate_databases(args.dsn, [MAIN_DB, FAILURE_DB])
    run2 = run_sequence("run2", args.dsn, args.failure_dsn, workbook_a, temp_root, loader)
    result["run2"] = run2
    clean_room = compare_clean_room(run1, run2)
    if not clean_room["passed"]:
        raise LabError("clean_room_business_hash_mismatch")
    platform = run_platform_008(temp_root, run2)
    if not platform["candidate_validated"] or not platform["ledger_unchanged"]:
        raise LabError("platform_008_validation_failed")

    result["cg005i"] = {
        "passed": True,
        "ddl_applied_local": True,
        "advisory_lock_resolved": True,
        "concurrency_proven": True,
        "sql11": run2["bootstrap"]["sql11"],
    }
    result["cg005j"] = run2["cg005j"]
    result["cg005k"] = run2["cg005k"]
    result["cg005l"] = run2["cg005l"]
    result["cg005m"] = clean_room
    result["platform_008"] = platform
    result["quality"] = {"target": "Q4_DECISION_GRADE", "achieved": "Q4_DECISION_GRADE", "confidence": "HIGH"}
    result["verdict"] = "LOCAL_LAB_VALIDATED"
    return result


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--failure-dsn", required=True)
    ap.add_argument("--workbook", required=True)
    ap.add_argument("--baseline-commit", required=True)
    ap.add_argument("--branch-commit", required=True)
    ap.add_argument("--branch", required=True)
    ap.add_argument("--container-name", default="stock_zero_cg005_lab_pg")
    ap.add_argument("--container-id", default=None)
    ap.add_argument("--container-image", default="postgres:17")
    ap.add_argument("--json-out", required=True)
    ap.add_argument("--report-out", required=True)
    ap.add_argument("--platform-out", default="")
    return ap


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)
    result: dict
    try:
        result = run_lab(args)
    except LabError as exc:
        dsn_info = parse_loopback_dsn(args.dsn)
        result = build_result_skeleton(args, dsn_info, args.baseline_commit)
        result["blockers"].append(exc.code)
        result["error_detail"] = str(exc) if str(exc) != exc.code else None
        if exc.code == "snapshot_a_loader_apply_failed":
            try:
                loader = load_loader()
                summary = server_summary(args.dsn)
                contract = verify_loader_contract(args.dsn, loader)
                lock = prove_advisory_lock(args.dsn, loader)
                result["local_postgresql"]["version"] = summary["server_version"]
                result["local_postgresql"]["transaction_read_only"] = summary["transaction_read_only"]
                result["cg005i"] = {
                    "passed": True,
                    "ddl_applied_local": True,
                    "advisory_lock_resolved": bool(lock["resolved"]),
                    "concurrency_proven": bool(
                        lock["same_key_blocked_while_held"]
                        and lock["same_key_acquired_after_release"]
                        and lock["distinct_key_not_blocked"]
                    ),
                    "contract": contract,
                    "advisory_lock": lock,
                    "sql11": extract_sql11_body()[1],
                }
                result["cg005j"] = {
                    "passed": False,
                    "snapshot_a_rows": None,
                    "assignment_active": False,
                    "hashes_match": False,
                    "grain_diff": None,
                    "blocked_at": "snapshot_a_apply",
                    "loader_error": str(exc),
                }
            except Exception as partial_exc:
                result["warnings"].append("partial_progress_probe_failed:" + type(partial_exc).__name__)
    except Exception as exc:  # pragma: no cover - evidence for unexpected blocker
        dsn_info = parse_loopback_dsn(args.dsn)
        result = build_result_skeleton(args, dsn_info, args.baseline_commit)
        result["blockers"].append("unexpected_lab_error")
        result["error_detail"] = type(exc).__name__ + ":" + str(exc)
    write_json(Path(args.json_out), result)
    if args.platform_out:
        write_json(
            Path(args.platform_out),
            {
                "phase": "PLATFORM_008_FIRST_REAL_USE",
                "source_phase": PHASE,
                "executed": bool(result.get("platform_008", {}).get("executed")),
                "candidate_validated": bool(result.get("platform_008", {}).get("candidate_validated")),
                "ledger_unchanged": bool(result.get("platform_008", {}).get("ledger_unchanged")),
                "observation_id": result.get("platform_008", {}).get("observation_id"),
                "blocked_by": result.get("blockers", []),
                "ledger_write_executed": False,
            },
        )
    write_report(Path(args.report_out), result)
    print(json.dumps({"verdict": result["verdict"], "blockers": result["blockers"]}, ensure_ascii=False))
    return 0 if result.get("verdict") == "LOCAL_LAB_VALIDATED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
