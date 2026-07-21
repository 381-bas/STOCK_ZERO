from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

import psycopg


ROOT = Path(__file__).resolve().parents[1]
PLAN_DEFAULT = ROOT / "plans" / "023_control_gestion_route_b_bridge_refresh_plan.json"
PLAN_TYPE = "stock_zero_control_gestion_route_b_bridge_refresh_plan_v1"
MODES = ("baseline", "postcheck", "app")
EXPECTED_HOST = "db.xheyrgfagpoigpgakilu.supabase.co"
EXPECTED_DATABASE = "postgres"
EXPECTED_PROJECT = "xheyrgfagpoigpgakilu"
EXPECTED_RO_ROLE = "stock_zero_codex_ro"
EXPECTED_APP_ROLE = "stock_zero_app_ro"
APP_ALLOWLIST_PENDING = "PENDING_PRODUCTIVE_READONLY_DISCOVERY"
APP_ALLOWLIST_FROZEN = "FROZEN_PRODUCTIVE_READONLY_ALLOWLIST"
EXPECTED_WEEKS = [
    "2026-06-01", "2026-06-08", "2026-06-15", "2026-06-22", "2026-06-29",
]
EXPECTED_DATES = [f"2026-06-{day:02d}" for day in range(1, 31)]
BRIDGE_SIGNATURE = (
    ("semana_inicio", "date"), ("fecha_visita", "date"), ("cod_rt", "text"),
    ("cod_b2b", "text"), ("cliente", "text"), ("cliente_norm", "text"),
    ("local_nombre", "text"), ("gestor", "text"), ("gestor_norm", "text"),
    ("rutero", "text"), ("reponedor_scope", "text"),
    ("reponedor_scope_norm", "text"), ("supervisor", "text"),
    ("jefe_operaciones", "text"), ("modalidad", "text"),
    ("semana_iso", "integer"), ("fuente_ganadora", "text"),
    ("fuentes_presentes", "text"), ("tiene_kpione2", "integer"),
    ("tiene_power_app", "integer"), ("tiene_kpione1", "integer"),
    ("power_app_fallback", "integer"), ("kpione1_audit_only", "integer"),
    ("useful_day", "integer"), ("raw_evidence_count", "integer"),
    ("same_source_multimark", "integer"), ("multisource_overlap", "integer"),
    ("kpione_rows_dia", "integer"), ("kpione2_rows_dia", "integer"),
    ("power_app_rows_dia", "integer"), ("persona_conflicto_rows_dia", "integer"),
    ("match_quality", "text"), ("registro_fuera_cruce", "text"),
)
AUTHORIZATION_NAMES = {
    "provision_refresh_role_authorized",
    "apply_bridge_authorized",
    "apply_june_refresh_authorized",
    "runtime_app_validation_authorized",
}
EXPECTED_TRANSITIONS = {
    "CONTRACT_READY_GATE_CLOSED": ["PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED"],
    "PRODUCTIVE_INFRASTRUCTURE_READY_GATE_CLOSED": [
        "BRIDGE_COMMITTED_REFRESH_PENDING_GATE_CLOSED"
    ],
    "BRIDGE_COMMITTED_REFRESH_PENDING_GATE_CLOSED": [
        "BRIDGE_AND_REFRESH_COMMITTED_APP_VALIDATION_PENDING"
    ],
    "BRIDGE_AND_REFRESH_COMMITTED_APP_VALIDATION_PENDING": ["COMPLETE_GATE_CLOSED"],
    "COMPLETE_GATE_CLOSED": [],
}
MODE_OPERATION = {
    "baseline": "readonly-baseline-023",
    "postcheck": "readonly-postcheck-023",
    "app": "validate-app-readonly-023",
}
MODE_CONFIRMATION_KEY = {
    "baseline": "readonly_baseline",
    "postcheck": "readonly_postcheck",
    "app": "runtime_app_validation",
}


class ReadonlyValidationError(RuntimeError):
    pass


def canonical_json_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def grain_metrics(rows: Iterable[tuple[Any, ...]]) -> dict[str, int]:
    multiplicities: dict[tuple[Any, ...], int] = {}
    null_or_blank = 0
    row_count = 0
    for row in rows:
        key = tuple(row)
        row_count += 1
        if any(value is None or (isinstance(value, str) and not value.strip()) for value in key):
            null_or_blank += 1
        multiplicities[key] = multiplicities.get(key, 0) + 1
    duplicate_keys = sum(1 for count in multiplicities.values() if count > 1)
    return {
        "row_count": row_count,
        "distinct_grain_count": len(multiplicities),
        "duplicate_key_count": duplicate_keys,
        "max_multiplicity": max(multiplicities.values(), default=0),
        "null_or_blank_key_rows": null_or_blank,
    }


def validate_static_plan_contract(plan: dict[str, Any]) -> None:
    contract = plan.get("state_contract")
    if not isinstance(contract, dict):
        raise ReadonlyValidationError("state_contract_missing")
    if contract.get("states") != list(EXPECTED_TRANSITIONS):
        raise ReadonlyValidationError("state_contract_states_mismatch")
    if contract.get("transitions") != EXPECTED_TRANSITIONS:
        raise ReadonlyValidationError("state_contract_transitions_mismatch")
    authorizations = plan.get("authorizations")
    if not isinstance(authorizations, dict) or set(authorizations) != AUTHORIZATION_NAMES:
        raise ReadonlyValidationError("authorization_contract_mismatch")
    if any(type(value) is not bool for value in authorizations.values()):
        raise ReadonlyValidationError("authorization_values_must_be_boolean")
    active = [name for name, enabled in authorizations.items() if enabled]
    if len(active) > 1:
        raise ReadonlyValidationError("multiple_authorizations_active")
    gate_open = plan.get("gate_open") is True
    productive = plan.get("productive_actions_authorized") is True
    if bool(active) != gate_open or gate_open != productive:
        raise ReadonlyValidationError("gate_authorization_coherence_failed")
    if active and re.fullmatch(r"[0-9a-f]{40}", str(plan.get("approved_git_sha", ""))) is None:
        raise ReadonlyValidationError("approved_git_sha_required_when_authorized")
    if plan.get("status") not in EXPECTED_TRANSITIONS:
        raise ReadonlyValidationError("unknown_plan_status")
    if plan.get("target") != {
        "project_ref": EXPECTED_PROJECT,
        "hostname": EXPECTED_HOST,
        "database": EXPECTED_DATABASE,
        "sslmode": "require",
    }:
        raise ReadonlyValidationError("registered_target_contract_mismatch")
    payload = plan.get("scope", {}).get("canonical_payload", {})
    if payload.get("affected_dates") != EXPECTED_DATES or payload.get("affected_weeks") != EXPECTED_WEEKS:
        raise ReadonlyValidationError("exact_june_scope_required")
    evidence = plan.get("evidence_contract", {})
    if evidence.get("closure_file_allowed") is not False:
        raise ReadonlyValidationError("closure_runtime_file_forbidden")
    if set(evidence.get("file_templates", {})) != {"01", "02", "03", "04", "05"}:
        raise ReadonlyValidationError("evidence_file_contract_mismatch")


def validate_complete_closure_contract(plan: dict[str, Any]) -> None:
    validate_static_plan_contract(plan)
    if plan.get("status") != "COMPLETE_GATE_CLOSED":
        return
    if any(plan["authorizations"].values()) or plan.get("gate_open") is not False:
        raise ReadonlyValidationError("complete_state_must_be_closed")
    state = plan.get("execution_state", {})
    if state.get("closure_complete") is not True:
        raise ReadonlyValidationError("complete_state_closure_required")
    closure_sha = state.get("closure_git_sha")
    if re.fullmatch(r"[0-9a-f]{40}", str(closure_sha or "")) is None:
        raise ReadonlyValidationError("closure_git_sha_required")
    run_id = plan.get("run_id")
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
        str(run_id or ""),
    ) is None:
        raise ReadonlyValidationError("closure_run_id_required")
    runtime = plan.get("runtime_evidence")
    if not isinstance(runtime, dict) or set(runtime) != {"01", "02", "03", "04", "05"}:
        raise ReadonlyValidationError("complete_runtime_evidence_set_mismatch")
    templates = plan["evidence_contract"]["file_templates"]
    for key, record in runtime.items():
        if not isinstance(record, dict):
            raise ReadonlyValidationError("runtime_evidence_record_invalid")
        expected_path = f"evidence/runtime/023/{run_id}/{templates[key]}"
        if record.get("path") != expected_path:
            raise ReadonlyValidationError("runtime_evidence_path_mismatch")
        if re.fullmatch(r"[0-9a-f]{64}", str(record.get("raw_sha256", ""))) is None:
            raise ReadonlyValidationError("runtime_evidence_sha256_required")
        if not str(record.get("verdict", "")).startswith("PASS_"):
            raise ReadonlyValidationError("runtime_evidence_pass_verdict_required")


def validate_operational_state(plan: dict[str, Any], mode: str, root: Path = ROOT) -> None:
    state = plan.get("execution_state", {})
    flags = {
        "bridge_executed": state.get("bridge_executed"),
        "june_refresh_executed": state.get("june_refresh_executed"),
        "app_validation_executed": state.get("app_validation_executed"),
    }
    runtime = plan.get("runtime_evidence", {})
    if mode == "baseline":
        if flags != {
            "bridge_executed": False,
            "june_refresh_executed": False,
            "app_validation_executed": False,
        }:
            raise ReadonlyValidationError("baseline_execution_state_mismatch")
        if any(key in runtime for key in ("03", "04", "05")):
            raise ReadonlyValidationError("baseline_productive_evidence_must_be_absent")
        return
    if flags["bridge_executed"] is not True or flags["june_refresh_executed"] is not True:
        raise ReadonlyValidationError("postcheck_execution_state_incoherent")
    if flags["app_validation_executed"] is not False:
        raise ReadonlyValidationError("app_validation_must_be_pending")
    run_id = plan.get("run_id")
    templates = plan.get("evidence_contract", {}).get("file_templates", {})
    for key in ("03", "04"):
        record = runtime.get(key)
        if not isinstance(record, dict):
            raise ReadonlyValidationError("required_productive_evidence_missing")
        expected_relative = f"evidence/runtime/023/{run_id}/{templates.get(key)}"
        if record.get("path") != expected_relative:
            raise ReadonlyValidationError("productive_evidence_path_mismatch")
        path = root / expected_relative
        if not path.is_file():
            raise ReadonlyValidationError("required_productive_evidence_missing")
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
        if record.get("raw_sha256") != observed:
            raise ReadonlyValidationError("productive_evidence_hash_mismatch")


def evaluate_app_acl_snapshot(
    expected_objects: set[str],
    direct_grants: Iterable[tuple[str, str]],
    public_grants: Iterable[tuple[str, str]],
    effective_writes: Iterable[tuple[str, str]],
) -> dict[str, Any]:
    direct = {tuple(row) for row in direct_grants}
    public = {tuple(row) for row in public_grants}
    writes = {tuple(row) for row in effective_writes}
    expected = {(name, "SELECT") for name in expected_objects}
    if direct != expected:
        raise ReadonlyValidationError("app_direct_grant_surface_drift")
    prohibited_public = {
        row for row in public
        if str(row[1]).upper() in {
            "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"
        }
    }
    if writes or prohibited_public:
        raise ReadonlyValidationError("app_effective_write_privilege_forbidden")
    return {
        "direct_grants": sorted(direct),
        "public_environmental_grants": sorted(public),
        "effective_write_privileges": sorted(writes),
    }


def evaluate_app_capability_snapshot(
    snapshot: dict[str, Any],
    required_select_objects: set[str],
    global_select_allowlist: set[str],
    allowlist_status: str,
) -> dict[str, Any]:
    if allowlist_status != APP_ALLOWLIST_FROZEN:
        raise ReadonlyValidationError("STOP_023_APP_READ_ALLOWLIST_NOT_FROZEN")
    attributes = snapshot.get("role_attributes", {})
    if attributes.get("login") is not True or any(
        attributes.get(name) is not False
        for name in (
            "superuser", "createdb", "createrole", "replication", "bypassrls"
        )
    ):
        raise ReadonlyValidationError("app_elevated_role_attribute_forbidden")
    memberships = list(snapshot.get("memberships", []))
    ownerships = list(snapshot.get("ownerships", []))
    if memberships:
        raise ReadonlyValidationError("app_membership_forbidden")
    if ownerships:
        raise ReadonlyValidationError("app_ownership_forbidden")
    grants = [dict(item) for item in snapshot.get("grants", [])]
    prohibited = {
        "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER",
        "CREATE", "TEMP", "TEMPORARY", "MAINTAIN",
    }
    for grant in grants:
        privilege = str(grant.get("privilege", "")).upper()
        object_type = str(grant.get("object_type", ""))
        schema = str(grant.get("schema", ""))
        if privilege in prohibited:
            raise ReadonlyValidationError("app_effective_write_privilege_forbidden")
        if object_type == "sequence":
            raise ReadonlyValidationError("app_sequence_privilege_forbidden")
        if object_type == "routine" and schema not in {"pg_catalog", "information_schema"}:
            raise ReadonlyValidationError("app_non_system_routine_execute_forbidden")
    effective_selects = {
        str(grant.get("identity")) for grant in grants
        if grant.get("object_type") == "relation"
        and str(grant.get("privilege", "")).upper() == "SELECT"
    }
    missing = required_select_objects - effective_selects
    if missing:
        raise ReadonlyValidationError("app_required_control_gestion_select_missing")
    if effective_selects != global_select_allowlist:
        raise ReadonlyValidationError("app_global_select_allowlist_drift")
    return {
        "role_attributes": attributes,
        "memberships": memberships,
        "ownerships": ownerships,
        "grants": grants,
        "required_select_objects": sorted(required_select_objects),
        "effective_select_objects": sorted(effective_selects),
    }


def _assert_app_allowlist_frozen(plan: dict[str, Any]) -> None:
    surface = plan.get("app_readonly_surface", {})
    if surface.get("status") != APP_ALLOWLIST_FROZEN:
        raise ReadonlyValidationError("STOP_023_APP_READ_ALLOWLIST_NOT_FROZEN")
    allowlist = surface.get("global_select_allowlist")
    if not isinstance(allowlist, list) or not allowlist:
        raise ReadonlyValidationError("app_global_select_allowlist_missing")


def _git(*args: str, text: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=text, check=False,
    )


def _load_plan(path: Path, expected_git_ref: str, mode: str) -> tuple[dict, dict[str, str]]:
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReadonlyValidationError("plan_023_unavailable_or_invalid") from exc
    if plan.get("document_type") != PLAN_TYPE:
        raise ReadonlyValidationError("plan_023_document_type_mismatch")
    validate_complete_closure_contract(plan)
    active = [name for name, enabled in plan["authorizations"].items() if enabled]
    if mode == "baseline":
        if plan.get("status") != "CONTRACT_READY_GATE_CLOSED" or active:
            raise ReadonlyValidationError("baseline_plan_state_mismatch")
    elif mode == "postcheck":
        if (
            plan.get("status") != "BRIDGE_AND_REFRESH_COMMITTED_APP_VALIDATION_PENDING"
            or active
        ):
            raise ReadonlyValidationError("postcheck_plan_state_mismatch")
    elif mode == "app":
        if (
            plan.get("status") != "BRIDGE_AND_REFRESH_COMMITTED_APP_VALIDATION_PENDING"
            or active != ["runtime_app_validation_authorized"]
        ):
            raise ReadonlyValidationError("app_validation_not_exclusively_authorized")
        _assert_app_allowlist_frozen(plan)
    validate_operational_state(plan, mode)
    payload = plan.get("scope", {}).get("canonical_payload")
    if canonical_json_sha256(payload) != plan.get("scope", {}).get("scope_sha256"):
        raise ReadonlyValidationError("scope_sha256_mismatch")
    if re.fullmatch(r"[0-9a-f]{40}", expected_git_ref or "") is None:
        raise ReadonlyValidationError("expected_git_ref_required")
    head = _git("rev-parse", "HEAD")
    if head.returncode != 0 or head.stdout.strip() != expected_git_ref:
        raise ReadonlyValidationError("repository_head_mismatch")
    if mode != "baseline" and plan.get("approved_git_sha") != expected_git_ref:
        raise ReadonlyValidationError("plan_approved_git_sha_mismatch")
    if _git("diff", "--quiet").returncode != 0 or _git("diff", "--cached", "--quiet").returncode != 0:
        raise ReadonlyValidationError("repository_not_clean")
    untracked = _git("ls-files", "--others", "--exclude-standard")
    if untracked.returncode != 0 or untracked.stdout.strip():
        raise ReadonlyValidationError("repository_not_clean")
    relative = path.resolve().relative_to(ROOT.resolve()).as_posix()
    blob = _git("show", f"HEAD:{relative}", text=False)
    if blob.returncode != 0 or blob.stdout != path.read_bytes():
        raise ReadonlyValidationError("plan_not_tracked_or_differs_from_head")
    return plan, {
        "approved_git_sha": expected_git_ref,
        "plan_path": relative,
        "plan_raw_sha256": hashlib.sha256(blob.stdout).hexdigest(),
    }


def _validate_dsn(dsn: str, expected_role: str, plan: dict) -> None:
    parsed = urlparse(dsn)
    target = plan["target"]
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ReadonlyValidationError("unsupported_db_scheme")
    if parsed.username != expected_role:
        raise ReadonlyValidationError("dsn_role_mismatch")
    if (parsed.hostname or "").lower() != target["hostname"]:
        raise ReadonlyValidationError("dsn_hostname_mismatch")
    if target["project_ref"] not in (parsed.hostname or ""):
        raise ReadonlyValidationError("dsn_project_mismatch")
    if (parsed.path or "").lstrip("/") != target["database"]:
        raise ReadonlyValidationError("dsn_database_mismatch")
    if parse_qs(parsed.query, keep_blank_values=True).get("sslmode", []) != ["require"]:
        raise ReadonlyValidationError("dsn_sslmode_require_required")


def _fetch_grain_metrics(cursor, *, include_daily: bool) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    if include_daily:
        cursor.execute(
            "SELECT fecha_visita,cod_rt,cliente_norm FROM "
            "cg_mart.fact_cg_visita_dia_resuelta_v2 "
            "WHERE fecha_visita BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'"
        )
        result["daily"] = grain_metrics(cursor.fetchall())
    cursor.execute(
        'SELECT "SEMANA_INICIO","COD_RT","CLIENTE_NORM_FILTER" FROM '
        'cg_mart.fact_cg_out_weekly_v2 WHERE "SEMANA_INICIO" = ANY(%s::date[])',
        (EXPECTED_WEEKS,),
    )
    result["weekly"] = grain_metrics(cursor.fetchall())
    for name, metrics in result.items():
        if (
            metrics["duplicate_key_count"] > 0
            or metrics["max_multiplicity"] > 1
            or metrics["null_or_blank_key_rows"] > 0
            or metrics["row_count"] != metrics["distinct_grain_count"]
        ):
            raise ReadonlyValidationError(f"{name}_grain_contract_failed")
    return result


def _collect_app_capability_snapshot(cursor) -> dict[str, Any]:
    cursor.execute(
        "SELECT oid,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls "
        "FROM pg_roles WHERE rolname=%s",
        (EXPECTED_APP_ROLE,),
    )
    role = cursor.fetchone()
    if role is None:
        raise ReadonlyValidationError("app_role_missing")
    target_oid = int(role[0])
    role_attributes = dict(zip(
        ("login", "superuser", "createdb", "createrole", "replication", "bypassrls"),
        role[1:],
    ))
    cursor.execute(
        "SELECT granted.oid,granted.rolname FROM pg_auth_members m "
        "JOIN pg_roles member ON member.oid=m.member "
        "JOIN pg_roles granted ON granted.oid=m.roleid WHERE member.rolname=%s",
        (EXPECTED_APP_ROLE,),
    )
    membership_rows = list(cursor.fetchall())
    membership_oids = {int(row[0]) for row in membership_rows}
    memberships = [str(row[1]) for row in membership_rows]
    cursor.execute(
        "SELECT kind,identity FROM ("
        "SELECT 'database' kind,d.datname identity,d.datdba owner FROM pg_database d "
        "UNION ALL SELECT 'schema',n.nspname,n.nspowner FROM pg_namespace n "
        "UNION ALL SELECT 'relation',n.nspname||'.'||c.relname,c.relowner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "UNION ALL SELECT 'routine',n.nspname||'.'||p.proname,p.proowner FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace"
        ") owned WHERE owner=%s",
        (target_oid,),
    )
    ownerships = [tuple(row) for row in cursor.fetchall()]
    raw_grants: list[tuple[Any, ...]] = []
    catalog_queries = (
        (
            "SELECT x.grantee,'database','',d.datname,x.privilege_type FROM pg_database d "
            "CROSS JOIN LATERAL aclexplode(COALESCE(d.datacl,acldefault('d',d.datdba))) x",
            (),
        ),
        (
            "SELECT x.grantee,'schema',n.nspname,n.nspname,x.privilege_type FROM pg_namespace n "
            "CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) x",
            (),
        ),
        (
            "SELECT x.grantee,CASE WHEN c.relkind='S' THEN 'sequence' ELSE 'relation' END,"
            "n.nspname,n.nspname||'.'||c.relname,x.privilege_type FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 'S'::\"char\" ELSE 'r'::\"char\" END,c.relowner))) x",
            (),
        ),
        (
            "SELECT x.grantee,'routine',n.nspname,n.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')',x.privilege_type "
            "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) x",
            (),
        ),
    )
    for query, params in catalog_queries:
        cursor.execute(query, params)
        raw_grants.extend(tuple(row) for row in cursor.fetchall())
    grants: list[dict[str, Any]] = []
    for grantee, object_type, schema, identity, privilege in raw_grants:
        grantee_oid = int(grantee)
        if grantee_oid == 0:
            source = "PUBLIC"
        elif grantee_oid == target_oid:
            source = "direct"
        elif grantee_oid in membership_oids:
            source = "membership"
        else:
            continue
        grants.append({
            "source": source,
            "object_type": str(object_type),
            "schema": str(schema),
            "identity": str(identity),
            "privilege": str(privilege),
        })
    return {
        "role_attributes": role_attributes,
        "memberships": memberships,
        "ownerships": ownerships,
        "grants": grants,
    }


def _run_observation(dsn: str, mode: str, plan: dict, authority: dict[str, str]) -> dict[str, Any]:
    expected_role = EXPECTED_APP_ROLE if mode == "app" else EXPECTED_RO_ROLE
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute("SET LOCAL statement_timeout='5min'")
            cursor.execute(
                "SELECT current_user,session_user,current_database(),current_setting('role'),"
                "current_setting('transaction_read_only')"
            )
            identity = cursor.fetchone()
            if identity != (expected_role, expected_role, EXPECTED_DATABASE, "none", "on"):
                raise ReadonlyValidationError("readonly_session_identity_mismatch")
            cursor.execute(
                "SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='cg_core' AND c.relname="
                "'v_cg_visita_dia_precedencia_route_b_v1'"
            )
            if cursor.fetchone() != ("v",):
                raise ReadonlyValidationError("bridge_signature_mismatch")
            cursor.execute(
                "SELECT a.attname,format_type(a.atttypid,a.atttypmod) "
                "FROM pg_attribute a WHERE a.attrelid="
                "'cg_core.v_cg_visita_dia_precedencia_route_b_v1'::regclass "
                "AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
            )
            if tuple(cursor.fetchall()) != BRIDGE_SIGNATURE:
                raise ReadonlyValidationError("bridge_column_signature_mismatch")
            cursor.execute(
                "SELECT has_table_privilege(%s,%s,'SELECT')",
                (expected_role, "cg_core.v_cg_visita_dia_precedencia_route_b_v1"),
            )
            bridge_select = cursor.fetchone() == (True,)
            if not bridge_select:
                raise ReadonlyValidationError("bridge_readonly_acl_mismatch")
            cursor.execute(
                "SELECT has_table_privilege(%s,%s,'SELECT')",
                (expected_role, "cg_mart.fact_cg_out_weekly_v2"),
            )
            if cursor.fetchone() != (True,):
                raise ReadonlyValidationError("weekly_readonly_acl_mismatch")
            app_acl = None
            if mode == "app":
                surface = plan.get("app_readonly_surface", {})
                expected_objects = set(
                    surface.get("required_control_gestion_select_objects", [])
                )
                if not expected_objects:
                    raise ReadonlyValidationError("app_readonly_surface_missing")
                app_acl = evaluate_app_capability_snapshot(
                    _collect_app_capability_snapshot(cursor),
                    expected_objects,
                    set(surface.get("global_select_allowlist", [])),
                    str(surface.get("status", "")),
                )
            grains = _fetch_grain_metrics(cursor, include_daily=mode != "app")
            cursor.execute(
                'SELECT "SEMANA_INICIO"::text,COUNT(*)::bigint FROM '
                'cg_mart.fact_cg_out_weekly_v2 WHERE "SEMANA_INICIO" = ANY(%s::date[]) '
                'GROUP BY "SEMANA_INICIO" ORDER BY "SEMANA_INICIO"',
                (EXPECTED_WEEKS,),
            )
            week_rows = [(str(week), int(rows)) for week, rows in cursor.fetchall()]
            if [week for week, rows in week_rows if rows > 0] != EXPECTED_WEEKS:
                raise ReadonlyValidationError("five_queryable_june_weeks_required")
            connection.rollback()
    return {
        **authority,
        "document_type": "stock_zero_cg_route_b_023_readonly_observation_v1",
        "schema_version": 1,
        "verdict": (
            "PASS_023_READONLY_BASELINE" if mode == "baseline"
            else "PASS_023_READONLY_POSTCHECK" if mode == "postcheck"
            else "PASS_023_READONLY_APP_POSTCHECK"
        ),
        "mode": mode,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "role": expected_role,
        "transaction_mode": "READ_ONLY_REPEATABLE_READ",
        "transaction_outcome": "ROLLED_BACK",
        "writes_attempted": False,
        "writes_executed": False,
        "scope_sha256": plan["scope"]["scope_sha256"],
        "grain_metrics": grains,
        "june_week_rows": week_rows,
        "queryable_weeks": EXPECTED_WEEKS,
        "app_acl": app_acl,
        "expected_ui_weeks": list(reversed(EXPECTED_WEEKS[-3:])),
        "secrets_printed": False,
        "dsn_printed": False,
    }


def _output_path(path: Path, mode: str, run_id: str) -> Path:
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
        run_id or "",
    ) is None:
        raise ReadonlyValidationError("canonical_run_id_required")
    if mode in {"baseline", "app"}:
        filename = "01_readonly_baseline.json" if mode == "baseline" else "05_readonly_app_postcheck.json"
        expected = (ROOT / "evidence" / "runtime" / "023" / run_id / filename).resolve()
        actual = (ROOT / path).resolve() if not path.is_absolute() else path.resolve()
        if actual != expected:
            raise ReadonlyValidationError("canonical_readonly_evidence_path_required")
    else:
        actual = path.resolve()
    if actual.exists() or not actual.parent.is_dir():
        raise ReadonlyValidationError("unused_readonly_output_path_required")
    return actual


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Closed-mode read-only observer for phase 023")
    result.add_argument("--mode", choices=MODES, required=True)
    result.add_argument("--plan-023", type=Path, default=PLAN_DEFAULT)
    result.add_argument("--expected-git-ref", required=True)
    result.add_argument("--expected-project-ref", required=True)
    result.add_argument("--confirm", required=True)
    result.add_argument("--run-id", required=True)
    result.add_argument("--report-json", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.expected_project_ref != EXPECTED_PROJECT:
            raise ReadonlyValidationError("expected_project_ref_mismatch")
        expected_env = "DB_URL_APP" if args.mode == "app" else "DB_URL_CODEX_RO"
        expected_profile = "app-readonly" if args.mode == "app" else "readonly"
        if os.getenv("STOCK_ZERO_OPERATION_PROFILE") != expected_profile:
            raise ReadonlyValidationError("readonly_wrapper_profile_mismatch")
        if os.getenv("STOCK_ZERO_OPERATION") != MODE_OPERATION[args.mode]:
            raise ReadonlyValidationError("readonly_wrapper_operation_mismatch")
        plan, authority = _load_plan(args.plan_023, args.expected_git_ref, args.mode)
        token_key = MODE_CONFIRMATION_KEY[args.mode]
        if args.confirm != plan.get("confirmation_tokens", {}).get(token_key):
            raise ReadonlyValidationError("readonly_confirmation_token_mismatch")
        output = _output_path(args.report_json, args.mode, args.run_id)
        dsn = os.getenv(expected_env, "")
        if not dsn:
            raise ReadonlyValidationError("readonly_child_environment_required")
        expected_role = EXPECTED_APP_ROLE if args.mode == "app" else EXPECTED_RO_ROLE
        _validate_dsn(dsn, expected_role, plan)
        report = _run_observation(dsn, args.mode, plan, authority)
        report["run_id"] = args.run_id
        rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        output.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
        return 0
    except (ReadonlyValidationError, OSError) as exc:
        print(json.dumps({"verdict": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2
    except Exception as exc:
        print(json.dumps({"verdict": "BLOCKED", "error": type(exc).__name__}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
