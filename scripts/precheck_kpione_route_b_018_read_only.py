from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import parse_qs, urlparse

try:
    from scripts.kpione_route_b_evidence_v1 import (
        EvidenceContractError,
        atomic_write_json,
        prepare_run_directory,
        require_canonical_evidence_path,
        validate_run_id,
    )
    from scripts.kpione_route_b_v1 import (
        PLANNED_PRODUCTIVE_ROLE,
        PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_STATUS,
        RouteBError,
        _expected_productive_counts,
        _observed_productive_counts,
        validate_productive_git_guard,
        validate_productive_local_artifacts,
        validate_productive_role_contract,
    )
except ModuleNotFoundError:  # Direct execution from scripts/.
    from kpione_route_b_evidence_v1 import (
        EvidenceContractError,
        atomic_write_json,
        prepare_run_directory,
        require_canonical_evidence_path,
        validate_run_id,
    )
    from kpione_route_b_v1 import (
        PLANNED_PRODUCTIVE_ROLE,
        PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_STATUS,
        RouteBError,
        _expected_productive_counts,
        _observed_productive_counts,
        validate_productive_git_guard,
        validate_productive_local_artifacts,
        validate_productive_role_contract,
    )


ROOT = Path(__file__).resolve().parents[1]
PLAN_TYPE = "kpione_route_b_productive_apply_plan"
ALLOWED_DB_ENV = "DB_URL_CODEX_RO"
EXPECTED_READONLY_ROLE = "stock_zero_codex_ro"
PREPARATION_STATUS = "TECHNICAL_BOUNDARY_READY_ROLE_PROVISIONING_PENDING"
BASELINE_DOCUMENT_TYPE = "kpione_route_b_readonly_baseline_evidence_v1"
POSTCHECK_DOCUMENT_TYPE = "kpione_route_b_readonly_postcheck_evidence_v1"
REATTESTATION_DOCUMENT_TYPE = "stock_zero_route_b_post_apply_reattestation_v1"
REATTESTATION_VERDICT = "PASS_ROUTE_B_POST_APPLY_REATTESTATION"
REATTESTATION_EVIDENCE_FILENAME = "04_route_b_post_apply_reattestation.json"


class PrecheckBlock(RuntimeError):
    def __init__(self, identifier: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(identifier)
        self.details = details or {}


def load_plan(path: Path) -> dict[str, Any]:
    plan = json.loads(path.read_text(encoding="utf-8"))
    if plan.get("document_type") != PLAN_TYPE:
        raise PrecheckBlock("invalid_plan_type")
    if plan.get("productive_apply_authorized") is not False:
        raise PrecheckBlock("plan_must_not_authorize_productive_apply")
    return plan


def validate_plan_readiness(plan: dict[str, Any], check_stage: str = "baseline") -> None:
    if check_stage not in {"baseline", "post-provision", "post-apply-reattestation"}:
        raise PrecheckBlock("invalid_check_stage")
    if check_stage == "post-apply-reattestation":
        try:
            validate_productive_role_contract(plan)
        except RouteBError as exc:
            raise PrecheckBlock(str(exc)) from None
        if plan.get("status") != PRODUCTIVE_APPLY_COMMITTED_CLOSURE_PENDING_STATUS:
            raise PrecheckBlock("plan_status_not_closure_pending")
        if plan.get("activation_gate", {}).get("gate_open") is not False:
            raise PrecheckBlock("productive_gate_must_be_closed")
        if plan.get("productive_apply_authorized") is not False:
            raise PrecheckBlock("productive_apply_must_not_be_authorized")
        return
    target = plan.get("target", {})
    gate = plan.get("activation_gate", {})
    blockers: list[str] = []
    if plan.get("status") != PREPARATION_STATUS:
        blockers.append("plan_status_not_preparation")
    if not target.get("expected_supabase_project_ref") or not target.get("expected_hostname"):
        blockers.append("target_identity_not_registered")
    if target.get("allowed_readonly_roles") != [EXPECTED_READONLY_ROLE]:
        blockers.append("readonly_role_contract_mismatch")
    if target.get("planned_productive_role") != PLANNED_PRODUCTIVE_ROLE:
        blockers.append("planned_productive_role_mismatch")
    if target.get("productive_role_status") != "PLANNED_NOT_PROVISIONED":
        blockers.append("committed_plan_role_status_must_remain_planned")
    if target.get("allowed_productive_roles") != []:
        blockers.append("committed_plan_productive_allowlist_must_be_empty")
    if gate.get("productive_role_registered") is not False:
        blockers.append("committed_plan_productive_role_must_not_be_registered")
    if gate.get("gate_open") is not False:
        blockers.append("productive_gate_must_be_closed")
    if plan.get("productive_apply_authorized") is not False:
        blockers.append("productive_apply_must_not_be_authorized")
    if plan.get("productive_rollback_authorized") is not False:
        blockers.append("productive_rollback_must_not_be_authorized")
    if blockers:
        raise PrecheckBlock("plan_blocked:" + ",".join(sorted(set(blockers))))


def validate_source_manifest(plan: dict[str, Any], root: Path = ROOT) -> dict[str, Any]:
    try:
        result = validate_productive_local_artifacts(plan, root, validate_sql=False)
        return result["_validation_summary"]
    except RouteBError as exc:
        raise PrecheckBlock(str(exc)) from None


def validate_local_artifacts(plan: dict[str, Any], root: Path = ROOT) -> dict[str, Any]:
    try:
        result = validate_productive_local_artifacts(plan, root)
        return result["_validation_summary"]
    except RouteBError as exc:
        raise PrecheckBlock(str(exc)) from None


def validate_target(
    dsn: str | None,
    env_name: str,
    plan: dict[str, Any],
    expected_project_ref: str,
) -> str:
    if env_name != ALLOWED_DB_ENV or not dsn:
        raise PrecheckBlock("readonly_db_env_required")
    parsed = urlparse(dsn)
    target = plan["target"]
    host = (parsed.hostname or "").lower()
    database = parsed.path.lstrip("/")
    registered_ref = target.get("expected_supabase_project_ref")
    registered_host = (target.get("expected_hostname") or "").lower()
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise PrecheckBlock("readonly_dsn_scheme_mismatch")
    if parsed.username != EXPECTED_READONLY_ROLE:
        raise PrecheckBlock("readonly_dsn_role_mismatch")
    if not parsed.password:
        raise PrecheckBlock("readonly_dsn_password_required")
    if not registered_ref or expected_project_ref != registered_ref:
        raise PrecheckBlock("project_ref_mismatch")
    if host != registered_host or registered_ref not in host or "supabase" not in host:
        raise PrecheckBlock("target_host_mismatch")
    if database != target.get("expected_database"):
        raise PrecheckBlock("target_database_mismatch")
    if parse_qs(parsed.query, keep_blank_values=True).get("sslmode") != ["require"]:
        raise PrecheckBlock("readonly_sslmode_require_required")
    return host


def target_fingerprint(plan: Mapping[str, Any]) -> str:
    target = plan["target"]
    canonical = "|".join((
        target["expected_supabase_project_ref"],
        target["expected_hostname"],
        target["expected_database"],
    ))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _public_acl_snapshot(cursor: Any, relations: list[str]) -> dict[str, Any]:
    cursor.execute(
        "SELECT n.nspname,COALESCE((SELECT array_agg(a.privilege_type::text "
        "ORDER BY a.privilege_type::text) FROM aclexplode(COALESCE(n.nspacl,"
        "acldefault('n',n.nspowner))) a WHERE a.grantee=0),ARRAY[]::text[]) "
        "FROM pg_namespace n WHERE n.nspname IN ('cg_raw','cg_core') ORDER BY n.nspname"
    )
    schemas = {name: list(privileges) for name, privileges in cursor.fetchall()}
    cursor.execute(
        "SELECT table_schema||'.'||table_name,"
        "array_agg(privilege_type ORDER BY privilege_type) "
        "FROM information_schema.table_privileges "
        "WHERE grantee='PUBLIC' AND table_schema IN ('cg_raw','cg_core') "
        "AND table_schema||'.'||table_name=ANY(%s) GROUP BY 1 ORDER BY 1",
        (relations,),
    )
    observed = {name: privileges for name, privileges in cursor.fetchall()}
    return {
        "schemas": schemas,
        "relations": {name: observed.get(name, []) for name in sorted(relations)},
    }


def _legacy_snapshot(cursor: Any) -> dict[str, Any]:
    cursor.execute(
        "SELECT c.oid::text,n.nspname||'.'||c.relname,c.relkind,"
        "c.relowner::regrole::text,COALESCE(c.relacl::text,'') "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='cg_raw' AND c.relname='kpione2_raw'"
    )
    row = cursor.fetchone()
    if row is None:
        return {
            "object_identity": None,
            "oid": None,
            "relation_kind": None,
            "owner": None,
            "acl": None,
            "column_signature_sha256": None,
            "row_count": "unavailable",
            "relation_size_bytes": "unavailable",
            "select_available": False,
        }
    oid, identity, relation_kind, owner, acl = row
    cursor.execute(
        "SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),"
        "a.attnotnull,a.attidentity,a.attgenerated FROM pg_attribute a "
        "WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT a.attisdropped "
        "ORDER BY a.attnum",
        (identity,),
    )
    columns = [list(value) for value in cursor.fetchall()]
    column_signature_sha256 = hashlib.sha256(
        json.dumps(columns, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    cursor.execute("SELECT has_table_privilege(current_user,%s,'SELECT')", (identity,))
    can_select = bool(cursor.fetchone()[0])
    row_count: int | str = "unavailable"
    relation_size: int | str = "unavailable"
    if can_select:
        cursor.execute("SELECT count(*) FROM cg_raw.kpione2_raw")
        row_count = cursor.fetchone()[0]
        cursor.execute("SELECT pg_total_relation_size('cg_raw.kpione2_raw'::regclass)")
        relation_size = cursor.fetchone()[0]
    return {
        "object_identity": identity,
        "oid": oid,
        "relation_kind": relation_kind,
        "owner": owner,
        "acl": acl,
        "column_signature_sha256": column_signature_sha256,
        "row_count": row_count,
        "relation_size_bytes": relation_size,
        "select_available": can_select,
    }


def legacy_structural_identity(legacy: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: legacy.get(key)
        for key in (
            "object_identity", "oid", "relation_kind", "owner",
            "column_signature_sha256", "acl",
        )
    }


def assert_legacy_structural_invariance(
    baseline: Mapping[str, Any],
    current: Mapping[str, Any],
) -> None:
    expected = legacy_structural_identity(baseline)
    observed = legacy_structural_identity(current)
    if observed.get("object_identity") is None:
        raise PrecheckBlock("legacy_object_missing")
    for field in expected:
        if observed[field] != expected[field]:
            raise PrecheckBlock(f"legacy_structural_drift:{field}")


def legacy_activity_delta(
    baseline: Mapping[str, Any],
    current: Mapping[str, Any],
) -> dict[str, Any]:
    def metric(name: str) -> dict[str, Any]:
        before = baseline.get(name, "unavailable")
        after = current.get(name, "unavailable")
        delta = after - before if isinstance(before, int) and isinstance(after, int) else "unavailable"
        return {"before": before, "after": after, "delta": delta}

    return {
        "row_count": metric("row_count"),
        "relation_size_bytes": metric("relation_size_bytes"),
    }


def _route_b_snapshot(cursor: Any, plan: dict[str, Any]) -> tuple[list[dict[str, str]], int]:
    expected = plan["physical_contract"]["object_signatures"]
    cursor.execute(
        "SELECT n.nspname||'.'||c.relname,c.relkind FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname||'.'||c.relname=ANY(%s) ORDER BY 1",
        (list(expected),),
    )
    objects = cursor.fetchall()
    if objects and set(objects) != {
        (name, spec["relation_kind"]) for name, spec in expected.items()
    }:
        raise PrecheckBlock("route_b_object_set_or_kind_mismatch")
    signatures: list[dict[str, str]] = []
    if objects:
        cursor.execute(
            "SELECT n.nspname||'.'||c.relname,a.attname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid=c.oid "
            "WHERE n.nspname||'.'||c.relname=ANY(%s) "
            "AND a.attnum>0 AND NOT a.attisdropped "
            "ORDER BY n.nspname,c.relname,a.attnum",
            (list(expected),),
        )
        actual_columns: dict[str, list[str]] = {}
        for relation, column in cursor.fetchall():
            actual_columns.setdefault(relation, []).append(column)
        expected_columns = {name: spec["columns"] for name, spec in expected.items()}
        if actual_columns != expected_columns:
            mismatched = [
                {
                    "object": name,
                    "expected_columns": expected_columns.get(name, []),
                    "actual_columns": actual_columns.get(name, []),
                }
                for name in sorted(expected_columns)
                if actual_columns.get(name, []) != expected_columns.get(name, [])
            ]
            raise PrecheckBlock(
                "route_b_object_column_signature_mismatch",
                details={"mismatched_objects": mismatched},
            )
        signatures = [
            {"object": name, "relation_kind": kind, "columns_sha256": hashlib.sha256(
                "\n".join(expected[name]["columns"]).encode("utf-8")
            ).hexdigest()}
            for name, kind in objects
        ]
    active_batches = 0
    if any(name == "cg_raw.kpione_raw_ingest_batch_v1" for name, _kind in objects):
        cursor.execute(
            "SELECT count(*) FROM cg_raw.kpione_raw_ingest_batch_v1 WHERE status='ACTIVE'"
        )
        active_batches = cursor.fetchone()[0]
    return signatures, active_batches


def _assert_post_provision(
    cursor: Any,
    plan: dict[str, Any],
    signatures: list[dict[str, str]],
    active_batches: int,
    legacy: dict[str, Any],
    public_acl: dict[str, Any],
    baseline: Mapping[str, Any] | None,
) -> None:
    if baseline is None:
        raise PrecheckBlock("baseline_evidence_required")
    expected_names = set(plan["physical_contract"]["object_signatures"])
    if {item["object"] for item in signatures} != expected_names:
        raise PrecheckBlock("postcheck_route_b_objects_missing_or_extra")
    if active_batches != 0:
        raise PrecheckBlock("unexpected_active_route_b_batch")
    cursor.execute(
        "SELECT bool_and(has_table_privilege(current_user, name, 'SELECT')) "
        "FROM unnest(%s::text[]) AS name",
        (sorted(expected_names),),
    )
    if cursor.fetchone()[0] is not True:
        raise PrecheckBlock("readonly_observer_select_privilege_missing")
    cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=%s)", (PLANNED_PRODUCTIVE_ROLE,))
    if cursor.fetchone()[0] is not True:
        raise PrecheckBlock("productive_role_not_found")
    cursor.execute(
        "SELECT has_schema_privilege(%s,'cg_raw','CREATE'),"
        "has_schema_privilege(%s,'cg_core','CREATE')",
        (PLANNED_PRODUCTIVE_ROLE, PLANNED_PRODUCTIVE_ROLE),
    )
    if tuple(cursor.fetchone()) != (False, False):
        raise PrecheckBlock("productive_role_schema_create_privilege_detected")
    if baseline.get("target_fingerprint") != target_fingerprint(plan):
        raise PrecheckBlock("baseline_target_fingerprint_mismatch")
    baseline_legacy = baseline.get("legacy")
    if not isinstance(baseline_legacy, dict):
        raise PrecheckBlock("legacy_structural_baseline_missing")
    assert_legacy_structural_invariance(baseline_legacy, legacy)
    if public_acl != baseline.get("public_acl"):
        raise PrecheckBlock("public_acl_drift")


def canonical_post_apply_reattestation_path(root: Path, run_id: str) -> Path:
    validate_run_id(run_id)
    return (
        root.resolve() / "evidence" / "runtime" / "022" / run_id
        / REATTESTATION_EVIDENCE_FILENAME
    )


def load_post_apply_source_evidence(
    plan: dict[str, Any], root: Path, run_id: str,
) -> dict[str, Any]:
    validate_run_id(run_id)
    execution = plan.get("productive_execution", {})
    if execution.get("productive_run_id") != run_id:
        raise PrecheckBlock("productive_run_id_mismatch")
    definitions = (
        ("01", "01_readonly_pre_apply_target_check", "01_readonly_pre_apply_target_check.json"),
        ("02", "02_route_b_june_productive_apply", "02_route_b_june_productive_apply.json"),
        ("03", "03_readonly_post_apply_verification", "03_readonly_post_apply_verification.json"),
    )
    configured = execution.get("source_evidence")
    if not isinstance(configured, dict):
        raise PrecheckBlock("productive_source_evidence_contract_missing")
    run_directory = root.resolve() / "evidence" / "runtime" / "022" / run_id
    payloads: dict[str, dict[str, Any]] = {}
    references: dict[str, dict[str, str]] = {}
    for sequence, key, filename in definitions:
        item = configured.get(key)
        if not isinstance(item, dict):
            raise PrecheckBlock(f"source_evidence_contract_missing:{sequence}")
        expected_path = run_directory / filename
        candidate = root.resolve() / str(item.get("path", ""))
        if candidate.resolve() != expected_path:
            raise PrecheckBlock(f"source_evidence_path_mismatch:{sequence}")
        try:
            raw = expected_path.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            raise PrecheckBlock(f"source_evidence_unreadable:{sequence}") from None
        digest = hashlib.sha256(raw).hexdigest()
        if digest != item.get("sha256"):
            raise PrecheckBlock(f"source_evidence_sha256_mismatch:{sequence}")
        payloads[sequence] = payload
        references[sequence] = {
            "path": expected_path.relative_to(root.resolve()).as_posix(),
            "sha256": digest,
        }

    approved_git_sha = execution.get("approved_git_sha")
    pre, productive, post = payloads["01"], payloads["02"], payloads["03"]
    if not (
        pre.get("document_type") == "stock_zero_route_b_productive_readonly_snapshot_v1"
        and pre.get("verdict") == "PASS_READONLY_PRE_APPLY_TARGET_CHECK"
        and pre.get("phase") == "PRE_APPLY"
        and pre.get("run_id") == run_id
        and pre.get("approved_git_sha") == approved_git_sha
        and pre.get("current_user") == EXPECTED_READONLY_ROLE
        and pre.get("session_user") == EXPECTED_READONLY_ROLE
        and pre.get("transaction_read_only") == "on"
        and pre.get("active_batch_count") == 0
        and isinstance(pre.get("legacy"), dict)
        and isinstance(pre.get("public_acl"), dict)
    ):
        raise PrecheckBlock("pre_apply_source_evidence_mismatch")
    expected_metrics = execution.get("expected_metrics_confirmed", {})
    observed = productive.get("observed_counts", {})
    if not (
        productive.get("document_type") == "kpione_route_b_productive_execution_evidence_v1"
        and productive.get("operation") == "APPLY"
        and productive.get("approved_git_sha") == approved_git_sha
        and productive.get("execution_uuid") == execution.get("runner_execution_uuid")
        and productive.get("batch_id") == execution.get("batch_id")
        and productive.get("committed") is True
        and productive.get("transaction_outcome") == "COMMITTED"
        and productive.get("postcheck_verdict") == "PASS"
        and productive.get("downstream_use_allowed") is True
        and productive.get("expected_counts") == productive.get("observed_counts")
        and all(observed.get(key) == value for key, value in expected_metrics.items())
        and productive.get("postcheck", {}).get("active_batch_count") == 1
        and productive.get("postcheck", {}).get("legacy_object") == "cg_raw.kpione2_raw"
    ):
        raise PrecheckBlock("productive_source_evidence_mismatch")
    legacy_delta = post.get("legacy_activity_delta", {})
    if not (
        post.get("document_type") == "stock_zero_route_b_productive_readonly_snapshot_v1"
        and post.get("verdict") == "PASS_READONLY_POST_APPLY_VERIFICATION"
        and post.get("phase") == "POST_APPLY"
        and post.get("run_id") == run_id
        and post.get("approved_git_sha") == approved_git_sha
        and post.get("current_user") == EXPECTED_READONLY_ROLE
        and post.get("session_user") == EXPECTED_READONLY_ROLE
        and post.get("transaction_read_only") == "on"
        and post.get("active_batch_count") == 1
        and legacy_delta.get("row_count", {}).get("delta") == 0
        and legacy_delta.get("relation_size_bytes", {}).get("delta") == 0
        and post.get("public_acl_unchanged") is True
        and post.get("public_acl") == pre.get("public_acl")
        and post.get("route_b_signatures") == pre.get("route_b_signatures")
    ):
        raise PrecheckBlock("post_apply_source_evidence_mismatch")
    return {
        "payloads": payloads,
        "references": references,
        "baseline_evidence_sha256": references["01"]["sha256"],
    }


def run_post_apply_reattestation(
    plan: dict[str, Any],
    dsn: str,
    connect_fn: Callable[[str], Any],
    *,
    source_evidence: Mapping[str, Any],
    run_id: str,
) -> dict[str, Any]:
    validate_plan_readiness(plan, "post-apply-reattestation")
    execution = plan["productive_execution"]
    payloads = source_evidence["payloads"]
    baseline = payloads["01"]
    expected_batch_id = execution["batch_id"]
    connection = connect_fn(dsn)
    rollback_completed = False
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("BEGIN READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5min'")
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            cursor.execute(
                "SELECT current_user,session_user,current_setting('transaction_read_only'),"
                "current_database()"
            )
            current_user, session_user, readonly, database = cursor.fetchone()
            if current_user != EXPECTED_READONLY_ROLE or session_user != EXPECTED_READONLY_ROLE:
                raise PrecheckBlock("readonly_role_mismatch")
            if readonly != "on":
                raise PrecheckBlock("transaction_not_read_only")
            if database != plan["target"]["expected_database"]:
                raise PrecheckBlock("readonly_session_database_mismatch")
            signatures, active_batch_count = _route_b_snapshot(cursor, plan)
            cursor.execute(
                "SELECT batch_id::text FROM cg_raw.kpione_raw_ingest_batch_v1 "
                "WHERE status='ACTIVE' ORDER BY batch_id"
            )
            active_batch_ids = [row[0] for row in cursor.fetchall()]
            if active_batch_count != 1 or active_batch_ids != [expected_batch_id]:
                raise PrecheckBlock("active_productive_batch_mismatch")
            observed = _observed_productive_counts(cursor, expected_batch_id)
            expected = _expected_productive_counts(plan)
            if observed != expected:
                raise PrecheckBlock("productive_counts_mismatch")
            legacy = _legacy_snapshot(cursor)
            acl_relations = list(plan["physical_contract"]["object_signatures"]) + [
                "cg_raw.kpione2_raw"
            ]
            public_acl = _public_acl_snapshot(cursor, acl_relations)
            assert_legacy_structural_invariance(baseline["legacy"], legacy)
            activity_delta = legacy_activity_delta(baseline["legacy"], legacy)
            if (
                activity_delta["row_count"]["delta"] != 0
                or activity_delta["relation_size_bytes"]["delta"] != 0
            ):
                raise PrecheckBlock("legacy_activity_delta_nonzero")
            if public_acl != baseline["public_acl"]:
                raise PrecheckBlock("public_acl_drift")
            if signatures != baseline["route_b_signatures"]:
                raise PrecheckBlock("route_b_physical_signature_drift")
        connection.rollback()
        rollback_completed = True
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    report = {
        "document_type": REATTESTATION_DOCUMENT_TYPE,
        "verdict": REATTESTATION_VERDICT,
        "productive_run_id": run_id,
        "approved_git_sha": execution["approved_git_sha"],
        "runner_execution_uuid": execution["runner_execution_uuid"],
        "batch_id": expected_batch_id,
        "reattestation_execution_uuid": str(uuid.uuid4()),
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_evidence": source_evidence["references"],
        "baseline_evidence_sha256": source_evidence["baseline_evidence_sha256"],
        "database_execution": {
            "current_user": current_user,
            "session_user": session_user,
            "expected_role": EXPECTED_READONLY_ROLE,
            "transaction_read_only": readonly,
            "begin_read_only": True,
            "rollback_completed": rollback_completed,
            "writes_attempted": False,
            "write_statements_defined": False,
        },
        "current_productive_state": {
            "active_batch_count": active_batch_count,
            "active_batch_id": expected_batch_id,
            "staged_rows": observed["staged_rows"],
            "events": observed["events"],
            "day_presence": observed["day_presence"],
            "files": observed["files"],
            "conflicts": observed["conflicts"],
        },
        "baseline_comparison": {
            "legacy_row_delta": activity_delta["row_count"]["delta"],
            "legacy_relation_size_delta": activity_delta["relation_size_bytes"]["delta"],
            "public_acl_unchanged": True,
            "physical_signatures_unchanged": True,
            "observer_role_unchanged": True,
        },
    }
    validate_post_apply_reattestation(report, plan)
    return report


def validate_post_apply_reattestation(report: Mapping[str, Any], plan: Mapping[str, Any]) -> None:
    execution = plan.get("productive_execution", {})
    database = report.get("database_execution", {})
    current = report.get("current_productive_state", {})
    comparison = report.get("baseline_comparison", {})
    references = report.get("source_evidence")
    if report.get("document_type") != REATTESTATION_DOCUMENT_TYPE:
        raise PrecheckBlock("reattestation_document_type_mismatch")
    if report.get("verdict") != REATTESTATION_VERDICT:
        raise PrecheckBlock("reattestation_verdict_mismatch")
    if report.get("approved_git_sha") != execution.get("approved_git_sha"):
        raise PrecheckBlock("reattestation_approved_git_sha_mismatch")
    if report.get("productive_run_id") != execution.get("productive_run_id"):
        raise PrecheckBlock("reattestation_run_id_mismatch")
    if report.get("runner_execution_uuid") != execution.get("runner_execution_uuid"):
        raise PrecheckBlock("reattestation_execution_uuid_mismatch")
    if report.get("batch_id") != execution.get("batch_id"):
        raise PrecheckBlock("reattestation_batch_id_mismatch")
    try:
        reattestation_uuid = uuid.UUID(str(report.get("reattestation_execution_uuid")))
    except ValueError:
        raise PrecheckBlock("reattestation_uuid_invalid") from None
    if reattestation_uuid.version != 4 or str(reattestation_uuid) != report.get(
        "reattestation_execution_uuid"
    ):
        raise PrecheckBlock("reattestation_uuid_invalid")
    if not isinstance(references, dict) or set(references) != {"01", "02", "03"}:
        raise PrecheckBlock("reattestation_source_evidence_missing")
    expected_sources = execution.get("source_evidence", {})
    for sequence, key in (
        ("01", "01_readonly_pre_apply_target_check"),
        ("02", "02_route_b_june_productive_apply"),
        ("03", "03_readonly_post_apply_verification"),
    ):
        if references[sequence] != expected_sources.get(key):
            raise PrecheckBlock(f"reattestation_source_sha256_mismatch:{sequence}")
    if report.get("baseline_evidence_sha256") != references["01"].get("sha256"):
        raise PrecheckBlock("reattestation_baseline_sha256_mismatch")
    if not (
        database.get("current_user") == EXPECTED_READONLY_ROLE
        and database.get("session_user") == EXPECTED_READONLY_ROLE
        and database.get("expected_role") == EXPECTED_READONLY_ROLE
        and database.get("transaction_read_only") == "on"
        and database.get("begin_read_only") is True
        and database.get("rollback_completed") is True
        and database.get("writes_attempted") is False
        and database.get("write_statements_defined") is False
    ):
        raise PrecheckBlock("reattestation_readonly_contract_mismatch")
    expected_metrics = execution.get("expected_metrics_confirmed", {})
    if not (
        current.get("active_batch_count") == 1
        and current.get("active_batch_id") == execution.get("batch_id")
        and all(current.get(key) == expected_metrics.get(key) for key in (
            "staged_rows", "events", "day_presence", "files", "conflicts",
        ))
    ):
        raise PrecheckBlock("reattestation_productive_state_mismatch")
    if not (
        comparison.get("legacy_row_delta") == 0
        and comparison.get("legacy_relation_size_delta") == 0
        and comparison.get("public_acl_unchanged") is True
        and comparison.get("physical_signatures_unchanged") is True
        and comparison.get("observer_role_unchanged") is True
    ):
        raise PrecheckBlock("reattestation_baseline_comparison_mismatch")


def run_precheck(
    plan: dict[str, Any],
    dsn: str,
    connect_fn: Callable[[str], Any],
    *,
    check_stage: str = "baseline",
    baseline: Mapping[str, Any] | None = None,
    baseline_sha256: str | None = None,
    authority: Mapping[str, str] | None = None,
    run_id: str,
) -> dict[str, Any]:
    validate_run_id(run_id)
    validate_plan_readiness(plan, check_stage)
    if check_stage == "post-provision" and baseline is None:
        raise PrecheckBlock("baseline_evidence_required")
    connection = connect_fn(dsn)
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("BEGIN READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '30s'")
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            cursor.execute(
                "SELECT current_user,session_user,current_setting('transaction_read_only'),"
                "current_setting('server_version'),current_database()"
            )
            current_user, session_user, readonly, server_version, database = cursor.fetchone()
            if current_user != EXPECTED_READONLY_ROLE or session_user != EXPECTED_READONLY_ROLE:
                raise PrecheckBlock("readonly_role_mismatch")
            if readonly != "on":
                raise PrecheckBlock("transaction_not_read_only")
            if database != plan["target"]["expected_database"]:
                raise PrecheckBlock("readonly_session_database_mismatch")
            cursor.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name=ANY(%s) ORDER BY schema_name",
                (plan["physical_contract"]["required_schemas"],),
            )
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.execute(
                "SELECT name,default_version,installed_version FROM pg_available_extensions "
                "WHERE name=ANY(%s) ORDER BY name",
                (plan["physical_contract"]["required_extensions"],),
            )
            extensions = [list(row) for row in cursor.fetchall()]
            signatures, active_batches = _route_b_snapshot(cursor, plan)
            legacy = _legacy_snapshot(cursor)
            acl_relations = list(plan["physical_contract"]["object_signatures"]) + [
                "cg_raw.kpione2_raw"
            ]
            public_acl = _public_acl_snapshot(cursor, acl_relations)
            if check_stage == "post-provision":
                _assert_post_provision(
                    cursor, plan, signatures, active_batches, legacy, public_acl, baseline,
                )
            elif active_batches != 0:
                raise PrecheckBlock("unexpected_active_route_b_batch")
        authority = authority or {}
        document_type = (
            BASELINE_DOCUMENT_TYPE if check_stage == "baseline" else POSTCHECK_DOCUMENT_TYPE
        )
        verdict = "PASS_READONLY_BASELINE" if check_stage == "baseline" else "PASS_READONLY_POSTCHECK"
        report = {
            "document_type": document_type,
            "run_id": run_id,
            "evidence_sequence_step": 1 if check_stage == "baseline" else 4,
            "verdict": verdict,
            "check_stage": check_stage,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "target_fingerprint": target_fingerprint(plan),
            "approved_git_sha": authority.get("approved_git_sha", "unavailable"),
            "plan_sha256": authority.get("plan_sha256", "unavailable"),
            "sql_sha256": plan["physical_contract"]["sql_sha256"],
            "current_user": current_user,
            "session_user": session_user,
            "transaction_read_only": readonly,
            "database": database,
            "server_version": server_version,
            "schemas": schemas,
            "extensions": extensions,
            "route_b_objects": [item["object"] for item in signatures],
            "route_b_signatures": signatures,
            "active_batch_count": active_batches,
            "legacy": legacy,
            "public_acl": public_acl,
            "writes_attempted": False,
            **plan["source_package"]["source_byte_metrics"],
        }
        if check_stage == "post-provision":
            report["baseline_evidence_sha256"] = baseline_sha256
            report["legacy_activity_delta"] = legacy_activity_delta(
                baseline.get("legacy", {}), legacy,
            )
        return report
    finally:
        connection.rollback()
        connection.close()


def _load_baseline(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    evidence = json.loads(raw.decode("utf-8"))
    if evidence.get("document_type") != BASELINE_DOCUMENT_TYPE:
        raise PrecheckBlock("baseline_document_type_mismatch")
    if evidence.get("verdict") != "PASS_READONLY_BASELINE":
        raise PrecheckBlock("baseline_verdict_mismatch")
    return evidence, hashlib.sha256(raw).hexdigest()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="KPIONE Route B staged read-only evidence precheck")
    result.add_argument("--plan", type=Path, required=True)
    result.add_argument("--run-id", required=True)
    result.add_argument(
        "--check-stage",
        choices=("baseline", "post-provision", "post-apply-reattestation"),
        default="baseline",
    )
    result.add_argument("--baseline-evidence", type=Path)
    result.add_argument("--expected-plan-git-ref", required=True)
    result.add_argument("--db-url-env", default=ALLOWED_DB_ENV)
    result.add_argument("--expected-project-ref", required=True)
    result.add_argument("--report-json", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        validate_run_id(args.run_id)
        if args.check_stage == "post-apply-reattestation":
            report_path = canonical_post_apply_reattestation_path(ROOT, args.run_id)
            candidate = args.report_json if args.report_json.is_absolute() else ROOT / args.report_json
            if candidate.resolve() != report_path:
                raise PrecheckBlock("reattestation_evidence_path_not_canonical")
            if not report_path.parent.is_dir():
                raise PrecheckBlock("productive_run_directory_missing")
        else:
            prepare_run_directory(ROOT, args.run_id)
            component = (
                "readonly_baseline_precheck"
                if args.check_stage == "baseline" else "readonly_postcheck"
            )
            report_path = require_canonical_evidence_path(
                args.report_json, ROOT, args.run_id, component,
            )
        if report_path.exists():
            raise PrecheckBlock("evidence_file_already_exists")
        plan = load_plan(args.plan)
        validate_plan_readiness(plan, args.check_stage)
        validate_local_artifacts(plan)
        try:
            authority = validate_productive_git_guard(args.plan, args.expected_plan_git_ref, ROOT)
        except RouteBError as exc:
            raise PrecheckBlock(str(exc)) from None
        baseline: dict[str, Any] | None = None
        baseline_sha256: str | None = None
        source_evidence: dict[str, Any] | None = None
        if args.check_stage == "post-provision":
            if args.baseline_evidence is None:
                raise PrecheckBlock("baseline_evidence_required")
            baseline_path = require_canonical_evidence_path(
                args.baseline_evidence, ROOT, args.run_id, "readonly_baseline_precheck",
            )
            baseline, baseline_sha256 = _load_baseline(baseline_path)
            if baseline.get("run_id") != args.run_id or baseline.get("evidence_sequence_step") != 1:
                raise PrecheckBlock("baseline_run_or_sequence_mismatch")
        elif args.baseline_evidence is not None:
            raise PrecheckBlock("baseline_evidence_not_allowed_for_baseline")
        if args.check_stage == "post-apply-reattestation":
            source_evidence = load_post_apply_source_evidence(plan, ROOT, args.run_id)
        dsn = os.environ.get(args.db_url_env)
        validate_target(dsn, args.db_url_env, plan, args.expected_project_ref)
        import psycopg
        if args.check_stage == "post-apply-reattestation":
            report = run_post_apply_reattestation(
                plan,
                dsn or "",
                psycopg.connect,
                source_evidence=source_evidence or {},
                run_id=args.run_id,
            )
        else:
            report = run_precheck(
                plan,
                dsn or "",
                psycopg.connect,
                check_stage=args.check_stage,
                baseline=baseline,
                baseline_sha256=baseline_sha256,
                authority=authority,
                run_id=args.run_id,
            )
        rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        atomic_write_json(report_path, report)
        print(rendered)
        return 0
    except Exception as exc:
        known = isinstance(
            exc,
            (OSError, ValueError, json.JSONDecodeError, EvidenceContractError, PrecheckBlock),
        )
        error = str(exc) if known and not isinstance(exc, OSError) else "reattestation_unexpected_failure"
        blocked = {"verdict": "BLOCKED", "error": error, "writes_attempted": False}
        if isinstance(exc, PrecheckBlock):
            blocked.update(exc.details)
        print(json.dumps(blocked, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
