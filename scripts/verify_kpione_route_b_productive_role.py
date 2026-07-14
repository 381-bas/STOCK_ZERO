from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlparse

from psycopg import sql

try:
    from scripts.kpione_route_b_v1 import (
        PLANNED_PRODUCTIVE_ROLE,
        RouteBError,
        validate_productive_dsn_target,
        validate_productive_git_guard,
        validate_registered_productive_target,
    )
    from scripts.precheck_kpione_route_b_018_read_only import load_plan, target_fingerprint
    from scripts.provision_kpione_route_b_role import validate_provisioning_plan
except ModuleNotFoundError:  # Direct execution from scripts/.
    from kpione_route_b_v1 import (
        PLANNED_PRODUCTIVE_ROLE,
        RouteBError,
        validate_productive_dsn_target,
        validate_productive_git_guard,
        validate_registered_productive_target,
    )
    from precheck_kpione_route_b_018_read_only import load_plan, target_fingerprint
    from provision_kpione_route_b_role import validate_provisioning_plan


ROOT = Path(__file__).resolve().parents[1]
PRODUCTIVE_DB_ENV = "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE"
DOCUMENT_TYPE = "kpione_route_b_productive_role_verification_evidence_v1"
EXPECTED_DENIAL_SQLSTATE = "42501"


class ProductiveRoleVerificationError(RuntimeError):
    def __init__(self, identifier: str, *, connection_attempted: bool = False) -> None:
        super().__init__(identifier)
        self.connection_attempted = connection_attempted


def _expect_denied(
    cursor: Any,
    label: str,
    statement: Any,
    params: tuple[Any, ...] = (),
) -> dict[str, str]:
    savepoint = "verify_" + label.replace("-", "_")
    cursor.execute(f"SAVEPOINT {savepoint}")
    try:
        cursor.execute(statement, params)
    except Exception as exc:
        sqlstate = getattr(exc, "sqlstate", None)
        cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        cursor.execute(f"RELEASE SAVEPOINT {savepoint}")
        if sqlstate != EXPECTED_DENIAL_SQLSTATE:
            raise ProductiveRoleVerificationError(
                f"negative_probe_unexpected_sqlstate:{label}:{sqlstate or 'unavailable'}",
                connection_attempted=True,
            ) from None
        return {
            "probe": label,
            "outcome": "DENIED",
            "sqlstate": sqlstate,
            "error_class": type(exc).__name__,
        }
    cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
    cursor.execute(f"RELEASE SAVEPOINT {savepoint}")
    raise ProductiveRoleVerificationError(
        f"negative_probe_unexpectedly_allowed:{label}", connection_attempted=True,
    )


def verify_productive_role(
    plan: dict[str, Any],
    dsn: str,
    *,
    authority: Mapping[str, str],
    connect_fn: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    validate_provisioning_plan(plan)
    validate_registered_productive_target(plan)
    try:
        target = validate_productive_dsn_target(dsn, plan)
    except RouteBError as exc:
        raise ProductiveRoleVerificationError(str(exc)) from None
    if not dsn or not authority.get("approved_git_sha") or not authority.get("plan_sha256"):
        raise ProductiveRoleVerificationError("productive_role_verification_authority_required")
    if not urlparse(dsn).password:
        raise ProductiveRoleVerificationError("productive_dsn_password_required")
    if connect_fn is None:
        import psycopg
        connect_fn = psycopg.connect

    batch_id = uuid.uuid4()
    runner_id = uuid.uuid4()
    synthetic_hash = hashlib.sha256(str(batch_id).encode("ascii")).hexdigest()
    connection: Any | None = None
    positive: list[dict[str, str]] = []
    negative: list[dict[str, str]] = []
    try:
        connection = connect_fn(dsn)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '30s'")
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            cursor.execute("SELECT current_user,session_user,current_database()")
            current_user, session_user, database = cursor.fetchone()
            if current_user != PLANNED_PRODUCTIVE_ROLE or session_user != PLANNED_PRODUCTIVE_ROLE:
                raise ProductiveRoleVerificationError(
                    "productive_role_session_identity_mismatch", connection_attempted=True,
                )
            if database != plan["target"]["expected_database"]:
                raise ProductiveRoleVerificationError(
                    "productive_role_session_database_mismatch", connection_attempted=True,
                )

            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_ingest_batch_v1")
            cursor.fetchone()
            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_ingest_batch_file_v1")
            cursor.fetchone()
            cursor.execute("SELECT count(*) FROM cg_raw.kpione_raw_event_photo_staging_v1")
            cursor.fetchone()
            positive.append({"probe": "select_allowed_route_b_tables", "outcome": "PASS"})

            cursor.execute(
                "INSERT INTO cg_raw.kpione_raw_ingest_batch_v1("
                "batch_id,runner_execution_id,semantic_plan_hash,status,coverage_start,"
                "coverage_end,file_count,row_count,event_count) "
                "VALUES (%s,%s,%s,'DISCOVERED',%s,%s,1,1,1)",
                (batch_id, runner_id, synthetic_hash, date(2099, 1, 1), date(2099, 1, 1)),
            )
            positive.append({"probe": "insert_synthetic_batch", "outcome": "PASS"})
            cursor.execute(
                "UPDATE cg_raw.kpione_raw_ingest_batch_v1 "
                "SET status='VALIDATING',activated_at=clock_timestamp(),rolled_back_at=NULL "
                "WHERE batch_id=%s",
                (batch_id,),
            )
            positive.append({"probe": "update_authorized_transition_columns", "outcome": "PASS"})
            cursor.execute(
                "INSERT INTO cg_raw.kpione_raw_ingest_batch_file_v1("
                "batch_id,source_file_sha256,source_file_name,source_sheet,file_size,"
                "coverage_start,coverage_end,row_count,event_count,validation_status) "
                "VALUES (%s,%s,'synthetic-verification.xlsx','Fotos',1,%s,%s,1,1,'VALIDATED')",
                (batch_id, synthetic_hash, date(2099, 1, 1), date(2099, 1, 1)),
            )
            positive.append({"probe": "insert_synthetic_batch_file", "outcome": "PASS"})
            cursor.execute(
                "INSERT INTO cg_raw.kpione_raw_event_photo_staging_v1("
                "batch_id,source_file_sha256,source_sheet,source_row_number,source_row_identity,"
                "event_id,sp_item_id,source_payload,photo_row_hash,event_stable_hash,event_date,"
                "location_key,cliente_norm,duplicate_classification,conflict_classification) "
                "VALUES (%s,%s,'Fotos',2,%s,'synthetic-event','synthetic-item','{}'::jsonb,"
                "%s,%s,%s,'synthetic-location','synthetic-client','UNIQUE','NONE') RETURNING staging_id",
                (
                    batch_id,
                    synthetic_hash,
                    hashlib.sha256(b"synthetic-row").hexdigest(),
                    hashlib.sha256(b"synthetic-photo").hexdigest(),
                    hashlib.sha256(b"synthetic-event").hexdigest(),
                    date(2099, 1, 1),
                ),
            )
            staging_id = cursor.fetchone()[0]
            if not isinstance(staging_id, int):
                raise ProductiveRoleVerificationError(
                    "identity_sequence_probe_failed", connection_attempted=True,
                )
            positive.append({"probe": "insert_staging_and_use_identity_sequence", "outcome": "PASS"})
            cursor.execute("SELECT count(*) FROM cg_core.kpione_event_normalized_v1")
            cursor.fetchone()
            cursor.execute("SELECT count(*) FROM cg_core.kpione_day_presence_v1")
            cursor.fetchone()
            positive.append({"probe": "select_allowed_route_b_views", "outcome": "PASS"})

            cursor.execute(
                "SELECT a.attname FROM pg_attribute a "
                "WHERE a.attrelid='cg_raw.kpione2_raw'::regclass "
                "AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum LIMIT 1"
            )
            legacy_column_row = cursor.fetchone()
            if legacy_column_row is None:
                raise ProductiveRoleVerificationError(
                    "legacy_relation_has_no_probeable_column", connection_attempted=True,
                )
            legacy_update = sql.SQL(
                "UPDATE cg_raw.kpione2_raw SET {column}={column} WHERE false"
            ).format(column=sql.Identifier(legacy_column_row[0]))

            negative_specs = (
                ("batch-protected-update", "UPDATE cg_raw.kpione_raw_ingest_batch_v1 SET semantic_plan_hash=semantic_plan_hash WHERE batch_id=%s", (batch_id,)),
                ("batch-file-update", "UPDATE cg_raw.kpione_raw_ingest_batch_file_v1 SET validation_status=validation_status WHERE batch_id=%s", (batch_id,)),
                ("batch-file-delete", "DELETE FROM cg_raw.kpione_raw_ingest_batch_file_v1 WHERE batch_id=%s", (batch_id,)),
                ("staging-update", "UPDATE cg_raw.kpione_raw_event_photo_staging_v1 SET cliente_norm=cliente_norm WHERE batch_id=%s", (batch_id,)),
                ("staging-delete", "DELETE FROM cg_raw.kpione_raw_event_photo_staging_v1 WHERE batch_id=%s", (batch_id,)),
                ("create-cg-raw", "CREATE TABLE cg_raw.kpione_verify_forbidden_raw(id integer)", ()),
                ("create-cg-core", "CREATE TABLE cg_core.kpione_verify_forbidden_core(id integer)", ()),
                ("legacy-insert", "INSERT INTO cg_raw.kpione2_raw SELECT * FROM cg_raw.kpione2_raw WHERE false", ()),
                ("legacy-update", legacy_update, ()),
                ("legacy-delete", "DELETE FROM cg_raw.kpione2_raw WHERE false", ()),
                ("route-b-ddl", "ALTER TABLE cg_raw.kpione_raw_ingest_batch_v1 ADD COLUMN forbidden_probe integer", ()),
            )
            for label, statement, params in negative_specs:
                negative.append(_expect_denied(cursor, label, statement, params))
        connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute("BEGIN READ ONLY")
            cursor.execute(
                "SELECT (SELECT count(*) FROM cg_raw.kpione_raw_ingest_batch_v1 WHERE batch_id=%s)+"
                "(SELECT count(*) FROM cg_raw.kpione_raw_ingest_batch_file_v1 WHERE batch_id=%s)+"
                "(SELECT count(*) FROM cg_raw.kpione_raw_event_photo_staging_v1 WHERE batch_id=%s)",
                (batch_id, batch_id, batch_id),
            )
            persistent_rows = cursor.fetchone()[0]
        connection.rollback()
        if persistent_rows != 0:
            raise ProductiveRoleVerificationError(
                "productive_verification_persistent_rows_detected", connection_attempted=True,
            )
    except ProductiveRoleVerificationError:
        if connection is not None:
            connection.rollback()
        raise
    except Exception:
        if connection is not None:
            connection.rollback()
        raise ProductiveRoleVerificationError(
            "productive_role_verification_failed", connection_attempted=connection is not None,
        ) from None
    finally:
        if connection is not None:
            connection.close()

    return {
        "document_type": DOCUMENT_TYPE,
        "verdict": "PASS_PRODUCTIVE_ROLE_VERIFICATION",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "target_fingerprint": target_fingerprint(plan),
        "approved_git_sha": authority["approved_git_sha"],
        "plan_sha256": authority["plan_sha256"],
        "sql_sha256": plan["physical_contract"]["sql_sha256"],
        "credential_role": target["username"],
        "current_user": current_user,
        "session_user": session_user,
        "database": database,
        "positive_probes": positive,
        "negative_probes": negative,
        "transaction_outcome": "ROLLED_BACK",
        "persistent_rows_written": 0,
        "connection_attempted": True,
        "writes_attempted": True,
        "committed": False,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Verify the effective KPIONE Route B productive role")
    result.add_argument("--plan", type=Path, required=True)
    result.add_argument("--expected-plan-git-ref", required=True)
    result.add_argument("--db-url-env", default=PRODUCTIVE_DB_ENV)
    result.add_argument("--expected-project-ref", required=True)
    result.add_argument("--evidence-json", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.db_url_env != PRODUCTIVE_DB_ENV:
            raise ProductiveRoleVerificationError("productive_db_url_env_required")
        plan = load_plan(args.plan)
        if args.expected_project_ref != plan["target"]["expected_supabase_project_ref"]:
            raise ProductiveRoleVerificationError("productive_expected_project_ref_mismatch")
        try:
            authority = validate_productive_git_guard(args.plan, args.expected_plan_git_ref, ROOT)
        except RouteBError as exc:
            raise ProductiveRoleVerificationError(str(exc)) from None
        dsn = os.environ.get(PRODUCTIVE_DB_ENV)
        if not dsn:
            raise ProductiveRoleVerificationError("productive_dsn_missing")
        report = verify_productive_role(plan, dsn, authority=authority)
        rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        args.evidence_json.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
        return 0
    except (OSError, ValueError, ProductiveRoleVerificationError) as exc:
        print(json.dumps({
            "verdict": "BLOCKED",
            "error": str(exc),
            "connection_attempted": getattr(exc, "connection_attempted", False),
            "persistent_rows_written": 0,
            "committed": False,
        }, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
