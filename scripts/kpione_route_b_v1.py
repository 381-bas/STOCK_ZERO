from __future__ import annotations

import hashlib
import json
import re
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

from openpyxl import load_workbook


RUNNER_VERSION = "017_ROUTE_B_V1"
SOURCE_SHEET = "Fotos"
FILE_PATTERN = "photo-excel-admin_*.xlsx"
LOCAL_DB_ENV = "DB_URL_CODEX_LOCAL"
LIFECYCLE = (
    "DISCOVERED", "VALIDATING", "VALIDATED", "STAGING", "STAGED", "ACTIVE",
    "QUARANTINED", "SUPERSEDED", "ROLLED_BACK", "FAILED",
)

COLUMN_ALIASES = {
    "event_id": ("id",), "sp_item_id": ("sp item id",), "holding": ("holding",),
    "subcadena": ("subcadena",), "cod_rt": ("codigo local",),
    "cliente_norm": ("marca",), "local_nombre": ("local",),
    "direccion": ("direccion",), "reponedor": ("reponedor",), "fecha": ("fecha",),
    "fecha_subida": ("fecha de subida",), "hora": ("hora",),
    "tipo_de_tarea": ("tipo de tarea",),
    "photo_count": ("n fotos", "foto no/total", "foto n/total", "foto n o/total"),
    "comentarios": ("comentarios",), "link_foto": ("link foto",),
}
REQUIRED = tuple(k for k in COLUMN_ALIASES if k != "fecha_subida")
EVENT_STABLE = (
    "event_id", "sp_item_id", "holding", "subcadena", "cod_rt", "cliente_norm",
    "local_nombre", "direccion", "reponedor", "fecha", "comentarios",
)
PHOTO_FIELDS = ("photo_count", "link_foto", "hora", "tipo_de_tarea", "fecha_subida")


class RouteBError(RuntimeError):
    pass


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    text = unicodedata.normalize("NFC", str(value)).strip()
    return re.sub(r"\s+", " ", text)


def identity_key(value: Any) -> str:
    text = unicodedata.normalize("NFKD", clean_text(value))
    return "".join(c for c in text if not unicodedata.combining(c)).upper()


def normalize_column(value: Any) -> str:
    text = identity_key(value).lower().replace("º", "o").replace("°", "o")
    return re.sub(r"\s+", " ", text)


def normalize_numeric_string(value: Any) -> str:
    text = clean_text(value)
    if re.fullmatch(r"[+-]?\d+(?:\.0+)?", text):
        return str(int(float(text)))
    return text


def parse_date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = clean_text(value)
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    raise RouteBError(f"invalid_date:{text}")


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_files(input_dir: Path) -> list[Path]:
    if not input_dir.is_dir():
        raise RouteBError(f"input_dir_not_found:{input_dir}")
    candidates = sorted(input_dir.glob(FILE_PATTERN), key=lambda p: p.name.casefold())
    result: list[Path] = []
    resolved: set[Path] = set()
    for path in candidates:
        if path.name.startswith("~$"):
            continue
        real = path.resolve()
        if real in resolved:
            raise RouteBError(f"duplicate_resolved_path:{path}")
        resolved.add(real)
        result.append(path)
    if not result:
        raise RouteBError("no_route_b_files_discovered")
    return result


def assert_local_target(env_name: str, dsn: str | None) -> str:
    if env_name != LOCAL_DB_ENV:
        raise RouteBError(f"unsafe_db_env:{env_name}")
    if not dsn:
        raise RouteBError("missing_local_db_url")
    parsed = urlparse(dsn)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise RouteBError("unsupported_db_scheme")
    if "supabase" in dsn.lower() or host not in {"localhost", "127.0.0.1", "::1"}:
        raise RouteBError(f"non_local_db_target:{host or 'missing_host'}")
    params = parse_qs(parsed.query)
    if params.get("sslmode", [""])[0].lower() in {"require", "verify-ca", "verify-full"}:
        raise RouteBError("remote_ssl_metadata_rejected")
    return "LOCAL_POSTGRESQL_LOOPBACK"


def _resolved_columns(headers: Iterable[Any]) -> tuple[dict[str, str], list[str], list[str]]:
    actual = {normalize_column(v): clean_text(v) for v in headers if clean_text(v)}
    resolved: dict[str, str] = {}
    for key, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if normalize_column(alias) in actual:
                resolved[key] = actual[normalize_column(alias)]
                break
    missing = [key for key in REQUIRED if key not in resolved]
    used = set(resolved.values())
    optional = sorted(v for v in actual.values() if v not in used)
    return resolved, missing, optional


@dataclass(frozen=True)
class WorkbookPlan:
    source_path: Path
    source_file_sha256: str
    source_file_name: str
    source_sheet: str
    file_size: int
    coverage_start: str
    coverage_end: str
    rows: tuple[dict[str, Any], ...]
    event_count: int
    day_presence_count: int
    duplicate_rows: int
    optional_columns: tuple[str, ...]


def inspect_workbook(path: Path) -> WorkbookPlan:
    content_hash = sha256_file(path)  # Identity is fixed before workbook parsing.
    try:
        workbook = load_workbook(path, read_only=True, data_only=True)
    except Exception as exc:
        raise RouteBError(f"workbook_open_failed:{path.name}:{exc}") from exc
    try:
        if SOURCE_SHEET not in workbook.sheetnames:
            raise RouteBError(f"missing_sheet:{SOURCE_SHEET}")
        sheet = workbook[SOURCE_SHEET]
        values = sheet.iter_rows(values_only=True)
        headers = next(values, None)
        if not headers:
            raise RouteBError("missing_header")
        resolved, missing, optional = _resolved_columns(headers)
        if missing:
            raise RouteBError("missing_required_columns:" + ",".join(missing))
        positions = {clean_text(value): idx for idx, value in enumerate(headers)}
        rows: list[dict[str, Any]] = []
        event_stability: dict[str, tuple[str, str]] = {}
        event_dates: dict[str, set[str]] = {}
        seen_photo: set[tuple[str, str]] = set()
        duplicates = 0
        for source_row_number, values_row in enumerate(values, start=2):
            raw = {key: values_row[positions[column]] for key, column in resolved.items()}
            event_id = normalize_numeric_string(raw["event_id"])
            sp_item_id = normalize_numeric_string(raw["sp_item_id"])
            if not event_id or not sp_item_id:
                raise RouteBError(f"missing_identity:row_{source_row_number}")
            fecha = parse_date(raw["fecha"])
            normalized = {key: clean_text(value) for key, value in raw.items()}
            normalized.update({
                "event_id": event_id, "sp_item_id": sp_item_id, "fecha": fecha,
                "cod_rt_norm": identity_key(raw["cod_rt"]),
                "cliente_norm": identity_key(raw["cliente_norm"]),
                "local_nombre_norm": identity_key(raw["local_nombre"]),
            })
            normalized["location_key"] = normalized["cod_rt_norm"] or normalized["local_nombre_norm"]
            stable_payload = {key: normalized.get(key, "") for key in EVENT_STABLE}
            photo_payload = {key: normalized.get(key, "") for key in PHOTO_FIELDS}
            event_stable_hash = sha256_text(stable_json(stable_payload))
            photo_row_hash = sha256_text(stable_json(photo_payload))
            previous = event_stability.setdefault(event_id, (sp_item_id, event_stable_hash))
            if previous != (sp_item_id, event_stable_hash):
                raise RouteBError(f"event_stability_conflict:{event_id}")
            event_dates.setdefault(event_id, set()).add(fecha)
            if len(event_dates[event_id]) > 1:
                raise RouteBError(f"event_multi_date_conflict:{event_id}")
            photo_identity = (event_id, photo_row_hash)
            duplicate = photo_identity in seen_photo
            duplicates += int(duplicate)
            seen_photo.add(photo_identity)
            source_row_identity = sha256_text(stable_json([content_hash, SOURCE_SHEET, source_row_number]))
            rows.append({
                **normalized,
                "source_row_number": source_row_number,
                "source_row_identity": source_row_identity,
                "photo_row_hash": photo_row_hash,
                "event_stable_hash": event_stable_hash,
                "duplicate_classification": "EXACT_DUPLICATE" if duplicate else "UNIQUE",
                "conflict_classification": "NONE",
            })
        if not rows:
            raise RouteBError("empty_source_file")
        dates = sorted({row["fecha"] for row in rows})
        presence = {(r["fecha"], r["location_key"], r["cliente_norm"]) for r in rows}
        return WorkbookPlan(path, content_hash, path.name, SOURCE_SHEET, path.stat().st_size,
                            dates[0], dates[-1], tuple(rows), len(event_stability), len(presence),
                            duplicates, tuple(optional))
    finally:
        workbook.close()


def build_plan(input_dir: Path) -> dict[str, Any]:
    plans = [inspect_workbook(path) for path in discover_files(input_dir)]
    hashes = [plan.source_file_sha256 for plan in plans]
    if len(hashes) != len(set(hashes)):
        # Same content under another name is one source version, never two active inputs.
        unique: dict[str, WorkbookPlan] = {}
        for plan in plans:
            unique.setdefault(plan.source_file_sha256, plan)
        plans = list(unique.values())
    all_rows = [row for plan in plans for row in plan.rows]
    event_stability: dict[str, tuple[str, str]] = {}
    for row in all_rows:
        value = (row["sp_item_id"], row["event_stable_hash"])
        if event_stability.setdefault(row["event_id"], value) != value:
            raise RouteBError(f"cross_file_event_stability_conflict:{row['event_id']}")
    coverage = sorted({row["fecha"] for row in all_rows})
    presence = {(r["fecha"], r["location_key"], r["cliente_norm"]) for r in all_rows}
    source_versions = sorted(p.source_file_sha256 for p in plans)
    semantic = {"runner_version": RUNNER_VERSION, "source_versions": source_versions,
                "source_sheet": SOURCE_SHEET, "grain": "immutable_event_photo_staging_row"}
    return {
        "runner_version": RUNNER_VERSION,
        "semantic_plan_hash": sha256_text(stable_json(semantic)),
        "apply_authorized": False,
        "db_target_classification": "NOT_EVALUATED_DRY_RUN",
        "coverage_start": coverage[0], "coverage_end": coverage[-1],
        "source_rows": len(all_rows), "duplicate_rows": sum(p.duplicate_rows for p in plans),
        "distinct_events": len(event_stability), "event_conflicts": 0,
        "day_presence_count": len(presence), "expected_inserts": len(all_rows),
        "expected_no_ops": 0, "expected_quarantines": 0,
        "expected_supersession_requirement": False,
        "files": [{
            "source_file_name": p.source_file_name, "source_file_sha256": p.source_file_sha256,
            "source_sheet": p.source_sheet, "file_size": p.file_size,
            "coverage_start": p.coverage_start, "coverage_end": p.coverage_end,
            "row_count": len(p.rows), "event_count": p.event_count,
            "optional_columns": list(p.optional_columns),
        } for p in plans],
        "_workbooks": plans,
    }


def public_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in plan.items() if not key.startswith("_")}


def apply_local(plan: dict[str, Any], dsn: str, ddl_path: Path,
                supersede_batch_id: str | None = None) -> dict[str, Any]:
    import psycopg
    target = assert_local_target(LOCAL_DB_ENV, dsn)
    execution_id = str(uuid.uuid4())
    batch_id = str(uuid.uuid4())
    workbooks: list[WorkbookPlan] = plan["_workbooks"]
    with psycopg.connect(dsn) as connection:
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute(ddl_path.read_text(encoding="utf-8"))
            hashes = [p.source_file_sha256 for p in workbooks]
            cursor.execute("SELECT b.batch_id::text, f.source_file_name FROM cg_raw.kpione_raw_ingest_batch_v1 b JOIN cg_raw.kpione_raw_ingest_batch_file_v1 f USING(batch_id) WHERE b.status='ACTIVE' AND f.source_file_sha256 = ANY(%s)", (hashes,))
            existing = cursor.fetchone()
            if existing:
                outcome = "NO_OP_ALREADY_REGISTERED" if existing[1] in {p.source_file_name for p in workbooks} else "NO_OP_SAME_SOURCE_VERSION"
                return {"outcome": outcome, "batch_id": existing[0], "apply_authorized": True, "db_target_classification": target}
            cursor.execute("SELECT batch_id::text FROM cg_raw.kpione_raw_ingest_batch_v1 WHERE status='ACTIVE' AND daterange(coverage_start, coverage_end, '[]') && daterange(%s,%s,'[]')", (plan["coverage_start"], plan["coverage_end"]))
            overlaps = [row[0] for row in cursor.fetchall()]
            if overlaps and not supersede_batch_id:
                return {"outcome": "NEW_SOURCE_VERSION_PENDING_SUPERSESSION", "active_batch_ids": overlaps, "apply_authorized": False, "db_target_classification": target}
            if supersede_batch_id and (supersede_batch_id not in overlaps or len(overlaps) != 1):
                raise RouteBError("invalid_or_unrelated_supersession")
            cursor.execute("INSERT INTO cg_raw.kpione_raw_ingest_batch_v1(batch_id,runner_execution_id,semantic_plan_hash,status,coverage_start,coverage_end,file_count,row_count,event_count,validated_at,supersedes_batch_id) VALUES(%s,%s,%s,'STAGING',%s,%s,%s,%s,%s,clock_timestamp(),%s)", (batch_id, execution_id, plan["semantic_plan_hash"], plan["coverage_start"], plan["coverage_end"], len(workbooks), plan["source_rows"], plan["distinct_events"], supersede_batch_id))
            for workbook in workbooks:
                cursor.execute("INSERT INTO cg_raw.kpione_raw_ingest_batch_file_v1(batch_id,source_file_sha256,source_file_name,source_sheet,file_size,coverage_start,coverage_end,row_count,event_count,validation_status) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,'VALIDATED')", (batch_id, workbook.source_file_sha256, workbook.source_file_name, workbook.source_sheet, workbook.file_size, workbook.coverage_start, workbook.coverage_end, len(workbook.rows), workbook.event_count))
                for row in workbook.rows:
                    cursor.execute("INSERT INTO cg_raw.kpione_raw_event_photo_staging_v1(batch_id,source_file_sha256,source_sheet,source_row_number,source_row_identity,event_id,sp_item_id,source_payload,photo_row_hash,event_stable_hash,event_date,location_key,cliente_norm,duplicate_classification,conflict_classification) VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s)", (batch_id, workbook.source_file_sha256, workbook.source_sheet, row["source_row_number"], row["source_row_identity"], row["event_id"], row["sp_item_id"], stable_json(row), row["photo_row_hash"], row["event_stable_hash"], row["fecha"], row["location_key"], row["cliente_norm"], row["duplicate_classification"], row["conflict_classification"]))
            if supersede_batch_id:
                cursor.execute("UPDATE cg_raw.kpione_raw_ingest_batch_v1 SET status='SUPERSEDED' WHERE batch_id=%s AND status='ACTIVE'", (supersede_batch_id,))
                if cursor.rowcount != 1:
                    raise RouteBError("supersession_predecessor_not_active")
            cursor.execute("UPDATE cg_raw.kpione_raw_ingest_batch_v1 SET status='ACTIVE',activated_at=clock_timestamp() WHERE batch_id=%s AND status='STAGING'", (batch_id,))
    return {"outcome": "ACTIVE", "batch_id": batch_id, "apply_authorized": True, "db_target_classification": target}


def rollback_local(dsn: str, batch_id: str) -> dict[str, Any]:
    import psycopg
    target = assert_local_target(LOCAL_DB_ENV, dsn)
    with psycopg.connect(dsn) as connection:
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute("SELECT supersedes_batch_id::text FROM cg_raw.kpione_raw_ingest_batch_v1 WHERE batch_id=%s AND status='ACTIVE' FOR UPDATE", (batch_id,))
            row = cursor.fetchone()
            if not row:
                raise RouteBError("rollback_batch_not_active")
            predecessor = row[0]
            cursor.execute("UPDATE cg_raw.kpione_raw_ingest_batch_v1 SET status='ROLLED_BACK',rolled_back_at=clock_timestamp() WHERE batch_id=%s", (batch_id,))
            if predecessor:
                cursor.execute("UPDATE cg_raw.kpione_raw_ingest_batch_v1 SET status='ACTIVE',activated_at=clock_timestamp() WHERE batch_id=%s AND status='SUPERSEDED'", (predecessor,))
                if cursor.rowcount != 1:
                    raise RouteBError("rollback_predecessor_not_restorable")
            cursor.execute("SELECT count(*) FROM cg_core.kpione_event_normalized_v1")
            events = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM cg_core.kpione_day_presence_v1")
            presence = cursor.fetchone()[0]
    return {"outcome": "ROLLED_BACK", "restored_batch_id": predecessor, "event_count": events,
            "day_presence_count": presence, "db_target_classification": target}
