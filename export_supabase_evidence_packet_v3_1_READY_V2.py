# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

ROOT = Path(__file__).resolve().parent


STAGE_CHOICES = (
    "auto",
    "sql_contract",
    "db_contract",
    "app_runtime",
    "ux_iteration",
)

GATE_EFFECT_CHOICES = (
    "",
    "blocks",
    "supports",
    "opens",
    "confirms",
    "informs",
)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_sslmode(db_url: str) -> str:
    if not db_url:
        return db_url
    if "sslmode=" in db_url:
        return db_url
    return db_url + ("&sslmode=require" if "?" in db_url else "?sslmode=require")


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def local_now_sao_paulo() -> dt.datetime:
    return utc_now().astimezone(dt.timezone(dt.timedelta(hours=-3)))


def json_safe(v: Any) -> Any:
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, Path):
        return str(v)
    if isinstance(v, dict):
        return {str(k): json_safe(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [json_safe(x) for x in v]
    try:
        json.dumps(v)
        return v
    except Exception:
        return str(v)


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def get_db_url(cli_db_url: str | None) -> tuple[str, str]:
    if cli_db_url:
        return ensure_sslmode(cli_db_url), "cli"
    primary = (os.getenv("DB_URL_APP", "") or os.getenv("DB_URL", "")).strip()
    fallback = os.getenv("DB_URL_FALLBACK", "").strip()
    if primary:
        return ensure_sslmode(primary), "env:DB_URL_APP|DB_URL"
    if fallback:
        return ensure_sslmode(fallback), "env:DB_URL_FALLBACK"
    raise SystemExit("Falta DB_URL. Usa --db-url o setea DB_URL_APP / DB_URL / DB_URL_FALLBACK.")


def load_env() -> None:
    for env_path in (ROOT / ".env", ROOT.parent / ".env", Path.cwd() / ".env"):
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=False)
            break


def compile_sql(query_sql: str) -> str:
    return query_sql.replace("{{sample_cod_rt}}", "%(sample_cod_rt)s")


def fetch_rows(conn, sql: str, params: dict[str, Any], timeout_sec: int) -> tuple[list[dict[str, Any]], list[str]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"SET statement_timeout = {int(timeout_sec * 1000)};")
        has_named_params = "%(" in sql
        if has_named_params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        cols = list(rows[0].keys()) if rows else []
    return [json_safe(dict(r)) for r in rows], cols


def query_result_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    if len(rows) == 1 and isinstance(rows[0], dict):
        return dict(rows[0])
    return {"rows": len(rows)}


def should_skip_query(qmeta: dict[str, Any], params_used: dict[str, Any]) -> tuple[bool, str | None]:
    req = qmeta.get("requires_params", [])
    missing = [p for p in req if not params_used.get(p)]
    if missing:
        return True, f"missing_params:{','.join(missing)}"
    return False, None


def pick_key(row: dict[str, Any], *names: str, default=None):
    for name in names:
        if name in row:
            return row[name]
    return default


def classify_gate_effect(q08_first_row: dict[str, Any] | None, has_failures: bool) -> tuple[str, list[dict[str, Any]]]:
    anomalies: list[dict[str, Any]] = []
    if has_failures:
        anomalies.append({
            "anomaly_id": "ANOM_RUN_FAIL",
            "type": "query_failures_present",
            "status": "fail",
            "severity": "high",
            "value": True,
            "threshold": False,
            "business_impact": "bundle_incompleto_o_inestable",
            "gate_effect": "blocks",
        })
        return "blocks", anomalies
    if not q08_first_row:
        anomalies.append({
            "anomaly_id": "ANOM_NO_Q08",
            "type": "missing_exact_contract_flags",
            "status": "warn",
            "severity": "medium",
            "value": True,
            "threshold": False,
            "business_impact": "sin_bandera_final_de_readiness",
            "gate_effect": "supports",
        })
        return "supports", anomalies

    observed_ok = bool(pick_key(q08_first_row, "ready_observed_scope_contract"))
    target_ready = bool(pick_key(q08_first_row, "ready_target_b3_contract", "ready_target_B3_contract"))
    target_mode = str(pick_key(q08_first_row, "target_b3_contract_mode", "target_B3_contract_mode", default="unknown"))

    if not observed_ok:
        anomalies.append({
            "anomaly_id": "ANOM_OBSERVED_SCOPE_CONTRACT",
            "type": "observed_scope_contract_incomplete",
            "status": "fail",
            "severity": "high",
            "value": False,
            "threshold": True,
            "business_impact": "falta_la_superficie_scope_observada_que_sostiene_la_lectura_actual",
            "gate_effect": "blocks",
        })
        return "blocks", anomalies

    if not target_ready:
        anomalies.append({
            "anomaly_id": "ANOM_TARGET_B3_PENDING",
            "type": "target_b3_contract_pending",
            "status": "warn",
            "severity": "medium",
            "value": target_mode,
            "threshold": "exact_contract_complete",
            "business_impact": "aun_no_hay_contrato_publico_final_B3_congelado",
            "gate_effect": "supports",
        })
        return "supports", anomalies

    # Cambio de filosofía: el exporter confirma evidencia SQL, no abre por sí mismo el siguiente gate.
    return "confirms", anomalies


def resolve_stage(requested_stage: str, has_failures: bool, q08_first_row: dict[str, Any] | None) -> tuple[str, str]:
    if requested_stage != "auto":
        return requested_stage, "cli"
    if has_failures:
        return "sql_contract", "auto:failures_present"
    if not q08_first_row:
        return "sql_contract", "auto:no_q08"
    target_ready = bool(pick_key(q08_first_row, "ready_target_b3_contract", "ready_target_B3_contract"))
    if target_ready:
        return "app_runtime", "auto:target_contract_ready"
    return "sql_contract", "auto:target_contract_pending"


def derive_stage_guidance(
    resolved_stage: str,
    q08_first_row: dict[str, Any] | None,
    has_failures: bool,
    next_action_override: str,
) -> dict[str, Any]:
    if next_action_override.strip():
        return {
            "recommended_next_action": next_action_override.strip(),
            "next_action_source": "cli_override",
            "stage_message": "se_aplico_override_manual_para_la_siguiente_accion",
        }

    if has_failures:
        return {
            "recommended_next_action": "corregir_queries_fallidas_y_reemitir_packet",
            "next_action_source": "failure_policy",
            "stage_message": "el_packet_es_util_parcialmente_pero_no_debe_cerrar_gates_superiores",
        }

    if not q08_first_row:
        return {
            "recommended_next_action": "validar_contract_audit_Q08_y_reemitir_packet",
            "next_action_source": "missing_q08_policy",
            "stage_message": "falta_bandera_final_para_interpretar_cierre_de_contrato",
        }

    observed_ok = bool(pick_key(q08_first_row, "ready_observed_scope_contract"))
    target_ready = bool(pick_key(q08_first_row, "ready_target_b3_contract", "ready_target_B3_contract"))
    q08_next = pick_key(q08_first_row, "recommended_next_step")

    if not observed_ok:
        return {
            "recommended_next_action": "cerrar_superficie_scope_observada_antes_de_avanzar",
            "next_action_source": "observed_contract_policy",
            "stage_message": "la_superficie_observada_aun_no_esta_completa",
        }

    if not target_ready:
        return {
            "recommended_next_action": q08_next or "completar_contrato_publico_B3_exacto",
            "next_action_source": "q08_or_pending_contract_policy",
            "stage_message": "el_contrato_target_sigue_pendiente; el_exporter_solo_confirma_evidencia_actual",
        }

    stage_map = {
        "sql_contract": "interpretar_packet_y_congelar_contrato_db.py_si_aplica",
        "db_contract": "validar_smoke_db_py_contract_y_cerrar_gate_DB_SMOKE",
        "app_runtime": "validar_consumo_real_app_db.py_y_emitir_packet_final_app_sql",
        "ux_iteration": "debatir_y_formalizar_directrices_ux_sobre_base_tecnica_estable",
    }
    stage_msg = {
        "sql_contract": "el_contrato_sql_ya_esta_listo; la_transicion_posterior_se_define_fuera_del_exporter",
        "db_contract": "el_exporter_confirma_sql; el_siguiente_cierre_es_consumo_desde_db.py",
        "app_runtime": "el_exporter_confirma_sql; el_siguiente_cierre_es_consumo_runtime_desde_app",
        "ux_iteration": "la_base_tecnica_queda_congelada; la_siguiente_iteracion_puede_enfocarse_en_ux",
    }
    return {
        "recommended_next_action": stage_map.get(resolved_stage, "interpretar_packet_segun_etapa_real_actual_y_actualizar_continuidad"),
        "next_action_source": f"stage_policy:{resolved_stage}",
        "stage_message": stage_msg.get(resolved_stage, "stage_no_mapeado_de_forma_especifica"),
    }


def resolve_gate_effect(
    auto_gate_effect: str,
    resolved_stage: str,
    gate_effect_override: str,
    has_failures: bool,
    q08_first_row: dict[str, Any] | None,
) -> tuple[str, str]:
    if gate_effect_override.strip():
        return gate_effect_override.strip(), "cli_override"
    if has_failures:
        return auto_gate_effect, "auto_failure_policy"
    if not q08_first_row:
        return auto_gate_effect, "auto_missing_q08_policy"
    if auto_gate_effect in ("blocks", "supports"):
        return auto_gate_effect, "auto_contract_policy"

    # Cuando el contrato target ya está listo, el exporter confirma evidencia SQL.
    if resolved_stage == "sql_contract":
        return "opens", "stage_policy:sql_contract"
    if resolved_stage == "db_contract":
        return "supports", "stage_policy:db_contract"
    if resolved_stage == "app_runtime":
        return "confirms", "stage_policy:app_runtime"
    if resolved_stage == "ux_iteration":
        return "informs", "stage_policy:ux_iteration"
    return "confirms", "default_ready_contract_policy"


def main() -> None:
    load_env()
    ap = argparse.ArgumentParser(description="Ejecuta bundle Supabase V3.1 y emite evidence packet V3.1.")
    ap.add_argument("--bundle-path", default="Supabase/STOCK_ZERO_SUPABASE_QUERY_BUNDLE_V3_1.json")
    ap.add_argument("--evidence-standard-path", default="Supabase/STOCK_ZERO_SUPABASE_EVIDENCE_PACKET_V3.json")
    ap.add_argument("--output-dir", default="Supabase/artifacts")
    ap.add_argument("--db-url", default="")
    ap.add_argument("--sample-cod-rt", default="")
    ap.add_argument("--workspace", default=os.getenv("APP_ENV", "prod_or_staging"))
    ap.add_argument("--db-role", default="app_or_admin")
    ap.add_argument("--timeout-sec", type=int, default=45)
    ap.add_argument("--preview-limit", type=int, default=5)
    ap.add_argument("--project-stage", default="auto", choices=STAGE_CHOICES)
    ap.add_argument("--next-action", default="")
    ap.add_argument("--gate-effect-override", default="", choices=GATE_EFFECT_CHOICES)
    args = ap.parse_args()

    bundle_path = Path(args.bundle_path).resolve()
    evidence_path = Path(args.evidence_standard_path).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if not bundle_path.exists():
        raise SystemExit(f"No existe bundle: {bundle_path}")
    if not evidence_path.exists():
        raise SystemExit(f"No existe evidence standard: {evidence_path}")

    bundle = load_json(bundle_path)
    evidence_standard = load_json(evidence_path)
    runner_contract = bundle.get("runner_contract", {})
    order = runner_contract.get("execution_order", [])
    bundle_queries = {q["query_id"]: q for q in bundle.get("query_bundle", [])}

    params_used: dict[str, Any] = {}
    if args.sample_cod_rt.strip():
        params_used["sample_cod_rt"] = args.sample_cod_rt.strip()

    db_url, db_source = get_db_url(args.db_url)
    query_runs: list[dict[str, Any]] = []
    raw_outputs: dict[str, Any] = {}
    t_bundle = time.perf_counter()

    with psycopg2.connect(db_url) as conn:
        conn.autocommit = True
        for item in order:
            qid = item.split("_", 1)[0]
            qmeta = bundle_queries[qid]
            skip, reason = should_skip_query(qmeta, params_used)
            if skip:
                query_runs.append({
                    "query_id": qid,
                    "query_name": qmeta.get("query_name"),
                    "query_group": qmeta.get("group"),
                    "source_objects": qmeta.get("source_objects", []),
                    "sql_hash": None,
                    "duration_ms": 0.0,
                    "row_count": 0,
                    "status": "skipped",
                    "severity": "info",
                    "result_summary": {"skip_reason": reason},
                    "comparison_vs_previous": "unknown",
                    "supports_gate": bundle.get("gate_target", "G3_open_B3_app_query_contract"),
                })
                raw_outputs[qid] = {
                    "status": "skipped",
                    "row_count": 0,
                    "columns": [],
                    "preview_rows": [],
                    "rows": [],
                    "compiled_sql": None,
                    "meta": {
                        "query_id": qid,
                        "query_name": qmeta.get("query_name"),
                        "purpose": qmeta.get("purpose"),
                        "group": qmeta.get("group"),
                    },
                }
                continue

            sql = compile_sql(qmeta["sql"])
            t0 = time.perf_counter()
            status, severity, error_text = "ok", "info", None
            rows: list[dict[str, Any]] = []
            cols: list[str] = []
            try:
                rows, cols = fetch_rows(conn, sql, params_used, timeout_sec=args.timeout_sec)
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                status, severity, error_text = "fail", "high", str(exc)
            duration_ms = round((time.perf_counter() - t0) * 1000.0, 3)
            preview_rows = rows[: max(0, int(args.preview_limit))]
            run_rec = {
                "query_id": qid,
                "query_name": qmeta.get("query_name"),
                "query_group": qmeta.get("group"),
                "source_objects": qmeta.get("source_objects", []),
                "sql_hash": sha256_text(sql),
                "duration_ms": duration_ms,
                "row_count": len(rows),
                "status": status,
                "severity": severity,
                "result_summary": query_result_summary(rows),
                "comparison_vs_previous": "unknown",
                "supports_gate": bundle.get("gate_target", "G3_open_B3_app_query_contract"),
            }
            if error_text:
                run_rec["error"] = error_text
            query_runs.append(run_rec)
            raw_outputs[qid] = {
                "status": status,
                "row_count": len(rows),
                "columns": cols,
                "preview_rows": preview_rows,
                "rows": rows,
                "compiled_sql": sql,
                "meta": {
                    "query_id": qid,
                    "query_name": qmeta.get("query_name"),
                    "purpose": qmeta.get("purpose"),
                    "group": qmeta.get("group"),
                },
            }

    local_now = local_now_sao_paulo()
    timestamp = local_now.strftime("%Y%m%d_%H%M")
    branch_id = bundle.get("branch_id", "B3_CONTROL_GESTION_SQL")
    packet_id = f"SZEP3_1_{branch_id}_{timestamp}"

    q08_rows = raw_outputs.get("Q08", {}).get("rows", [])
    q08_first = q08_rows[0] if q08_rows else None
    has_failures = any(q["status"] == "fail" for q in query_runs)
    auto_gate_effect, anomalies = classify_gate_effect(q08_first, has_failures)
    resolved_stage, stage_source = resolve_stage(args.project_stage, has_failures, q08_first)
    stage_guidance = derive_stage_guidance(resolved_stage, q08_first, has_failures, args.next_action)
    final_gate_effect, gate_effect_source = resolve_gate_effect(
        auto_gate_effect=auto_gate_effect,
        resolved_stage=resolved_stage,
        gate_effect_override=args.gate_effect_override,
        has_failures=has_failures,
        q08_first_row=q08_first,
    )

    performance_trace = {
        "total_bundle_ms": round((time.perf_counter() - t_bundle) * 1000.0, 3),
        "slowest_query_name": max(query_runs, key=lambda x: x.get("duration_ms") or 0).get("query_name") if query_runs else None,
        "slowest_query_ms": max((q.get("duration_ms") or 0) for q in query_runs) if query_runs else 0,
        "queries_over_500ms": [q["query_name"] for q in query_runs if (q.get("duration_ms") or 0) > 500],
        "queries_failed": [q["query_name"] for q in query_runs if q.get("status") == "fail"],
        "queries_skipped": [q["query_name"] for q in query_runs if q.get("status") == "skipped"],
        "bundle_status": "fail" if has_failures else "ok",
    }

    packet = {
        "packet_id": packet_id,
        "project": bundle.get("project", "STOCK_ZERO/GESTIONZERO"),
        "branch_id": branch_id,
        "intent": evidence_standard.get("template", {}).get(
            "intent",
            "congelar_evidencia_de_consulta_real_para_abrir_contrato_db.py_sin_recalculo_python",
        ),
        "date_factual": {
            "generated_at_local": local_now.isoformat(),
            "generated_at_utc": utc_now().isoformat().replace("+00:00", "Z"),
            "timezone": "America/Sao_Paulo",
        },
        "env": {
            "platform": "supabase",
            "workspace": args.workspace,
            "db_role": args.db_role,
            "source_mode": "python_runner_bundle",
            "db_url_source": db_source,
        },
        "data_version": {
            **(raw_outputs.get("Q00", {}).get("rows", [{}])[0] if raw_outputs.get("Q00", {}).get("rows") else {}),
            "source_object": "public.v_data_version",
            "comparison_vs_previous": "unknown",
        },
        "query_bundle_name": bundle.get("query_bundle_name", "control_gestion_contract_v3_1_exact_contract"),
        "execution_context": {
            "driver": Path(__file__).name,
            "bundle_version": "v3_1",
            "operator": os.getenv("USERNAME") or os.getenv("USER") or "user",
            "branch_gate_target": bundle.get("gate_target", "G3_open_B3_app_query_contract"),
            "bundle_path": str(bundle_path),
            "evidence_standard_path": str(evidence_path),
            "script_path": str(Path(__file__).resolve()),
            "bundle_sha256": sha256_file(bundle_path),
            "evidence_standard_sha256": sha256_file(evidence_path),
            "script_sha256": sha256_file(Path(__file__).resolve()),
            "params_used": params_used,
            "sample_probe_policy": "only_if_sample_cod_rt_provided",
            "exporter_role": "sql_evidence_canonical_not_project_orchestrator",
        },
        "project_stage": {
            "requested_stage": args.project_stage,
            "resolved_stage": resolved_stage,
            "stage_source": stage_source,
            "exporter_scope": "exporta_evidencia_sql_y_contexto_de_etapa_sin_definir_por_si_solo_toda_la_orquestacion_del_proyecto",
            "stage_message": stage_guidance["stage_message"],
            "next_action_source": stage_guidance["next_action_source"],
        },
        "queries_run": query_runs,
        "validated_counts": {
            "Q06_scope_rollup_counts": query_result_summary(raw_outputs.get("Q06", {}).get("rows", [])),
            "Q07_scope_top_responsables": {
                "row_count": raw_outputs.get("Q07", {}).get("row_count", 0),
                "preview_rows": raw_outputs.get("Q07", {}).get("preview_rows", []),
            },
        },
        "observed_contract_inventory": {
            "Q01_objects": raw_outputs.get("Q01", {}).get("rows", []),
            "Q02_columns": raw_outputs.get("Q02", {}).get("rows", []),
            "Q03_definitions": raw_outputs.get("Q03", {}).get("preview_rows", []),
        },
        "target_contract_discovery": {
            "Q04_candidate_objects": raw_outputs.get("Q04", {}).get("rows", []),
            "Q05_candidate_columns": raw_outputs.get("Q05", {}).get("rows", []),
        },
        "contract_audit": q08_first or {},
        "sample_lookups": [
            {
                "query_id": qid,
                "query_name": raw_outputs.get(qid, {}).get("meta", {}).get("query_name"),
                "status": raw_outputs.get(qid, {}).get("status"),
                "row_count": raw_outputs.get(qid, {}).get("row_count", 0),
                "columns": raw_outputs.get(qid, {}).get("columns", []),
                "preview_rows": raw_outputs.get(qid, {}).get("preview_rows", []),
            }
            for qid in ("S01", "S02", "S03", "S04", "S05")
            if raw_outputs.get(qid, {}).get("status") != "skipped"
        ],
        "anomalies": anomalies,
        "performance_trace": performance_trace,
        "index_inventory": [],
        "artifacts_touched": [
            {"artifact": bundle_path.name, "status": "read", "reason": "bundle_contract"},
            {"artifact": evidence_path.name, "status": "read", "reason": "output_standard"},
        ],
        "sql_evidence": {
            "bundle_status": performance_trace["bundle_status"],
            "has_failures": has_failures,
            "contract_target_ready": bool(pick_key(q08_first or {}, "ready_target_b3_contract", "ready_target_B3_contract")),
            "observed_scope_ready": bool(pick_key(q08_first or {}, "ready_observed_scope_contract")),
            "evidence_is_stage_agnostic": True,
        },
        "gate_impact": {
            "target_gate": bundle.get("gate_target", "G3_open_B3_app_query_contract"),
            "effect": final_gate_effect,
            "effect_source": gate_effect_source,
            "why": [
                "bundle_ejecutado_sobre_SQL_real",
                "data_version_congelada_en_la_corrida",
                "Q08_usa_contrato_canonico_B3_por_nombres_exactos",
                "el_exporter_confirma_evidencia_y_no_define_en_solitario_el_workflow_total",
            ],
            "open_conditions_progress": q08_first or {},
        },
        "closing_state": {
            "run_status": "usable_for_interpretation" if not has_failures else "partial_with_failures",
            "recommended_next_action": stage_guidance["recommended_next_action"],
            "must_preserve": [
                "1_corrida=1_packet",
                "no_recalculo_negocio_en_python",
                "bundle_y_packet_con_nombres_estables",
                "sample_probe_opcional_no_define_el_modelo",
                "Q08_exacto_para_contrato_B3_canonico",
                "exporter_unico_pero_stage_aware",
            ],
            "must_not_do": [
                "no_subir_snippets_sueltos_como_fuente_principal",
                "no_mezclar_fechas_distintas_en_un_mismo_packet",
                "no_omitir_data_version",
                "no_usar_el_exporter_como_unico_orquestador_del_proyecto",
            ],
        },
        "raw_query_outputs": raw_outputs,
    }

    pretty_path = output_dir / f"STOCK_ZERO_SUPABASE_EVIDENCE_PACKET_V3_1_{branch_id}_{timestamp}.json"
    min_path = output_dir / f"STOCK_ZERO_SUPABASE_EVIDENCE_PACKET_V3_1_{branch_id}_{timestamp}.min.json"
    pretty_path.write_text(json.dumps(packet, ensure_ascii=False, indent=2), encoding="utf-8")
    min_path.write_text(json.dumps(packet, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"[OK] pretty={pretty_path}")
    print(f"[OK] min={min_path}")
    print(f"[INFO] sample_cod_rt={params_used.get('sample_cod_rt', '<none>')}")
    print(f"[INFO] stage={packet['project_stage']['resolved_stage']}")
    print(f"[INFO] gate_effect={packet['gate_impact']['effect']}")
    print(f"[INFO] next={packet['closing_state']['recommended_next_action']}")


if __name__ == "__main__":
    main()
