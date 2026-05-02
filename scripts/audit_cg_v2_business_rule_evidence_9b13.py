#!/usr/bin/env python
from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

MV_NAME = "cg_mart.mv_cg_out_weekly_v2"
ROUTE_VIEW = "cg_core.v_rr_frecuencia_base_resuelta_v2"
ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "Supabase" / "artifacts"
DAY_META = [
    ("LUN", "LUNES_PLAN", "LUNES_FLAG"),
    ("MAR", "MARTES_PLAN", "MARTES_FLAG"),
    ("MIE", "MIERCOLES_PLAN", "MIERCOLES_FLAG"),
    ("JUE", "JUEVES_PLAN", "JUEVES_FLAG"),
    ("VIE", "VIERNES_PLAN", "VIERNES_FLAG"),
    ("SAB", "SABADO_PLAN", "SABADO_FLAG"),
    ("DOM", "DOMINGO_PLAN", "DOMINGO_FLAG"),
]


def ensure_sslmode(db_url: str) -> str:
    if not db_url:
        return db_url
    if "sslmode=" in db_url:
        return db_url
    if "?" in db_url:
        return db_url + "&sslmode=require"
    return db_url + "?sslmode=require"


def resolve_db_url() -> str:
    return ensure_sslmode(
        os.getenv("DB_URL_LOAD", "")
        or os.getenv("DB_URL_APP", "")
        or os.getenv("DB_URL", "")
    )


def db_url_presence() -> dict[str, bool]:
    return {
        "DB_URL_LOAD_PRESENT": bool(os.getenv("DB_URL_LOAD")),
        "DB_URL_APP_PRESENT": bool(os.getenv("DB_URL_APP")),
        "DB_URL_PRESENT": bool(os.getenv("DB_URL")),
    }


def print_trace(tag: str, **fields: Any) -> None:
    parts = [tag]
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    print(" ".join(parts))


def git_status_payload() -> dict[str, list[str]]:
    sb = subprocess.run(
        ["git", "status", "-sb"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    short = subprocess.run(
        ["git", "status", "--short"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "status_sb": [line for line in sb.stdout.splitlines() if line.strip()],
        "status_short": [line for line in short.stdout.splitlines() if line.strip()],
    }


def query_df(conn, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    stripped = sql.lstrip().upper()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        raise RuntimeError("NON_SELECT_QUERY_BLOCKED")
    return pd.read_sql_query(sql, conn, params=params)


def norm_text(value: Any) -> str:
    return str(value or "").strip().upper()


def split_pipe_tokens(value: Any) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for part in raw.split("|"):
        token = part.strip()
        token_norm = norm_text(token)
        if token and token_norm and token_norm not in seen:
            out.append(token)
            seen.add(token_norm)
    return out


def unique_pipe_tokens(values: list[Any], preferred_first: str | None = None) -> list[str]:
    token_map: dict[str, str] = {}
    for raw in values:
        for token in split_pipe_tokens(raw):
            token_norm = norm_text(token)
            if token_norm and token_norm not in token_map:
                token_map[token_norm] = token
    if not token_map:
        return []
    ordered = sorted(token_map)
    preferred_norm = norm_text(preferred_first)
    if preferred_norm and preferred_norm in token_map:
        ordered = [preferred_norm] + [key for key in ordered if key != preferred_norm]
    return [token_map[key] for key in ordered]


def pipe_contains(raw_value: Any, selected: str | None) -> bool:
    selected_norm = norm_text(selected)
    if not selected_norm:
        return True
    raw_norm = norm_text(raw_value)
    if not raw_norm:
        return False
    if raw_norm == selected_norm:
        return True
    return selected_norm in {norm_text(token) for token in split_pipe_tokens(raw_value)}


def choose_focus_token(values: list[Any]) -> str:
    raw_values = [str(value or "").strip() for value in values if str(value or "").strip()]
    standalone = [raw for raw in raw_values if "|" not in raw]
    token_values = unique_pipe_tokens(standalone or raw_values)
    return token_values[0] if token_values else ""


def route_shared_display(raw_values: list[Any], selected_rutero: str | None = None) -> str:
    token_values = unique_pipe_tokens(raw_values, preferred_first=selected_rutero)
    if len(token_values) <= 1:
        return "No"
    return " | ".join(token_values)


def checklist_status(plan_value: Any, flag_value: Any) -> str:
    plan = int(plan_value or 0)
    flag = int(flag_value or 0)
    if plan >= 1 and flag >= 1:
        return "REQ_OK"
    if plan >= 1 and flag == 0:
        return "REQ"
    if plan == 0 and flag >= 1:
        return "OK"
    return ""


def classify_kpi_vs_visual(kpi: dict[str, int], visual_summary: dict[str, int]) -> str:
    same = (
        kpi["visita_plan"] == visual_summary["sum_exigidas_sem"]
        and kpi["visitas_pendientes"] == visual_summary["sum_pendiente"]
        and kpi["cumple_rows"] == visual_summary["alerta_cumple_count"]
        and kpi["incumple_rows"] == visual_summary["alerta_incumple_count"]
    )
    if same:
        return "EXPECTED_BY_DESIGN"
    if (
        visual_summary["sum_exigidas_sem"] <= kpi["visita_plan"]
        and visual_summary["sum_pendiente"] <= kpi["visitas_pendientes"]
    ):
        return "CONSOLIDATION_EFFECT"
    return "POSSIBLE_BUG"


def classify_edge_case(row: dict[str, Any]) -> str:
    if row.get("exigidas_sem", 0) <= 0:
        return "NEEDS_BUSINESS_DECISION"
    if row.get("alerta", "") not in {"CUMPLE", "INCUMPLE"}:
        return "BUG_OPERATIVO"
    if row.get("offplan_evidence_days", 0) > 0 and row.get("ruta_compartida", "No") != "No":
        return "EXPECTED_BY_DESIGN"
    if row.get("route_shared_token_count", 0) >= 3 or row.get("gestion_shared_token_count", 0) >= 3:
        return "EXPECTED_BY_DESIGN"
    if row.get("diff_days_vs_exigidas", 0) >= 3:
        return "DEUDA_UX"
    if row.get("pendiente", 0) > row.get("dias_pendientes_visibles", 0):
        return "WATCHLIST"
    if row.get("sobrecumplimiento", 0) > 0:
        return "EXPECTED_BY_DESIGN"
    return "DEUDA_DATOS"


def current_week_summary(conn) -> dict[str, Any]:
    sql = f"""
    WITH current_week AS (
        SELECT MAX("SEMANA_INICIO") AS semana_inicio
        FROM {MV_NAME}
    )
    SELECT
        CAST(cw.semana_inicio AS text) AS semana_inicio,
        COUNT(*)::int AS total_rows,
        COALESCE(SUM(COALESCE(v."VISITA", 0)), 0)::int AS visita_plan,
        COALESCE(SUM(COALESCE(v."VISITA_REALIZADA_CAP", 0)), 0)::int AS visita_realizada_cap,
        COALESCE(SUM(COALESCE(v."VISITAS_PENDIENTES_CALC", 0)), 0)::int AS visitas_pendientes,
        COALESCE(SUM(CASE WHEN COALESCE(v."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(v."ALERTA", '')))) = 'CUMPLE' THEN 1 ELSE 0 END), 0)::int AS cumple_rows,
        COALESCE(SUM(CASE WHEN COALESCE(v."ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE(v."ALERTA", '')))) = 'INCUMPLE' THEN 1 ELSE 0 END), 0)::int AS incumple_rows
    FROM {MV_NAME} v
    CROSS JOIN current_week cw
    WHERE v."SEMANA_INICIO" = cw.semana_inicio
    GROUP BY cw.semana_inicio
    """
    df = query_df(conn, sql)
    if df.empty:
        raise RuntimeError("NO_CURRENT_WEEK_IN_MV")
    return df.iloc[0].to_dict()


def load_current_week_frames(conn, semana_inicio: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    weekly_sql = f"""
    SELECT
        CAST("SEMANA_INICIO" AS text) AS "SEMANA_INICIO",
        CAST("GESTOR" AS text) AS "GESTOR",
        CAST("RUTERO" AS text) AS "RUTERO",
        CAST("REPONEDOR" AS text) AS "REPONEDOR",
        CAST("COD_RT" AS text) AS "COD_RT",
        CAST("LOCAL" AS text) AS "LOCAL",
        CAST("CLIENTE" AS text) AS "CLIENTE",
        CAST("MODALIDAD" AS text) AS "MODALIDAD",
        COALESCE("VISITA", 0)::int AS "VISITA",
        COALESCE("VISITA_REALIZADA_RAW", 0)::int AS "VISITA_REALIZADA_RAW",
        COALESCE("VISITA_REALIZADA_CAP", 0)::int AS "VISITA_REALIZADA_CAP",
        COALESCE("SOBRE_CUMPLIMIENTO", 0)::int AS "SOBRE_CUMPLIMIENTO",
        CAST(COALESCE("ALERTA_NORM_FILTER", UPPER(TRIM(COALESCE("ALERTA", '')))) AS text) AS "ALERTA_NORM_FILTER",
        CAST(COALESCE("ALERTA", '') AS text) AS "ALERTA",
        COALESCE("RUTA_DUPLICADA_FLAG", 0)::int AS "RUTA_DUPLICADA_FLAG",
        COALESCE("RUTA_DUPLICADA_ROWS", 0)::int AS "RUTA_DUPLICADA_ROWS",
        COALESCE("GESTION_COMPARTIDA_FLAG_CALC", 0)::int AS "GESTION_COMPARTIDA_FLAG_CALC",
        COALESCE("VISITAS_PENDIENTES_CALC", 0)::int AS "VISITAS_PENDIENTES_CALC",
        COALESCE("LUNES_PLAN", 0)::int AS "LUNES_PLAN",
        COALESCE("LUNES_FLAG", 0)::int AS "LUNES_FLAG",
        COALESCE("MARTES_PLAN", 0)::int AS "MARTES_PLAN",
        COALESCE("MARTES_FLAG", 0)::int AS "MARTES_FLAG",
        COALESCE("MIERCOLES_PLAN", 0)::int AS "MIERCOLES_PLAN",
        COALESCE("MIERCOLES_FLAG", 0)::int AS "MIERCOLES_FLAG",
        COALESCE("JUEVES_PLAN", 0)::int AS "JUEVES_PLAN",
        COALESCE("JUEVES_FLAG", 0)::int AS "JUEVES_FLAG",
        COALESCE("VIERNES_PLAN", 0)::int AS "VIERNES_PLAN",
        COALESCE("VIERNES_FLAG", 0)::int AS "VIERNES_FLAG",
        COALESCE("SABADO_PLAN", 0)::int AS "SABADO_PLAN",
        COALESCE("SABADO_FLAG", 0)::int AS "SABADO_FLAG",
        COALESCE("DOMINGO_PLAN", 0)::int AS "DOMINGO_PLAN",
        COALESCE("DOMINGO_FLAG", 0)::int AS "DOMINGO_FLAG"
    FROM {MV_NAME}
    WHERE "SEMANA_INICIO" = %(semana_inicio)s
    """
    route_sql = f"""
    SELECT
        CAST(effective_week_start AS text) AS semana_inicio,
        CAST(gestor AS text) AS gestor,
        CAST(rutero AS text) AS rutero,
        CAST(cliente AS text) AS cliente,
        CAST(local_nombre AS text) AS local_nombre
    FROM {ROUTE_VIEW}
    WHERE CAST(effective_week_start AS text) = %(semana_inicio)s
    """
    return (
        query_df(conn, weekly_sql, {"semana_inicio": semana_inicio}),
        query_df(conn, route_sql, {"semana_inicio": semana_inicio}),
    )


def build_gestores(route_df: pd.DataFrame) -> list[str]:
    return unique_pipe_tokens(route_df["gestor"].dropna().astype(str).tolist())


def build_rutero_selector(route_df: pd.DataFrame, gestor: str | None) -> dict[str, Any]:
    scope = route_df.copy()
    if gestor:
        mask = scope["gestor"].apply(lambda raw: pipe_contains(raw, gestor))
        scope = scope.loc[mask].copy()
    raw_ruteros = scope["rutero"].dropna().astype(str).tolist()
    standalone = [raw.strip() for raw in raw_ruteros if raw and "|" not in raw]
    selector = unique_pipe_tokens(standalone or raw_ruteros)
    secondary_only = [token for token in unique_pipe_tokens(raw_ruteros) if token not in selector]
    return {
        "selector": selector,
        "raw_ruteros": raw_ruteros,
        "standalone_ruteros": unique_pipe_tokens(standalone),
        "pipe_ruteros": [raw for raw in raw_ruteros if "|" in raw],
        "fallback_used": not bool(standalone),
        "secondary_only": secondary_only,
    }


def filter_weekly_df(
    week_df: pd.DataFrame,
    *,
    gestor: str | None = None,
    rutero: str | None = None,
    cliente: str | None = None,
    local: str | None = None,
    alerta: str | None = None,
) -> pd.DataFrame:
    df = week_df.copy()
    if gestor:
        df = df.loc[df["GESTOR"].apply(lambda raw: pipe_contains(raw, gestor))].copy()
    if rutero:
        df = df.loc[df["RUTERO"].apply(lambda raw: pipe_contains(raw, rutero))].copy()
    if cliente:
        cliente_norm = norm_text(cliente)
        df = df.loc[df["CLIENTE"].apply(lambda raw: norm_text(raw) == cliente_norm)].copy()
    if local:
        local_norm = norm_text(local)
        df = df.loc[df["LOCAL"].apply(lambda raw: norm_text(raw) == local_norm)].copy()
    if alerta:
        alerta_norm = norm_text(alerta)
        df = df.loc[df["ALERTA_NORM_FILTER"].apply(lambda raw: norm_text(raw) == alerta_norm)].copy()
    return df.reset_index(drop=True)


def summarize_kpi(filtered_df: pd.DataFrame) -> dict[str, int]:
    if filtered_df.empty:
        return {
            "total_rows": 0,
            "visita_plan": 0,
            "visita_realizada_cap": 0,
            "visitas_pendientes": 0,
            "cumple_rows": 0,
            "incumple_rows": 0,
            "gestion_compartida_rows": 0,
        }
    alerta_norm = filtered_df["ALERTA_NORM_FILTER"].fillna("").astype(str).str.upper().str.strip()
    return {
        "total_rows": int(len(filtered_df)),
        "visita_plan": int(filtered_df["VISITA"].fillna(0).sum()),
        "visita_realizada_cap": int(filtered_df["VISITA_REALIZADA_CAP"].fillna(0).sum()),
        "visitas_pendientes": int(filtered_df["VISITAS_PENDIENTES_CALC"].fillna(0).sum()),
        "cumple_rows": int((alerta_norm == "CUMPLE").sum()),
        "incumple_rows": int((alerta_norm == "INCUMPLE").sum()),
        "gestion_compartida_rows": int(filtered_df["GESTION_COMPARTIDA_FLAG_CALC"].fillna(0).sum()),
    }


def build_visual_table(
    filtered_df: pd.DataFrame,
    *,
    selected_gestor: str | None = None,
    selected_rutero: str | None = None,
) -> pd.DataFrame:
    if filtered_df.empty:
        return pd.DataFrame()
    records: list[dict[str, Any]] = []
    group_cols = ["SEMANA_INICIO", "COD_RT", "LOCAL", "CLIENTE", "MODALIDAD"]
    for keys, group in filtered_df.groupby(group_cols, dropna=False, sort=False):
        semana_inicio, cod_rt, local, cliente, modalidad = keys
        record: dict[str, Any] = {
            "SEMANA_INICIO": semana_inicio,
            "GESTOR_FOCO": selected_gestor or choose_focus_token(group["GESTOR"].tolist()),
            "RUTERO_FOCO": selected_rutero or choose_focus_token(group["RUTERO"].tolist()),
            "COD_RT": cod_rt,
            "LOCAL": local,
            "CLIENTE": cliente,
            "MODALIDAD": modalidad,
            "EXIGIDAS SEM.": int(group["VISITA"].fillna(0).max()),
            "VISITAS_REALIZADAS_CAP_VISUAL": int(group["VISITA_REALIZADA_CAP"].fillna(0).max()),
            "VISITAS_REALIZADAS_RAW_VISUAL": int(group["VISITA_REALIZADA_RAW"].fillna(0).max()),
            "PENDIENTE": int(group["VISITAS_PENDIENTES_CALC"].fillna(0).max()),
            "SOBRECUMPLIMIENTO": int(group["SOBRE_CUMPLIMIENTO"].fillna(0).max()),
        }
        alerta_norm = group["ALERTA_NORM_FILTER"].fillna("").astype(str).str.upper().str.strip()
        record["ALERTA"] = "INCUMPLE" if (alerta_norm == "INCUMPLE").any() else "CUMPLE"

        shared_flag = int(group["GESTION_COMPARTIDA_FLAG_CALC"].fillna(0).max())
        all_gestor_tokens = unique_pipe_tokens(group["GESTOR"].dropna().astype(str).tolist(), preferred_first=selected_gestor)
        record["GESTION_COMPARTIDA_TOKENS"] = all_gestor_tokens
        if shared_flag:
            pipe_values = [
                str(value).strip()
                for value in group["GESTOR"].dropna().astype(str).tolist()
                if "|" in str(value)
            ]
            if pipe_values:
                detail = max(pipe_values)
            else:
                raw_values = [str(value).strip() for value in group["GESTOR"].dropna().astype(str).tolist() if str(value).strip()]
                detail = max(raw_values) if raw_values else "Compartida"
            record["GESTION COMPARTIDA"] = f"Si | {detail}" if detail else "Si"
        else:
            record["GESTION COMPARTIDA"] = "No"

        route_raw_values = [
            str(value).strip()
            for value in group["RUTERO"].dropna().astype(str).tolist()
            if str(value).strip()
        ]
        record["RUTA_COMPARTIDA"] = route_shared_display(route_raw_values, selected_rutero=record["RUTERO_FOCO"])
        route_tokens = unique_pipe_tokens(route_raw_values, preferred_first=record["RUTERO_FOCO"])
        record["RUTA_COMPARTIDA_TOKENS"] = route_tokens

        day_statuses: dict[str, str] = {}
        days_planificados = 0
        days_con_evidencia = 0
        days_pendientes_visibles = 0
        offplan_evidence_days = 0
        for display, plan_col, flag_col in DAY_META:
            plan_max = int(group[plan_col].fillna(0).max())
            flag_max = int(group[flag_col].fillna(0).max())
            status = checklist_status(plan_max, flag_max)
            day_statuses[display] = status
            if plan_max >= 1:
                days_planificados += 1
            if flag_max >= 1:
                days_con_evidencia += 1
            if status == "REQ":
                days_pendientes_visibles += 1
            if status == "OK":
                offplan_evidence_days += 1
        record.update(day_statuses)
        record["dias_planificados"] = days_planificados
        record["dias_con_evidencia"] = days_con_evidencia
        record["dias_pendientes_visibles"] = days_pendientes_visibles
        record["offplan_evidence_days"] = offplan_evidence_days
        record["diff_days_vs_exigidas"] = abs(days_planificados - record["EXIGIDAS SEM."])
        record["route_shared_token_count"] = len(route_tokens) if route_tokens else 0
        record["gestion_shared_token_count"] = len(all_gestor_tokens) if all_gestor_tokens else 0
        records.append(record)

    return pd.DataFrame(records)


def interpret_rule_case(category: str) -> str:
    mapping = {
        "A": "Cumple semanalmente aunque existan mas dias planificados que exigencia semanal.",
        "B": "La exigencia semanal es mayor que la evidencia acumulada y por eso incumple.",
        "C": "La exigencia semanal coincide con los dias planificados, pero la evidencia es parcial.",
        "D": "Existe evidencia registrada en al menos un dia no planificado.",
        "E": "Existe sobrecumplimiento semanal sobre la exigencia definida.",
        "F": "Hay exigencia semanal y no existe evidencia registrada para el universo visible.",
        "G": "Se detecta exigencia semanal nula o cero; revisar si es esperado o anomalia.",
    }
    return mapping.get(category, "")


def select_rule_cases(visual_df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    if visual_df.empty:
        return {key: [] for key in "ABCDEFG"}

    def pack(df: pd.DataFrame, category: str) -> list[dict[str, Any]]:
        if df.empty:
            return []
        out: list[dict[str, Any]] = []
        for _, row in df.head(10).iterrows():
            out.append(
                {
                    "gestor": row["GESTOR_FOCO"],
                    "rutero_foco": row["RUTERO_FOCO"],
                    "cod_rt": row["COD_RT"],
                    "local": row["LOCAL"],
                    "cliente": row["CLIENTE"],
                    "modalidad": row["MODALIDAD"],
                    "exigidas_sem": int(row["EXIGIDAS SEM."]),
                    "dias_planificados": int(row["dias_planificados"]),
                    "visitas_realizadas_cap": int(row["VISITAS_REALIZADAS_CAP_VISUAL"]),
                    "pendiente": int(row["PENDIENTE"]),
                    "alerta": row["ALERTA"],
                    "LUN": row["LUN"],
                    "MAR": row["MAR"],
                    "MIE": row["MIE"],
                    "JUE": row["JUE"],
                    "VIE": row["VIE"],
                    "SAB": row["SAB"],
                    "DOM": row["DOM"],
                    "ruta_compartida": row["RUTA_COMPARTIDA"],
                    "gestion_compartida": row["GESTION COMPARTIDA"],
                    "interpretacion_operacional": interpret_rule_case(category),
                }
            )
        return out

    a_df = visual_df.loc[
        (visual_df["EXIGIDAS SEM."] == 1)
        & (visual_df["dias_planificados"] > 1)
        & (visual_df["VISITAS_REALIZADAS_CAP_VISUAL"] >= 1)
        & (visual_df["ALERTA"] == "CUMPLE")
    ].sort_values(["dias_planificados", "LOCAL", "CLIENTE"], ascending=[False, True, True])
    b_df = visual_df.loc[
        (visual_df["EXIGIDAS SEM."] == 2)
        & (visual_df["dias_planificados"] > 2)
        & (visual_df["VISITAS_REALIZADAS_CAP_VISUAL"] == 1)
        & (visual_df["ALERTA"] == "INCUMPLE")
    ].sort_values(["PENDIENTE", "LOCAL", "CLIENTE"], ascending=[False, True, True])
    c_df = visual_df.loc[
        (visual_df["EXIGIDAS SEM."] == visual_df["dias_planificados"])
        & (visual_df["VISITAS_REALIZADAS_CAP_VISUAL"] > 0)
        & (visual_df["VISITAS_REALIZADAS_CAP_VISUAL"] < visual_df["EXIGIDAS SEM."])
        & (visual_df["ALERTA"] == "INCUMPLE")
    ].sort_values(["PENDIENTE", "LOCAL", "CLIENTE"], ascending=[False, True, True])
    d_df = visual_df.loc[visual_df["offplan_evidence_days"] > 0].sort_values(
        ["offplan_evidence_days", "LOCAL", "CLIENTE"], ascending=[False, True, True]
    )
    e_df = visual_df.loc[visual_df["SOBRECUMPLIMIENTO"] > 0].sort_values(
        ["SOBRECUMPLIMIENTO", "LOCAL", "CLIENTE"], ascending=[False, True, True]
    )
    f_df = visual_df.loc[
        (visual_df["EXIGIDAS SEM."] > 0)
        & (visual_df["VISITAS_REALIZADAS_CAP_VISUAL"] == 0)
        & (visual_df["ALERTA"] == "INCUMPLE")
    ].sort_values(["PENDIENTE", "LOCAL", "CLIENTE"], ascending=[False, True, True])
    g_df = visual_df.loc[visual_df["EXIGIDAS SEM."] <= 0].sort_values(["LOCAL", "CLIENTE"])
    return {
        "A": pack(a_df, "A"),
        "B": pack(b_df, "B"),
        "C": pack(c_df, "C"),
        "D": pack(d_df, "D"),
        "E": pack(e_df, "E"),
        "F": pack(f_df, "F"),
        "G": pack(g_df, "G"),
    }


def select_shared_cases(visual_df: pd.DataFrame) -> dict[str, Any]:
    if visual_df.empty:
        return {"cases": {}, "validations": {}}

    def rows_to_cases(df: pd.DataFrame, label: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for _, row in df.head(5).iterrows():
            out.append(
                {
                    "gestor": row["GESTOR_FOCO"],
                    "rutero_foco": row["RUTERO_FOCO"],
                    "cod_rt": row["COD_RT"],
                    "local": row["LOCAL"],
                    "cliente": row["CLIENTE"],
                    "gestion_compartida": row["GESTION COMPARTIDA"],
                    "ruta_compartida": row["RUTA_COMPARTIDA"],
                    "exigidas_sem": int(row["EXIGIDAS SEM."]),
                    "alerta": row["ALERTA"],
                    "explicacion": label,
                }
            )
        return out

    route_no = visual_df["RUTA_COMPARTIDA"].fillna("No").astype(str).str.upper().eq("NO")
    gestion_no = visual_df["GESTION COMPARTIDA"].fillna("No").astype(str).str.upper().eq("NO")
    none_df = visual_df.loc[route_no & gestion_no]
    route_only_df = visual_df.loc[~route_no & gestion_no]
    gestion_only_df = visual_df.loc[route_no & ~gestion_no]
    both_df = visual_df.loc[~route_no & ~gestion_no]

    route_token_count_ok = int(
        visual_df.loc[~route_no, "RUTA_COMPARTIDA_TOKENS"].apply(lambda tokens: len(tokens) > 1).sum()
    )
    gestion_single_token = int(
        visual_df.loc[~gestion_no, "GESTION_COMPARTIDA_TOKENS"].apply(lambda tokens: len(tokens) <= 1).sum()
    )

    return {
        "cases": {
            "ninguna_compartida": rows_to_cases(none_df, "No hay comparticion visible de gestores ni rutas."),
            "solo_ruta_compartida": rows_to_cases(route_only_df, "La ruta tiene multiples ruteros pero la gestion visible no esta compartida."),
            "solo_gestion_compartida": rows_to_cases(gestion_only_df, "La gestion visible esta compartida, pero la ruta no muestra rutero secundario."),
            "ambas_compartidas": rows_to_cases(both_df, "Conviven gestores compartidos y ruta compartida en el mismo universo visible."),
        },
        "validations": {
            "ruta_compartida_with_more_than_one_rutero_rows": route_token_count_ok,
            "gestion_compartida_single_token_rows": gestion_single_token,
        },
    }


def pick_universes(route_df: pd.DataFrame, week_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    gestores = build_gestores(route_df)
    gestor_counts: list[tuple[str, int]] = []
    for gestor in gestores:
        count = int(len(filter_weekly_df(week_df, gestor=gestor)))
        gestor_counts.append((gestor, count))
    gestor_counts = [(gestor, count) for gestor, count in gestor_counts if count > 0]
    gestor_counts.sort(key=lambda item: (-item[1], item[0]))

    high_gestor = gestor_counts[0][0] if gestor_counts else None
    universes: dict[str, dict[str, Any]] = {"global_week": {}}
    if high_gestor:
        universes["high_volume_gestor"] = {"gestor": high_gestor}

    route_shared_pick: dict[str, Any] | None = None
    no_shared_pick: dict[str, Any] | None = None
    for gestor, _count in gestor_counts:
        rutero_info = build_rutero_selector(route_df, gestor)
        for rutero in rutero_info["selector"]:
            scoped_raw = filter_weekly_df(week_df, gestor=gestor, rutero=rutero)
            scoped_visual = build_visual_table(scoped_raw, selected_gestor=gestor, selected_rutero=rutero)
            if scoped_visual.empty:
                continue
            has_route_shared = (scoped_visual["RUTA_COMPARTIDA"] != "No").any()
            if has_route_shared and route_shared_pick is None:
                route_shared_pick = {"gestor": gestor, "rutero": rutero}
            if not has_route_shared and no_shared_pick is None:
                no_shared_pick = {"gestor": gestor, "rutero": rutero}
            if route_shared_pick and no_shared_pick:
                break
        if route_shared_pick and no_shared_pick:
            break

    if route_shared_pick:
        universes["gestor_rutero_route_shared"] = route_shared_pick
    if no_shared_pick:
        universes["gestor_rutero_no_route_shared"] = no_shared_pick

    client_counts = (
        week_df.groupby("CLIENTE", dropna=False)["COD_RT"]
        .count()
        .sort_values(ascending=False)
        .reset_index(name="row_count")
    )
    if not client_counts.empty:
        top_client = str(client_counts.iloc[0]["CLIENTE"])
        universes["high_volume_client"] = {"cliente": top_client}
    return universes


def summarize_visual(visual_df: pd.DataFrame) -> dict[str, int]:
    if visual_df.empty:
        return {
            "visible_rows": 0,
            "sum_exigidas_sem": 0,
            "sum_pendiente": 0,
            "alerta_cumple_count": 0,
            "alerta_incumple_count": 0,
        }
    alerta = visual_df["ALERTA"].fillna("").astype(str).str.upper().str.strip()
    return {
        "visible_rows": int(len(visual_df)),
        "sum_exigidas_sem": int(visual_df["EXIGIDAS SEM."].fillna(0).sum()),
        "sum_pendiente": int(visual_df["PENDIENTE"].fillna(0).sum()),
        "alerta_cumple_count": int((alerta == "CUMPLE").sum()),
        "alerta_incumple_count": int((alerta == "INCUMPLE").sum()),
    }


def build_kpi_vs_visual_cases(route_df: pd.DataFrame, week_df: pd.DataFrame) -> list[dict[str, Any]]:
    universes = pick_universes(route_df, week_df)
    cases: list[dict[str, Any]] = []
    for name, scope in universes.items():
        raw_df = filter_weekly_df(
            week_df,
            gestor=scope.get("gestor"),
            rutero=scope.get("rutero"),
            cliente=scope.get("cliente"),
        )
        visual_df = build_visual_table(
            raw_df,
            selected_gestor=scope.get("gestor"),
            selected_rutero=scope.get("rutero"),
        )
        kpi = summarize_kpi(raw_df)
        visual_summary = summarize_visual(visual_df)
        classification = classify_kpi_vs_visual(kpi, visual_summary)
        cases.append(
            {
                "universe_name": name,
                "scope": scope,
                "kpi_weekly_raw": kpi,
                "tabla_visual_consolidada": visual_summary,
                "classification": classification,
            }
        )
    return cases


def build_edge_cases(visual_df: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    if visual_df.empty:
        return []
    frames: list[pd.DataFrame] = []
    frames.append(visual_df.sort_values("diff_days_vs_exigidas", ascending=False).head(5))
    frames.append(visual_df.sort_values("route_shared_token_count", ascending=False).head(5))
    frames.append(visual_df.sort_values("gestion_shared_token_count", ascending=False).head(5))
    frames.append(visual_df.sort_values("PENDIENTE", ascending=False).head(5))
    frames.append(visual_df.sort_values("SOBRECUMPLIMIENTO", ascending=False).head(5))
    frames.append(visual_df.loc[visual_df["offplan_evidence_days"] > 0].sort_values("offplan_evidence_days", ascending=False).head(5))
    merged = pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["COD_RT", "LOCAL", "CLIENTE", "MODALIDAD"]
    )
    out: list[dict[str, Any]] = []
    for _, row in merged.head(limit).iterrows():
        item = {
            "gestor": row["GESTOR_FOCO"],
            "rutero_foco": row["RUTERO_FOCO"],
            "cod_rt": row["COD_RT"],
            "local": row["LOCAL"],
            "cliente": row["CLIENTE"],
            "modalidad": row["MODALIDAD"],
            "exigidas_sem": int(row["EXIGIDAS SEM."]),
            "dias_planificados": int(row["dias_planificados"]),
            "dias_con_evidencia": int(row["dias_con_evidencia"]),
            "dias_pendientes_visibles": int(row["dias_pendientes_visibles"]),
            "pendiente": int(row["PENDIENTE"]),
            "alerta": row["ALERTA"],
            "ruta_compartida": row["RUTA_COMPARTIDA"],
            "gestion_compartida": row["GESTION COMPARTIDA"],
            "sobrecumplimiento": int(row["SOBRECUMPLIMIENTO"]),
            "offplan_evidence_days": int(row["offplan_evidence_days"]),
            "classification": classify_edge_case(row.to_dict()),
        }
        out.append(item)
    return out


def build_export_risk_summary() -> dict[str, Any]:
    return {
        "operational_consolidated_ready_columns": [
            "COD_RT",
            "LOCAL",
            "CLIENTE",
            "EXIGIDAS SEM.",
            "LUN",
            "MAR",
            "MIE",
            "JUE",
            "VIE",
            "SAB",
            "DOM",
            "PENDIENTE",
            "ALERTA",
            "MODALIDAD",
            "GESTION COMPARTIDA",
            "RUTA COMPARTIDA",
        ],
        "raw_audit_only_columns": [
            "VISITA_REALIZADA_RAW",
            "VISITA_REALIZADA_CAP",
            "SOBRE_CUMPLIMIENTO",
            "GESTOR",
            "RUTERO",
            "REPONEDOR",
            "RUTA_DUPLICADA_FLAG",
            "RUTA_DUPLICADA_ROWS",
        ],
        "labels_to_keep_explicit": [
            "EXIGIDAS SEM.",
            "PENDIENTE",
            "GESTION COMPARTIDA",
            "RUTA COMPARTIDA",
        ],
        "fields_not_to_compare_one_to_one_with_kpi": [
            "sum EXIGIDAS SEM. visual vs visita_plan raw",
            "sum PENDIENTE visual vs visitas_pendientes raw",
            "count ALERTA visual vs cumple_rows/incumple_rows raw",
            "count filas visibles compartidas vs gestion_compartida_rows raw",
        ],
        "recommended_grain": {
            "resumen_ejecutivo": "weekly raw aggregated",
            "detalle_operativo": "visual consolidated",
            "auditoria_tecnica": "weekly raw mv rows",
        },
    }


def final_classification(
    rule_cases: dict[str, list[dict[str, Any]]],
    kpi_vs_visual: list[dict[str, Any]],
    edge_cases: list[dict[str, Any]],
) -> tuple[list[str], str, str]:
    classes: list[str] = []
    if any(case["classification"] == "POSSIBLE_BUG" for case in kpi_vs_visual):
        classes.append("DEUDA_DATOS")
    if any(item["classification"] == "NEEDS_BUSINESS_DECISION" for item in edge_cases):
        classes.append("NEEDS_BUSINESS_DECISION")
    if any(item["classification"] == "DEUDA_UX" for item in edge_cases):
        classes.append("DEUDA_UX")
    if not classes:
        classes.append("EXPECTED_BY_DESIGN")

    has_anomaly_g = len(rule_cases.get("G", [])) > 0
    if "DEUDA_DATOS" in classes:
        verdict = "NEEDS_DATA_CONTRACT_FIX"
        next_action = "Review raw-to-visual consistency before export design."
    elif has_anomaly_g or "NEEDS_BUSINESS_DECISION" in classes:
        verdict = "NEEDS_BUSINESS_RULE_DECISION"
        next_action = "Clarify zero-or-null weekly requirement semantics before export design."
    elif "DEUDA_UX" in classes:
        verdict = "NEEDS_UX_LABEL_PATCH"
        next_action = "Keep labels explicit in any export contract."
    else:
        verdict = "READY_FOR_EXPORT_CONTRACT_DESIGN"
        next_action = "Proceed to export contract design using the visual consolidated grain."
    return classes, verdict, next_action


@dataclass
class AuditResult:
    artifact_path: Path
    payload: dict[str, Any]


def run_audit(statement_timeout_seconds: int = 300) -> AuditResult:
    db_url = resolve_db_url()
    presence = db_url_presence()
    print(json.dumps(presence, ensure_ascii=False))
    if not db_url:
        print("NO_DB_URL_AVAILABLE")
        raise RuntimeError("NO_DB_URL_AVAILABLE")

    import psycopg2

    with psycopg2.connect(db_url) as conn:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout TO {max(int(statement_timeout_seconds), 1) * 1000}")

        current_week = current_week_summary(conn)
        semana_inicio = str(current_week["semana_inicio"])
        print_trace("CG_V2_AUDIT_WEEK", semana_inicio=semana_inicio, total_rows=current_week["total_rows"])

        week_df, route_df = load_current_week_frames(conn, semana_inicio)
        global_visual_df = build_visual_table(week_df)
        rule_cases = select_rule_cases(global_visual_df)
        shared_cases = select_shared_cases(global_visual_df)
        kpi_vs_visual_cases = build_kpi_vs_visual_cases(route_df, week_df)
        edge_cases = build_edge_cases(global_visual_df, limit=20)
        export_risk = build_export_risk_summary()
        classes, verdict, next_action = final_classification(
            rule_cases,
            kpi_vs_visual_cases,
            edge_cases,
        )

    payload = {
        "meta": {
            "phase": "FASE_9B13_CONTROL_GESTION_V2_BUSINESS_RULE_EVIDENCE_PACK",
            "generated_at_local": datetime.now().isoformat(),
            "cwd": str(ROOT),
            "mv_name": MV_NAME,
            "route_view": ROUTE_VIEW,
        },
        "git_status": git_status_payload(),
        "db_url_presence": presence,
        "current_week_summary": current_week,
        "weekly_rule_cases": rule_cases,
        "shared_route_gestion_cases": shared_cases,
        "kpi_vs_visual_grain_cases": kpi_vs_visual_cases,
        "edge_cases": edge_cases,
        "export_risk": export_risk,
        "classification": classes,
        "verdict": verdict,
        "recommended_next_action": next_action,
    }

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    artifact_path = ARTIFACT_DIR / f"CG_V2_BUSINESS_RULE_EVIDENCE_PACK_9B13_{ts}.json"
    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print_trace("CG_V2_AUDIT_ARTIFACT_OK", path=artifact_path)
    return AuditResult(artifact_path=artifact_path, payload=payload)


def main() -> int:
    try:
        result = run_audit()
        print(json.dumps({"artifact_path": str(result.artifact_path), "verdict": result.payload["verdict"]}, ensure_ascii=False))
        return 0
    except Exception as exc:
        if str(exc) == "NO_DB_URL_AVAILABLE":
            print(json.dumps({"status": "error", "error": "NO_DB_URL_AVAILABLE"}, ensure_ascii=False))
            return 1
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
