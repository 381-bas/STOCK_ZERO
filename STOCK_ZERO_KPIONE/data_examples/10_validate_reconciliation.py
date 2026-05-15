from __future__ import annotations

import csv
import re
import sys
from collections import defaultdict
from pathlib import Path


BASE = Path(__file__).resolve().parent
EXPECTED = BASE / "06_expected_outputs"

TESTS_RUN = 0
MISMATCHES: list[str] = []


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def as_int(value: object) -> int:
    text = str(value or "").strip()
    return int(text or "0")


def otros_validos(value: object) -> bool:
    text = str(value or "").strip().upper()
    return bool(text and text not in {"NO", "N/A", "NA", "-"})


def norm_key(row: dict[str, str], *fields: str) -> tuple[str, ...]:
    return tuple(str(row.get(field, "")).strip() for field in fields)


def compare_rows(name: str, actual: list[dict[str, object]], expected: list[dict[str, str]], key_fields: list[str]) -> None:
    global TESTS_RUN
    TESTS_RUN += 1
    actual_map = {tuple(str(row.get(k, "")) for k in key_fields): row for row in actual}
    expected_map = {tuple(str(row.get(k, "")) for k in key_fields): row for row in expected}

    for key in sorted(set(actual_map) | set(expected_map)):
        if key not in actual_map:
            MISMATCHES.append(f"{name}: missing calculated row for key={key}")
            continue
        if key not in expected_map:
            MISMATCHES.append(f"{name}: unexpected calculated row for key={key}")
            continue

        actual_row = actual_map[key]
        expected_row = expected_map[key]
        for field, expected_value in expected_row.items():
            if field not in actual_row:
                continue
            if str(actual_row[field]) != str(expected_value):
                MISMATCHES.append(
                    f"{name}: key={key} field={field} actual={actual_row[field]!r} expected={expected_value!r}"
                )


def calc_stock_kpis() -> list[dict[str, object]]:
    rows = read_csv(BASE / "01_stock_ux_sample.csv")
    scopes: list[tuple[str, str, str, list[dict[str, str]]]] = [("GLOBAL", "ALL", "ALL", rows)]
    for cod_rt in sorted({r["cod_rt"] for r in rows}):
        local_rows = [r for r in rows if r["cod_rt"] == cod_rt]
        scopes.append(("LOCAL", cod_rt, "ALL", local_rows))
        for cliente in sorted({r["cliente"] for r in local_rows}):
            scopes.append(("LOCAL_CLIENTE", cod_rt, cliente, [r for r in local_rows if r["cliente"] == cliente]))

    out = []
    for scope, cod_rt, cliente, subset in scopes:
        out.append(
            {
                "scope": scope,
                "cod_rt": cod_rt,
                "cliente": cliente,
                "total_skus": len(subset),
                "venta_0": sum(as_int(r["venta_7"]) == 0 for r in subset),
                "negativos": sum(r["negativo"].strip().upper() == "SI" for r in subset),
                "quiebres": sum(r["riesgo_quiebre"].strip().upper() == "SI" for r in subset),
                "otros_validos": sum(otros_validos(r["otros"]) for r in subset),
            }
        )
    return out


def calc_cliente_scope_summary() -> list[dict[str, object]]:
    rows = read_csv(BASE / "03_cliente_scope_sample.csv")
    groups: dict[tuple[str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[(row["responsable_tipo"], row["responsable"], row["cliente_norm"], row["cod_rt"])].append(row)

    out = []
    for (tipo, responsable, cliente, cod_rt), subset in sorted(groups.items()):
        out.append(
            {
                "responsable_tipo": tipo,
                "responsable": responsable,
                "cliente": cliente,
                "cod_rt": cod_rt,
                "total_skus": len(subset),
                "venta_0": sum(as_int(r["venta_7"]) == 0 for r in subset),
                "negativos": sum(r["negativo"].strip().upper() == "SI" for r in subset),
                "quiebres": sum(r["riesgo_quiebre"].strip().upper() == "SI" for r in subset),
                "otros_validos": sum(otros_validos(r["otros"]) for r in subset),
            }
        )
    return out


def calc_cliente_scope_responsable_totals() -> list[dict[str, object]]:
    rows = read_csv(BASE / "03_cliente_scope_sample.csv")
    groups: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[(row["responsable_tipo"], row["responsable"], row["responsable_norm"])].append(row)

    out = []
    for (tipo, responsable, responsable_norm), subset in sorted(groups.items()):
        out.append(
            {
                "responsable_tipo": tipo,
                "responsable": responsable,
                "responsable_norm": responsable_norm,
                "total_skus": len(subset),
                "venta_0": sum(as_int(r["venta_7"]) == 0 for r in subset),
                "negativos": sum(r["negativo"].strip().upper() == "SI" for r in subset),
                "quiebres": sum(r["riesgo_quiebre"].strip().upper() == "SI" for r in subset),
                "otros_validos": sum(otros_validos(r["otros"]) for r in subset),
                "locales_distintos": len({r["cod_rt"] for r in subset}),
                "clientes_distintos": len({r["cliente_norm"] for r in subset}),
            }
        )
    return out


def calc_cg_daily_resolution() -> list[dict[str, object]]:
    rows = read_csv(BASE / "05_cg_daily_evidence_sample.csv")
    out = []
    for row in rows:
        out.append(
            {
                "fecha_visita": row["fecha_visita"],
                "cod_rt": row["cod_rt"],
                "cliente": row["cliente"],
                "fuente_resuelta": row["fuente_resuelta"],
                "visita_valida": row["visita_valida"],
                "audit_only_flag": row["audit_only_flag"],
                "doble_marcaje_dia": row["doble_marcaje_dia"],
                "triple_marcaje_dia": row["triple_marcaje_dia"],
                "motivo_resolucion": row["motivo_resolucion"],
            }
        )
    return out


def audit_flags_for_weekly(daily_rows: list[dict[str, str]], semana_inicio: str, cod_rt: str, cliente: str) -> str:
    subset = [
        row
        for row in daily_rows
        if row["semana_inicio"] == semana_inicio and row["cod_rt"] == cod_rt and row["cliente"] == cliente
    ]
    if any(as_int(row["triple_marcaje_dia"]) == 1 for row in subset):
        return "triple_marcaje_dia=1"
    if any(as_int(row["doble_marcaje_dia"]) == 1 for row in subset):
        return "doble_marcaje_dia=1"
    if any(as_int(row["audit_only_flag"]) == 1 for row in subset):
        return "audit_only=1"
    if any(row["fuente_resuelta"] == "POWER_APP" for row in subset):
        return "power_app_fallback=1"
    return "none"


def calc_cg_weekly_result() -> list[dict[str, object]]:
    weekly = read_csv(BASE / "04_cg_weekly_sample.csv")
    daily = read_csv(BASE / "05_cg_daily_evidence_sample.csv")
    out = []
    for row in weekly:
        out.append(
            {
                "semana_inicio": row["semana_inicio"],
                "cod_rt": row["cod_rt"],
                "cliente": row["cliente"],
                "visita": row["visita"],
                "visita_realizada_raw_operativa": row["visita_realizada_raw_operativa"],
                "visita_realizada_cap": row["visita_realizada_cap"],
                "sobrecumplimiento": row["sobrecumplimiento"],
                "visitas_pendientes": row["visitas_pendientes"],
                "alerta": row["alerta"],
                "audit_flags": audit_flags_for_weekly(daily, row["semana_inicio"], row["cod_rt"], row["cliente"]),
            }
        )
    return out


def validate_cg_business_rules() -> None:
    global TESTS_RUN
    daily = read_csv(BASE / "05_cg_daily_evidence_sample.csv")
    weekly = read_csv(BASE / "04_cg_weekly_sample.csv")

    for row in weekly:
        TESTS_RUN += 1
        visita = as_int(row["visita"])
        cap = as_int(row["visita_realizada_cap"])
        raw = as_int(row["visita_realizada_raw_operativa"])
        sobre = as_int(row["sobrecumplimiento"])
        pendientes = as_int(row["visitas_pendientes"])
        expected_pendientes = max(visita - cap, 0)
        expected_sobre = max(raw - visita, 0)
        expected_alerta = "CUMPLE" if expected_pendientes == 0 else "INCUMPLE"
        key = norm_key(row, "semana_inicio", "cod_rt", "cliente")
        if cap > visita:
            MISMATCHES.append(f"weekly cap violation key={key}: cap={cap} visita={visita}")
        if pendientes != expected_pendientes:
            MISMATCHES.append(f"weekly pendientes mismatch key={key}: actual={pendientes} expected={expected_pendientes}")
        if sobre != expected_sobre:
            MISMATCHES.append(f"weekly sobrecumplimiento mismatch key={key}: actual={sobre} expected={expected_sobre}")
        if row["alerta"] != expected_alerta:
            MISMATCHES.append(f"weekly alerta mismatch key={key}: actual={row['alerta']} expected={expected_alerta}")

        daily_sum = sum(
            as_int(d["visita_valida"])
            for d in daily
            if d["semana_inicio"] == row["semana_inicio"] and d["cod_rt"] == row["cod_rt"] and d["cliente"] == row["cliente"]
        )
        if daily_sum != raw:
            MISMATCHES.append(f"weekly/daily mismatch key={key}: weekly_raw={raw} daily_sum={daily_sum}")

    for row in daily:
        TESTS_RUN += 1
        key = norm_key(row, "fecha_visita", "cod_rt", "cliente")
        k2 = as_int(row["kpione2_mark"])
        power = as_int(row["power_app_mark"])
        k1 = as_int(row["kpione1_mark"])
        visita_valida = as_int(row["visita_valida"])

        if k2 and (row["fuente_resuelta"] != "KPIONE2" or visita_valida != 1):
            MISMATCHES.append(f"KPIONE2 precedence violation key={key}")
        if row["fuente_resuelta"] == "POWER_APP" and k2:
            MISMATCHES.append(f"POWER_APP fallback violation key={key}: KPIONE2 also marked")
        if (not k2) and (not power) and k1:
            if row["fuente_resuelta"] != "KPIONE1_AUDIT_ONLY" or visita_valida != 0 or as_int(row["audit_only_flag"]) != 1:
                MISMATCHES.append(f"KPIONE1 audit-only violation key={key}")
        if (as_int(row["doble_marcaje_dia"]) or as_int(row["triple_marcaje_dia"])) and visita_valida > 1:
            MISMATCHES.append(f"multi-source inflation violation key={key}")


def validate_no_credentials_or_urls() -> None:
    global TESTS_RUN
    TESTS_RUN += 1
    pattern_parts = ["postgres://", r"https?://", "DB_URL", "password", "secret", "token"]
    pattern = re.compile("(" + "|".join(pattern_parts) + ")", re.IGNORECASE)
    for path in BASE.rglob("*"):
        if not path.is_file() or path.name == Path(__file__).name:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for idx, line in enumerate(text.splitlines(), start=1):
            if pattern.search(line):
                MISMATCHES.append(f"credential/url-like token in {path.relative_to(BASE)}:{idx}")


def validate_no_bad_precedence_phrase() -> None:
    global TESTS_RUN
    TESTS_RUN += 1
    bad = " > ".join(["KPIONE2", "POWER_APP", "KPIONE1"])
    for path in BASE.rglob("*"):
        if path.is_file() and path.name != Path(__file__).name and bad in path.read_text(encoding="utf-8", errors="ignore"):
            MISMATCHES.append(f"ambiguous precedence phrase found in {path.relative_to(BASE)}")


def main() -> int:
    compare_rows(
        "stock_kpis",
        calc_stock_kpis(),
        read_csv(EXPECTED / "expected_stock_kpis.csv"),
        ["scope", "cod_rt", "cliente"],
    )
    compare_rows(
        "cliente_scope_summary",
        calc_cliente_scope_summary(),
        read_csv(EXPECTED / "expected_cliente_scope_summary.csv"),
        ["responsable_tipo", "responsable", "cliente", "cod_rt"],
    )
    compare_rows(
        "cliente_scope_responsable_totals",
        calc_cliente_scope_responsable_totals(),
        read_csv(EXPECTED / "expected_cliente_scope_responsable_totals.csv"),
        ["responsable_tipo", "responsable", "responsable_norm"],
    )
    compare_rows(
        "cg_daily_resolution",
        calc_cg_daily_resolution(),
        read_csv(EXPECTED / "expected_cg_daily_resolution.csv"),
        ["fecha_visita", "cod_rt", "cliente"],
    )
    compare_rows(
        "cg_weekly_result",
        calc_cg_weekly_result(),
        read_csv(EXPECTED / "expected_cg_weekly_result.csv"),
        ["semana_inicio", "cod_rt", "cliente"],
    )
    validate_cg_business_rules()
    validate_no_credentials_or_urls()
    validate_no_bad_precedence_phrase()

    print("STOCK_ZERO data_examples reconciliation")
    print(f"tests_executed={TESTS_RUN}")
    print(f"mismatches={len(MISMATCHES)}")
    if MISMATCHES:
        for mismatch in MISMATCHES:
            print(f"FAIL: {mismatch}")
        return 1
    print("status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
