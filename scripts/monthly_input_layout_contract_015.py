# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import date, datetime, timedelta


PHASE_ID = "015A_MONTHLY_INPUT_LAYOUT_TRACEABILITY_NO_APPLY"

OPERATIONAL_WEEK_MONTH_RULE = (
    "Semana operativa lunes-domingo; una semana pertenece al mes que contiene >=4 días."
)

RULE_AUTHORITY = {
    "promoted_from": "014A_JUNE_DATA_FOUNDATION_GATE_NO_APPLY",
    "source_file": "scripts/validate_june_data_foundation_gate_014A_no_apply.py",
    "source_text": "Semana operativa lunes-domingo; asignación al mes con >=4 días.",
    "source_example": {
        "week_start": "2026-06-29",
        "week_end": "2026-07-05",
        "assigned_month": "2026-07",
    },
}


def validate_month_id(month_id: str) -> str:
    raw = str(month_id or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", raw):
        raise ValueError("month_id must use YYYY-MM")
    year, month = raw.split("-")
    month_number = int(month)
    if month_number < 1 or month_number > 12:
        raise ValueError("month_id month must be between 01 and 12")
    return f"{int(year):04d}-{month_number:02d}"


def _coerce_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip()[:10])


def monday_start_from_fecha(fecha: date | datetime | str) -> date:
    day = _coerce_date(fecha)
    return day - timedelta(days=day.weekday())


def assigned_operational_month_from_week_start(week_start: date | datetime | str) -> str:
    start = _coerce_date(week_start)
    if start.weekday() != 0:
        raise ValueError("week_start must be Monday")
    counts: dict[str, int] = {}
    for offset in range(7):
        day = start + timedelta(days=offset)
        month_id = f"{day.year:04d}-{day.month:02d}"
        counts[month_id] = counts.get(month_id, 0) + 1
    assigned_month, assigned_days = max(counts.items(), key=lambda item: item[1])
    if assigned_days < 4:
        raise ValueError("no month contains at least four days for this week")
    return assigned_month


def assigned_operational_month_from_fecha(fecha: date | datetime | str) -> str:
    return assigned_operational_month_from_week_start(monday_start_from_fecha(fecha))


def folder_month_matches_assigned_week(folder_month_id: str, fecha: date | datetime | str) -> bool:
    return validate_month_id(folder_month_id) == assigned_operational_month_from_fecha(fecha)


def week_assignment(fecha: date | datetime | str, *, folder_month_id: str | None = None) -> dict[str, object]:
    start = monday_start_from_fecha(fecha)
    assigned_month = assigned_operational_month_from_week_start(start)
    result: dict[str, object] = {
        "fecha": _coerce_date(fecha).isoformat(),
        "week_start": start.isoformat(),
        "week_end": (start + timedelta(days=6)).isoformat(),
        "assigned_operational_month": assigned_month,
        "rule": OPERATIONAL_WEEK_MONTH_RULE,
    }
    if folder_month_id is not None:
        folder_month = validate_month_id(folder_month_id)
        result["folder_month_id"] = folder_month
        result["folder_month_governs_week_ownership"] = False
        result["folder_month_matches_assigned_operational_month"] = folder_month == assigned_month
    return result
