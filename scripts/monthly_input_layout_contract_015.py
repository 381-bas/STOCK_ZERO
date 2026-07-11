# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from calendar import monthrange
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


def _month_start(month_id: str) -> date:
    normalized = validate_month_id(month_id)
    year, month = (int(part) for part in normalized.split("-"))
    return date(year, month, 1)


def _month_end(month_id: str) -> date:
    normalized = validate_month_id(month_id)
    year, month = (int(part) for part in normalized.split("-"))
    return date(year, month, monthrange(year, month)[1])


def operational_weeks_for_month(month_id: str) -> list[dict[str, object]]:
    operational_month = validate_month_id(month_id)
    first_day = _month_start(operational_month)
    last_day = _month_end(operational_month)
    current = monday_start_from_fecha(first_day)
    if assigned_operational_month_from_week_start(current) != operational_month:
        current += timedelta(days=7)

    weeks: list[dict[str, object]] = []
    while current <= last_day + timedelta(days=6):
        assigned_month = assigned_operational_month_from_week_start(current)
        if assigned_month != operational_month:
            break
        week_end = current + timedelta(days=6)
        days_in_month = sum(
            1
            for offset in range(7)
            if f"{(current + timedelta(days=offset)).year:04d}-{(current + timedelta(days=offset)).month:02d}"
            == operational_month
        )
        weeks.append(
            {
                "week_label": f"S{len(weeks) + 1}",
                "week_start": current.isoformat(),
                "week_end": week_end.isoformat(),
                "assigned_operational_month": operational_month,
                "days_in_assigned_month": days_in_month,
            }
        )
        current += timedelta(days=7)
    return weeks


def operational_coverage_for_month(month_id: str) -> dict[str, object]:
    weeks = operational_weeks_for_month(month_id)
    if not weeks:
        raise ValueError("operational month must contain at least one week")
    return {
        "month_id": validate_month_id(month_id),
        "operational_coverage_start": weeks[0]["week_start"],
        "operational_coverage_end": weeks[-1]["week_end"],
        "week_count": len(weeks),
    }


def required_calendar_months_for_operational_month(month_id: str) -> list[str]:
    coverage = operational_coverage_for_month(month_id)
    current = date.fromisoformat(str(coverage["operational_coverage_start"]))
    end = date.fromisoformat(str(coverage["operational_coverage_end"]))
    months: list[str] = []
    while current <= end:
        token = f"{current.year:04d}-{current.month:02d}"
        if token not in months:
            months.append(token)
        current += timedelta(days=1)
    return months


def route_week_mapping_for_month(month_id: str) -> list[dict[str, object]]:
    return [
        {
            "week_label": str(week["week_label"]),
            "week_start": str(week["week_start"]),
            "week_end": str(week["week_end"]),
            "assigned_operational_month": str(week["assigned_operational_month"]),
        }
        for week in operational_weeks_for_month(month_id)
    ]
