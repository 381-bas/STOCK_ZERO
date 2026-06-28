from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from pathlib import Path
from datetime import datetime

import pandas as pd


PHASE = "FAST_REFORM_009F_LOADER_STRUCTURE_VALIDATION"
SOURCE_FILE = Path("data/photo-excel-admin_1782440454408.xlsx")
SHEET_NAME = "Fotos"
OUT_DIR = Path("research/FAST_REFORM_009F_LOADER_STRUCTURE_VALIDATION")
OUT_JSON = OUT_DIR / "009F_loader_structure_validation.json"
OUT_DAILY = OUT_DIR / "009F_day_coverage.csv"
OUT_CLASSIFICATION = OUT_DIR / "009F_column_classification.csv"
OUT_CONFLICT_SAMPLE = OUT_DIR / "009F_real_content_conflict_sample.csv"

EXPECTED = {
    "photo_rows": 37908,
    "distinct_event_ids": 5892,
    "fecha_min": "2026-06-20",
    "fecha_max": "2026-06-24",
    "required_dates": [
        "2026-06-20",
        "2026-06-21",
        "2026-06-22",
        "2026-06-23",
        "2026-06-24",
    ],
    "expected_weeks_present": [
        "2026-06-15",
        "2026-06-22",
    ],
    "week_start_contract": {
        "2026-06-20": "2026-06-15",
        "2026-06-21": "2026-06-15",
        "2026-06-22": "2026-06-22",
    },
}

REQUIRED_COLUMNS_CANONICAL = {
    "id": "ID",
    "sp item id": "SP Item ID",
    "holding": "Holding",
    "subcadena": "Subcadena",
    "codigo local": "Codigo Local",
    "marca": "Marca",
    "local": "Local",
    "direccion": "Direccion",
    "reponedor": "Reponedor",
    "fecha": "Fecha",
    "hora": "Hora",
    "tipo de tarea": "Tipo de Tarea",
    "comentarios": "Comentarios",
    "link foto": "Link Foto",
}

PHOTO_COUNT_ALIASES = [
    "n fotos",
    "foto no/total",
    "foto n/total",
    "foto nº/total",
    "foto n°/total",
    "foto nÂº/total",
]

EVENT_STABLE_BASE_KEYS = [
    "id",
    "sp item id",
    "holding",
    "subcadena",
    "codigo local",
    "marca",
    "local",
    "direccion",
    "reponedor",
    "fecha",
]

OPTIONAL_DIAGNOSTIC_KEYS = [
    "hora",
    "tipo de tarea",
    "comentarios",
]


def norm_col(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("Âº", "º").replace("°", "º")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.strip().lower()
    text = text.replace("º", "o")
    text = re.sub(r"\s+", " ", text)
    return text


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def week_start_monday(series: pd.Series) -> pd.Series:
    return series - pd.to_timedelta(series.dt.weekday, unit="D")


def parse_total_from_foto_total(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip()
    extracted = text.str.extract(r"/\s*(\d+)\s*$")[0]
    return pd.to_numeric(extracted, errors="coerce")


def normalized_frame_for_hash(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df[cols].astype("string").fillna("")
    for col in cols:
        out[col] = out[col].str.strip().str.upper()
    return out


def nunique_by_event(df: pd.DataFrame, event_col: str, target_col: str) -> pd.Series:
    return (
        df.groupby(event_col, dropna=False)[target_col]
          .agg(lambda s: s.astype("string").fillna("<NA>").str.strip().nunique())
    )


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    result: dict = {
        "phase": PHASE,
        "verdict": "UNKNOWN",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_file": str(SOURCE_FILE),
        "source_file_exists": SOURCE_FILE.exists(),
        "sheet_name": SHEET_NAME,
        "db_apply": False,
        "file_movement": False,
        "expected": EXPECTED,
        "errors": [],
        "warnings": [],
    }

    if not SOURCE_FILE.exists():
        result["verdict"] = "BLOCKED_SOURCE_FILE_NOT_FOUND"
        result["errors"].append("Source file does not exist")
        OUT_JSON.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 2

    result["source_file_sha256"] = sha256_file(SOURCE_FILE)
    result["source_file_size_bytes"] = SOURCE_FILE.stat().st_size

    xls = pd.ExcelFile(SOURCE_FILE, engine="openpyxl")
    result["sheet_names"] = xls.sheet_names

    if SHEET_NAME not in xls.sheet_names:
        result["verdict"] = "BLOCKED_SHEET_NOT_FOUND"
        result["errors"].append(f"Missing sheet: {SHEET_NAME}")
        OUT_JSON.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 2

    df = pd.read_excel(SOURCE_FILE, sheet_name=SHEET_NAME, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    norm_to_actual = {norm_col(c): c for c in df.columns}

    missing_required = [
        pretty for key, pretty in REQUIRED_COLUMNS_CANONICAL.items()
        if key not in norm_to_actual
    ]

    photo_count_col = None
    photo_count_contract = None
    for alias in PHOTO_COUNT_ALIASES:
        key = norm_col(alias)
        if key in norm_to_actual:
            photo_count_col = norm_to_actual[key]
            photo_count_contract = "N_FOTOS_DIRECT" if key == "n fotos" else "FOTO_NUM_TOTAL_DERIVED"
            break

    if not photo_count_col:
        missing_required.append("N Fotos OR Foto Nº/Total")

    col_id = norm_to_actual.get("id")
    col_fecha = norm_to_actual.get("fecha")
    col_link = norm_to_actual.get("link foto")
    col_fecha_subida = norm_to_actual.get("fecha de subida")

    result["columns"] = list(df.columns)
    result["missing_required_columns"] = missing_required
    result["photo_count_column"] = photo_count_col
    result["photo_count_contract"] = photo_count_contract
    result["confirmed_photo_level_columns"] = [
        c for c in [photo_count_col, col_link, col_fecha_subida] if c
    ]

    if missing_required:
        result["errors"].append("Required columns missing")
    if not col_id:
        result["errors"].append("ID column not resolved")
    if not col_fecha:
        result["errors"].append("Fecha column not resolved")
    if not col_fecha_subida:
        result["warnings"].append("Fecha de subida column not found; cannot explicitly exclude upload timestamp")

    if result["errors"]:
        result["verdict"] = "BLOCKED_STRUCTURE"
        OUT_JSON.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 2

    df["_fecha_dt"] = pd.to_datetime(df[col_fecha], errors="coerce").dt.normalize()
    df["_event_id"] = df[col_id].astype("string").str.strip()
    df["_week_start"] = week_start_monday(df["_fecha_dt"])

    if photo_count_contract == "N_FOTOS_DIRECT":
        df["_n_fotos_calculado"] = pd.to_numeric(df[photo_count_col], errors="coerce")
    else:
        df["_n_fotos_calculado"] = parse_total_from_foto_total(df[photo_count_col])

    photo_rows = int(len(df))
    distinct_event_ids = int(df["_event_id"].dropna().nunique())
    null_event_id_rows = int(df["_event_id"].isna().sum() + (df["_event_id"] == "").sum())
    null_fecha_rows = int(df["_fecha_dt"].isna().sum())
    null_n_fotos_calculado_rows = int(df["_n_fotos_calculado"].isna().sum())

    fecha_min = None if df["_fecha_dt"].dropna().empty else df["_fecha_dt"].min().date().isoformat()
    fecha_max = None if df["_fecha_dt"].dropna().empty else df["_fecha_dt"].max().date().isoformat()

    event_date_counts = (
        df.dropna(subset=["_event_id", "_fecha_dt"])
          .groupby("_event_id")["_fecha_dt"]
          .nunique()
    )
    event_week_counts = (
        df.dropna(subset=["_event_id", "_week_start"])
          .groupby("_event_id")["_week_start"]
          .nunique()
    )

    event_ids_multi_fecha = int((event_date_counts > 1).sum())
    event_ids_multi_week = int((event_week_counts > 1).sum())

    per_event_photo_contract = (
        df.dropna(subset=["_event_id"])
          .groupby("_event_id")
          .agg(
              row_count=(col_id, "size"),
              n_fotos_calculado=("_n_fotos_calculado", "max"),
          )
          .reset_index()
    )
    per_event_photo_contract["row_count_equals_n_fotos"] = (
        per_event_photo_contract["row_count"] == per_event_photo_contract["n_fotos_calculado"]
    )
    row_count_n_fotos_mismatch_events = int(
        (~per_event_photo_contract["row_count_equals_n_fotos"]).sum()
    )

    column_classification = []
    dynamic_photo_level_cols = []
    dynamic_event_stable_cols = []

    for key in OPTIONAL_DIAGNOSTIC_KEYS:
        col = norm_to_actual.get(key)
        if not col:
            column_classification.append({
                "column": key,
                "actual_column": None,
                "classification": "MISSING",
                "variable_events": None,
                "max_distinct_values_within_event": None,
            })
            result["warnings"].append(f"Optional diagnostic column missing: {key}")
            continue

        counts = nunique_by_event(df, "_event_id", col)
        variable_events = int((counts > 1).sum())
        max_distinct = int(counts.max()) if len(counts) else 0

        if variable_events > 0:
            classification = "PHOTO_LEVEL"
            dynamic_photo_level_cols.append(col)
            result["warnings"].append(f"{col} varies within {variable_events} event IDs; excluded from stable_hash")
        else:
            classification = "EVENT_STABLE"
            dynamic_event_stable_cols.append(col)

        column_classification.append({
            "column": key,
            "actual_column": col,
            "classification": classification,
            "variable_events": variable_events,
            "max_distinct_values_within_event": max_distinct,
        })

    pd.DataFrame(column_classification).to_csv(
        OUT_CLASSIFICATION,
        index=False,
        encoding="utf-8-sig",
    )

    confirmed_photo_level_cols = [c for c in [photo_count_col, col_link, col_fecha_subida] if c]
    photo_level_cols = set(confirmed_photo_level_cols + dynamic_photo_level_cols)

    base_event_stable_cols = [
        norm_to_actual[k] for k in EVENT_STABLE_BASE_KEYS
        if k in norm_to_actual
    ]

    stable_cols = []
    for col in base_event_stable_cols + dynamic_event_stable_cols:
        if col not in photo_level_cols and col not in stable_cols:
            stable_cols.append(col)

    stable_hash_source = normalized_frame_for_hash(df, stable_cols)
    df["_stable_hash"] = (
        stable_hash_source
        .agg("||".join, axis=1)
        .map(lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest())
    )

    content_hash_counts = (
        df.dropna(subset=["_event_id"])
          .groupby("_event_id")["_stable_hash"]
          .nunique()
    )
    real_content_conflict_event_ids = int((content_hash_counts > 1).sum())

    if real_content_conflict_event_ids > 0:
        conflict_ids = content_hash_counts[content_hash_counts > 1].index.astype(str).tolist()[:25]
        sample_cols = [col_id] + stable_cols + list(photo_level_cols)
        sample_cols = [c for c in sample_cols if c in df.columns]
        df[df["_event_id"].astype(str).isin(conflict_ids)][sample_cols].to_csv(
            OUT_CONFLICT_SAMPLE,
            index=False,
            encoding="utf-8-sig",
        )
    else:
        pd.DataFrame([]).to_csv(OUT_CONFLICT_SAMPLE, index=False, encoding="utf-8-sig")

    exact_duplicate_photo_rows = int(
        df.drop(columns=["_fecha_dt", "_event_id", "_week_start", "_stable_hash", "_n_fotos_calculado"])
          .duplicated(keep=False)
          .sum()
    )

    day_coverage = (
        df.dropna(subset=["_fecha_dt"])
          .groupby("_fecha_dt")
          .agg(
              photo_rows=(col_id, "size"),
              distinct_event_ids=("_event_id", pd.Series.nunique),
          )
          .reset_index()
    )
    day_coverage["coverage_date"] = day_coverage["_fecha_dt"].dt.date.astype(str)
    day_coverage = day_coverage[["coverage_date", "photo_rows", "distinct_event_ids"]]
    day_coverage.to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")

    dates_present = day_coverage["coverage_date"].tolist()
    weeks_present = sorted(
        df["_week_start"].dropna().dt.date.astype(str).unique().tolist()
    )

    week_start_checks = {}
    for date_text, expected_week in EXPECTED["week_start_contract"].items():
        dt = pd.Series(pd.to_datetime([date_text]))
        actual_week = week_start_monday(dt).dt.date.astype(str).iloc[0]
        week_start_checks[date_text] = {
            "expected_week_start": expected_week,
            "actual_week_start": actual_week,
            "pass": actual_week == expected_week,
        }

    week_start_contract_pass = all(v["pass"] for v in week_start_checks.values())

    flags = {
        "photo_rows_match": photo_rows == EXPECTED["photo_rows"],
        "distinct_event_ids_match": distinct_event_ids == EXPECTED["distinct_event_ids"],
        "fecha_min_match": fecha_min == EXPECTED["fecha_min"],
        "fecha_max_match": fecha_max == EXPECTED["fecha_max"],
        "required_dates_match": dates_present == EXPECTED["required_dates"],
        "expected_weeks_present_match": weeks_present == EXPECTED["expected_weeks_present"],
        "week_start_contract_pass": week_start_contract_pass,
        "no_null_event_id_rows": null_event_id_rows == 0,
        "no_null_fecha_rows": null_fecha_rows == 0,
        "no_null_n_fotos_calculado_rows": null_n_fotos_calculado_rows == 0,
        "no_event_ids_multi_fecha": event_ids_multi_fecha == 0,
        "no_event_ids_multi_week": event_ids_multi_week == 0,
        "no_row_count_n_fotos_mismatch_events": row_count_n_fotos_mismatch_events == 0,
        "no_real_content_conflict_event_ids": real_content_conflict_event_ids == 0,
        "optional_columns_classified": all(
            x["classification"] in {"EVENT_STABLE", "PHOTO_LEVEL"}
            for x in column_classification
        ),
    }

    result.update({
        "grain_contract": {
            "input_grain": "photo_row",
            "normalized_grain": "event_row",
            "compliance_grain": "day_presence",
            "forbidden_assumption": "one_excel_row_equals_one_visit",
        },
        "column_contract": {
            "event_stable_columns_used_for_hash": stable_cols,
            "photo_level_columns_excluded_from_hash": sorted([c for c in photo_level_cols if c]),
            "column_classification": column_classification,
        },
        "metrics": {
            "photo_rows": photo_rows,
            "distinct_event_ids": distinct_event_ids,
            "fecha_min": fecha_min,
            "fecha_max": fecha_max,
            "dates_present": dates_present,
            "weeks_present": weeks_present,
            "null_event_id_rows": null_event_id_rows,
            "null_fecha_rows": null_fecha_rows,
            "null_n_fotos_calculado_rows": null_n_fotos_calculado_rows,
            "event_ids_multi_fecha": event_ids_multi_fecha,
            "event_ids_multi_week": event_ids_multi_week,
            "exact_duplicate_photo_rows": exact_duplicate_photo_rows,
            "real_content_conflict_event_ids": real_content_conflict_event_ids,
            "row_count_n_fotos_mismatch_events": row_count_n_fotos_mismatch_events,
        },
        "week_start_checks": week_start_checks,
        "daily_coverage": day_coverage.to_dict(orient="records"),
        "flags": flags,
        "outputs": {
            "json": str(OUT_JSON),
            "daily_coverage_csv": str(OUT_DAILY),
            "column_classification_csv": str(OUT_CLASSIFICATION),
            "real_content_conflict_sample_csv": str(OUT_CONFLICT_SAMPLE),
        },
    })

    hard_flags = [
        flags["photo_rows_match"],
        flags["distinct_event_ids_match"],
        flags["fecha_min_match"],
        flags["fecha_max_match"],
        flags["required_dates_match"],
        flags["expected_weeks_present_match"],
        flags["week_start_contract_pass"],
        flags["no_null_event_id_rows"],
        flags["no_null_fecha_rows"],
        flags["no_null_n_fotos_calculado_rows"],
        flags["no_event_ids_multi_fecha"],
        flags["no_event_ids_multi_week"],
        flags["no_row_count_n_fotos_mismatch_events"],
        flags["no_real_content_conflict_event_ids"],
        flags["optional_columns_classified"],
    ]

    result["verdict"] = "PASS_LOADER_CONTRACT" if all(hard_flags) else "WARN_REVIEW_REQUIRED"

    OUT_JSON.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["verdict"] == "PASS_LOADER_CONTRACT" else 1


if __name__ == "__main__":
    raise SystemExit(main())
