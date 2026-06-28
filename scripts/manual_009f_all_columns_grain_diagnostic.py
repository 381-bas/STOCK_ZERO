from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from datetime import datetime

import pandas as pd


SOURCE_FILE = Path("data/photo-excel-admin_1782440454408.xlsx")
SHEET_NAME = "Fotos"
OUT_DIR = Path("research/FAST_REFORM_009F_LOADER_STRUCTURE_VALIDATION")
OUT_JSON = OUT_DIR / "009F_all_columns_grain_diagnostic.json"
OUT_CSV = OUT_DIR / "009F_all_columns_grain_diagnostic.csv"
OUT_SAMPLE = OUT_DIR / "009F_remaining_conflict_sample.csv"


def norm_col(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("Âº", "º").replace("°", "º")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.strip().lower()
    text = text.replace("º", "o")
    text = re.sub(r"\s+", " ", text)
    return text


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(SOURCE_FILE, sheet_name=SHEET_NAME, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    norm_to_actual = {norm_col(c): c for c in df.columns}

    col_id = norm_to_actual["id"]
    df["_event_id"] = df[col_id].astype("string").str.strip()

    rows = []
    for col in df.columns:
        if col == "_event_id":
            continue

        by_event = (
            df.groupby("_event_id", dropna=False)[col]
              .agg(lambda s: s.astype("string").fillna("<NA>").str.strip().nunique())
        )

        variable_events = int((by_event > 1).sum())
        max_distinct = int(by_event.max()) if len(by_event) else 0

        if variable_events == 0:
            suggested = "EVENT_STABLE"
        elif col in ["Foto Nº/Total", "Foto NÂº/Total", "Link Foto", "Fecha de subida", "Hora"]:
            suggested = "PHOTO_LEVEL_CONFIRMED"
        else:
            suggested = "REVIEW_LIKELY_PHOTO_LEVEL_OR_CONFLICT_SOURCE"

        rows.append({
            "column": col,
            "variable_events": variable_events,
            "max_distinct_values_within_event": max_distinct,
            "suggested_classification": suggested
        })

    out = pd.DataFrame(rows).sort_values(
        ["variable_events", "column"],
        ascending=[False, True]
    )
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    top_variable_cols = out[out["variable_events"] > 0]["column"].head(12).tolist()

    sample_ids = set()
    for col in top_variable_cols:
        by_event = (
            df.groupby("_event_id", dropna=False)[col]
              .agg(lambda s: s.astype("string").fillna("<NA>").str.strip().nunique())
        )
        sample_ids.update(by_event[by_event > 1].index.astype(str).tolist()[:5])

    sample_cols = [col_id] + top_variable_cols
    sample_cols = [c for c in sample_cols if c in df.columns]
    sample = df[df["_event_id"].astype(str).isin(sorted(sample_ids)[:10])][sample_cols]
    sample.to_csv(OUT_SAMPLE, index=False, encoding="utf-8-sig")

    result = {
        "phase": "FAST_REFORM_009F_ALL_COLUMNS_GRAIN_DIAGNOSTIC",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_file": str(SOURCE_FILE),
        "db_apply": False,
        "file_movement": False,
        "rows": int(len(df)),
        "distinct_event_ids": int(df["_event_id"].nunique()),
        "top_variable_columns": out.head(20).to_dict(orient="records"),
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "sample_csv": str(OUT_SAMPLE)
        }
    }

    OUT_JSON.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
