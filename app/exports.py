# app/exports.py
import io
import pandas as pd
from openpyxl.styles import PatternFill, Font
from reportlab.lib.pagesizes import A4, A3, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, LongTable, TableStyle, Paragraph, Spacer
from reportlab.lib.units import cm


EXPORT_COLS = [
    "Fecha stock",
    "MARCA",
    "Sku",
    "Descripción del Producto",
    "Stock",
    "VENTA 0",
    "NEGATIVO",
    "RIESGO DE QUIEBRE",
    "OTROS",
]

FOCUS_EXPORT_COLS = [
    "Fecha stock",
    "COD_RT",
    "LOCAL",
    "CLIENTE",
    "RUTERO",
    "REPONEDOR",
    "MARCA",
    "Sku",
    "Descripción del Producto",
    "Stock",
    "FOCO PRINCIPAL",
    "ACCIÓN SUGERIDA",
    "VENTA 0",
    "NEGATIVO",
    "RIESGO DE QUIEBRE",
    "OTROS",
]

INVENTORY_CLIENTE_EXPORT_COLS = [
    "Fecha stock",
    "COD_RT",
    "LOCAL",
    "CLIENTE",
    "GESTOR",
    "SUPERVISOR",
    "RUTERO",
    "REPONEDOR",
    "MODALIDAD",
    "MARCA",
    "Sku",
    "Descripción del Producto",
    "Stock",
    "Venta(+7)",
    "VENTA 0",
    "FOCO PRINCIPAL",
    "ACCIÓN SUGERIDA",
    "NEGATIVO",
    "RIESGO DE QUIEBRE",
    "OTROS",
]


def _collapse_pipe_values(values) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw or "").strip()
        if not text or text.lower() in {"nan", "none"}:
            continue
        for item in text.split("|"):
            token = item.strip()
            if not token:
                continue
            key = token.upper()
            if key in seen:
                continue
            seen.add(key)
            parts.append(token)
    return " | ".join(parts)


def _coalesce_duplicate_rr_columns(df_in: pd.DataFrame) -> pd.DataFrame:
    if df_in is None:
        return df_in

    df = df_in.copy()
    for base in ["GESTOR", "SUPERVISOR", "RUTERO", "REPONEDOR", "MODALIDAD"]:
        variants = [c for c in [base, f"{base}_x", f"{base}_y"] if c in df.columns]
        if len(variants) <= 1:
            continue

        cols = list(df.columns)
        insert_at = min(cols.index(c) for c in variants)
        merged = df[variants].apply(lambda row: _collapse_pipe_values(row.tolist()), axis=1)

        drop_cols = [c for c in variants if c != base]
        if base in df.columns:
            df[base] = merged
        else:
            df.insert(insert_at, base, merged)
        if drop_cols:
            df = df.drop(columns=drop_cols, errors="ignore")

    return df


def _clean_yes(v) -> str:
    return "SI" if str(v or "").strip().upper() == "SI" else ""


def _clean_otros(v) -> str:
    out = str(v or "").strip()
    return "" if out.upper() in {"", "NO", "N/A", "NA", "-"} else out


def _venta_0_flag(v) -> str:
    try:
        return "SI" if int(pd.to_numeric(v, errors="coerce") or 0) == 0 else ""
    except Exception:
        return ""


def build_export_df(df_ux: pd.DataFrame) -> pd.DataFrame:
    df = df_ux.copy()

    needed = [
        "fecha",
        "MARCA",
        "Sku",
        "Descripción del Producto",
        "Stock",
        "Venta(+7)",
        "NEGATIVO",
        "RIESGO DE QUIEBRE",
        "OTROS",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = ""

    df["Fecha stock"] = pd.to_datetime(df["fecha"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    df["Sku"] = df["Sku"].astype(str)
    df["Stock"] = pd.to_numeric(df["Stock"], errors="coerce").fillna(0).astype(int)
    df["Venta(+7)"] = pd.to_numeric(df["Venta(+7)"], errors="coerce").fillna(0).astype(int)

    df["VENTA 0"] = df["Venta(+7)"].apply(_venta_0_flag)
    df["NEGATIVO"] = df["NEGATIVO"].apply(_clean_yes)
    df["RIESGO DE QUIEBRE"] = df["RIESGO DE QUIEBRE"].apply(_clean_yes)
    df["OTROS"] = df["OTROS"].apply(_clean_otros)

    return df[EXPORT_COLS]


def _sorted_for_export(df_in: pd.DataFrame) -> pd.DataFrame:
    if df_in is None or df_in.empty:
        return df_in

    df = df_in.copy()

    if "MARCA" not in df.columns:
        df["MARCA"] = ""
    if "Sku" not in df.columns:
        df["Sku"] = ""
    if "Descripción del Producto" not in df.columns:
        df["Descripción del Producto"] = ""

    for c in ["MARCA", "Sku", "Descripción del Producto", "CLIENTE", "COD_RT", "LOCAL", "RUTERO", "REPONEDOR", "FOCO PRINCIPAL"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    sku_num = pd.to_numeric(df["Sku"], errors="coerce")
    df["_sku_is_text"] = sku_num.isna().astype(int)
    df["_sku_num"] = sku_num.fillna(0)

    if "FOCO PRINCIPAL" in df.columns and ({"CLIENTE", "COD_RT", "LOCAL"} & set(df.columns)):
        if "CLIENTE" not in df.columns:
            df["CLIENTE"] = ""
        if "COD_RT" not in df.columns:
            df["COD_RT"] = ""
        if "LOCAL" not in df.columns:
            df["LOCAL"] = ""
        if "RUTERO" not in df.columns:
            df["RUTERO"] = ""
        if "REPONEDOR" not in df.columns:
            df["REPONEDOR"] = ""
        sort_cols = [
            "CLIENTE", "COD_RT", "LOCAL", "RUTERO", "REPONEDOR",
            "MARCA", "FOCO PRINCIPAL", "_sku_is_text", "_sku_num", "Sku", "Descripción del Producto"
        ]
    elif "FOCO PRINCIPAL" in df.columns:
        sort_cols = ["MARCA", "FOCO PRINCIPAL", "_sku_is_text", "_sku_num", "Sku", "Descripción del Producto"]
    else:
        sort_cols = ["MARCA", "_sku_is_text", "_sku_num", "Sku", "Descripción del Producto"]

    df = df.sort_values(
        by=sort_cols,
        ascending=[True] * len(sort_cols),
        kind="mergesort",
    ).drop(columns=["_sku_is_text", "_sku_num"])

    return df


def _focus_principal(row) -> str:
    neg = _clean_yes(row.get("NEGATIVO", ""))
    quiebre = _clean_yes(row.get("RIESGO DE QUIEBRE", ""))
    venta0 = _venta_0_flag(row.get("Venta(+7)", 0))
    otros = _clean_otros(row.get("OTROS", ""))

    # prioridad semántica operativa
    if neg == "SI":
        return "NEGATIVO"
    if quiebre == "SI":
        return "QUIEBRE"
    if venta0 == "SI":
        return "VENTA 0"
    if otros:
        return "OTROS"
    return ""


def _accion_sugerida(foco_principal: str) -> str:
    if foco_principal == "NEGATIVO":
        return "Ajustar inventario y validar sala"
    if foco_principal == "QUIEBRE":
        return "Solicitar empuje o reposición"
    if foco_principal == "VENTA 0":
        return "Revisar exhibición, precio y rotación"
    if foco_principal == "OTROS":
        return "Revisar observación cliente"
    return ""


def _normalize_focos_export(foco) -> list[str]:
    valid = ["Venta 0", "Negativo", "Quiebres", "Otros"]

    if foco is None:
        raw = []
    elif isinstance(foco, str):
        raw = [x.strip() for x in foco.replace("|", ",").split(",") if x.strip()]
    else:
        raw = [str(x).strip() for x in foco if str(x).strip()]

    out = []
    for x in raw:
        if x in valid and x not in out:
            out.append(x)
    return out


def build_inventory_cliente_export_df(df_ux: pd.DataFrame) -> pd.DataFrame:
    df = df_ux.copy()

    needed = [
        "fecha",
        "COD_RT",
        "LOCAL",
        "CLIENTE",
        "GESTOR",
        "SUPERVISOR",
        "RUTERO",
        "REPONEDOR",
        "MODALIDAD",
        "MARCA",
        "Sku",
        "Descripción del Producto",
        "Stock",
        "Venta(+7)",
        "NEGATIVO",
        "RIESGO DE QUIEBRE",
        "OTROS",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = ""

    df["Fecha stock"] = pd.to_datetime(df["fecha"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    for c in [
        "COD_RT",
        "LOCAL",
        "CLIENTE",
        "GESTOR",
        "SUPERVISOR",
        "RUTERO",
        "REPONEDOR",
        "MODALIDAD",
        "MARCA",
    ]:
        df[c] = df[c].astype(str).replace({"nan": "", "None": ""}).fillna("")
    df["Sku"] = df["Sku"].astype(str)
    df["Stock"] = pd.to_numeric(df["Stock"], errors="coerce").fillna(0).astype(int)
    df["Venta(+7)"] = pd.to_numeric(df["Venta(+7)"], errors="coerce").fillna(0).astype(int)

    df["VENTA 0"] = df["Venta(+7)"].apply(_venta_0_flag)
    df["NEGATIVO"] = df["NEGATIVO"].apply(_clean_yes)
    df["RIESGO DE QUIEBRE"] = df["RIESGO DE QUIEBRE"].apply(_clean_yes)
    df["OTROS"] = df["OTROS"].apply(_clean_otros)
    df["FOCO PRINCIPAL"] = df.apply(_focus_principal, axis=1)
    df["ACCIÓN SUGERIDA"] = df["FOCO PRINCIPAL"].apply(_accion_sugerida)

    if df.empty:
        return pd.DataFrame(columns=INVENTORY_CLIENTE_EXPORT_COLS)

    df = _sorted_for_export(df)
    return df[INVENTORY_CLIENTE_EXPORT_COLS]


def build_focus_export_df(df_ux: pd.DataFrame, foco: str | list[str] = "Todo") -> pd.DataFrame:
    df = df_ux.copy()

    needed = [
        "fecha",
        "COD_RT",
        "LOCAL",
        "CLIENTE",
        "RUTERO",
        "REPONEDOR",
        "MARCA",
        "Sku",
        "Descripción del Producto",
        "Stock",
        "Venta(+7)",
        "NEGATIVO",
        "RIESGO DE QUIEBRE",
        "OTROS",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = ""

    df["Fecha stock"] = pd.to_datetime(df["fecha"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    for c in ["COD_RT", "LOCAL", "CLIENTE", "RUTERO", "REPONEDOR", "MARCA"]:
        df[c] = df[c].astype(str).replace({"nan": "", "None": ""}).fillna("")
    df["Sku"] = df["Sku"].astype(str)
    df["Stock"] = pd.to_numeric(df["Stock"], errors="coerce").fillna(0).astype(int)
    df["Venta(+7)"] = pd.to_numeric(df["Venta(+7)"], errors="coerce").fillna(0).astype(int)

    df["VENTA 0"] = df["Venta(+7)"].apply(_venta_0_flag)
    df["NEGATIVO"] = df["NEGATIVO"].apply(_clean_yes)
    df["RIESGO DE QUIEBRE"] = df["RIESGO DE QUIEBRE"].apply(_clean_yes)
    df["OTROS"] = df["OTROS"].apply(_clean_otros)
    df["FOCO PRINCIPAL"] = df.apply(_focus_principal, axis=1)
    df["ACCIÓN SUGERIDA"] = df["FOCO PRINCIPAL"].apply(_accion_sugerida)

    focos = _normalize_focos_export(foco)

    if focos:
        mask = pd.Series(False, index=df.index)

        if "Venta 0" in focos:
            mask = mask | (df["VENTA 0"] == "SI")
        if "Negativo" in focos:
            mask = mask | (df["NEGATIVO"] == "SI")
        if "Quiebres" in focos:
            mask = mask | (df["RIESGO DE QUIEBRE"] == "SI")
        if "Otros" in focos:
            mask = mask | (df["OTROS"] != "")

        df = df[mask].copy()
    else:
        df = df[df["FOCO PRINCIPAL"] != ""].copy()

    if df.empty:
        return pd.DataFrame(columns=FOCUS_EXPORT_COLS)

    df = _sorted_for_export(df)
    return df[FOCUS_EXPORT_COLS]


def export_excel_generic(sheet_name: str, df_export: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    safe_sheet = str(sheet_name or "DATA")[:31]
    df_export = _coalesce_duplicate_rr_columns(df_export)
    df_export = _sorted_for_export(df_export)

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name=safe_sheet)
        ws = writer.sheets[safe_sheet]
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        for col_cells in ws.columns:
            col_letter = col_cells[0].column_letter
            values = []
            for cell in col_cells[:200]:
                if cell.value is not None:
                    values.append(len(str(cell.value)))
            max_len = max(values) if values else 10
            ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 45)

    return out.getvalue()


def export_excel_one_sheet(cod_rt: str, df_export: pd.DataFrame) -> bytes:
    return export_excel_generic(str(cod_rt), df_export)


def export_pdf_table(title_lines: list[str], df_export: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    doc = SimpleDocTemplate(
        out,
        pagesize=landscape(A4),
        leftMargin=1.0 * cm,
        rightMargin=1.0 * cm,
        topMargin=1.0 * cm,
        bottomMargin=1.0 * cm,
    )

    styles = getSampleStyleSheet()
    body = ParagraphStyle(
        "BodySmall",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=7,
        leading=8,
    )

    story = []
    for line in title_lines:
        story.append(Paragraph(line, styles["Heading4"]))
    story.append(Spacer(1, 8))

    df = _sorted_for_export(df_export)
    if "Descripción del Producto" not in df.columns:
        df["Descripción del Producto"] = ""
    df["Descripción del Producto"] = df["Descripción del Producto"].astype(str)

    data = [EXPORT_COLS]
    for _, r in df.iterrows():
        row = []
        for c in EXPORT_COLS:
            v = r.get(c, "")
            if c == "Descripción del Producto":
                row.append(Paragraph(str(v), body))
            else:
                row.append(str(v))
        data.append(row)

    col_widths = [
        2.4 * cm,
        2.6 * cm,
        2.6 * cm,
        9.8 * cm,
        1.8 * cm,
        2.0 * cm,
        2.0 * cm,
        3.0 * cm,
        2.0 * cm,
    ]

    table = LongTable(data, colWidths=col_widths, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E6E6E6")),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 7),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CFCFCF")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7F7F7")]),
        ("ALIGN", (4, 1), (4, -1), "RIGHT"),
        ("ALIGN", (5, 1), (7, -1), "CENTER"),
        ("ALIGN", (8, 1), (8, -1), "LEFT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))

    story.append(table)
    doc.build(story)
    return out.getvalue()



def _pdf_column_widths(columns: list[str]) -> tuple[list[float], object]:
    width_map_cm = {
        "Fecha stock": 2.1,
        "FECHA STOCK": 2.1,
        "MARCA": 2.2,
        "Sku": 2.0,
        "SKU": 2.0,
        "COD_RT": 2.0,
        "LOCAL": 4.2,
        "CLIENTE": 3.2,
        "RESP. TIPO": 2.5,
        "RESPONSABLE": 3.2,
        "RUTERO": 3.0,
        "REPONEDOR": 3.4,
        "CLIENTES": 2.2,
        "LOCALES": 2.0,
        "TOTAL SKUS": 2.3,
        "SKUS EN FOCO": 2.5,
        "Stock": 1.6,
        "STOCK": 1.6,
        "VENTA(+7)": 2.0,
        "VENTA 0": 1.9,
        "NEGATIVO": 2.0,
        "RIESGO DE QUIEBRE": 3.0,
        "QUIEBRES OBS.": 2.8,
        "OTROS": 2.4,
        "Descripción del Producto": 8.4,
        "PRODUCTO": 8.4,
        "FOCO PRINCIPAL": 2.8,
        "ACCIÓN SUGERIDA": 4.8,
    }
    widths_cm = [width_map_cm.get(col, 2.5) for col in columns]
    total_cm = sum(widths_cm)
    pagesize = landscape(A3) if len(columns) >= 10 or total_cm > 27.2 else landscape(A4)
    return [w * cm for w in widths_cm], pagesize


def export_pdf_generic(title_lines: list[str], df_export: pd.DataFrame, columns: list[str]) -> bytes:
    out = io.BytesIO()
    col_widths, pagesize = _pdf_column_widths(columns)
    doc = SimpleDocTemplate(
        out,
        pagesize=pagesize,
        leftMargin=0.8 * cm,
        rightMargin=0.8 * cm,
        topMargin=0.8 * cm,
        bottomMargin=0.8 * cm,
    )

    styles = getSampleStyleSheet()
    body = ParagraphStyle(
        "BodySmallWrap",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=6.3,
        leading=7.2,
    )
    header = ParagraphStyle(
        "HeaderSmallWrap",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=7,
        leading=8,
    )

    wrap_cols = {
        "Descripción del Producto",
        "PRODUCTO",
        "ACCIÓN SUGERIDA",
        "OTROS",
        "LOCAL",
        "CLIENTE",
        "RESPONSABLE",
        "RESP. TIPO",
        "RUTERO",
        "REPONEDOR",
        "FOCO PRINCIPAL",
    }
    numeric_cols = {
        "Stock",
        "STOCK",
        "TOTAL SKUS",
        "SKUS EN FOCO",
        "VENTA(+7)",
        "VENTA 0",
        "CLIENTES",
        "LOCALES",
    }
    center_cols = {
        "VENTA 0",
        "NEGATIVO",
        "RIESGO DE QUIEBRE",
        "QUIEBRES OBS.",
        "FOCO PRINCIPAL",
    }

    story = []
    for line in title_lines:
        story.append(Paragraph(line, styles["Heading4"]))
    story.append(Spacer(1, 8))

    df = _sorted_for_export(df_export)
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    df = df[columns].copy()

    data = [[Paragraph(str(col), header) for col in columns]]
    for _, r in df.iterrows():
        row = []
        for c in columns:
            v = r.get(c, "")
            if c in wrap_cols:
                row.append(Paragraph(str(v), body))
            else:
                row.append(str(v))
        data.append(row)

    table = LongTable(data, colWidths=col_widths, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E6E6E6")),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CFCFCF")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7F7F7")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 2.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
    ]

    for idx, col in enumerate(columns):
        if col in numeric_cols:
            style.append(("ALIGN", (idx, 1), (idx, -1), "RIGHT"))
        elif col in center_cols:
            style.append(("ALIGN", (idx, 1), (idx, -1), "CENTER"))
        else:
            style.append(("ALIGN", (idx, 1), (idx, -1), "LEFT"))

    table.setStyle(TableStyle(style))
    story.append(table)
    doc.build(story)
    return out.getvalue()


def export_pdf_focus_table(title_lines: list[str], df_export: pd.DataFrame) -> bytes:
    return export_pdf_generic(title_lines, df_export, FOCUS_EXPORT_COLS)
