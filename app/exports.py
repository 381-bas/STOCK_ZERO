# app/exports.py
import io
import pandas as pd
from openpyxl.styles import PatternFill, Font
from reportlab.lib.pagesizes import A4, landscape
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

    base_cols = [c for c in df.columns]
    for c in base_cols:
        if c not in df.columns:
            df[c] = ""

    if "MARCA" not in df.columns:
        df["MARCA"] = ""
    if "Sku" not in df.columns:
        df["Sku"] = ""
    if "Descripción del Producto" not in df.columns:
        df["Descripción del Producto"] = ""

    df["MARCA"] = df["MARCA"].astype(str)
    df["Sku"] = df["Sku"].astype(str)
    df["Descripción del Producto"] = df["Descripción del Producto"].astype(str)

    sku_num = pd.to_numeric(df["Sku"], errors="coerce")
    df["_sku_is_text"] = sku_num.isna().astype(int)
    df["_sku_num"] = sku_num.fillna(0)

    sort_cols = ["MARCA", "_sku_is_text", "_sku_num", "Sku", "Descripción del Producto"]
    if "FOCO PRINCIPAL" in df.columns:
        sort_cols = ["MARCA", "FOCO PRINCIPAL", "_sku_is_text", "_sku_num", "Sku", "Descripción del Producto"]

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


def build_focus_export_df(df_ux: pd.DataFrame, foco: str = "Todo") -> pd.DataFrame:
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
    df["FOCO PRINCIPAL"] = df.apply(_focus_principal, axis=1)
    df["ACCIÓN SUGERIDA"] = df["FOCO PRINCIPAL"].apply(_accion_sugerida)

    foco = (foco or "Todo").strip()

    if foco == "Venta 0":
        df = df[df["VENTA 0"] == "SI"].copy()
    elif foco == "Negativo":
        df = df[df["NEGATIVO"] == "SI"].copy()
    elif foco == "Quiebres":
        df = df[df["RIESGO DE QUIEBRE"] == "SI"].copy()
    elif foco == "Otros":
        df = df[df["OTROS"] != ""].copy()
    else:
        df = df[df["FOCO PRINCIPAL"] != ""].copy()

    if df.empty:
        return pd.DataFrame(columns=FOCUS_EXPORT_COLS)

    df = _sorted_for_export(df)
    return df[FOCUS_EXPORT_COLS]


def export_excel_generic(sheet_name: str, df_export: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    safe_sheet = str(sheet_name or "DATA")[:31]
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