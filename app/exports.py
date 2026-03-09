# app/exports.py
import io
import pandas as pd
from openpyxl.styles import PatternFill, Font
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, LongTable, TableStyle, Paragraph, Spacer
from reportlab.lib.units import cm

# app/exports.py

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

def build_export_df(df_ux: pd.DataFrame) -> pd.DataFrame:
    df = df_ux.copy()

    # Asegura columnas base (fecha incluida)
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

    # Fecha stock = fecha comercial del dato
    df["Fecha stock"] = pd.to_datetime(df["fecha"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["Fecha stock"] = df["Fecha stock"].fillna("")

    # Tipos
    df["Sku"] = df["Sku"].astype(str)
    df["Stock"] = pd.to_numeric(df["Stock"], errors="coerce").fillna(0).astype(int)
    df["Venta(+7)"] = pd.to_numeric(df["Venta(+7)"], errors="coerce").fillna(0).astype(int)

    # Flag VENTA 0: SOLO "SI"
    df["VENTA 0"] = df["Venta(+7)"].apply(lambda x: "SI" if int(x or 0) == 0 else "")

    # Flags: SOLO "SI"
    for c in ["NEGATIVO", "RIESGO DE QUIEBRE"]:
        df[c] = df[c].astype(str).str.strip().str.upper().apply(lambda v: "SI" if v == "SI" else "")

    # OTROS: limpia ruido
    df["OTROS"] = df["OTROS"].astype(str).str.strip()
    df.loc[df["OTROS"].str.upper().isin(["NO", "N/A", "NA", "-"]), "OTROS"] = ""

    # DEVUELVE SOLO COLS DE EXPORT (SIN Venta(+7))
    return df[EXPORT_COLS]

def _sorted_for_export(df_in: pd.DataFrame) -> pd.DataFrame:
    """Orden consistente: MARCA A→Z, SKU numérico primero, luego SKU texto, luego producto."""
    if df_in is None or df_in.empty:
        return df_in

    df = df_in.copy()

    # Asegura columnas base
    for c in EXPORT_COLS:
        if c not in df.columns:
            df[c] = ""

    df["MARCA"] = df["MARCA"].astype(str)
    df["Sku"] = df["Sku"].astype(str)
    df["Descripción del Producto"] = df["Descripción del Producto"].astype(str)

    sku_num = pd.to_numeric(df["Sku"], errors="coerce")
    df["_sku_is_text"] = sku_num.isna().astype(int)       # 0 numérico, 1 texto -> numérico primero
    df["_sku_num"] = sku_num.fillna(0)

    df = df.sort_values(
        by=["MARCA", "_sku_is_text", "_sku_num", "Sku", "Descripción del Producto"],
        ascending=[True, True, True, True, True],
        kind="mergesort",  # estable
    ).drop(columns=["_sku_is_text", "_sku_num"])

    return df[EXPORT_COLS]


def export_excel_one_sheet(cod_rt: str, df_export: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    sheet_name = str(cod_rt)[:31]

    df_export = _sorted_for_export(df_export)

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name=sheet_name)

        ws = writer.sheets[sheet_name]
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        # Ajuste simple de anchos (cap)
        for col_cells in ws.columns:
            col_letter = col_cells[0].column_letter
            values = []
            for cell in col_cells[:200]:
                if cell.value is not None:
                    values.append(len(str(cell.value)))
            max_len = max(values) if values else 10
            ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 45)

    return out.getvalue()


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
        2.4 * cm,  # Fecha stock
        2.6 * cm,  # MARCA
        2.6 * cm,  # Sku
        9.8 * cm,  # Descripción del Producto
        1.8 * cm,  # Stock
        2.0 * cm,  # VENTA 0
        2.0 * cm,  # NEGATIVO
        3.0 * cm,  # RIESGO DE QUIEBRE
        2.0 * cm,  # OTROS
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

        # Alineación correcta por índices (0..7)
        ("ALIGN", (4, 1), (4, -1), "RIGHT"),    # Stock
        ("ALIGN", (5, 1), (7, -1), "CENTER"),   # VENTA 0 / NEGATIVO / RIESGO
        ("ALIGN", (8, 1), (8, -1), "LEFT"),     # OTROS

        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))

    story.append(table)
    doc.build(story)
    return out.getvalue()