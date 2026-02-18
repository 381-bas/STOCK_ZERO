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
    "MARCA", "Sku", "Descripción del Producto",
    "Stock", "Venta(+7)", "NEGATIVO", "RIESGO DE QUIEBRE", "OTROS"
]

def build_export_df(df_ux: pd.DataFrame) -> pd.DataFrame:
    df = df_ux.copy()

    for c in EXPORT_COLS:
        if c not in df.columns:
            df[c] = ""

    df["Sku"] = df["Sku"].astype(str)

    for c in ["Stock", "Venta(+7)"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    return df[EXPORT_COLS]

def export_excel_one_sheet(cod_rt: str, df_export: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    sheet_name = str(cod_rt)[:31]

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

    df = df_export.copy()
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

    # Ajustado para que quepa dentro de ~27.7cm (landscape A4 con márgenes)
    col_widths = [
        2.6 * cm,  # MARCA
        2.4 * cm,  # Sku
        10.8 * cm, # Descripción
        1.6 * cm,  # Stock
        2.0 * cm,  # Venta(+7)
        2.0 * cm,  # NEGATIVO
        3.1 * cm,  # RIESGO
        2.2 * cm,  # OTROS
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

        ("ALIGN", (3, 1), (4, -1), "RIGHT"),  # Stock, Venta
        ("ALIGN", (5, 1), (7, -1), "CENTER"), # Flags

        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))

    story.append(table)
    doc.build(story)
    return out.getvalue()