# -*- coding: utf-8 -*-
"""
scanner.py — Auditor del proyecto (sin ejecutar tu app).

Genera 1 SOLO .txt con:
- Entorno Python (sys.executable, sys.version)
- Chequeo paquetes (streamlit, pandas, openpyxl)
- Imports por archivo (resumen)
- ÍNDICE por archivo + sub-índice de defs/classes (con línea original del .py)
- Dump completo de cada .py con números de línea y marcadores:
    <<<BEGIN FILE: path>>>
    <<<END FILE: path>>>

Uso:
    python skills/scanner.py
    python skills/scanner.py --root "C:\\Users\\basti\\Desktop\\STOCK_ZERO\\app" --out "scanner_audit.txt"
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple


IGNORE_DIRS = {
    ".git", ".svn", ".hg",
    "__pycache__", ".pytest_cache", ".mypy_cache",
    ".venv", "venv", "env",
    ".streamlit",
    "node_modules",
}

TARGET_PKGS = ["streamlit", "pandas", "openpyxl"]


@dataclass
class PyFileInfo:
    rel: str
    abs_path: Path
    src: str
    size: int
    mtime: str
    sha256: str
    imports: List[str]
    has_streamlit: bool
    symbols: List[Tuple[int, str, str]]  # (lineno, kind, name) kind in {"def","class","async def"}


def pkg_status(pkg: str) -> str:
    spec = importlib.util.find_spec(pkg)
    return "OK" if spec is not None else "MISSING"


def safe_read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return p.read_text(encoding="cp1252", errors="replace")


def collect_py_files(root: Path) -> List[Path]:
    files: List[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]
        for fn in filenames:
            if fn.lower().endswith(".py"):
                files.append(Path(dirpath) / fn)
    return sorted(files, key=lambda x: str(x).lower())


def sha256_text(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode("utf-8", errors="replace"))
    return h.hexdigest()


def parse_imports(tree: ast.AST) -> List[str]:
    imports: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.append(n.name)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
    # unique preserving order
    seen = set()
    uniq = []
    for i in imports:
        if i and i not in seen:
            seen.add(i)
            uniq.append(i)
    return uniq


def parse_symbols(tree: ast.AST) -> List[Tuple[int, str, str]]:
    """
    Extrae defs/classes con su lineno del archivo original.
    Incluye funciones anidadas también (sirve para auditoría).
    """
    syms: List[Tuple[int, str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            syms.append((getattr(node, "lineno", -1), "def", node.name))
        elif isinstance(node, ast.AsyncFunctionDef):
            syms.append((getattr(node, "lineno", -1), "async def", node.name))
        elif isinstance(node, ast.ClassDef):
            syms.append((getattr(node, "lineno", -1), "class", node.name))
    syms = [s for s in syms if s[0] and s[0] > 0]
    syms.sort(key=lambda x: x[0])
    return syms


def format_code_with_lineno(src: str, width: int = 5) -> str:
    out_lines = []
    for i, line in enumerate(src.splitlines(), start=1):
        out_lines.append(f"{i:0{width}d} | {line}")
    return "\n".join(out_lines)


def load_file_info(root: Path, p: Path) -> PyFileInfo:
    rel = str(p.relative_to(root))
    src = safe_read_text(p)

    try:
        st = p.stat()
        size = st.st_size
        mtime = datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")
    except Exception:
        size = -1
        mtime = "N/A"

    digest = sha256_text(src)

    tree = ast.parse(src, filename=str(p))
    imps = parse_imports(tree)
    syms = parse_symbols(tree)
    has_streamlit = any(i == "streamlit" or i.startswith("streamlit.") for i in imps)

    return PyFileInfo(
        rel=rel,
        abs_path=p,
        src=src,
        size=size,
        mtime=mtime,
        sha256=digest,
        imports=imps,
        has_streamlit=has_streamlit,
        symbols=syms,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Ruta raíz del proyecto")
    ap.add_argument("--out", default="scanner_audit.txt", help="Archivo de salida (un solo .txt)")
    ap.add_argument("--max-dump-lines", type=int, default=0,
                    help="Máximo líneas por archivo en el dump (0 = sin límite).")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    out_path = Path(args.out).resolve()

    py_files = collect_py_files(root)

    file_infos: List[PyFileInfo] = []
    syntax_errors: List[Tuple[str, str]] = []

    for p in py_files:
        rel = str(p.relative_to(root))
        try:
            file_infos.append(load_file_info(root, p))
        except SyntaxError as e:
            syntax_errors.append((rel, f"{e.msg} (line {e.lineno}:{e.offset})"))
        except Exception as e:
            syntax_errors.append((rel, f"ERROR parseando: {e}"))

    # ---------------- Report build ----------------
    lines: List[str] = []
    lines.append("=" * 96)
    lines.append("SCANNER AUDIT REPORT (FULL DUMP)")
    lines.append(f"Fecha: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"Root : {root}")
    lines.append("=" * 96)
    lines.append("")

    # Python env
    lines.append("[PYTHON]")
    lines.append(f"sys.executable: {sys.executable}")
    lines.append(f"sys.version   : {sys.version.replace(os.linesep, ' ')}")
    lines.append("")

    # Packages
    lines.append("[PACKAGES CHECK]")
    for pkg in TARGET_PKGS:
        lines.append(f"{pkg:10s}: {pkg_status(pkg)}")
    lines.append("")

    # Syntax
    lines.append("[SYNTAX]")
    if not syntax_errors:
        lines.append("OK: Sin errores de sintaxis.")
    else:
        lines.append(f"Errores: {len(syntax_errors)}")
        for rel, err in syntax_errors:
            lines.append(f"- {rel}: {err}")
    lines.append("")

    # Imports summary
    lines.append("[IMPORTS BY FILE] (resumen)")
    for fi in sorted(file_infos, key=lambda x: x.rel.lower()):
        imps = ", ".join(fi.imports) if fi.imports else "(sin imports)"
        lines.append(f"- {fi.rel}: {imps}")
    lines.append("")

    # Streamlit list
    st_files = [fi.rel for fi in file_infos if fi.has_streamlit]
    lines.append("[STREAMLIT IMPORTERS]")
    lines.append(f"Archivos que importan streamlit: {len(st_files)}")
    for f in st_files:
        lines.append(f"- {f}")
    lines.append("")

    # Index (toc)
    lines.append("=" * 96)
    lines.append("[INDEX / TOC]")
    lines.append("Tip: usa Ctrl+F por el marcador exacto:  <<<BEGIN FILE: <ruta>>>")
    lines.append("=" * 96)

    for idx, fi in enumerate(sorted(file_infos, key=lambda x: x.rel.lower()), start=1):
        tag = "STREAMLIT" if fi.has_streamlit else "PY"
        lines.append(f"{idx:02d}) [{tag}] {fi.rel}  | size={fi.size} | mtime={fi.mtime} | sha256={fi.sha256[:12]}...")
        if fi.symbols:
            for (lineno, kind, name) in fi.symbols:
                lines.append(f"    - L{lineno:04d}  {kind}  {name}")
        else:
            lines.append("    - (sin defs/classes detectables)")
    lines.append("")

    # Full dump
    lines.append("=" * 96)
    lines.append("[FULL CODE DUMP]")
    lines.append("Formato: '00001 | <línea>'")
    if args.max_dump_lines and args.max_dump_lines > 0:
        lines.append(f"Max líneas por archivo: {args.max_dump_lines}")
    lines.append("=" * 96)
    lines.append("")

    for fi in sorted(file_infos, key=lambda x: x.rel.lower()):
        header = [
            "-" * 96,
            f"<<<BEGIN FILE: {fi.rel}>>>",
            f"SIZE : {fi.size} bytes",
            f"MTIME: {fi.mtime}",
            f"SHA256(text): {fi.sha256}",
            "-" * 96,
        ]
        lines.extend(header)

        dump = format_code_with_lineno(fi.src, width=5)
        if args.max_dump_lines and args.max_dump_lines > 0:
            dump_lines = dump.splitlines()
            if len(dump_lines) > args.max_dump_lines:
                dump = "\n".join(dump_lines[: args.max_dump_lines]) + "\n... (TRUNCADO) ..."

        lines.append(dump)
        lines.append(f"\n<<<END FILE: {fi.rel}>>>")
        lines.append("")  # separación

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"OK — Reporte FULL generado en: {out_path}")


if __name__ == "__main__":
    main()