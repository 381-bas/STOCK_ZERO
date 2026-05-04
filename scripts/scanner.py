# -*- coding: utf-8 -*-
"""
scanner.py - Auditor del proyecto (sin ejecutar tu app).

Genera 1 SOLO .txt con:
- Entorno Python (sys.executable, sys.version)
- Chequeo paquetes (streamlit, pandas, openpyxl)
- Imports por archivo (resumen)
- INDICE por archivo + sub-indice de defs/classes (con linea original del .py)
- Dump completo de cada .py con numeros de linea y marcadores:
    <<<BEGIN FILE: path>>>
    <<<END FILE: path>>>

Uso:
    python scripts/scanner.py
    python scripts/scanner.py --root "C:\\Users\\basti\\Desktop\\STOCK_ZERO" --out "codigo_app_stockzero.txt"
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


SCANNER_VERSION = "9B14_5_P1B"
IGNORE_DIRS = {
    ".git",
    ".svn",
    ".hg",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".venv",
    "venv",
    "env",
    ".streamlit",
    "node_modules",
}
TARGET_PKGS = ["streamlit", "pandas", "openpyxl"]
ARCHIVE_PREFIXES = ("app/v_i/",)
UTF8_BOM = b"\xef\xbb\xbf"


@dataclass
class PyFileInfo:
    rel: str
    abs_path: Path
    src: str
    size: int
    mtime: str
    encoding_used: str
    has_utf8_bom: bool
    sha256_bytes: str
    sha256_text: str
    read_error: str | None
    syntax_error: str | None
    parse_ok: bool
    imports: list[str] = field(default_factory=list)
    has_streamlit: bool = False
    symbols: list[tuple[int, str, str]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "path": self.rel,
            "size": self.size,
            "mtime": self.mtime,
            "encoding_used": self.encoding_used,
            "has_utf8_bom": self.has_utf8_bom,
            "sha256_bytes": self.sha256_bytes,
            "sha256_text": self.sha256_text,
            "parse_ok": self.parse_ok,
            "read_error": self.read_error,
            "syntax_error": self.syntax_error,
            "warnings": list(self.warnings),
            "imports": list(self.imports),
            "symbols": [
                {"lineno": lineno, "kind": kind, "name": name}
                for lineno, kind, name in self.symbols
            ],
        }


def pkg_status(pkg: str) -> str:
    spec = importlib.util.find_spec(pkg)
    return "OK" if spec is not None else "MISSING"


def normalize_text_for_hash(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    h = hashlib.sha256()
    h.update(normalize_text_for_hash(text).encode("utf-8", errors="replace"))
    return h.hexdigest()


def is_archive_file(rel_path: str) -> bool:
    rel_posix = rel_path.replace("\\", "/")
    return any(rel_posix.startswith(prefix) for prefix in ARCHIVE_PREFIXES)


def collect_py_files(root: Path, scope: str) -> list[Path]:
    files: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]
        for fn in filenames:
            if not fn.lower().endswith(".py"):
                continue
            abs_path = Path(dirpath) / fn
            rel = str(abs_path.relative_to(root)).replace("\\", "/")
            if scope == "active" and is_archive_file(rel):
                continue
            files.append(abs_path)
    return sorted(files, key=lambda path: str(path).lower())


def collect_all_py_files(root: Path) -> list[Path]:
    return collect_py_files(root, scope="all")


def safe_decode_bytes(data: bytes) -> tuple[str, str, str | None]:
    has_bom = data.startswith(UTF8_BOM)
    candidates: list[tuple[str, str]] = []
    if has_bom:
        candidates.extend([("utf-8-sig", "strict"), ("utf-8", "strict")])
    else:
        candidates.extend([("utf-8", "strict"), ("utf-8-sig", "strict")])
    candidates.append(("cp1252", "replace"))

    last_error: str | None = None
    for encoding, errors in candidates:
        try:
            return data.decode(encoding, errors=errors), encoding, None
        except Exception as exc:  # pragma: no cover - defensive
            last_error = f"{type(exc).__name__}: {exc}"
    return "", "binary", last_error or "UNKNOWN_READ_ERROR"


def parse_imports(tree: ast.AST) -> list[str]:
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for name in node.names:
                imports.append(name.name)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
    seen: set[str] = set()
    ordered: list[str] = []
    for item in imports:
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def parse_symbols(tree: ast.AST) -> list[tuple[int, str, str]]:
    symbols: list[tuple[int, str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            symbols.append((getattr(node, "lineno", -1), "def", node.name))
        elif isinstance(node, ast.AsyncFunctionDef):
            symbols.append((getattr(node, "lineno", -1), "async def", node.name))
        elif isinstance(node, ast.ClassDef):
            symbols.append((getattr(node, "lineno", -1), "class", node.name))
    symbols = [item for item in symbols if item[0] and item[0] > 0]
    symbols.sort(key=lambda item: item[0])
    return symbols


def format_code_with_lineno(src: str, width: int = 5) -> str:
    return "\n".join(f"{i:0{width}d} | {line}" for i, line in enumerate(src.splitlines(), start=1))


def format_syntax_error(exc: SyntaxError) -> str:
    return f"{exc.msg} (line {exc.lineno}:{exc.offset})"


def load_file_info(root: Path, path: Path) -> PyFileInfo:
    rel = str(path.relative_to(root))
    warnings: list[str] = []
    src = ""
    size = -1
    mtime = "N/A"
    encoding_used = ""
    has_utf8_bom = False
    sha_bytes = ""
    sha_text = ""
    read_error: str | None = None
    syntax_error: str | None = None
    parse_ok = False
    imports: list[str] = []
    symbols: list[tuple[int, str, str]] = []
    has_streamlit = False

    try:
        stat = path.stat()
        size = stat.st_size
        mtime = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds")
    except Exception:
        pass

    try:
        raw = path.read_bytes()
        has_utf8_bom = raw.startswith(UTF8_BOM)
        if has_utf8_bom:
            warnings.append("bom_warning")
        sha_bytes = sha256_bytes(raw)
        src, encoding_used, decode_error = safe_decode_bytes(raw)
        if decode_error:
            read_error = decode_error
        sha_text = sha256_text(src)
    except Exception as exc:
        read_error = f"{type(exc).__name__}: {exc}"
        warnings.append("read_error")

    if read_error is None:
        try:
            tree = ast.parse(src, filename=str(path))
            parse_ok = True
            imports = parse_imports(tree)
            symbols = parse_symbols(tree)
            has_streamlit = any(item == "streamlit" or item.startswith("streamlit.") for item in imports)
        except SyntaxError as exc:
            syntax_error = format_syntax_error(exc)
            warnings.append("syntax_error")
        except Exception as exc:  # pragma: no cover - defensive
            syntax_error = f"{type(exc).__name__}: {exc}"
            warnings.append("syntax_error")

    return PyFileInfo(
        rel=rel,
        abs_path=path,
        src=src,
        size=size,
        mtime=mtime,
        encoding_used=encoding_used or "unknown",
        has_utf8_bom=has_utf8_bom,
        sha256_bytes=sha_bytes,
        sha256_text=sha_text,
        read_error=read_error,
        syntax_error=syntax_error,
        parse_ok=parse_ok,
        imports=imports,
        has_streamlit=has_streamlit,
        symbols=symbols,
        warnings=warnings,
    )


def build_summary(
    file_infos: list[PyFileInfo],
    *,
    scope: str,
    active_files_count: int,
    archive_v_i_files_count: int,
) -> dict[str, Any]:
    total_py_files = len(file_infos)
    parse_ok_count = sum(1 for fi in file_infos if fi.parse_ok)
    syntax_errors = [f"{fi.rel}: {fi.syntax_error}" for fi in file_infos if fi.syntax_error]
    read_errors = [f"{fi.rel}: {fi.read_error}" for fi in file_infos if fi.read_error]
    bom_warnings = [fi.rel for fi in file_infos if fi.has_utf8_bom]
    streamlit_importers = [fi.rel for fi in file_infos if fi.has_streamlit]
    blockers = read_errors + syntax_errors
    next_actions: list[str] = []
    if blockers:
        next_actions.append("Fix read_error/syntax_error files before relying on scanner output as preflight.")
    if bom_warnings:
        next_actions.append("Review bom_warning files; BOM is tolerated now but should be tracked explicitly.")
    if scope == "all" and archive_v_i_files_count:
        next_actions.append("Use --scope active for operational audits to exclude app/v_i historical files.")
    if not next_actions:
        next_actions.append("No blockers detected in the selected scope.")
    return {
        "total_py_files": total_py_files,
        "parse_ok": parse_ok_count,
        "syntax_errors": len(syntax_errors),
        "read_errors": len(read_errors),
        "bom_warnings": len(bom_warnings),
        "streamlit_importers": len(streamlit_importers),
        "active_files_count": active_files_count,
        "archive_v_i_files_count": archive_v_i_files_count,
        "blockers": blockers,
        "next_actions": next_actions,
    }


def build_txt_report(
    *,
    root: Path,
    args: argparse.Namespace,
    file_infos: list[PyFileInfo],
    summary: dict[str, Any],
) -> list[str]:
    syntax_entries = [(fi.rel, fi.syntax_error) for fi in file_infos if fi.syntax_error]
    read_entries = [(fi.rel, fi.read_error) for fi in file_infos if fi.read_error]
    bom_entries = [fi.rel for fi in file_infos if fi.has_utf8_bom]
    st_files = [fi.rel for fi in file_infos if fi.has_streamlit]

    lines: list[str] = []
    lines.append("=" * 96)
    lines.append("SCANNER AUDIT REPORT (FULL DUMP)")
    lines.append(f"Scanner version: {SCANNER_VERSION}")
    lines.append(f"Fecha         : {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"Root          : {root}")
    lines.append("=" * 96)
    lines.append("")

    lines.append("[SUMMARY]")
    lines.append(f"- scope                   : {args.scope}")
    lines.append(f"- total_py_files          : {summary['total_py_files']}")
    lines.append(f"- parse_ok                : {summary['parse_ok']}")
    lines.append(f"- syntax_errors           : {summary['syntax_errors']}")
    lines.append(f"- read_errors             : {summary['read_errors']}")
    lines.append(f"- bom_warnings            : {summary['bom_warnings']}")
    lines.append(f"- streamlit_importers     : {summary['streamlit_importers']}")
    lines.append(f"- active_files_count      : {summary['active_files_count']}")
    lines.append(f"- archive_v_i_files_count : {summary['archive_v_i_files_count']}")
    lines.append("- blockers                :")
    if summary["blockers"]:
        for blocker in summary["blockers"]:
            lines.append(f"  - {blocker}")
    else:
        lines.append("  - (none)")
    lines.append("- next_actions            :")
    for action in summary["next_actions"]:
        lines.append(f"  - {action}")
    lines.append("")

    lines.append("[PYTHON]")
    lines.append(f"sys.executable: {sys.executable}")
    lines.append(f"sys.version   : {sys.version.replace(os.linesep, ' ')}")
    lines.append("")

    lines.append("[PACKAGES CHECK]")
    for pkg in TARGET_PKGS:
        lines.append(f"{pkg:10s}: {pkg_status(pkg)}")
    lines.append("")

    lines.append("[SYNTAX]")
    if not syntax_entries and not read_entries:
        lines.append("OK: Sin errores de sintaxis ni de lectura.")
    else:
        lines.append(f"syntax_errors: {len(syntax_entries)}")
        lines.append(f"read_errors  : {len(read_entries)}")
        for rel, err in syntax_entries:
            lines.append(f"- {rel}: {err}")
        for rel, err in read_entries:
            lines.append(f"- {rel}: {err}")
    lines.append("")

    lines.append("[WARNINGS]")
    if bom_entries:
        for rel in bom_entries:
            lines.append(f"- {rel}: bom_warning")
    else:
        lines.append("OK: Sin warnings.")
    lines.append("")

    lines.append("[IMPORTS BY FILE] (resumen)")
    for fi in sorted(file_infos, key=lambda item: item.rel.lower()):
        imports = ", ".join(fi.imports) if fi.imports else "(sin imports)"
        lines.append(f"- {fi.rel}: {imports}")
    lines.append("")

    lines.append("[STREAMLIT IMPORTERS]")
    lines.append(f"Archivos que importan streamlit: {len(st_files)}")
    for rel in st_files:
        lines.append(f"- {rel}")
    lines.append("")

    lines.append("=" * 96)
    lines.append("[INDEX / TOC]")
    lines.append("Tip: usa Ctrl+F por el marcador exacto:  <<<BEGIN FILE: <ruta>>>")
    lines.append("=" * 96)
    for idx, fi in enumerate(sorted(file_infos, key=lambda item: item.rel.lower()), start=1):
        tag = "STREAMLIT" if fi.has_streamlit else "PY"
        flag_parts: list[str] = []
        if fi.has_utf8_bom:
            flag_parts.append("BOM")
        if fi.read_error:
            flag_parts.append("READ_ERROR")
        elif fi.syntax_error:
            flag_parts.append("SYNTAX_ERROR")
        elif fi.parse_ok:
            flag_parts.append("PARSE_OK")
        flags = ",".join(flag_parts) if flag_parts else "NONE"
        lines.append(
            f"{idx:02d}) [{tag}] {fi.rel}  | size={fi.size} | mtime={fi.mtime} | "
            f"encoding={fi.encoding_used} | sha256_bytes={fi.sha256_bytes[:12]}... | flags={flags}"
        )
        if fi.symbols:
            for lineno, kind, name in fi.symbols:
                lines.append(f"    - L{lineno:04d}  {kind}  {name}")
        elif fi.read_error:
            lines.append(f"    - read_error  {fi.read_error}")
        elif fi.syntax_error:
            lines.append(f"    - syntax_error  {fi.syntax_error}")
        else:
            lines.append("    - (sin defs/classes detectables)")
    lines.append("")

    lines.append("=" * 96)
    lines.append("[FULL CODE DUMP]")
    lines.append("Formato: '00001 | <linea>'")
    if args.max_dump_lines and args.max_dump_lines > 0:
        lines.append(f"Max lineas por archivo: {args.max_dump_lines}")
    lines.append("=" * 96)
    lines.append("")

    for fi in sorted(file_infos, key=lambda item: item.rel.lower()):
        lines.extend(
            [
                "-" * 96,
                f"<<<BEGIN FILE: {fi.rel}>>>",
                f"SIZE           : {fi.size} bytes",
                f"MTIME          : {fi.mtime}",
                f"ENCODING       : {fi.encoding_used}",
                f"HAS_UTF8_BOM   : {fi.has_utf8_bom}",
                f"SHA256(bytes)  : {fi.sha256_bytes}",
                f"SHA256(text)   : {fi.sha256_text}",
                f"READ_ERROR     : {fi.read_error or 'None'}",
                f"SYNTAX_ERROR   : {fi.syntax_error or 'None'}",
                f"PARSE_OK       : {fi.parse_ok}",
                f"WARNINGS       : {', '.join(fi.warnings) if fi.warnings else '(none)'}",
                "-" * 96,
            ]
        )
        if fi.src:
            dump = format_code_with_lineno(fi.src, width=5)
            if args.max_dump_lines and args.max_dump_lines > 0:
                dump_lines = dump.splitlines()
                if len(dump_lines) > args.max_dump_lines:
                    dump = "\n".join(dump_lines[: args.max_dump_lines]) + "\n... (TRUNCADO) ..."
            lines.append(dump)
        else:
            lines.append("(sin dump por read_error)")
        lines.append(f"\n<<<END FILE: {fi.rel}>>>")
        lines.append("")
    return lines


def build_json_payload(
    *,
    root: Path,
    scope: str,
    summary: dict[str, Any],
    file_infos: list[PyFileInfo],
) -> dict[str, Any]:
    return {
        "scanner_version": SCANNER_VERSION,
        "scope": scope,
        "root": str(root),
        "summary": summary,
        "files": [fi.to_json() for fi in file_infos],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="Ruta raiz del proyecto")
    parser.add_argument("--out", default="codigo_app_stockzero.txt", help="Archivo de salida (un solo .txt)")
    parser.add_argument(
        "--max-dump-lines",
        type=int,
        default=0,
        help="Maximo lineas por archivo en el dump (0 = sin limite).",
    )
    parser.add_argument("--scope", choices=["all", "active"], default="all")
    parser.add_argument("--json-out", default="", help="Archivo JSON de salida opcional")
    parser.add_argument("--fail-on-syntax", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    out_path = Path(args.out).resolve()
    json_out_path = Path(args.json_out).resolve() if args.json_out else None

    all_py_files = collect_all_py_files(root)
    selected_py_files = collect_py_files(root, scope=args.scope)
    active_files_count = sum(1 for path in all_py_files if not is_archive_file(str(path.relative_to(root)).replace("\\", "/")))
    archive_v_i_files_count = len(all_py_files) - active_files_count

    file_infos = [load_file_info(root, path) for path in selected_py_files]
    summary = build_summary(
        file_infos,
        scope=args.scope,
        active_files_count=active_files_count,
        archive_v_i_files_count=archive_v_i_files_count,
    )

    txt_lines = build_txt_report(root=root, args=args, file_infos=file_infos, summary=summary)
    out_path.write_text("\n".join(txt_lines), encoding="utf-8")

    if json_out_path is not None:
        payload = build_json_payload(root=root, scope=args.scope, summary=summary, file_infos=file_infos)
        json_out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OK - Reporte FULL generado en: {out_path}")
    if json_out_path is not None:
        print(f"OK - Reporte JSON generado en: {json_out_path}")

    if args.fail_on_syntax and (summary["syntax_errors"] > 0 or summary["read_errors"] > 0):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
