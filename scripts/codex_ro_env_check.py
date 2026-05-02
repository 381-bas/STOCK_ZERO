#!/usr/bin/env python
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SECRET_FILE = ROOT / ".local_secrets" / "codex_ro.env"


def _normalize_db_url(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if value.startswith("$env:DB_URL_CODEX_RO="):
        value = value.split("=", 1)[1].strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1].strip()
    return value


def _load_db_url_from_file(path: Path) -> str:
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.replace("\ufeff", "").strip()
        if not raw or raw.startswith("#"):
            continue
        if raw.startswith("DB_URL_CODEX_RO="):
            _, value = raw.split("=", 1)
            return _normalize_db_url(value)
    return ""


def resolve_db_url() -> tuple[str, str]:
    env_value = _normalize_db_url(os.getenv("DB_URL_CODEX_RO", ""))
    if env_value:
        return env_value, "env"
    file_value = _load_db_url_from_file(SECRET_FILE)
    if file_value:
        return file_value, "file"
    return "", "none"


def run_select(cur, sql: str):
    stripped = sql.lstrip().upper()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        raise RuntimeError("NON_SELECT_QUERY_BLOCKED")
    cur.execute(sql)


def main() -> int:
    db_url, source_used = resolve_db_url()
    present = bool(db_url)
    if not present:
        print(f"DB_URL_CODEX_RO_PRESENT={'true' if present else 'false'}")
        print(f"SOURCE_USED={source_used}")
        print("NO_DB_URL_AVAILABLE")
        return 1

    try:
        import psycopg2
    except ModuleNotFoundError:
        venv_python = ROOT / ".venv" / "Scripts" / "python.exe"
        if venv_python.exists() and Path(sys.executable).resolve() != venv_python.resolve():
            completed = subprocess.run([str(venv_python), str(Path(__file__).resolve())], check=False)
            return int(completed.returncode)
        raise

    print(f"DB_URL_CODEX_RO_PRESENT={'true' if present else 'false'}")
    print(f"SOURCE_USED={source_used}")
    conn = psycopg2.connect(db_url)
    try:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            run_select(
                cur,
                """
                SELECT
                    current_user::text,
                    current_setting('default_transaction_read_only')::text,
                    current_setting('statement_timeout')::text
                """,
            )
            current_user, readonly_state, statement_timeout = cur.fetchone()
            print(f"CURRENT_USER={current_user}")
            print(f"DEFAULT_TRANSACTION_READ_ONLY={readonly_state}")
            print(f"STATEMENT_TIMEOUT={statement_timeout}")

            run_select(cur, 'SELECT COUNT(*)::bigint FROM cg_mart.mv_cg_out_weekly_v2')
            mv_count = int(cur.fetchone()[0])
            print(f"MV_COUNT={mv_count}")

            run_select(
                cur,
                '''
                SELECT COALESCE(CAST("ALERTA" AS text), '<NULL>') AS alerta, COUNT(*)::bigint AS rows
                FROM cg_mart.mv_cg_out_weekly_v2
                GROUP BY COALESCE(CAST("ALERTA" AS text), '<NULL>')
                ORDER BY alerta
                ''',
            )
            alerta_counts = [{"ALERTA": alerta, "rows": int(rows)} for alerta, rows in cur.fetchall()]
            print("ALERTA_COUNTS_JSON=" + json.dumps(alerta_counts, ensure_ascii=False))
        conn.rollback()
        return 0
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
