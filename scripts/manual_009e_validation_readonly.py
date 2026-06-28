from pathlib import Path
import os
import json
import psycopg
from psycopg.rows import dict_row

ROOT = Path(r"C:\Users\basti\Desktop\STOCK_ZERO")

DB_URL = os.environ["SUPABASE_DB_URL_ADMIN"]

SQL_5 = ROOT / "research" / "FAST_REFORM_009E_SQL_PACKAGE_AFTER_CLAUDE_GATE" / "5_VALIDATION_READONLY.sql"

result = {
    "phase": "BASTIAN_MANUAL_009E_VALIDATION_READONLY",
    "verdict": None,
    "connection_user": None,
    "transaction_read_only": None,
    "sql_file": str(SQL_5),
    "sql_file_exists": SQL_5.exists(),
    "result_sets": [],
    "commit": False,
    "rollback_executed": False,
    "error": None,
}

conn = None

try:
    if not SQL_5.exists():
        raise FileNotFoundError(f"SQL file not found: {SQL_5}")

    sql_text = SQL_5.read_text(encoding="utf-8-sig")

    conn = psycopg.connect(DB_URL, autocommit=False, row_factory=dict_row)

    with conn.cursor() as cur:
        # Force read-only transaction before any validation work.
        cur.execute("set transaction read only")

        cur.execute("select current_user as current_user")
        result["connection_user"] = cur.fetchone()["current_user"]

        cur.execute("show transaction_read_only")
        result["transaction_read_only"] = cur.fetchone()["transaction_read_only"]

        if result["transaction_read_only"] != "on":
            raise RuntimeError("transaction_read_only must be on for validation")

        cur.execute("set local statement_timeout = '5min'")
        cur.execute("set local lock_timeout = '5s'")

        # Execute readonly validation SQL.
        cur.execute(sql_text)

        set_index = 1

        while True:
            if cur.description is not None:
                rows = cur.fetchall()
                result["result_sets"].append({
                    "set_index": set_index,
                    "columns": [col.name for col in cur.description],
                    "rows": [dict(row) for row in rows],
                })
                set_index += 1

            if not cur.nextset():
                break

    conn.rollback()
    result["rollback_executed"] = True
    result["verdict"] = "READONLY_VALIDATION_COMPLETE"

except Exception as e:
    result["verdict"] = "BLOCKED_OR_ERROR"
    result["error"] = str(e)

    if conn is not None:
        try:
            conn.rollback()
            result["rollback_executed"] = True
        except Exception as rollback_error:
            result["rollback_error"] = str(rollback_error)

finally:
    if conn is not None:
        conn.close()

print(json.dumps(result, indent=2, ensure_ascii=False, default=str))