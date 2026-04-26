# -*- coding: utf-8 -*-
from __future__ import annotations

"""
STOCK_ZERO / GESTIONZERO
Supabase preflight for local network diagnosis (V3).

Changes vs V2:
- catches DNS resolution failures instead of crashing
- reports host parsing and resolution problems per target
- continues testing remaining targets
- keeps TCP / SQL phases separate
"""

import argparse
import json
import socket
import time
from dataclasses import dataclass, asdict

import psycopg2

READ_SQL = "select now(), current_user, inet_server_addr(), inet_server_port();"
WRITE_SQL = """
begin;
insert into cg_audit.batch_registry
(source_name, file_name, file_hash, row_count_raw, loaded_by, status)
values
('PING_PRECHECK', 'ping.txt', 'x', 1, 'stock_zero_preflight', 'in_progress')
returning batch_id::text;
"""

@dataclass
class AddrCheck:
    family: str
    ip: str
    port: int
    tcp_ok: bool
    tcp_ms: float | None
    tcp_error: str | None

@dataclass
class TargetResult:
    name: str
    dsn_present: bool
    host: str | None
    port: int | None
    resolve_ok: bool
    resolve_error: str | None
    addresses: list[AddrCheck]
    connect_ok: bool
    connect_ms: float | None
    read_ok: bool
    read_ms: float | None
    write_ok: bool
    write_ms: float | None
    rollback_ok: bool
    error_type: str | None
    error_text: str | None

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--direct-url", default=None)
    ap.add_argument("--pooler-url", default=None)
    ap.add_argument("--session-pooler-url", default=None)
    ap.add_argument("--connect-timeout", type=int, default=10)
    ap.add_argument("--tcp-timeout", type=float, default=3.0)
    ap.add_argument("--prefer-ipv4", action="store_true", default=True)
    ap.add_argument("--no-prefer-ipv4", dest="prefer_ipv4", action="store_false")
    ap.add_argument("--json", action="store_true")
    return ap.parse_args()

def dsn_sources(args: argparse.Namespace) -> dict[str, str | None]:
    import os
    return {
        "direct_5432": args.direct_url or os.getenv("DB_URL_DIRECT"),
        "pooler_6543": args.pooler_url or os.getenv("DB_URL_POOLER") or os.getenv("DB_URL_LOAD"),
        "session_pooler_5432": args.session_pooler_url or os.getenv("DB_URL_SESSION_POOLER"),
    }

def family_name(fam: int) -> str:
    return {socket.AF_INET: "IPv4", socket.AF_INET6: "IPv6"}.get(fam, str(fam))

def extract_host_port(dsn: str) -> tuple[str | None, int | None]:
    try:
        params = psycopg2.extensions.parse_dsn(dsn)
        host = params.get("host")
        port_raw = params.get("port")
        port = int(port_raw) if port_raw else 5432
        return host, port
    except Exception:
        return None, None

def resolve_addrs(host: str, port: int) -> tuple[bool, str | None, list[tuple[int, str]]]:
    seen: set[tuple[int, str]] = set()
    out: list[tuple[int, str]] = []
    try:
        for fam, _, _, _, sockaddr in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM):
            ip = sockaddr[0]
            key = (fam, ip)
            if key not in seen:
                seen.add(key)
                out.append((fam, ip))
        return True, None, out
    except Exception as e:
        return False, f"{type(e).__name__}: {e}", []

def tcp_probe(ip: str, port: int, fam: int, timeout: float) -> AddrCheck:
    t0 = time.perf_counter()
    s = None
    try:
        s = socket.socket(fam, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((ip, port))
        return AddrCheck(
            family=family_name(fam),
            ip=ip,
            port=port,
            tcp_ok=True,
            tcp_ms=round((time.perf_counter() - t0) * 1000, 1),
            tcp_error=None,
        )
    except Exception as e:
        return AddrCheck(
            family=family_name(fam),
            ip=ip,
            port=port,
            tcp_ok=False,
            tcp_ms=round((time.perf_counter() - t0) * 1000, 1),
            tcp_error=f"{type(e).__name__}: {e}",
        )
    finally:
        try:
            if s is not None:
                s.close()
        except Exception:
            pass

def choose_connect_order(addrs: list[tuple[int, str]], prefer_ipv4: bool) -> list[tuple[int, str]]:
    if not prefer_ipv4:
        return addrs
    v4 = [x for x in addrs if x[0] == socket.AF_INET]
    v6 = [x for x in addrs if x[0] == socket.AF_INET6]
    return v4 + v6

def try_sql(dsn: str | None, host: str | None, port: int | None, connect_timeout: int, tcp_timeout: float, prefer_ipv4: bool) -> TargetResult:
    if not dsn:
        return TargetResult("", False, host, port, False, "MissingDSN: No DSN supplied", [], False, None, False, None, False, None, False, "MissingDSN", "No DSN supplied")

    if not host or not port:
        return TargetResult("", True, host, port, False, "HostOrPortMissingFromDSN", [], False, None, False, None, False, None, False, "HostParseError", "Unable to parse host/port from DSN")

    resolve_ok, resolve_error, addrs = resolve_addrs(host, port)
    ordered = choose_connect_order(addrs, prefer_ipv4)
    addresses = [tcp_probe(ip, port, fam, timeout=tcp_timeout) for fam, ip in ordered]

    conn = None
    cur = None
    connect_ms = None
    read_ms = None
    write_ms = None
    rollback_ok = False

    try:
        t0 = time.perf_counter()
        conn = psycopg2.connect(
            dsn,
            connect_timeout=connect_timeout,
            application_name="stock_zero_preflight_v3",
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        connect_ms = round((time.perf_counter() - t0) * 1000, 1)

        cur = conn.cursor()

        t1 = time.perf_counter()
        cur.execute(READ_SQL)
        cur.fetchone()
        read_ms = round((time.perf_counter() - t1) * 1000, 1)

        t2 = time.perf_counter()
        cur.execute(WRITE_SQL)
        cur.fetchone()
        write_ms = round((time.perf_counter() - t2) * 1000, 1)

        conn.rollback()
        rollback_ok = True

        return TargetResult("", True, host, port, resolve_ok, resolve_error, addresses, True, connect_ms, True, read_ms, True, write_ms, rollback_ok, None, None)

    except Exception as e:
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        return TargetResult("", True, host, port, resolve_ok, resolve_error, addresses, connect_ms is not None, connect_ms, read_ms is not None, read_ms, write_ms is not None, write_ms, rollback_ok, type(e).__name__, str(e))

    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

def print_target(name: str, result: TargetResult) -> None:
    print("=" * 90)
    print(f"[TARGET] {name}")
    print(f"[DSN] {'present' if result.dsn_present else 'missing'} | host={result.host} | port={result.port}")
    print(f"[RESOLVE] {'OK' if result.resolve_ok else 'FAIL'} | err={result.resolve_error}")
    if result.addresses:
        for a in result.addresses:
            status = "OK" if a.tcp_ok else "FAIL"
            print(f"[TCP] {a.family} {a.ip}:{a.port} -> {status} | ms={a.tcp_ms} | err={a.tcp_error}")
    else:
        print("[TCP] no addresses resolved")
    print(f"[CONNECT] {'OK' if result.connect_ok else 'FAIL'} | ms={result.connect_ms}")
    print(f"[READ] {'OK' if result.read_ok else 'FAIL'} | ms={result.read_ms}")
    print(f"[WRITE] {'OK' if result.write_ok else 'FAIL'} | ms={result.write_ms}")
    print(f"[ROLLBACK] {'OK' if result.rollback_ok else 'FAIL'}")
    if result.error_type or result.error_text:
        print(f"[ERROR] {result.error_type}: {result.error_text}")

def recommendation(results: dict[str, TargetResult]) -> str:
    for preferred in ("session_pooler_5432", "pooler_6543", "direct_5432"):
        r = results.get(preferred)
        if r and r.write_ok:
            return f"usable_sql_endpoint={preferred}"
    for preferred in ("session_pooler_5432", "pooler_6543", "direct_5432"):
        r = results.get(preferred)
        if r and r.read_ok:
            return f"read_only_endpoint={preferred}; write_blocked_or_unverified"
    for preferred in ("session_pooler_5432", "pooler_6543", "direct_5432"):
        r = results.get(preferred)
        if r and r.resolve_ok and any(a.tcp_ok for a in r.addresses):
            return f"tcp_only_endpoint={preferred}; sql_handshake_blocked"
    for preferred in ("session_pooler_5432", "pooler_6543", "direct_5432"):
        r = results.get(preferred)
        if r and not r.resolve_ok:
            return f"dns_blocked_or_broken_endpoint={preferred}"
    return "no_usable_endpoint_from_current_network"

def main() -> int:
    args = parse_args()
    urls = dsn_sources(args)
    results = {}

    for name, dsn in urls.items():
        host, port = extract_host_port(dsn) if dsn else (None, None)
        res = try_sql(dsn, host, port, args.connect_timeout, args.tcp_timeout, args.prefer_ipv4)
        res.name = name
        results[name] = res
        if not args.json:
            print_target(name, res)

    rec = recommendation(results)
    if args.json:
        payload = {"results": {k: asdict(v) for k, v in results.items()}, "recommendation": rec}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print("=" * 90)
        print(f"[RECOMMENDATION] {rec}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
