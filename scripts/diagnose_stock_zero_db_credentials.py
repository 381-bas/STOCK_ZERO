from __future__ import annotations

import argparse
import json
import os
from urllib.parse import parse_qs, urlparse


EXPECTED_HOSTNAME = "db.xheyrgfagpoigpgakilu.supabase.co"
EXPECTED_DATABASE = "postgres"
PROFILE_CONTRACT = {
    "readonly": {
        "environment_variable": "DB_URL_CODEX_RO",
        "secret_name": "STOCK_ZERO_DB_CODEX_RO",
        "expected_username": "stock_zero_codex_ro",
    },
    "route-b-productive": {
        "environment_variable": "DB_URL_KPIONE_ROUTE_B_PRODUCTIVE",
        "secret_name": "STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE",
        "expected_username": "stock_zero_kpione_route_b_load",
    },
    "admin-provisioning": {
        "environment_variable": "DB_URL_ADMIN",
        "secret_name": "STOCK_ZERO_DB_ADMIN",
        "expected_username": "postgres",
    },
}


def diagnose(profile: str, environment: dict[str, str] | os._Environ[str]) -> dict[str, object]:
    contract = PROFILE_CONTRACT[profile]
    raw = environment.get(contract["environment_variable"])
    parsed = urlparse(raw or "")
    sslmodes = parse_qs(parsed.query, keep_blank_values=True).get("sslmode", [])
    return {
        "credential_class": profile,
        "vault": "STOCK_ZERO",
        "secret_name": contract["secret_name"],
        "environment_variable": contract["environment_variable"],
        "secret_env_present": bool(raw),
        "username_matches": bool(raw) and parsed.username == contract["expected_username"],
        "hostname_matches": bool(raw) and (parsed.hostname or "").lower() == EXPECTED_HOSTNAME,
        "database_matches": bool(raw) and parsed.path.lstrip("/") == EXPECTED_DATABASE,
        "ssl_required": bool(raw) and sslmodes == ["require"],
        "role_password_env_present": (
            bool(environment.get("KPIONE_ROUTE_B_PRODUCTIVE_PASSWORD"))
            if profile == "admin-provisioning" else None
        ),
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Redacted STOCK_ZERO credential diagnostics")
    result.add_argument("--credential-class", choices=sorted(PROFILE_CONTRACT), required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    print(json.dumps(diagnose(args.credential_class, os.environ), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
