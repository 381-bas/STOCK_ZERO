# AGENTS.md - STOCK_ZERO / GESTIONZERO

## Purpose

This repository powers STOCK_ZERO / GESTIONZERO. Work must stay small, auditable, evidence-driven, and safe.

## Self-preflight rule

Before modifying files, run the relevant preflight unless the task is read-only investigation.

After meaningful changes, rerun the same preflight before closing the task.

Use PowerShell from the repository root.

## Phase commands

### scanner_only

Use for scanner work or repo-structure checks.

```powershell
python scripts/sz_preflight.py --phase scanner_only --root . --json-out $env:TEMP\sz_preflight_scanner_only.json
```

### generic

Use for normal local analysis/patch work that does not need DB.

```powershell
python scripts/sz_preflight.py --phase generic --root . --skip-db --json-out $env:TEMP\sz_preflight_generic.json
```

### control_gestion_v2

Use for CONTROL_GESTION v2 work. DB access, if needed, must stay read-only through the preflight helper.

```powershell
python scripts/sz_preflight.py --phase control_gestion_v2 --root . --json-out $env:TEMP\sz_preflight_cg_v2.json
```

### 9B15

Use for export-contract design or similar gated phases.

```powershell
python scripts/sz_preflight.py --phase 9B15 --root . --require-clean-git --expected-head <expected_head> --json-out $env:TEMP\sz_preflight_9b15.json
```

## Blocking rules

Preflight blocks progress when:
- `final_verdict = block`
- scanner reports `syntax_errors > 0`
- scanner reports `read_errors > 0`
- `--require-clean-git` is used and the worktree is not clean
- `--expected-head` is used and HEAD does not match
- DB-aware preflight shows `current_user != stock_zero_codex_ro`
- DB-aware preflight shows `readonly_state != on`

Warnings do not block by themselves. They allow continuation unless the user explicitly wants a warning-free lane.

Typical warnings:
- `bom_warnings > 0`
- kernel HEAD mismatch
- missing non-critical kernels
- dirty worktree when clean git was not required

## Credential and DB rules

- Never use `DB_URL_LOAD`, `DB_URL_APP`, `DB_URL`, or admin credentials for Codex checks.
- DB access is allowed only through `scripts/codex_ro_env_check.py` or `scripts/sz_preflight.py` in DB-aware phases.
- Never print DSNs or secrets.
- DB checks must remain read-only.

## Hard restrictions

- Do not run loaders from Codex unless the user explicitly asks and the phase allows it.
- Do not run product refresh flows from Codex unless explicitly requested.
- Do not touch Supabase SQL unless the task explicitly allows it.
- Do not commit or push unless the user explicitly asks.

## Output discipline

Report results in a compact structured block in the Codex response.

Do not create persistent JSON, TXT, log, report or artifact files unless explicitly requested.

Temporary validation outputs must go to `%TEMP%` and may be overwritten.

Use `scripts/sz_preflight.py` as an internal validation tool before and after relevant changes.

The deliverable to the user is the structured screen report, not the temporary file.

If a blocker appears, stop and report it.

If warnings are expected and non-blocking, Codex may continue but must declare them.

For implementation work, report:

```json
{
  "phase": "",
  "task_type": "investigation|implementation|smoke|review",
  "preflight": {
    "executed": false,
    "phase": "",
    "final_verdict": "ok|warn|block|not_run",
    "blockers": [],
    "warnings": []
  },
  "files_modified": [],
  "db_access": {
    "used": false,
    "mode": "none|DB_URL_CODEX_RO",
    "writes_attempted": false,
    "dsn_printed": false
  },
  "commands_run": [],
  "smoke_results": {},
  "git_status_after": "",
  "final_verdict": "READY_FOR_CHAT_REVIEW|BLOCKED"
}
```
