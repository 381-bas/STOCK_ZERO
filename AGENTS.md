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

## Loader execution guardrails

- Codex may inspect, audit, grep, compile, and review loader scripts.
- Codex must not execute DB loaders, product refresh, MV refresh, or incremental apply unless the user explicitly authorizes that exact command and phase.
- Productive loader execution remains Bastian-only by default.

Current guarded loader roles:
- `scripts/load_control_gestion_raw_v17.py`: main CONTROL_GESTION raw loader.
- `scripts/refresh_control_gestion_v2_incremental.py`: guarded incremental refresh helper; dry-run/apply semantics must be respected.
- `scripts/refresh_control_gestion_v2_mv.py`: fallback/full MV refresh helper.
- `scripts/load_fact_from_excel.py`: manual/fallback inventory loader.
- `scripts/load_ruta_rutero_from_excel.py`: manual/fallback route loader.
- `scripts/cliente_mvs.py`: helper used by stock/ruta loaders for cliente MVs.

Legacy loaders removed by Casa Limpia:
- `scripts/load_cg_power_app_raw_v1.py`

If a task asks to run a loader, Codex must block or ask for an explicit reprompt unless command, phase, DB mode, and authorization are unambiguous.

Codex must never infer DB write authorization from a cleanup or audit task.

## ChatGPT + Codex task gates

Codex must classify each request before acting:

- `read_only`: inspect, audit, review, list, summarize, or diagnose. Do not edit files. Do not run preflight unless explicitly requested or needed as a read-only smoke.
- `implementation`: modify files only when the user explicitly asks for a change/fix/patch or clearly authorizes implementation.
- `smoke`: run safe validation commands only. Do not modify files.
- `blocked_or_unclear`: stop and ask for a reprompt when scope, permissions, phase, expected HEAD, DB mode, or write authorization is unclear.

### Verdicts

Use these user-facing verdicts for small ChatGPT + Codex tasks:

- `APROBADO`: requested read/write scope completed, no blockers, only acceptable warnings if any.
- `WARN`: task can continue, but there are non-blocking warnings, dirty git, missing optional context, or partial coverage.
- `FAIL`: blocker found, unsafe permission boundary, required file/tool missing, preflight block, SQL/write risk, or unclear instruction that could cause unsafe work.

### When to block

Block immediately when:
- the user requests DB writes, SQL execution, loaders, product refresh, Supabase changes, kernel changes, commit, push, branch creation, or dependency installation without explicit authorization;
- preflight returns `final_verdict = block`;
- scanner reports syntax/read errors;
- DB-aware checks are not read-only;
- required phase, expected HEAD, or write permission is missing for a gated task.

### When to ask for reprompt

Ask for a reprompt instead of guessing when:
- the task mixes read-only and implementation instructions;
- the requested phase is unclear;
- the user asks to proceed but does not authorize required writes;
- the change target is ambiguous;
- safety requirements conflict.

### Output format for small tasks

For read-only audits and smoke checks, report:

```json
{
  "task_type": "read_only|smoke|review|implementation",
  "verdict": "APROBADO|WARN|FAIL",
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
  "findings": [],
  "next_step": "continuar|aplicar patch|pedir reprompt|bloquear"
}
```

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
