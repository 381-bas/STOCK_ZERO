@AGENTS.md

# STOCK_ZERO CLAUDE BOOTSTRAP

STOCK_ZERO / GESTIONZERO is an evidence-driven, multi-agent project. Claude proposes,
Codex validates and executes, ChatGPT synthesizes, Bastián decides. Keep work small,
auditable, and gated. This file is guidance, not authorization.

## Before you act

- Read `research/AI_PROJECT_HORIZON.json` before proposing any direction or roadmap.
- Read `research/AI_CAPABILITY_MAP.json` before classifying any component or capability.
- Read `research/AI_SHARED_MEMORY.json` before re-investigating anything — do not repeat
  VALIDATED findings without new evidence.
- Read `research/AI_BACKLOG.json` before proposing work. The backlog is a candidate list;
  it is NOT authorization to implement.

Do not import these JSON files with `@` here. They are read on demand (use `/sz-context`)
so the initial context stays small.

## Hard rules

- Never treat `latest` (latest raw, latest route) as a temporal contract. Builds must
  declare window, raw batch set, weekly route snapshot, precedence version, build version.
- Do not modify runtime, DB, loaders, SQL, kernels, or validated memory without an explicit
  task that authorizes that exact change. No implementation is authorized by this file.
- No DB, Docker, loader, refresh, commit, or push unless the task explicitly asks.
- payload_json is REQUIRED_CURRENT_SURFACE; it cannot be removed without proven parity.
- G0 parity must close before any Supabase retention, deletion, or cleanup.

## Workflow

- Run `/sz-context <domain>` before significant research or implementation to load a compact,
  current context for one domain (control_gestion, inventory, route, database, runtime, research).
- Git/GitHub is the shared channel between Claude, Codex, and ChatGPT. Commit/push only the
  exact files an explicit task authorizes.
- Distinguish FACT, INFERENCE, and HYPOTHESIS in every research claim.
