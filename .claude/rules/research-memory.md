---
description: How to use the shared research memory and the multi-agent protocol.
paths:
  - research/**/*.json
  - research/**/*.jsonl
  - research/**/*.md
---

# Research and memory rules

The `research/` tree is the shared, durable memory of the project. Respect each file's role.

## File roles

- `AI_PROJECT_HORIZON.json` — the ACTIVE direction (H0–H5). Read it before proposing direction.
- `AI_CAPABILITY_MAP.json` — the functional classification of capabilities. Read it before
  classifying any component.
- `AI_SHARED_MEMORY.json` — VALIDATED knowledge. The source of truth for what is settled.
- `AI_FINDINGS_LEDGER.jsonl` — the append-only history of findings (PROPOSED → VALIDATED/…).
- `AI_BACKLOG.json` — candidate work. It does NOT authorize implementation.

## Discipline

- Distinguish FACT, INFERENCE, and HYPOTHESIS in every claim; give each finding a unique id,
  evidence, and the baseline commit.
- Do not repeat a VALIDATED finding without genuinely new evidence.
- Prefer reading the domain slice via `/sz-context <domain>` over re-reading everything.

## Roles and authority

- Claude proposes (may append PROPOSED findings only when a task explicitly authorizes it).
- Codex validates and executes; it may update shared memory and backlog when authorized.
- ChatGPT synthesizes and sets priorities; Bastián is the final authority.
- Do not edit validated memory (`AI_SHARED_MEMORY.json`, `AI_BACKLOG.json`) unless a task
  expressly authorizes that exact change.
- No finding, backlog item, or horizon authorizes implementation by itself.
- Commit/push only the exact files an explicit task names.
