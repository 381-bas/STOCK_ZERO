# STOCK_ZERO Bootstrap Protocol

## Official bootstrap phrases

Use this short phrase for normal continuation:

`BOOTSTRAP STOCK_ZERO`

Use this full phrase for a new conversation, a master prompt, or a new controlled idea:

`BOOTSTRAP STOCK_ZERO DESDE MAIN: lee PROJECT_STATUS_INDEX, ACTIVE_ORDER_LOCK, EXECUTION_DOCTRINE, AGENT_ACCESS_POLICY y contratos activos antes de proponer pasos.`

## Expected assistant behavior

When a bootstrap phrase is used, the assistant must:

1. Treat `origin/main` as the source of truth.
2. Read `governance/PROJECT_STATUS_INDEX.json`.
3. Read `governance/ACTIVE_ORDER_LOCK.json`.
4. Read `governance/EXECUTION_DOCTRINE.md`.
5. Read `governance/AGENT_ACCESS_POLICY.json`.
6. Read active contracts listed in the status index.
7. Identify the active phase.
8. Identify allowed and forbidden actions.
9. Avoid implementation until a gate is confirmed.
10. Avoid presenting operational JSON/tables as if they were registered artifacts.

## Local terminal fallback

If GitHub access is unavailable, ask for:

- `git switch main`
- `git pull origin main`
- `git log -5 --oneline --decorate`
- `git status --short`
- `Get-Content governance/PROJECT_STATUS_INDEX.json`
- `Get-Content governance/ACTIVE_ORDER_LOCK.json`
- `Get-Content governance/EXECUTION_DOCTRINE.md`
- `Get-Content governance/AGENT_ACCESS_POLICY.json`

## Rule for new ideas

A new idea must first be classified as one of:

- research
- governance
- contract
- implementation
- cleanup
- decision record

No implementation starts until classification and phase gate are explicit.
