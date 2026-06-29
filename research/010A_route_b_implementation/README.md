# 010A Route B Implementation Lock

## Phase

`FAST_REFORM_010A_ROUTE_B_IMPLEMENTATION_LOCK`

## Base

`main @ 36654d1`

## Purpose

This phase prepares Route B implementation work under the active delegated agent authority model.

It does not implement Route B.

It does not apply DB changes.

It does not apply SQL.

It does not patch the loader.

## Active governance

- `governance/AGENT_AUTHORITY_MATRIX_V2.json`
- `governance/ACTIVE_ORDER_LOCK.json`
- `governance/PROJECT_STATUS_INDEX.json`
- `contracts/control_gestion/kpione2_photo_export_contract_v1.json`

## Route B hard contract

Route B must preserve:

`photo_row -> event_row -> day_presence`

Forbidden assumption:

`one_excel_row_equals_one_visit`

## Agent model

- Claude audits scope, contract risk and ORANGE changes.
- Codex executes only after the lock is merged and only within a new explicit implementation branch.
- ChatGPT arbitrates against committed governance and contracts.
- Bastián authorizes RED actions only.

## Non-goals

- No DB apply.
- No SQL apply.
- No loader patch.
- No data movement.
- No production cutover.
