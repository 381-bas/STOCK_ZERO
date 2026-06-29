# GitHub Issue #18 Thread Synthesis

## Issue

`#18 FAST_REFORM_009_JUNE_DIRECT_REFORM_LOAD_ROUTE_B`

## Purpose

This file synthesizes the usable status of the long GitHub issue thread. The thread remains an audit trail, but it no longer governs execution by itself.

## Validated direction

Route B remains the strategic direction:

- do not keep growing `cg_raw.kpione2_raw` as the permanent loading surface
- move toward a lighter direct load path into `cg_reform` / current model structures
- use controlled source files, manifests, audit checks, and explicit gates
- avoid app cutover, grants, public/cg_mart replacement, or reclaim until explicitly authorized

## Validated and promoted

| Item | Artifact |
|---|---|
| Chat/GitHub comments are not sufficient governance | `governance/EXECUTION_DOCTRINE.md` |
| Agent access/roles | `governance/AGENT_ACCESS_POLICY.json` |
| KPIONE2 photo export grain contract | `contracts/control_gestion/kpione2_photo_export_contract_v1.json` |
| Active phase control | `governance/ACTIVE_ORDER_LOCK.json` |
| Bootstrap continuity | `governance/PROJECT_STATUS_INDEX.json` and `governance/BOOTSTRAP_PROTOCOL.md` |
| Decision promotion rule | `governance/DECISION_PROMOTION_PROTOCOL.md` |

## Superseded by 009F / governance

- one Excel row equals one visit
- comments alone can govern execution
- GitHub issue thread can act as the source of truth
- chat JSON/table previews are operationally durable
- 009G can start before 009F/governance closure

## Still relevant for future Route B work

- file-drop pipeline under `data/control_gestion/`
- manifests and file hash ledger
- month-to-date rebuild strategy for hot months
- frozen/cold month guard
- day coverage audit
- idempotent rebuild policy
- additive cg_reform DDL review
- source_generated_at / supersedes_file_hash decision
- no mutation of `cg_raw.kpione2_raw`
- no public/cg_mart replacement without explicit cutover decision

## Open doubts

This synthesis does not rewrite all historical comments line by line. The issue thread is long and remains available as audit evidence.

Before implementing Route B/009G, create a new clean phase or issue that references this synthesis and the versioned contracts.

## Governance rule

Issue #18 comments remain historical audit trail only. The source of truth is the versioned repository state on `main`.
