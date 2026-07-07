# Verdict Taxonomy - STOCK_ZERO

## Purpose

Prevent authorization drift between audit opinions, phase decisions, script results and productive approval.

## Canonical meanings

| Term | Layer | Meaning | Productive authorization |
|---|---|---|---|
| APPROVE | Auditor opinion | Auditor accepts the reviewed artifact without blocking warnings. | No |
| APPROVE_WITH_WARNINGS | Auditor opinion | Auditor accepts continuation with explicit warnings or deferred risks. | No |
| BLOCK | Auditor opinion | Auditor found a blocker; do not proceed until resolved. | No |
| ACCEPT_WITH_SCOPE_CONTROL | ChatGPT/Basti?n filter | Finding is accepted only inside the current scope and limits. | No |
| GO_WITH_LIMITS | Phase verdict | Phase may proceed or close only under stated limits. | No |
| NO_GO | Phase verdict | Phase must stop or rework before continuing. | No |
| PASS_* | Script/test result | A technical check passed under its local assumptions. | No |
| READY_FOR_PULL_REQUEST | Git/review state | Branch is ready for PR review. | No |
| MERGED_TO_MAIN | Git state | PR has been merged to main. | No |
| RED_AUTHORIZED | Business/productive authorization | Explicit Basti?n authorization for productive apply/cutover. | Yes, only for the named scope |

## Rules

- Auditor verdicts are evidence, not authorization.
- Script verdicts are evidence, not authorization.
- GO_WITH_LIMITS never means productive apply.
- READY_FOR_PULL_REQUEST never means production-ready.
- RED_AUTHORIZED must name scope, evidence reviewed, limits accepted and rollback/cutover expectations.
