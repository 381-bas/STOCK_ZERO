# STOCK_ZERO Decision Promotion Protocol

## Purpose

This protocol prevents operational decisions from remaining only in chat, GitHub comments, screenshots, prompts, memory, or temporary summaries.

A chat response, JSON block, table, diagram, or draft is only a preview. It does not govern execution unless it is promoted into a versioned repository artifact and merged to `main`.

## Promotion rule

If a decision affects business rules, data contracts, loader behavior, database apply policy, agent roles, execution order, phase gates, source-of-truth status, cleanup policy, or destructive action policy, it must be promoted to the repository.

## Classification

| Decision type | Destination |
|---|---|
| Business/data contract | `contracts/<module>/...` |
| Execution/governance rule | `governance/...` |
| Active project status | `governance/PROJECT_STATUS_INDEX.json` |
| Active phase lock | `governance/ACTIVE_ORDER_LOCK.json` |
| Historical decision | `governance/decisions/ADR_*.md` |
| Exploratory evidence | `research/<phase>/...` |
| GitHub discussion synthesis | `governance/thread_summaries/...` |

## Required flow

1. Identify the decision.
2. Classify the decision.
3. Select the target repo artifact.
4. Produce a minimal diff or script.
5. Validate syntax and scope.
6. Commit on a lab branch.
7. Push branch.
8. Open PR.
9. Review.
10. Merge to `main`.
11. Treat `main` as the source of truth.

## Anti-patterns

The following are not sufficient:

- Chat said it.
- It is in a GitHub comment.
- It is in a prompt.
- It is in memory.
- It was shown as a JSON/table.
- We agreed verbally.
- It appears in a screenshot.

## Rule

Displayed artifacts are previews only. If an operational rule matters, it must become a versioned repo artifact.
