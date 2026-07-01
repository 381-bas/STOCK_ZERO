# STOCK_ZERO Execution Doctrine

## Purpose

This file defines execution discipline for STOCK_ZERO. Chat messages, agent replies, and GitHub comments are not sufficient governance. Only versioned repo files, committed evidence, and explicit phase gates govern execution.

## Hard rules

1. No phase executes code without a defined phase objective.
2. No script defines the contract. Scripts implement contracts.
3. Research evidence does not govern by itself. Stable rules must be promoted to `contracts/` or `governance/`.
4. ChatGPT acts as controller and gatekeeper.
5. Claude acts as critical auditor and contract reviewer.
6. Codex acts as executor only after the contract is locked.
7. Ambiguous user instructions do not authorize execution.
8. Phrases such as `sigamos`, `continuemos`, `validemos`, `veamos`, or `dale` do not authorize scripts, commits, DB actions, or cleanup.
9. DB apply requires explicit authorization.
10. Commit requires explicit authorization.
11. Merge to main requires explicit review.
12. Cleanup is a phase, not an incidental action.
13. If an important rule only exists in chat, it is not operationally durable.
14. External agent status must not be assumed. Wait for explicit output or evidence.
15. Do not start a new implementation phase while the current phase has unresolved close conditions.

## Required phase gate

Each phase must define:

- phase name
- objective
- allowed actions
- forbidden actions
- expected evidence
- close condition
- commit/push decision

## Evidence hierarchy

1. Committed repo files
2. PR / GitHub issue records
3. Local terminal output
4. Chat conversation

Chat alone is not a source of truth.

## Display artifact rule

JSON blocks, tables, drafts, diagrams, or structured summaries shown in chat are previews only.

If any displayed artifact affects business rules, data contracts, execution order, agent behavior, loader behavior, database policy, cleanup policy, or implementation scope, it must be promoted to a versioned repository artifact before it can govern execution.

The required promotion path is:

`preview -> target artifact -> diff/script -> validation -> commit -> push -> PR -> merge to main`

No operational rule is considered durable until it exists in the repository.

## PR unit of value rule

From 011 onward, PRs must represent a verifiable unit of value, not every administrative event.

Default rule:

`1 operational objective = 1 branch = 1 PR`

A PR may contain multiple internal commits, for example:

- contract / lock
- implementation or documentation
- tests / evidence
- audit
- closeout metadata

Split into another PR only when the risk class changes.

Risk split rule:

- GREEN and YELLOW may be grouped when paths are bounded.
- ORANGE requires explicit review and usually a separate PR or sub-phase.
- RED requires separate phase and explicit Basti?n authorization.

Do not create closeout-only or audit-only PRs unless they unlock a decision, change a rule, or reduce real risk.

## Git vs ChatGPT Project source rule

Git is the versioned source of truth for governance and KERNEL files.

ChatGPT Project sources are operational context, not final authority.

If the KERNEL/governance files loaded in ChatGPT Project differ from the files committed in Git, the assistant must alert the divergence and treat Git as authoritative.

After a KERNEL update is merged to `main`, the ChatGPT Project sources should be replaced with the new version.
