# Agent Workflow Rules - STOCK_ZERO

## Purpose

Keep the ChatGPT + Claude + Codex + Basti?n workflow useful without turning it into bureaucracy.

## Roles

| Actor | Role |
|---|---|
| ChatGPT | Direction, architecture, scope control, synthesis and final recommendation. |
| Claude | Adversarial review for risk, semantics, gates and post-implementation audit. |
| Codex | Implementation, tests, git/diff evidence and local execution. |
| Basti?n | Business validation and explicit risk/productive authorization. |

## Claude usage

Claude is used when the phase is ORANGE or RED, or when the work touches:

- executable code with compliance impact;
- denominator/fulfillment semantics;
- active contracts;
- productive apply preparation;
- rollback/cutover gates;
- post-implementation audit before important commit/PR.

Claude is not required for:

- simple git commands;
- low-risk documentation;
- typo/format fixes;
- PR body creation;
- command paste corrections;
- GREEN/YELLOW changes without semantic risk.

## Codex usage

Codex works after scope is filtered.

Codex must receive:

- allowed files;
- forbidden files;
- exact tests;
- evidence expectations;
- no automatic commit unless requested.

## Required pattern for ORANGE/RED

1. ChatGPT/Basti?n define scope.
2. Claude performs adversarial pre-review.
3. ChatGPT/Basti?n classify findings: incorporate, reject, defer.
4. Codex implements only accepted in-scope items.
5. ChatGPT reviews diff and tests.
6. Claude performs post-audit when executable risk exists.
7. Basti?n/ChatGPT authorize commit/PR.
8. Productive action requires separate RED_AUTHORIZED decision.

## Anti-bureaucracy rule

Do not use the full ORANGE/RED ritual for low-risk work.

A process that prevents errors but cannot deliver visible value has failed its operating purpose.
