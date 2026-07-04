# 013 Route B ORANGE No-Apply Dry-Run Claude Review

## Objective

Validate Route B no-apply/dry-run integration with mandatory Claude adversarial review before Codex implementation.

## Current mode

ORANGE_NO_APPLY_DRY_RUN. No productive writes, SQL apply, Supabase writes, backfill, cutover, productive loader modification or active contract semantic change are authorized.

## Agent model

- ChatGPT: direction, architecture, final synthesis and scope control.
- Claude: mandatory adversarial technical review and suggestions.
- Codex: implementation, tests, git and evidence after review filter.
- Bastian: business validation and risk authorization.

## Required invariant

photo_row -> event_row -> day_presence

## Critical failure mode

one Excel photo row = one visit

## Planned artifacts

- CLAUDE_REVIEW_PACKET.md
- CLAUDE_REVIEW.md
- CLAUDE_REVIEW_SCORECARD.md
- DRY_RUN_INTEGRATION_EVIDENCE.md
- DENOMINATOR_RECONCILIATION.md
- CLOSEOUT.md
