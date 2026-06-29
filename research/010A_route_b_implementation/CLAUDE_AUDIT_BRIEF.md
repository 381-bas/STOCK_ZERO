# Claude Audit Brief — 010A Route B

## Role

Claude acts as critical auditor and contract reviewer.

## Audit target

Review the Route B implementation lock before Codex begins implementation.

## Questions to answer

1. Does the proposed Route B scope preserve `photo_row -> event_row -> day_presence`?
2. Is there any hidden assumption equivalent to `one_excel_row_equals_one_visit`?
3. Which parts are GREEN, YELLOW, ORANGE or RED?
4. What exact implementation areas require cross-audit?
5. What evidence should Codex produce before PR review?
6. What rollback notes are required?
7. Is any DB/SQL apply implied or accidentally allowed?

## Required output

Persist findings as a versioned artifact or PR comment.

Do not execute DB, SQL, loader patch or destructive cleanup.
