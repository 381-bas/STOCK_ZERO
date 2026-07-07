# Flow Retrospective 012-013

## Verdict

The 013 workflow is adopted as useful for risk-bearing phases, with limits.

## What worked

- One objective, one branch, one PR.
- Claude found real issues before Codex implementation.
- Codex implemented inside scope.
- ChatGPT/Basti?n filtered Claude findings instead of blindly accepting them.
- Post-audit verified implementation and tests.
- No forbidden productive files were modified.
- Evidence became more falsifiable.

## What must not repeat

- Large static source packets when Claude can read the repo directly.
- New verdict vocabulary without taxonomy.
- Governance that repeats the same rules in several places.
- Full Claude ritual for low-risk changes.
- Long chains of validators without visible product value.

## Adopted corrections

- Use `governance/VERDICT_TAXONOMY.md` before RED/apply decisions.
- Use `governance/AGENT_WORKFLOW_RULES.md` to decide when Claude/Codex enter.
- Treat phase locks as the detailed phase contract.
- Keep ACTIVE_ORDER_LOCK and PROJECT_STATUS_INDEX as operational pointers/summaries where possible.
- Make the next phase deliver visible Control Gesti?n value, not only more validation infrastructure.

## Product visibility rule

After 012-013, the next unit of value must produce something a business user can inspect or use.

Route B remains GO_WITH_LIMITS and is not RED-authorized.
