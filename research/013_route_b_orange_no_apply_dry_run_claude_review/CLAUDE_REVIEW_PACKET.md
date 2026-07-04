# Claude Review Packet - 013 Route B ORANGE No-Apply Dry-Run Integration

Generated UTC: 2026-07-02T02:26:26+00:00

## Role

Act as an adversarial technical auditor for STOCK_ZERO / Route B. Your job is to find risks, missing tests, scope violations and weak assumptions before Codex implementation.

## Current phase

- Phase: FAST_REFORM_013_ROUTE_B_ORANGE_NO_APPLY_DRY_RUN_CLAUDE_REVIEW
- Mode: ORANGE_NO_APPLY_DRY_RUN
- Previous phase 012 verdict: GO_WITH_LIMITS
- ChatGPT direction: criterion, scope, synthesis and final decision
- Claude role: mandatory adversarial technical review and suggestions
- Codex role: implementation/tests/git/evidence only after review filter
- Bastian role: business validation and risk authorization

## Hard restrictions

The following are NOT authorized in 013:

- Supabase writes
- SQL apply / DDL against real DB
- data movement
- production cutover
- backfill execution
- productive loader modification
- active contract semantic modification
- activating Route B as productive source

## Required invariant

Route B must preserve this grain chain:

photo_row -> event_row -> day_presence

Critical failure mode to prevent:

one Excel photo row = one visit

## Protected compliance outputs

- EXIGIDAS
- VISITA
- VISITA_REALIZADA
- VISITA_REALIZADA_RAW
- VISITA_REALIZADA_CAP
- PENDIENTE
- ALERTA

## Questions you must answer

1. Does the current 013 scope preserve no-apply boundaries?
2. What is the most likely way Route B could accidentally alter the denominator?
3. What local dry-run evidence is mandatory before Codex implementation is accepted?
4. What tests are missing?
5. Should 013 implement a new dry-run validator, extend existing tests, or both?
6. Which files must remain untouched?
7. What would make this phase BLOCK instead of APPROVE_WITH_WARNINGS?
8. Which suggestions are inside 013 scope and which require a future RED/apply phase?

## Required output format

- Verdict: APPROVE / APPROVE_WITH_WARNINGS / BLOCK
- Top risks
- Required tests
- Scope violations if any
- Suggested implementation path
- What not to do
- Final recommendation for ChatGPT/Bastian/Codex

---

# Source excerpts

### governance/ACTIVE_ORDER_LOCK.json

```json
{
  "artifact": "ACTIVE_ORDER_LOCK",
  "project": "STOCK_ZERO",
  "status": "ACTIVE",
  "phase": "FAST_REFORM_013_ROUTE_B_ORANGE_NO_APPLY_DRY_RUN_CLAUDE_REVIEW",
  "base_main_commit": "e8421b9",
  "order": [
    "Open 013 as one verifiable ORANGE no-apply unit of value.",
    "Integrate Claude review as mandatory adversarial review before Codex implementation.",
    "Build or extend Route B dry-run/local integration only if it does not touch productive runtime.",
    "Preserve the grain chain photo_row -> event_row -> day_presence.",
    "Prevent one Excel photo row = one visit failure mode.",
    "Produce denominator reconciliation evidence for controlled local samples.",
    "Define implementation evidence, Claude scorecard and final GO/NO-GO/GO_WITH_LIMITS verdict.",
    "Do not execute productive writes, SQL apply, backfill, cutover or active contract semantic changes."
  ],
  "allowed_now": [
    "update governance active phase to 013",
    "record 012 as merged to main",
    "create 013 phase lock",
    "create research/013_route_b_orange_no_apply_dry_run_claude_review artifacts",
    "prepare Claude review packet",
    "store Claude review and ChatGPT scorecard",
    "inspect existing Route B scripts/tests/contracts/research",
    "add or modify local tests if they do not alter productive runtime behavior",
    "create new no-apply/dry-run validation script if no productive loader or active contract is modified",
    "run local pytest and dry-run validation commands"
  ],
  "forbidden_now": [
    "modify scripts/load_control_gestion_raw_v17.py",
    "modify scripts/load_kpione2_photo_from_excel.py as productive implementation",
    "modify active contract semantics",
    "activate Route B as productive source",
    "DB apply",
    "SQL apply against Supabase real",
    "Supabase writes",
    "data movement",
    "productive compliance view changes",
    "destructive cleanup",
    "production cutover",
    "backfill execution",
    "git add ."
  ],
  "close_condition": [
    "013 phase lock exists",
    "Claude review packet exists",
    "Claude adversarial review recorded",
    "Claude scorecard recorded by ChatGPT/Bastian criteria",
    "dry-run/local integration evidence recorded if implemented",
    "denominator reconciliation evidence recorded for controlled local samples",
    "GO/NO-GO/GO_WITH_LIMITS final verdict emitted",
    "no Supabase writes",
    "no SQL apply",
    "no productive loader modification",
    "no active contract semantic change",
    "no backfill or production cutover"
  ],
  "next_allowed_phase_after_close": "ROUTE_B_RED_APPLY_GATE_OR_CONTROL_GESTION_VISIBLE_PRODUCT_SLICE",
  "active_branch": "lab/FAST_REFORM_013_route_b_orange_no_apply_dry_run_claude_review",
  "updated_at_utc": "2026-07-02T02:19:20+00:00",
  "closeout": null
}

```


### governance/PROJECT_STATUS_INDEX.json

```json
{
  "artifact": "PROJECT_STATUS_INDEX",
  "project": "STOCK_ZERO",
  "status": "ACTIVE",
  "source_of_truth": "origin/main",
  "created_from_main_commit": "a48d423",
  "last_closed_phases": [
    {
      "phase": "FAST_REFORM_009F_LOADER_STRUCTURE_VALIDATION",
      "merge_pr": 19,
      "main_merge_commit": "df04e17",
      "included_commit": "4961873",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_009F_GOVERNANCE_PROMOTION",
      "merge_pr": 20,
      "main_merge_commit": "a48d423",
      "included_commit": "7cfba7a",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_009F_BOOTSTRAP_PROTOCOL_AND_THREAD_CLOSEOUT",
      "merge_pr": 21,
      "main_merge_commit": "5148b5d",
      "included_commit": "0f703ed",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_009F_REPO_ORGANIZATION_CLEANUP",
      "merge_pr": 22,
      "main_merge_commit": "a91fc60",
      "included_commit": "bd56f51",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_009G_AGENT_AUTHORITY_AND_ROUTE_B_LOCK",
      "merge_pr": 24,
      "main_merge_commit": "4a87d12",
      "included_commit": "486baf5",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_009G_AGENT_AUTHORITY_ACTIVATION",
      "merge_pr": 25,
      "main_merge_commit": "36654d1",
      "included_commit": "6b337d9",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_010A_ROUTE_B_IMPLEMENTATION_LOCK",
      "merge_pr": 26,
      "main_merge_commit": "19b3204",
      "included_commit": "8b17ae7",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_010B_ROUTE_B_CODEX_IMPLEMENTATION_AFTER_CLAUDE_AUDIT",
      "merge_pr": 27,
      "main_merge_commit": "c56dcfb",
      "included_commits": [
        "d5eab9a",
        "4d5800b"
      ],
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_010C_ROUTE_B_REVIEW_AND_DRY_RUN_VALIDATION",
      "merge_pr": 28,
      "main_merge_commit": "d834055",
      "included_commits": [
        "3df063b",
        "0c11930"
      ],
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_010D_ROUTE_B_POST_MERGE_CLOSE_AND_SOURCE_ROW_NUMBER_LOCK",
      "merge_pr": 29,
      "main_merge_commit": "e80d0f7",
      "included_commit": "49807ca",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH",
      "merge_pr": 30,
      "main_merge_commit": "3cd0e9a",
      "included_commits": [
        "cd33e66",
        "ae1b018"
      ],
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_010F_ROUTE_B_SOURCE_ROW_NUMBER_POST_AUDIT_CLOSE",
      "merge_pr": 31,
      "main_merge_commit": "904205a",
      "included_commit": "34d5e7a",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_010G_ROUTE_B_INTEGRATION_PLANNING_NO_APPLY",
      "merge_pr": 32,
      "main_merge_commit": "11295f7",
      "included_commit": "bce93dc",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_010H_ROUTE_B_INTEGRATION_PLAN_CROSS_AUDIT",
      "merge_pr": 33,
      "main_merge_commit": "23bab44",
      "included_commit": "cb2b1b6",
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_011_EXECUTION_MODEL_AND_KERNEL_REALIGNMENT",
      "merge_pr": 34,
      "main_merge_commit": "e4a8e94",
      "included_commits": [
        "853f3a7",
        "c6b6c2d"
      ],
      "status": "MERGED_TO_MAIN"
    },
    {
      "phase": "FAST_REFORM_012_ROUTE_B_READINESS_GATES_NO_APPLY",
      "merge_pr": 35,
      "main_merge_commit": "e8421b9",
      "included_commits": [
        "e2fb111",
        "f83c6e0",
        "d30f82a"
      ],
      "status": "MERGED_TO_MAIN"
    }
  ],
  "active_phase": "FAST_REFORM_013_ROUTE_B_ORANGE_NO_APPLY_DRY_RUN_CLAUDE_REVIEW",
  "active_branch": "lab/FAST_REFORM_013_route_b_orange_no_apply_dry_run_claude_review",
  "next_allowed_phase_after_close": "ROUTE_B_RED_APPLY_GATE_OR_CONTROL_GESTION_VISIBLE_PRODUCT_SLICE",
  "bootstrap_phrases": [
    "BOOTSTRAP STOCK_ZERO",
    "BOOTSTRAP STOCK_ZERO DESDE MAIN: lee PROJECT_STATUS_INDEX, ACTIVE_ORDER_LOCK, EXECUTION_DOCTRINE, AGENT_ACCESS_POLICY y contratos activos antes de proponer pasos."
  ],
  "must_read_on_bootstrap": [
    "governance/PROJECT_STATUS_INDEX.json",
    "governance/ACTIVE_ORDER_LOCK.json",
    "governance/EXECUTION_DOCTRINE.md",
    "governance/AGENT_ACCESS_POLICY.json",
    "contracts/control_gestion/kpione2_photo_export_contract_v1.json"
  ],
  "active_contracts": [
    "contracts/control_gestion/kpione2_photo_export_contract_v1.json"
  ],
  "thread_summaries": [
    "governance/thread_summaries/GITHUB_ISSUE_18_THREAD_SYNTHESIS.md",
    "governance/thread_summaries/PR_19_20_CLOSEOUT.md"
  ],
  "forbidden_until_close": [
    "modify scripts/load_control_gestion_raw_v17.py",
    "modify scripts/load_kpione2_photo_from_excel.py as productive implementation",
    "modify active contract semantics",
    "activate Route B as productive source",
    "DB apply",
    "SQL apply against Supabase real",
    "Supabase writes",
    "data movement",
    "productive compliance view changes",
    "destructive cleanup",
    "production cutover",
    "backfill execution",
    "git add ."
  ],
  "rule": "Chat previews do not govern execution. Operational rules must be promoted to versioned repo artifacts through commit, PR and merge.",
  "cleanup_artifacts": [
    "governance/cleanup/009F_repo_organization_cleanup_manifest.json",
    "governance/cleanup/009F_repo_organization_cleanup_summary.md",
    "governance/cleanup/009F_optional_branch_delete_gate_result.json",
    "governance/cleanup/009F_cleanup_closeout_summary.md"
  ],
  "optional_cleanup_gates_completed": [
    "OPTIONAL_BRANCH_DELETE_GATE_FOR_SAFE_009F_BRANCHES"
  ],
  "agent_authority_artifacts": [
    "governance/AGENT_AUTHORITY_MATRIX_V2.json",
    "governance/agent_workflow/DELEGATED_AGENT_WORKFLOW.md",
    "governance/phase_locks/FAST_REFORM_009G_agent_authority_and_route_b_lock.json",
    "research/009G_agent_authority/README.md"
  ],
  "agent_authority_status": {
    "matrix": "governance/AGENT_AUTHORITY_MATRIX_V2.json",
    "status": "ACTIVE",
    "activated_by_pr": 25,
    "activated_at_main_commit": "36654d1",
    "authority_model": [
      "GREEN",
      "YELLOW",
      "ORANGE",
      "RED"
    ]
  },
  "route_b_artifacts": [
    "governance/phase_locks/FAST_REFORM_010G_route_b_integration_planning_no_apply.json",
    "research/010G_route_b_integration_planning_no_apply/README.md",
    "research/010G_route_b_integration_planning_no_apply/ROUTE_B_INTEGRATION_PLAN.md",
    "research/010G_route_b_integration_planning_no_apply/RISK_MATRIX.md",
    "research/010G_route_b_integration_planning_no_apply/VALIDATION_GATES.md"
  ],
  "route_b_source_row_number_lock": {
    "status": "SATISFIED_IN_DRY_RUN_APPROVED",
    "phase": "FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH",
    "merge_pr": 30,
    "main_merge_commit": "3cd0e9a",
    "audit": "research/010E_route_b_source_row_number/CLAUDE_POST_AUDIT.md",
    "still_required_before_real_writes": true
  },
  "route_b_integration_planning": {
    "status": "CLOSED_AFTER_010H_APPROVE_WITH_WARNINGS",
    "phase": "FAST_REFORM_010H_ROUTE_B_INTEGRATION_PLAN_CROSS_AUDIT",
    "main_merge_commit": "23bab44",
    "db_apply": false,
    "sql_apply": false,
    "productive_loader_modification": false,
    "future_orange_required": true,
    "future_red_required_for_apply": true,
    "missing_gates_before_implementation": [
      "compliance denominator reconciliation",
      "forward-only/backfill scope decision",
      "rollback and production cutover gate"
    ]
  },
  "active_phase_started_from_main": "e8421b9",
  "updated_at_utc": "2026-07-02T02:19:20+00:00",
  "execution_model_reform": {
    "status": "ACTIVE_IN_011",
    "reason": "010 series protected the project but over-fragmented administrative PRs.",
    "new_rule": "1 operational objective = 1 branch = 1 PR; use internal commits for subtasks.",
    "kernel_rule": "Git repo KERNEL is versioned source of truth; ChatGPT Project sources are operational context.",
    "divergence_alert_rule": "If Git KERNEL/governance differs from ChatGPT Project sources, alert the divergence and treat Git as authoritative."
  },
  "route_b_readiness_012": {
    "status": "MERGED_TO_MAIN",
    "purpose": "Convert Route B unresolved warnings into gates before implementation.",
    "required_gates": [
      "compliance denominator reconciliation",
      "forward-only/backfill scope decision",
      "rollback and production cutover gate"
    ],
    "productive_writes_authorized": false,
    "productive_loader_modification_authorized": false,
    "sql_apply_authorized": false,
    "supabase_writes_authorized": false,
    "verdict": "GO_WITH_LIMITS",
    "closeout": "research/012_route_b_readiness_gates_no_apply/CLOSEOUT.md",
    "merge_pr": 35,
    "main_merge_commit": "e8421b9"
  },
  "kernel_volatility_policy": {
    "status": "DIRECTIVE_ALIGNMENT",
    "rule": "KERNEL base is directive context; Git/GitHub keeps exact PR/hash/microhistory.",
    "repo_policy": "Do not update KERNEL base for every PR; promote only doctrine, contract, source-of-truth, strategic direction or high-impact risk changes."
  },
  "route_b_orange_013": {
    "status": "ACTIVE",
    "phase": "FAST_REFORM_013_ROUTE_B_ORANGE_NO_APPLY_DRY_RUN_CLAUDE_REVIEW",
    "base_main_commit": "e8421b9",
    "claude_review_required": true,
    "codex_implementation_allowed_after_claude_review": true,
    "mode": "ORANGE_NO_APPLY_DRY_RUN",
    "productive_writes_authorized": false,
    "productive_loader_modification_authorized": false,
    "sql_apply_authorized": false,
    "supabase_writes_authorized": false,
    "backfill_authorized": false,
    "production_cutover_authorized": false,
    "required_controls": [
      "Claude adversarial review",
      "Claude scorecard",
      "photo_row -> event_row -> day_presence invariant",
      "denominator reconciliation evidence",
      "rollback/cutover requirements maintained as pre-production blockers"
    ]
  }
}

```


### governance/phase_locks/FAST_REFORM_013_route_b_orange_no_apply_dry_run_claude_review.json

```json
{
  "artifact": "PHASE_LOCK",
  "project": "STOCK_ZERO",
  "phase": "FAST_REFORM_013_ROUTE_B_ORANGE_NO_APPLY_DRY_RUN_CLAUDE_REVIEW",
  "status": "ACTIVE",
  "branch": "lab/FAST_REFORM_013_route_b_orange_no_apply_dry_run_claude_review",
  "base_main_commit": "e8421b9",
  "created_at_utc": "2026-07-02T02:19:20+00:00",
  "risk_level": "ORANGE_NO_APPLY",
  "objective": "Validate Route B no-apply/dry-run integration with mandatory Claude adversarial review before Codex implementation.",
  "agent_model": {
    "ChatGPT": "direction, architecture, final synthesis and scope control",
    "Claude": "mandatory adversarial technical review and suggestions",
    "Codex": "implementation, tests, git and evidence after review filter",
    "Bastian": "business validation and risk authorization"
  },
  "allowed": [
    "update governance active phase to 013",
    "record 012 as merged to main",
    "create 013 phase lock",
    "create research/013_route_b_orange_no_apply_dry_run_claude_review artifacts",
    "prepare Claude review packet",
    "store Claude review and ChatGPT scorecard",
    "inspect existing Route B scripts/tests/contracts/research",
    "add or modify local tests if they do not alter productive runtime behavior",
    "create new no-apply/dry-run validation script if no productive loader or active contract is modified",
    "run local pytest and dry-run validation commands"
  ],
  "forbidden": [
    "modify scripts/load_control_gestion_raw_v17.py",
    "modify scripts/load_kpione2_photo_from_excel.py as productive implementation",
    "modify active contract semantics",
    "activate Route B as productive source",
    "DB apply",
    "SQL apply against Supabase real",
    "Supabase writes",
    "data movement",
    "productive compliance view changes",
    "destructive cleanup",
    "production cutover",
    "backfill execution",
    "git add ."
  ],
  "close_condition": [
    "013 phase lock exists",
    "Claude review packet exists",
    "Claude adversarial review recorded",
    "Claude scorecard recorded by ChatGPT/Bastian criteria",
    "dry-run/local integration evidence recorded if implemented",
    "denominator reconciliation evidence recorded for controlled local samples",
    "GO/NO-GO/GO_WITH_LIMITS final verdict emitted",
    "no Supabase writes",
    "no SQL apply",
    "no productive loader modification",
    "no active contract semantic change",
    "no backfill or production cutover"
  ],
  "success_metrics": [
    "time to first useful commit",
    "number of command corrections",
    "files touched outside scope equals zero",
    "valid Claude findings incorporated or explicitly rejected",
    "tests or dry-run evidence improved",
    "final verdict is auditable"
  ]
}

```


### research/012_route_b_readiness_gates_no_apply/ROUTE_B_READINESS_GATES.md

```markdown
# Route B Readiness Gates - No Apply

## Context

Phase: FAST_REFORM_012_ROUTE_B_READINESS_GATES_NO_APPLY
Branch: lab/FAST_REFORM_012_route_b_readiness_gates_no_apply
HEAD: e2fb111
Generated UTC: 2026-07-02T01:22:04Z

## Purpose

Convert unresolved Route B warnings from prior planning/audit phases into explicit readiness gates before any implementation that can affect productive runtime, active loader behavior, active contracts, SQL apply, Supabase writes, data movement, backfill or cutover.

## Evidence used

- research/012_route_b_readiness_gates_no_apply/SOURCE_INVENTORY.md
- research/012_route_b_readiness_gates_no_apply/EXISTING_TEST_RUN.md
- tests/test_kpione2_photo_grain.py
- tests/test_cg_route_weekly_local_lab.py

## Test evidence

Command:

python -m pytest tests/test_kpione2_photo_grain.py tests/test_cg_route_weekly_local_lab.py -q

Observed result:

19 passed, 1 skipped, 10 subtests passed
Exit code: 0
Runtime: approximately 2m24s.

Interpretation:

- Existing local grain/control tests pass.
- This supports readiness analysis.
- This does not authorize productive writes, productive loader changes, SQL apply, Supabase writes, backfill or cutover.

---

# Gate 1 - Compliance denominator reconciliation

## Question

Can Route B proceed toward implementation without changing the active compliance denominator or active fulfillment semantics?

## Protected metrics

Route B must preserve the active meaning of:

- EXIGIDAS
- VISITA
- VISITA_REALIZADA
- VISITA_REALIZADA_RAW
- VISITA_REALIZADA_CAP
- PENDIENTE
- ALERTA

## Required invariant

Route B photo data must preserve the grain chain:

photo_row -> event_row -> day_presence

Compliance must continue operating at day-presence semantics, not photo-row semantics.

## Explicit risk

The critical failure mode is:

one Excel photo row = one visit

That would inflate evidence and distort compliance.

## Readiness status

PASS_FOR_NO_APPLY_READINESS

## Evidence

The existing local tests passed. The source inventory confirms the active contract and prior audits identify the same grain risk.

## Condition for future ORANGE implementation

Future implementation must include before/after reconciliation showing no unintended change in weekly denominator and fulfillment outputs for selected controlled samples.

Minimum future comparison:

before Route B active path vs after Route B active path
by semana_inicio, cod_rt, cliente_norm
metrics: EXIGIDAS/VISITA, VISITA_REALIZADA_RAW, VISITA_REALIZADA_CAP, PENDIENTE, ALERTA

## Blocker condition

Any implementation that maps photo rows directly to visits is NO_GO.

---

# Gate 2 - Forward-only / backfill scope decision

## Decision

FORWARD_ONLY_DEFAULT

## Rule

Route B implementation should enter as forward-only by default.

## Reason

Backfill can alter historical compliance interpretation and must not be mixed with initial implementation.

## Backfill classification

Historical backfill is a separate higher-risk phase.

Required before any backfill:

- explicit business reason
- affected date range
- expected metric deltas
- rollback plan
- Bastian approval
- separate PR/fase if risk scope changes
- no silent overwrite of historical interpretation

## Readiness status

PASS_WITH_LIMIT

## Condition for future ORANGE implementation

Implementation may proceed only as forward-only unless a separate authorization explicitly promotes backfill.

---

# Gate 3 - Rollback and production cutover

## Decision

ROLLBACK_REQUIRED_BEFORE_PRODUCTIVE_ACTIVATION

## Required cutover model

Future implementation must define:

- activation method
- deactivation method
- default inactive/safe state
- before/after comparison
- evidence required to continue
- evidence requiring rollback
- owner of final productive authorization

## Required rollback model

At minimum:

code rollback = git revert
DB rollback = committed no-apply rollback SQL before apply phase
runtime rollback = feature flag/source order exclusion/default inactive path
data rollback = not applicable unless data movement/backfill is explicitly authorized

## Readiness status

PASS_WITH_LIMIT

## Blocker condition

No productive cutover can occur without a committed rollback path and explicit RED authorization.

---

# Overall readiness verdict

GO_WITH_LIMITS

## Meaning

Route B may proceed to a later controlled implementation phase only if the next phase respects these limits:

- no automatic backfill
- no direct photo-row-to-visit mapping
- no productive writes without explicit authorization
- no modification of productive loader without ORANGE scope
- no SQL/Supabase apply without RED authorization
- denominator reconciliation must be evidenced before productive activation
- rollback/cutover must exist before productive activation

## Recommended next phase

FAST_REFORM_013_ROUTE_B_ORANGE_IMPLEMENTATION_NO_APPLY_OR_DRY_RUN_INTEGRATION

Only after 012 is reviewed and merged.

## Not authorized by this verdict

- SQL apply
- Supabase writes
- productive loader modification
- active contract semantic modification
- production cutover
- data movement
- historical backfill

```


### contracts/control_gestion/kpione2_photo_export_contract_v1.json

```json
{
  "artifact": "kpione2_photo_export_contract_v1",
  "project": "STOCK_ZERO",
  "module": "CONTROL_GESTION",
  "status": "ACTIVE",
  "created_after_phase": "FAST_REFORM_009F_LOADER_STRUCTURE_VALIDATION",
  "source_export": {
    "file_pattern": "photo-excel-admin_*.xlsx",
    "sheet": "Fotos",
    "example_file": "data/photo-excel-admin_1782440454408.xlsx",
    "example_sha256": "1e345f7bdbad142ebff41472ffc7917e31412a81d88e3f99bd0b0248c60fb180"
  },
  "grain_contract": {
    "input_grain": "photo_row",
    "normalized_grain": "event_row",
    "compliance_grain": "day_presence",
    "forbidden_assumption": "one_excel_row_equals_one_visit"
  },
  "event_identity": [
    "ID",
    "SP Item ID"
  ],
  "denominator_mapping": {
    "Codigo Local": "cod_rt",
    "Marca": "cliente_norm"
  },
  "event_stable_hash_columns": [
    "ID",
    "SP Item ID",
    "Holding",
    "Subcadena",
    "Codigo Local",
    "Marca",
    "Local",
    "Direccion",
    "Reponedor",
    "Fecha",
    "Comentarios"
  ],
  "excluded_from_stable_hash": [
    {
      "column": "Foto Nº/Total",
      "classification": "PHOTO_LEVEL",
      "reason": "photo sequence/count denominator"
    },
    {
      "column": "Link Foto",
      "classification": "PHOTO_LEVEL",
      "reason": "per-photo URL"
    },
    {
      "column": "Hora",
      "classification": "PHOTO_LEVEL",
      "reason": "varies within event ID"
    },
    {
      "column": "Tipo de Tarea",
      "classification": "PHOTO_LEVEL_TASK_LABEL",
      "reason": "task label varies by photo within same visit event"
    },
    {
      "column": "Fecha de subida",
      "classification": "AUDIT_ONLY_EXCLUDED_BY_POLICY",
      "reason": "upload metadata must not affect visit identity hash"
    }
  ],
  "derived_fields": {
    "event_key": "trim(ID)",
    "week_start": "monday_start_from_Fecha",
    "n_fotos_calculado": "max(parse_total(Foto Nº/Total)) per ID",
    "photo_rows": "count rows per ID",
    "tipos_de_tarea": "sorted distinct list of Tipo de Tarea per ID",
    "hora_primera_foto": "min(Hora) per ID"
  },
  "blocking_validations": [
    "photo_rows_match",
    "distinct_event_ids_match",
    "fecha_min_match",
    "fecha_max_match",
    "required_dates_match",
    "expected_weeks_present_match",
    "week_start_contract_pass",
    "no_null_event_id_rows",
    "no_null_fecha_rows",
    "no_null_n_fotos_calculado_rows",
    "no_event_ids_multi_fecha",
    "no_event_ids_multi_week",
    "no_row_count_n_fotos_mismatch_events",
    "no_real_content_conflict_event_ids",
    "optional_columns_classified"
  ],
  "009F_evidence": {
    "verdict": "PASS_LOADER_CONTRACT",
    "photo_rows": 37908,
    "distinct_event_ids": 5892,
    "fecha_min": "2026-06-20",
    "fecha_max": "2026-06-24",
    "real_content_conflict_event_ids": 0,
    "row_count_n_fotos_mismatch_events": 0,
    "week_start_contract_pass": true,
    "db_apply": false,
    "file_movement": false
  }
}

```


### tests/test_kpione2_photo_grain.py

```python
import argparse
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import load_kpione2_photo_from_excel as loader


def fixture_contract(expected: dict) -> dict:
    return {
        "status": "ACTIVE",
        "grain_contract": dict(loader.GRAIN_CONTRACT),
        "009F_evidence": dict(expected),
    }


def sample_photo_df(comment_conflict: bool = False) -> pd.DataFrame:
    columns = [
        "ID",
        "SP Item ID",
        "Holding",
        "Subcadena",
        "Codigo Local",
        "Marca",
        "Local",
        "Direccion",
        "Reponedor",
        "Fecha",
        "Fecha de subida",
        "Hora",
        "Tipo de Tarea",
        "Foto N/Total",
        "Comentarios",
        "Link Foto",
    ]
    rows = [
        ["E1", "SP1", "H", "S", "100", "Marca A", "Local A", "Dir", "Repo", "2026-06-20", "2026-06-20 09:01", "09:00", "A", "1/3", "OK", "https://x/1"],
        ["E1", "SP1", "H", "S", "100", "Marca A", "Local A", "Dir", "Repo", "2026-06-20", "2026-06-20 09:02", "09:01", "B", "2/3", "OK", "https://x/2"],
        ["E1", "SP1", "H", "S", "100", "Marca A", "Local A", "Dir", "Repo", "2026-06-20", "2026-06-20 09:03", "09:02", "C", "3/3", "OK", "https://x/3"],
        ["E2", "SP2", "H", "S", "100", "Marca A", "Local A", "Dir", "Repo", "2026-06-20", "2026-06-20 10:01", "10:00", "A", "1/2", "OK", "https://x/4"],
        ["E2", "SP2", "H", "S", "100", "Marca A", "Local A", "Dir", "Repo", "2026-06-20", "2026-06-20 10:02", "10:01", "B", "2/2", "OK", "https://x/5"],
        ["E3", "SP3", "H", "S", "200", "Marca B", "Local B", "Dir", "Repo", "2026-06-21", "2026-06-21 11:01", "11:00", "A", "1/1", "OK", "https://x/6"],
    ]
    if comment_conflict:
        rows[1][14] = "DIFFERENT"
    return pd.DataFrame(rows, columns=columns)


class Kpione2PhotoGrainTests(unittest.TestCase):
    def _payload(self, df: pd.DataFrame, expected: dict | None = None) -> dict:
        expected = expected or {
            "photo_rows": 6,
            "distinct_event_ids": 3,
            "fecha_min": "2026-06-20",
            "fecha_max": "2026-06-21",
        }
        return loader.analyze_photo_dataframe(
            df,
            contract=fixture_contract(expected),
            expected=expected,
            source_file="fixture.xlsx",
            source_file_sha256="A" * 64,
            sheet_name="Fotos",
        )

    def test_photo_rows_are_grouped_to_event_rows(self):
        payload = self._payload(sample_photo_df())
        self.assertEqual(payload["metrics"]["photo_rows"], 6)
        self.assertEqual(payload["metrics"]["distinct_event_ids"], 3)
        self.assertEqual(payload["metrics"]["event_rows"], 3)
        self.assertTrue(payload["flags"]["forbidden_assumption_rejected"])
        self.assertEqual(payload["verdict"], "PASS_ROUTE_B_DRY_RUN")

    def test_source_row_number_maps_each_photo_row_to_excel_origin(self):
        payload = self._payload(sample_photo_df())
        metrics = payload["metrics"]
        traceability = payload["photo_row_traceability"]

        self.assertEqual(metrics["source_row_number_min"], 2)
        self.assertEqual(metrics["source_row_number_max"], 7)
        self.assertEqual(metrics["source_row_number_distinct"], 6)
        self.assertEqual(metrics["source_row_number_null_rows"], 0)
        self.assertTrue(payload["flags"]["source_row_number_present"])
        self.assertTrue(payload["flags"]["source_row_number_complete"])
        self.assertTrue(payload["flags"]["source_row_number_unique"])
        self.assertTrue(payload["flags"]["source_row_number_matches_excel_rows"])
        self.assertEqual(traceability["photo_rows_mapped"], 6)
        self.assertEqual(traceability["event_identity"], ["ID", "SP Item ID"])
        self.assertFalse(traceability["event_identity_replaced"])
        self.assertEqual(
            [row["source_row_number"] for row in traceability["sample_rows"]],
            [2, 3, 4, 5, 6, 7],
        )

    def test_source_row_number_is_stable_within_workbook_sheet(self):
        original = sample_photo_df()
        reindexed = original.copy()
        reindexed.index = [90, 70, 50, 30, 10, 0]

        first = self._payload(original)["photo_row_traceability"]
        second = self._payload(reindexed)["photo_row_traceability"]

        self.assertEqual(first["trace_manifest_sha256"], second["trace_manifest_sha256"])
        self.assertEqual(first["sample_rows"], second["sample_rows"])

    def test_day_presence_is_binary_not_event_count(self):
        payload = self._payload(sample_photo_df())
        self.assertEqual(payload["metrics"]["day_presence_rows"], 2)
        self.assertEqual(payload["metrics"]["max_events_per_day_presence"], 2)
        self.assertEqual(payload["day_presence_summary"]["binary_presence_values"], [1])
        self.assertTrue(payload["flags"]["day_presence_is_binary"])

    def test_photo_level_columns_are_excluded_from_event_hash(self):
        payload = self._payload(sample_photo_df())
        self.assertEqual(payload["metrics"]["real_content_conflict_event_ids"], 0)
        self.assertIn("Hora", payload["column_contract"]["photo_level_columns_excluded_from_hash"])
        self.assertIn("Tipo de Tarea", payload["column_contract"]["photo_level_columns_excluded_from_hash"])
        self.assertIn("Link Foto", payload["column_contract"]["photo_level_columns_excluded_from_hash"])

    def test_event_stable_column_conflict_is_flagged(self):
        payload = self._payload(sample_photo_df(comment_conflict=True))
        self.assertEqual(payload["metrics"]["real_content_conflict_event_ids"], 1)
        self.assertFalse(payload["flags"]["no_real_content_conflict_event_ids"])
        self.assertEqual(payload["verdict"], "WARN_REVIEW_REQUIRED")

    def test_apply_flag_is_blocked(self):
        args = argparse.Namespace(apply=True)
        with self.assertRaises(loader.LoaderUsageError) as ctx:
            loader.validate_cli_args(args)
        self.assertEqual(ctx.exception.code, "apply_not_supported_in_route_b_dry_run")

    def test_cli_writes_json_without_db(self):
        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            excel = tmp / "photo.xlsx"
            contract = tmp / "contract.json"
            out = tmp / "out.json"
            expected = {
                "photo_rows": 6,
                "distinct_event_ids": 3,
                "fecha_min": "2026-06-20",
                "fecha_max": "2026-06-21",
            }
            sample_photo_df().to_excel(excel, sheet_name="Fotos", index=False)
            contract.write_text(json.dumps(fixture_contract(expected)), encoding="utf-8")
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = loader.main(
                    [
                        "--dry-run",
                        "--excel",
                        str(excel),
                        "--contract",
                        str(contract),
                        "--json-out",
                        str(out),
                    ]
                )
            self.assertEqual(code, 0)
            payload = json.loads(out.read_text(encoding="utf-8"))
            self.assertFalse(payload["db_apply"])
            self.assertFalse(payload["sql_apply"])
            self.assertFalse(payload["writes_executed"])
            self.assertFalse(payload["productive_loader_touched"])
            self.assertEqual(payload["metrics"]["source_row_number_min"], 2)
            self.assertEqual(payload["metrics"]["source_row_number_max"], 7)
            self.assertEqual(payload["photo_row_traceability"]["photo_rows_mapped"], 6)

    def test_real_workbook_matches_required_010c_evidence(self):
        source = ROOT / "data" / "photo-excel-admin_1782440454408.xlsx"
        if not source.exists():
            self.skipTest("route b source workbook is not present")
        payload = loader.build_dry_run_payload(
            source,
            sheet_name="Fotos",
            contract_path=ROOT / "contracts" / "control_gestion" / "kpione2_photo_export_contract_v1.json",
        )
        self.assertEqual(payload["metrics"]["photo_rows"], 37908)
        self.assertEqual(payload["metrics"]["distinct_event_ids"], 5892)
        self.assertEqual(payload["metrics"]["fecha_min"], "2026-06-20")
        self.assertEqual(payload["metrics"]["fecha_max"], "2026-06-24")
        self.assertEqual(payload["metrics"]["source_row_number_min"], 2)
        self.assertEqual(payload["metrics"]["source_row_number_max"], 37909)
        self.assertEqual(payload["metrics"]["source_row_number_distinct"], 37908)
        self.assertEqual(payload["metrics"]["source_row_number_null_rows"], 0)
        self.assertEqual(payload["photo_row_traceability"]["photo_rows_mapped"], 37908)
        self.assertFalse(payload["photo_row_traceability"]["event_identity_replaced"])
        self.assertFalse(payload["db_apply"])
        self.assertFalse(payload["sql_apply"])
        self.assertFalse(payload["productive_loader_touched"])
        self.assertEqual(payload["verdict"], "PASS_ROUTE_B_DRY_RUN")

    def test_sql_files_are_review_only(self):
        ddl = (ROOT / "sql" / "15_kpione2_photo_raw_ddl.sql").read_text(encoding="utf-8")
        rollback = (ROOT / "sql" / "16_kpione2_photo_raw_ddl_rollback.sql").read_text(encoding="utf-8")
        self.assertTrue(ddl.startswith("-- NO APPLY"))
        self.assertTrue(rollback.startswith("-- NO APPLY"))
        self.assertIn("create table if not exists cg_raw.kpione2_photo_raw", ddl)
        self.assertIn("drop table if exists cg_raw.kpione2_photo_raw", rollback)

    def test_new_loader_does_not_import_productive_loader_or_db_clients(self):
        source = (ROOT / "scripts" / "load_kpione2_photo_from_excel.py").read_text(encoding="utf-8")
        self.assertNotIn("import load_control_gestion_raw_v17", source)
        self.assertNotIn("from load_control_gestion_raw_v17", source)
        self.assertNotIn("psycopg2", source)
        self.assertNotIn("sqlalchemy", source.lower())
        self.assertNotIn("DB_URL", source)


if __name__ == "__main__":
    unittest.main()

```


### tests/test_cg_route_weekly_local_lab.py

```python
import json
import re
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import cg_route_weekly_local_lab as lab
import load_ruta_rutero_from_excel as loader


class LocalLabSafetyTests(unittest.TestCase):
    def test_input_file_code_is_contract_safe(self):
        self.assertEqual(lab.INPUT_FILE_CODE, "DB_GLOBAL_INVENTARIO_XLSX")
        self.assertRegex(lab.INPUT_FILE_CODE, r"^[A-Z][A-Z0-9_:-]{0,119}$")
        self.assertIsNotNone(re.fullmatch(r"^[A-Z][A-Z0-9_:-]{0,119}$", lab.INPUT_FILE_CODE))
        for forbidden in (".", "/", "\\", " ", "C:", "Users"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, lab.INPUT_FILE_CODE)

    def test_platform_008_observation_uses_technical_input_code(self):
        import sz_load_observation as obs

        seen = {}

        def fake_run(argv):
            if argv[0] == "draft":
                phase_json = Path(argv[argv.index("--phase-json") + 1])
                phase_payload = json.loads(phase_json.read_text(encoding="utf-8"))
                draft = phase_payload["observation_draft"]
                seen["draft"] = draft
                print(
                    json.dumps(
                        {
                            "observation_id": "OBS_TEST",
                            "input_file_name": draft["input_file_name"],
                            "input_file_sha256": draft["input_file_sha256"],
                            "anomaly_label": draft["anomaly_label"],
                            "implementation_authorized": draft["implementation_authorized"],
                        }
                    )
                )
                return 0
            if argv[0] == "validate":
                print(json.dumps({"validate": "ok", "observation_id": "OBS_TEST", "record": {}}))
                return 0
            raise AssertionError(f"unexpected argv: {argv}")

        lab_summary = {
            "cg005j": {
                "workbook_sha256": "A" * 64,
                "schema_signature": "B" * 64,
                "snapshot_a_rows": 10,
                "exact_duplicate_excess": 1,
                "history_rows": 11,
                "source_check": "ok",
            },
            "cg005k": {
                "snapshot_b": {
                    "synthetic_highs": 1,
                    "removed_logical_grains": 2,
                    "changed_responsables": 2,
                    "changed_frequency_or_days": 1,
                }
            },
        }
        with tempfile.TemporaryDirectory() as raw:
            with mock.patch.object(obs, "register_test_input_root", lambda _path: None):
                with mock.patch.object(obs, "run", side_effect=fake_run):
                    result = lab.run_platform_008(Path(raw), lab_summary)

        draft = seen["draft"]
        self.assertEqual(draft["input_file_name"], lab.INPUT_FILE_CODE)
        self.assertNotEqual(draft["input_file_name"], "DB_GLOBAL_INVENTARIO.xlsx")
        self.assertEqual(len(draft["input_file_sha256"]), 64)
        self.assertEqual(draft["input_file_sha256"], "A" * 64)
        self.assertTrue(result["candidate_validated"])
        self.assertTrue(result["ledger_unchanged"])
        self.assertEqual(result["input_file_name"], lab.INPUT_FILE_CODE)
        self.assertEqual(result["anomaly_label"], "UNREVIEWED")
        self.assertFalse(result["implementation_authorized"])
        self.assertFalse(result["ledger_write_executed"])
        self.assertTrue(result["temporary_candidate_deleted"])

    def test_loopback_dsn_is_allowed(self):
        info = lab.parse_loopback_dsn("postgresql://postgres@127.0.0.1:55433/stock_zero_cg005_lab?sslmode=disable")
        self.assertEqual(info.host, "127.0.0.1")
        self.assertEqual(info.database, "stock_zero_cg005_lab")
        self.assertEqual(info.sslmode, "disable")

    def test_remote_dsn_is_rejected(self):
        for dsn in (
            "postgresql://user@example.com/db",
            "postgresql://user@db.supabase.co/db",
            "postgresql://user@aws-prod.example.com/db",
            "postgresql://user@10.0.0.5/db",
        ):
            with self.subTest(dsn=dsn):
                with self.assertRaises(lab.LabError):
                    lab.parse_loopback_dsn(dsn)

    def test_database_dsn_rewrite_preserves_query(self):
        dsn = lab.dsn_for_database("postgresql://postgres@localhost:55433/stock_zero_cg005_lab?sslmode=disable", "postgres")
        self.assertEqual(dsn, "postgresql://postgres@localhost:55433/postgres?sslmode=disable")

    def test_execute_values_statement_shape(self):
        stmt = lab._values_statement("insert into x(a,b,c) values %s", 3)
        self.assertIn("values (%s,%s,%s)", stmt)

    def test_sql11_body_extracts_no_apply_wrapper(self):
        body, meta = lab.extract_sql11_body()
        self.assertTrue(meta["no_apply_header"])
        self.assertTrue(meta["begin_rollback_wrapper"])
        self.assertNotIn("-- NO APPLY", body)
        self.assertNotRegex(body.lower(), r"^\s*begin\s*;")
        self.assertNotRegex(body.lower(), r"rollback\s*;\s*$")
        self.assertIn("create table if not exists cg_core.ruta_rutero_week_assignment", body.lower())


class SnapshotBTests(unittest.TestCase):
    def _workbook(self, tmp: Path) -> Path:
        cols = list(loader.ROUTE_COLUMN_KEYS.values())
        rows = []
        for i in range(8):
            row = {col: "" for col in cols}
            row.update(
                {
                    "CADENA": "C",
                    "FORMATO": "F",
                    "REGION": "R",
                    "COMUNA": "COM",
                    "COD KPI ONE": f"RT{i:03d}",
                    "COD B2B": f"B2B{i:03d}",
                    "LOCAL": f"L{i:03d}",
                    "DIRECCION": f"D{i:03d}",
                    "VECES POR SEMANA": 2,
                    "RUTERO": f"RUT{i:03d}",
                    "JEFE DE OPERACIONES": "J",
                    "GESTORES": "G",
                    "CLIENTE": f"CLIENTE{i:03d}",
                    "SUPERVISOR": "S",
                    "REPONEDOR": f"P{i:03d}",
                    "LUNES": 1,
                    "MARTES": 1,
                    "MIERCOLES": 0,
                    "JUEVES": 0,
                    "VIERNES": 0,
                    "SABADO": 0,
                    "DOMINGO": 0,
                    "VISITA MENSUAL": 0,
                    "DIF": 0,
                    "OBS": "",
                    "AUX": "",
                    "GG": 0,
                    "MODALIDAD": "M",
                }
            )
            rows.append(row)
        path = tmp / "a.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=lab.SHEET)
        return path

    def test_snapshot_b_profile_is_metric_only(self):
        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            workbook = self._workbook(tmp)
            snapshot_b, profile = lab.make_snapshot_b(loader, workbook, tmp)
            self.assertTrue(snapshot_b.exists())
            self.assertEqual(profile["removed_logical_grains"], 2)
            self.assertEqual(profile["changed_responsables"], 2)
            self.assertEqual(profile["changed_frequency_or_days"], 1)
            self.assertEqual(profile["synthetic_highs"], 1)
            self.assertEqual(len(profile["sha256"]), 64)
            public_profile = {k: v for k, v in profile.items() if not k.startswith("_")}
            self.assertNotIn("LAB_ONLY_CLIENTE", str(public_profile))


class LocalPostgresIntegrationTests(unittest.TestCase):
    DSN = "postgresql://postgres@127.0.0.1:55433/stock_zero_cg005_lab?sslmode=disable"

    def setUp(self):
        try:
            lab.query_one(self.DSN, "select 1")
        except Exception as exc:
            self.skipTest(f"local PostgreSQL lab unavailable: {type(exc).__name__}")

    def test_assignment_insert_executes_with_notes_on_local_postgres(self):
        lab.apply_bootstrap_and_sql11(self.DSN)
        plan = {
            "input_file_name": "lab.xlsx",
            "input_file_sha256": "A" * 64,
            "schema_signature": "B" * 64,
            "planned_assignment": {
                "current_surface_hash": "C" * 64,
                "resolved_surface_hash": "D" * 64,
            },
        }
        with lab.connect(self.DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into cg_core.ruta_rutero_load_batch
                    (source_file, source_sheet, loader_name, loaded_rows, status, loaded_at, notes)
                    values (%s, %s, %s, 0, 'pending', %s, %s)
                    returning ruta_batch_id
                    """,
                    ("lab.xlsx", lab.SHEET, "test", datetime.now(timezone.utc), "arity integration"),
                )
                batch_id = int(cur.fetchone()[0])
                assignment_id = loader.create_week_assignment(
                    cur,
                    effective_week_start_value="2026-07-06",
                    ruta_batch_id=batch_id,
                    plan=plan,
                    assigned_by="arity-integration-test",
                    replaces_ruta_batch_id=None,
                )
                cur.execute(
                    """
                    select assigned_by, replaces_ruta_batch_id, notes
                      from cg_core.ruta_rutero_week_assignment
                     where assignment_id = %s
                    """,
                    (assignment_id,),
                )
                assigned_by, replaces_ruta_batch_id, notes = cur.fetchone()
            conn.rollback()
        self.assertEqual(assigned_by, "arity-integration-test")
        self.assertIsNone(replaces_ruta_batch_id)
        self.assertEqual(notes, "weekly replacement assignment created by guarded loader")


if __name__ == "__main__":
    unittest.main()

```


### scripts/cg_route_weekly_local_lab.py

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CG005I-M local PostgreSQL behavioral lab runner.

LAB ONLY. This runner is intentionally narrow: it validates the route weekly
replacement contract against a loopback PostgreSQL database and never contacts
Supabase or remote hosts.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import re
import sys
import tempfile
import threading
import time
import types
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

SQL11_PATH = ROOT / "sql" / "11_control_gestion_route_week_replacement_contract.sql"
BOOTSTRAP_PATH = ROOT / "sql" / "lab" / "12_cg005_route_weekly_lab_bootstrap.sql"
OBS_LEDGER_PATH = ROOT / "research" / "AI_LOAD_OBSERVATION_LEDGER.jsonl"

PHASE = "CG005I_M_LOCAL_POSTGRESQL_BEHAVIORAL_LAB"
WEEK = "2026-06-08"
SOURCE = "DB_GLOBAL_INVENTARIO.xlsx:RUTA_RUTERO"
INPUT_FILE_CODE = "DB_GLOBAL_INVENTARIO_XLSX"
SHEET = "RUTA_RUTERO"
MAIN_DB = "stock_zero_cg005_lab"
FAILURE_DB = "stock_zero_cg005_lab_failure"
SNAPSHOT_TRANSFORM_VERSION = "CG005K_B_SNAPSHOT_V1"
ROUTE_POLICY_VERSION = "ROUTE_WEEK_RETROACTIVE_REPLACEMENT_V1"
ROLLBACK_CONFIRM_TOKEN = "ROUTE_WEEK_ROLLBACK_V1"

REMOTE_HOST_MARKERS = ("supabase", "pooler", "aws-")
LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}
HEX64_RE = re.compile(r"^[0-9A-Fa-f]{64}$")
DB_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


class LabError(RuntimeError):
    def __init__(self, code: str, detail: str | None = None):
        super().__init__(detail or code)
        self.code = code


@dataclass(frozen=True)
class DsnInfo:
    dsn: str
    host: str
    port: int
    database: str
    sslmode: str | None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest().upper()


def safe_read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_report(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# CG005I-M local PostgreSQL lab",
        "",
        f"- Phase: `{PHASE}`",
        f"- Verdict: `{payload.get('verdict')}`",
        f"- Baseline: `{payload.get('baseline_commit')}`",
        f"- PostgreSQL: `{payload.get('local_postgresql', {}).get('version')}`",
        f"- Database: `{payload.get('local_postgresql', {}).get('database')}`",
        f"- Supabase contacted: `{payload.get('local_postgresql', {}).get('supabase_contacted')}`",
        "",
        "## Gates",
        "",
    ]
    for key in ("cg005i", "cg005j", "cg005k", "cg005l", "cg005m", "platform_008"):
        section = payload.get(key, {})
        lines.append(f"- `{key}` passed: `{section.get('passed', section.get('executed'))}`")
    if payload.get("blockers"):
        lines.extend(["", "## Blockers", ""])
        for blocker in payload["blockers"]:
            lines.append(f"- `{blocker}`")
        if payload.get("error_detail"):
            lines.append(f"- Detail: `{payload['error_detail']}`")
        cg005j = payload.get("cg005j", {})
        if cg005j.get("blocked_at"):
            lines.append(f"- Blocked at: `{cg005j['blocked_at']}`")
        if cg005j.get("loader_error"):
            lines.append(f"- Loader error: `{cg005j['loader_error']}`")
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- No DSN, password, row payload, customer, store, address or person values are recorded.",
            "- Writes are limited to the dedicated loopback PostgreSQL lab databases.",
            "- Snapshot B was generated under the OS temp directory and is not recorded in the repo.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_loopback_dsn(dsn: str) -> DsnInfo:
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise LabError("dsn_scheme_not_postgresql")
    host = (parsed.hostname or "").lower()
    if not host:
        raise LabError("dsn_host_missing")
    lowered = dsn.lower()
    if any(marker in lowered for marker in REMOTE_HOST_MARKERS):
        raise LabError("remote_dsn_marker_blocked")
    if host not in LOOPBACK_HOSTS:
        raise LabError("dsn_host_not_loopback")
    database = parsed.path.lstrip("/")
    if not database:
        raise LabError("dsn_database_missing")
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    sslmode = query.get("sslmode")
    if sslmode == "require" and host not in LOOPBACK_HOSTS:
        raise LabError("remote_sslmode_require_blocked")
    return DsnInfo(dsn=dsn, host=host, port=parsed.port or 5432, database=database, sslmode=sslmode)


def dsn_for_database(dsn: str, database: str) -> str:
    if not DB_NAME_RE.fullmatch(database):
        raise LabError("invalid_database_name")
    parsed = urlparse(dsn)
    return urlunparse((parsed.scheme, parsed.netloc, "/" + database, "", parsed.query, ""))


def import_psycopg():
    try:
        import psycopg
        from psycopg import sql
        from psycopg.types.json import Jsonb
    except Exception as exc:  # pragma: no cover - environment guard
        raise LabError("psycopg_v3_unavailable", type(exc).__name__) from exc
    return psycopg, sql, Jsonb


class _JsonCompat:
    def __init__(self, adapted, dumps=None):
        self.adapted = adapted
        self.dumps = dumps or json.dumps


def _values_statement(sql_text: str, width: int) -> str:
    placeholder = "(" + ",".join(["%s"] * width) + ")"
    stmt, count = re.subn(r"values\s+%s", "values " + placeholder, sql_text, count=1, flags=re.IGNORECASE)
    if count != 1:
        raise LabError("execute_values_sql_shape_unsupported")
    return stmt


def install_psycopg2_compat() -> None:
    psycopg, _sql, Jsonb = import_psycopg()

    def connect(dsn: str):
        parse_loopback_dsn(dsn)
        return psycopg.connect(dsn)

    def convert(value):
        if isinstance(value, _JsonCompat):
            return Jsonb(value.adapted, dumps=value.dumps)
        return value

    def execute_values(cur, sql_text: str, rows: list[tuple], page_size: int = 5000) -> None:
        if not rows:
            return
        stmt = _values_statement(sql_text, len(rows[0]))
        for start in range(0, len(rows), page_size):
            chunk = rows[start : start + page_size]
            converted = [tuple(convert(v) for v in row) for row in chunk]
            cur.executemany(stmt, converted)

    psycopg2_module = types.ModuleType("psycopg2")
    psycopg2_module.connect = connect
    extras_module = types.ModuleType("psycopg2.extras")
    extras_module.Json = _JsonCompat
    extras_module.execute_values = execute_values
    psycopg2_module.extras = extras_module
    sys.modules["psycopg2"] = psycopg2_module
    sys.modules["psycopg2.extras"] = extras_module


def connect(dsn: str):
    psycopg, _sql, _jsonb = import_psycopg()
    parse_loopback_dsn(dsn)
    return psycopg.connect(dsn)


def admin_recreate_databases(dsn: str, names: list[str]) -> None:
    _psycopg, sql, _jsonb = import_psycopg()
    admin_dsn = dsn_for_database(dsn, "postgres")
    with connect(admin_dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for name in names:
                if not DB_NAME_RE.fullmatch(name):
                    raise LabError("invalid_database_name")
                cur.execute(
                    "select pg_terminate_backend(pid) from pg_stat_activity where datname = %s and pid <> pg_backend_pid()",
                    (name,),
                )
                cur.execute(sql.SQL("drop database if exists {} with (force)").format(sql.Identifier(name)))
                cur.execute(sql.SQL("create database {}").format(sql.Identifier(name)))


def execute_sql_text(dsn: str, sql_text: str) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()


def query_one(dsn: str, sql_text: str, params: tuple = ()) -> tuple:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text, params)
            row = cur.fetchone()
            if row is None:
                raise LabError("query_returned_no_rows")
            return tuple(row)


def query_all(dsn: str, sql_text: str, params: tuple = ()) -> list[tuple]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text, params)
            return [tuple(row) for row in cur.fetchall()]


def server_summary(dsn: str) -> dict:
    row = query_one(
        dsn,
        """
        select
            current_user,
            coalesce(inet_server_addr()::text, 'local'),
            inet_server_port(),
            current_setting('server_version'),
            current_setting('transaction_read_only')
        """,
    )
    return {
        "current_user": row[0],
        "server_addr_loopback": str(row[1]) in {"127.0.0.1", "::1", "local"},
        "server_port": int(row[2]),
        "server_version": str(row[3]),
        "transaction_read_only": str(row[4]),
    }


def extract_sql11_body(path: Path = SQL11_PATH) -> tuple[str, dict]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("-- NO APPLY"):
        raise LabError("sql11_missing_no_apply_header")
    begin_match = re.search(r"(?im)^\s*begin\s*;\s*$", text)
    rollback_matches = list(re.finditer(r"(?im)^\s*rollback\s*;\s*$", text))
    if not begin_match or not rollback_matches:
        raise LabError("sql11_missing_begin_rollback_wrapper")
    rollback_match = rollback_matches[-1]
    if begin_match.end() >= rollback_match.start():
        raise LabError("sql11_invalid_wrapper_order")
    body = text[begin_match.end() : rollback_match.start()].strip() + "\n"
    return body, {
        "path": "sql/11_control_gestion_route_week_replacement_contract.sql",
        "sha256": sha256_file(path),
        "body_sha256": sha256_text(body),
        "no_apply_header": True,
        "begin_rollback_wrapper": True,
    }


def apply_bootstrap_and_sql11(dsn: str) -> dict:
    bootstrap_sql = BOOTSTRAP_PATH.read_text(encoding="utf-8")
    execute_sql_text(dsn, bootstrap_sql)
    body, sql11 = extract_sql11_body()
    execute_sql_text(dsn, body)
    return {
        "bootstrap_sha256": sha256_file(BOOTSTRAP_PATH),
        "sql11": sql11,
    }


def load_loader():
    install_psycopg2_compat()
    import load_ruta_rutero_from_excel as loader

    return loader


def verify_loader_contract(dsn: str, loader) -> dict:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            contract = loader.verify_db_contract(cur)
    if not contract.get("ok"):
        raise LabError("missing_loader_db_contract", ",".join(contract.get("missing", [])))
    return {"ok": True, "missing": []}


def prove_advisory_lock(dsn: str, loader) -> dict:
    key = loader.weekly_assignment_lock_key(
        source=SOURCE,
        effective_week_start_value=WEEK,
        route_policy_version=loader.ROUTE_POLICY_VERSION,
    )
    other_key = loader.weekly_assignment_lock_key(
        source=SOURCE,
        effective_week_start_value="2026-06-15",
        route_policy_version=loader.ROUTE_POLICY_VERSION,
    )
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select pg_advisory_xact_lock(%s)", (key,))
        conn.rollback()

    acquired = threading.Event()
    timings: dict[str, float] = {}
    errors: list[str] = []

    conn_a = connect(dsn)
    conn_b = connect(dsn)
    try:
        cur_a = conn_a.cursor()
        cur_b = conn_b.cursor()
        cur_a.execute("select pg_advisory_xact_lock(%s)", (key,))
        start = time.perf_counter()

        def wait_same_key():
            try:
                cur_b.execute("select pg_advisory_xact_lock(%s)", (key,))
                timings["blocked_seconds"] = time.perf_counter() - start
                acquired.set()
            except Exception as exc:  # pragma: no cover - failure evidence
                errors.append(type(exc).__name__)
                acquired.set()

        t = threading.Thread(target=wait_same_key, daemon=True)
        t.start()
        time.sleep(0.35)
        blocked_while_held = not acquired.is_set()
        conn_a.rollback()
        t.join(timeout=5)
        acquired_after_release = acquired.is_set() and not errors
        conn_b.rollback()
    finally:
        conn_a.close()
        conn_b.close()

    conn_c = connect(dsn)
    conn_d = connect(dsn)
    try:
        cur_c = conn_c.cursor()
        cur_d = conn_d.cursor()
        cur_c.execute("select pg_advisory_xact_lock(%s)", (key,))
        start_other = time.perf_counter()
        cur_d.execute("select pg_advisory_xact_lock(%s)", (other_key,))
        other_elapsed = time.perf_counter() - start_other
        distinct_key_not_blocked = other_elapsed < 0.35
        conn_d.rollback()
        conn_c.rollback()
    finally:
        conn_c.close()
        conn_d.close()

    if not blocked_while_held or not acquired_after_release or not distinct_key_not_blocked:
        raise LabError("advisory_lock_behavior_failed")
    return {
        "resolved": True,
        "same_key_blocked_while_held": blocked_while_held,
        "same_key_acquired_after_release": acquired_after_release,
        "distinct_key_not_blocked": distinct_key_not_blocked,
        "same_key_blocked_seconds": round(float(timings.get("blocked_seconds", 0.0)), 3),
        "distinct_key_elapsed_seconds": round(float(other_elapsed), 3),
    }


def loader_help_flags(loader) -> list[str]:
    text = loader.build_arg_parser().format_help()
    return sorted(set(re.findall(r"--[a-zA-Z0-9][a-zA-Z0-9-]*", text)))


def run_source_and_plan(loader, workbook: Path, expected_hash: str | None = None, operation: str = "dry_run") -> dict:
    source_check = loader.run_source_check_ruta(excel_path=workbook, sheet=SHEET, strict=False)
    plan = loader.build_dry_run_plan(
        excel_path=workbook,
        sheet=SHEET,
        source=SOURCE,
        effective_week_start_value=WEEK,
        expected_workbook_sha256=expected_hash,
        source_check=source_check,
    )
    return {
        "operation": operation,
        "source_check_verdict": source_check["final_verdict"],
        "input_rows": int(plan["input_rows"]),
        "accepted_rows": int(plan["accepted_rows"]),
        "history_insert_rows": int(plan["history_insert_rows"]),
        "planned_public_insert_rows": int(plan["planned_public_insert_rows"]),
        "exact_duplicate_excess": int(plan["exact_duplicate_excess"]),
        "grain_duplicate_groups": int(plan["grain_duplicate_groups"]),
        "schema_signature": plan["schema_signature"],
        "input_file_sha256": plan["input_file_sha256"],
        "current_surface_hash": plan["planned_assignment"]["current_surface_hash"],
        "resolved_surface_hash": plan["planned_assignment"]["resolved_surface_hash"],
        "writes_executed": False,
    }


def run_loader_apply(loader, dsn: str, workbook: Path, expected_hash: str, temp_dir: Path, label: str) -> dict:
    json_out = temp_dir / f"{label}_apply.json"
    argv = [
        "--excel",
        str(workbook),
        "--sheet",
        SHEET,
        "--source",
        SOURCE,
        "--effective-week-start",
        WEEK,
        "--apply",
        "--expected-workbook-sha256",
        expected_hash,
        "--confirm-weekly-replacement",
        loader.ROUTE_POLICY_VERSION,
        "--db_url",
        dsn,
        "--json-out",
        str(json_out),
    ]
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            loader.main(argv)
    except SystemExit as exc:
        payload = safe_read_json(json_out) if json_out.exists() else {}
        code = payload.get("error_code", f"system_exit_{exc.code}")
        detail = str(code)
        if payload.get("error"):
            detail = f"{detail}:{payload['error']}"
        raise LabError(f"{label}_loader_apply_failed", detail) from exc
    result = safe_read_json(json_out)
    if result.get("mode") != "apply" or not result.get("writes_executed"):
        raise LabError("loader_apply_failed")
    return result


def run_loader_rollback(loader, dsn: str, failed_assignment_id: int, expected_current_surface_hash: str, temp_dir: Path, label: str) -> dict:
    json_out = temp_dir / f"{label}_rollback.json"
    argv = [
        "--source",
        SOURCE,
        "--effective-week-start",
        WEEK,
        "--rollback-weekly-replacement",
        "--failed-assignment-id",
        str(failed_assignment_id),
        "--expected-current-surface-hash",
        expected_current_surface_hash,
        "--confirm-rollback",
        loader.ROLLBACK_CONFIRM_TOKEN,
        "--db_url",
        dsn,
        "--json-out",
        str(json_out),
    ]
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            loader.main(argv)
    except SystemExit as exc:
        payload = safe_read_json(json_out) if json_out.exists() else {}
        code = payload.get("error_code", f"system_exit_{exc.code}")
        detail = str(code)
        if payload.get("error"):
            detail = f"{detail}:{payload['error']}"
        raise LabError(f"{label}_loader_rollback_failed", detail) from exc
    result = safe_read_json(json_out)
    if result.get("mode") != "rollback" or not result.get("writes_executed"):
        raise LabError("loader_rollback_failed")
    return result


def assignment_summary(dsn: str) -> dict:
    rows = query_all(
        dsn,
        """
        select assignment_status, count(*)::bigint
          from cg_core.ruta_rutero_week_assignment
         where effective_week_start = %s
           and route_policy_version = %s
         group by assignment_status
         order by assignment_status
        """,
        (WEEK, ROUTE_POLICY_VERSION),
    )
    active = query_one(
        dsn,
        """
        select count(*)::bigint
          from cg_core.ruta_rutero_week_assignment
         where effective_week_start = %s
           and route_policy_version = %s
           and assignment_status = 'ACTIVE'
        """,
        (WEEK, ROUTE_POLICY_VERSION),
    )[0]
    return {"status_counts": {str(k): int(v) for k, v in rows}, "active_count": int(active)}


def week_view_summary(dsn: str) -> dict:
    row = query_one(
        dsn,
        """
        select count(*)::bigint,
               count(*) filter (where route_week_source = 'EXPLICIT_ASSIGNMENT')::bigint
          from cg_core.v_ruta_rutero_load_batch_week_v2
         where effective_week_start = %s
        """,
        (WEEK,),
    )
    return {"rows": int(row[0]), "explicit_assignment_rows": int(row[1])}


def batch_grains(dsn: str, batch_id: int) -> set[tuple[str, str]]:
    rows = query_all(
        dsn,
        """
        with exact_deduped as (
            select distinct on (row_hash)
                   nullif(trim(coalesce(cod_rt_norm, cod_rt)), '') as cod_rt_norm,
                   upper(trim(coalesce(nullif(trim(cliente_norm), ''), nullif(trim(cliente), ''), ''))) as cliente_norm,
                   row_hash,
                   source_row
              from cg_core.ruta_rutero_load_rows
             where ruta_batch_id = %s
               and nullif(trim(coalesce(cod_rt_norm, cod_rt)), '') is not null
               and nullif(trim(coalesce(cliente_norm, cliente)), '') is not null
             order by row_hash, source_row
        )
        select cod_rt_norm, cliente_norm
          from exact_deduped
        """,
        (batch_id,),
    )
    return {(str(a), str(b)) for a, b in rows}


def resolved_grain_diff(dsn: str, batch_id: int) -> dict:
    assigned = batch_grains(dsn, batch_id)
    resolved = {
        (str(a), str(b))
        for a, b in query_all(
            dsn,
            """
            select cod_rt_norm, cliente_norm
              from cg_core.v_rr_frecuencia_base_resuelta_v2
             where effective_week_start = %s
            """,
            (WEEK,),
        )
    }
    return {"missing": len(assigned - resolved), "extra": len(resolved - assigned)}


def current_hash(dsn: str) -> str:
    row = query_one(
        dsn,
        """
        select encode(
            digest(
                coalesce(string_agg(source_row::text || '|' || row_hash, E'\n' order by source_row, row_hash), ''),
                'sha256'
            ),
            'hex'
        )
          from public.ruta_rutero
         where source = %s
        """,
        (SOURCE,),
    )
    return str(row[0]).upper()


def ensure_pgcrypto(dsn: str) -> None:
    execute_sql_text(dsn, "create extension if not exists pgcrypto;")


def raw_index_for_source_row(df: pd.DataFrame, loader, source_row: int) -> int:
    normalized = loader.normalize_header_map(df.columns)
    if "IDROW" in normalized:
        matches = df.index[pd.to_numeric(df[normalized["IDROW"]], errors="coerce").fillna(-1).astype(int) == int(source_row)]
        if len(matches) != 1:
            raise LabError("snapshot_b_source_row_match_failed")
        return int(matches[0])
    idx = int(source_row) - 2
    if idx < 0 or idx >= len(df):
        raise LabError("snapshot_b_source_row_out_of_range")
    return idx


def col_for(loader, df: pd.DataFrame, key: str) -> str:
    normalized = loader.normalize_header_map(df.columns)
    if key not in normalized:
        raise LabError("snapshot_b_missing_column", key)
    return normalized[key]


def make_snapshot_b(loader, workbook_a: Path, temp_dir: Path) -> tuple[Path, dict]:
    df = loader.read_route_excel(workbook_a, SHEET)
    accepted = loader.transform_route_dataframe(df, source=SOURCE)
    current = loader.current_surface_rows(accepted)
    frame = current.copy()
    frame["_cod_rt_norm"] = frame["cod_rt"].map(loader.normalize_text)
    frame["_cliente_norm"] = frame["cliente"].map(loader.normalize_key)
    usable = frame[(frame["_cod_rt_norm"] != "") & (frame["_cliente_norm"] != "")].sort_values("source_row")
    if len(usable) < 6:
        raise LabError("snapshot_b_not_enough_rows")
    removal_rows = [int(v) for v in usable["source_row"].head(2).tolist()]
    change_rows = [int(v) for v in usable["source_row"].iloc[2:4].tolist()]
    frequency_row = int(usable["source_row"].iloc[4])
    removal_grains = {
        (str(row["_cod_rt_norm"]), str(row["_cliente_norm"]))
        for _, row in usable.head(2).iterrows()
    }

    df_b = df.copy()
    remove_indexes = [raw_index_for_source_row(df_b, loader, row) for row in removal_rows]
    df_b = df_b.drop(index=remove_indexes).reset_index(drop=True)

    reponedor_col = col_for(loader, df_b, "REPONEDOR")
    supervisor_col = col_for(loader, df_b, "SUPERVISOR")
    for i, source_row in enumerate(change_rows, start=1):
        idx = raw_index_for_source_row(df_b, loader, source_row)
        df_b.at[idx, reponedor_col] = f"LAB_ONLY_RESPONSABLE_{i}"
        df_b.at[idx, supervisor_col] = f"LAB_ONLY_SUPERVISOR_{i}"

    freq_idx = raw_index_for_source_row(df_b, loader, frequency_row)
    df_b.at[freq_idx, col_for(loader, df_b, "VECES POR SEMANA")] = 1
    for day in ("LUNES", "MARTES", "MIERCOLES", "JUEVES", "VIERNES", "SABADO", "DOMINGO"):
        df_b.at[freq_idx, col_for(loader, df_b, day)] = 1 if day == "LUNES" else 0

    synthetic = df_b.iloc[0].copy()
    synthetic_values = {
        "CADENA": "LAB_ONLY",
        "FORMATO": "LAB_ONLY",
        "REGION": "LAB_ONLY",
        "COMUNA": "LAB_ONLY",
        "COD KPI ONE": "LAB_ONLY_RT_20260608",
        "COD B2B": "LAB_ONLY_B2B_20260608",
        "LOCAL": "LAB_ONLY_LOCAL",
        "DIRECCION": "LAB_ONLY_ADDRESS",
        "RUTERO": "LAB_ONLY_RUTERO",
        "JEFE DE OPERACIONES": "LAB_ONLY_JEFE",
        "GESTORES": "LAB_ONLY_GESTOR",
        "CLIENTE": "LAB_ONLY_CLIENTE",
        "SUPERVISOR": "LAB_ONLY_SUPERVISOR",
        "REPONEDOR": "LAB_ONLY_RESPONSABLE",
        "VECES POR SEMANA": 1,
        "LUNES": 1,
        "MARTES": 0,
        "MIERCOLES": 0,
        "JUEVES": 0,
        "VIERNES": 0,
        "SABADO": 0,
        "DOMINGO": 0,
        "VISITA MENSUAL": 0,
        "DIF": 0,
        "OBS": "LAB_ONLY",
        "AUX": "",
        "GG": 0,
        "MODALIDAD": "LAB_ONLY",
    }
    normalized = loader.normalize_header_map(df_b.columns)
    for required, value in synthetic_values.items():
        if required in normalized:
            synthetic[normalized[required]] = value
    if "IDROW" in normalized:
        synthetic[normalized["IDROW"]] = int(pd.to_numeric(df_b[normalized["IDROW"]], errors="coerce").fillna(0).max()) + 1

    df_b = pd.concat([pd.DataFrame([synthetic]), df_b], ignore_index=True)
    if len(df_b) > 17:
        df_b = pd.concat([df_b.iloc[17:], df_b.iloc[:17]], ignore_index=True)
    df_b = df_b.iloc[::-1].reset_index(drop=True)

    snapshot_path = temp_dir / "snapshot_b.xlsx"
    with pd.ExcelWriter(snapshot_path, engine="openpyxl") as writer:
        df_b.to_excel(writer, index=False, sheet_name=SHEET)

    _, accepted_b = loader.prepare_route_rows(snapshot_path, sheet=SHEET, source=SOURCE)
    current_b = loader.current_surface_rows(accepted_b)
    synthetic_grain = ("LAB_ONLY_RT_20260608", "LAB_ONLY_CLIENTE")
    profile = {
        "transform_version":

[TRUNCATED_FOR_PACKET] 26956 chars omitted.
```


### scripts/load_kpione2_photo_from_excel.py

```python
# scripts/load_kpione2_photo_from_excel.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


LOADER_NAME = "load_kpione2_photo_from_excel"
PHASE = "FAST_REFORM_010E_ROUTE_B_SOURCE_ROW_NUMBER_DRY_RUN_PATCH"
LOCAL_TZ = ZoneInfo("America/Santiago")

DEFAULT_EXCEL = Path("data/photo-excel-admin_1782440454408.xlsx")
DEFAULT_SHEET = "Fotos"
DEFAULT_CONTRACT = Path("contracts/control_gestion/kpione2_photo_export_contract_v1.json")
PRODUCTIVE_LOADER_PATH = "scripts/load_control_gestion_raw_v17.py"
EXCEL_HEADER_ROW_NUMBER = 1
EXCEL_FIRST_DATA_ROW_NUMBER = EXCEL_HEADER_ROW_NUMBER + 1

GRAIN_CONTRACT = {
    "input_grain": "photo_row",
    "normalized_grain": "event_row",
    "compliance_grain": "day_presence",
    "forbidden_assumption": "one_excel_row_equals_one_visit",
}

COLUMN_ALIASES = {
    "event_id": ["id"],
    "sp_item_id": ["sp item id"],
    "holding": ["holding"],
    "subcadena": ["subcadena"],
    "cod_rt": ["codigo local"],
    "cliente_norm": ["marca"],
    "local_nombre": ["local"],
    "direccion": ["direccion"],
    "reponedor": ["reponedor"],
    "fecha": ["fecha"],
    "fecha_subida": ["fecha de subida"],
    "hora": ["hora"],
    "tipo_de_tarea": ["tipo de tarea"],
    "photo_count": ["n fotos", "foto no/total", "foto n/total", "foto n o/total"],
    "comentarios": ["comentarios"],
    "link_foto": ["link foto"],
}

REQUIRED_KEYS = [
    "event_id",
    "sp_item_id",
    "holding",
    "subcadena",
    "cod_rt",
    "cliente_norm",
    "local_nombre",
    "direccion",
    "reponedor",
    "fecha",
    "hora",
    "tipo_de_tarea",
    "photo_count",
    "comentarios",
    "link_foto",
]

EVENT_STABLE_KEYS = [
    "event_id",
    "sp_item_id",
    "holding",
    "subcadena",
    "cod_rt",
    "cliente_norm",
    "local_nombre",
    "direccion",
    "reponedor",
    "fecha",
    "comentarios",
]

PHOTO_LEVEL_KEYS = [
    "photo_count",
    "link_foto",
    "hora",
    "tipo_de_tarea",
    "fecha_subida",
]

BLOCKING_FLAG_KEYS = [
    "grain_contract_match",
    "forbidden_assumption_rejected",
    "photo_rows_match",
    "distinct_event_ids_match",
    "fecha_min_match",
    "fecha_max_match",
    "db_apply_false",
    "sql_apply_false",
    "productive_loader_touched_false",
    "no_null_event_id_rows",
    "no_null_fecha_rows",
    "no_null_n_fotos_calculado_rows",
    "no_event_ids_multi_fecha",
    "no_event_ids_multi_week",
    "no_event_ids_multi_sp_item",
    "no_row_count_n_fotos_mismatch_events",
    "no_real_content_conflict_event_ids",
    "day_presence_is_binary",
    "source_row_number_present",
    "source_row_number_complete",
    "source_row_number_unique",
    "source_row_number_matches_excel_rows",
]


class LoaderUsageError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def normalize_column_name(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\u00c2\u00ba", "\u00ba").replace("\u00c2\u00b0", "\u00ba")
    text = text.replace("\u00b0", "\u00ba")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("\u00ba", "o")
    text = text.strip().lower()
    return re.sub(r"\s+", " ", text)


def clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        if value.time() == datetime.min.time():
            return value.date().isoformat()
        return value.isoformat()
    text = str(value).strip()
    return re.sub(r"\s+", " ", text)


def normalize_key(value: object) -> str:
    text = unicodedata.normalize("NFKD", clean_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.strip().upper()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def now_local_iso() -> str:
    return datetime.now(LOCAL_TZ).isoformat(timespec="seconds")


def load_contract(contract_path: Path) -> dict[str, Any]:
    if not contract_path.exists():
        raise LoaderUsageError("contract_not_found", f"Contract file not found: {contract_path}")
    return json.loads(contract_path.read_text(encoding="utf-8"))


def expected_from_contract(contract: dict[str, Any]) -> dict[str, Any]:
    evidence = dict(contract.get("009F_evidence") or {})
    return {
        "photo_rows": evidence.get("photo_rows"),
        "distinct_event_ids": evidence.get("distinct_event_ids"),
        "fecha_min": evidence.get("fecha_min"),
        "fecha_max": evidence.get("fecha_max"),
    }


def resolve_columns(df: pd.DataFrame) -> tuple[dict[str, str], list[str]]:
    norm_to_actual: dict[str, str] = {}
    for col in df.columns:
        norm_to_actual.setdefault(normalize_column_name(col), str(col).strip())

    resolved: dict[str, str] = {}
    missing: list[str] = []
    for key, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            actual = norm_to_actual.get(normalize_column_name(alias))
            if actual:
                resolved[key] = actual
                break
        if key in REQUIRED_KEYS and key not in resolved:
            missing.append(key)
    return resolved, missing


def parse_total_from_photo_count(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip()
    slash_total = text.str.extract(r"/\s*(\d+)\s*$")[0]
    direct_total = text.str.extract(r"^\s*(\d+)\s*$")[0]
    return pd.to_numeric(slash_total.fillna(direct_total), errors="coerce")


def week_start_monday(series: pd.Series) -> pd.Series:
    return series - pd.to_timedelta(series.dt.weekday, unit="D")


def sorted_distinct(series: pd.Series) -> list[str]:
    values = sorted({clean_text(value) for value in series if clean_text(value)})
    return values


def first_clean(series: pd.Series) -> str:
    for value in series:
        text = clean_text(value)
        if text:
            return text
    return ""


def stable_hash_frame(df: pd.DataFrame, stable_cols: list[str]) -> pd.Series:
    if not stable_cols:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    normalized = pd.DataFrame(index=df.index)
    for col in stable_cols:
        normalized[col] = df[col].map(lambda value: normalize_key(value))
    return normalized.agg("||".join, axis=1).map(sha256_text)


def photo_row_hash_frame(df: pd.DataFrame) -> pd.Series:
    normalized = pd.DataFrame(index=df.index)
    for col in df.columns:
        normalized[str(col)] = df[col].map(lambda value: clean_text(value))
    return normalized.agg("||".join, axis=1).map(sha256_text)


def assign_excel_source_row_numbers(df: pd.DataFrame) -> pd.DataFrame:
    numbered = df.copy()
    numbered["_source_row_number"] = pd.RangeIndex(
        start=EXCEL_FIRST_DATA_ROW_NUMBER,
        stop=EXCEL_FIRST_DATA_ROW_NUMBER + len(numbered),
    )
    return numbered


def source_trace_manifest_sha256(df: pd.DataFrame) -> str:
    h = hashlib.sha256()
    trace_columns = [
        "_source_row_number",
        "_event_id",
        "_sp_item_id",
        "_photo_row_hash",
    ]
    for source_row_number, event_id, sp_item_id, photo_row_hash in df[trace_columns].itertuples(
        index=False,
        name=None,
    ):
        trace_record = [
            int(source_row_number),
            clean_text(event_id),
            clean_text(sp_item_id),
            clean_text(photo_row_hash),
        ]
        h.update(json.dumps(trace_record, ensure_ascii=True, separators=(",", ":")).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def source_trace_sample(df: pd.DataFrame) -> list[dict[str, Any]]:
    trace = df[
        ["_source_row_number", "_event_id", "_sp_item_id", "_photo_row_hash"]
    ].rename(
        columns={
            "_source_row_number": "source_row_number",
            "_event_id": "event_id",
            "_sp_item_id": "sp_item_id",
            "_photo_row_hash": "photo_row_hash",
        }
    )
    if len(trace) > 6:
        trace = pd.concat([trace.head(3), trace.tail(3)], ignore_index=True)
    records = trace.to_dict(orient="records")
    for record in records:
        record["source_row_number"] = int(record["source_row_number"])
    return records


def _date_iso(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).date().isoformat()


def _date_list(series: pd.Series) -> list[str]:
    return sorted({x for x in (_date_iso(value) for value in series.dropna()) if x})


def analyze_photo_dataframe(
    df: pd.DataFrame,
    *,
    contract: dict[str, Any],
    expected: dict[str, Any],
    source_file: str,
    source_file_sha256: str | None,
    sheet_name: str,
) -> dict[str, Any]:
    df = assign_excel_source_row_numbers(df)
    df.columns = [str(c).strip() for c in df.columns]
    resolved, missing = resolve_columns(df)

    base_payload: dict[str, Any] = {
        "phase": PHASE,
        "loader_name": LOADER_NAME,
        "mode": "dry_run",
        "generated_at": now_local_iso(),
        "source_file": source_file,
        "source_file_sha256": source_file_sha256,
        "sheet_name": sheet_name,
        "contract_status": contract.get("status"),
        "db_apply": False,
        "sql_apply": False,
        "writes_executed": False,
        "dsn_printed": False,
        "productive_loader_path": PRODUCTIVE_LOADER_PATH,
        "productive_loader_touched": False,
        "grain_contract": GRAIN_CONTRACT,
        "expected": expected,
        "columns": list(df.columns),
        "resolved_columns": resolved,
        "missing_required_columns": missing,
        "errors": [],
        "warnings": [],
    }

    contract_grain = contract.get("grain_contract") or {}
    if contract_grain != GRAIN_CONTRACT:
        base_payload["errors"].append("grain_contract_mismatch")

    if missing:
        base_payload["errors"].append("missing_required_columns")
        base_payload["flags"] = {
            "grain_contract_match": contract_grain == GRAIN_CONTRACT,
            "db_apply_false": True,
            "sql_apply_false": True,
            "productive_loader_touched_false": True,
        }
        base_payload["verdict"] = "BLOCKED_STRUCTURE"
        return base_payload

    col_event = resolved["event_id"]
    col_sp = resolved["sp_item_id"]
    col_fecha = resolved["fecha"]
    col_photo_count = resolved["photo_count"]

    df["_event_id"] = df[col_event].astype("string").fillna("").str.strip()
    df["_sp_item_id"] = df[col_sp].astype("string").fillna("").str.strip()
    df["_fecha_dt"] = pd.to_datetime(df[col_fecha], errors="coerce").dt.normalize()
    df["_week_start"] = week_start_monday(df["_fecha_dt"])
    df["_n_fotos_calculado"] = parse_total_from_photo_count(df[col_photo_count])

    stable_cols = [resolved[key] for key in EVENT_STABLE_KEYS if key in resolved]
    photo_level_cols = [resolved[key] for key in PHOTO_LEVEL_KEYS if key in resolved]
    df["_event_stable_hash"] = stable_hash_frame(df, stable_cols)
    df["_photo_row_hash"] = photo_row_hash_frame(df[[c for c in df.columns if not c.startswith("_")]])

    photo_rows = int(len(df))
    source_row_numbers = df["_source_row_number"]
    source_row_number_null_rows = int(source_row_numbers.isna().sum())
    source_row_number_distinct = int(source_row_numbers.nunique(dropna=True))
    source_row_number_min = int(source_row_numbers.min()) if photo_rows else None
    source_row_number_max = int(source_row_numbers.max()) if photo_rows else None
    expected_source_row_numbers = pd.Series(
        pd.RangeIndex(
            start=EXCEL_FIRST_DATA_ROW_NUMBER,
            stop=EXCEL_FIRST_DATA_ROW_NUMBER + photo_rows,
        ),
        dtype="int64",
    )
    source_row_number_matches_excel_rows = source_row_numbers.reset_index(drop=True).equals(
        expected_source_row_numbers
    )
    trace_manifest_sha256 = source_trace_manifest_sha256(df)
    valid_event_mask = df["_event_id"] != ""
    valid_events = df[valid_event_mask].copy()
    distinct_event_ids = int(valid_events["_event_id"].nunique())
    distinct_event_identity_pairs = int(
        valid_events[["_event_id", "_sp_item_id"]].drop_duplicates().shape[0]
    )
    null_event_id_rows = int((df["_event_id"] == "").sum())
    null_fecha_rows = int(df["_fecha_dt"].isna().sum())
    null_n_fotos_calculado_rows = int(df["_n_fotos_calculado"].isna().sum())
    fecha_min = _date_iso(df["_fecha_dt"].min()) if not df["_fecha_dt"].dropna().empty else None
    fecha_max = _date_iso(df["_fecha_dt"].max()) if not df["_fecha_dt"].dropna().empty else None

    event_date_counts = valid_events.groupby("_event_id")["_fecha_dt"].nunique(dropna=True)
    event_week_counts = valid_events.groupby("_event_id")["_week_start"].nunique(dropna=True)
    event_sp_counts = valid_events.groupby("_event_id")["_sp_item_id"].nunique(dropna=False)
    event_hash_counts = valid_events.groupby("_event_id")["_event_stable_hash"].nunique(dropna=False)

    event_ids_multi_fecha = int((event_date_counts > 1).sum())
    event_ids_multi_week = int((event_week_counts > 1).sum())
    event_ids_multi_sp_item = int((event_sp_counts > 1).sum())
    real_content_conflict_event_ids = int((event_hash_counts > 1).sum())

    per_event = (
        valid_events.groupby("_event_id", dropna=False)
        .agg(
            source_photo_rows=("_event_id", "size"),
            first_source_row_number=("_source_row_number", "min"),
            last_source_row_number=("_source_row_number", "max"),
            n_fotos_calculado=("_n_fotos_calculado", "max"),
            fecha=("_fecha_dt", "first"),
            week_start=("_week_start", "first"),
            sp_item_id=("_sp_item_id", first_clean),
            cod_rt=(resolved["cod_rt"], first_clean),
            cliente_norm=(resolved["cliente_norm"], first_clean),
            local_nombre=(resolved["local_nombre"], first_clean),
            reponedor=(resolved["reponedor"], first_clean),
            hora_primera_foto=(resolved["hora"], first_clean),
            event_stable_hash=("_event_stable_hash", first_clean),
        )
        .reset_index()
    )
    per_event["tipos_de_tarea"] = (
        valid_events.groupby("_event_id")[resolved["tipo_de_tarea"]]
        .agg(sorted_distinct)
        .reindex(per_event["_event_id"])
        .tolist()
    )
    per_event["row_count_equals_n_fotos"] = (
        per_event["source_photo_rows"] == per_event["n_fotos_calculado"]
    )
    row_count_n_fotos_mismatch_events = int((~per_event["row_count_equals_n_fotos"]).sum())
    per_event["fecha"] = per_event["fecha"].map(_date_iso)
    per_event["week_start"] = per_event["week_start"].map(_date_iso)
    per_event["cod_rt_norm"] = per_event["cod_rt"].map(normalize_key)
    per_event["cliente_norm_key"] = per_event["cliente_norm"].map(normalize_key)

    day_presence = (
        per_event.groupby(["fecha", "cod_rt_norm", "cliente_norm_key"], dropna=False)
        .agg(event_rows=("_event_id", "size"))
        .reset_index()
    )
    day_presence["presence"] = 1
    day_presence_rows = int(len(day_presence))
    max_events_per_day_presence = int(day_presence["event_rows"].max()) if day_presence_rows else 0
    day_presence_is_binary = bool((day_presence["presence"] == 1).all())

    daily_raw = (
        df.dropna(subset=["_fecha_dt"])
        .groupby("_fecha_dt")
        .agg(photo_rows=(col_event, "size"), distinct_event_ids=("_event_id", pd.Series.nunique))
        .reset_index()
    )
    daily_raw["coverage_date"] = daily_raw["_fecha_dt"].map(_date_iso)
    daily_coverage = daily_raw[["coverage_date", "photo_rows", "distinct_event_ids"]].to_dict(
        orient="records"
    )

    metrics = {
        "photo_rows": photo_rows,
        "distinct_event_ids": distinct_event_ids,
        "distinct_event_identity_pairs": distinct_event_identity_pairs,
        "fecha_min": fecha_min,
        "fecha_max": fecha_max,
        "dates_present": _date_list(df["_fecha_dt"]),
        "weeks_present": _date_list(df["_week_start"]),
        "event_rows": int(len(per_event)),
        "day_presence_rows": day_presence_rows,
        "max_events_per_day_presence": max_events_per_day_presence,
        "null_event_id_rows": null_event_id_rows,
        "null_fecha_rows": null_fecha_rows,
        "null_n_fotos_calculado_rows": null_n_fotos_calculado_rows,
        "event_ids_multi_fecha": event_ids_multi_fecha,
        "event_ids_multi_week": event_ids_multi_week,
        "event_ids_multi_sp_item": event_ids_multi_sp_item,
        "real_content_conflict_event_ids": real_content_conflict_event_ids,
        "row_count_n_fotos_mismatch_events": row_count_n_fotos_mismatch_events,
        "source_row_number_null_rows": source_row_number_null_rows,
        "source_row_number_distinct": source_row_number_distinct,
        "source_row_number_min": source_row_number_min,
        "source_row_number_max": source_row_number_max,
    }

    flags = {
        "grain_contract_match": contract_grain == GRAIN_CONTRACT,
        "forbidden_assumption_rejected": photo_rows != distinct_event_ids,
        "photo_rows_match": photo_rows == expected.get("photo_rows"),
        "distinct_event_ids_match": distinct_event_ids == expected.get("distinct_event_ids"),
        "fecha_min_match": fecha_min == expected.get("fecha_min"),
        "fecha_max_match": fecha_max == expected.get("fecha_max"),
        "db_apply_false": True,
        "sql_apply_false": True,
        "productive_loader_touched_false": True,
        "no_null_event_id_rows": null_event_id_rows == 0,
        "no_null_fecha_rows": null_fecha_rows == 0,
        "no_null_n_fotos_calculado_rows": null_n_fotos_calculado_rows == 0,
        "no_event_ids_multi_fecha": event_ids_multi_fecha == 0,
        "no_event_ids_multi_week": event_ids_multi_week == 0,
        "no_event_ids_multi_sp_item": event_ids_multi_sp_item == 0,
        "no_row_count_n_fotos_mismatch_events": row_count_n_fotos_mismatch_events == 0,
        "no_real_content_conflict_event_ids": real_content_conflict_event_ids == 0,
        "day_presence_is_binary": day_presence_is_binary,
        "source_row_number_present": "_source_row_number" in df.columns,
        "source_row_number_complete": source_row_number_null_rows == 0,
        "source_row_number_unique": source_row_number_distinct == photo_rows,
        "source_row_number_matches_excel_rows": source_row_number_matches_excel_rows,
    }

    payload = {
        **base_payload,
        "column_contract": {
            "event_stable_columns_used_for_hash": stable_cols,
            "photo_level_columns_excluded_from_hash": photo_level_cols,
        },
        "normalization": {
            "photo_row_to_event_row": "group_by_event_id",
            "event_identity": ["ID", "SP Item ID"],
            "event_key": "trim(ID)",
            "source_row_number": "one_based_excel_worksheet_row_after_header_resolution",
            "day_presence_key": ["fecha", "cod_rt_norm", "cliente_norm_key"],
            "day_presence_value": "binary_1_if_any_event",
        },
        "photo_row_traceability": {
            "source_row_number_field": "source_row_number",
            "excel_header_row_number": EXCEL_HEADER_ROW_NUMBER,
            "first_data_row_number": EXCEL_FIRST_DATA_ROW_NUMBER,
            "stability_scope": "source_workbook_and_sheet",
            "mapping_cardinality": "one_photo_row_to_one_source_row_number",
            "photo_rows_mapped": photo_rows - source_row_number_null_rows,
            "event_identity": ["ID", "SP Item ID"],
            "event_identity_replaced": False,
            "trace_manifest_algorithm": "sha256(source_row_number,event_id,sp_item_id,photo_row_hash)",
            "trace_manifest_sha256": trace_manifest_sha256,
            "sample_rows": source_trace_sample(df),
        },
        "metrics": metrics,
        "daily_coverage": daily_coverage,
        "day_presence_summary": {
            "rows": day_presence_rows,
            "max_events_per_day_presence": max_events_per_day_presence,
            "binary_presence_values": sorted(day_presence["presence"].unique().tolist())
            if day_presence_rows
            else [],
        },
        "flags": flags,
        "sample_event_rows": per_event.head(5).to_dict(orient="records"),
    }
    payload["verdict"] = "PASS_ROUTE_B_DRY_RUN" if all(flags[k] for k in BLOCKING_FLAG_KEYS) else "WARN_REVIEW_REQUIRED"
    return payload


def build_dry_run_payload(
    excel_path: Path,
    *,
    sheet_name: str = DEFAULT_SHEET,
    contract_path: Path = DEFAULT_CONTRACT,
) -> dict[str, Any]:
    contract = load_contract(contract_path)
    if not excel_path.exists():
        raise LoaderUsageError("excel_not_found", f"Excel file not found: {excel_path}")

    with pd.ExcelFile(excel_path, engine="openpyxl") as workbook:
        if sheet_name not in workbook.sheet_names:
            raise LoaderUsageError("sheet_not_found", f"Sheet not found: {sheet_name}")
        df = pd.read_excel(
            workbook,
            sheet_name=sheet_name,
            header=EXCEL_HEADER_ROW_NUMBER - 1,
        )
    return analyze_photo_dataframe(
        df,
        contract=contract,
        expected=expected_from_contract(contract),
        source_file=str(excel_path),
        source_file_sha256=sha256_file(excel_path),
        sheet_name=sheet_name,
    )


def redact_secret_text(text: str) -> str:
    text = re.sub(r"postgres(?:ql)?://[^ \n\r\t]+", "[REDACTED_DSN]", text, flags=re.IGNORECASE)
    return re.sub(r"password=[^& \n\r\t]+", "password=[REDACTED]", text, flags=re.IGNORECASE)


def safe_error_payload(exc: BaseException) -> dict[str, Any]:
    code = getattr(exc, "code", exc.__class__.__name__)
    return {
        "phase": PHASE,
        "loader_name": LOADER_NAME,
        "mode": "dry_run",
        "verdict": "FAIL",
        "error_code": str(code),
        "error": redact_secret_text(str(exc)),
        "db_apply": False,
        "sql_apply": False,
        "writes_executed": False,
        "dsn_printed": False,
        "productive_loader_path": PRODUCTIVE_LOADER_PATH,
        "productive_loader_touched": False,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dry-run KPIONE2 photo export grain validator.")
    parser.add_argument("--excel", default=str(DEFAULT_EXCEL))
    parser.add_argument("--sheet", default=DEFAULT_SHEET)
    parser.add_argument("--contract", default=str(DEFAULT_CONTRACT))
    parser.add_argument("--json-out")
    parser.add_argument("--dry-run", action="store_true", help="Run validation without DB or SQL apply.")
    parser.add_argument("--apply", action="store_true", help="Blocked guard: DB apply is not supported here.")
    return parser


def validate_cli_args(args: argparse.Namespace) -> None:
    if args.apply:
        raise LoaderUsageError("apply_not_supported_in_route_b_dry_run", "DB apply is RED and not implemented.")


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        validate_cli_args(args)
        payload = build_dry_run_payload(
            Path(args.excel),
            sheet_name=args.sheet,
            contract_path=Path(args.contract),
        )
        exit_code = 0 if payload.get("verdict") == "PASS_ROUTE_B_DRY_RUN" else 1
    except BaseException as exc:
        payload = safe_error_payload(exc)
        exit_code = 2 if isinstance(exc, LoaderUsageError) else 1

    if args.json_out:
        write_json(Path(args.json_out), payload)
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

```
