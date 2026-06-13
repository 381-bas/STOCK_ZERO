# CODEX BAYES 001 - Independent Validation

**Phase:** `FASE_BAYES_001_CODEX_INDEPENDENT_VALIDATION_NO_IMPLEMENTATION`  
**Codex verdict:** `VALIDATED_WITH_CORRECTIONS`  
**Validated commit:** `1e08ac20c3a3a812bcb2790d01f097eb022d3cbb`  
**DB / Docker / loaders / Supabase:** not used  
**Implementation authorized:** `false`

## Executive Result

Codex independently validates the main direction of Claude's assessment:

- `NOT_YET_JUSTIFIED` is supported.
- `DO_NOT_BUILD` now plus `OBSERVE_ONLY` for UC-DL-01 is superior to building a Bayesian or hybrid probabilistic implementation today.
- UC-DL-01 is the best first technical candidate when evidence exists, but it is not pilot-ready now.

The validation is not a blanket `VALIDATED` because four corrections are required:

1. The phase validates commit `1e08ac20...`, while Claude's JSON/report metadata state `baseline_commit=e5eef29...`.
2. The report still references active `G0 parity`; the live horizon is `H1 Real Week Dual-Run Shadow` with gate `CG-005`.
3. The proposed `20-30 clean + 3-5 anomalous` threshold is a reasonable heuristic, not a formal sample-size justification.
4. Claude's self-grade `Q4_DECISION_GRADE` is too strong for this allowed scope; Codex grades the result as `Q3_DECISION_GRADE_WITH_CORRECTIONS`.

These corrections do not reverse the recommendation. They make the decision record sharper and better calibrated.

## Integrity

The requested commit exists and `HEAD` matched `1e08ac20c3a3a812bcb2790d01f097eb022d3cbb` during validation. That commit added exactly:

- `research/BAYES_001_INDEPENDENT_ASSESSMENT.json`
- `research/BAYES_001_USE_CASE_MATRIX.json`
- `research/reports/CLAUDE_BAYES_001_INDEPENDENT_ASSESSMENT.md`

The two JSON artifacts parse correctly. The reviewed artifacts declare `implementation_authorized=false`, `db_access=none`, no loaders, no Docker, and no personal data written. A text scan found no DSN or credential material; the only hits around personal data/secrets were policy statements saying they were not written.

## Central Claims

| Claim | Codex classification | Rationale |
| --- | --- | --- |
| Current Inventory and Control Gestion surfaces are mostly deterministic | `SUPPORTED` | `reposicion.py` uses fixed flags for `Venta(+7)`, `NEGATIVO`, `RIESGO DE QUIEBRE`; `control_gestion.py` renders fixed `ALERTA` surfaces; C007 validates deterministic canonical outputs. |
| `decision_and_action_tracking` is not functionally implemented | `SUPPORTED` | `AI_CAPABILITY_MAP.json` lists it as future capability with `current_implementation=[]`. |
| Architecture and phase decisions are path-dependent and poorly exchangeable | `SUPPORTED` | `AI_PROJECT_HORIZON.json` defines dependency-gated H0-H5 horizons and Bastian decision points. |
| Load reliability is the most repetitive and observable candidate surface | `SUPPORTED` | Shared memory records CG batch registry behavior and route batch history; incremental refresh exposes repeated validation states. |
| Observations/anomaly labels are insufficient for a probabilistic layer | `PARTIAL` | Small aggregate counts and no label corpus are supported, but exact per-source bad/good labels require a future DB/evidence phase. |
| There is not enough evidence that Bayes beats a rule or scorecard today | `SUPPORTED` | No use case is `BAYES_SUITABLE_NOW`; UC-DL-01 still needs labels and a deterministic baseline. |
| Personal-growth rail must stay separate and private | `SUPPORTED` | Both artifacts keep it conceptual/private and state no personal data was written. |

Counts: `SUPPORTED=6`, `PARTIAL=1`, `DISPUTED=0`, `REJECTED=0`, `NOT_VERIFIABLE=0`.

## Alternatives

The five alternatives were compared fairly:

- `RULES_ONLY`
- `DETERMINISTIC_SCORECARD`
- `BAYESIAN_DECISION_SUPPORT`
- `HYBRID`
- `DO_NOT_BUILD`

Codex does not find undue favoring of `DO_NOT_BUILD`. The assessment gives Bayes/HYBRID a future role where uncertainty, labels, and feedback loops exist, while preserving rules for safety gates and deterministic surfaces. That is proportionate to the current evidence.

## Matrix Audit

The 12-case matrix is internally coherent enough for a decision memo. It does not rely on a single aggregate score, which reduces aggregation bias. Still, several dimensions partially overlap: `outcome_observability`, `data_availability`, and `measurement_cost` are not fully independent; likewise `implementation_cost` and `measurement_cost` overlap for uninstrumented product cases.

Some values are judgment calls, not measured facts. That is acceptable for research triage, but it should be labeled as such. The most important example is UC-DL-01's observation trigger: `20-30 clean` plus `3-5 anomalous` batches per source is a sensible review threshold, not a proof that a Bayesian model would be identifiable or superior.

Simple rules remain sufficient for UC-AW-02, UC-EN-01, UC-EN-02, UC-DL-02, and UC-DL-03. Probabilistic uncertainty could matter later in UC-DL-01, UC-PA-02, and possibly UC-PA-03, but only after the missing observation contracts exist.

## UC-DL-01

Codex agrees that batch anomaly scoring is the best first candidate when justified. It is exchangeable enough, operationally relevant, low privacy risk, and already close to existing batch metrics.

It is not pilot-ready:

- The metrics are sufficient for observation, not model fitting.
- Good/bad labels must be created or confirmed.
- A deterministic fixed-band or scorecard baseline must be the first comparator.
- The pilot status should remain `NO_PILOT_YET`.

Future comparison should be enabled only after normal operations produce labeled per-source batch outcomes. The fair test is retrospective and read-only: compare fixed deterministic bands against predictive bands at equal recall, and require fewer false positives without missing labeled bad batches.

## Priority

| Priority tier | Codex validation |
| --- | --- |
| Conceptual | `LOW_MEDIUM` |
| Instrumentation | `LOW_MEDIUM_OBSERVE_ONLY` |
| Implementation | `NONE_NOW` |
| Production | `NONE_NOW` |

This does not interfere with `RUTA_RUTERO`, the new `KPIONE2` format, frequency loads, `CG-005`, or `H1`, because no runtime, loader, DB, SQL, or Supabase work is authorized.

## Temporal Correction

The Claude report says production priority collides with active gates including `G0 parity` and `CG-005`. That wording is stale. The live horizon is `H1 Real Week Dual-Run Shadow`, with `CG-005` active.

Material impact: `false`. The conclusion is unchanged because implementation and production priority remain `NONE_NOW`.

Required correction: documentary.

## Quality

| Dimension | Codex assessment |
| --- | --- |
| contract_adherence | `HIGH` |
| evidence_quality | `MEDIUM_HIGH` |
| validation_strength | `MEDIUM_HIGH_FOR_ALLOWED_SCOPE` |
| alternative_fairness | `HIGH` |
| falsifiability | `HIGH` |
| causal_discipline | `HIGH` |
| operational_utility | `MEDIUM_HIGH` |
| confidence calibration | `IMPROVED_BY_DOWNGRADING_Q4_TO_Q3_WITH_CORRECTIONS` |

**Quality target:** `Q4_DECISION_GRADE`  
**Quality achieved by Codex:** `Q3_DECISION_GRADE_WITH_CORRECTIONS`  
**Confidence:** `MEDIUM_HIGH`  
**Rework required:** `DOCUMENTARY_ONLY`

## Final Verdict

`VALIDATED_WITH_CORRECTIONS`

The Bayesian layer should not be built now. The only defensible near-term action is observation-only instrumentation planning for UC-DL-01, without DB writes, loaders, Supabase changes, runtime changes, or product claims.
