# CLAUDE BAYES 001 — Independent Value Assessment

**Task:** `CLAUDE_BAYES_001_INDEPENDENT_VALUE_ASSESSMENT`
**Verdict:** `NOT_YET_JUSTIFIED`
**Quality target / achieved:** `Q4_DECISION_GRADE` / `Q4_DECISION_GRADE` (confidence MEDIUM–HIGH)
**Baseline commit:** `e5eef29` · **Runtime code baseline:** `2c135a7`
**Authority:** `implementation_authorized=false`, `db_access=none`, no loaders/SQL/Docker run, no personal data written.

This is an independent research desk assessment. It does not authorize implementation and is
pending Codex independent validation.

---

## 1. Question

`HYPOTHESIS_BAYES_001`: a probabilistic decision-support layer can improve quality, efficiency,
and learning of repeated decisions in STOCK_ZERO and the multi-agent flow, above static rules or
deterministic scorecards, without measurement/maintenance/complexity cost exceeding the benefit.

The assessment was conducted against five mandatory alternatives — `RULES_ONLY`,
`DETERMINISTIC_SCORECARD`, `BAYESIAN_DECISION_SUPPORT`, `HYBRID`, `DO_NOT_BUILD` — and was not
steered toward any predetermined conclusion. The pilot was allowed to be null.

## 2. Executive conclusion

There **is** real, repeated uncertainty in the project, so a Bayesian layer is **not rejected**.
But **no decision surface currently satisfies all the justification criteria at once**:

- The strongest *technical* seam — per-batch **data-load anomaly scoring** — is exchangeable,
  observable, and already partially logged, yet limited by **small sample and sparse failure labels**.
- The highest-*value* product seams — **alert→action efficacy** and **operational prioritization** —
  have **no live feedback loop**, because action/outcome tracking is not implemented.
- **AI-workflow** and **engineering** decisions are largely **non-exchangeable** (one-off,
  path-dependent) or **already rules-sufficient**.
- Every **product/CG** decision surface inspected is **deterministic by design today**.

Proportionate move: **`DO_NOT_BUILD` now**, with at most a cheap, private, append-only
**observation contract** on the one strong seam so the question becomes answerable later.

## 3. Evidence quality legend

Each material claim below is tagged `FACT` / `INFERENCE` / `HYPOTHESIS` / `RECOMMENDATION` with a
confidence and a falsification condition. Full machine-readable classification is in
`research/BAYES_001_INDEPENDENT_ASSESSMENT.json` → `evidence_classification`.

## 4. Where decisions are deterministic today (FACT, HIGH)

- **Inventory focos** are boolean flags, not probabilities:
  `Venta(+7)==0`, `NEGATIVO=='SI'`, `RIESGO DE QUIEBRE=='SI'`
  (`app/screens/reposicion.py` → `_row_indicadores`).
- **Control Gestión alerts** `CUMPLE`/`INCUMPLE` derive deterministically from capped valid visits
  vs plan (`app/screens/control_gestion.py`); `alert_generation` is a `FUTURE_CAPABILITY`.
- **Safe-load go/no-go** is an exact staged-validate-then-commit gate
  (`refresh_control_gestion_v2_incremental.py`).
- **Canonical parity** is deterministic: 3 runs + clean-room produced **1 variant** for each of
  key/business/technical hashes, `ALERT` diff keys = 0 (C007).

> _Falsification:_ find probability/threshold-with-uncertainty logic feeding any of these surfaces.

## 5. Where the feedback loop is missing (FACT, HIGH)

`decision_and_action_tracking` has **empty `current_implementation`** in
`research/AI_CAPABILITY_MAP.json`. No action→outcome ledger exists, so "did this action fix the
alert?" cannot be learned today regardless of method.

## 6. Alternatives — fit today

| Alt | Fit now | Why |
| --- | --- | --- |
| A `RULES_ONLY` | **Strong** | Correct for safety/irreversible and one-off decisions (gates, go/no-go, schema contracts). |
| B `DETERMINISTIC_SCORECARD` | **Good next step** | Right ceiling for anomaly bands and build/reuse/retire checklists. |
| C `BAYESIAN_DECISION_SUPPORT` | **Not yet** | Best theoretical fit for load-anomaly and action-efficacy, but blocked by sample / missing feedback loop. |
| D `HYBRID` | **Preferred eventual shape** | Hard rules keep loads safe; Bayes flags only the uncertain middle — *after* observation. |
| E `DO_NOT_BUILD` | **Correct for now** | Incremental benefit does not yet justify complexity; pair with a cheap observation contract. |

## 7. Domain assessments (summary)

Full 15-dimension scoring (1–5) for 12 use cases is in
`research/BAYES_001_USE_CASE_MATRIX.json`. Headlines:

- **AI_WORKFLOW** (model/effort selection, validate-vs-ship): high frequency but **non-comparable
  units**, continuous process change, coarse/confounded labels, high measurement cost; safety
  validation is already a correct hard rule. → *not justified* (INFERENCE, MEDIUM).
- **ENGINEERING** (phase ordering, build/reuse/retire, value-of-information): phase/architecture
  decisions are **one-off and non-exchangeable**; build/reuse/retire is at most a scorecard; VoI is
  better served by an explicit gate-criteria stop rule. → *not justified* (INFERENCE, MEDIUM–HIGH).
- **DATA_LOAD_RELIABILITY** (batch anomaly, schema-change risk, safe-load): **UC-DL-01 is the
  strongest seam** — exchangeable, observable, metrics already logged (`batch_registry` 37 CG
  batches; 19 route batches) — but **small n and sparse failure labels** mean a distribution would
  not yet beat a fixed band. Schema-change and go/no-go are rules-sufficient. → *not justified yet,
  natural home for instrumentation* (INFERENCE, MEDIUM).
- **PRODUCT_AND_APP** (prioritization, alert→action efficacy, dead-SKU): today's surfaces are
  deterministic; the probabilistic questions need an **action/outcome ledger that does not exist**,
  plus privacy gating and a causal design for worker-level efficacy. → *premature, instrument first*
  (INFERENCE, MEDIUM).
- **PERSONAL_GROWTH_PRIVATE**: evaluated **as a conceptual contract only**, highest privacy risk,
  must stay in **private, git-ignored** storage separate from the technical rail. **No real personal
  data was recorded.** (RECOMMENDATION, LOW).

## 8. First finding — should this be a parallel priority now?

**No, not as a parallel build priority** (RECOMMENDATION, MEDIUM–HIGH). The benefit of starting now
is low (no live feedback loop, small samples), the distraction/over-engineering cost is real, and it
depends on data that does not yet exist. The four priority tiers are deliberately **different**:

| Priority tier | Level | Rationale |
| --- | --- | --- |
| Conceptual | LOW–MEDIUM | Keep the probabilistic lens alive; don't let it compete with operational lanes. |
| Instrumentation | LOW–MEDIUM | The **only** tier that may start: a cheap, private, append-only observation log on UC-DL-01. |
| Implementation | NONE NOW | No model until labels accrue and a distribution beats a rule. |
| Production | NONE NOW | Collides with active gates (G0 parity, CG-005) and forbidden actions. |

## 9. Pilot

**`NO_PILOT_YET`.** No use case currently meets all justification criteria simultaneously. The
recommended next step is an **observation/measurement contract**, not a Bayesian pilot.

**Candidate first pilot, *when justified*** (recorded, not selected): a **shadow, read-only,
retrospective `HYBRID` anomaly band for UC-DL-01**.

- **Success:** on retrospective replay, the band flags all labeled bad batches **and** beats a fixed
  band on false-positive rate at equal recall.
- **Failure:** posteriors too wide to separate good/bad, or no improvement over a fixed band.
- **Abandon:** fewer than ~3–5 labeled anomalies per source accrue, or rules already catch every case.
- **Minimum observations:** ~20–30 clean + ≥3–5 labeled anomalous batches per source.
- **Reversibility:** HIGH (pure analysis over historical evidence; nothing applied).
- **Privacy:** LOW (batch metadata, no PII).
- **Constraint:** do **not** trigger loads to gather data; let batches accrue from normal operations.

## 10. Econometrics & causality (kept separate)

- **Prediction** (dead-SKU) ≠ **decision support** (anomaly flag) ≠ **association** (drift correlates
  with problems) ≠ **causal effect** (action efficacy) ≠ **counterfactual** ("would compliance have
  improved anyway"). No causal claim is asserted.
- Future methods, only when a design and data exist: local-cliente-week panel, hierarchical models,
  frequency/route/responsible-change studies, time series, synthetic/counterfactual controls. **No
  causal model is designed here.**

## 11. Falsification conditions

- Flip toward `SUPPORTED_FOR_PILOT` if a seam shows, on **real logged data**, a calibrated
  distribution that changes a repeated decision **and** beats a fixed rule at equal recall.
- UC-DL-01 specifically: accumulate the minimum labeled set per source and show a predictive band
  strictly dominating a fixed band on the FP/recall frontier.
- Move toward `REJECTED` if, after instrumentation, deterministic rules match the probabilistic layer
  on every labeled case.
- The determinism claim itself is falsified if any inspected surface (focos, CUMPLE/INCUMPLE) is shown
  to already consume a hidden probability — a re-read of the underlying SQL would settle it.

## 12. Unresolved dependencies

- Exact per-source count of historically labeled bad batches (needs read-only `batch_registry`
  inspection in an authorized phase).
- Whether any downstream consumer already wants a probability (re-check `app/db.py` SQL in an
  authorized phase).
- **Codex independent validation** of this assessment.

## 13. Scope discipline

This assessment did **not** modify or block the parallel lanes (RUTA_RUTERO load, KPIONE2 format,
frequency loads, CG-005, loaders, Supabase, Docker). It is limited to research, design, and
prioritization. Only the three authorized artifacts were created.

---

**Next step:** Codex independent validation.
