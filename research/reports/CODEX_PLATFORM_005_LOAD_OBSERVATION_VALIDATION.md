# CODEX PLATFORM 005 Load Observation Validation

Phase: `FASE_PLATFORM_005_CODEX_INDEPENDENT_LOAD_OBSERVATION_VALIDATION`

Baseline commit: `9f19cca04164bd01b2a58a782671ce001a6da61c`

Verdict: `BLOCKED_BEFORE_FIRST_USE`

Quality target: `Q4_DECISION_GRADE`

Confidence: `HIGH`

## Scope And Guardrails

This validation was read-only against production code. No DB, network, Docker, SQL, loaders, product refresh, ledger write, skill edit, script edit, or test edit was performed. Synthetic adversarial fixtures were created only under `%TEMP%` and removed by the temp harness.

Authorized persistent outputs created:

- `research/PLATFORM_005_CODEX_VALIDATION_MATRIX.json`
- `research/reports/CODEX_PLATFORM_005_LOAD_OBSERVATION_VALIDATION.md`

## Integrity Results

- Target commit exists: yes.
- Commit changed exactly the declared three files: yes.
- Script compile: pass.
- Unit tests: 47 collected, 47 passed, 0 failed.
- Skill frontmatter: valid.
- `disable-model-invocation`: true.
- `allowed-tools`: only `Bash(python scripts/sz_load_observation.py *)`.
- Ledger exists: yes.
- Ledger initial SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- Ledger initial size: 0 bytes.
- Ledger records: 0.
- Static source safety: standard library imports only; no DB/network/subprocess import; no `shell=True`; no file write API found beyond stdout emission.

Commands executed:

```powershell
python scripts/sz_preflight.py --phase generic --root . --skip-db --json-out $env:TEMP\sz_preflight_platform005_codex.json
python -m py_compile scripts/sz_load_observation.py
python -m unittest discover -s tests -p "test_sz_load_observation.py" -v
```

Preflight result: `warn`, not blocking. Preexisting warnings were dirty worktree and `kernel_02_head_mismatch`.

## Contract Parity

Exact list parity is good for:

- `required_fields`
- `optional_fields`
- `sources`
- `labels`
- default label `UNREVIEWED`
- fixed `implementation_authorized=false`

However, behavioral parity is not complete. Privacy rules and semantic validation are not enforced strongly enough to make the generated candidate contractually safe.

## Blocking Findings

### PLATFORM005-F001 Path Scope Is Not Safe

`read_json_path` accepts any path that is a file and below 5 MB, then reads it before privacy scanning (`scripts/sz_load_observation.py:389-397`). Temp probes showed accepted valid JSON from an absolute external path, traversal path, synthetic `.env` filename, synthetic `.local_secrets` path, and synthetic `credentials.json`.

Consequence: the tool can read prohibited or sensitive files if invoked with such a path, even when it does not print raw file content.

Required correction: reject external absolute paths, traversal, symlink escapes, sensitive filenames/directories, and paths outside an allowed repo scope before opening the file.

### PLATFORM005-F002 Semantic Validation Is Insufficient

`draft` and `validate` mostly check presence, selected enums, dates for `recorded_at` and `effective_week_start`, evidence syntax, role, label, and observation ID. They do not validate many field types or cross-field relationships (`scripts/sz_load_observation.py:331-343`, `scripts/sz_load_observation.py:359-383`, `scripts/sz_load_observation.py:435-465`).

Temp probes accepted:

- `input_rows` as text
- negative row counts
- `accepted_rows > input_rows`
- booleans represented as strings
- invalid date optionals
- short, non-hex, null, and empty `input_file_sha256`

Consequence: the script can emit and validate candidates that are structurally present but contractually invalid.

Required correction: add explicit schema validation for all required and optional fields, including count relationships, boolean types, list element types, date formats, nullability, and SHA-256 hex format.

### PLATFORM005-F003 CLI/Input Contradictions Are Ignored

The script takes `source`, `effective_week_start`, and `operation_type` from CLI and does not compare them to the same fields when present in SHAPE A or SHAPE B (`scripts/sz_load_observation.py:406-429`).

Temp probes showed:

- phase source `RUTA_RUTERO` + CLI source `KPIONE2` emitted `KPIONE2`
- phase week `2026-06-08` + CLI week `2026-06-15` emitted `2026-06-15`
- phase operation `DRY_RUN` + CLI operation `APPLY` emitted `APPLY`

Consequence: evidence from one source/week/operation can be labeled as another.

Required correction: if phase JSON contains source, week, operation, or hash fields, require exact agreement with CLI or fail closed.

### PLATFORM005-F004 Observation ID Is Deterministic But Not Ledger-Suitable

`observation_id` hashes only source, week, operation, and `sha or ""` (`scripts/sz_load_observation.py:305-308`). Null and empty hash produce the same ID. The same file/source/week/operation with different `recorded_at` also produces the same ID.

Consequence: the future ledger could conflate distinct observations or accept IDs based on invalid identity inputs.

Required correction: require valid `input_file_sha256` and decide whether the ID is an idempotency key or an event identity. If event identity is required, add an explicit run/attempt discriminator.

## High Findings

### PLATFORM005-F005 Shape Extraction Is Too Permissive

SHAPE B is detected if any one detect key is present (`scripts/sz_load_observation.py:81-86`, `scripts/sz_load_observation.py:314-343`). Partial and accidental objects were accepted, producing candidates with null metrics. Contradictory canonical and alias values are silently resolved by precedence.

Required correction: require a complete minimal shape, reject canonical/alias contradictions, and fail when required technical fields are absent.

### PLATFORM005-F006 Privacy Is Heuristic

The scanner catches several forbidden classes but misses generic token-like fields, URL query tokens, and personal-name free text. It also blocks benign text containing the substring `secret` (`scripts/sz_load_observation.py:92-104`, `scripts/sz_load_observation.py:123-152`).

Required correction: constrain free-text fields, broaden credential detection, reject sensitive URL queries, and replace raw substring blocking with documented allowlist/denylist behavior.

### PLATFORM005-F007 Label Coherence Is Incomplete

Label rules check some required fields but allow contradictory operational states (`scripts/sz_load_observation.py:240-299`). Accepted probes included `CLEAN` in `PREFLIGHT` with `loader_executed=true`, `LOAD_FAILURE` with an approved postcheck, and `POST_LOAD_REGRESSION` without DB write.

Required correction: define operation-specific invariants for source-check, preflight, dry-run, apply, post-load validation, rollback, and each label.

## Medium Findings

### PLATFORM005-F008 Evidence Refs Are Syntax-Only

Evidence refs reject URLs, absolute paths, traversal, backslash, data paths, evidence paths, and Unicode. They do not check existence or length. Nonexistent commit, phase, research path, and very long research path refs were accepted.

This may be acceptable if syntax-only validation is intentional, but it must be documented.

### PLATFORM005-F009 Argparse Errors Are Not JSON-Normalized

`parse_args` runs before the `ObsError` wrapper (`scripts/sz_load_observation.py:510-520`). Missing required CLI args and unknown subcommands emitted argparse usage text on stderr rather than JSON.

Required correction: subclass or wrap `ArgumentParser.error` to emit normalized JSON.

## Determinism And Zero Writes

Determinism:

- Draft compact repeated byte-identically for same args.
- Draft pretty repeated byte-identically for same args.
- Validate repeated byte-identically for same args.

Ledger:

- Initial SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- Final SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- Unchanged: yes.

Git status was unchanged during the temp-only adversarial harness.

## First Real Use Assessment

Not ready for first real RUTA_RUTERO use.

The tool is safe in the narrow sense that it does not run loaders, DB, subprocesses, shell commands, or ledger writes. It is not safe as a candidate-preparation gate because it can read prohibited paths, mislabel phase evidence through CLI/input contradictions, emit semantically invalid candidates, and produce non-unique or invalid observation identities.

Required before first use:

1. Path allowlist and sensitive-path rejection before file open.
2. Full schema/type/count/hash/nullability validation.
3. CLI/input contradiction detection.
4. Ledger-suitable identity design.
5. Complete shape requirements and alias contradiction checks.
6. Stronger privacy controls for free text and token-like data.
7. Operation-specific label invariants.
8. JSON-normalized argparse errors.
