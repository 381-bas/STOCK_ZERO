---
description: Validated facts and hard constraints for Control Gestión work.
paths:
  - app/screens/control_gestion.py
  - scripts/refresh_control_gestion_v2_incremental.py
  - scripts/load_control_gestion_raw_v17.py
  - sql/**/*control_gestion*
  - research/**/*G0*
  - research/**/*PARITY*
---

# Control Gestión rules

These are validated facts (C001–C004, Codex-validated). Treat them as constraints, not
suggestions. They do not authorize implementation.

## What Control Gestión is

- Control Gestión is the predominant future core of the project.
- V2 is the primary path.
- B3 remains an active fallback. Do not remove it; make it observable only.

## Temporal contract (mandatory)

- RUTA_RUTERO is corrected retroactively for the whole week. There is NO intra-week validity:
  a later route load for a week reattributes that entire week.
- Never select `latest raw` or `latest route` implicitly. Every build must explicitly pin:
  - affected_date_window (start/end),
  - raw_batch_set per source (KPIONE, KPIONE2, POWER_APP),
  - the weekly route snapshot (route_snapshot_by_period),
  - source_precedence_version,
  - daily_builder_version and weekly_builder_version.
- Weekly is built from the staged daily surface, not recomputed from raw. The raw-recomputing
  view path (v_cg_out_weekly_v2) is the timeout path; mirror the staged `_cg_weekly_stage` recipe.

## Data surfaces

- payload_json is REQUIRED_CURRENT_SURFACE. It cannot be removed without proven equivalent
  consumers and parity (KPIONE2 SP Item ID lives only in payload).
- cg_core.ruta_rutero_load_rows is a live dependency, not audit-only.
- row_hash is an analysis fingerprint, not the UPSERT key.

## Gating

- G0 parity (daily + weekly + reproducibility) must close before any retention, deletion,
  archival, or cleanup of Supabase raw data.
- The route policy decision is semantic (Bastián); the technical snapshot lineage is Codex's.
  G0 parity pins the existing baseline's lineage; operational policy governs future builds.
- No DB, Docker, loader, refresh, SQL, commit, or push without an explicit authorizing task.
