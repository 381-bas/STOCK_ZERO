# FAST_REFORM_009F Repo Organization Cleanup Summary

## Purpose

This cleanup phase inventories repo organization debt after PR #19, PR #20 and PR #21 were merged to `main`.

This phase does not delete branches, delete worktrees, move data, change loaders, apply SQL, or start 009G.

## Base

`main @ 5148b5d`

## Current cleanup branch

`lab/FAST_REFORM_009F_repo_organization_cleanup`

## Current status

Only `governance/cleanup/` is being added as evidence.

## Safe delete candidates after this manifest is reviewed

These branches are merged to `main` and are not worktree-bound:

- `lab/FAST_REFORM_009F_loader_structure_validation`
- `lab/FAST_REFORM_009F_governance_promotion`
- `lab/FAST_REFORM_009F_bootstrap_protocol_and_thread_closeout`

Remote counterparts:

- `origin/lab/FAST_REFORM_009F_loader_structure_validation`
- `origin/lab/FAST_REFORM_009F_governance_promotion`
- `origin/lab/FAST_REFORM_009F_bootstrap_protocol_and_thread_closeout`

Deletion is not performed in this manifest commit. It requires a separate explicit gate.

## Keep because worktree-bound

- `codex/CG005G-route-lock-rollback-correction`
- `codex/CG005N-package-correction`
- `codex/PLATFORM_005B-load-observation-correction`
- `codex/supabase-cleanup-no-drop-register`
- `lab/CG005I_M-route-weekly-replacement`

These branches are connected to worktrees and must not be deleted in this cleanup pass.

## Guardrails

Forbidden in this manifest commit:

- DB apply
- 009G implementation
- SQL apply
- loader patch
- file deletion
- branch deletion
- worktree deletion
- data movement
- `git add .`
