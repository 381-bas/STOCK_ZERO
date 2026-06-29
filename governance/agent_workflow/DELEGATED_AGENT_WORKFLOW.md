# Delegated Agent Workflow

## Purpose

This workflow reduces manual copy/paste while allowing Claude and Codex to operate as complementary competitors.

The goal is not to remove control. The goal is to move control into durable repo artifacts, PRs and explicit gates.

## Roles

| Agent | Role | Primary value |
|---|---|---|
| ChatGPT | Controller, gatekeeper and continuity layer | Keeps phase logic coherent |
| Claude | Critical auditor and contract reviewer | Finds contradictions and risk |
| Codex | Executor and implementation agent | Builds, tests and produces evidence |
| Bastián | Owner of irreversible decisions | Authorizes RED actions |

## Operating model

1. ChatGPT validates bootstrap state and proposes a phase lock.
2. Codex may implement only after the phase lock allows implementation.
3. Claude audits contract, scope and risk.
4. Codex responds to Claude with changes or written counterarguments.
5. ChatGPT arbitrates using committed artifacts and PR evidence.
6. Bastián authorizes only irreversible or business-critical gates.

## Semaphore authority

### GREEN

Autonomous.

Examples:

- read repo
- create research notes
- create governance drafts
- run non-destructive validations
- open draft PRs

### YELLOW

Autonomous inside branch, but must remain reviewable.

Examples:

- tests
- dry-run helpers
- SQL files as files only
- rollback drafts
- non-production script changes

### ORANGE

Requires cross-audit.

Examples:

- loader logic
- active contracts
- compliance calculation logic
- route/frequency resolution
- critical runtime queries

### RED

Requires explicit Bastián authorization.

Examples:

- DB apply
- SQL apply against Supabase real
- DROP/TRUNCATE/DELETE
- data movement
- secret changes
- merge to main
- worktree deletion
- production cutover

## Copy/paste reduction rule

Important outputs from Claude and Codex should be persisted as:

- committed files under `governance/`, `contracts/` or `research/`
- PR comments
- review comments
- validation artifacts

User terminal paste is required only when local state cannot be inspected through GitHub or when a RED gate is involved.

## Branch lifecycle

Branches are temporary execution tools, not memory.

Memory lives in:

- `governance/`
- `contracts/`
- `research/`
- merged PRs

After merge, phase branches should be deleted if they are not worktree-bound.
