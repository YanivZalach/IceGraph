# Agent Guidance

This directory is the canonical home for repository-local agent guidance and internal skills.

## Repository instructions

- [`../../AGENTS.md`](../../AGENTS.md) contains the primary repository instructions and project map.
- [`../../CLAUDE.md`](../../CLAUDE.md) loads `AGENTS.md` for Claude Code.

These entry points remain at the repository root so supported agents discover them automatically.

## Guidance

- [`domain.md`](domain.md) describes domain documentation and ADR conventions.
- [`issue-tracker.md`](issue-tracker.md) describes GitHub issue workflows.
- [`frontend/development.md`](frontend/development.md) describes frontend development and navigation.
- [`frontend/philosophy.md`](frontend/philosophy.md) defines frontend engineering rules.

The legacy frontend paths remain as symlinks so existing links continue to work.

## Internal skills

- [`skills/frontend-smoke-test/SKILL.md`](skills/frontend-smoke-test/SKILL.md) is the canonical frontend smoke-test skill.

Claude Code discovers that skill through the compatibility symlink at
`.claude/skills/frontend-smoke-test/SKILL.md`.

## External IceGraph skill

`claude-plugin/skills/icegraph/SKILL.md` is the distributable skill for IceGraph users. It is not
repository-local agent guidance and remains in its existing location. Copies under
`frontend/public/` and `frontend/dist/` are generated artifacts.
