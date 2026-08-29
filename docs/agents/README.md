# Agent Guidance

Canonical repository-local guidance:

- [`../../AGENTS.md`](../../AGENTS.md): workflow, boundaries, invariants, and verification
- [`issue-tracker.md`](issue-tracker.md): how to interpret and update IceGraph issues
- [`frontend/development.md`](frontend/development.md): frontend implementation workflow
- [`frontend/philosophy.md`](frontend/philosophy.md): frontend technical conventions
- [`skills/frontend-smoke-test/SKILL.md`](skills/frontend-smoke-test/SKILL.md): deterministic frontend
  smoke test

`CLAUDE.md` loads `AGENTS.md`. Legacy frontend guidance paths and the Claude smoke-test discovery
path are compatibility symlinks to these canonical files.

`claude-plugin/skills/icegraph/SKILL.md` is the distributable skill for IceGraph users and is outside
this repository-guidance structure. Copies under `frontend/public/` and `frontend/dist/` are
generated.
