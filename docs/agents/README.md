# Agent Guidance

Canonical repository-local guidance:

- [`../../AGENTS.md`](../../AGENTS.md): workflow, boundaries, invariants, and verification
- [`issue-tracker.md`](issue-tracker.md): how to interpret and update IceGraph issues
- [`frontend/development.md`](frontend/development.md): frontend implementation workflow
- [`frontend/philosophy.md`](frontend/philosophy.md): frontend technical conventions
- [`skills/frontend-smoke-test/SKILL.md`](skills/frontend-smoke-test/SKILL.md): deterministic frontend
  smoke test
- [`../browser-graph-cache.md`](../browser-graph-cache.md): browser graph-cache contract

`CLAUDE.md` loads `AGENTS.md`. Legacy frontend guidance paths and the Claude smoke-test discovery
path are compatibility symlinks to these canonical files.

`claude-plugin/skills/icegraph/SKILL.md` is the distributable skill for IceGraph users and is outside
this repository-guidance structure. `frontend/public/SKILL.md` is refreshed by `copy-skill`, while
`frontend/dist/SKILL.md` reflects the last build. Both are generated and must not be edited.
