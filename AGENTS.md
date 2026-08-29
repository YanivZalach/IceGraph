# AGENTS.md

Instructions for coding agents working in IceGraph.

## Workflow

- Ask clarifying questions before changing code. Present a plan and wait for approval, even when the
  plan is one line.
- For bugs, explain the root cause before proposing or applying a fix.
- Make the smallest change that satisfies the request. Do not refactor, rename, or alter unrelated
  behavior.
- If a repository rule conflicts with a clearer or more correct solution, explain the conflict and
  ask before departing from the rule.

## Boundaries

- Never add a dependency without approval. Explain its purpose, why existing code is insufficient,
  and its maintenance cost. Read [`ARCHITECTURE_PHILOSOPHY.md`](ARCHITECTURE_PHILOSOPHY.md) before
  proposing dependencies, services, or persistent state.
- Never run `git add`, `git commit`, or `git push`. Read-only Git commands are allowed.
- Do not create or mutate issues, pull requests, releases, deployments, or other external state
  unless the user explicitly requests it.
- Do not disable lint, type, or formatting rules to make checks pass.
- Python files must not exceed 400 lines. This is a repository convention, not a formatter check.
- Define every backend runtime environment setting in `backend/env.py`.

## Product invariants

- IceGraph is read-only and targets Spark Connect backends with Iceberg Table Version 2.
- The backend owns Iceberg interpretation, collection, analysis, and graph normalization.
- The frontend presents normalized data and must preserve its meaning.
- Snapshot IDs and other large Iceberg integers must not lose precision.
- Supported URL parameters must round-trip without silent coercion or stale propagation.

## Required related updates

- Before frontend work, read
  [`docs/agents/frontend/development.md`](docs/agents/frontend/development.md) and
  [`docs/agents/frontend/philosophy.md`](docs/agents/frontend/philosophy.md).
- User-visible frontend behavior changes require the relevant Docs page content to be updated.
- Frontend route, path parameter, query parameter, or deep-link changes require
  `claude-plugin/skills/icegraph/SKILL.md` to be updated.
- Public `icegraph-client` API or CLI changes require its sections in `README.md` and the frontend
  Docs page to be updated.
- Changed API responses must remain compatible with the MSW demo or update its handlers.

## Verification

Run all applicable checks before presenting work. Fix errors without disabling rules.

### Backend

```bash
cd backend
uv run ruff format .
```

### Python client

Run when `icegraph-client` changes:

```bash
cd icegraph-client
uv run ruff format .
```

### Frontend

Use pnpm `11.24.0`, as pinned in `frontend/package.json`. If the installed version differs, use
Corepack. Ask before installing Corepack or pnpm globally.

```bash
cd frontend
pnpm run format
pnpm run lint
pnpm run typecheck
VITE_OUT_DIR=dist pnpm run build
```

Tests may be created and run temporarily while developing. Remove every temporary test file before
handoff and never commit it to IceGraph. Verify removal with `git status` and `git diff`.

Use focused behavioral checks that directly prove the change. Do not start a dev server or browser
session unless the user requests it or approves it. Report any required check that was not run and
why.

## Repository guidance

- GitHub issue usage: [`docs/agents/issue-tracker.md`](docs/agents/issue-tracker.md)
- Frontend workflow: [`docs/agents/frontend/development.md`](docs/agents/frontend/development.md)
- Frontend technical conventions:
  [`docs/agents/frontend/philosophy.md`](docs/agents/frontend/philosophy.md)
- Frontend smoke test:
  [`docs/agents/skills/frontend-smoke-test/SKILL.md`](docs/agents/skills/frontend-smoke-test/SKILL.md)
- Browser graph-cache contract: [`docs/browser-graph-cache.md`](docs/browser-graph-cache.md)

`CLAUDE.md` loads this file. `.claude/skills/frontend-smoke-test/SKILL.md` is a symlink to the smoke
test above; Claude Code only discovers skills at that path, so it is required, not a leftover.

`claude-plugin/skills/icegraph/SKILL.md` is the distributable skill for IceGraph users and is outside
this repository-guidance structure. `frontend/public/SKILL.md` is refreshed by `copy-skill`, while
`frontend/dist/SKILL.md` reflects the last build. Both are generated and must not be edited.
