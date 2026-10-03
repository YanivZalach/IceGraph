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
- After the approved change is implemented and its checks pass, run the
  [review cycle](#review-cycle) before presenting the work.
- When working in a Git worktree, the plan must ask how to run the application; see
  [Running from a worktree](#running-from-a-worktree).

## Boundaries

- Never add a dependency without approval. Explain its purpose, why existing code is insufficient,
  and its maintenance cost. Read [`ARCHITECTURE_PHILOSOPHY.md`](ARCHITECTURE_PHILOSOPHY.md) before
  proposing dependencies, services, or persistent state.
- Never run `git add`, `git commit`, or `git push`. Read-only Git commands are allowed.
- Do not create or mutate issues, pull requests, releases, deployments, or other external state
  unless the user explicitly requests it.
- Do not disable lint, type, or formatting rules to make checks pass.
- Python files must not exceed 400 lines. This is a repository convention, not a formatter check.
- In Python, use an explicit `for` loop with `break` for first-match searches. Do not use
  `next(...)` with a generator expression for these lookups.
- Define every backend runtime environment setting in `backend/env.py`.
- Never write docstrings, or comments in Python code, unless the user asks for them. Exception: follow an
  existing per-entry comment convention, such as the one-line comment above each setting in `backend/env.py`.

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

### Review cycle

1. Spawn a separate review agent. Instruct it to only read: no file edits, no Git writes, no dev
   servers or browser sessions.
2. Give it the goal of the change, the decisions the user already approved and which must not be
   re-litigated, this file and any related guidance to read, and the risks to examine. Ask it to
   verify each suspicion against the code, and to report numbered findings, most severe first,
   each marked MUST FIX or OPTIONAL with file, line, triggering scenario, and recommended fix,
   followed by what it checked and found correct.
3. Fix every MUST FIX finding. Apply an OPTIONAL finding only when it is small and within the
   approved scope; otherwise record why it was skipped.
4. Rerun the applicable checks, then send the same agent the updated diff with what changed and
   what was deliberately left unchanged.
5. Repeat until the agent reports that nothing must be fixed. Then present the work with the
   findings fixed, the findings skipped and why, and any check that was not run.

The review agent's report is not user approval. A fix that goes beyond the approved plan or changes
agreed behavior still requires the user's approval first.

## Development ports

The backend listens on `APPLICATION_PORT` (default `5050`), defined in `backend/env.py`. The Vite
dev server proxies `/api` to `VITE_DEV_BACKEND_PORT` (default `5050`), read in
`frontend/vite.config.ts`. The two are independent: to run the backend on another port, such as a
second copy beside one already running, set both to the same value.

```bash
cd backend
APPLICATION_PORT=5051 uv run python main.py
```

```bash
cd frontend
VITE_DEV_BACKEND_PORT=5051 pnpm run dev
```

The Vite dev server itself uses port `3000` and moves to the next free port when it is taken.

### Running from a worktree

A worktree usually runs beside the main checkout, whose servers already hold the default ports. The
checkout is a worktree when `git rev-parse --git-dir` and `git rev-parse --git-common-dir` differ.

When presenting the plan from a worktree, ask the user whether to run the application in a new tmux
session or a new [herdr](https://herdr.dev/docs/) session. tmux is the default: a plain approval of
the plan means tmux. Either answer is the approval that [Verification](#verification) requires for a
dev server; a browser session still needs its own approval. If the chosen tool is not installed, say
so and ask; do not install it.

Prepare the worktree first. Git-ignored files are missing from a new worktree: copy `backend/.env`
from the main checkout, and run `pnpm install` in `frontend`. `uv run` creates the backend
environment itself.

Choose a backend port that is not in use, for example with `ss -ltn`, and pass it as both
`APPLICATION_PORT` and `VITE_DEV_BACKEND_PORT`. Never stop or reconfigure servers that belong to the
main checkout or to another session.

Either way, create a new session named `icegraph-<worktree directory name>` with the backend in a
left pane and the frontend in a right pane, so the user can attach and watch both servers and their
logs. Start each server inside its pane's shell rather than as a detached process.

For tmux, use one window split into two panes:

```bash
tmux new-session -d -s icegraph-<name> -c <worktree>/backend
tmux split-window -h -t icegraph-<name> -c <worktree>/frontend
tmux send-keys -t icegraph-<name>.{left} 'env APPLICATION_PORT=<port> uv run python main.py' Enter
tmux send-keys -t icegraph-<name>.{right} 'env VITE_DEV_BACKEND_PORT=<port> pnpm run dev' Enter
```

For herdr, build the same layout with its CLI.

Confirm both servers are listening, and read the frontend URL from its pane, since Vite may have
moved to another port. Then report the backend port, the frontend URL, and the command to attach to
the session. Leave the servers running at handoff and say how to stop the session.

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
