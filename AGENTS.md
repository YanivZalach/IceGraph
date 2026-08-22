# AGENTS.md

This file provides guidance to coding agents working in this repository.

## Before any work

- **Never start coding immediately.** First ask the user clarifying questions, then present a plan and get their approval. Only implement after the plan is agreed.
- If the task seems trivial and you think planning is overkill, say so and propose a one-line plan, but still wait for the user's go-ahead.

## Bugs

- **Never just apply a fix.** Explain the root cause first and let the user decide how to proceed. Diagnosis before surgery.

## Before presenting work

- Run `format`, `lint`, and `typecheck` (see **Commands**; the backend has formatting only); fix all violations before showing the user anything. Never fix a violation by disabling a rule.

## Scope & Code Change Policy

- Don't refactor, rename, or "improve" code the user didn't ask you to touch. Never change behavior that was not explicitly requested.
- Keep all changes as minimal as possible unless explicitly asked for more. Prefer the simplest solution that works.
- **Never add a dependency without asking the user first**: present what it does, why hand-rolling is worse, and its cost. Read [ARCHITECTURE_PHILOSOPHY.md](ARCHITECTURE_PHILOSOPHY.md) before proposing new dependencies, services, or persistent state: weigh every such change against its pillars.
- Apply DRY principles where possible.
- Python files must not exceed 400 lines.
- Agents may run read-only Git commands such as `git status` and `git diff`. Never run `git add`, `git commit`, or `git push`: those commands are reserved for the user.
- When `icegraph-client`'s public API or CLI commands change, update the CLI section in `frontend/src/pages/DocsPage.jsx` and the Python Client & CLI bullet in `README.md` to match.
- Whenever creating a setting that is configurable via an environment variable, define it in `backend/env.py`.
- Before modifying anything under `frontend/`, read and follow [frontend/DEVELOPMENT.md](frontend/DEVELOPMENT.md) and [frontend/PHILOSOPHY.md](frontend/PHILOSOPHY.md).

## Project Overview

IceGraph is an interactive Apache Iceberg debugging and visualization platform. It provides a hierarchical, graph-based view of Iceberg table metadata to help engineers debug complex table states and trace metadata evolution. It is **read-only** and built exclusively for **Spark Connect** backends targeting **Iceberg Table Version 2**.

## Commands

### Backend (Python + UV)

```bash
cd backend
uv sync                    # Install dependencies
uv run python main.py      # Start Flask server on port 5050
uv run ruff format .       # Format (CI enforces this)
```

### Frontend (Node/React)

```bash
cd frontend
npm i                      # Install dependencies
npm run dev                # Start Vite dev server on port 3000 (proxies /api to port 5050)
npm run build              # Production build to /dist
npm run lint               # ESLint
npm run format             # Prettier
npm run typecheck          # tsc --noEmit
```

### Docker

```bash
docker build -t icegraph .                                          # Multi-stage build
docker run -e SPARK_REMOTE=sc://<ip>:15002 -p 5050:5050 icegraph   # Run container

cd docker_demo && docker compose up   # Full demo stack with mock Iceberg tables
```

### icegraph-client (Python client + CLI)

```bash
cd icegraph-client
uv sync                                                    # Install dependencies (editable install)
uv run icegraph --base-url http://localhost:5050 tables    # Run the CLI against a local backend
```

## Architecture

**Backend** (`/backend/`): Flask API that reads Iceberg metadata via Spark Connect:

- `main.py`: Flask app with API routes; uses `ThreadPoolExecutor` for async job processing
- `collectors/`: Pull Iceberg metadata via Spark: snapshots → metadata files → manifests → data files
- `table_inventory/`: Orchestrates collection into a unified inventory structure
- `search_cutoff/`: Optimizes snapshot iteration range to avoid full scans
- `snapshot_map/`: Builds the snapshot history served for UI selection
- `table_list_catalog/`: Lists selectable tables (see **Table Catalog** below)
- `snapshot_analyzer/`: Runs between inventory and normalization; fills each snapshot's `operation_description` (a copy of `operation` by default) with the `replace` sub-type (`rewrite data files`, `rewrite delete files`, `rewrite manifests`) derived from the snapshot summary. `operation` itself is never modified; the Timeline node labels display `operation_description`
- `graph_normalizer/`: Transforms inventory data into graph nodes/links for the frontend
- `extractors/`: Extract useful information from specific file types (manifests, data files) via Spark Connect
- `base_classes/`: Abstractions for files and Spark actions
- `env.py` / `constants.py`: Environment-backed settings and fixed constants

**Frontend** (`/frontend/src/`): React SPA (Vite + Tailwind v4), migrating from legacy JSX to strict TypeScript per [frontend/PHILOSOPHY.md](frontend/PHILOSOPHY.md). Page/component layout, dev notes, and styling conventions live in [frontend/DEVELOPMENT.md](frontend/DEVELOPMENT.md).

**icegraph-client** (`/icegraph-client/`): Python client + CLI for the backend API, published to PyPI:

- `icegraph_client/clients/`: `TablesClient`, `SnapshotsClient`, `GraphClient` (one per API endpoint) composed by `IceGraphClient`
- `icegraph_client/utils/`: `http_utils.raise_for_status` (surfaces the backend's JSON error body), `json_utils.jsonify` (dataclass/Arrow-aware JSON serialization)
- `icegraph_client/cli.py`: `icegraph` console command (`tables` / `snapshots` / `graph` subcommands); JSON results go to stdout, status/progress messages to stderr so output pipes cleanly
- Version is derived from git tags via `setuptools_scm` (see Deployment Notes): not set manually in `pyproject.toml`

**API flow:**

1. `GET /api/v1/tables`: list Iceberg tables from the Spark catalog (for Home and navbar picker)
2. `GET /api/v1/snapshot-map/<table>`: load snapshot history for UI selection
3. `POST /api/v1/graph-data`: submit async job with table name + snapshot range
4. `GET /api/v1/graph-data/<job_id>`: poll until complete, returns graph JSON. The job token returned by step 3 must be sent in the `X-IceGraph-Job-Token` header; without it the endpoint answers 404

## Table Catalog (Backend)

`backend/table_list_catalog/` (`TableListCatalog`) serves `GET /api/v1/tables`:

- Walks the catalog with SQL: `SHOW CATALOGS`, `SHOW DATABASES IN <catalog>`, `SHOW TABLES IN <database>`
- Keeps only catalogs whose `spark.sql.catalog.<name>` is `org.apache.iceberg.spark.SparkCatalog`, unless `INCLUDE_NONE_ICEBERG_CATALOGS` is on (the default)
- Subtracts `SHOW VIEWS IN <database>` from the table list, and strips the default catalog's prefix from the returned names
- Caches results: see `TABLE_LIST_CACHE_TTL_SECONDS` in **Key Configuration** below

## Key Configuration

`SPARK_REMOTE` is the only required backend environment variable and has no default. It configures the Spark Connect endpoint. All other settings have defaults defined in [`backend/env.py`](backend/env.py) and can be overridden with environment variables, including through `backend/.env`.

## Deployment Notes

- `.github/workflows/release.yml` is the only workflow triggered by version tags (`v*`); it calls `docker-publish.yml`, `deploy.yml`, and `publish-icegraph-client.yml` as reusable workflows, so a tag publishes the Docker image to Docker Hub, deploys the frontend to GitHub Pages, and publishes `icegraph-client` to PyPI together
- The client's version is derived from the tag itself via `setuptools_scm` in `icegraph-client/pyproject.toml`, so it always matches the server version: `icegraph-client` is only guaranteed compatible with the IceGraph server at the same version
- GitHub Pages demo uses MSW to mock API responses (no backend); enabled via `VITE_USE_MSW=true` in the deploy workflow
- The Vite `base` path is `/IceGraph/` for GitHub Pages but `/` for Docker

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues (YanivZalach/IceGraph) via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default label vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
