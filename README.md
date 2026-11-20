<p align="center">
  <img src="images/icegraph.png" alt="IceGraph" width="200">
</p>

# <p align="center">IceGraph</p>

**IceGraph** is an interactive Apache Iceberg debugging and visualization platform that provides a hierarchical, graph-based view of Iceberg metadata. It maps the DNA of your production tables - helping engineers debug complex table states, trace metadata evolution, and understand Iceberg internals visually.

Product page: [https://yanivzalach.github.io/IceGraph-Site/](https://yanivzalach.github.io/IceGraph-Site/)

Look at Live Demo! [https://yanivzalach.github.io/IceGraph/](https://yanivzalach.github.io/IceGraph/)

> **Opinionated Design**: IceGraph is built exclusively for **Spark Connect** backends.

> **Iceberg Format Versions**: IceGraph supports Iceberg format versions 1, 2, and 3.



## 🛠 Features

* **Production-Safe & Read-Only** — Built for production Iceberg tables without modifying data or metadata.
* **Graph-Based Visualization** — Explore metadata, snapshots, manifests, data files, and delete files through an interactive graph UI. For all your table branches.
* **Snapshot & Metadata Lineage** — Trace table evolution, commits, schema changes, and snapshot history over time.
* **Partition & File Browser** — Navigate partitions and files through a familiar hierarchical view.
* **Debugging & Learning Tool** — Designed for both production debugging and understanding Iceberg internals.
* **Python Client & CLI** — Script access to tables, snapshots, and the metadata graph via `icegraph-client`. See the [Python Client & CLI section of the docs](https://yanivzalach.github.io/IceGraph/docs) for details.


> **Recommended**: In production, use a user with read-only permissions for the Spark Connect server, for extra peace of mind.


## Quick Start Using Docker

The easiest way to run IceGraph is via [DockerHub](https://hub.docker.com/r/yanivzalach/icegraph)

### Spark connect 3.5.4

```bash
docker run -e SPARK_REMOTE=sc://<spark-connect-ip>:15002 -p 5050:5050 yanivzalach/icegraph:latest
```

### Other Spark Connect versions

Clone the repo, update the Spark Connect version in `backend/pyproject.toml`, then build from the project root:
```bash
docker build -t icegraph .
```

Then run with the same command:
```bash
docker run -e SPARK_REMOTE=sc://<spark-connect-ip>:15002 -p 5050:5050 icegraph
```

### Deploying Multiple Pods

Graph jobs are tracked in each pod's memory, so if you run more than one replica, your load balancer/ingress must route all requests from a given client to the same pod (session affinity / sticky sessions) for the duration of a job, otherwise the polling requests can land on a pod that never received it.

### Logs

IceGraph's own logs are JSON lines on stderr.

```json
{"time": "2026-11-02T10:15:30.123456+00:00", "level": "INFO", "logger": "IceGraph", "message": "[db.table] CollectSnapshots.collect took 0.55s", "user_id": "3f2a..."}
```

Count distinct `user_id` values to estimate users.

### Metrics

Prometheus metrics are served at `/metrics`. Disable with `METRICS_ENABLED=false`.

- `icegraph_operation_duration_seconds`: operation durations.
- `icegraph_graph_computations_total`: graphs computed.
- `icegraph_cache_reads_total`: server cache hits.
- `icegraph_assistant_questions_total`: questions submitted to the AI assistant.

### AI Assistant

IceGraph can show an **Ask AI** button on the snapshot selection page and the table pages that answers questions about the table. It is off by default and turns on when both `AI_API_BASE_URL` and `AI_MODEL` are set. Any OpenAI-compatible chat completions API works (OpenAI, Azure OpenAI, vLLM, Ollama, LiteLLM, and others). The API URL, key, and model stay on the server and are never sent to the browser.

The `AI_*` settings, their defaults, and their descriptions are in the [`Env` class](backend/env.py). How the assistant works is described in [`docs/ai-assistant.md`](docs/ai-assistant.md).

```bash
docker run -e SPARK_REMOTE=sc://<spark-connect-ip>:15002 \
  -e AI_API_BASE_URL=https://api.openai.com/v1 -e AI_MODEL=<model> -e AI_API_KEY=<key> \
  -p 5050:5050 yanivzalach/icegraph:latest
```

The user chooses which parts of the table metadata loaded in the browser are sent with each question. That context can include file paths, table properties, and column lower and upper bounds, which are real data values, so point IceGraph only at a model you are allowed to send them to. IceGraph has no authentication: anyone who can reach it can use the configured model, within the `AI_MAX_CONCURRENT_REQUESTS` and `AI_MAX_CONTEXT_CHARACTERS` limits.

## Start Using Source Code (Contributor to IceGraph)

Before contributing, read [ARCHITECTURE_PHILOSOPHY.md](ARCHITECTURE_PHILOSOPHY.md) — the design pillars IceGraph is built on.

Work is tracked in GitHub Issues and organized on the [IceGraph Roadmap](https://github.com/users/YanivZalach/projects/3) board.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the
contribution and licensing process.

### Prerequisites

- pnpm 11.24.0, installed directly, or Corepack for managing the pinned pnpm version
- UV (python)
- Spark Connect server (Quick setup using https://github.com/YanivZalach/Docker_Spark_Connect_Iceberg)

### 1. Setup

Sync the environments:

```bash
cd backend
uv sync
```

```bash
cd icegraph-client
uv sync
```

```bash
cd frontend
pnpm install
```

### 2. Setup your Envs

We will create an `.env` file in the root of the backend directory:

```bash
SPARK_REMOTE=sc://localhost:15002 # Our local testing spark, If you use docker, change it to your ip.
```

The supported environment variables, their defaults, and their descriptions are defined in the [`Env` class](backend/env.py).

### 3. Run

Open one terminal in the backend directory and run:

```bash
uv run python main.py
```

Open a second terminal in the front end directory and run:
```bash
pnpm run dev
```

Go to `http://localhost:3000` and explore your tables.

The backend listens on port `5050`, and the frontend dev server proxies `/api` and `/metrics` to it. To use another backend port, set `APPLICATION_PORT` for the backend and `VITE_DEV_BACKEND_PORT` for the frontend to the same value:

```bash
APPLICATION_PORT=5051 uv run python main.py
```

```bash
VITE_DEV_BACKEND_PORT=5051 pnpm run dev
```

### 4. Before Every Commit

CI checks Python formatting and the frontend toolchain. Format each Python project from its own directory.

Backend:

```bash
cd backend

uv run ruff format .
```

Python client:

```bash
cd icegraph-client

uv run ruff format .
```

Run the frontend checks and production build from the `frontend` directory:

```bash
cd frontend

pnpm run format
pnpm run lint
pnpm run typecheck
VITE_OUT_DIR=dist pnpm run build
```

Each command catches a different class of problem, so all four are needed. Tests may be used
temporarily during development, but test files must be removed before submission and must not be
included in the contribution.

### 5. icegraph-client

To work on the CLI/client itself:

```bash
cd icegraph-client
uv sync
uv run icegraph --base-url http://localhost:3000 tables
```

List every data and delete file of one partition at one snapshot (the latest when `-s` is omitted). In Python, the same call is `IceGraphClient.get_partition_files(table, filters, snapshot_id)`:

```bash
uv run icegraph --base-url http://localhost:3000 partition <database.table> -f <partition field>=<value> [-s <snapshot id>]
```

## License

IceGraph is open source under the [Apache License, Version 2.0](LICENSE). See
[NOTICE](NOTICE) for attribution.
