<p align="center">
  <img src="images/icegraph.png" alt="IceGraph" width="200">
</p>

# <p align="center">IceGraph</p>

**IceGraph** is an interactive Apache Iceberg debugging and visualization platform that provides a hierarchical, graph-based view of Iceberg metadata. It maps the DNA of your production tables - helping engineers debug complex table states, trace metadata evolution, and understand Iceberg internals visually.

Prodact page: [https://yanivzalach.github.io/IceGraph-Site/](https://yanivzalach.github.io/IceGraph-Site/)

Look at Live Demo! [https://yanivzalach.github.io/IceGraph/](https://yanivzalach.github.io/IceGraph/)

> **Opinionated Design**: IceGraph is built exclusively for **Spark Connect** backends.

> **Table Version**: Currently IceGraph officially supports Table Version 2.



## 🛠 Features

* **Production-Safe & Read-Only** — Built for production Iceberg tables without modifying data or metadata.
* **Graph-Based Visualization** — Explore metadata, snapshots, manifests, data files, and delete files through an interactive graph UI. For all your table branches.
* **Snapshot & Metadata Lineage** — Trace table evolution, commits, schema changes, and snapshot history over time.
* **Partition & File Browser** — Navigate partitions and files through a familiar hierarchical view.
* **Debugging & Learning Tool** — Designed for both production debugging and understanding Iceberg internals.
* **Python Client & CLI** — Script access to tables, snapshots, and the metadata graph via `icegraph-client`. See the [Python Client & CLI section of the docs](https://yanivzalach.github.io/IceGraph/docs) for details.


> **Recommended**: In production, use a user with read-only permissions for the Spark Connect server, for extra peace of mind.


## Mock Data Example Using Docker

Clone the repo, and in it, go to:
```
cd docker_demo
```

Run the docker compose:
```
docker compose up -d
```

Go to `http://localhost:5050` and explore table `default.events` and table `default.logging`.

To rebuild only the IceGraph image after local code changes (without restarting Spark or other services):
```bash
docker compose up -d --build icegraph
```

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

## Start Using Source Code (Contributor to IceGraph)

Before contributing, read [ARCHITECTURE_PHILOSOPHY.md](ARCHITECTURE_PHILOSOPHY.md) — the design pillars IceGraph is built on.

Work is tracked as tickets of three types: Frontend Refactor, Enrichments, and Bugs, on the [IceGraph Roadmap](https://github.com/users/YanivZalach/projects/3) board.

All contributors must accept the [Contributor License Agreement](CLA.md) before
a pull request can be merged. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
contribution and licensing process.

### Prerequisites

- npm
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
npm i
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
npm run dev
```

Go to `http://localhost:3000` and explore your tables.

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

Run all three frontend checks from the `frontend` directory:

```bash
cd frontend

npm run format
npm run lint
npm run typecheck
```

Each one catches a different class of problem, so all three are needed.

### 5. icegraph-client

To work on the CLI/client itself:

```bash
cd icegraph-client
uv sync
uv run icegraph --base-url http://localhost:3000 tables
```

## License

Current IceGraph versions are open source under the
[GNU Affero General Public License version 3 only](LICENSE). Personal, internal,
production, and commercial use are permitted subject to the AGPL terms. If you
offer a modified version to users over a network, the AGPL includes corresponding
source obligations.

Revisions released before the license change remain available under the
[MIT License](https://github.com/YanivZalach/IceGraph/blob/v0.16.2/LICENSE). Contributions require the
[Contributor License Agreement](CLA.md), which permits future relicensing and
separate commercial or enterprise licensing.

Copyright (c) 2026 Yaniv Zalach and the IceGraph contributors.
