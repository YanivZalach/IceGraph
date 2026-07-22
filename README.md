<p align="center">
  <img src="images/icegraph.png" alt="IceGraph" width="200">
</p>

# <p align="center">IceGraph</p>

**IceGraph** is an interactive Apache Iceberg debugging and visualization platform that provides a hierarchical, graph-based view of Iceberg metadata. It maps the DNA of your production tables - helping engineers debug complex table states, trace metadata evolution, and understand Iceberg internals visually.

Look at Live Demo! [https://yanivzalach.github.io/IceGraph/](https://yanivzalach.github.io/IceGraph/)

> **Opinionated Design**: IceGraph is built exclusively for **Spark Connect** backends.

> **Table Version**: Currently IceGraph officially supports Table Version 2.



## 🛠 Features

* **Production-Safe & Read-Only** — Built for production Iceberg tables without modifying data or metadata.
* **Graph-Based Visualization** — Explore metadata, snapshots, manifests, data files, and delete files through an interactive graph UI. For all your table branches.
* **Snapshot & Metadata Lineage** — Trace table evolution, commits, schema changes, and snapshot history over time.
* **Partition & File Browser** — Navigate partitions and files through a familiar hierarchical view.
* **Debugging & Learning Tool** — Designed for both production debugging and understanding Iceberg internals.

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

Go to `http://localhost:5000` and explore table `default.events` and table `default.logging`.

To rebuild only the IceGraph image after local code changes (without restarting Spark or other services):
```bash
docker compose up -d --build icegraph
```

## Quick Start Using Docker

The easiest way to run IceGraph is via [DockerHub](https://hub.docker.com/r/yanivzalach/icegraph)

### Spark connect 3.5.4

```bash
docker run -e SPARK_REMOTE=sc://<spark-connect-ip>:15002 -p 5000:5000 yanivzalach/icegraph:latest
```

### Other Spark Connect versions

Clone the repo, update the Spark Connect version in `backend/pyproject.toml`, then build from the project root:
```bash
docker build -t icegraph .
```

Then run with the same command:
```bash
docker run -e SPARK_REMOTE=sc://<spark-connect-ip>:15002 -p 5000:5000 icegraph
```

### Deploying Multiple Pods

Graph jobs are tracked in each pod's memory, so if you run more than one replica, your load balancer/ingress must route all requests from a given client to the same pod (session affinity / sticky sessions) for the duration of a job, otherwise the polling requests can land on a pod that never received it.

## Start Using Source Code (Contributor to IceGraph)

Before contributing, read [ARCHITECTURE_PHILOSOPHY.md](ARCHITECTURE_PHILOSOPHY.md) — the design pillars IceGraph is built on.

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
cd frontend
npm i
```

### 2. Setup your Envs

We will create an `.env` file in the root of the backend directory:

```bash
SPARK_REMOTE=sc://localhost:15002 # Our local testing spark, If you use docker, change it to your ip.
```

If you want to change the default values of the application, you can set the following environment variables:

- `MAX_NUMBER_OF_GRAPHS_TO_COMPUTE`: The maximum number of graphs to compute in parallel. Default is 15.
- `MAX_SNAPSHOTS_TO_SHOW`: The maximum number of snapshots to show in the snapshot selection page. Default is 2000.
- `COMPUTE_CLEANUP_TIME_SECONDS`: The time to wait before cleaning up the computed graphs. Default is 12.
- `MAX_DATA_FILES_TO_COLLECT`: The maximum number of data files to collect. Default is 5000.
- `MAX_SNAPSHOTS_TO_COMPUTE`: The maximum number of snapshots to compute. Default is 50.
- `MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS`: The time to wait before forcing an exit of the application when shutting it down. Default is 10.
- `INCLUDE_NONE_ICEBERG_CATALOGS`: Whether to include catalogs that are not exclusively Iceberg (such as the Spark session catalog) in the table selection autocompletion. Default is true.
- `TABLE_LIST_CACHE_TTL_SECONDS`: The cache time-to-live for table selection autocompletion endpoint before refresh. Default is 60.

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
