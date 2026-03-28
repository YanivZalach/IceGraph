<p align="center">
  <img src="images/icegraph.png" alt="IceGraph" width="200">
</p>

# <p align="center">IceGraph</p>

**IceGraph** provides an interactive, hierarchical view of **Apache Iceberg** metadata. It maps the DNA of your tables—from root metadata down to individual data and delete files.

> **Opinionated Design**: IceGraph is built exclusively for **Spark Connect** backends.

> **Table Version**: Currently IceGraph officially supports Table Version 2.



## 🛠 Features

* 🔒 **Read-Only**: The application is read-only and does not modify the table.
* 🕰 **Time-Travel**: View the physical state of your table as of any `datetime`.
* 📋 **Metadata Inspector**: Displaying record counts, stats, and file paths.
* 🌴 **Branches**: View all the branches of the table, even when detached from the main branch.

> **Recommended**: In production, use a user with read-only permissions for the Spark Connect server, for extra peace of mind.

## Quick Start Using Docker

```bash
docker run -e SPARK_REMOTE=sc://<spark-connect-ip>:15002 -e TIMEZONE=my/timezone -p 5000:5000 yanivzalach/icegraph:latest
```

## Start Using Source Code

### Prerequisites

- npm
- UV (python)
- Python 3.9

### 1. Spark Connect Backend

Start your Spark Connect server (example via Docker):

```bash
cd tests/spark_connect_docker && docker-compose up -d
```

### 2. Setup

Sync the environments:

```bash
cd backend
uv sync
```

```bash
cd frontend
npm i
```

### 3. Create Mock Data

Create mock if needed for testing, from within the backend directory:
```bash
uv run python tests/create_mock_table.py
uv run python tests/create_mock_table_no_branches.py
```

### 3. Setup your Envs

We will create an `.env` file in the root of the backend directory:

```bash
TIMEZONE=my/timezone # Put your local timezone name
SPARK_REMOTE=sc://localhost:15002 # Our local testing spark, If you use docker, change it to your ip.
```

### 4. Run

Open one terminal in the backend directory and run:

```bash
uv run python main.py
```

Open a second terminal in the front end directory and run:
```bash
npm run dev
```

Go to `http://localhost:3000` and explore your mock tables.