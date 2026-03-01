# 🧊 IceGraph

**IceGraph** provides an interactive, hierarchical view of **Apache Iceberg** metadata. It maps the DNA of your tables—from root metadata down to individual data and delete files.

> **Opinionated Design**: IceGraph is built exclusively for **Spark Connect** backends.

![IceGraph Visualization](images/example.png)

## 🛠 Features

* 🕰 **Time-Travel**: View the physical state of your table as of any `datetime`.
* 🎯 **Lineage Focus**: Click a node to isolate its specific upstream and downstream path.
* 🔒 **Inspect Mode**: Toggle the **Lock View** to explore file metadata in the side panel without shifting the graph's visibility.
* 📋 **Metadata Inspector**: A sticky side panel displaying record counts, stats, and file paths.
* 🌳 **Directed Layout**: Left-to-Right (LR) flow representing the Metadata ➔ Data hierarchy.
* 🔴 **MOR Awareness**: Visual tracking of Equality and Position delete files.

## 🚦 Quick Start

### 1. Backend

Start your Spark Connect server (example via Docker):

```bash
cd tests/spark_connect_docker && docker-compose up -d
```

### 2. Setup & Mock Data

Install:
```bash
pip install -r requirements.txt
```

Create mock if needed:
```bash
python tests/create_mock_tables.py
```

### 3. Setup your Envs

We will create inside the inner icegraph directory a file, named `.env`:

```bash
TIMEZONE=My timezone
SPARK_REMOTE=sc://localhost:15002 # Our local testing spark
APPLICATION_PORT=5000
```

### 4. Run
Be in the icegraph inner directory:

```bash
python main.py
```

Go to `http://localhost:5000` and explore your mock tables.

## 📊 Node Legend

| Color | Type | Role |
| --- | --- | --- |
| 🟣 | **Metadata** | The root JSON source. The **Pink** node is the current state; others fade with age. |
| 🔵 | **Snapshot** | The Manifest List representing a specific table version. |
| 🟠 | **Manifest** | Groupings of physical data files (Avro). |
| 🟢 | **Data** | Parquet/Avro files containing actual records. |
| 🔴 | **Deletes** | MOR markers (Equality or Position delete files). |

## 🎮 UI Controls

* **Reset Full View**: Clears all filters and returns the graph to its full hierarchical state.
* **Mode: Lineage Traversal**: Default mode. Clicking a node hides everything except its direct parents and children.
* **Mode: Inspect (Locked)**: Keeps the current graph layout static. Clicking nodes updates the **Metadata Inspector** without changing visibility.
* **Table Info**: Show you the Schema and Partition spec of the table.
