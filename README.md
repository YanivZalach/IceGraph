# 🧊 IceGraph

**IceGraph** provides an interactive, hierarchical view of **Apache Iceberg** metadata. It maps the DNA of your tables—from root metadata down to individual data and delete files.

> **Opinionated Design**: IceGraph is built exclusively for **Spark Connect** backends.

![IceGraph Visualization](images/example.png)

## 🛠 Features

* 🕰 **Time-Travel**: View the physical state of your table as of any `datetime`.
* 🎯 **Lineage Focus**: Click a node to isolate its specific upstream and downstream path.
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

```bash
pip install -r requirements.txt
python tests/create_mock_tables.py
```

### 3. Run

```bash
python main.py
```

Go to `http://localhost:5000` and explore your mock tables.

## 📊 Node Legend

| Color | Type | Role |
| --- | --- | --- |
| 🟣 | **Metadata** | The root JSON source of truth. |
| 🔵 | **Snapshot** | Manifest List representing a table version. |
| 🟠 | **Manifest** | Groupings of physical data files. |
| 🟢 | **Data** | Parquet/Avro files containing records. |
| 🔴 | **Deletes** | MOR markers (Equality/Position deletes). |
