from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyvis.network import Network
from enum import Enum
import arrow
import pandas as pd
import os


# ============================================================
# Configuration
# ============================================================

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", None)


class FileType(Enum):
    METADATA = "metadata"
    SNAPSHOT = "snapshot"
    MANIFEST = "manifest"
    DATA = "DATA"
    POSITION_DELETE = "POSITION DELETE"
    EQUALITY_DELETE = "EQUALITY DELETE"
    UNKNOWN = "UNKNOWN"


# ============================================================
# Node Style Configuration (Constant Mapping)
# ============================================================

NODE_STYLE_MAP = {
    FileType.METADATA.value: ("#7D3C98", -1),
    FileType.SNAPSHOT.value: ("#2E86C1", 0),
    FileType.MANIFEST.value: ("#F39C12", 1),
    FileType.DATA.value: ("#28B463", 2),
    FileType.POSITION_DELETE.value: ("#CB4335", 2),
    FileType.EQUALITY_DELETE.value: ("#CB4335", 2),
    FileType.UNKNOWN.value: ("#CB4335", 2),
}


# ============================================================
# Spark Session
# ============================================================

def open_spark_connect_session():
    os.environ["SPARK_REMOTE"] = "sc://localhost:15002"
    return SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()


spark = open_spark_connect_session()


# ============================================================
# Utility Helpers
# ============================================================

def to_utc_timestamp(date_str: str):
    return arrow.get(date_str).replace(tzinfo="Asia/Jerusalem").to("UTC").datetime


def format_node_info(file_type: str, file_info: dict):
    return (
        file_type.upper()
        + "\n"
        + "\n".join(f"{k}: {v}" for k, v in file_info.items())
    )


# ============================================================
# Iceberg Metadata Queries
# ============================================================

def get_table_all_files_info(full_table_name: str):
    all_file_info = spark.sql(f"SELECT * FROM {full_table_name}.entries")

    return all_file_info.select(
        F.col("data_file.file_path").alias("path"),
        F.when(F.col("data_file.content") == 0, FileType.DATA.value)
        .when(F.col("data_file.content") == 1, FileType.POSITION_DELETE.value)
        .when(F.col("data_file.content") == 2, FileType.EQUALITY_DELETE.value)
        .otherwise(FileType.UNKNOWN.value)
        .alias("file_type"),
        F.col("data_file.record_count").alias("row_count"),
        F.col("readable_metrics").alias("metrics_map"),
    )


def discover_baseline_manifests(full_table_name: str, date_to_view: str):
    if not date_to_view:
        return set()

    utc_cutoff = to_utc_timestamp(date_to_view)

    baseline_snap_row = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {full_table_name}.snapshots
        WHERE committed_at < '{utc_cutoff}'
        ORDER BY committed_at DESC
        LIMIT 1
        """
    ).collect()

    if not baseline_snap_row:
        return set()

    base_id = baseline_snap_row[0]["snapshot_id"]

    old_manifests = spark.sql(
        f"""
        SELECT path
        FROM {full_table_name}.manifests
        VERSION AS OF {base_id}
        """
    ).collect()

    manifests = {m["path"] for m in old_manifests}

    print(f"Pruning {len(manifests)} manifests from baseline snapshot {base_id}")
    return manifests


def load_metadata_and_snapshots(full_table_name: str):
    metadata_df = (
        spark.sql(f"SELECT * FROM {full_table_name}.metadata_log_entries")
        .withColumnRenamed("latest_snapshot_id", "snapshot_id")
        .withColumnRenamed("timestamp", "meta_log_timestamp")
    )

    snapshots_df = spark.sql(
        f"SELECT * FROM {full_table_name}.snapshots"
    ).withColumnRenamed("committed_at", "snapshot_timestamp")

    return metadata_df.join(snapshots_df, on="snapshot_id", how="full")


# ============================================================
# Inventory Builder
# ============================================================

def get_linked_table_inventory(full_table_name: str, date_to_view: str = None):
    inventory = []
    referenced_data_files = set()
    processed_manifests = set()

    manifests_to_ignore = discover_baseline_manifests(
        full_table_name, date_to_view
    )

    df = load_metadata_and_snapshots(full_table_name)

    if date_to_view:
        cutoff = arrow.get(date_to_view).replace(tzinfo="Asia/Jerusalem")
        df = df.filter(
            F.coalesce(F.col("snapshot_timestamp"), F.col("meta_log_timestamp"))
            >= F.lit(str(cutoff)).cast("timestamp")
        )

    rows = df.sort(F.desc("meta_log_timestamp")).collect()

    for row in rows:
        process_row(
            row,
            full_table_name,
            inventory,
            referenced_data_files,
            processed_manifests,
            manifests_to_ignore,
        )

    append_data_files(
        full_table_name,
        referenced_data_files,
        inventory,
    )

    return inventory


def process_row(
    row,
    full_table_name,
    inventory,
    referenced_data_files,
    processed_manifests,
    manifests_to_ignore,
):
    row_dict = row.asDict()
    snap_id = row_dict.get("snapshot_id")
    meta_file = row_dict.get("file")

    # ------------------ Metadata Node ------------------
    if meta_file:
        inventory.append(
            {
                "type": FileType.METADATA.value,
                "file_path": meta_file,
                "file_info": {
                    "meta_log_timestamp": str(row_dict.get("meta_log_timestamp")),
                    "snapshot_id_referenced": snap_id,
                    "child_files": (
                        [row_dict.get("manifest_list")]
                        if row_dict.get("manifest_list")
                        else []
                    ),
                },
            }
        )

    # ------------------ Snapshot Node ------------------
    if snap_id and row_dict.get("manifest_list"):
        manifests = spark.sql(
            f"""
            SELECT *
            FROM {full_table_name}.manifests
            VERSION AS OF {snap_id}
            """
        ).collect()

        manifest_paths = [m["path"] for m in manifests]

        inventory.append(
            {
                "type": FileType.SNAPSHOT.value,
                "file_path": row_dict["manifest_list"],
                "file_info": {
                    "snapshot_id": snap_id,
                    "snapshot_timestamp": str(row_dict["snapshot_timestamp"]),
                    "operation": row_dict["operation"],
                    "child_files": manifest_paths,
                },
            }
        )

        process_manifests(
            manifests,
            inventory,
            referenced_data_files,
            processed_manifests,
            manifests_to_ignore,
        )


def process_manifests(
    manifests,
    inventory,
    referenced_data_files,
    processed_manifests,
    manifests_to_ignore,
):
    for m_row in manifests:
        m_path = m_row["path"]

        if m_path in manifests_to_ignore:
            continue

        if m_path in processed_manifests:
            continue

        entries = (
            spark.read.format("avro")
            .load(m_path)
            .select(F.col("data_file.file_path").alias("path"))
            .collect()
        )

        file_paths = [e["path"] for e in entries]
        referenced_data_files.update(file_paths)

        inventory.append(
            {
                "type": FileType.MANIFEST.value,
                "file_path": m_path,
                "file_info": {
                    "added_snapshot_id": m_row["added_snapshot_id"],
                    "child_files": file_paths,
                },
            }
        )

        processed_manifests.add(m_path)


def append_data_files(full_table_name, referenced_data_files, inventory):
    all_files = get_table_all_files_info(full_table_name).collect()

    for f_row in all_files:
        if f_row["path"] not in referenced_data_files:
            continue

        inventory.append(
            {
                "type": f_row["file_type"],
                "file_path": f_row["path"],
                "row_count": f_row["row_count"],
                "file_info": {
                    col_name: stats.asDict()
                    for col_name, stats in f_row["metrics_map"].asDict().items()
                },
            }
        )


# ============================================================
# Graph Generator
# ============================================================

def generate_interactive_tree(inventory_list):
    net = Network(
        height="100vh",
        width="100%",
        bgcolor="#ffffff",
        font_color="black",
        directed=True,
    )

    added_nodes = set()

    # ------------------ Add Nodes ------------------
    for item in inventory_list:
        path = item.get("file_path")
        f_type = item.get("type")

        if not path or not f_type:
            continue

        name = os.path.basename(path)
        file_info = item.get("file_info", {})
        color, level = NODE_STYLE_MAP.get(f_type, ("#CB4335", 2))

        net.add_node(
            path,
            label=name,
            title=format_node_info(f_type, file_info),
            color=color,
            level=level,
            shape="box",
            margin=10,
            font={"face": "monospace", "size": 12, "color": "white"},
            shapeProperties={"borderRadius": 6},
        )

        added_nodes.add(path)

    # ------------------ Add Edges ------------------
    for item in inventory_list:
        parent = item.get("file_path")
        children = item.get("file_info", {}).get("child_files", [])

        for child in children:
            if parent in added_nodes and child in added_nodes:
                net.add_edge(parent, child)

    # ------------------ Options ------------------
    net.set_options("""
    var options = {
      "layout": { "hierarchical": { "enabled": true, "direction": "LR", "nodeSpacing": 150, "levelSeparation": 600 } },
      "physics": { "enabled": false },
      "edges": { 
        "color": "#999",
        "smooth": { "type": "cubicBezier", "forceDirection": "horizontal" } 
      },
      "interaction": { "hover": true, "navigationButtons": true, "multiselect": true }
    }
    """)

    output_path = "iceberg_inventory_tree.html"
    net.write_html(output_path)
    append_custom_ui(output_path)

    print(f"Success! File: {os.path.abspath(output_path)}")


def append_custom_ui(output_path):
    sticky_js = """
    <style>
        body, html, .card {
            margin: 0;
            padding: 0;
            overflow: hidden !important; 
            width: 100%;
            height: 100%;
        }
        center, h1 { display: none !important;}

        #sticky-info {
            position: fixed; top: 20px; right: 20px; width: 450px; max-height: 80vh;
            overflow-y: auto; background: #ffffff; border-left: 10px solid #2E86C1;
            border-radius: 4px; padding: 25px; z-index: 1000;
            box-shadow: -5px 5px 20px rgba(0,0,0,0.15);
            font-family: sans-serif;
            display: none;
        }

        .meta-header {
            font-weight: bold;
            font-size: 1.3em;
            margin-bottom: 15px;
            border-bottom: 2px solid #eee;
        }

        .meta-row {
            margin-bottom: 8px;
            display: flex;
            flex-direction: column;
        }

        .meta-label {
            font-weight: bold;
            color: #555;
            font-size: 0.85em;
            text-transform: uppercase;
        }

        .meta-value {
            font-family: 'Courier New', monospace;
            background: #f4f4f4;
            padding: 6px;
            border-radius: 4px;
            word-break: break-all;
        }

        #reset-btn {
            position: fixed;
            top: 20px;
            left: 20px;
            padding: 12px 24px; 
            background: #222;
            color: #fff;
            border: none;
            border-radius: 4px; 
            cursor: pointer;
            z-index: 2000;
            font-family: sans-serif;
            font-weight: bold;
        }
    </style>

    <div id="sticky-info">
        <button onclick="document.getElementById('sticky-info').style.display='none'" 
            style="float:right; cursor:pointer; border:none; background:none; font-size:20px;">
            ✕
        </button>
        <div id="sticky-content"></div>
    </div>

    <button id="reset-btn" onclick="resetView()">RESET FULL VIEW</button>

    <script type="text/javascript">

    function resetView() {
        let allNodes = nodes.get();
        allNodes.forEach(node => { node.hidden = false; });
        nodes.update(allNodes);

        let allEdges = edges.get();
        allEdges.forEach(edge => { edge.hidden = false; });
        edges.update(allEdges);

        document.getElementById('sticky-info').style.display = 'none';
    }

    network.on("click", function(params) {

        if (params.nodes.length === 0) {
            return;
        }

        let selectedNode = params.nodes[0];
        let relatedNodes = new Set();
        relatedNodes.add(selectedNode);

        function findConnected(nodeId, direction) {
            let connected = (direction === 'out') 
                ? network.getConnectedNodes(nodeId, 'to')
                : network.getConnectedNodes(nodeId, 'from');

            connected.forEach(id => {
                if (!relatedNodes.has(id)) {
                    relatedNodes.add(id);
                    findConnected(id, direction);
                }
            });
        }

        findConnected(selectedNode, 'out');
        findConnected(selectedNode, 'in');

        let allNodes = nodes.get();
        allNodes.forEach(node => {
            node.hidden = !relatedNodes.has(node.id);
        });
        nodes.update(allNodes);

        let allEdges = edges.get();
        allEdges.forEach(edge => {
            edge.hidden = !(relatedNodes.has(edge.from) && relatedNodes.has(edge.to));
        });
        edges.update(allEdges);

        var nodeData = nodes.get(selectedNode);
        var panel = document.getElementById('sticky-info');
        panel.style.borderColor = nodeData.color;

        let lines = nodeData.title.split('\\n');
        let html = '<div class="meta-header">' + lines[0] + '</div>';

        for (let i = 1; i < lines.length; i++) {
            if (!lines[i].trim()) continue;
            let parts = lines[i].split(':');
            html += '<div class="meta-row">';
            html += '<span class="meta-label">' + parts[0] + '</span>';
            html += '<span class="meta-value">' + (parts.slice(1).join(':') || '') + '</span>';
            html += '</div>';
        }

        document.getElementById('sticky-content').innerHTML = html;
        panel.style.display = 'block';
    });

    </script>
    """

    with open(output_path, "r", encoding="utf-8") as f:
        html = f.read()

    html = html.replace("</body>", sticky_js + "\n</body>")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


# ============================================================
# Execution
# ============================================================

if __name__ == "__main__":
    full_table_name = "default.test_table"
    inventory = get_linked_table_inventory(full_table_name,"2026-02-21 11")
    generate_interactive_tree(inventory)
