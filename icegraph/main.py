import json
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
from flask import Flask, request, Response, redirect
import re
from pyspark.sql import functions as F
from pyvis.network import Network
from enum import Enum
import arrow
import pandas as pd
import os

# ============================================================
# Flask App
# ============================================================

app = Flask(__name__)

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
        file_type.upper() + "\n" + "\n".join(f"{k}: {v}" for k, v in file_info.items())
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

    manifests_to_ignore = discover_baseline_manifests(full_table_name, date_to_view)

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
# Sticky UI Injection
# ============================================================


def inject_custom_ui(html: str) -> str:
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
    <style>
        body, html { margin:0; padding:0; overflow:hidden; background-color: #f8fafc; }
        
        #sticky-info {
            position: fixed; top: 20px; right: 20px; width: 420px; max-height: 85vh;
            overflow-y: auto; background: rgba(255, 255, 255, 0.98); 
            border-left: 12px solid #2E86C1; border-radius: 8px; padding: 25px; 
            z-index: 1000; box-shadow: -10px 10px 30px rgba(0,0,0,0.15);
            font-family: 'Inter', -apple-system, sans-serif; display: none;
            backdrop-filter: blur(5px);
        }

        .meta-header { 
            font-weight: 800; font-size: 1.4em; color: #1a202c;
            margin-bottom: 15px; padding-bottom: 10px; border-bottom: 2px solid #edf2f7; 
            letter-spacing: -0.02em;
        }

        .meta-row { margin-bottom: 12px; }

        .meta-label { 
            font-weight: 700; color: #4a5568; font-size: 0.75em; 
            text-transform: uppercase; display: block; margin-bottom: 4px;
            letter-spacing: 0.05em;
        }

        .meta-value { 
            font-family: 'JetBrains Mono', 'Fira Code', monospace; 
            background: #2d3748; color: #ebf8ff; padding: 8px 12px; 
            border-radius: 6px; word-break: break-all; display: block;
            font-size: 0.9em; line-height: 1.4; box-shadow: inset 0 2px 4px rgba(0,0,0,0.1);
        }

        #reset-btn {
            position: fixed; top: 20px; left: 20px; padding: 12px 24px;
            background: #1a202c; color: #fff; border: none; border-radius: 6px;
            cursor: pointer; z-index: 2000; font-weight: 800; font-size: 0.9em;
            text-transform: uppercase; letter-spacing: 0.05em;
            transition: all 0.2s ease; box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }

        #reset-btn:hover { background: #2d3748; transform: translateY(-1px); box-shadow: 0 6px 8px rgba(0,0,0,0.15); }
        #reset-btn:active { transform: translateY(0); }

        /* Close Button Styling */
        .close-btn {
            float: right; cursor: pointer; border: none; background: #edf2f7;
            border-radius: 50%; width: 30px; height: 30px; font-weight: bold;
            color: #4a5568; transition: background 0.2s;
        }
        .close-btn:hover { background: #e2e8f0; color: #1a202c; }
    </style>

    <div id="sticky-info">
        <button class="close-btn" onclick="document.getElementById('sticky-info').style.display='none'">✕</button>
        <div id="sticky-content"></div>
    </div>

    <button id="reset-btn" onclick="resetView()">RESET FULL VIEW</button>

    <script>
    function resetView() {
        nodes.update(nodes.get().map(n => { n.hidden = false; return n; }));
        edges.update(edges.get().map(e => { e.hidden = false; return e; }));
        document.getElementById('sticky-info').style.display = 'none';
    }

    network.on("click", function(params) {
        if (params.nodes.length === 0) return;
        
        let selectedNodeId = params.nodes[0];
        let relatedNodes = new Set([selectedNodeId]);

        function traverse(nodeId, direction) {
            network.getConnectedNodes(nodeId, direction).forEach(id => {
                if (!relatedNodes.has(id)) {
                    relatedNodes.add(id);
                    traverse(id, direction);
                }
            });
        }
        traverse(selectedNodeId, 'to');
        traverse(selectedNodeId, 'from');

        nodes.update(nodes.get().map(n => { n.hidden = !relatedNodes.has(n.id); return n; }));
        edges.update(edges.get().map(e => { e.hidden = !(relatedNodes.has(e.from) && relatedNodes.has(e.to)); return e; }));

        // Update Side Panel
        let nodeData = nodes.get(selectedNodeId);
        let panel = document.getElementById('sticky-info');
        panel.style.borderLeftColor = nodeData.color;
        
        let lines = nodeData.title.split('\\n');
        let contentHtml = '<div class="meta-header">' + lines[0] + '</div>';
        for (let i = 1; i < lines.length; i++) {
            if (!lines[i].includes(':')) continue;
            let splitIdx = lines[i].indexOf(':');
            let label = lines[i].substring(0, splitIdx);
            let val = lines[i].substring(splitIdx + 1);
            contentHtml += `<div class="meta-row"><span class="meta-label">${label}</span><span class="meta-value">${val}</span></div>`;
        }
        document.getElementById('sticky-content').innerHTML = contentHtml;
        panel.style.display = 'block';
    });
    </script>
    """

    return html.replace("</body>", sticky_js + "</body>")


# ============================================================
# Graph Generator (IN MEMORY)
# ============================================================


def generate_graph_html(inventory_list):

    net = Network(
        height="100vh",
        width="100%",
        directed=True,
        cdn_resources="local",
    )

    added_nodes = set()

    for item in inventory_list:
        path = item.get("file_path")
        f_type = item.get("type")
        if not path or not f_type:
            continue

        color, level = NODE_STYLE_MAP.get(f_type, ("#CB4335", 2))

        net.add_node(
            path,
            label=os.path.basename(path),
            title=format_node_info(f_type, item.get("file_info", {})),
            color=color,
            level=level,
            shape="box",
        )

        added_nodes.add(path)

    for item in inventory_list:
        parent = item.get("file_path")
        children = item.get("file_info", {}).get("child_files", [])
        for child in children:
            if parent in added_nodes and child in added_nodes:
                net.add_edge(parent, child)

    options = {
        "layout": {
            "hierarchical": {
                "enabled": True,
                "direction": "LR",
                "nodeSpacing": 150,
                "levelSeparation": 600,
            }
        },
        "physics": {"enabled": False},
        "edges": {
            "color": "#999",
            "smooth": {"type": "cubicBezier", "forceDirection": "horizontal"},
        },
        "interaction": {"hover": True, "navigationButtons": True, "multiselect": True},
        "physics": {"stabilization": {"enabled": True, "iterations": 1000}},
    }

    net.set_options(json.dumps(options))

    html = net.generate_html()  # 🔥 IN MEMORY
    html = inject_custom_ui(html)

    return html


# ============================================================
# Routes
# ============================================================


@app.route("/", methods=["GET"])
def home():
    error_flag = request.args.get("error")
    error_script = ""
    if error_flag:
        error_script = "<script>alert('Error: Could not find or access the Iceberg table. Please check the name and try again.');</script>"

    return f"""
    <html>
    <head>
        <title>IceGraph</title>
        {error_script}
        <style>
            body {{ font-family: sans-serif; background-color: #f0f4f8; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }}
            .card {{ background: white; padding: 40px; border-radius: 12px; box-shadow: 0 10px 25px rgba(0,0,0,0.1); width: 380px; }}
            h2 {{ color: #2E86C1; text-align: center; margin-top: 0; }}
            p {{ font-size: 0.85em; color: #666; text-align: center; margin-bottom: 20px; line-height: 1.4; }}
            input {{ width: 100%; padding: 12px; margin: 10px 0 20px 0; border: 1px solid #ccc; border-radius: 6px; box-sizing: border-box; }}
            button {{ width: 100%; padding: 14px; background-color: #2E86C1; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: bold; }}
            #loader {{ display: none; text-align: center; margin-top: 20px; color: #2E86C1; }}
            .spinner {{ border: 4px solid #f3f3f3; border-top: 4px solid #2E86C1; border-radius: 50%; width: 25px; height: 25px; animation: spin 1s linear infinite; display: inline-block; }}
            @keyframes spin {{ 0% {{ transform: rotate(0deg); }} 100% {{ transform: rotate(360deg); }} }}
        </style>
    </head>
    <body>
        <div class="card">
            <h2>IceGraph</h2>
            <p>Enter a table to visualize its structure. <br><b>Time Filter:</b> Prunes metadata logs, snapshots, and data files added after the selected time.</p>
            <form id="gen-form" method="POST" action="/generate">
                <label>Table Name</label>
                <input type="text" name="table_name" placeholder="db.table" required>

                <label>View As Of</label>
                <input type="datetime-local" name="date">

                <button type="submit" id="submit-btn">Generate Graph</button>
            </form>

            <div id="loader">
                <div class="spinner"></div> <b>Analyzing Lineage...</b>
            </div>
        </div>

        <script>
            document.getElementById('gen-form').onsubmit = function() {{
                document.getElementById('submit-btn').style.display = 'none';
                document.getElementById('loader').style.display = 'block';
            }};
        </script>
    </body>
    </html>
    """


@app.route("/generate", methods=["POST"])
def generate():
    table_name = request.form.get("table_name")
    date_value = request.form.get("date")

    try:
        inventory = get_linked_table_inventory(table_name, date_value)
        html = generate_graph_html(inventory)
        return Response(html, mimetype="text/html")

    except AnalysisException as e:
        print(f"Spark Error: {e}")
        return redirect("/?error=table_not_found")


# ============================================================
# Run
# ============================================================

if __name__ == "__main__":
    app.run(debug=True, port=5000)
