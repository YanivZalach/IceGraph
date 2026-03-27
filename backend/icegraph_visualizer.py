import json
import os
import re
from pathlib import Path
from typing import Dict, Any

from constants import (
    NODE_STYLE_MAP,
    VISUALIZATION_OPTIONS,
    DELETED_DATA_FILE_CONNECTION_COLOR,
    BRANCH_CONNECTION_COLOR,
)
from utils import format_node_info

_VIS_JS_CDN = "https://unpkg.com/vis-network@9.1.2/standalone/umd/vis-network.min.js"
_VIS_CSS_CDN = "https://unpkg.com/vis-network@9.1.2/styles/vis-network.min.css"


class IceGraphVisualizer:
    def __init__(self, table_data: Dict[str, Any]):
        self.inventory = table_data["inventory"]
        self.metadata_specs = table_data["metadata_specs"]
        self.errors = table_data["errors"]

    def generate(self) -> str:
        nodes_data = []
        edges_data = []
        added_nodes = set()

        for item in self.inventory:
            path = item.get("file_path")
            f_type = item.get("type")
            style = NODE_STYLE_MAP[f_type]
            rgb_colors = ",".join([str(val) for val in style["rgb"]])
            color_shift = item.get("hidden_metadata", {}).get("color_append", 1)

            nodes_data.append(
                {
                    "id": path,
                    "label": os.path.basename(path),
                    "details": format_node_info(item),
                    "shape": "box",
                    "color": f"rgba({rgb_colors},{color_shift})",
                    "level": style["level"],
                }
            )
            added_nodes.add(path)

        for item in self.inventory:
            parent = item.get("file_path")
            children = item.get("child_files", [])
            deleted_children = set(item.get("deleted_child_files", []))
            branch_children = item.get("hidden_metadata", {}).get("branch_files", {})
            connected_branches = set()

            for child in children:
                if parent in added_nodes and child in added_nodes:
                    edge = {
                        "from": parent,
                        "to": child,
                    }
                    if child in deleted_children:
                        edge["color"] = DELETED_DATA_FILE_CONNECTION_COLOR
                        edge["title"] = "deleted"
                    elif (
                        child in branch_children
                        and branch_children[child] not in connected_branches
                    ):
                        branch_names = branch_children[child]
                        edge["dashes"] = [15, 20, 5, 20]
                        edge["color"] = BRANCH_CONNECTION_COLOR
                        edge["title"] = branch_names
                        connected_branches.add(branch_names)

                    edges_data.append(edge)

        nodes_json = json.dumps(nodes_data)
        edges_json = json.dumps(edges_data)
        options_json = json.dumps(VISUALIZATION_OPTIONS)

        html = self._build_vis_network_html(nodes_json, edges_json, options_json)
        html = self._create_html_with_inject_errors(html)
        html = self._create_html_with_inject_metadata(html)
        html = self._create_html_with_reroute_libs(html)
        html = self._create_html_with_custom_ui(html)

        return html

    def _build_vis_network_html(
        self, nodes_json: str, edges_json: str, options_json: str
    ) -> str:
        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>IceGraph</title>
    <link rel="stylesheet" href="{_VIS_CSS_CDN}">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ background: #f8fafc; overflow: hidden; width: 100vw; height: 100vh; }}
        #mynetwork {{ width: 100%; height: 100vh; }}
    </style>
</head>
<body>
<div id="mynetwork"></div>
<script src="{_VIS_JS_CDN}"></script>
<script>
    const nodes_json = {nodes_json};
    const edges_json = {edges_json};
    const nodes = new vis.DataSet(nodes_json);
    const edges = new vis.DataSet(edges_json);
    const network = new vis.Network(
        document.getElementById('mynetwork'),
        {{ nodes: nodes, edges: edges }},
        {options_json}
    );

    network.once('afterDrawing', function() {{
        network.fit();
    }});

    // The graph page is served via document.write() from /app, so the home
    // button's hardcoded '/' would go to the old Flask template. Override it
    // to return to the React app instead.
    document.addEventListener('DOMContentLoaded', function() {{
        var btn = document.getElementById('home-btn');
        if (btn) btn.onclick = function() {{ window.location.href = '/'; }};
    }});
</script>
</body>
</html>"""

    def _create_html_with_inject_metadata(self, html: str) -> str:
        specs_json = json.dumps(self.metadata_specs)
        inject_metadata = f"<script>const TABLE_METADATA = {specs_json};</script>"

        return html.replace("</body>", f"{inject_metadata}</body>")

    def _create_html_with_inject_errors(self, html: str) -> str:
        errors_json = json.dumps(self.errors)
        inject_errors = f"<script>const TABLE_ERRORS = {errors_json};</script>"

        return html.replace("</body>", f"{inject_errors}</body>")

    def _create_html_with_reroute_libs(self, html: str) -> str:
        # vis-network is loaded from CDN (standalone UMD build) so vis.DataSet
        # and vis.Network are available on the vis global — do not reroute it.

        html = re.sub(
            r'<link[^>]+href="https?://[^"]+/bootstrap\.min\.css"[^>]*>',
            '<link rel="stylesheet" href="/lib/bootstrap/bootstrap.min.css">',
            html,
        )

        html = re.sub(
            r'<script[^>]+src="https?://[^"]+/bootstrap\.bundle\.min\.js"[^>]*></script>',
            '<script src="/lib/bootstrap/bootstrap.bundle.min.js"></script>',
            html,
        )

        return html

    def _create_html_with_custom_ui(self, html) -> str:
        base_dir = Path(__file__).parent
        with open(base_dir / "js_inject.html", "r", encoding="utf-8") as f:
            custom_ui = f.read()

        return html.replace("</body>", custom_ui + "</body>")
