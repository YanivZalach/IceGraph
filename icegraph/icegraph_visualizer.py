import json
import os
import re
from pathlib import Path
from typing import Dict, Any

from pyvis.network import Network

from constants import NODE_STYLE_MAP, VISUALIZATION_OPTIONS
from utils import format_node_info


class IceGraphVisualizer:
    def __init__(self, table_data: Dict[str, Any]):
        self.inventory = table_data["inventory"]
        self.metadata_specs = table_data["metadata_specs"]
        self.errors = table_data["errors"]

    def generate(self) -> str:
        net = Network(
            height="100vh",
            width="100%",
            directed=True,
            cdn_resources="local",
        )

        added_nodes = set()

        for item in self.inventory:
            path = item.get("file_path")
            f_type = item.get("type")
            style = NODE_STYLE_MAP[f_type]
            rgb_colors = ",".join([str(val) for val in style["rgb"]])
            color_shift = item.get("hidden_metadata", {}).get("color_append", 1)

            net.add_node(
                path,
                label=os.path.basename(path),
                title=format_node_info(item),
                shape="box",
                color=f"rgba({rgb_colors} ,{color_shift})",
                level=style["level"],
            )
            added_nodes.add(path)

        for item in self.inventory:
            parent = item.get("file_path")
            children = item.get("child_files", [])
            for child in children:
                if parent in added_nodes and child in added_nodes:
                    net.add_edge(parent, child)

        net.set_options(json.dumps(VISUALIZATION_OPTIONS))

        html = net.generate_html()
        html = self._create_html_with_inject_errors(html)
        html = self._create_html_with_inject_metadata(html)
        html = self._create_html_with_reroute_libs(html)
        html = self._create_html_with_custom_ui(html)

        return html

    def _create_html_with_inject_metadata(self, html: str) -> str:
        specs_json = json.dumps(self.metadata_specs)
        inject_metadata = f"<script>const TABLE_METADATA = {specs_json};</script>"

        return html.replace("</body>", f"{inject_metadata}</body>")

    def _create_html_with_inject_errors(self, html: str) -> str:
        errors_json = json.dumps(self.errors)
        inject_errors = f"<script>const TABLE_ERRORS = {errors_json};</script>"

        return html.replace("</body>", f"{inject_errors}</body>")

    def _create_html_with_reroute_libs(self, html: str) -> str:
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

        html = re.sub(
            r'<link[^>]+href="https?://[^"]+/vis-network(?:\.min)?\.css"[^>]*>',
            '<link rel="stylesheet" href="/lib/vis-9.1.2/vis-network.css">',
            html,
        )
        html = re.sub(
            r'<script[^>]+src="https?://[^"]+/vis-network(?:\.min)?\.js"[^>]*></script>',
            '<script src="/lib/vis-9.1.2/vis-network.min.js"></script>',
            html,
        )

        return html

    def _create_html_with_custom_ui(self, html) -> str:
        base_dir = Path(__file__).parent
        with open(base_dir / "js_inject.html", "r", encoding="utf-8") as f:
            custom_ui = f.read()

        return html.replace("</body>", custom_ui + "</body>")
