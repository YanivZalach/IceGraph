import json
import os
import re

from pyvis.network import Network

from constants import NODE_STYLE_MAP, VISUALIZATION_OPTIONS, FileType
from utils import format_node_info


class IceGraphVisualizer:
    def __init__(self, inventory):
        self.inventory = inventory
        self._custom_ui_content = self._load_injection_script()

    def _load_injection_script(self) -> str:
        with open("js_inject.html", "r") as f:
            return f.read()
        return ""

    def generate(self) -> str:
        net = Network(
            height="100vh",
            width="100%",
            directed=True,
            cdn_resources="local",
        )

        added_nodes = set()

        # 1. Add Nodes
        for item in self.inventory:
            path = item.get("file_path")
            f_type = item.get("type")
            if not path or not f_type:
                continue

            style = NODE_STYLE_MAP.get(f_type, NODE_STYLE_MAP[FileType.UNKNOWN.value])

            net.add_node(
                path,
                label=os.path.basename(path),
                title=format_node_info(f_type, item.get("file_info", {})),
                color=style["color"],
                level=style["level"],
                shape="box",
            )
            added_nodes.add(path)

        # 2. Add Edges
        for item in self.inventory:
            parent = item.get("file_path")
            children = item.get("file_info", {}).get("child_files", [])
            for child in children:
                if parent in added_nodes and child in added_nodes:
                    net.add_edge(parent, child)

        # 3. Apply Options & Generate
        net.set_options(json.dumps(VISUALIZATION_OPTIONS))
        html = net.generate_html()

        if self._custom_ui_content:
            html = html.replace("</body>", self._custom_ui_content + "</body>")

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
