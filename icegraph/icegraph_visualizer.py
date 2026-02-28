import json
import os
import re

from pyvis.network import Network

from constants import NODE_STYLE_MAP, VISUALIZATION_OPTIONS
from utils import format_node_info


class IceGraphVisualizer:
    def __init__(self, inventory):
        self.inventory = inventory

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

            style = NODE_STYLE_MAP[f_type]

            net.add_node(
                path,
                label=os.path.basename(path),
                title=format_node_info(item),
                shape="box",
                **style
            )
            added_nodes.add(path)

        # 2. Add Edges
        for item in self.inventory:
            parent = item.get("file_path")
            children = item.get("child_files", [])
            for child in children:
                if parent in added_nodes and child in added_nodes:
                    net.add_edge(parent, child)

        net.set_options(json.dumps(VISUALIZATION_OPTIONS))

        html = net.generate_html()
        html = self._load_custom_ui(html)
        html = self._reroute_libs(html)

        return html

    def _load_custom_ui(self, html) -> str:
        with open("js_inject.html", "r") as f:
            custom_ui = f.read()

        return html.replace("</body>", custom_ui + "</body>")

    def _reroute_libs(self, html):
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
