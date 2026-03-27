import os
from typing import Dict, Any, List

from utils import format_node_info


def normalize_graph_data(table_data: Dict[str, Any]) -> Dict[str, Any]:
    inventory = table_data.get("inventory", [])
    metadata_specs = table_data.get("metadata_specs", {})
    errors = table_data.get("errors", {})

    nodes_data = []
    edges_data = []
    added_nodes = set()

    for item in inventory:
        path = item.get("file_path")
        f_type = item.get("type")
        color_shift = item.get("hidden_metadata", {}).get("color_append", 1)

        nodes_data.append(
            {
                "id": path,
                "label": os.path.basename(path),
                "details": format_node_info(item),
                "type": f_type,
                "color_shift": color_shift,
            }
        )
        added_nodes.add(path)

    for item in inventory:
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
                    edge["is_deleted"] = True
                elif (
                    child in branch_children
                    and branch_children[child] not in connected_branches
                ):
                    edge["branch_names"] = branch_children[child]
                    connected_branches.add(edge["branch_names"])

                edges_data.append(edge)

    return {
        "nodes": nodes_data,
        "edges": edges_data,
        "metadata": metadata_specs,
        "errors": errors,
    }
