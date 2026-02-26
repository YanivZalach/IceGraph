from enum import Enum

SPARK_CONNECT = "sc://localhost:15002"
APPLICATION_PORT = 5000


class FileType(Enum):
    METADATA = "metadata"
    SNAPSHOT = "snapshot"
    MANIFEST = "manifest"
    DATA = "data"
    DELETE = "delete"
    UNKNOWN = "unknown"


NODE_STYLE_MAP = {
    FileType.METADATA.value: {"color": "#BF5AF2", "level": -1},
    FileType.SNAPSHOT.value: {"color": "#3ABEF9", "level": 0},
    FileType.MANIFEST.value: {"color": "#F39C12", "level": 1},
    FileType.DATA.value: {"color": "#2ECC71", "level": 2},
    FileType.DELETE.value: {"color": "#E74C3C", "level": 2},
    FileType.UNKNOWN.value: {"color": "#95A5A6", "level": 2},
}

VISUALIZATION_OPTIONS = {
    "layout": {
        "hierarchical": {
            "enabled": True,
            "direction": "LR",
            "nodeSpacing": 170,
            "levelSeparation": 800,
            "sortMethod": "hubsize",
            "shakeNeighbours": False,
            "blockShifting": True,
            "edgeMinimization": True,
            "parentCentralization": True
        },
        "improvedLayout": False
    },
    "edges": {
        "color": "#999",
        "smooth": {
            "enabled": True,
            "type": "cubicBezier",
            "forceDirection": "horizontal",
            "roundness": 0.5
        }
    },
    "physics": {
        "enabled": False,
        "stabilization": {
            "enabled": True,
            "iterations": 1000,
            "updateInterval": 100,
            "onlyDynamicEdges": False,
            "fit": True
        }
    },
    "interaction": {
        "hover": True,
        "navigationButtons": True,
        "multiselect": True,
        "tooltipDelay": 100,
        "hideEdgesOnDrag": True
    }
}
