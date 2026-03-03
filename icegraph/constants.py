from enum import Enum


class FileType(Enum):
    MAIN_METADATA = "main_metadata"
    METADATA = "metadata"
    SNAPSHOT = "snapshot"
    MANIFEST = "manifest"
    DATA = "data"
    POSITION_DELETE = "position_delete"
    EQUALITY_DELETE = "equality_delete"


NODE_STYLE_MAP = {
    FileType.MAIN_METADATA.value: {"rgb": (250, 112, 181), "level": -1},
    FileType.METADATA.value: {"rgb": (191, 90, 242), "level": -1},
    FileType.SNAPSHOT.value: {"rgb": (58, 190, 249), "level": 0},
    FileType.MANIFEST.value: {"rgb": (243, 156, 18), "level": 1},
    FileType.DATA.value: {"rgb": (46, 204, 113), "level": 2},
    FileType.POSITION_DELETE.value: {"rgb": (231, 76, 60), "level": 2},
    FileType.EQUALITY_DELETE.value: {"rgb": (231, 76, 60), "level": 2},
}

VISUALIZATION_OPTIONS = {
    "layout": {
        "hierarchical": {
            "enabled": True,
            "direction": "LR",
            "nodeSpacing": 150,
            "levelSeparation": 800,
            "sortMethod": "directed",
            "blockShifting": True,
            "edgeMinimization": True,
            "parentCentralization": True,
        },
        "improvedLayout": True,
    },
    "edges": {
        "color": "#999",
        "smooth": {
            "enabled": True,
            "type": "cubicBezier",
            "forceDirection": "horizontal",
            "roundness": 0.5,
        },
    },
    "physics": {
        "enabled": False,
    },
    "interaction": {
        "hover": True,
        "navigationButtons": True,
        "multiselect": True,
        "tooltipDelay": 100,
        "hideEdgesOnDrag": True,
    },
}
