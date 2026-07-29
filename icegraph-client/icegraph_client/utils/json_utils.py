import json
from dataclasses import asdict, is_dataclass

import arrow


def _default(obj):
    if isinstance(obj, arrow.Arrow):
        return obj.isoformat()
    return str(obj)


def _serializable(obj):
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    if isinstance(obj, list):
        return [_serializable(item) for item in obj]
    return obj


def jsonify(obj, **json_dumps_kwargs) -> str:
    return json.dumps(_serializable(obj), default=_default, **json_dumps_kwargs)
