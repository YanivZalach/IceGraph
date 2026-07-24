from typing import Any

JS_MAX_SAFE_INTEGER = 2**53 - 1


def to_json_safe(value: Any) -> Any:
    if isinstance(value, int):
        return str(value) if abs(value) > JS_MAX_SAFE_INTEGER else value

    if isinstance(value, dict):
        return {key: to_json_safe(v) for key, v in value.items()}

    if isinstance(value, (set, frozenset)):
        return sorted(to_json_safe(v) for v in value)

    if isinstance(value, (list, tuple)):
        return [to_json_safe(v) for v in value]

    return value
