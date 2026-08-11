import sys
import arrow


def to_local_time(value, field_name: str):
    try:
        return arrow.get(value).to("local")
    except Exception as e:
        print(f"Warning: could not read '{field_name}' value {value!r} as a timestamp, leaving it as is ({e})", file=sys.stderr)
        return value
