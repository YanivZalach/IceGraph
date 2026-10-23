# Ported from Apache Iceberg's type conversion implementation:
# https://github.com/apache/iceberg/blob/7f879b11366e17a676a03f15247a821751415529/api/src/main/java/org/apache/iceberg/types/Conversions.java

import base64
import math
import re
import struct
from decimal import Decimal
from typing import Dict
from uuid import UUID

from iceberg_ports.date_time_decoder import decode_date, decode_nanosecond_timestamp, decode_time, decode_timestamp
from iceberg_ports.variant_decoder import decode_variant


def decode_iceberg_bound(field_type: str, encoded_bound: bytes):
    if field_type == "boolean":
        return encoded_bound[0] != 0

    if field_type in {"int", "date"}:
        value = struct.unpack("<i", encoded_bound)[0]

        return decode_date(value) if field_type == "date" else value

    if field_type in {"long", "time", "timestamp", "timestamptz", "timestamp_ns", "timestamptz_ns"}:
        value = _decode_promoted_long(encoded_bound)

        return _decode_long_value(field_type, value)

    if field_type == "float":
        return struct.unpack("<f", encoded_bound)[0]

    if field_type == "double":
        return struct.unpack("<d", encoded_bound)[0] if len(encoded_bound) >= 8 else float(struct.unpack("<f", encoded_bound)[0])

    if field_type == "string":
        return encoded_bound.decode("utf-8")

    if field_type == "uuid":
        return str(UUID(bytes=encoded_bound))

    if field_type == "binary" or field_type.startswith("fixed["):
        return base64.b64encode(encoded_bound).decode("ascii")

    decimal_match = re.fullmatch(r"decimal\(\s*\d+\s*,\s*(\d+)\s*\)", field_type)
    if decimal_match:
        scale = int(decimal_match.group(1))
        unscaled_value = int.from_bytes(encoded_bound, byteorder="big", signed=True)

        return format(Decimal(f"{unscaled_value}E-{scale}"), "f")

    if field_type == "unknown":
        return None

    if field_type == "variant":
        return decode_variant(encoded_bound)

    if re.fullmatch(r"(geometry|geography)(\(.*\))?", field_type):
        return _decode_geospatial_bound(encoded_bound)

    raise ValueError(f"Unsupported Iceberg primitive type: {field_type}")


def _decode_promoted_long(encoded_bound: bytes) -> int:
    if len(encoded_bound) < 8:
        return struct.unpack("<i", encoded_bound)[0]

    return struct.unpack("<q", encoded_bound)[0]


def _decode_long_value(field_type: str, value: int):
    if field_type == "long":
        return value

    if field_type == "time":
        return decode_time(value)

    if field_type in {"timestamp", "timestamptz"}:
        return decode_timestamp(value)

    return decode_nanosecond_timestamp(value)


def _decode_geospatial_bound(encoded_bound: bytes) -> Dict[str, float]:
    if len(encoded_bound) not in {16, 24, 32}:
        raise ValueError(f"Invalid geospatial bound size: {len(encoded_bound)}. Valid sizes are 16, 24, or 32 bytes.")

    coordinates = struct.unpack(f"<{len(encoded_bound) // 8}d", encoded_bound)
    bound = {"x": coordinates[0], "y": coordinates[1]}
    if len(coordinates) >= 3 and not math.isnan(coordinates[2]):
        bound["z"] = coordinates[2]
    if len(coordinates) == 4:
        bound["m"] = coordinates[3]

    return bound
