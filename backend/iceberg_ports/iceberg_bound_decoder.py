# Ported from Apache Iceberg's type conversion implementation:
# https://github.com/apache/iceberg/blob/7f879b11366e17a676a03f15247a821751415529/api/src/main/java/org/apache/iceberg/types/Conversions.java

import base64
import re
import struct
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import UUID


def decode_iceberg_bound(field_type: str, encoded_bound: bytes):
    if field_type == "boolean":
        return encoded_bound[0] != 0

    if field_type in {"int", "date"}:
        value = struct.unpack("<i", encoded_bound)[0]
        return _decode_date(value) if field_type == "date" else value

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
        return str(Decimal(unscaled_value).scaleb(-scale))

    if field_type == "unknown":
        return None

    raise ValueError(f"Unsupported Iceberg primitive type: {field_type}")


def _decode_promoted_long(encoded_bound: bytes) -> int:
    if len(encoded_bound) < 8:
        return struct.unpack("<i", encoded_bound)[0]

    return struct.unpack("<q", encoded_bound)[0]


def _decode_long_value(field_type: str, value: int):
    if field_type == "long":
        return value

    if field_type == "time":
        return _decode_time(value)

    if field_type in {"timestamp", "timestamptz"}:
        return _decode_timestamp(value)

    return _decode_nanosecond_timestamp(value)


def _decode_date(days_since_epoch: int) -> str:
    return (date(1970, 1, 1) + timedelta(days=days_since_epoch)).isoformat()


def _decode_time(microseconds_since_midnight: int) -> str:
    hours, remainder = divmod(microseconds_since_midnight, 3_600_000_000)
    minutes, remainder = divmod(remainder, 60_000_000)
    seconds, microseconds = divmod(remainder, 1_000_000)
    return time(hours, minutes, seconds, microseconds).isoformat()


def _decode_timestamp(microseconds_since_epoch: int) -> str:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    timestamp = epoch + timedelta(microseconds=microseconds_since_epoch)
    return timestamp.isoformat()


def _decode_nanosecond_timestamp(nanoseconds_since_epoch: int) -> str:
    seconds, nanoseconds = divmod(nanoseconds_since_epoch, 1_000_000_000)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    timestamp = epoch + timedelta(seconds=seconds)
    return f"{timestamp:%Y-%m-%dT%H:%M:%S}.{nanoseconds:09d}+00:00"
