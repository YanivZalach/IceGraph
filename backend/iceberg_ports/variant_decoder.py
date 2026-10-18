# Ported from Apache Iceberg's Variant implementation:
# https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/variants/SerializedMetadata.java
# https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/variants/SerializedPrimitive.java
# https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/variants/VariantUtil.java

import base64
import struct
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List
from uuid import UUID

from iceberg_ports.date_time_decoder import decode_date, decode_nanosecond_timestamp, decode_time, decode_timestamp

SUPPORTED_METADATA_VERSION = 1

BASIC_TYPE_PRIMITIVE = 0
BASIC_TYPE_SHORT_STRING = 1
BASIC_TYPE_OBJECT = 2
BASIC_TYPE_ARRAY = 3


@dataclass(frozen=True)
class VariantMetadata:
    keys: List[str]
    size_in_bytes: int


def decode_variant(encoded_variant: bytes) -> Any:
    metadata = _decode_metadata(encoded_variant)

    return _decode_value(encoded_variant, metadata.size_in_bytes, metadata.keys)


def _decode_metadata(encoded_variant: bytes) -> VariantMetadata:
    header = encoded_variant[0]
    version = header & 0b1111
    if version != SUPPORTED_METADATA_VERSION:
        raise ValueError(f"Unsupported Variant metadata version: {version}")

    offset_size = 1 + ((header & 0b11000000) >> 6)
    dictionary_size = _read_unsigned(encoded_variant, 1, offset_size)
    offsets_start = 1 + offset_size
    data_start = offsets_start + (dictionary_size + 1) * offset_size
    offsets = [_read_unsigned(encoded_variant, offsets_start + index * offset_size, offset_size) for index in range(dictionary_size + 1)]
    keys = [encoded_variant[data_start + offsets[index] : data_start + offsets[index + 1]].decode("utf-8") for index in range(dictionary_size)]

    return VariantMetadata(keys=keys, size_in_bytes=data_start + offsets[dictionary_size])


def _decode_value(buffer: bytes, offset: int, keys: List[str]) -> Any:
    header = buffer[offset]
    basic_type = header & 0b11
    value_header = header >> 2

    if basic_type == BASIC_TYPE_SHORT_STRING:
        return buffer[offset + 1 : offset + 1 + value_header].decode("utf-8")

    if basic_type == BASIC_TYPE_OBJECT:
        return _decode_object(buffer, offset, value_header, keys)

    if basic_type == BASIC_TYPE_ARRAY:
        return _decode_array(buffer, offset, value_header, keys)

    return _decode_primitive(buffer, offset + 1, value_header)


def _decode_object(buffer: bytes, offset: int, value_header: int, keys: List[str]) -> Dict[str, Any]:
    offset_size = 1 + (value_header & 0b11)
    field_id_size = 1 + ((value_header >> 2) & 0b11)
    is_large = (value_header >> 4) & 0b1
    count_size = 4 if is_large else 1
    count = _read_unsigned(buffer, offset + 1, count_size)
    field_ids_start = offset + 1 + count_size
    offsets_start = field_ids_start + count * field_id_size
    values_start = offsets_start + (count + 1) * offset_size

    decoded_object = {}
    for index in range(count):
        field_id = _read_unsigned(buffer, field_ids_start + index * field_id_size, field_id_size)
        value_offset = _read_unsigned(buffer, offsets_start + index * offset_size, offset_size)
        decoded_object[keys[field_id]] = _decode_value(buffer, values_start + value_offset, keys)

    return decoded_object


def _decode_array(buffer: bytes, offset: int, value_header: int, keys: List[str]) -> List[Any]:
    offset_size = 1 + (value_header & 0b11)
    is_large = (value_header >> 2) & 0b1
    count_size = 4 if is_large else 1
    count = _read_unsigned(buffer, offset + 1, count_size)
    offsets_start = offset + 1 + count_size
    values_start = offsets_start + (count + 1) * offset_size

    return [
        _decode_value(buffer, values_start + _read_unsigned(buffer, offsets_start + index * offset_size, offset_size), keys) for index in range(count)
    ]


def _decode_primitive(buffer: bytes, offset: int, type_id: int) -> Any:
    if type_id == 0:
        return None

    if type_id in {1, 2}:
        return type_id == 1

    if type_id in {3, 4, 5, 6}:
        size = {3: 1, 4: 2, 5: 4, 6: 8}[type_id]

        return int.from_bytes(buffer[offset : offset + size], byteorder="little", signed=True)

    if type_id == 7:
        return struct.unpack_from("<d", buffer, offset)[0]

    if type_id in {8, 9, 10}:
        size = {8: 4, 9: 8, 10: 16}[type_id]
        unscaled_value = int.from_bytes(buffer[offset + 1 : offset + 1 + size], byteorder="little", signed=True)

        return format(Decimal(f"{unscaled_value}E-{buffer[offset]}"), "f")

    if type_id == 11:
        return decode_date(struct.unpack_from("<i", buffer, offset)[0])

    if type_id in {12, 13, 17, 18, 19}:
        return _decode_date_time_long(struct.unpack_from("<q", buffer, offset)[0], type_id)

    if type_id == 14:
        return struct.unpack_from("<f", buffer, offset)[0]

    if type_id in {15, 16}:
        size = struct.unpack_from("<I", buffer, offset)[0]
        value = buffer[offset + 4 : offset + 4 + size]

        return base64.b64encode(value).decode("ascii") if type_id == 15 else value.decode("utf-8")

    if type_id == 20:
        return str(UUID(bytes=bytes(buffer[offset : offset + 16])))

    raise ValueError(f"Unsupported Variant primitive type: {type_id}")


def _decode_date_time_long(value: int, type_id: int) -> str:
    if type_id == 17:
        return decode_time(value)

    if type_id in {12, 13}:
        return decode_timestamp(value)

    return decode_nanosecond_timestamp(value)


def _read_unsigned(buffer: bytes, offset: int, size: int) -> int:
    return int.from_bytes(buffer[offset : offset + size], byteorder="little", signed=False)
