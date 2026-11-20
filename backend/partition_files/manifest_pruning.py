import re
import struct
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from pyspark.sql import Row
from pyspark.sql.types import ArrayType, BinaryType, BooleanType, IntegerType, LongType, StringType, StructField, StructType

from base_classes.utils import timed
from icegraph_logger import logger
from partition_files.constants import UNKNOWN_PARTITION_SPEC_ERROR
from partition_files.partition_filter import PartitionFilter, SpecFilter, SpecPredicate
from spark_connect import open_spark_connect_session

EPOCH_DATE = date(1970, 1, 1)

FIELD_SUMMARY_SCHEMA = StructType(
    [
        StructField("contains_null", BooleanType(), True),
        StructField("contains_nan", BooleanType(), True),
        StructField("lower_bound", BinaryType(), True),
        StructField("upper_bound", BinaryType(), True),
    ]
)

MANIFEST_LIST_SCHEMA = StructType(
    [
        StructField("manifest_path", StringType(), True),
        StructField("partition_spec_id", IntegerType(), True),
        StructField("content", IntegerType(), True),
        StructField("sequence_number", LongType(), True),
        StructField("added_snapshot_id", LongType(), True),
        StructField("added_files_count", IntegerType(), True),
        StructField("existing_files_count", IntegerType(), True),
        StructField("added_data_files_count", IntegerType(), True),
        StructField("existing_data_files_count", IntegerType(), True),
        StructField("partitions", ArrayType(FIELD_SUMMARY_SCHEMA), True),
    ]
)


@dataclass(frozen=True)
class ManifestCandidate:
    manifest_path: str
    spec_filter: SpecFilter
    content: int
    sequence_number: Optional[int]
    added_snapshot_id: Optional[int]


@dataclass(frozen=True)
class ManifestPruningResult:
    manifests: list[ManifestCandidate]
    errors: list[str]


@timed
def prune_manifests(table_name: str, manifest_list_path: str, partition_filter: PartitionFilter) -> ManifestPruningResult:
    try:
        rows = open_spark_connect_session().read.format("avro").schema(MANIFEST_LIST_SCHEMA).load(manifest_list_path).collect()

    except Exception as e:
        logger.error(f"[{table_name}] Failed to read manifest list {manifest_list_path}", exc_info=True)

        return ManifestPruningResult(manifests=[], errors=[str(e)])

    manifests = []
    for row in rows:
        spec_filter = partition_filter.spec_filters.get(row.partition_spec_id)
        if spec_filter is None:
            raise ValueError(UNKNOWN_PARTITION_SPEC_ERROR.format(manifest_path=row.manifest_path, spec_id=row.partition_spec_id))

        if _has_no_live_files(row) or not _can_contain_match(row, spec_filter):
            continue

        manifests.append(
            ManifestCandidate(
                manifest_path=row.manifest_path,
                spec_filter=spec_filter,
                content=row.content or 0,
                sequence_number=row.sequence_number,
                added_snapshot_id=row.added_snapshot_id,
            )
        )

    return ManifestPruningResult(manifests=manifests, errors=[])


def _has_no_live_files(row: Row) -> bool:
    added_files_count = row.added_files_count if row.added_files_count is not None else row.added_data_files_count
    existing_files_count = row.existing_files_count if row.existing_files_count is not None else row.existing_data_files_count

    return added_files_count == 0 and existing_files_count == 0


def _can_contain_match(row: Row, spec_filter: SpecFilter) -> bool:
    summaries = row.partitions
    if summaries is None:
        return True

    for predicate in spec_filter.predicates:
        if predicate.spec_field_position >= len(summaries):
            continue

        if not _summary_can_contain(summaries[predicate.spec_field_position], predicate):
            return False

    return True


def _summary_can_contain(summary: Row, predicate: SpecPredicate) -> bool:
    if summary is None:
        return True

    if summary.lower_bound is None:
        return False

    result_type = predicate.filter_field.result_type
    lower_bound = _decode_comparable_bound(result_type, summary.lower_bound)
    upper_bound = _decode_comparable_bound(result_type, summary.upper_bound) if summary.upper_bound is not None else None
    value = _comparable_value(result_type, predicate.filter_field.value)
    if lower_bound is None or value is None:
        return True

    if value < lower_bound:
        return False

    return upper_bound is None or value <= upper_bound


def _comparable_value(result_type: str, value: Any) -> Any:
    if result_type == "date":
        return (value - EPOCH_DATE).days

    return value


def _decode_comparable_bound(result_type: str, encoded_bound: bytes) -> Any:
    if result_type in {"int", "date"} and len(encoded_bound) == 4:
        return struct.unpack("<i", encoded_bound)[0]

    if result_type == "long":
        return struct.unpack("<q", encoded_bound)[0] if len(encoded_bound) == 8 else struct.unpack("<i", encoded_bound)[0]

    if result_type == "string":
        return encoded_bound.decode("utf-8")

    if result_type == "boolean" and len(encoded_bound) == 1:
        return encoded_bound[0] != 0

    decimal_match = re.fullmatch(r"decimal\(\s*\d+\s*,\s*(\d+)\s*\)", result_type)
    if decimal_match:
        unscaled_value = int.from_bytes(encoded_bound, byteorder="big", signed=True)

        return Decimal(f"{unscaled_value}E-{decimal_match.group(1)}")

    return None
