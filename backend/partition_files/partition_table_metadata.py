import json
from dataclasses import dataclass
from typing import Any, Optional

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

from base_classes.spark_table_action import SparkTableAction
from base_classes.utils import collect_graph_metadata_file, timed
from collectors.utils import normalize_legacy_v1_metadata_dict
from partition_files.constants import INLINE_MANIFESTS_ERROR, LEGACY_PARTITION_FIELD_ID_START, MISSING_SNAPSHOT_ERROR, VOID_TRANSFORM
from partition_files.utils import format_epoch_millis

METADATA_SPEC_KEYS = ["format-version", "partition-specs", "partition-spec", "default-spec-id", "schemas", "schema", "current-schema-id"]
SNAPSHOT_TIMESTAMPS_SCHEMA = ArrayType(StructType([StructField("snapshot-id", LongType()), StructField("timestamp-ms", LongType())]))


@dataclass(frozen=True)
class PartitionSpecField:
    field_id: int
    name: str
    transform: str
    source_id: Optional[int]


@dataclass(frozen=True)
class PartitionSpec:
    spec_id: int
    fields: list[PartitionSpecField]

    @property
    def is_unpartitioned(self) -> bool:
        for spec_field in self.fields:
            if spec_field.transform != VOID_TRANSFORM:
                return False

        return True


@dataclass
class PartitionTableMetadata:
    format_version: int
    specs_by_id: dict[int, PartitionSpec]
    schemas: list[dict[str, Any]]
    current_schema_id: int
    snapshot: dict[str, Any]
    snapshot_timestamps: dict[int, str]


class PartitionTableMetadataReader(SparkTableAction):
    def __init__(self, table_name: str, snapshot_id: int):
        super().__init__(table_name)
        self._snapshot_id = snapshot_id

    @timed
    def read(self) -> PartitionTableMetadata:
        metadata_path = collect_graph_metadata_file(self._table_name, None)
        metadata_row = self._metadata_row_df(metadata_path).first()

        if metadata_row.snapshot is None:
            raise ValueError(MISSING_SNAPSHOT_ERROR.format(snapshot_id=self._snapshot_id, table_name=self._table_name))

        snapshot = json.loads(metadata_row.snapshot)
        if not snapshot.get("manifest-list"):
            raise ValueError(INLINE_MANIFESTS_ERROR.format(snapshot_id=self._snapshot_id))

        metadata = {key: json.loads(value) for key, value in (metadata_row.metadata or {}).items() if value is not None}
        normalize_legacy_v1_metadata_dict(metadata)

        return PartitionTableMetadata(
            format_version=metadata.get("format-version", 1),
            specs_by_id=self._parse_specs(metadata.get("partition-specs") or []),
            schemas=metadata.get("schemas") or [],
            current_schema_id=metadata.get("current-schema-id", 0),
            snapshot=snapshot,
            snapshot_timestamps={
                entry["snapshot-id"]: format_epoch_millis(entry["timestamp-ms"]) for entry in metadata_row.snapshot_timestamps or []
            },
        )

    def _metadata_row_df(self, metadata_path: str):
        metadata = F.col("metadata")
        snapshots = F.from_json(metadata.getItem("snapshots"), ArrayType(StringType()))

        return (
            self._spark.read.text(metadata_path, wholetext=True)
            .select(F.from_json("value", MapType(StringType(), StringType())).alias("metadata"))
            .select(
                F.map_filter(metadata, lambda key, _value: key.isin(METADATA_SPEC_KEYS)).alias("metadata"),
                F.get(F.filter(snapshots, lambda snapshot: self._snapshot_id_of(snapshot) == F.lit(str(self._snapshot_id))), 0).alias("snapshot"),
                F.from_json(metadata.getItem("snapshots"), SNAPSHOT_TIMESTAMPS_SCHEMA).alias("snapshot_timestamps"),
            )
        )

    @staticmethod
    def _snapshot_id_of(snapshot: Column) -> Column:
        return F.get_json_object(snapshot, "$['snapshot-id']")

    @staticmethod
    def _parse_specs(raw_specs: list[dict[str, Any]]) -> dict[int, PartitionSpec]:
        specs_by_id = {}
        for raw_spec in raw_specs:
            fields = [
                PartitionSpecField(
                    field_id=raw_field.get("field-id", LEGACY_PARTITION_FIELD_ID_START + index),
                    name=raw_field["name"],
                    transform=raw_field["transform"],
                    source_id=raw_field.get("source-id"),
                )
                for index, raw_field in enumerate(raw_spec.get("fields") or [])
            ]
            specs_by_id[raw_spec["spec-id"]] = PartitionSpec(spec_id=raw_spec["spec-id"], fields=fields)

        return specs_by_id
