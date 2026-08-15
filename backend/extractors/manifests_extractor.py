import pyspark
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from collectors.collect_snapshots import SnapshotRecord
from extractors.extractor import Extractor

SNAPSHOT_TO_TIMESTAMP_SCHEMA = StructType(
    [
        StructField("lookup_snap_id", LongType(), False),
        StructField("added_snapshot_timestamp", StringType(), True),
    ]
)

MANIFEST_BASE_SCHEMA = StructType(
    [
        StructField("path", StringType(), False),
        StructField("added_snapshot_id", LongType(), False),
        StructField("snapshot_id", LongType(), False),
    ]
)


class ManifestsExtractor(Extractor):
    def __init__(
        self,
        table_name: str,
        snapshots: list[SnapshotRecord],
        manifests_to_ignore_df: pyspark.sql.DataFrame,
    ):
        super().__init__(table_name)
        self._snapshots = snapshots
        self._manifests_to_ignore_df = manifests_to_ignore_df

    def extract_dataframe(self) -> pyspark.sql.DataFrame:
        manifests_df = self._union_manifests_for_snapshots()
        manifests_with_timestamps_df = self._enrich_manifests_with_timestamps(manifests_df)

        valid_manifests_df = self._filter_ignored_manifests(manifests_with_timestamps_df)

        return self._aggregate_snapshots_by_manifests_sorted(valid_manifests_df)

    def _union_manifests_for_snapshots(self) -> pyspark.sql.DataFrame:
        result = None
        for snapshot in self._snapshots:
            snap_id = snapshot.snapshot_id
            manifest_list_path = snapshot.file_path

            df = self._read_source(snapshot, lambda: self._read_manifests_for_snapshot(manifest_list_path, snap_id))
            if df is None:
                continue

            if result is None:
                result = df
            else:
                result = result.unionByName(df, allowMissingColumns=True)

        if result is None:
            result = self._spark.createDataFrame([], MANIFEST_BASE_SCHEMA)

        return result.select(*(MANIFEST_BASE_SCHEMA.fieldNames()))

    def _read_manifests_for_snapshot(self, manifest_list_path: str, snap_id: str) -> pyspark.sql.DataFrame:
        return (
            self._spark.read.format("avro")
            .load(manifest_list_path)
            .select(
                F.col("manifest_path").alias("path"),
                F.col("added_snapshot_id"),
                F.lit(snap_id).alias("snapshot_id"),
            )
        )

    def _enrich_manifests_with_timestamps(self, manifests_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return manifests_df.join(
            self._snapshot_to_timestamp_df(),
            F.col("added_snapshot_id") == F.col("lookup_snap_id"),
            "left",
        )

    def _snapshot_to_timestamp_df(self) -> pyspark.sql.DataFrame:
        return self._spark.createDataFrame(
            [(snap.snapshot_id, snap.timestamp) for snap in self._snapshots],
            SNAPSHOT_TO_TIMESTAMP_SCHEMA,
        )

    def _filter_ignored_manifests(self, manifests_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return manifests_df.join(self._manifests_to_ignore_df, on="path", how="left_anti").select(
            "path", "added_snapshot_id", "added_snapshot_timestamp", "snapshot_id"
        )

    @staticmethod
    def _aggregate_snapshots_by_manifests_sorted(manifests_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return (
            manifests_df.groupBy("path", "added_snapshot_id", "added_snapshot_timestamp")
            .agg(F.collect_list("snapshot_id").alias("snapshot_ids"))
            .orderBy(F.col("added_snapshot_timestamp"), ascending=False)
        )
