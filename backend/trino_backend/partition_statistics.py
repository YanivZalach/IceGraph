import json
from pathlib import PurePosixPath
from typing import Dict, List

from collectors.collect_partition_statistics import CollectPartitionStatistics, PartitionStatisticsFileRecord
from env import Env
from extractors.partition_statistics_extractor import DEFAULT_PARTITION_STATISTICS_FILE_FORMAT, PARTITION_STATISTICS_FILE_FORMATS
from trino_backend.partition_layout import AVRO, PARQUET, PartitionRowType, load_table_layout, partition_row_type
from trino_backend.raw_files import MISSING_FILE_ERROR, RawFileTable, raw_files_union_sql
from trino_backend.statistics import TrinoStatisticsMixin
from trino_backend.trino_client import Row, run_query

STATISTICS_COLUMNS = {
    "spec_id": "integer",
    "data_record_count": "bigint",
    "data_file_count": "integer",
    "total_data_file_size_in_bytes": "bigint",
    "position_delete_record_count": "bigint",
    "position_delete_file_count": "integer",
    "equality_delete_record_count": "bigint",
    "equality_delete_file_count": "integer",
    "total_record_count": "bigint",
    "last_updated_at": "bigint",
    "last_updated_snapshot_id": "bigint",
    "dv_count": "integer",
}
DISTRIBUTION_METRICS = {
    "data_record_count": "data_record_count",
    "total_data_file_size_in_bytes": "total_data_file_size_in_bytes",
    "data_file_count": "data_file_count",
    "average_data_file_size_in_bytes": "IF(data_file_count > 0, CAST(total_data_file_size_in_bytes AS double) / data_file_count)",
}
TRINO_FILE_FORMATS = {"parquet": "PARQUET", "orc": "ORC", "avro": "AVRO"}
AVRO_TYPES = {"integer": "int", "bigint": "long"}
OPTIONAL_STATISTICS_COLUMNS = {"dv_count"}


def partition_statistics_avro_schema(partition_type: PartitionRowType) -> str:
    fields = [{"name": "partition", "type": {"type": "record", "name": "r1", "fields": partition_type.avro_fields()}}]
    fields.extend({"name": name, "type": ["null", AVRO_TYPES[data_type]], "default": None} for name, data_type in STATISTICS_COLUMNS.items())

    return json.dumps({"type": "record", "name": "partition_statistics", "fields": fields})


class TrinoCollectPartitionStatistics(TrinoStatisticsMixin, CollectPartitionStatistics):
    def _read_partition_summary_rows(self, statistics_files: Dict[str, PartitionStatisticsFileRecord], include_samples: bool) -> List[Row]:
        layout = load_table_layout(self._table_name)
        partition_types = {}
        branches = []
        for file_format, file_paths in self._group_by_format(statistics_files).items():
            partition_type = partition_row_type(layout, layout.partition_specs.keys(), AVRO if file_format == "avro" else PARQUET)
            partition_types[file_format] = partition_type
            branches.append(self._statistics_files_sql(file_format, file_paths, partition_type))

        rows = run_query(self._summary_sql("\nUNION ALL\n".join(branches), Env.MAX_PARTITION_STATISTICS_ROWS if include_samples else 1))

        rows_by_file: Dict[str, List[Row]] = {}
        for row in rows:
            rows_by_file.setdefault(row.file_path, []).append(row)

        for file_path, statistics_file in statistics_files.items():
            if file_path not in rows_by_file:
                statistics_file.errors.append(MISSING_FILE_ERROR.format(file_path=file_path))
            warning = partition_types[self._file_format(file_path)].warning()
            if warning:
                statistics_file.warnings.append(warning)

        return [
            self._summary_row(file_path, file_rows, partition_types[self._file_format(file_path)], include_samples)
            for file_path, file_rows in rows_by_file.items()
        ]

    def _group_by_format(self, statistics_files: Dict[str, PartitionStatisticsFileRecord]) -> Dict[str, List[str]]:
        file_paths_by_format: Dict[str, List[str]] = {}
        for file_path in statistics_files:
            file_paths_by_format.setdefault(self._file_format(file_path), []).append(file_path)

        return file_paths_by_format

    @staticmethod
    def _file_format(file_path: str) -> str:
        file_format = PurePosixPath(file_path).suffix.lstrip(".").lower()

        return file_format if file_format in PARTITION_STATISTICS_FILE_FORMATS else DEFAULT_PARTITION_STATISTICS_FILE_FORMAT

    @staticmethod
    def _statistics_files_sql(file_format: str, file_paths: List[str], partition_type: PartitionRowType) -> str:
        partition_ddl = partition_type.ddl()
        columns = [f"{name} {data_type}" for name, data_type in STATISTICS_COLUMNS.items()]
        if partition_ddl:
            columns.insert(0, f"partition {partition_ddl}")

        raw_file_table = RawFileTable(
            f"partition_statistics_{file_format}",
            TRINO_FILE_FORMATS[file_format],
            ", ".join(columns),
            partition_statistics_avro_schema(partition_type) if file_format == "avro" and partition_type.needs_avro_schema else None,
        )
        partition_json = partition_type.json_sql("partition") if partition_ddl else "CAST(NULL AS varchar)"

        return raw_files_union_sql(raw_file_table, file_paths, f"{partition_json} AS partition, {', '.join(STATISTICS_COLUMNS)}")

    @staticmethod
    def _summary_sql(statistics_sql: str, sample_rows: int) -> str:
        distribution = ", ".join(
            f"{aggregate}({expression}) OVER statistics_file AS {metric}_{aggregate}"
            for metric, expression in DISTRIBUTION_METRICS.items()
            for aggregate in ("min", "avg", "max")
        )

        return f"""
            SELECT *
            FROM (
                SELECT
                    statistics.*,
                    count(*) OVER statistics_file AS partitions_count,
                    count_if(
                        coalesce(position_delete_file_count, 0) > 0 OR coalesce(equality_delete_file_count, 0) > 0 OR coalesce(dv_count, 0) > 0
                    ) OVER statistics_file AS partitions_with_deletes,
                    {distribution},
                    row_number() OVER (PARTITION BY file_path ORDER BY last_updated_at DESC NULLS LAST) AS sample_rank
                FROM ({statistics_sql}) AS statistics
                WINDOW statistics_file AS (PARTITION BY file_path)
            )
            WHERE sample_rank <= {sample_rows}
        """

    @staticmethod
    def _summary_row(file_path: str, file_rows: List[Row], partition_type: PartitionRowType, include_samples: bool) -> Row:
        first_row = file_rows[0]
        summary = {
            "partitions_count": first_row.partitions_count,
            "partitions_with_deletes": first_row.partitions_with_deletes,
            "partition_distribution": {
                metric: {aggregate: first_row[f"{metric}_{aggregate}"] for aggregate in ("min", "avg", "max")} for metric in DISTRIBUTION_METRICS
            },
        }
        statistics_row = Row(file_path=file_path, summary=summary)
        if include_samples:
            statistics_row["sampled_partitions"] = [
                {
                    "partition": partition_type.to_python(row.partition),
                    **{column: row[column] for column in STATISTICS_COLUMNS if column not in OPTIONAL_STATISTICS_COLUMNS or row[column] is not None},
                }
                for row in file_rows
            ]

        return statistics_row
