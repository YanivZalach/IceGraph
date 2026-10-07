from dataclasses import dataclass
from typing import Any

from pyspark.errors import AnalysisException
from pyspark.sql.types import ArrayType, DataType, MapType, StructField, StructType

from base_classes.spark_table_action import SparkTableAction
from base_classes.utils import timed
from icegraph_logger import logger

SECTION_TITLE_PREFIX = "#"
SKIPPED_DESCRIBE_ROW_NAMES = {"", "# col_name"}
TABLE_PROPERTIES_ROW_NAME = "Table Properties"
COLUMNS_SECTION_TITLE = "Columns"
PARTITION_SECTION_TITLES = {"Partition Information", "Partitioning"}


@dataclass(frozen=True)
class DescribeFormattedResult:
    column_rows: list[dict[str, str]]
    sections: list[dict[str, Any]]


class TableDescriptionCollector(SparkTableAction):
    def __init__(self, full_table_name: str):
        super().__init__(full_table_name)
        self._warnings: dict[str, list[str]] = {}

    @timed
    def collect(self) -> dict[str, Any]:
        spark_schema = self._collect_spark_schema()
        properties = self._collect_properties()
        describe_result = self._collect_describe(skip_table_properties=properties is not None)
        sections = describe_result.sections

        if spark_schema is None:
            sections.insert(0, {"title": COLUMNS_SECTION_TITLE, "rows": describe_result.column_rows})

        spark_partitions = []
        for section in sections:
            if section["title"] in PARTITION_SECTION_TITLES:
                spark_partitions = section["rows"]
                sections.remove(section)
                break

        return {
            "spark_schema": spark_schema,
            "spark_partitions": spark_partitions,
            "sections": sections,
            "properties": properties,
            "warnings": self._warnings,
        }

    def _collect_spark_schema(self) -> dict[str, Any] | None:
        try:
            return self._convert_type(self._spark.table(self._table_name).schema)

        except AnalysisException as error:
            logger.warning(f"[{self._table_name}] Could not read the Spark schema: {error}")
            self._warnings["spark_schema"] = [f"Could not read the Spark schema, so the columns are shown as Spark describes them: {error}"]

            return None

    def _collect_properties(self) -> dict[str, str] | None:
        try:
            return {row.key: row.value for row in self._spark.sql(f"SHOW TBLPROPERTIES {self._table_name}").collect()}

        except AnalysisException as error:
            logger.warning(f"[{self._table_name}] Could not read the table properties: {error}")
            self._warnings["properties"] = [f"Could not read the table properties, so they are shown as one raw row: {error}"]

            return None

    def _collect_describe(self, skip_table_properties: bool) -> DescribeFormattedResult:
        column_rows = []
        sections = []
        current_rows = column_rows

        for row in self._spark.sql(f"DESCRIBE FORMATTED {self._table_name}").collect():
            name = row.col_name or ""
            if name in SKIPPED_DESCRIBE_ROW_NAMES or (skip_table_properties and name == TABLE_PROPERTIES_ROW_NAME):
                continue

            if name.startswith(SECTION_TITLE_PREFIX):
                current_rows = []
                sections.append({"title": name.removeprefix(SECTION_TITLE_PREFIX).strip(), "rows": current_rows})
            else:
                current_rows.append({"name": name, "value": row.data_type or "", "comment": row.comment or ""})

        return DescribeFormattedResult(column_rows=column_rows, sections=sections)

    def _convert_type(self, data_type: DataType) -> dict[str, Any] | str:
        if isinstance(data_type, StructType):
            return {"type": "struct", "fields": [self._convert_field(field) for field in data_type.fields]}

        if isinstance(data_type, ArrayType):
            return {"type": "list", "element-required": not data_type.containsNull, "element": self._convert_type(data_type.elementType)}

        if isinstance(data_type, MapType):
            return {
                "type": "map",
                "key": self._convert_type(data_type.keyType),
                "value-required": not data_type.valueContainsNull,
                "value": self._convert_type(data_type.valueType),
            }

        return data_type.simpleString()

    def _convert_field(self, field: StructField) -> dict[str, Any]:
        converted_field = {"name": field.name, "required": not field.nullable, "type": self._convert_type(field.dataType)}

        comment = field.metadata.get("comment")
        if comment:
            converted_field["doc"] = comment

        return converted_field
