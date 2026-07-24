from spark_connect import open_spark_connect_session
import json
from contextlib import suppress
from typing import Any, Dict, List


def format_schemas_to_full_dict(schemas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    formatted_schemas = []
    for schema in schemas:
        formatted_schema = schema.copy()
        for field in formatted_schema["fields"]:
            with suppress(Exception):
                field["type"] = json.loads(field["type"])

        formatted_schemas.append(formatted_schema)

    return formatted_schemas


def parse_json_string_fields(data: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
    for field in fields:
        if isinstance(data.get(field), str):
            with suppress(Exception):
                data[field] = json.loads(data[field])

    return data


def get_json_metadata_from_path(metadata_path: str) -> Dict[str, Any]:
    spark = open_spark_connect_session()

    row = (
        spark.read.option("multiLine", True)
        .json(metadata_path)
        .drop("metadata-log")
        .drop("snapshot-log")
        .drop("snapshots")
        .drop("statistics")
        .first()
    )

    return row.asDict(recursive=True)
