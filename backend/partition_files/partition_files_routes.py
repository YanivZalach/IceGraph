import json
import traceback

from flask import Blueprint, jsonify, request
from pyspark.errors import AnalysisException

from base_classes.utils import verify_iceberg_table
from constants import JOB_TOKEN_FIELD
from icegraph_logger import logger
from partition_files.constants import EMPTY_PARTITION_FILTER_ERROR, SPARK_SQL_FIELD
from partition_files.partition_files_jobs import PartitionFilesJobs

partition_files_blueprint = Blueprint("partition_files", __name__)
partition_files_jobs = PartitionFilesJobs()


@partition_files_blueprint.route("/api/v1/partition-files", methods=["POST"])
def partition_files():
    table_name = request.form.get("table_name")
    snapshot_id = request.form.get("snapshot_id", "")
    if not table_name or not snapshot_id.lstrip("-").isdigit():
        return jsonify({"error": "table_name and a numeric snapshot_id are required."}), 400

    try:
        filters = json.loads(request.form.get("filters") or "{}")

    except ValueError:
        return jsonify({"error": "filters must be a JSON object."}), 400

    if not isinstance(filters, dict) or not all(isinstance(key, str) and isinstance(value, str) for key, value in filters.items()):
        return jsonify({"error": "filters must be a JSON object of partition field names to string values."}), 400

    if not filters:
        return jsonify({"error": EMPTY_PARTITION_FILTER_ERROR}), 400

    try:
        verify_iceberg_table(table_name)

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}\n{traceback.format_exc()}")

        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}")

        return jsonify({"error": str(e)}), 500

    return jsonify(partition_files_jobs.submit(table_name, int(snapshot_id), filters)), 202


@partition_files_blueprint.route("/api/v1/partition-files/<job_id>", methods=["GET"])
def get_partition_files_job(job_id):
    job = partition_files_jobs.read(job_id, request.headers.get(JOB_TOKEN_FIELD, ""))
    if job is None:
        return jsonify({"error": "Job not found"}), 404

    if job["status"] == "completed":
        return jsonify(job["result"]), 200

    if job["status"] == "failed":
        return jsonify({"error": job.get("error", "Unknown error"), "stages": job.get("stages"), SPARK_SQL_FIELD: job.get(SPARK_SQL_FIELD)}), 400

    return jsonify({"key": job_id, "status": "processing", "stages": job.get("stages")}), 202
