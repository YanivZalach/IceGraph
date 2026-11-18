import traceback

from flask import Blueprint, jsonify, request
from pyspark.errors import AnalysisException

from base_classes.utils import verify_iceberg_table
from constants import JOB_TOKEN_FIELD
from graph.graph_jobs import GraphJobs
from icegraph_logger import logger

graph_blueprint = Blueprint("graph", __name__)
graph_jobs = GraphJobs()


@graph_blueprint.route("/api/v1/graph-data", methods=["POST"])
def graph_data():
    table_name = request.form.get("table_name")
    start_snapshot_id = request.form.get("start_snapshot_id")
    if start_snapshot_id:
        start_snapshot_id = int(start_snapshot_id)
    end_snapshot_id = request.form.get("end_snapshot_id")
    if end_snapshot_id:
        end_snapshot_id = int(end_snapshot_id)

    try:
        verify_iceberg_table(table_name)

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}\n{traceback.format_exc()}")

        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}")

        return jsonify({"error": str(e)}), 500

    response = graph_jobs.submit(table_name, start_snapshot_id, end_snapshot_id)

    return jsonify(response), 202


@graph_blueprint.route("/api/v1/graph-data/<job_id>", methods=["GET"])
def get_job_status(job_id):
    token = request.headers.get(JOB_TOKEN_FIELD, "")

    job = graph_jobs.read(job_id, token)
    if job is None:
        return jsonify({"error": "Job not found"}), 404

    status = job["status"]

    if status == "completed":
        return jsonify(job["result"]), 200

    elif status == "failed":
        return jsonify({"error": job.get("error", "Unknown error"), "stages": job.get("stages")}), 400

    else:
        return jsonify(
            {
                "key": job_id,
                "status": "processing",
                "stages": job.get("stages"),
            }
        ), 202
