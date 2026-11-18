import os
import threading
import traceback
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_from_directory
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pyspark.errors import AnalysisException

from assistant.assistant_routes import assistant_blueprint, assistant_jobs
from base_classes.utils import collect_graph_metadata_file, verify_iceberg_table
from constants import JOB_TOKEN_FIELD
from env import Env
from graph_jobs.graph_jobs import GraphJobs
from icegraph_logger import logger
from snapshot_map.snapshot_mapping import collect_snapshot_map
from spark_connect import close_spark_connect_session
from collectors.collect_table_description import TableDescriptionCollector
from collectors.collect_table_metadata import TableMetadataCollector
from table_list_catalog.table_list_catalog import TableListCatalog

app = Flask(__name__, static_url_path="/static")
app.json.sort_keys = False
app.register_blueprint(assistant_blueprint)

graph_jobs = GraphJobs()


@app.route("/metrics", methods=["GET"])
def metrics():
    if not Env.METRICS_ENABLED:
        return jsonify({"error": "Metrics are disabled"}), 404

    return Response(generate_latest(), content_type=CONTENT_TYPE_LATEST)


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve(path):
    static_path = Path(app.static_folder).resolve()
    requested_path = (static_path / path).resolve()

    if requested_path.is_file():
        return send_from_directory(static_path, path)

    return send_from_directory(static_path, "index.html")


@app.route("/api/v1/tables", methods=["GET"])
def list_tables():
    try:
        tables = TableListCatalog().collect()

        return jsonify(
            {
                "tables": tables,
                "include_none_iceberg_catalogs": Env.INCLUDE_NONE_ICEBERG_CATALOGS,
                "browser_refresh_seconds": Env.TABLE_LIST_BROWSER_REFRESH_SECONDS,
            }
        )

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/graph-metadata-file/<path:table_name>", methods=["GET"])
def graph_metadata_file(table_name):
    end_snapshot_id = request.args.get("end_snapshot_id")

    try:
        verify_iceberg_table(table_name)
        parsed_end_snapshot_id = int(end_snapshot_id) if end_snapshot_id else None
        metadata_file = collect_graph_metadata_file(table_name, parsed_end_snapshot_id)
        response = jsonify({"metadata_file": metadata_file})
        response.headers["Cache-Control"] = "no-store"

        return response

    except ValueError as e:
        logger.error(f"Invalid graph metadata request: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/table-metadata/<path:table_name>", methods=["GET"])
def table_metadata(table_name):
    try:
        verify_iceberg_table(table_name)
        return jsonify(TableMetadataCollector(table_name).collect_latest())

    except ValueError as e:
        logger.error(f"Invalid table metadata request: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/table-description", methods=["GET"])
def table_description():
    table_name = request.args.get("table_name")
    try:
        return jsonify(TableDescriptionCollector(table_name).collect())

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/snapshot-map/<path:table_name>", methods=["GET"])
def snapshot_map(table_name):
    before_snapshot_id = request.args.get("before_snapshot_id")

    try:
        verify_iceberg_table(table_name)
        parsed_before_snapshot_id = int(before_snapshot_id) if before_snapshot_id else None

        result = collect_snapshot_map(table_name, Env.MAX_SNAPSHOTS_TO_SHOW, parsed_before_snapshot_id)

        return jsonify(result)

    except ValueError as e:
        logger.error(f"Invalid snapshot map request: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/graph-data", methods=["POST"])
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


@app.route("/api/v1/graph-data/<job_id>", methods=["GET"])
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


def _force_exit():
    logger.error(f"Graceful shutdown timed out after {Env.MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS} seconds - forcing exit.")
    os._exit(1)


if __name__ == "__main__":
    try:
        if Env.PRODUCTION_MODE:
            from waitress import serve

            serve(app, host="0.0.0.0", port=Env.APPLICATION_PORT, threads=Env.WSGI_THREADS)
        else:
            app.run(host="0.0.0.0", port=Env.APPLICATION_PORT, debug=True)

    finally:
        watchdog = threading.Timer(Env.MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS, _force_exit)
        watchdog.daemon = True
        watchdog.start()

        close_spark_connect_session()
        graph_jobs.shutdown()
        assistant_jobs.shutdown()
        logger.info("Compute executor pool shutdown")
