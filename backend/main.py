import os
import re
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from pyspark.errors import AnalysisException

from constants import (
    APPLICATION_PORT,
    COMPUTE_CLEANUP_TIME_SECONDS,
    MAX_NUMBER_OF_GRAPHS_TO_COMPUTE,
    MAX_SNAPSHOTS_TO_SHOW,
    MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS,
    INCLUDE_NONE_ICEBERG_CATALOGS,
    PRODUCTION_MODE,
    WSGI_THREADS,
)
from spark_connect import close_spark_connect_session
from graph_normalizer.graph_normalizer import GraphNormalizer
from icegraph_logger import logger
from snapshot_map.snapshot_mapping import collect_snapshot_map
from table_list_catalog.table_list_catalog import TableListCatalog
from table_inventory.table_inventory import TableInventory
from base_classes.utils import verify_iceberg_table

load_dotenv()
app = Flask(__name__, static_url_path="/static")

job_lock = threading.Lock()
jobs: dict[str, dict] = {}

max_number_of_graphs_to_compute = int(os.getenv("MAX_NUMBER_OF_GRAPHS_TO_COMPUTE", MAX_NUMBER_OF_GRAPHS_TO_COMPUTE))
compute_cleanup_time_seconds = int(os.getenv("COMPUTE_CLEANUP_TIME_SECONDS", COMPUTE_CLEANUP_TIME_SECONDS))
max_snapshots_to_show = int(os.getenv("MAX_SNAPSHOTS_TO_SHOW", MAX_SNAPSHOTS_TO_SHOW))
max_graceful_shutdown_time_seconds = int(os.getenv("MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS", MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS))
include_none_iceberg_catalogs = str(os.getenv("INCLUDE_NONE_ICEBERG_CATALOGS", INCLUDE_NONE_ICEBERG_CATALOGS)).lower() == "true"
production_mode = str(os.getenv("PRODUCTION_MODE", PRODUCTION_MODE)).lower() == "true"
wsgi_threads = int(os.getenv("WSGI_THREADS", WSGI_THREADS))

executor_pool = ThreadPoolExecutor(max_workers=max_number_of_graphs_to_compute)


def _safe_update_job(job_id, **fields):
    with job_lock:
        if job_id in jobs:
            jobs[job_id].update(fields)


def _cleanup_job(job_id):
    with job_lock:
        jobs.pop(job_id, None)
    logger.info(f"Removed job {job_id}")


def _schedule_cleanup(job_id, is_in_lock_block=False):
    timer = threading.Timer(
        compute_cleanup_time_seconds,
        lambda job_id=job_id: _cleanup_job(job_id),
    )
    timer.daemon = True

    if is_in_lock_block:
        jobs[job_id]["timer"] = timer
    else:
        _safe_update_job(job_id, timer=timer)

    timer.start()


def _compute_graph_background(job_id, table_name, start_snapshot_id, end_snapshot_id):
    try:
        table_data = TableInventory(table_name, start_snapshot_id, end_snapshot_id).build()

        result = GraphNormalizer(table_data).normalize()

        _safe_update_job(job_id, status="completed", result=result)
        logger.info(f"Job {job_id} completed")

    except AnalysisException as e:
        logger.error(f"Spark Error in job {job_id}: {e}\n{traceback.format_exc()}")
        _safe_update_job(job_id, status="failed", error=str(e))

    except Exception as e:
        logger.error(f"Unexpected error in job {job_id}: {e}\n{traceback.format_exc()}")
        _safe_update_job(job_id, status="failed", error=str(e))

    finally:
        _schedule_cleanup(job_id)


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
                "include_none_iceberg_catalogs": include_none_iceberg_catalogs,
            }
        )

    except AnalysisException as e:
        logger.error(f"Spark Error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/snapshot-map/<path:table_name>", methods=["GET"])
def snapshot_map(table_name):
    try:
        verify_iceberg_table(table_name)

        result = collect_snapshot_map(table_name, max_snapshots_to_show)

        return jsonify(result)

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

    key_table_name = table_name.replace(".", "_")
    job_id = re.sub(
        r"[^a-zA-Z0-9_]",
        "",
        f"{key_table_name}_{start_snapshot_id}_{end_snapshot_id}",
    )

    response = {"key": job_id, "status": "processing"}

    with job_lock:
        job = jobs.get(job_id)
        if job:
            if job["status"] == "completed":
                job["timer"].cancel()
                _schedule_cleanup(job_id, is_in_lock_block=True)
                logger.info(f"Job {job_id} completed, extended cleanup timer")

            else:
                logger.info(f"Duplicate request for {job_id}")

            return jsonify(response), 202

        jobs[job_id] = response

    executor_pool.submit(
        _compute_graph_background,
        job_id,
        table_name,
        start_snapshot_id,
        end_snapshot_id,
    )

    logger.info(f"Submitted job {job_id}")
    return jsonify(response), 202


@app.route("/api/v1/graph-data/<job_id>", methods=["GET"])
def get_job_status(job_id):
    with job_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404

        job = job.copy()

    status = job["status"]

    if status == "completed":
        return jsonify(job["result"]), 200

    elif status == "failed":
        return jsonify({"error": job.get("error", "Unknown error")}), 400

    else:
        return jsonify({"key": job_id, "status": "processing"}), 202


def _force_exit():
    logger.error(f"Graceful shutdown timed out after {max_graceful_shutdown_time_seconds} seconds - forcing exit.")
    os._exit(1)


if __name__ == "__main__":
    try:
        if production_mode:
            from waitress import serve

            serve(app, host="0.0.0.0", port=APPLICATION_PORT, threads=wsgi_threads)
        else:
            app.run(host="0.0.0.0", port=APPLICATION_PORT, debug=True)

    finally:
        watchdog = threading.Timer(max_graceful_shutdown_time_seconds, _force_exit)
        watchdog.daemon = True
        watchdog.start()

        close_spark_connect_session()
        executor_pool.shutdown(wait=False, cancel_futures=True)
        logger.info("Compute executor pool shutdown")
