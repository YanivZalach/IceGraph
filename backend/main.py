import os
import re
import secrets
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from pyspark.errors import AnalysisException

from base_classes.utils import verify_iceberg_table
from constants import APPLICATION_PORT, COLLECTION_STAGES, JOB_TOKEN_FIELD, STAGE_BUILD_GRAPH
from env import Env
from graph_cache.metadata_file import collect_graph_metadata_file
from graph_normalizer.graph_normalizer import GraphNormalizer
from icegraph_logger import logger
from snapshot_analyzer.snapshot_analyzer import SnapshotAnalyzer
from snapshot_map.snapshot_mapping import collect_snapshot_map
from spark_connect import close_spark_connect_session
from table_inventory.table_inventory import TableInventory
from table_list_catalog.table_list_catalog import TableListCatalog

app = Flask(__name__, static_url_path="/static")
app.json.sort_keys = False

job_lock = threading.Lock()
jobs: dict[str, dict] = {}

executor_pool = ThreadPoolExecutor(max_workers=Env.MAX_NUMBER_OF_GRAPHS_TO_COMPUTE)


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
        Env.COMPUTE_CLEANUP_TIME_SECONDS,
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
        stages = {stage_name: "pending" for stage_name in COLLECTION_STAGES}

        def _on_stage(stage_name, status):
            stages[stage_name] = status
            _safe_update_job(job_id, stages=dict(stages))

        table_data = TableInventory(table_name, _on_stage, start_snapshot_id, end_snapshot_id).build()

        _on_stage(STAGE_BUILD_GRAPH, "in_progress")
        try:
            table_data = SnapshotAnalyzer(table_data).analyze()
            result = GraphNormalizer(table_data).normalize()
        finally:
            _on_stage(STAGE_BUILD_GRAPH, "done")

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
                "include_none_iceberg_catalogs": Env.INCLUDE_NONE_ICEBERG_CATALOGS,
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
        if not end_snapshot_id:
            raise ValueError("end_snapshot_id is required")

        parsed_end_snapshot_id = int(end_snapshot_id)
        metadata_file = collect_graph_metadata_file(table_name, parsed_end_snapshot_id)

        return jsonify({"metadata_file": metadata_file})

    except ValueError as e:
        logger.error(f"Invalid graph metadata request: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 400

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

        result = collect_snapshot_map(table_name, Env.MAX_SNAPSHOTS_TO_SHOW)

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

            response[JOB_TOKEN_FIELD] = job[JOB_TOKEN_FIELD]
            return jsonify(response), 202

        token = secrets.token_urlsafe(32)
        response[JOB_TOKEN_FIELD] = token
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
    token = request.headers.get(JOB_TOKEN_FIELD, "")

    with job_lock:
        job = jobs.get(job_id)
        if not job or not token or not secrets.compare_digest(token, job[JOB_TOKEN_FIELD]):
            return jsonify({"error": "Job not found"}), 404

        job = job.copy()

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

            serve(app, host="0.0.0.0", port=APPLICATION_PORT, threads=Env.WSGI_THREADS)
        else:
            app.run(host="0.0.0.0", port=APPLICATION_PORT, debug=True)

    finally:
        watchdog = threading.Timer(Env.MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS, _force_exit)
        watchdog.daemon = True
        watchdog.start()

        close_spark_connect_session()
        executor_pool.shutdown(wait=False, cancel_futures=True)
        logger.info("Compute executor pool shutdown")
