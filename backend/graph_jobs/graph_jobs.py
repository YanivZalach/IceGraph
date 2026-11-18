import re
import secrets
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor

from flask import copy_current_request_context
from pyspark.errors import AnalysisException

from constants import COLLECTION_STAGES, JOB_TOKEN_FIELD, STAGE_BUILD_GRAPH
from env import Env
from graph_normalizer.graph_normalizer import GraphNormalizer
from icegraph_logger import logger
from metrics import graph_computations_total, graph_job_cache_reads_total
from snapshot_analyzer.snapshot_analyzer import SnapshotAnalyzer
from table_inventory.table_inventory import TableInventory


class GraphJobs:
    def __init__(self):
        self._lock = threading.Lock()
        self._jobs: dict[str, dict] = {}
        self._executor_pool = ThreadPoolExecutor(max_workers=Env.MAX_NUMBER_OF_GRAPHS_TO_COMPUTE)

    def submit(self, table_name: str, start_snapshot_id: int | None, end_snapshot_id: int | None) -> dict:
        job_id = self._build_job_id(table_name, start_snapshot_id, end_snapshot_id)
        response = {"key": job_id, "status": "processing"}

        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                if job["status"] == "completed":
                    graph_job_cache_reads_total.inc()
                    job["timer"].cancel()
                    self._schedule_cleanup(job_id, is_in_lock_block=True)
                    logger.info(f"Job {job_id} completed, extended cleanup timer")

                else:
                    logger.info(f"Duplicate request for {job_id}")

                response[JOB_TOKEN_FIELD] = job[JOB_TOKEN_FIELD]

                return response

            token = secrets.token_urlsafe(32)
            response[JOB_TOKEN_FIELD] = token
            self._jobs[job_id] = response

        self._executor_pool.submit(
            copy_current_request_context(self._compute_graph_background),
            job_id,
            table_name,
            start_snapshot_id,
            end_snapshot_id,
        )

        graph_computations_total.inc()
        logger.info(f"Submitted job {job_id}")

        return response

    def read(self, job_id: str, token: str) -> dict | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or not token or not secrets.compare_digest(token, job[JOB_TOKEN_FIELD]):
                return None

            return job.copy()

    def shutdown(self):
        self._executor_pool.shutdown(wait=False, cancel_futures=True)

    @staticmethod
    def _build_job_id(table_name: str, start_snapshot_id: int | None, end_snapshot_id: int | None) -> str:
        key_table_name = table_name.replace(".", "_")

        return re.sub(
            r"[^a-zA-Z0-9_]",
            "",
            f"{key_table_name}_{start_snapshot_id}_{end_snapshot_id}",
        )

    def _safe_update_job(self, job_id, **fields):
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].update(fields)

    def _cleanup_job(self, job_id):
        with self._lock:
            self._jobs.pop(job_id, None)
        logger.info(f"Removed job {job_id}")

    def _schedule_cleanup(self, job_id, is_in_lock_block=False):
        timer = threading.Timer(
            Env.COMPUTE_CLEANUP_TIME_SECONDS,
            lambda job_id=job_id: self._cleanup_job(job_id),
        )
        timer.daemon = True

        if is_in_lock_block:
            self._jobs[job_id]["timer"] = timer
        else:
            self._safe_update_job(job_id, timer=timer)

        timer.start()

    def _compute_graph_background(self, job_id, table_name, start_snapshot_id, end_snapshot_id):
        try:
            stages = {stage_name: "pending" for stage_name in COLLECTION_STAGES}

            def _on_stage(stage_name, status):
                stages[stage_name] = status
                self._safe_update_job(job_id, stages=dict(stages))

            table_data = TableInventory(table_name, _on_stage, start_snapshot_id, end_snapshot_id).build()

            _on_stage(STAGE_BUILD_GRAPH, "in_progress")
            try:
                table_data = SnapshotAnalyzer(table_data).analyze()
                result = GraphNormalizer(table_data).normalize()
            finally:
                _on_stage(STAGE_BUILD_GRAPH, "done")

            self._safe_update_job(job_id, status="completed", result=result)
            logger.info(f"Job {job_id} completed")

        except AnalysisException as e:
            logger.error(f"Spark Error in job {job_id}: {e}\n{traceback.format_exc()}")
            self._safe_update_job(job_id, status="failed", error=str(e))

        except Exception as e:
            logger.error(f"Unexpected error in job {job_id}: {e}\n{traceback.format_exc()}")
            self._safe_update_job(job_id, status="failed", error=str(e))

        finally:
            self._schedule_cleanup(job_id)
