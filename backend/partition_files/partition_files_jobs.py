import hashlib
import json
import secrets
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor

from flask import copy_current_request_context
from pyspark.errors import AnalysisException

from constants import JOB_TOKEN_FIELD
from env import Env
from icegraph_logger import logger
from partition_files.constants import PARTITION_FILES_STAGES, SPARK_SQL_FIELD
from partition_files.partition_files import PartitionFiles, PartitionFilesLimitError


class PartitionFilesJobs:
    def __init__(self):
        self._lock = threading.Lock()
        self._jobs: dict[str, dict] = {}
        self._executor_pool = ThreadPoolExecutor(max_workers=Env.MAX_NUMBER_OF_GRAPHS_TO_COMPUTE)

    def submit(self, table_name: str, snapshot_id: int | None, filters: dict[str, str]) -> dict:
        job_id = self._build_job_id(table_name, snapshot_id, filters)
        response = {"key": job_id, "status": "processing"}

        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                if job["status"] == "completed":
                    job["timer"].cancel()
                    self._schedule_cleanup(job_id, is_in_lock_block=True)
                response[JOB_TOKEN_FIELD] = job[JOB_TOKEN_FIELD]

                return response

            response[JOB_TOKEN_FIELD] = secrets.token_urlsafe(32)
            self._jobs[job_id] = dict(response)

        self._executor_pool.submit(copy_current_request_context(self._compute_in_background), job_id, table_name, snapshot_id, filters)
        logger.info(f"Submitted partition files job {job_id}")

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
    def _build_job_id(table_name: str, snapshot_id: int | None, filters: dict[str, str]) -> str:
        request_key = json.dumps([table_name, str(snapshot_id), sorted(filters.items())])

        return f"partition_{hashlib.sha256(request_key.encode('utf-8')).hexdigest()}"

    def _safe_update_job(self, job_id: str, **fields):
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].update(fields)

    def _cleanup_job(self, job_id: str):
        with self._lock:
            self._jobs.pop(job_id, None)

    def _schedule_cleanup(self, job_id: str, is_in_lock_block: bool = False):
        timer = threading.Timer(Env.COMPUTE_CLEANUP_TIME_SECONDS, lambda: self._cleanup_job(job_id))
        timer.daemon = True
        if is_in_lock_block:
            self._jobs[job_id]["timer"] = timer
        else:
            self._safe_update_job(job_id, timer=timer)
        timer.start()

    def _compute_in_background(self, job_id: str, table_name: str, snapshot_id: int | None, filters: dict[str, str]):
        stages = {stage_name: "pending" for stage_name in PARTITION_FILES_STAGES}

        def _on_stage(stage_name: str, status: str):
            stages[stage_name] = status
            self._safe_update_job(job_id, stages=dict(stages))

        try:
            result = PartitionFiles(table_name, snapshot_id, filters, _on_stage).build()
            self._safe_update_job(job_id, status="completed", result=result)

        except PartitionFilesLimitError as e:
            logger.warning(f"Partition files job {job_id} reached its limit: {e}")
            self._safe_update_job(job_id, status="failed", error=str(e), **{SPARK_SQL_FIELD: e.spark_sql})

        except AnalysisException as e:
            logger.error(f"Spark Error in partition files job {job_id}: {e}\n{traceback.format_exc()}")
            self._safe_update_job(job_id, status="failed", error=str(e))

        except Exception as e:
            logger.error(f"Unexpected error in partition files job {job_id}: {e}\n{traceback.format_exc()}")
            self._safe_update_job(job_id, status="failed", error=str(e))

        finally:
            self._schedule_cleanup(job_id)
