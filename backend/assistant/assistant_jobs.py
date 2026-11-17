import secrets
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor

from flask import copy_current_request_context

from assistant.assistant_chat import answer_question, describe_assistant_error
from assistant.constants import STATUS_COMPLETED, STATUS_FAILED, STATUS_PROCESSING
from constants import JOB_TOKEN_FIELD
from env import Env
from icegraph_logger import logger

job_lock = threading.Lock()
jobs: dict[str, dict] = {}

executor_pool = ThreadPoolExecutor(max_workers=Env.AI_MAX_CONCURRENT_REQUESTS)


def _cleanup_job(job_id):
    with job_lock:
        jobs.pop(job_id, None)
    logger.info(f"Removed assistant job {job_id}")


def _schedule_cleanup(job_id):
    timer = threading.Timer(Env.COMPUTE_CLEANUP_TIME_SECONDS, _cleanup_job, args=(job_id,))
    timer.daemon = True
    timer.start()


def _answer_in_background(job_id, chat_messages):
    def _append_answer(answer_part):
        with job_lock:
            jobs[job_id]["answer"] += answer_part

    try:
        answer_question(chat_messages, _append_answer)

        with job_lock:
            jobs[job_id]["status"] = STATUS_COMPLETED
        logger.info(f"Assistant job {job_id} completed")

    except Exception as e:
        logger.error(f"Assistant job {job_id} failed: {e}\n{traceback.format_exc()}")
        with job_lock:
            jobs[job_id].update(status=STATUS_FAILED, error=describe_assistant_error(e))

    finally:
        _schedule_cleanup(job_id)


def submit_assistant_job(chat_messages: list[dict]) -> dict | None:
    job_id = secrets.token_urlsafe(16)
    token = secrets.token_urlsafe(32)

    with job_lock:
        processing_jobs = sum(1 for job in jobs.values() if job["status"] == STATUS_PROCESSING)
        if processing_jobs >= Env.AI_MAX_CONCURRENT_REQUESTS:
            return None

        jobs[job_id] = {"status": STATUS_PROCESSING, "answer": "", JOB_TOKEN_FIELD: token}

    executor_pool.submit(copy_current_request_context(_answer_in_background), job_id, chat_messages)

    return {"key": job_id, "status": STATUS_PROCESSING, JOB_TOKEN_FIELD: token}


def read_assistant_job(job_id: str, token: str) -> dict | None:
    with job_lock:
        job = jobs.get(job_id)
        if not job or not secrets.compare_digest(token.encode(), job[JOB_TOKEN_FIELD].encode()):
            return None

        return job.copy()


def shutdown_assistant_jobs():
    executor_pool.shutdown(wait=False, cancel_futures=True)
