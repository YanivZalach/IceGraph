import secrets
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace

from flask import copy_current_request_context

from assistant.assistant_chat import AssistantChat
from assistant.constants import STATUS_COMPLETED, STATUS_FAILED, STATUS_PROCESSING
from env import Env
from icegraph_logger import logger


@dataclass
class AssistantJob:
    job_id: str
    token: str
    status: str = STATUS_PROCESSING
    answer: str = ""
    error: str = ""


class AssistantJobs:
    def __init__(self):
        self._lock = threading.Lock()
        self._jobs: dict[str, AssistantJob] = {}
        self._executor_pool = ThreadPoolExecutor(max_workers=Env.AI_MAX_CONCURRENT_REQUESTS)

    def submit(self, chat: AssistantChat) -> AssistantJob | None:
        job = AssistantJob(job_id=secrets.token_urlsafe(16), token=secrets.token_urlsafe(32))

        with self._lock:
            processing_jobs = sum(1 for existing_job in self._jobs.values() if existing_job.status == STATUS_PROCESSING)
            if processing_jobs >= Env.AI_MAX_CONCURRENT_REQUESTS:
                return None

            self._jobs[job.job_id] = job
            submitted_job = replace(job)

        self._executor_pool.submit(copy_current_request_context(self._answer_in_background), job.job_id, chat)

        return submitted_job

    def read(self, job_id: str, token: str) -> AssistantJob | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or not secrets.compare_digest(token.encode(), job.token.encode()):
                return None

            return replace(job)

    def shutdown(self):
        self._executor_pool.shutdown(wait=False, cancel_futures=True)

    def _answer_in_background(self, job_id: str, chat: AssistantChat):
        def _append_answer(answer_part):
            with self._lock:
                self._jobs[job_id].answer += answer_part

        try:
            chat.answer(_append_answer)

            with self._lock:
                self._jobs[job_id].status = STATUS_COMPLETED
            logger.info(f"Assistant job {job_id} completed")

        except Exception as e:
            logger.error(f"Assistant job {job_id} failed: {e}\n{traceback.format_exc()}")
            with self._lock:
                self._jobs[job_id].status = STATUS_FAILED
                self._jobs[job_id].error = AssistantChat.describe_error(e)

        finally:
            self._schedule_cleanup(job_id)

    def _schedule_cleanup(self, job_id: str):
        timer = threading.Timer(Env.COMPUTE_CLEANUP_TIME_SECONDS, self._cleanup_job, args=(job_id,))
        timer.daemon = True
        timer.start()

    def _cleanup_job(self, job_id: str):
        with self._lock:
            self._jobs.pop(job_id, None)
        logger.info(f"Removed assistant job {job_id}")
