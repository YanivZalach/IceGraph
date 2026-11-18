from flask import Blueprint, jsonify, request

from assistant.assistant_chat import AssistantChat
from assistant.assistant_jobs import AssistantJobs
from assistant.assistant_request import AssistantRequest
from assistant.constants import (
    BUSY_ERROR,
    CONTEXT_TOO_LARGE_ERROR,
    INVALID_REQUEST_ERROR,
    JOB_NOT_FOUND_ERROR,
    NOT_CONFIGURED_ERROR,
    REQUEST_TOO_LARGE_ERROR,
    STATUS_COMPLETED,
    STATUS_FAILED,
)
from constants import JOB_TOKEN_FIELD
from env import Env
from icegraph_logger import logger
from metrics import assistant_questions_total

MAX_BYTES_PER_CHARACTER = 6
REQUEST_STRUCTURE_BYTES = 65536

assistant_blueprint = Blueprint("assistant", __name__)
assistant_jobs = AssistantJobs()


def _context_too_large(characters):
    error = CONTEXT_TOO_LARGE_ERROR.format(characters=characters, max_characters=Env.AI_MAX_CONTEXT_CHARACTERS)

    return jsonify({"error": error}), 413


@assistant_blueprint.route("/api/v1/assistant", methods=["GET"])
def assistant_status():
    return jsonify({"enabled": AssistantChat.is_enabled(), "max_context_characters": Env.AI_MAX_CONTEXT_CHARACTERS})


@assistant_blueprint.route("/api/v1/assistant/messages", methods=["POST"])
def ask_assistant():
    if not AssistantChat.is_enabled():
        return jsonify({"error": NOT_CONFIGURED_ERROR}), 404

    content_length = request.content_length or 0
    if content_length > Env.AI_MAX_CONTEXT_CHARACTERS * MAX_BYTES_PER_CHARACTER + REQUEST_STRUCTURE_BYTES:
        return jsonify({"error": REQUEST_TOO_LARGE_ERROR}), 413

    body = request.get_json(silent=True)
    problem = AssistantRequest.find_problem(body)
    if problem:
        return jsonify({"error": INVALID_REQUEST_ERROR.format(reason=problem)}), 400

    assistant_request = AssistantRequest.from_body(body)
    characters = assistant_request.count_characters()
    if characters > Env.AI_MAX_CONTEXT_CHARACTERS:
        return _context_too_large(characters)

    table_name = assistant_request.table_name
    job = assistant_jobs.submit(AssistantChat(assistant_request))
    if job is None:
        logger.warning(f"[{table_name}] Assistant question rejected, {Env.AI_MAX_CONCURRENT_REQUESTS} answers already in progress")

        return jsonify({"error": BUSY_ERROR}), 429

    assistant_questions_total.inc()
    logger.info(f"[{table_name}] Submitted assistant job {job.job_id} with {characters} characters")

    return jsonify({"key": job.job_id, "status": job.status, JOB_TOKEN_FIELD: job.token}), 202


@assistant_blueprint.route("/api/v1/assistant/messages/<job_id>", methods=["GET"])
def get_assistant_answer(job_id):
    job = assistant_jobs.read(job_id, request.headers.get(JOB_TOKEN_FIELD, ""))
    if job is None:
        return jsonify({"error": JOB_NOT_FOUND_ERROR}), 404

    if job.status == STATUS_FAILED:
        return jsonify({"error": job.error}), 502

    response = {"key": job_id, "status": job.status, "answer": job.answer}

    return jsonify(response), 200 if job.status == STATUS_COMPLETED else 202
