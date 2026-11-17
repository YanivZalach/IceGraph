from flask import Blueprint, jsonify, request

from assistant.assistant_chat import build_chat_messages, is_assistant_enabled
from assistant.assistant_jobs import read_assistant_job, submit_assistant_job
from assistant.constants import (
    ASSISTANT_ROLE,
    BUSY_ERROR,
    CONTEXT_TOO_LARGE_ERROR,
    INVALID_REQUEST_ERROR,
    JOB_NOT_FOUND_ERROR,
    NOT_CONFIGURED_ERROR,
    PAGE_DESCRIPTIONS,
    REQUEST_TOO_LARGE_ERROR,
    STATUS_COMPLETED,
    STATUS_FAILED,
    USER_ROLE,
)
from constants import JOB_TOKEN_FIELD
from env import Env
from icegraph_logger import logger
from metrics import assistant_questions_total

MAX_BYTES_PER_CHARACTER = 6
REQUEST_STRUCTURE_BYTES = 65536

assistant_blueprint = Blueprint("assistant", __name__)


def _is_valid_text(text: str) -> bool:
    try:
        text.encode("utf-8")

    except UnicodeEncodeError:
        return False

    return True


def _find_request_problem(body) -> str | None:
    if not isinstance(body, dict):
        return "the body must be a JSON object."

    if not isinstance(body.get("table_name"), str) or not body["table_name"]:
        return "table_name is required."

    if not isinstance(body.get("page"), str) or body["page"] not in PAGE_DESCRIPTIONS:
        return f"page must be one of {', '.join(PAGE_DESCRIPTIONS)}."

    for field in ("start_snapshot_id", "end_snapshot_id", "context"):
        if not isinstance(body.get(field, ""), str):
            return f"{field} must be a string."

    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        return "messages must be a non-empty list."

    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in (USER_ROLE, ASSISTANT_ROLE):
            return "every message needs a user or assistant role."

        if not isinstance(message.get("content"), str) or not message["content"]:
            return "every message needs non-empty content."

    if messages[-1]["role"] != USER_ROLE:
        return "the last message must be from the user."

    texts = [body["table_name"], body.get("start_snapshot_id", ""), body.get("end_snapshot_id", ""), body.get("context", "")]
    texts.extend(message["content"] for message in messages)
    for text in texts:
        if not _is_valid_text(text):
            return "text must be valid Unicode."

    return None


def _count_characters(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _context_too_large(characters):
    error = CONTEXT_TOO_LARGE_ERROR.format(characters=characters, max_characters=Env.AI_MAX_CONTEXT_CHARACTERS)

    return jsonify({"error": error}), 413


@assistant_blueprint.route("/api/v1/assistant", methods=["GET"])
def assistant_status():
    return jsonify({"enabled": is_assistant_enabled(), "max_context_characters": Env.AI_MAX_CONTEXT_CHARACTERS})


@assistant_blueprint.route("/api/v1/assistant/messages", methods=["POST"])
def ask_assistant():
    if not is_assistant_enabled():
        return jsonify({"error": NOT_CONFIGURED_ERROR}), 404

    content_length = request.content_length or 0
    if content_length > Env.AI_MAX_CONTEXT_CHARACTERS * MAX_BYTES_PER_CHARACTER + REQUEST_STRUCTURE_BYTES:
        return jsonify({"error": REQUEST_TOO_LARGE_ERROR}), 413

    body = request.get_json(silent=True)
    problem = _find_request_problem(body)
    if problem:
        return jsonify({"error": INVALID_REQUEST_ERROR.format(reason=problem)}), 400

    context = body.get("context", "")
    messages = [{"role": message["role"], "content": message["content"]} for message in body["messages"]]
    characters = _count_characters(context) + sum(_count_characters(message["content"]) for message in messages)
    if characters > Env.AI_MAX_CONTEXT_CHARACTERS:
        return _context_too_large(characters)

    table_name = body["table_name"]
    chat_messages = build_chat_messages(
        table_name,
        body.get("start_snapshot_id", ""),
        body.get("end_snapshot_id", ""),
        body["page"],
        context,
        messages,
    )

    job = submit_assistant_job(chat_messages)
    if job is None:
        logger.warning(f"[{table_name}] Assistant question rejected, {Env.AI_MAX_CONCURRENT_REQUESTS} answers already in progress")

        return jsonify({"error": BUSY_ERROR}), 429

    assistant_questions_total.inc()
    logger.info(f"[{table_name}] Submitted assistant job {job['key']} with {characters} characters")

    return jsonify(job), 202


@assistant_blueprint.route("/api/v1/assistant/messages/<job_id>", methods=["GET"])
def get_assistant_answer(job_id):
    job = read_assistant_job(job_id, request.headers.get(JOB_TOKEN_FIELD, ""))
    if job is None:
        return jsonify({"error": JOB_NOT_FOUND_ERROR}), 404

    if job["status"] == STATUS_FAILED:
        return jsonify({"error": job["error"]}), 502

    response = {"key": job_id, "status": job["status"], "answer": job["answer"]}

    return jsonify(response), 200 if job["status"] == STATUS_COMPLETED else 202
