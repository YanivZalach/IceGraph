import asyncio
from collections.abc import Callable

from openai import APIConnectionError, APIError, APIStatusError, APITimeoutError, AsyncOpenAI, BadRequestError

from assistant.constants import (
    ASSISTANT_SYSTEM_PROMPT,
    CONNECTION_ERROR,
    EMPTY_ANSWER_ERROR,
    KEYLESS_API_KEY,
    MODEL_ERROR,
    PAGE_DESCRIPTIONS,
    REJECTED_ERROR,
    SNAPSHOT_NOT_SET,
    STATUS_ERROR,
    SYSTEM_ROLE,
    TIMEOUT_ERROR,
    UNEXPECTED_ERROR,
)
from env import Env


class EmptyAnswerError(Exception):
    pass


def is_assistant_enabled() -> bool:
    return bool(Env.AI_API_BASE_URL and Env.AI_MODEL)


def build_chat_messages(table_name: str, start_snapshot_id: str, end_snapshot_id: str, page: str, context: str, messages: list[dict]) -> list[dict]:
    system_prompt = ASSISTANT_SYSTEM_PROMPT.format(
        table_name=table_name,
        page_description=PAGE_DESCRIPTIONS[page],
        start_snapshot_id=start_snapshot_id or SNAPSHOT_NOT_SET,
        end_snapshot_id=end_snapshot_id or SNAPSHOT_NOT_SET,
        context=context,
    )

    return [{"role": SYSTEM_ROLE, "content": system_prompt}, *messages]


async def _stream_answer(chat_messages: list[dict], on_answer_part: Callable[[str], None]):
    has_content = False

    async with AsyncOpenAI(
        base_url=Env.AI_API_BASE_URL,
        api_key=Env.AI_API_KEY or KEYLESS_API_KEY,
        timeout=Env.AI_REQUEST_TIMEOUT_SECONDS,
    ) as client:
        async with await client.chat.completions.create(model=Env.AI_MODEL, messages=chat_messages, stream=True) as stream:
            async for chunk in stream:
                if not chunk.choices:
                    continue

                content = chunk.choices[0].delta.content
                if content:
                    has_content = True
                    on_answer_part(content)

    if not has_content:
        raise EmptyAnswerError()


async def _stream_answer_within_deadline(chat_messages: list[dict], on_answer_part: Callable[[str], None]):
    async with asyncio.timeout(Env.AI_REQUEST_TIMEOUT_SECONDS):
        await _stream_answer(chat_messages, on_answer_part)


def answer_question(chat_messages: list[dict], on_answer_part: Callable[[str], None]):
    asyncio.run(_stream_answer_within_deadline(chat_messages, on_answer_part))


def describe_assistant_error(error: Exception) -> str:
    if isinstance(error, (APITimeoutError, TimeoutError)):
        return TIMEOUT_ERROR

    if isinstance(error, APIConnectionError):
        return CONNECTION_ERROR

    if isinstance(error, BadRequestError):
        return REJECTED_ERROR

    if isinstance(error, APIStatusError):
        return STATUS_ERROR.format(status_code=error.status_code)

    if isinstance(error, APIError):
        return MODEL_ERROR

    if isinstance(error, EmptyAnswerError):
        return EMPTY_ANSWER_ERROR

    return UNEXPECTED_ERROR
