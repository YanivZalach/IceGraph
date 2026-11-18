import asyncio
from collections.abc import Callable

from openai import APIConnectionError, APIError, APIStatusError, APITimeoutError, AsyncOpenAI, BadRequestError

from assistant.assistant_request import AssistantRequest
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


class AssistantChat:
    def __init__(self, assistant_request: AssistantRequest):
        self._chat_messages = self._build_chat_messages(assistant_request)

    @staticmethod
    def is_enabled() -> bool:
        return bool(Env.AI_API_BASE_URL and Env.AI_MODEL)

    @staticmethod
    def describe_error(error: Exception) -> str:
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

    @staticmethod
    def _build_chat_messages(assistant_request: AssistantRequest) -> list[dict]:
        system_prompt = ASSISTANT_SYSTEM_PROMPT.format(
            table_name=assistant_request.table_name,
            page_description=PAGE_DESCRIPTIONS[assistant_request.page],
            start_snapshot_id=assistant_request.start_snapshot_id or SNAPSHOT_NOT_SET,
            end_snapshot_id=assistant_request.end_snapshot_id or SNAPSHOT_NOT_SET,
            context=assistant_request.context,
        )

        return [{"role": SYSTEM_ROLE, "content": system_prompt}, *assistant_request.messages]

    def answer(self, on_answer_part: Callable[[str], None]):
        asyncio.run(self._stream_answer_within_deadline(on_answer_part))

    async def _stream_answer_within_deadline(self, on_answer_part: Callable[[str], None]):
        async with asyncio.timeout(Env.AI_REQUEST_TIMEOUT_SECONDS):
            await self._stream_answer(on_answer_part)

    async def _stream_answer(self, on_answer_part: Callable[[str], None]):
        has_content = False

        async with AsyncOpenAI(
            base_url=Env.AI_API_BASE_URL,
            api_key=Env.AI_API_KEY or KEYLESS_API_KEY,
            timeout=Env.AI_REQUEST_TIMEOUT_SECONDS,
        ) as client:
            async with await client.chat.completions.create(model=Env.AI_MODEL, messages=self._chat_messages, stream=True) as stream:
                async for chunk in stream:
                    if not chunk.choices:
                        continue

                    content = chunk.choices[0].delta.content
                    if content:
                        has_content = True
                        on_answer_part(content)

        if not has_content:
            raise EmptyAnswerError()
