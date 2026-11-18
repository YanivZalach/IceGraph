from dataclasses import dataclass

from assistant.constants import ASSISTANT_ROLE, PAGE_DESCRIPTIONS, USER_ROLE


def _is_valid_text(text: str) -> bool:
    try:
        text.encode("utf-8")

    except UnicodeEncodeError:
        return False

    return True


def _count_characters(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


@dataclass
class AssistantRequest:
    table_name: str
    page: str
    start_snapshot_id: str
    end_snapshot_id: str
    context: str
    messages: list[dict]

    @staticmethod
    def find_problem(body) -> str | None:
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

    @classmethod
    def from_body(cls, body: dict) -> "AssistantRequest":
        return cls(
            table_name=body["table_name"],
            page=body["page"],
            start_snapshot_id=body.get("start_snapshot_id", ""),
            end_snapshot_id=body.get("end_snapshot_id", ""),
            context=body.get("context", ""),
            messages=[{"role": message["role"], "content": message["content"]} for message in body["messages"]],
        )

    def count_characters(self) -> int:
        return _count_characters(self.context) + sum(_count_characters(message["content"]) for message in self.messages)
