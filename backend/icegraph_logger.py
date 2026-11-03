import json
import logging
from datetime import datetime, timezone

from flask import has_request_context, request

from constants import USER_ID_HEADER


class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            "time": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        user_id = request.headers.get(USER_ID_HEADER) if has_request_context() else None
        if user_id:
            entry["user_id"] = user_id

        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            entry["exception"] = record.exc_text

        if record.stack_info:
            entry["stack"] = self.formatStack(record.stack_info)

        return json.dumps(entry, default=str)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

json_handler = logging.StreamHandler()
json_handler.setFormatter(JsonFormatter())

logger = logging.getLogger("IceGraph")
logger.addHandler(json_handler)
logger.propagate = False
