import uuid
from functools import lru_cache
from pathlib import Path

USER_ID_HEADER = "X-IceGraph-User-Id"


@lru_cache(maxsize=1)
def get_user_id() -> str:
    new_user_id = uuid.uuid4().hex

    try:
        path = Path.home() / ".config" / "icegraph" / "client_id"
        if path.exists():
            saved_user_id = path.read_text().strip()
            if saved_user_id:
                return saved_user_id

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(new_user_id)

    except (OSError, RuntimeError, UnicodeError):
        return new_user_id

    return new_user_id
