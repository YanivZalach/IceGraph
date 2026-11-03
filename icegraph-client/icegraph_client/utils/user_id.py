import uuid
from functools import lru_cache
from pathlib import Path

USER_ID_HEADER = "X-IceGraph-User-Id"


def _load_or_create_saved_user_id() -> str:
    path = Path.home() / ".config" / "icegraph" / "client_id"
    if path.exists():
        saved_user_id = path.read_text().strip()
        if saved_user_id:
            return saved_user_id

    user_id = uuid.uuid4().hex
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(user_id)

    return user_id


@lru_cache(maxsize=1)
def get_user_id() -> str:
    try:
        return _load_or_create_saved_user_id()

    except (OSError, RuntimeError, UnicodeError):
        return uuid.uuid4().hex
