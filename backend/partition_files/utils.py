from typing import Optional

from base_classes.utils import epoch_millis_to_arrow_utc

TIMESTAMP_FORMAT = "YYYY-MM-DD HH:mm:ss.SSSSSS"


def format_epoch_millis(timestamp_ms: Optional[int]) -> Optional[str]:
    if timestamp_ms is None:
        return None

    return epoch_millis_to_arrow_utc(timestamp_ms).format(TIMESTAMP_FORMAT)
