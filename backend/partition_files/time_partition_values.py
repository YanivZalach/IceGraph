import re
from datetime import date, timedelta
from typing import Any, Optional

EPOCH_YEAR = 1970
EPOCH_DATE = date(1970, 1, 1)
MONTHS_PER_YEAR = 12
HOURS_PER_DAY = 24

TIME_TRANSFORM_FORMATS = {
    "year": "yyyy, for example 2024",
    "month": "yyyy-MM, for example 2024-03",
    "hour": "yyyy-MM-dd-HH, for example 2024-03-15-10",
}

YEAR_PATTERN = r"(\d{4})"
MONTH_PATTERN = r"(\d{4})-(\d{2})"
HOUR_PATTERN = r"(\d{4}-\d{2}-\d{2})[- T](\d{2})"


def is_time_transform(transform: str) -> bool:
    return transform in TIME_TRANSFORM_FORMATS


def parse_time_transform_value(transform: str, raw_value: str) -> int:
    value = raw_value.strip()
    if transform == "year":
        year = date(int(_full_match(YEAR_PATTERN, value).group(1)), 1, 1).year

        return year - EPOCH_YEAR

    if transform == "month":
        match = _full_match(MONTH_PATTERN, value)
        month_start = date(int(match.group(1)), int(match.group(2)), 1)

        return (month_start.year - EPOCH_YEAR) * MONTHS_PER_YEAR + month_start.month - 1

    match = _full_match(HOUR_PATTERN, value)
    hour = int(match.group(2))
    if hour >= HOURS_PER_DAY:
        raise ValueError(raw_value)

    return (date.fromisoformat(match.group(1)) - EPOCH_DATE).days * HOURS_PER_DAY + hour


def format_time_transform_value(transform: str, value: Any) -> Optional[str]:
    if not is_time_transform(transform) or not isinstance(value, int) or isinstance(value, bool):
        return None

    try:
        if transform == "year":
            return f"{date(EPOCH_YEAR + value, 1, 1).year:04d}"

        if transform == "month":
            month_start = date(EPOCH_YEAR + value // MONTHS_PER_YEAR, value % MONTHS_PER_YEAR + 1, 1)

            return f"{month_start.year:04d}-{month_start.month:02d}"

        day = EPOCH_DATE + timedelta(days=value // HOURS_PER_DAY)

        return f"{day.year:04d}-{day.month:02d}-{day.day:02d}-{value % HOURS_PER_DAY:02d}"

    except (ValueError, OverflowError):
        return None


def _full_match(pattern: str, value: str) -> re.Match:
    match = re.fullmatch(pattern, value)
    if match is None:
        raise ValueError(value)

    return match
