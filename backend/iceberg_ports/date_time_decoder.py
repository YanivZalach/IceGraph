from datetime import date, datetime, time, timedelta, timezone


def decode_date(days_since_epoch: int) -> str:
    return (date(1970, 1, 1) + timedelta(days=days_since_epoch)).isoformat()


def decode_time(microseconds_since_midnight: int) -> str:
    hours, remainder = divmod(microseconds_since_midnight, 3_600_000_000)
    minutes, remainder = divmod(remainder, 60_000_000)
    seconds, microseconds = divmod(remainder, 1_000_000)

    return time(hours, minutes, seconds, microseconds).isoformat()


def decode_timestamp(microseconds_since_epoch: int) -> str:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    timestamp = epoch + timedelta(microseconds=microseconds_since_epoch)

    return timestamp.isoformat()


def decode_nanosecond_timestamp(nanoseconds_since_epoch: int) -> str:
    seconds, nanoseconds = divmod(nanoseconds_since_epoch, 1_000_000_000)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    timestamp = epoch + timedelta(seconds=seconds)

    return f"{timestamp:%Y-%m-%dT%H:%M:%S}.{nanoseconds:09d}+00:00"
