from datetime import datetime, timezone

def floor_timestamp(ts: datetime, window_minutes: int) -> datetime:
    """Floors a timestamp to the start of its tumbling window."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    window_start_minute = (ts.minute // window_minutes) * window_minutes

    return ts.replace(
        minute=window_start_minute,
        second=0,
        microsecond=0,
    )


def window_ttl_seconds(window_minutes: int) -> int:
    """TTL policy: window + small buffer"""
    return int((window_minutes * 60) * 1.4)