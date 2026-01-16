from datetime import datetime, timezone, timedelta

import redis

WINDOWS = [1, 5, 15]


def floor_completed_window(ts: datetime, window_minutes: int) -> datetime:
    """
    Returns the most recent COMPLETED tumbling window.
    
    Unlike floor_timestamp (which returns the current window),
    this returns the previous window if we're still inside the current one,
    ensuring we only return fully aggregated data.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    floored_minute = (ts.minute // window_minutes) * window_minutes
    window_start = ts.replace(
        minute=floored_minute,
        second=0,
        microsecond=0,
    )

    if window_start + timedelta(minutes=window_minutes) > ts:
        window_start -= timedelta(minutes=window_minutes)

    return window_start


def redis_key(city: str, h3_index: str, window: int, window_start: datetime) -> str:
    """Build Redis key for a feature window."""
    return f"{city}:{h3_index}:{window}m:{window_start.isoformat()}"


def fetch_window(
    redis_client: redis.Redis,
    city: str,
    h3_index: str,
    window: int,
    ts: datetime,
) -> dict:
    """
    Fetch latest completed window, with one-step fallback.
    
    Returns the most recent completed window's data. If that window
    has no data (sparse H3 cell), falls back to the previous window.
    """
    window_start = floor_completed_window(ts, window)
    key = redis_key(city, h3_index, window, window_start)

    data = redis_client.hgetall(key)
    if data:
        return data

    prev_start = window_start - timedelta(minutes=window)
    prev_key = redis_key(city, h3_index, window, prev_start)
    return redis_client.hgetall(prev_key)