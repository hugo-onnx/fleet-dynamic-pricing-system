import time
from datetime import datetime, timezone
import redis

METRIC_TTL = 3600  # 1 hour


def record_latency(redis_client: redis.Redis, key: str, duration_ms: float):
    """Record latency metric in a sorted set with timestamp as score."""
    ts = datetime.now(timezone.utc).isoformat()
    redis_client.zadd(
        f"metrics:latency:{key}",
        {ts: duration_ms},
    )
    redis_client.expire(f"metrics:latency:{key}", METRIC_TTL)


def record_feature_freshness(redis_client: redis.Redis, window: int, delay_sec: int):
    """Record feature freshness delay in a list."""
    redis_client.lpush(
        f"metrics:feature_freshness:{window}m",
        delay_sec,
    )
    redis_client.ltrim(f"metrics:feature_freshness:{window}m", 0, 1000)