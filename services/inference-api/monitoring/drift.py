from datetime import datetime, timezone
import redis
import json

DRIFT_TTL = 86400  # 24 hours


def record_feature_snapshot(
    redis_client: redis.Redis,
    city: str,
    features: dict,
):
    """
    Store lightweight snapshot for drift analysis.
    
    Maintains a rolling window of the last 2000 feature snapshots
    for detecting feature drift over time.
    """
    ts = datetime.now(timezone.utc).isoformat()

    payload = {
        "timestamp": ts,
        "features": features,
    }

    key = f"drift:{city}"
    redis_client.rpush(key, json.dumps(payload))
    redis_client.ltrim(key, -2000, -1)
    redis_client.expire(key, DRIFT_TTL)