import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import h3
import redis
from fastapi import FastAPI, HTTPException

from services.common.config import REDIS_HOST, REDIS_PORT, CITY
from features import fetch_window, WINDOWS
from derive import derive_features

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

redis_client: redis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage Redis connection lifecycle."""
    global redis_client
    
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_keepalive=True,
    )
    
    try:
        redis_client.ping()
        logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise
    yield
    
    if redis_client:
        redis_client.close()
        logger.info("Redis connection closed")


app = FastAPI(
    title="Fleet Dynamic Pricing System",
    description="Real-time feature retrieval for dynamic pricing inference",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    """Health check endpoint."""
    try:
        redis_client.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unhealthy: {e}")


@app.get("/v1/features")
def get_features(
    lat: float,
    lng: float,
    timestamp: str | None = None,
):
    """
    Get derived features for a location.
    
    Args:
        lat: Latitude
        lng: Longitude  
        timestamp: Optional ISO timestamp (defaults to now)
    
    Returns:
        Feature vector with 1m, 5m, 15m window aggregations
    """
    if timestamp:
        try:
            ts = datetime.fromisoformat(timestamp)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid timestamp format")
    else:
        ts = datetime.now(timezone.utc)
    
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    try:
        h3_index = h3.latlng_to_cell(lat, lng, 8)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid coordinates: {e}")

    feature_vector = {}
    for window in WINDOWS:
        raw = fetch_window(
            redis_client=redis_client,
            city=CITY,
            h3_index=h3_index,
            window=window,
            ts=ts,
        )
        derived = derive_features(raw)
        feature_vector[f"{window}m"] = derived

    return {
        "h3_res8": h3_index,
        "timestamp": ts.isoformat(),
        "features": feature_vector,
    }


@app.get("/v1/features/debug")
def get_features_debug(
    lat: float,
    lng: float,
    timestamp: str | None = None,
):
    """
    Debug endpoint to inspect assembled feature vector with raw data.
    
    Same as /v1/features but includes raw Redis snapshots.
    """
    if timestamp:
        try:
            ts = datetime.fromisoformat(timestamp)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid timestamp format")
    else:
        ts = datetime.now(timezone.utc)
    
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    try:
        h3_index = h3.latlng_to_cell(lat, lng, 8)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid coordinates: {e}")

    feature_vector = {}
    raw_snapshots = {}
    
    for window in WINDOWS:
        raw = fetch_window(
            redis_client=redis_client,
            city=CITY,
            h3_index=h3_index,
            window=window,
            ts=ts,
        )
        derived = derive_features(raw)
        feature_vector[f"{window}m"] = derived
        raw_snapshots[f"{window}m"] = raw

    return {
        "h3_res8": h3_index,
        "timestamp": ts.isoformat(),
        "features": feature_vector,
        "raw": raw_snapshots,
    }