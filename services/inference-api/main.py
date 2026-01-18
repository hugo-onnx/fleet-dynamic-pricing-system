import h3
import time
import json
import redis
import logging
import numpy as np

from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

from services.common.config import REDIS_HOST, REDIS_PORT, CITY
from features import fetch_window, WINDOWS
from derive import derive_features
from pricing import compute_price_multiplier
from monitoring.metrics import record_latency
from monitoring.drift import record_feature_snapshot
from eta.model import ETAEstimator
from eta.features import assemble_eta_features

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

redis_client: redis.Redis | None = None
eta_model: ETAEstimator | None = None

ETA_MODEL_PATH = "/app/models/eta_model.joblib"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage Redis connection lifecycle."""
    global redis_client, eta_model
    
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

    try:
        eta_model = ETAEstimator(ETA_MODEL_PATH)
        logger.info(f"Loaded ETA model from {ETA_MODEL_PATH}")
    except FileNotFoundError:
        logger.warning(f"ETA model not found at {ETA_MODEL_PATH}, ETA endpoint will be disabled")
        eta_model = None
    except Exception as e:
        logger.error(f"Failed to load ETA model: {e}")
        eta_model = None

    yield
    
    if redis_client:
        redis_client.close()
        logger.info("Redis connection closed")


app = FastAPI(
    title="Ride-Hailing Dynamic Pricing ETA System",
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


@app.post("/v1/pricing/quote")
def pricing_quote(
    lat: float,
    lng: float,
    timestamp: str | None = None,
):
    """
    Get a dynamic pricing quote for a location.
    
    Uses 5-minute window features to compute surge pricing multiplier.
    Includes monitoring for latency and feature drift.
    
    Args:
        lat: Latitude
        lng: Longitude
        timestamp: Optional ISO timestamp (defaults to now)
    
    Returns:
        Pricing quote with multiplier and feature breakdown
    """
    start = time.perf_counter()

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

    raw_5m = fetch_window(
        redis_client=redis_client,
        city=CITY,
        h3_index=h3_index,
        window=5,
        ts=ts,
    )

    features_5m = derive_features(raw_5m)
    pricing = compute_price_multiplier(features_5m)

    record_feature_snapshot(
        redis_client=redis_client,
        city=CITY,
        features=features_5m,
    )

    latency_ms = (time.perf_counter() - start) * 1000
    record_latency(redis_client, "pricing", latency_ms)

    return {
        "city": CITY,
        "h3_res8": h3_index,
        "timestamp": ts.isoformat(),
        "features": features_5m,
        "pricing": pricing,
        "latency_ms": round(latency_ms, 2),
    }


@app.get("/v1/monitoring/drift")
def drift_summary():
    """
    Get feature drift summary statistics.
    
    Analyzes stored feature snapshots to compute percentile statistics
    for key features, useful for detecting distribution drift over time.
    
    Returns:
        Summary with p50 and p95 for key features, or insufficient_data status
    """
    key = f"drift:{CITY}"
    data = redis_client.lrange(key, 0, -1)

    if len(data) < 50:
        return {"status": "insufficient_data", "samples": len(data)}

    parsed = [json.loads(x)["features"] for x in data]

    def summarize(field):
        values = [f[field] for f in parsed if field in f]
        if not values:
            return {"p50": 0.0, "p95": 0.0}
        return {
            "p50": round(float(np.percentile(values, 50)), 3),
            "p95": round(float(np.percentile(values, 95)), 3),
        }

    return {
        "city": CITY,
        "samples": len(parsed),
        "features": {
            "supply_demand_ratio": summarize("supply_demand_ratio"),
            "deadhead_km_avg": summarize("deadhead_km_avg"),
            "surge_pressure": summarize("surge_pressure"),
        },
    }


@app.post("/v1/eta/quote")
def eta_quote(
    lat: float,
    lng: float,
    trip_distance_km: float,
    timestamp: str | None = None,
):
    """
    Get an ETA estimate for a trip.
    
    Uses marketplace features and trip distance to predict
    estimated time of arrival using an XGBoost model.
    
    Args:
        lat: Pickup latitude
        lng: Pickup longitude
        trip_distance_km: Estimated trip distance in kilometers
        timestamp: Optional ISO timestamp (defaults to now)
    
    Returns:
        ETA prediction in seconds and minutes with latency
    """
    if eta_model is None:
        raise HTTPException(
            status_code=503, 
            detail="ETA model not loaded. Please ensure model file exists."
        )
    
    start = time.perf_counter()

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

    raw_5m = fetch_window(
        redis_client=redis_client,
        city=CITY,
        h3_index=h3_index,
        window=5,
        ts=ts,
    )

    features_5m = derive_features(raw_5m)

    eta_features = assemble_eta_features(
        trip_distance_km=trip_distance_km,
        features_5m=features_5m,
    )

    eta_seconds = eta_model.predict(eta_features)

    latency_ms = (time.perf_counter() - start) * 1000
    record_latency(redis_client, "eta", latency_ms)

    return {
        "city": CITY,
        "h3_res8": h3_index,
        "trip_distance_km": trip_distance_km,
        "eta_seconds": int(eta_seconds),
        "eta_minutes": round(eta_seconds / 60, 1),
        "latency_ms": round(latency_ms, 2),
    }