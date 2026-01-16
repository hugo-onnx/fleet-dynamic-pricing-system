import h3
import json
import time
import uuid
import random
import logging

from datetime import datetime, timezone
from confluent_kafka import Producer

from services.common.config import KAFKA_BOOTSTRAP_SERVERS, CITY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}

# Topics
RIDE_TOPIC = f"rides.requested.{CITY}"
DRIVER_TOPIC = f"drivers.location.{CITY}"

# Location config
MADRID_CENTER = (40.4168, -3.7038)
LAT_RANGE = 0.02  # ~2.2 km range
LNG_RANGE = 0.02
H3_RESOLUTION = 8
EVENT_INTERVAL = 1.0

# Driver simulation config
NUM_DRIVERS = 200
DRIVERS = {
    f"d_{i}": {
        "lat": MADRID_CENTER[0] + random.uniform(-LAT_RANGE, LAT_RANGE),
        "lng": MADRID_CENTER[1] + random.uniform(-LNG_RANGE, LNG_RANGE),
        "status": "available",
    }
    for i in range(NUM_DRIVERS)
}


def get_producer():
    """Retries connection until Kafka is available"""
    while True:
        try:
            p = Producer(KAFKA_CONFIG)
            p.poll(0)
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return p
        except Exception as e:
            logger.warning(f"Waiting for Kafka... {e}")
            time.sleep(3)


def delivery_report(err, msg):
    """Callback for Kafka message delivery reports"""
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} partition {msg.partition()}")


def generate_ride_event():
    """Generate a random ride request event"""
    lat = MADRID_CENTER[0] + random.uniform(-LAT_RANGE, LAT_RANGE)
    lng = MADRID_CENTER[1] + random.uniform(-LNG_RANGE, LNG_RANGE)
    
    h3_index = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)

    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "h3_res8": h3_index,
    }
    
    return event


def move_driver(lat: float, lng: float, delta: float = 0.005) -> tuple[float, float]:
    """Simulate driver movement with random walk"""
    return (
        lat + random.uniform(-delta, delta),
        lng + random.uniform(-delta, delta),
    )


def produce_driver_events(producer: Producer):
    """Generate and produce location events for all drivers"""
    for driver_id, state in DRIVERS.items():
        # Move driver randomly
        lat, lng = move_driver(state["lat"], state["lng"])
        state["lat"], state["lng"] = lat, lng

        # Randomly update status (70% available, 30% on_trip)
        status = random.choices(
            ["available", "on_trip"],
            weights=[0.7, 0.3],
        )[0]
        state["status"] = status

        # Build event
        event = {
            "driver_id": driver_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "lat": lat,
            "lng": lng,
            "h3_res8": h3.latlng_to_cell(lat, lng, H3_RESOLUTION),
            "status": status,
            "idle_seconds": random.randint(0, 600) if status == "available" else 0,
        }

        producer.produce(
            topic=DRIVER_TOPIC,
            key=driver_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report
        )
    
    # Trigger delivery callbacks
    producer.poll(0)


def main():
    """Main event producer loop"""
    producer = get_producer()
    logger.info(f"Starting event producer")
    logger.info(f"  Ride requests topic: {RIDE_TOPIC}")
    logger.info(f"  Driver locations topic: {DRIVER_TOPIC}")
    logger.info(f"  Simulating {NUM_DRIVERS} drivers")
    
    event_count = 0
    
    try:
        while True:
            try:
                # Produce ride request event
                ride_event = generate_ride_event()
                producer.produce(
                    topic=RIDE_TOPIC,
                    value=json.dumps(ride_event).encode("utf-8"),
                    callback=delivery_report
                )
                
                # Produce driver location events
                produce_driver_events(producer)
                
                # Poll to trigger callbacks
                producer.poll(0)
                
                event_count += 1
                if event_count % 10 == 0:
                    logger.info(
                        f"Produced {event_count} ride events, "
                        f"{event_count * NUM_DRIVERS} driver pings"
                    )
                
                time.sleep(EVENT_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error producing event: {e}", exc_info=True)
                time.sleep(1)
                
    except KeyboardInterrupt:
        logger.info("Shutting down event producer...")
    finally:
        logger.info("Flushing remaining messages...")
        producer.flush()
        logger.info("Event producer stopped")


if __name__ == "__main__":
    main()