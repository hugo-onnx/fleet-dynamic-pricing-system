import json
import time
import redis
import logging
from confluent_kafka import Consumer, KafkaError

from services.common.config import KAFKA_BOOTSTRAP_SERVERS, REDIS_HOST, REDIS_PORT, CITY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": f"{CITY}-feature-service",
    "auto.offset.reset": "latest",
}
TOPIC = f"rides.requested.{CITY}"

def get_redis_client():
    """Retry Redis connection"""
    while True:
        try:
            client = redis.Redis(
                host=REDIS_HOST, 
                port=REDIS_PORT, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True
            )
            client.ping()
            logger.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return client
        except Exception as e:
            logger.warning(f"Waiting for Redis... {e}")
            time.sleep(3)

def get_consumer():
    """Retry Kafka connection"""
    while True:
        try:
            consumer = Consumer(KAFKA_CONFIG)
            consumer.list_topics(timeout=5)
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as e:
            logger.warning(f"Waiting for Kafka... {e}")
            time.sleep(3)

def process_event(event, redis_client):
    """Process a single ride event and update Redis"""
    try:
        h3_index = event.get("h3_res8")
        event_id = event.get("event_id", "unknown")
        
        if not h3_index:
            logger.warning(f"Event {event_id} missing h3_res8 field")
            return False
        
        key = f"{CITY}:{h3_index}:raw_count"
        redis_client.incr(key)
        
        logger.info(f"Processed event {event_id} → {key}")
        return True
        
    except redis.RedisError as e:
        logger.error(f"Redis error processing event: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Error processing event: {e}", exc_info=True)
        return False

def main():
    """Main consumer loop"""
    redis_client = get_redis_client()
    consumer = get_consumer()
    
    consumer.subscribe([TOPIC])
    logger.info(f"Feature consumer started, subscribed to: {TOPIC}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue
            
            try:
                event = json.loads(msg.value().decode("utf-8"))
                process_event(event, redis_client)
                    
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message: {e}")
            except Exception as e:
                logger.error(f"Error handling message: {e}", exc_info=True)
    
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    finally:
        logger.info("Closing consumer...")
        consumer.close()
        redis_client.close()
        logger.info("Feature consumer stopped")

if __name__ == "__main__":
    main()