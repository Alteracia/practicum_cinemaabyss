import os
import json
import logging
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get Kafka broker address from environment variable
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka:9092')


class EventProducer:
    """Kafka producer for sending events to Kafka topics."""
    
    def __init__(self):
        self.producer = None
        self._connect_with_retry()
    
    def _connect_with_retry(self, max_retries=5, retry_delay=5):
        """Establish connection to Kafka broker with retry logic."""
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKERS],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: str(k).encode('utf-8') if k is not None else None,
                    acks='all',
                    retries=3
                )
                logger.info(f"Kafka producer connected to broker: {KAFKA_BROKERS}")
                return
            except KafkaError as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed to connect to Kafka broker: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to Kafka broker after {max_retries} attempts")
                    self.producer = None
    
    def _connect(self):
        """Establish connection to Kafka broker."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k is not None else None,
                acks='all',
                retries=3
            )
            logger.info(f"Kafka producer connected to broker: {KAFKA_BROKERS}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka broker: {e}")
            self.producer = None
    
    def send_event(self, topic, event_data, key=None):
        """
        Send an event to a Kafka topic.
        
        Args:
            topic (str): The Kafka topic to send the event to
            event_data (dict): The event data to send
            key (str, optional): Optional key for partitioning
            
        Returns:
            bool: True if successful, False otherwise
        """
        if self.producer is None:
            logger.warning("Kafka producer is not connected, attempting to reconnect...")
            self._connect_with_retry(max_retries=3, retry_delay=2)
            if self.producer is None:
                logger.error("Kafka producer is still not connected after retry")
                return False
        
        try:
            # Add timestamp to event if not present
            if 'timestamp' not in event_data:
                event_data['timestamp'] = datetime.utcnow().isoformat() + 'Z'
            
            # Send the event
            future = self.producer.send(
                topic,
                value=event_data,
                key=key
            )
            
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Message produced to topic '{record_metadata.topic}', "
                f"partition {record_metadata.partition}, "
                f"offset {record_metadata.offset}, "
                f"key: {key}, "
                f"value: {event_data}"
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send message to topic '{topic}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message to topic '{topic}': {e}")
            return False
    
    def close(self):
        """Close the Kafka producer connection."""
        if self.producer is not None:
            try:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer connection closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
