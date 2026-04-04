import os
import json
import logging
import threading
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get Kafka broker address from environment variable
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka:9092')

# Topics to subscribe to
TOPICS = ['movie-events', 'user-events', 'payment-events']


class EventConsumer:
    """Kafka consumer for reading events from Kafka topics."""
    
    def __init__(self):
        self.consumer = None
        self.running = False
        self.consumer_thread = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Kafka broker."""
        try:
            self.consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=[KAFKA_BROKERS],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='events-service-consumer-group',
                consumer_timeout_ms=1000
            )
            logger.info(f"Kafka consumer connected to broker: {KAFKA_BROKERS}")
            logger.info(f"Subscribed to topics: {TOPICS}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka broker: {e}")
            self.consumer = None
    
    def _consume_messages(self):
        """Continuously consume messages from Kafka topics."""
        while self.running:
            if self.consumer is None:
                # Try to reconnect if connection is lost
                logger.warning("Kafka consumer is not connected, attempting to reconnect...")
                self._connect()
                time.sleep(5)
                continue
            
            try:
                # Poll for messages with a timeout
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        self._process_message(message)
                        
            except KafkaError as e:
                logger.error(f"Kafka error while consuming messages: {e}")
                # Try to reconnect
                self.consumer = None
                time.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error while consuming messages: {e}")
                time.sleep(5)
    
    def _process_message(self, message):
        """
        Process a received Kafka message.
        
        Args:
            message: The Kafka message to process
        """
        try:
            logger.info(
                f"Message consumed from topic '{message.topic}', "
                f"partition {message.partition}, "
                f"offset {message.offset}, "
                f"key: {message.key}, "
                f"value: {message.value}"
            )
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def start(self):
        """Start the consumer in a background thread."""
        if self.running:
            logger.warning("Consumer is already running")
            return
        
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume_messages, daemon=True)
        self.consumer_thread.start()
        logger.info("Kafka consumer started in background thread")
    
    def stop(self):
        """Stop the consumer."""
        self.running = False
        if self.consumer_thread is not None:
            self.consumer_thread.join(timeout=5)
            logger.info("Kafka consumer stopped")
    
    def close(self):
        """Close the Kafka consumer connection."""
        self.stop()
        if self.consumer is not None:
            try:
                self.consumer.close()
                logger.info("Kafka consumer connection closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
