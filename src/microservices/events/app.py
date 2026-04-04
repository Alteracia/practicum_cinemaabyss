import os
import logging
from flask import Flask, request, jsonify
from producer import EventProducer
from consumer import EventConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Initialize producer and consumer
producer = EventProducer()
consumer = EventConsumer()

# Start the consumer in background thread
consumer.start()

logger.info("Events service started")


@app.route('/api/events/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": True}), 200


@app.route('/api/events/movie', methods=['POST'])
def create_movie_event():
    """
    Create a movie event.
    
    Expected payload:
    {
        "movie_id": 1,
        "title": "Test Movie Event",
        "action": "viewed",
        "user_id": 1
    }
    """
    try:
        event_data = request.get_json()
        
        if not event_data:
            return jsonify({"error": "No JSON payload provided"}), 400
        
        # Validate required fields
        required_fields = ['movie_id', 'title', 'action', 'user_id']
        for field in required_fields:
            if field not in event_data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        # Send event to Kafka topic
        success = producer.send_event(
            topic='movie-events',
            event_data=event_data,
            key=str(event_data.get('movie_id'))
        )
        
        if success:
            return jsonify({"status": "success"}), 201
        else:
            return jsonify({"error": "Failed to send event to Kafka"}), 500
            
    except Exception as e:
        logger.error(f"Error processing movie event: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/api/events/user', methods=['POST'])
def create_user_event():
    """
    Create a user event.
    
    Expected payload:
    {
        "user_id": 1,
        "username": "testuser",
        "action": "logged_in",
        "timestamp": "2024-01-01T00:00:00Z"
    }
    """
    try:
        event_data = request.get_json()
        
        if not event_data:
            return jsonify({"error": "No JSON payload provided"}), 400
        
        # Validate required fields
        required_fields = ['user_id', 'username', 'action']
        for field in required_fields:
            if field not in event_data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        # Send event to Kafka topic
        success = producer.send_event(
            topic='user-events',
            event_data=event_data,
            key=str(event_data.get('user_id'))
        )
        
        if success:
            return jsonify({"status": "success"}), 201
        else:
            return jsonify({"error": "Failed to send event to Kafka"}), 500
            
    except Exception as e:
        logger.error(f"Error processing user event: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/api/events/payment', methods=['POST'])
def create_payment_event():
    """
    Create a payment event.
    
    Expected payload:
    {
        "payment_id": 1,
        "user_id": 1,
        "amount": 9.99,
        "status": "completed",
        "timestamp": "2024-01-01T00:00:00Z",
        "method_type": "credit_card"
    }
    """
    try:
        event_data = request.get_json()
        
        if not event_data:
            return jsonify({"error": "No JSON payload provided"}), 400
        
        # Validate required fields
        required_fields = ['payment_id', 'user_id', 'amount', 'status', 'method_type']
        for field in required_fields:
            if field not in event_data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        # Send event to Kafka topic
        success = producer.send_event(
            topic='payment-events',
            event_data=event_data,
            key=str(event_data.get('payment_id'))
        )
        
        if success:
            return jsonify({"status": "success"}), 201
        else:
            return jsonify({"error": "Failed to send event to Kafka"}), 500
            
    except Exception as e:
        logger.error(f"Error processing payment event: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(405)
def method_not_allowed(error):
    """Handle 405 errors."""
    return jsonify({"error": "Method not allowed"}), 405


if __name__ == '__main__':
    # Get port from environment variable or use default
    port = int(os.getenv('PORT', 8082))
    
    try:
        app.run(host='0.0.0.0', port=port, debug=False)
    finally:
        # Clean up resources on shutdown
        logger.info("Shutting down events service...")
        consumer.close()
        producer.close()
