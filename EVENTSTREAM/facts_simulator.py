# INSTALL azure-eventhub before running!
# pip install azure-eventhub

from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime, timezone
import hashlib
import random
import time
import json

# Replace the placeholders with your Event Hubs connection string and event hub name
EVENTHUB_NAME = 'XXXX'
CONNECTION_STR = 'XXXX'

# Configuration variables
MIN_DELAY = 0  # Minimum delay in seconds
MAX_DELAY = 10  # Maximum delay in seconds

# Lists for constructing random messages
STARTERS = [
    "HERNAN", "LIMBER", "JOSE", "PEDRO"
]
SUBJECTS = [
    "PLANTA I", "PLANTA II", "PLANTA III"
]
ACTIONS = [
    "APAGAR", "ENCENDER", 
    "MANTENER"
]
ENDINGS = [
    "MAQUINA1", "MAQUINA2", 
    "MAQUINA3"
]

# Create a producer client to send messages to the event hub
producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

def generate_message_id(payload):
    """Generate a SHA256 hash as a unique message ID."""
    hash_object = hashlib.sha256(json.dumps(payload, sort_keys=True).encode('utf-8'))
    return hash_object.hexdigest()

def get_current_timestamp():
    """Return the current timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()

def generate_random_message():
    """Generate a random message by combining elements from different lists."""
    return f"{random.choice(STARTERS)} {random.choice(SUBJECTS)} {random.choice(ACTIONS)} {random.choice(ENDINGS)}"

try:
    while True:
        # Create a batch
        event_data_batch = producer.create_batch()
        
        # Generate a random message
        random_message = generate_random_message()
        
        # Create the payload
        payload = {
            "timestamp": get_current_timestamp(),
            "message": random_message
        }
        
        # Generate a unique message ID
        payload["message_id"] = generate_message_id(payload)
        
        # Format the message as JSON
        message = json.dumps(payload)
        
        # Add the JSON-formatted message to the batch
        event_data_batch.add(EventData(message))
        
        # Send the batch of events to the event hub
        producer.send_batch(event_data_batch)
        
        print(json.dumps(json.loads(message), indent=4))
        print("Message sent successfully!")
        
        # Sleep for a random interval before sending the next message
        sleep_time = random.uniform(MIN_DELAY, MAX_DELAY)
        print(f"Next message in {sleep_time:.2f} seconds...\n")
        time.sleep(sleep_time)
        
except KeyboardInterrupt:
    print("Stopped by the user")
except Exception as e:
    print(f"Error: {e}")
finally:
    # Close the producer
    producer.close()
