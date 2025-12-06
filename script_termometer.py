# INSTALL azure-eventhub before running!
# pip command:
# pip install azure-eventhub

from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime, timezone
import hashlib
import random
import time
import json

# Replace the placeholders with your Event Hubs connection string and event hub name
EVENTHUB_NAME = 'es_0d383749-fd0d-4fdf-b5aa-46598cd83c08'
CONNECTION_STR = 'Endpoint=sb://esehdbos9k5azx62clbh4o.servicebus.windows.net/;SharedAccessKeyName=key_dd13243e-a0ad-43d1-a0dc-69534b293236;SharedAccessKey=KspMfooLxQybiz2Yym6ZLUkZb63ibk+8v+AEhMrsKEk=;EntityPath=es_0d383749-fd0d-4fdf-b5aa-46598cd83c08'


# Configuration variables
MIN_TEMPERATURE = 19.0          # Minimum temperature value
MAX_TEMPERATURE = 20.0          # Maximum temperature value
COUNTRY = "USA"                 # Country
CITY = "new york"               # City
SLEEP_TIME = 3                  # Sleep time before sending next event

# Example message
'''
{
    "country": "USA",
    "city": "new york",
    "timestamp": "2025-01-25T12:36:03.826769+00:00",
    "temperature_readings": [
        {
            "sensor": "sensor_1",
            "temperature": "19.34"
        },
        {
            "sensor": "sensor_2",
            "temperature": "19.21"
        },
        {
            "sensor": "sensor_3",
            "temperature": "19.69"
        }
    ],
    "event_id": "88709af29e138d8d906e009a800ebaddacd46d89d20ec91151e2ab91557170c5"
}
'''

# Create a producer client to send messages to the event hub
producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

def generate_fake_temperature(min_temp, max_temp):
    """Simulate a fake temperature reading within the specified range or generate null."""
    return str(round(random.uniform(min_temp, max_temp), 2))

def get_random_sensor_readings(min_temp, max_temp):
    """Generate a list of temperature readings for random sensors."""
    sensors = ["sensor_1", "sensor_2", "sensor_3"]
    selected_sensors = random.sample(sensors, random.randint(1, len(sensors)))
    return [{"sensor": sensor, "temperature": generate_fake_temperature(min_temp, max_temp)}
            for sensor in selected_sensors]

def generate_event_id(payload):
    """Generate a SHA256 hash as a unique event ID."""
    hash_object = hashlib.sha256(json.dumps(payload, sort_keys=True).encode('utf-8'))
    return hash_object.hexdigest()

def get_current_timestamp():
    """Return the current timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()

try:
    # Continuously generate and send fake temperature readings
    while True:
        # Create a batch.
        event_data_batch = producer.create_batch()
        
        # Generate random sensor readings
        temperature_readings = get_random_sensor_readings(MIN_TEMPERATURE, MAX_TEMPERATURE)
        
        # Create the payload
        payload = {
            "country": COUNTRY,
            "city": CITY,
            "timestamp": get_current_timestamp(),
            "temperature_readings": temperature_readings
        }
        
        # Generate an event_id
        payload["event_id"] = generate_event_id(payload)
        
        # Format the message as JSON
        message = json.dumps(payload)

        # Add the JSON-formatted message to the batch
        event_data_batch.add(EventData(message))
        
        # Send the batch of events to the event hub
        producer.send_batch(event_data_batch)
        
        print(json.dumps(json.loads(message), indent=4))
        print(event_data_batch)
        
        # Wait for a bit before sending the next reading
        time.sleep(SLEEP_TIME)
except KeyboardInterrupt:
    print("Stopped by the user")
except Exception as e:
    print(f"Error: {e}")
finally:
    # Close the producer
    producer.close()